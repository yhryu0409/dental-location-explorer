"""Conserved resident-choice reallocation; workforce and original scores fixed.

B[o,j] is the ORIGINAL share of origin o choosing market j. Outside share is
1-sum(B[o,:]). No new origin edges, residents, or clinician capacity are created.
"""
import hashlib
import math
import threading
from collections import OrderedDict
from pathlib import Path
import numpy as np
from scipy import sparse
from clusters import encode,sha

KEYS=('dental','medical','local','flow')

def config(effect,weights):
    if not math.isfinite(effect) or not 0<=effect<=1:raise ValueError('invalid_destination_effect')
    w=np.asarray(weights,dtype=float)
    if w.shape!=(4,) or not np.isfinite(w).all() or (w<0).any() or (w>100).any() or (effect>0 and w.sum()<=0):raise ValueError('invalid_destination_weights')
    return float(effect),w/w.sum() if w.sum()>0 else w

def reallocate(B,p,outside,features,weights,effect):
    """Exact component attribution of the net change, not causal estimates."""
    h=features@weights
    v=np.asarray(B@h).ravel()
    den=1+effect*v
    t=p/den
    incoming=np.asarray(B.T@t).ravel()
    pop=incoming*(1+effect*h)
    out=p*outside/den
    # Sum of these four signed contributions equals pop - B.T@p.
    change=np.empty((B.shape[1],4))
    for f in range(4):
        vf=np.asarray(B@features[:,f]).ravel()
        loss=np.asarray(B.T@(t*vf)).ravel()
        change[:,f]=effect*weights[f]*(features[:,f]*incoming-loss)
    origin_error=float(np.max(np.abs(t*(np.asarray(B.sum(axis=1)).ravel()+effect*v+outside)-p))) if len(p) else 0.
    if not np.isfinite(pop).all() or not np.isfinite(change).all():raise ValueError('nonfinite_demand')
    return pop,out,change,origin_error

class DemandModel:
    def __init__(self,model,inputs):
        from server import RUN,read,decay
        self.m=model;self.inputs=inputs;self.rows=inputs['rows'];self.cache=OrderedDict();self.lock=threading.Lock()
        n=len(model.ids); blocks=[];pp=[];oo=[]
        for code,pin in sorted(model.data['source_pins'].items()):
            sp=RUN/f'population/support/{code}.json';ep=RUN/f'population/edges/{code}.npz'
            if sha(sp)!=pin['support'] or sha(ep)!=pin['edges']:raise ValueError('demand_source_hash_mismatch')
            p=np.array([c[2] for c in read(sp)['cells']],dtype=float)
            with np.load(ep) as z:
                rr,cc,dd=z['rr'],z['cc'],z['dd']
                if len(p)!=len(z['pop']):raise ValueError('demand_origin_count_mismatch')
                a=decay(dd)*model.attraction[rr]*model.keep[rr]
                den=1+np.bincount(cc,weights=a,minlength=len(p))
                used=a>0
                block=sparse.csr_matrix((a[used]/den[cc[used]],(cc[used],rr[used])),shape=(len(p),n))
            out=1/den
            np.testing.assert_allclose(block.sum(axis=1).A1+out,1,atol=1e-12,rtol=0)
            blocks.append(block);pp.append(p);oo.append(out)
        self.B=sparse.vstack(blocks,format='csr');self.p=np.concatenate(pp);self.outside=np.concatenate(oo)
        del blocks,pp,oo
        self.base_population=np.asarray(self.B.T@self.p).ravel()
        np.testing.assert_allclose(self.base_population,model.population,atol=.02,rtol=0)
        W=model.W[[model.nfac+model.cix[r['node']] for r in self.rows]].tocsr()
        W.data/=model.supply[W.indices]+W.data
        self.F=W
        self.raw=np.array([r['one'] for r in self.rows])
        self.reproduced=np.asarray(self.F@self.base_population).ravel()
        self.max_baseline_error=float(np.max(np.abs(self.raw-self.reproduced)))
        if self.max_baseline_error>.02:raise ValueError('demand_baseline_reproduction_failed')
        self.features=np.full((n,4),np.nan)
        self.candidate_features=np.full((len(self.rows),4),np.nan)
        for i,r in enumerate(self.rows):
            g=inputs['groups'][r['growth_id']];reg=inputs['regions'].get(r['region_key'])
            values=[g['factors'][k] for k in KEYS[:3]]+[reg['inbound_share'] if reg and reg['status']=='ok' else None]
            values=np.array([np.nan if v is None else v for v in values])
            self.candidate_features[i]=values
            if not r['pending']:self.features[model.ix[r['node']]]=values
        self.pins={'input_release':inputs['release'],'baseline_sha256':sha(model.release),'source_pins_sha256':hashlib.sha256(encode(model.data['source_pins'])).hexdigest(),'supply_weights_sha256':sha(RUN/'integrated_model/supply_weights.npz'),'population_supply_sha256':sha(RUN/'integrated_model/population_supply.npz'),'code_sha256':sha(Path(__file__)),'calculator_sha256':sha(Path(__file__).with_name('destination-demand-math.js'))}
        self.release=hashlib.sha256(encode(self.pins)).hexdigest()[:20]
        self.base_outside=float(np.dot(self.p,self.outside))
        print('destination_demand_ready',self.B.shape,self.B.nnz,'baseline_error',self.max_baseline_error,flush=True)

    def evaluate(self,effect,weights):
        effect,w=config(effect,weights);key=(effect,*w.tolist())
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key);return self.cache[key]
            observed=np.isfinite(self.features[:,w>0]).all(axis=1)
            f=np.where(np.isfinite(self.features),self.features,0.)
            # Missing evidence means NO added attraction, not an estimated zero H.
            f[~observed]=0
            pop,out,change,origin_error=reallocate(self.B,self.p,self.outside,f,w,effect)
            parts=np.asarray(self.F@change)
            delta=parts.sum(axis=1)
            scores=self.raw+delta if effect else self.raw.copy()
            np.testing.assert_allclose(pop-self.base_population,change.sum(axis=1),atol=1e-6,rtol=0)
            np.testing.assert_allclose(scores,self.raw+np.asarray(self.F@(pop-self.base_population)).ravel(),atol=1e-6,rtol=0)
            if (scores<-.000001).any():raise ValueError('negative_demand')
            mass_error=float(pop.sum()+out.sum()-self.p.sum())
            if abs(mass_error)>.01:raise ValueError('resident_mass_not_conserved')
            valid=np.isfinite(self.candidate_features[:,w>0]).all(axis=1)
            rows=[]
            for i,r in enumerate(self.rows):
                eligible=not r['pending'] and (effect==0 or bool(valid[i]))
                rows.append({'node':r['node'],'demand':float(scores[i]) if eligible else None,'delta':float(delta[i]) if eligible else None,'components':dict(zip(KEYS,map(float,parts[i]))) if eligible else None,'missing':[KEYS[j] for j in range(4) if w[j]>0 and not np.isfinite(self.candidate_features[i,j])]})
            result={'release':self.release,'input_release':self.inputs['release'],'config':{'effect':effect,'weights':dict(zip(KEYS,map(float,w)))},'rows':rows,'checks':{'baseline_max_error':self.max_baseline_error,'effect_zero_exact':bool(np.array_equal(scores,self.raw)) if effect==0 else None,'origin_mass_error':origin_error,'national_mass_error':mass_error,'source_residents':float(self.p.sum()),'allocated_before':float(self.base_population.sum()),'allocated_after':float(pop.sum()),'outside_before':self.base_outside,'outside_after':float(out.sum()),'markets_with_effect_evidence':int((observed&self.m.keep).sum()),'markets_kept_at_original_attraction':int((~observed&self.m.keep).sum())},'pins':self.pins}
            self.cache[key]=result
            while len(self.cache)>12:self.cache.popitem(last=False)
            return result
