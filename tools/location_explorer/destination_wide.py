"""Exploratory two-stage national hub choice, preserving resident mass and Q0.
Long-distance geometry is a 5 km source-grid straight-line proxy, NOT travel time.
"""
import hashlib
import math
import os
import threading
from collections import OrderedDict,defaultdict
from pathlib import Path
import numpy as np
from scipy import sparse
from scipy.spatial.distance import cdist
from clusters import encode,sha

DEFAULTS={'effect':1.,'purpose':.2,'national':.2,'regionalKm':40.,'nationalKm':180.,'team':1}
VERSION='wide-choice-v1'

def config(values):
    c={k:float(values.get(k,v)) for k,v in DEFAULTS.items()}
    if not math.isfinite(c['team']) or c['team'] not in (1,3):raise ValueError('invalid_wide_team')
    c['team']=int(c['team'])
    for k,(lo,hi) in {'effect':(0,1),'purpose':(0,1),'national':(0,1),'regionalKm':(10,150),'nationalKm':(50,500)}.items():
        if not math.isfinite(c[k]) or not lo<=c[k]<=hi:raise ValueError('invalid_wide_'+k)
    if c['nationalKm']<c['regionalKm']:raise ValueError('national_scale_smaller_than_regional')
    return c

def team_fractions(weights,supply,team):
    """Entrant team capture in each market, with unchanged per-doctor weights."""
    result=weights.copy().tocsr()
    result.data=team*result.data/(supply[result.indices]+team*result.data)
    return result

def attraction(dental,regional=True):
    """Continuous size screen: no hand-selected cities, tiers, or candidate counts."""
    d=np.asarray(dental,dtype=float);k=5. if regional else 20.
    return np.power(d,.7)*np.square(d/(d+k))

def choice(distance_km,weights,scale,exponent):
    utility=weights[None,:]*np.power(1+distance_km/scale,-exponent)
    den=utility.sum(axis=1)
    if (den<=0).any():raise ValueError('no_destination_options')
    return utility/den[:,None]

def combine(raw,regional,national,c):
    t=c['effect']*c['purpose'];local=raw*(1-t)
    r=t*(1-c['national'])*regional;n=t*c['national']*national
    total=raw.copy() if t==0 else local+r+n
    return total,local,r,n

def origin_labels(crosswalk,codes):
    current=defaultdict(list);original=defaultdict(list)
    for r in crosswalk:
        original[r['district']].append(r['name'].split()[:-1])
        for name in r.get('current_names') or [r['name']]:current[r['district']].append(name.split()[:-1])
    labels={}
    for code in codes:
        now=os.path.commonprefix(current[code]);old=os.path.commonprefix(original[code])
        # A frozen district may now span two new districts. Do not call either one the whole origin.
        labels[code]=(' '.join(old)+' (원천 경계)') if len(now)<2 and len(old)>=2 else (' '.join(now) or '원천 경계 '+code)
    return labels

class WideDemand:
    def __init__(self,m,inputs):
        from server import RUN,read,decay,T
        self.m=m;self.inputs=inputs;self.rows=inputs['rows'];self.rowix={r['node']:i for i,r in enumerate(self.rows)}
        self.lock=threading.RLock();self.cache=OrderedDict();self.routes=OrderedDict()
        W=m.W[[m.nfac+m.cix[r['node']] for r in self.rows]].tocsr()
        self.fractions={n:team_fractions(W,m.supply,n) for n in (1,3)};self.F=self.fractions[1]
        self.raw=np.array([r['one'] for r in self.rows]);self.codes=sorted(m.data['source_pins']);district_p=[];grid_xy=[];grid_pop=[];grid_mass=[];grid_source=[];outside_total=0.
        for ci,code in enumerate(self.codes):
            sp=RUN/f'population/support/{code}.json';ep=RUN/f'population/edges/{code}.npz';pin=m.data['source_pins'][code]
            if sha(sp)!=pin['support'] or sha(ep)!=pin['edges']:raise ValueError('wide_source_mismatch')
            cells=np.array([c[:3] for c in read(sp)['cells']],dtype=float);p=cells[:,2]
            with np.load(ep) as z:
                rr,cc,dd=z['rr'],z['cc'],z['dd'];a=decay(dd)*m.attraction[rr]*m.keep[rr]
                den=1+np.bincount(cc,weights=a,minlength=len(p));allocated=p*(1-1/den)
                district_p.append(np.bincount(rr,weights=p[cc]*a/den[cc],minlength=len(m.ids)))
            outside_total+=float(np.sum(p/den))
            x,y=T.transform(cells[:,0],cells[:,1]);xy=np.column_stack((x,y));bins=np.floor(xy/5000).astype(np.int64)
            _,inv=np.unique(bins,axis=0,return_inverse=True);count=int(inv.max())+1
            mass=np.bincount(inv,weights=allocated,minlength=count);population=np.bincount(inv,weights=p,minlength=count)
            # Demand-weighted centroids for travel proxy; population centroid for empty demand bins.
            weight=np.where(allocated>0,allocated,0);sx=np.bincount(inv,weights=xy[:,0]*weight,minlength=count);sy=np.bincount(inv,weights=xy[:,1]*weight,minlength=count)
            use=mass>0;centers=np.column_stack((sx[use]/mass[use],sy[use]/mass[use]))
            grid_xy.extend(centers.tolist());grid_mass.extend(mass[use].tolist());grid_pop.extend(population.tolist());grid_source.extend([ci]*int(use.sum()))
        self.district_p=np.array(district_p);baseline=self.district_p.sum(axis=0)
        np.testing.assert_allclose(baseline,m.population,atol=.02,rtol=0)
        reproduced=np.asarray(self.F@baseline).ravel();self.baseline_error=float(np.max(np.abs(reproduced-self.raw)))
        if self.baseline_error>.02:raise ValueError('wide_baseline_mismatch')
        raw_three=np.array([r['three'] for r in self.rows])
        reproduced_three=np.asarray(self.fractions[3]@baseline).ravel()
        self.team3_baseline_error=float(np.max(np.abs(reproduced_three-raw_three)))
        if self.team3_baseline_error>.02:raise ValueError('wide_team3_baseline_mismatch')
        self.raw_by_team={1:self.raw,3:raw_three}
        self.mass=np.array(grid_mass);self.source=np.array(grid_source,dtype=int);self.origin_xy=np.array(grid_xy)
        self.source_population=float(sum(grid_pop));self.outside_total=outside_total
        np.testing.assert_allclose(self.mass.sum(),m.population.sum(),atol=.001,rtol=0)
        self.hubs=[];ii=[];jj=[];vv=[];centers=[]
        for g in sorted(m.clusters().data['rows'],key=lambda g:g['id']):
            if g['mode']!='growth' or g['pending'] or g['dental_count']<=0:continue
            members=[m.ix[k] for k in g['members'] if m.keep[m.ix[k]] and m.supply[m.ix[k]]>0]
            if not members:continue
            capacity=m.supply[members];shares=capacity/capacity.sum();j=len(self.hubs)
            self.hubs.append({k:g[k] for k in ('id','label','province','dental_count','facility_count','candidate_count')}|{'allocated_competition':float(capacity.sum())})
            ii.extend(members);jj.extend([j]*len(members));vv.extend(shares.tolist());centers.append(np.average(m.anchors[members],axis=0,weights=shares))
        if not self.hubs:raise ValueError('no_eligible_hubs')
        self.hubix={h['id']:i for i,h in enumerate(self.hubs)}
        self.internal=sparse.csr_matrix((vv,(ii,jj)),shape=(len(m.ids),len(self.hubs)))
        np.testing.assert_allclose(self.internal.sum(axis=0).A1,1,atol=1e-12)
        self.captures={n:(fraction@self.internal).tocsr() for n,fraction in self.fractions.items()}
        for capture in self.captures.values():
            if capture.data.size and (capture.data.min()<0 or capture.data.max()>1+1e-10):raise ValueError('invalid_hub_capture')
        self.distances=cdist(self.origin_xy,np.array(centers))/1000
        self.dental=np.array([h['dental_count'] for h in self.hubs],dtype=float)
        audit_path=RUN/'population/audit.json';audit=read(audit_path)
        self.names=origin_labels(audit['crosswalk'],self.codes)
        self.pins={'input_release':inputs['release'],'baseline_sha256':sha(m.release),'source_pins_sha256':hashlib.sha256(encode(m.data['source_pins'])).hexdigest(),'population_audit_sha256':sha(audit_path),'supply_sha256':sha(RUN/'integrated_model/supply_weights.npz'),'code_sha256':sha(Path(__file__)),'calculator_sha256':sha(Path(__file__).with_name('destination-wide-math.js'))}
        self.release=hashlib.sha256(encode(self.pins)).hexdigest()[:20]
        print('wide_destination_ready',len(self.mass),'origins',len(self.hubs),'hubs','error',self.baseline_error,flush=True)

    def route(self,c):
        key=(c['regionalKm'],c['nationalKm'])
        if key not in self.routes:
            pr=choice(self.distances,attraction(self.dental,True),key[0],3.)
            pn=choice(self.distances,attraction(self.dental,False),key[1],2.)
            err=max(float(np.max(np.abs(pr.sum(axis=1)-1))),float(np.max(np.abs(pn.sum(axis=1)-1))))
            if err>1e-10:raise ValueError('wide_origin_not_conserved')
            hr=self.mass@pr;hn=self.mass@pn
            self.routes[key]=(pr,pn,hr,hn,err)
            while len(self.routes)>2:self.routes.popitem(last=False)
        self.routes.move_to_end(key);return self.routes[key]

    def evaluate(self,values):
        c=config(values);key=tuple(c.values())
        with self.lock:
            if key in self.cache:self.cache.move_to_end(key);return self.cache[key]
            pr,pn,hr,hn,err=self.route(c);capture=self.captures[c['team']];raw=self.raw_by_team[c['team']]
            qr=np.asarray(capture@hr).ravel();qn=np.asarray(capture@hn).ravel();total,local,reg,nat=combine(raw,qr,qn,c)
            t=c['effect']*c['purpose'];allocated=(1-t)*self.mass.sum()+t*(1-c['national'])*hr.sum()+t*c['national']*hn.sum()
            mass_error=float(allocated+self.outside_total-self.source_population)
            if abs(mass_error)>.01:raise ValueError('wide_mass_not_conserved')
            rows=[]
            for i,r in enumerate(self.rows):
                valid=not r['pending'];parts={'local':float(local[i]),'regional':float(reg[i]),'national':float(nat[i])}
                rows.append({'node':r['node'],'raw':float(raw[i]),'demand':float(total[i]) if valid else None,'delta':float(total[i]-raw[i]) if valid else None,'parts':parts if valid else None,'hub_id':r['growth_id'] if r['growth_id'] in self.hubix else None})
            result={'model':VERSION,'release':self.release,'input_release':self.inputs['release'],'config':c,'rows':rows,'checks':{'baseline_max_error':self.baseline_error,'zero_exact':bool(np.array_equal(total,raw)) if t==0 else None,'origin_probability_error':err,'national_mass_error':mass_error,'source_residents':self.source_population,'allocated_before':float(self.mass.sum()),'allocated_after':float(allocated),'outside_unchanged':self.outside_total,'source_grid_count':len(self.mass),'hubs':len(self.hubs)},'pins':self.pins}
            self.cache[key]=result
            while len(self.cache)>8:self.cache.popitem(last=False)
            return result

    def origins(self,node,values):
        c=config(values)
        if node not in self.rowix:raise KeyError(node)
        i=self.rowix[node];r=self.rows[i]
        if r['pending']:return {'model':VERSION,'release':self.release,'config':c,'node':node,'status':'pending','candidate_sources':[],'hub_sources':[],'hub':None}
        with self.lock:
            pr,pn,hr,hn,err=self.route(c);t=c['effect']*c['purpose'];capture=self.captures[c['team']];raw=self.raw_by_team[c['team']];co=capture.getrow(i).toarray().ravel()
            local=(1-t)*np.asarray(self.district_p@self.fractions[c['team']].getrow(i).toarray().ravel()).ravel()
            reg=np.bincount(self.source,weights=self.mass*(pr@co)*t*(1-c['national']),minlength=len(self.codes))
            nat=np.bincount(self.source,weights=self.mass*(pn@co)*t*c['national'],minlength=len(self.codes))
            def sources(a,b,d):
                return sorted([{'code':code,'label':self.names[code],'local':float(a[k]),'regional':float(b[k]),'national':float(d[k]),'total':float(a[k]+b[k]+d[k])} for k,code in enumerate(self.codes) if a[k]+b[k]+d[k]>1e-9],key=lambda x:-x['total'])
            candidate=sources(local,reg,nat);hub=None;hub_sources=[];hi=self.hubix.get(r['growth_id'])
            if hi is not None:
                hreg=np.bincount(self.source,weights=self.mass*pr[:,hi]*t*(1-c['national']),minlength=len(self.codes));hnat=np.bincount(self.source,weights=self.mass*pn[:,hi]*t*c['national'],minlength=len(self.codes))
                hub_sources=sources(np.zeros(len(self.codes)),hreg,hnat)
                hub=self.hubs[hi]|{'regional':float(hreg.sum()),'national':float(hnat.sum()),'total_destination':float(hreg.sum()+hnat.sum()),'candidate_capture_fraction':float(co[hi]),'candidate_from_own_hub':float(co[hi]*(hreg.sum()+hnat.sum()))}
            qr=np.asarray(capture@hr).ravel();qn=np.asarray(capture@hn).ravel()
            predicted=combine(raw,qr,qn,c)[0][i];source_error=float(local.sum()+reg.sum()+nat.sum()-predicted)
            if abs(source_error)>.02:raise ValueError('wide_source_attribution_mismatch')
            return {'model':VERSION,'release':self.release,'config':c,'node':node,'status':'ok','candidate_sources':candidate,'hub_sources':hub_sources,'hub':hub,'source_sum_error':source_error}
