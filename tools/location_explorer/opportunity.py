"""Independent non-medical candidate universe and resident-origin entry scenarios.

Research comparison only. No resident is owned by an incumbent catchment.
For each dentist f, sum_i P_i*k_if/Z_f = 1; no commercial partition enters scores.
Known-facility conditional allocation, not an estimate of unmet treatment need.
"""
import os
os.environ.setdefault('OPENBLAS_NUM_THREADS', '1')
import argparse
import fcntl
import hashlib
import json
from pathlib import Path
import numpy as np
from scipy.spatial import cKDTree
from scipy import sparse
from server import Model, ROOT, RUN, DENSITY, HERE, T, INV, read, sha

OUT = ROOT / 'research/resident_origin_opportunity'
RADII = (3000, 6000)
SCALE = 750.

def kernel(distance, radius):
    return np.exp(-distance/SCALE)*np.maximum(1-(distance/radius)**2, 0)

def qualifies(commercial, screen, floors):
    # No medical-presence or housing-project-evidence requirement.
    return bool(commercial['source_eligible'] and commercial['candidate_supported']
                and all(screen['features'][k]>=v for k,v in floors.items()))

def entry_parts(pop, distance, existing_density, radius):
    k=kernel(distance,radius); norm=float(pop@k)
    if norm<=0: return np.zeros(len(pop)), np.zeros(len(pop)), 1.
    density=k/norm
    denominator=existing_density+density
    fraction=np.divide(density,denominator,out=np.zeros(len(pop)),where=denominator>0)
    return pop*fraction, density, 0.

def build():
    OUT.mkdir(parents=True,exist_ok=True)
    with (OUT/'build.lock').open('a+') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        m=Model(); screen=read(RUN/'commercial/screen.json'); audit=read(RUN/'population/audit.json')
        if audit['pending'] or audit['unrepresented_mois_dongs'] or audit['unlocated_population']:
            raise ValueError('population_gaps_need_explicit_spatial_mask_before_ranking')
        assert sha(RUN/'commercial/screen.json')==m.data['pins']['current_screen']
        pins={'base_release':sha(m.release),'code':sha(Path(__file__)),
              'population_audit':sha(RUN/'population/audit.json'),
              'screen':sha(RUN/'commercial/screen.json'),
              'commercial':sha(RUN/'commercial/features.json'),
              'sites':sha(DENSITY/'sites.npz'),'density':sha(DENSITY/'allocation.json'),
              'medical':m.data['pins']['medical'],'medical_repairs':m.data['pins']['medical_coordinate_repairs'],
              'sources':m.data['source_pins']}
        release=hashlib.sha256(json.dumps(pins,sort_keys=True).encode()).hexdigest()[:16]
        dest=OUT/'releases'/release; dest.mkdir(parents=True,exist_ok=True)
        if (dest/'result.json').exists():
            result=read(dest/'result.json')
            assert sha(dest/'origins.npz')==result['arrays_sha256']
            publish(dest); print('REUSED',dest,flush=True); return
        arrays=[]; source_ranges=[]; offset=0
        for code,pin in sorted(m.data['source_pins'].items()):
            p=RUN/f'population/support/{code}.json'; assert sha(p)==pin['support']
            a=np.array([c[:3] for c in read(p)['cells']],dtype=float).reshape(-1,3)
            source_ranges.append({'district':code,'offset':offset,'count':len(a)})
            arrays.append(a); offset+=len(a)
        cells=np.concatenate(arrays); pop=cells[:,2]
        assert np.isfinite(cells).all() and (pop>=0).all()
        np.testing.assert_allclose(pop.sum(),m.data['checks']['input_population'],atol=.02,rtol=0)
        xy=np.array(T.transform(cells[:,0],cells[:,1])).T
        facxy=np.array([r['xy'] for r in m.facilities]); doctors=np.array([r['doctors'] for r in m.facilities])
        active=doctors>0
        if not active.any(): raise ValueError('no_known_positive_workforce')
        ftree=cKDTree(facxy); ptree=cKDTree(xy)
        nearest=cKDTree(facxy[active]).query(xy)[0]
        norms=np.zeros((2,len(facxy))); density=np.zeros((2,len(pop)))
        mass=np.zeros((2,len(facxy)))
        # Bounded memory; competition includes all districts, not candidate regions.
        chunk=4096
        for passno in range(2):
            for start in range(0,len(pop),chunk):
                end=min(start+chunk,len(pop))
                ds=ftree.sparse_distance_matrix(cKDTree(xy[start:end]),RADII[-1],output_type='coo_matrix')
                for j,radius in enumerate(RADII):
                    vals=kernel(ds.data,radius)
                    K=sparse.csr_matrix((vals,(ds.row,ds.col)),shape=(len(facxy),end-start)); K.eliminate_zeros()
                    if passno==0: norms[j]+=np.asarray(K@pop[start:end]).ravel()
                    else:
                        inv=np.divide(1.,norms[j],out=np.zeros(len(facxy)),where=norms[j]>0)
                        density[j,start:end]=np.asarray(K.T@(doctors*inv)).ravel()
                        mass[j]+=np.asarray(K@pop[start:end]).ravel()*inv
                if start%(chunk*40)==0: print('origin_pass',passno+1,start,len(pop),flush=True)
        unit_error=float(np.max(abs(mass+(norms==0)-1)))
        assert unit_error<1e-8
        baseline={b['representative']:b for b in m.rows}
        rows=[]; max_entry_unit_error=0.; max_origin_error=0.
        for i,(com,s) in enumerate(zip(m.com,screen['rows'])):
            assert com['id']==s['id']==m.ids[i]
            if not qualifies(com,s,screen['floors']):continue
            anchor=m.anchors[i]; ix=np.array(ptree.query_ball_point(anchor,RADII[-1]),dtype=int)
            dist=np.linalg.norm(xy[ix]-anchor,axis=1); p=pop[ix]
            scores=[]; reserve=0.
            for j,radius in enumerate(RADII):
                part,new_density,reserve=entry_parts(p,dist,density[j,ix],radius)
                max_entry_unit_error=max(max_entry_unit_error,abs(float(p@new_density)+reserve-1))
                den=density[j,ix]+new_density
                remain=p*np.divide(density[j,ix],den,out=np.zeros(len(p)),where=den>0)
                outside=np.where(den==0,p,0)
                max_origin_error=max(max_origin_error,float(np.max(abs(part+remain+outside-p),initial=0)))
                scores.append(float(part.sum()))
            near=dist<=1000; gap=nearest[ix]>1000; old=baseline.get(com['id'])
            nearfac=np.linalg.norm(facxy-anchor,axis=1)
            lon,lat=INV.transform(*anchor)
            rows.append({'id':com['id'],'label':com['current_label'],'province':com['current_province'],
                         'lat':lat,'lon':lon,'score':scores[1],'score_3km':scores[0],
                         'population_1km':float(p[near].sum()),'gap_population_1km':float(p[near&gap].sum()),
                         'gap_share_1km':float(p[near&gap].sum()/p[near].sum()) if p[near].sum()>0 else None,
                         'entry_within_1km':float(part[near].sum()),
                         'entry_from_previously_unallocated':float(part[density[1,ix]==0].sum()),
                         'nearest_dental_m':float(nearfac[active].min()),
                         'dental_500m':int(((nearfac<=500)&active).sum()),
                         'dentists_1km':float(doctors[nearfac<=1000].sum()),
                         'life':com['life'],'locations':com['locations'],
                         'recovered_candidate':s['status']=='housing_evidence_pending',
                         'previous_screen':s['status'],'has_medical_near300':bool(s['medical_near300']+s['oriental_near300']),
                         'old_score':old['entry_market_population_equivalent'] if old else None,
                         'old_rank':old.get('national_rank') if old else None,
                         'old_pending':old['pending'] if old else None,
                         'pending':bool(reserve),'scope':'independent_point_scenario'})
            if len(rows)%1500==0:print('candidate_scores',len(rows),flush=True)
        for rank,r in enumerate(sorted((x for x in rows if not x['pending']),key=lambda x:(-x['score'],x['id'])),1):r['rank']=rank
        np.savez_compressed(dest/'origins.npz',xy=xy,pop=pop,nearest=nearest,density=density,norms=norms)
        result={'release':release,'pins':pins,'source_ranges':source_ranges,'rows':rows,
                'arrays_sha256':sha(dest/'origins.npz'),'source_vintages':m.data['source_vintages'],
                'floors':screen['floors'],'parameters':{'scale_m':SCALE,'radii_m':RADII,
                    'candidate_requirement':'nonmedical floors + supported commercial footprint; medical and project evidence not required',
                    'supply_density':'a_if=k(distance_if)/sum_i P_i*k(distance_if); sum_i P_i*a_if=1 per dentist',
                    'entry_score':'sum_i P_i*a_i,new/(sum_f doctors_f*a_if+a_i,new)',
                    'outside':'before/after: origins without reachable positive supply remain unallocated; otherwise conditional on known network',
                    'primary_radius_m':6000,'primary_radius_is_assumption':True,'independent_candidate_scenarios':True},
                'checks':{'population':float(pop.sum()),'origins':len(pop),'facilities':len(facxy),
                    'known_dentists':float(doctors.sum()),'physician_unit_error':unit_error,
                    'entrant_unit_error':max_entry_unit_error,'origin_mass_error':max_origin_error,
                    'previously_unallocated_population_6km':float(pop[density[1]==0].sum()),
                    'dentists_reserve_6km':float(doctors[norms[1]==0].sum()),
                    'candidate_count':len(rows),'recovered_count':sum(r['recovered_candidate'] for r in rows),
                    'base_release_unchanged':sha(m.release)==pins['base_release']},
                'limitations':['Conditional resident equivalents, not patients, revenue, or unmet medical need.',
                    '6km/3km straight-line scenarios; no road barriers, measured travel or patient-choice calibration.',
                    'No exogenous outside option among covered origins; distant alternatives beyond each radius are omitted.',
                    'Fixed commercial footprints only: no detection of brand-new retail centers.',
                    'Eligibility floors reused as exploratory screening, not clinical viability thresholds.',
                    'Origin supply normalization redistributes capacity, not an observed patient-choice probability.',
                    'Overlapping candidate scenarios must not be added; ranks are point-level, not neighborhood-level.'],
                'canonical_publish_allowed':False}
        assert max_entry_unit_error<1e-8 and max_origin_error<1e-7
        (dest/'result.json').write_text(json.dumps(result,ensure_ascii=False,allow_nan=False))
        publish(dest);print('COMPLETE',dest,result['checks'],flush=True)

def publish(dest):
    p=OUT/'latest.tmp';p.write_text(json.dumps({'path':str(dest.relative_to(ROOT)),'result_sha256':sha(dest/'result.json')}));p.replace(OUT/'latest.json')

class Opportunity:
    def __init__(self,model):
        self.m=model;pointer=read(OUT/'latest.json');self.path=ROOT / pointer['path']
        assert sha(self.path/'result.json')==pointer['result_sha256']
        self.data=read(self.path/'result.json');assert sha(model.release)==self.data['pins']['base_release']
        assert sha(self.path/'origins.npz')==self.data['arrays_sha256']
        z=np.load(self.path/'origins.npz');self.xy=z['xy'];self.pop=z['pop'];self.nearest=z['nearest'];self.density=z['density'];self.norms=z['norms']
        self.tree=cKDTree(self.xy);self.byid={r['id']:r for r in self.data['rows']}

    def meta(self):
        return {k:self.data[k] for k in ('release','source_vintages','parameters','checks','limitations','floors')}|{'regions':sorted({r['province'] for r in self.data['rows']})}

    def detail(self,key):
        r=self.byid[key];anchor=self.m.anchors[self.m.ix[key]]
        ii=np.array(self.tree.query_ball_point(anchor,6000),dtype=int)
        distances=np.linalg.norm(self.xy[ii]-anchor,axis=1);p=self.pop[ii];b=self.density[1,ii]
        part,newdens,_=entry_parts(p,distances,b,6000)
        # Sum losses from each incumbent plus previously unallocated contribution.
        fxy=np.array([f['xy'] for f in self.m.facilities]);selected=np.flatnonzero(np.linalg.norm(fxy-anchor,axis=1)<=12000)
        ds=cKDTree(fxy[selected]).sparse_distance_matrix(cKDTree(self.xy[ii]),6000,output_type='coo_matrix')
        norm=self.norms[1,selected];docs=np.array([self.m.facilities[j]['doctors'] for j in selected])
        coeff=np.divide(docs,norm,out=np.zeros(len(norm)),where=norm>0)
        values=kernel(ds.data,6000)*coeff[ds.row]
        lost=values*np.divide(part[ds.col],b[ds.col],out=np.zeros(len(ds.data)),where=b[ds.col]>0)
        losses=np.bincount(ds.row,weights=lost,minlength=len(selected))
        dental=[]
        for j,f in enumerate(selected):
            if losses[j]>1e-8 or np.linalg.norm(fxy[f]-anchor)<=6000:
                dental.append({k:v for k,v in self.m.facilities[f].items() if k!='xy'}|{'shifted_equivalent':float(losses[j])})
        dental.sort(key=lambda f:-f['shifted_equivalent'])
        previously=float(part[b==0].sum())
        assert abs(losses.sum()+previously-part.sum())<1e-5
        assert abs(part.sum()-r['score'])<1e-5
        # Display only aggregation at 200m, preserving every metric.
        bins={}
        for idx,v in zip(ii,part):
            if v<=0:continue
            x,y=self.xy[idx];a=bins.setdefault((int(np.floor(x/200)),int(np.floor(y/200))),[0.,0.,0.,0.,0.])
            a[0]+=x*self.pop[idx];a[1]+=y*self.pop[idx];a[2]+=self.pop[idx];a[3]+=v
            a[4]+=self.pop[idx]*(self.nearest[idx]>1000)
        origins=[]
        for a in bins.values():
            lon,lat=INV.transform(a[0]/a[2],a[1]/a[2]);origins.append([lon,lat,a[2],a[3],a[4]])
        nearby=np.flatnonzero(np.linalg.norm(self.m.anchors-anchor,axis=1)<=2500)
        return {'candidate':r,'origins':origins,'dental':dental,'previously_unallocated_entry':previously,
                'reassigned_from_existing':float(losses.sum()),'origin_error':float(part.sum()-r['score']),
                'boundaries':{'type':'FeatureCollection','features':[{'type':'Feature','geometry':self.m.geometry(i),
                    'properties':{'id':self.m.ids[i],'label':self.m.com[i]['current_label'],'selected':self.m.ids[i]==key,
                                  'eligible':self.m.ids[i] in self.byid}} for i in nearby]}}

if __name__=='__main__':
    argparse.ArgumentParser(description=__doc__).parse_args();build()
