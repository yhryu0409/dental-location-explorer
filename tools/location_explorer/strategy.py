"""Two reporting scales over the unchanged baseline allocation and workforce.

No opportunity-model inputs. Team size changes capacity, never travel radius.
Grouping parameters are exploratory, not inferred physical living-area borders.
"""
import os
os.environ.setdefault('OPENBLAS_NUM_THREADS', '1')
import argparse
import fcntl
import hashlib
import json
from functools import lru_cache
from pathlib import Path
import numpy as np
from scipy import sparse
from scipy.spatial import cKDTree
from scipy.spatial.distance import cdist
from server import Model, ROOT, RUN, T, INV, read, sha, decay

OUT = ROOT / 'research/location_strategy'
PARAMS = {
    'local': {'diameter_m': 750, 'min_pair_similarity': .85},
    'growth': {'anchor_radius_m': 1500, 'diameter_m': 2000,
               'min_anchor_similarity': .85, 'min_pair_similarity': .75},
    'travel_decay_m': 750, 'travel_limit_m': 3000,
    'team_sizes': [1, 2, 3], 'base_bundles_preserved': True,
    'parameters_are_exploratory': True,
    'similarity': 'resident-weighted footprint distance potential, independent of allocation eligibility',
    'province_limits_grouping_only_not_demand_or_competition': True,
}

def team_parts(pop, supply, weight, n):
    if n not in (1, 2, 3):
        raise ValueError('team_must_be_1_2_or_3')
    return pop * n * weight / (supply + n * weight)

def local_groups(seeds, distance, overlap, ids):
    clusters = {i: set(s) for i, s in enumerate(seeds)}
    owner = {k: i for i, s in clusters.items() for k in s}
    ok = (distance <= PARAMS['local']['diameter_m']) & (overlap >= .85 - 1e-12)
    pairs = sorted(((overlap[i,j], ids[i], ids[j], i, j)
                    for i in range(len(ids)) for j in range(i+1, len(ids)) if ok[i,j]),
                   key=lambda x: (-x[0], x[1], x[2]))
    for _, _, _, i, j in pairs:
        a, b = owner[i], owner[j]
        if a == b or not all(ok[p,q] for p in clusters[a] for q in clusters[b]):
            continue
        clusters[a] |= clusters[b]
        for k in clusters.pop(b): owner[k] = a
    return sorted(clusters.values(), key=lambda s: min(ids[i] for i in s))

def representative(members, ids, commercial):
    return min(members, key=lambda i: (not commercial[i].get('preferred', False),
               -commercial[i]['life'], -commercial[i]['stores'], ids[i]))

def growth_groups(seeds, distance, overlap, ids, commercial):
    # Fixed anchor: added members never become stepping stones for chain merges.
    remaining = {i: set(s) for i, s in enumerate(seeds)}
    order = sorted(remaining, key=lambda q: (-sum(commercial[i]['life'] for i in remaining[q]),
                                            min(ids[i] for i in remaining[q])))
    result = []
    for q in order:
        if q not in remaining: continue
        members = remaining.pop(q)
        anchor = representative(members, ids, commercial)
        options = sorted(remaining, key=lambda j: (-min(overlap[anchor,k] for k in remaining[j]),
                                                   min(ids[k] for k in remaining[j])))
        for j in options:
            other = remaining[j]
            union = sorted(members | other)
            if (all(distance[anchor,k] <= 1500 and overlap[anchor,k] >= .85-1e-12 for k in other)
                and distance[np.ix_(union,union)].max() <= 2000
                and overlap[np.ix_(union,union)].min() >= .75-1e-12):
                members |= remaining.pop(j)
        result.append((members, anchor))
    return result

def build(region, model=None):
    OUT.mkdir(parents=True, exist_ok=True)
    with (OUT/'build.lock').open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        m = model if model is not None else Model()
        old = [b for b in m.rows if b['province'] == region]
        if not old: raise ValueError('unknown_region')
        ids = sorted({k for b in old for k in b['members']})
        index = {k:i for i,k in enumerate(ids)}
        gi = np.array([m.ix[k] for k in ids])
        com = [dict(m.com[i],preferred=m.ids[i]==m.baseline['representative']) for i in gi]
        anchors = m.anchors[gi]
        centers = np.array([m.sites[m.groups[i]['sites']].mean(0) for i in gi])
        W = m.W[[m.nfac + m.cix[k] for k in ids]].tocsr()
        # Sources are selected by all destinations of these entrants, not province.
        destination = set(W.indices.tolist()) | set(gi.tolist())
        fractions = W.copy()
        fractions.data /= m.supply[fractions.indices] + fractions.data
        F = fractions.T.tocsr()
        blocks = []; observed=np.zeros(len(ids)); near = np.zeros(len(ids)); reproduced = np.zeros(len(ids))
        sources = []
        for code in sorted(m.data['source_pins']):
            if not m.district_markets(code).intersection(destination): continue
            cells, A = m.district(code)
            observed += A[:,gi].sum(axis=0).A1
            zedge=np.load(RUN/f'population/edges/{code}.npz')
            # A zero allocation can mean a screening exclusion, not absent residents.
            # Grouping therefore uses resident-distance potential, with no medical
            # presence, commercial attraction or eligibility multiplier.
            rr,cc,dd=zedge['rr'],zedge['cc'],zedge['dd']
            pp=np.array([c[2] for c in cells])
            potential=sparse.csr_matrix((pp[cc]*decay(dd),(rr,cc)),shape=(len(m.ids),len(cells)))
            blocks.append(potential[gi].tocsr())
            E = (A @ F).T.tocsr()
            reproduced += E.sum(axis=1).A1
            ll = np.array([c[:2] for c in cells]); xy = np.array(T.transform(ll[:,0],ll[:,1])).T
            ds = cKDTree(anchors).sparse_distance_matrix(cKDTree(xy),1000,output_type='coo_matrix')
            mask = sparse.csr_matrix((np.ones(len(ds.data)),(ds.row,ds.col)),shape=E.shape)
            near += E.multiply(mask).sum(axis=1).A1
            sources.append(code)
            print('strategy_population',region,code,flush=True)
        A = sparse.hstack(blocks,format='csr'); totals = A.sum(axis=1).A1
        np.testing.assert_allclose(observed,m.population[gi],atol=.02,rtol=0)
        B=A.copy(); B.data=np.sqrt(B.data)
        B=sparse.diags(np.divide(1.,np.sqrt(totals),out=np.zeros(len(totals)),where=totals>0))@B
        overlap=np.clip((B@B.T).toarray(),0,1); distance=cdist(centers,centers)
        seeds=[{index[k] for k in b['members']} for b in old]
        local=local_groups(seeds,distance,overlap,ids)
        growth=growth_groups(local,distance,overlap,ids,com)
        screen={r['id']:r for r in read(RUN/'commercial/screen.json')['rows']}
        pending_markets=np.array([screen[k]['status']=='data_pending' for k in m.ids])
        nodes={}; max_error=0.; z=np.load(RUN/'integrated_model/population_supply.npz')
        for i,k in enumerate(ids):
            w=W.getrow(i); p=m.population[w.indices]; d=m.supply[w.indices]
            scenarios={str(n):float(team_parts(p,d,w.data,n).sum()) for n in (1,2,3)}
            max_error=max(max_error,abs(scenarios['1']-reproduced[i]))
            raw_index=m.nfac+m.cix[k]
            # Use the persisted outside/reserve values; never renormalize covered markets to 1.
            outside=float(z['outside'][raw_index]); reserve=float(z['reserve'][raw_index])
            assert abs(float(w.sum())+outside+reserve-1)<1e-8
            reason=list(screen[k]['screen_reasons']) if screen[k]['status'] not in ('ordinary','emerging') else []
            if screen[k]['status'] not in ('ordinary','emerging') and not reason: reason.append(screen[k]['status'])
            if pending_markets[w.indices].any(): reason.append('source_gap_in_destination')
            if reserve>0: reason.append('no_resident_support')
            lon,lat=INV.transform(*anchors[i])
            nodes[k]={'id':k,'label':com[i]['current_label'],'lat':lat,'lon':lon,
                      'life':com[i]['life'],'stores':com[i]['stores'],
                      'team':scenarios,'shock':scenarios['2']/2,'near_1km':float(near[i]),
                      'near_share':float(near[i]/scenarios['1']) if scenarios['1'] else None,
                      'outside_supply':outside,'reserve':reserve,'pending':bool(reason),'reasons':reason,
                      'retained_for_allocation':bool(m.keep[m.ix[k]]),
                      'status':screen[k]['status']}
        assert max_error<.02
        for b in old:
            assert abs(nodes[b['representative']]['team']['1']-b['entry_market_population_equivalent'])<1e-5
        modes={}; owners={}
        for mode,parts in [('local',[(s,representative(s,ids,com)) for s in local]),('growth',growth)]:
            rows=[]; owner={}
            for members,anchor in parts:
                keys=sorted(ids[i] for i in members); ix=[m.ix[k] for k in keys]
                valid={i for i in members if not nodes[ids[i]]['pending']}
                rep=representative(valid or members,ids,com); node=nodes[ids[rep]]
                bid=mode+'-'+hashlib.sha256('|'.join(keys).encode()).hexdigest()[:12]
                union=float(m.population[ix].sum()); supply=float(m.supply[ix].sum())
                all_i=sorted(members)
                row={'id':bid,'mode':mode,'members':keys,'representative':ids[rep],
                     'anchor':ids[anchor],'label':node['label'],'lat':node['lat'],'lon':node['lon'],
                     'population':union,'supply':supply,'life':sum(com[i]['life'] for i in members),
                     'stores':sum(com[i]['stores'] for i in members),'pending':not bool(valid),
                     'pending_members':sum(nodes[k]['pending'] for k in keys),
                     'allocation_excluded_members':sum(not nodes[k]['retained_for_allocation'] for k in keys),
                     'team':node['team'],'shock':node['shock'],'near_1km':node['near_1km'],
                     'diameter_m':float(distance[np.ix_(all_i,all_i)].max()),
                     'min_similarity':float(overlap[np.ix_(all_i,all_i)].min()),
                     'min_anchor_similarity':float(overlap[anchor,all_i].min()),
                     'source_bundles':sum(bool(set(b['members'])&set(keys)) for b in old)}
                rows.append(row)
                for k in keys: owner[k]=bid
            sort=lambda r:(r['pending'],-(r['shock'] if mode=='local' else r['population']),r['id'])
            rows.sort(key=sort)
            for rank,row in enumerate([r for r in rows if not r['pending']],1):row['rank']=rank
            modes[mode]=rows;owners[mode]=owner
        assert all(len({owners[mode][k] for k in b['members']})==1 for mode in modes for b in old)
        assert all(len({owners['growth'][k] for k in g['members']})==1 for g in modes['local'])
        np.testing.assert_allclose([sum(g['population'] for g in modes[x]) for x in modes],m.population[gi].sum(),atol=.02)
        np.testing.assert_allclose([sum(g['supply'] for g in modes[x]) for x in modes],m.supply[gi].sum(),atol=1e-7)
        pins={'base_release':sha(m.release),'code':sha(Path(__file__)),'server_loader':sha(Path(__file__).with_name('server.py'))}
        release=hashlib.sha256(json.dumps({'region':region,'pins':pins,'params':PARAMS},sort_keys=True).encode()).hexdigest()[:16]
        path=OUT/'releases'/release;path.mkdir(parents=True,exist_ok=True)
        result={'release':release,'region':region,'pins':pins,'parameters':PARAMS,'source_vintages':m.data['source_vintages'],
                'nodes':nodes,'modes':modes,'owners':owners,'checks':{'base_bundles':len(old),'points':len(ids),
                'local_bundles':len(local),'growth_bundles':len(growth),'source_districts':sources,
                'max_origin_score_error':max_error,'base_release_unchanged':sha(m.release)==pins['base_release'],
                'population_preserved':float(m.population[gi].sum()),'supply_preserved':float(m.supply[gi].sum())},
                'notes':['기본 배분식·3km 영향권·750m 감쇠·outside 유지. opportunity 비교안 미사용.',
                         '병합은 표시·집계만 변경. 인구와 경쟁을 새로 흡수하거나 의사 몫을 합하지 않음.',
                         '팀 2·3명은 같은 위치·동일 개인 가중치. 브랜드·시너지·진료 구성·직장인 수요 미반영.',
                         '대표는 생활서비스→점포 수, 명촌8길은 기존 사용자 지정 유지. 점수 최댓값으로 선택하지 않음.',
                         '그룹 경계는 도로·하천 장벽을 검증한 생활권이 아님. 시도는 그룹 탐색 범위만 제한.',
                         '묶음 유사도는 주민×거리 잠재 연결로 계산. 배분 제외의 0을 주민 0으로 취급하지 않음.',
                         '로컬 기본 순위는 같은 위치 경쟁의사 1명 추가 후 몫, 성장형은 전체 배분인구 규모 순위.',
                         '생활권 인구는 전체 파이, 팀 몫은 일부. 두 값은 서로 대체하지 않음.']}
        output=path/'result.json'
        if output.exists(): assert read(output)==result
        else: output.write_text(json.dumps(result,ensure_ascii=False,allow_nan=False))
        pointer=read(OUT/'latest.json') if (OUT/'latest.json').exists() else {'regions':{}}
        pointer['regions'][region]={'path':str(path.relative_to(ROOT)),'sha256':sha(output)}
        tmp=OUT/'latest.tmp';tmp.write_text(json.dumps(pointer,ensure_ascii=False));tmp.replace(OUT/'latest.json')
        print('STRATEGY_COMPLETE',release,result['checks'],flush=True)

def reusable(region, model):
    """Resume only a hash-verified release with identical inputs and parameters."""
    if not (OUT/'latest.json').exists(): return False
    p=read(OUT/'latest.json').get('regions',{}).get(region)
    if not p: return False
    path=ROOT / p['path'] / 'result.json'
    if not path.exists() or sha(path)!=p['sha256']: return False
    d=read(path)
    expected={k for b in model.rows if b['province']==region for k in b['members']}
    return (d['region']==region and d['pins']['base_release']==sha(model.release)
            and d['parameters']==PARAMS and set(d['nodes'])==expected)

def build_national():
    m=Model()
    for region in sorted({b['province'] for b in m.rows}):
        if reusable(region,m):
            print('STRATEGY_REUSED',region,flush=True)
        else:
            build(region,m)
    s=Strategies(m);meta=s.meta()
    assert meta['national_ready']
    pointer=read(OUT/'latest.json')
    summary={'base_release':str(m.release),'base_sha256':sha(m.release),
             'regions':pointer['regions'],'meta':meta,
             'checks':{'original_bundles_preserved':True,'candidate_members_preserved':True,
                       'no_duplicate_members_across_regions':True,'population_and_supply_preserved':True},
             'scope_note':'새로운 전국 수집이 아닌 기존 기본안 후보 범위의 재집계. 보류 지역·거점은 기존 상태 유지.'}
    release=hashlib.sha256(json.dumps(summary,sort_keys=True,ensure_ascii=False).encode()).hexdigest()[:16]
    path=OUT/'national'/release;path.mkdir(parents=True,exist_ok=True)
    output=path/'summary.json'
    if output.exists(): assert read(output)==summary
    else: output.write_text(json.dumps(summary,ensure_ascii=False,indent=2,allow_nan=False))
    tmp=OUT/'national_latest.tmp'
    tmp.write_text(json.dumps({'path':str(path.relative_to(ROOT)),'sha256':sha(output)},ensure_ascii=False))
    tmp.replace(OUT/'national_latest.json')
    print('NATIONAL_COMPLETE',release,meta['checks']['전국'],flush=True)

class Strategies:
    def __init__(self, model):
        self.m=model
        pointer=read(OUT/'latest.json');self.regions={}
        self.expected_regions=sorted({r['province'] for r in model.rows})
        self.node_regions={}
        for region,p in sorted(pointer['regions'].items()):
            path=ROOT / p['path'] / 'result.json'
            assert sha(path)==p['sha256']
            d=read(path);assert d['pins']['base_release']==sha(model.release)
            assert d['parameters']==PARAMS
            expected=[b for b in model.rows if b['province']==region]
            assert set(d['nodes'])=={k for b in expected for k in b['members']}
            assert not set(d['nodes']).intersection(self.node_regions)
            self.node_regions.update({k:region for k in d['nodes']})
            for mode,rows in d['modes'].items():
                flat=[k for r in rows for k in r['members']]
                assert len(flat)==len(set(flat)) and set(flat)==set(d['nodes'])
                assert all(len({d['owners'][mode][k] for k in b['members']})==1 for b in expected)
                np.testing.assert_allclose(sum(r['population'] for r in rows),d['checks']['population_preserved'],atol=.02,rtol=0)
                np.testing.assert_allclose(sum(r['supply'] for r in rows),d['checks']['supply_preserved'],atol=1e-7,rtol=0)
            for b in expected:
                assert abs(d['nodes'][b['representative']]['team']['1']-b['entry_market_population_equivalent'])<1e-5
            self.regions[region]=d

    def meta(self):
        checks={k:dict(v['checks'],ranked_local=sum(not r['pending'] for r in v['modes']['local']),
                       ranked_growth=sum(not r['pending'] for r in v['modes']['growth']),
                       pending_points=sum(r['pending'] for r in v['nodes'].values())) for k,v in self.regions.items()}
        ready=set(self.regions)==set(self.expected_regions)
        if ready:
            checks['전국']={k:sum(c[k] for c in checks.values()) for k in
                           ('base_bundles','points','local_bundles','growth_bundles','population_preserved',
                            'supply_preserved','ranked_local','ranked_growth','pending_points')}
        return {'regions':(['전국'] if ready else [])+list(self.regions),'parameters':PARAMS,
                'national_ready':ready,'expected_regions':self.expected_regions,'built_regions':list(self.regions),
                'checks':checks,
                'notes':next(iter(self.regions.values()))['notes'],
                'source_vintages':self.m.data['source_vintages']}

    def region(self, region, mode, compact=False):
        if mode not in ('local','growth'):raise ValueError('invalid_mode')
        if region=='전국':
            if not self.meta()['national_ready']:raise ValueError('national_build_incomplete')
            rows=[dict(r,region=p) for p,d in self.regions.items() for r in d['modes'][mode]]
            rows.sort(key=lambda r:(r['pending'],-(r['shock'] if mode=='local' else r['population']),r['id']))
            for rank,row in enumerate([r for r in rows if not r['pending']],1):row['rank']=rank
            result = {'release':{p:d['release'] for p,d in self.regions.items()},'rows':rows,
                    'nodes':{k:r for d in self.regions.values() for k,r in d['nodes'].items()},
                    'owners':{x:{k:g for d in self.regions.values() for k,g in d['owners'][x].items()} for x in ('local','growth')}}
        else:
            d=self.regions[region]
            result = {'release':d['release'],'rows':d['modes'][mode],'nodes':d['nodes'],'owners':d['owners']}
        if compact:
            # Search and case shortcuts need labels, not every site's scenario metrics.
            # Full member metrics are returned only for the selected group in detail().
            result['nodes']={k:{f:r[f] for f in ('id','label','life','pending')} for k,r in result['nodes'].items()}
            result.pop('owners')
        return result

    @lru_cache(maxsize=24)
    def detail(self, region, mode, key, node, n=1):
        if region=='전국':region=self.node_regions[node]
        d=self.regions[region];group=next((g for g in d['modes'][mode] if g['id']==key),None)
        if not group or node not in group['members']:raise ValueError('invalid_group_or_node')
        row=d['nodes'][node];m=self.m
        w=m.W.getrow(m.nfac+m.cix[node]);p=m.population[w.indices];s=m.supply[w.indices]
        parts=team_parts(p,s,w.data,n);fractions=n*w.data/(s+n*w.data)
        members={m.ix[k] for k in group['members']};destinations=set(w.indices.tolist())|members
        records=[];total=0.;union_total=0.
        for code in d['checks']['source_districts']:
            if not m.district_markets(code).intersection(destinations):continue
            cells,A=m.district(code);entry=np.asarray(A[:,w.indices]@fractions).ravel()
            union=np.asarray(A[:,sorted(members)].sum(axis=1)).ravel()
            total+=float(entry.sum());union_total+=float(union.sum())
            for j in np.flatnonzero((entry>0)|(union>0)):
                c=cells[j];records.append([float(c[0]),float(c[1]),float(c[2]),float(entry[j]),float(union[j])])
        assert abs(total-parts.sum())<.02 and abs(union_total-group['population'])<.02
        bins={}
        for lon,lat,pop,value,union in records:
            x,y=T.transform(lon,lat);b=bins.setdefault((int(x//200),int(y//200)),np.zeros(5))
            b+=np.array([lon*pop,lat*pop,pop,value,union])
        origins=[[a[0]/a[2],a[1]/a[2],*a[2:].tolist()] for a in bins.values() if a[2]>0]
        shares=np.asarray(m.W[:m.nfac,sorted(members)].sum(axis=1)).ravel()
        assert shares.max()<=1+1e-7
        dental=[];center=m.anchors[m.ix[node]]
        for j,f in enumerate(m.facilities):
            if shares[j]>1e-8 or np.linalg.norm(np.array(f['xy'])-center)<=3500:
                dental.append({k:v for k,v in f.items() if k!='xy'}|{'allocated':float(shares[j]*f['doctors'])})
        dental.sort(key=lambda f:-f['allocated'])
        metrics=[]
        local=np.isin(w.indices,sorted(members))
        previous=0.
        for team in (1,2,3):
            v=team_parts(p,s,w.data,team);value=float(v.sum())
            metrics.append({'team':team,'total':value,'per_doctor':value/team,'marginal':value-previous,
                            'inside':float(v[local].sum()),'outside':float(v[~local].sum())})
            previous=value
        return {'group':group,'node':row,'nodes':{k:d['nodes'][k] for k in group['members']},
                'local_group_count':len({d['owners']['local'][k] for k in group['members']}),
                'team_size':n,'team':metrics,'origins':origins,'dental':dental,
                'origin_error':total-float(parts.sum()),'union_error':union_total-group['population'],
                'boundaries':{'type':'FeatureCollection','features':[{'type':'Feature','geometry':m.geometry(m.ix[k]),
                'properties':{'id':k,'label':m.com[m.ix[k]]['current_label'],'selected':k==node,
                              'retained':bool(m.keep[m.ix[k]]),
                              'population':float(m.population[m.ix[k]])}} for k in group['members']]}}

if __name__=='__main__':
    parser=argparse.ArgumentParser();group=parser.add_mutually_exclusive_group()
    group.add_argument('--region',default='울산광역시');group.add_argument('--all',action='store_true')
    args=parser.parse_args()
    if args.all:build_national()
    else:build(args.region)
