"""Medical evidence attached to immutable commercial/living-area membership."""
import hashlib
from collections import Counter,defaultdict
from pathlib import Path
import numpy as np
from scipy.spatial import cKDTree
from shapely.geometry import Point
from clusters import (ROOT,OUT,RADIUS,encode,read,sha,RegionMatcher,entry_summary,
                      MEDICAL_CODES,DENTAL_CODES,KOREAN_CODES,HOSPITAL_CODES,classification)
from cluster_flows import build_flows


def footprint_owners(sites, site_nodes, xy):
    """Use the SAME 25m, resolution=4 polygons as Model.geometry; stable overlap tie."""
    tree=cKDTree(sites);shape=Point(0,0).buffer(25,resolution=4)
    owners=[];ambiguous=0
    for point,near in zip(xy,tree.query_ball_point(xy,25)):
        options=[]
        for j in near:
            delta=point-sites[j]
            if shape.covers(Point(*delta)):
                for node in site_nodes[j]:options.append((float(delta@delta),node))
        keys={node for distance,node in options}
        ambiguous+=len(keys)>1
        owners.append(min(options)[1] if options else None)
    return owners,ambiguous


def counts(rows):
    c=Counter(r['code'] for r in rows);dent=[r for r in rows if r['code'] in DENTAL_CODES]
    missing=sum(r['doctors'] is None for r in dent)
    return {'facility_count':len(rows),'medical_count':sum(c[k] for k in MEDICAL_CODES),
            'dental_count':len(dent),'korean_count':sum(c[k] for k in KOREAN_CODES),
            'hospital_count':sum(c[k] for k in HOSPITAL_CODES),
            'dental_workforce':None if missing else sum(r['doctors'] for r in dent),
            'workforce_missing':missing}


def type_label(rows):
    if not rows:return '내부 관측기관 없음'
    return classification(Counter(r['code'] for r in rows),False)[0]


def build():
    from server import Model,DENSITY
    from strategy import Strategies,OUT as STRATEGY_OUT
    m=Model();strategies=Strategies(m);baseline=sha(m.release)
    fs=sorted(m.medical,key=lambda r:r['id']);xy=np.array([r['xy'] for r in fs]);tree=cKDTree(xy)
    nodes={};node_regions={};site_map=defaultdict(set)
    for province,d in sorted(strategies.regions.items()):
        for node,r in d['nodes'].items():
            nodes[node]={'node':node,'label':r['label'],'lat':r['lat'],'lon':r['lon'],'source_region':province,
                         'one':r['team']['1'],'three':r['team']['3'],'shock':r['shock'],'pending':r['pending'],'distance_m':0}
            node_regions[node]=province
            for site in m.groups[m.ix[node]]['sites']:site_map[int(site)].add(node)
    site_ids=sorted(site_map)
    owners,ambiguous=footprint_owners(m.sites[site_ids],[site_map[j] for j in site_ids],xy)
    inside_by_node=defaultdict(set)
    for i,node in enumerate(owners):
        if node:inside_by_node[node].add(i)
    near_by_node={node:set(tree.query_ball_point(m.anchors[m.ix[node]],RADIUS)) for node in nodes}
    flow=build_flows();matcher=RegionMatcher(flow['regions'])
    f_regions=[matcher.match(r['address']) for r in fs];totals=Counter(k for k in f_regions if k)
    rows=[];details={};node_map={mode:{} for mode in ('local','growth')};summary_modes={}
    for mode in ('local','growth'):
        mode_rows=[];inside_coverage=set();near_coverage=set()
        for province,d in sorted(strategies.regions.items()):
            for g in d['modes'][mode]:
                members=g['members'];cid=g['id']
                inside=set().union(*(inside_by_node[n] for n in members))
                surrounding=set().union(*(near_by_node[n] for n in members))-inside
                inside_coverage|=inside;near_coverage|=surrounding
                inside_rows=[fs[j] for j in sorted(inside)];near_rows=[fs[j] for j in sorted(surrounding)]
                linked=[dict(nodes[n],distance_m=float(np.linalg.norm(m.anchors[m.ix[n]]-m.anchors[m.ix[g['representative']]]))) for n in members]
                entry=entry_summary(linked)
                # Keep the original representative, even if another candidate has a higher score.
                entry.update(representative_node=g['representative'],representative_label=g['label'])
                region_keys={matcher.match(nodes[n]['label']) for n in members}|{f_regions[j] for j in inside}
                regional_counts=Counter(f_regions[j] for j in inside)
                contexts=[];concentration=[]
                for key in sorted(k for k in region_keys if k):
                    f=flow['regions'][key];name=f['province']+' '+f['district']
                    contexts.append(dict(f,key=key,name=name))
                    concentration.append({'key':key,'name':name,'cluster_facilities':regional_counts[key],
                                          'region_facilities':totals[key],'share':regional_counts[key]/totals[key] if totals[key] else None})
                if None in region_keys:
                    contexts.append({'key':None,'name':'과거 시군구 연결 확인 필요','status':'unmapped',
                                     'inbound_share':None,'inbound_krw':None,'outbound_krw':None,'net_inflow_krw':None,'source_tables':[]})
                children=sorted({d['owners']['local'][n] for n in members}) if mode=='growth' else []
                parent=d['owners']['growth'][members[0]] if mode=='local' else None
                row={'id':cid,'mode':mode,'label':g['label'],'province':province,'provinces':[province],
                     'members':members,'representative':g['representative'],'lat':g['lat'],'lon':g['lon'],
                     'type':type_label(inside_rows),'surrounding_type':type_label(near_rows),
                     'scale':('정착형 생활권' if mode=='local' else '성장형 생활권'),
                     **counts(inside_rows),'surrounding':counts(near_rows),
                     'candidate_count':len(members),'entry':entry,'regions':contexts,'regional_concentration':concentration,
                     'local_groups':children,'local_labels':{x['id']:x['label'] for x in d['modes']['local'] if x['id'] in children},'growth_group':parent,'pending':g['pending'],
                     'existing_population':g['population'],'existing_supply':g['supply'],
                     'existing_rank':g.get('rank'),'size_percentile':None,
                     'search_text':' '.join(sorted({nodes[n]['label'] for n in members}|{r['name'] for r in inside_rows+near_rows}))}
                mode_rows.append(row)
                details[cid]={'facility_ids':[fs[j]['id'] for j in sorted(inside)],
                              'surrounding_ids':[fs[j]['id'] for j in sorted(surrounding)],'candidate_ids':members}
                for n in members:
                    assert n not in node_map[mode];node_map[mode][n]=cid
        sizes=sorted(r['facility_count'] for r in mode_rows)
        for r in mode_rows:r['size_percentile']=float(np.searchsorted(sizes,r['facility_count'],side='right')/len(sizes)*100) if r['facility_count'] else None
        assert set(node_map[mode])==set(nodes)
        assert sum(r['facility_count'] for r in mode_rows)==len(inside_coverage)
        summary_modes[mode]={'clusters':len(mode_rows),'facilities':len(fs),'clustered_facilities':len(inside_coverage),
                             'unclustered_facilities':len(fs)-len(inside_coverage),'surrounding_facilities':len(near_coverage),
                             'candidates_linked':len(nodes),'candidates_unlinked':0,
                             'flow_regions_available':sum(f['status']=='ok' for f in flow['regions'].values())}
        rows+=mode_rows
    assert sha(m.release)==baseline
    pins={'baseline_release_sha256':baseline,'baseline_source_pins':m.data['pins'],
          'strategy_pointer_sha256':sha(STRATEGY_OUT/'latest.json'),'strategy_releases':read(STRATEGY_OUT/'latest.json')['regions'],
          'code_sha256':sha(Path(__file__)),'adapter_sha256':sha(Path(__file__).with_name('clusters.py')),
          'anchors_sources':{str(p.relative_to(ROOT)):sha(p) for p in (DENSITY/'allocation.json',DENSITY/'sites.npz')},
          'kosis_sources':flow['source_hashes']}
    definitions={
        'basis':'기존 정착형·성장형 생활권의 ID, 구성 상권, 대표 후보, 계층을 그대로 재사용. 의료기관 수나 거리로 생활권을 새로 묶지 않음.',
        'inside':'기존 상권 표시 영역(점포 위치별 25m, 16각형 버퍼의 합집합) 안에 위치한 의료기관. 실제 공인 상권 경계가 아니며 누락·좌표 오차에 민감. 겹치는 표시 영역은 가장 가까운 점포, 동률이면 상권 ID로 단일 연결.',
        'surrounding':'구성 상권의 기존 개원 거점 각각에서 750m 내 의료기관 합집합 중 해당 생활권 내부 기관을 제외. 보조 관측 범위이며 생활권 경계를 결정하지 않음. 다른 생활권과 기관을 공유하므로 합산 금지.',
        'types':'내부 관측기관의 종별 구성으로 분류. 주변 의료기관 구성은 별도로 표시. 특화 진료과·브랜드·실제 집객 성과 판단 아님.',
        'entry':'구성 후보 및 기존 치과 1명·3명 팀 몫·경쟁충격 값 유지. 보류 제외 중앙값은 요약이며 실제 환자수·매출 아님.',
        'regional_concentration':'내부 관측기관 / 같은 시군구 전체 8종 기관. 의료 이용 점유율 아님.',
        'flows':flow['definitions'],'mapping':'구성 후보 주소 및 내부 기관의 과거 시군구를 정확 매칭. 주변 기관 지역을 생활권 유입으로 가져오지 않음.',
        'scope':'8종 의료기관; 요양·정신·보건기관 제외. 치과 인력은 치과병원·치과의원 신고 치과의사만 집계.',
        'vintages':{'healthcare':m.data['source_vintages']['healthcare'],'population':m.data['source_vintages']['population'],'flow':'2024'},
        'baseline_unchanged':True}
    # Migrate the previously published radial release's bookmarks through its representative candidate.
    legacy_path=OUT/'270d4a6b70e52aa8a22a'/'data.json'
    legacy_ids={mode:{} for mode in ('local','growth')}
    if legacy_path.exists():
        if not sha(legacy_path).startswith(legacy_path.parent.name):raise ValueError('legacy_release_hash_mismatch')
        pins['legacy_radial_release_sha256']=sha(legacy_path)
        for old in read(legacy_path)['rows']:
            node=old['entry'].get('representative_node')
            for mode in legacy_ids:
                if node in node_map[mode]:legacy_ids[mode][old['id']]=node_map[mode][node]
    payload={'version':'commercial-living-area-medical-v3','rows':rows,'details':details,'node_map':node_map,
             'legacy_ids':legacy_ids,'facilities':{r['id']:{k:r[k] for k in ('id','name','code','lat','lon')} for r in fs},'candidates':nodes,
             'summary':summary_modes['local'],'summaries':summary_modes,'definitions':definitions,'pins':pins}
    raw=encode(payload);release=hashlib.sha256(raw).hexdigest()[:20];directory=OUT/release;directory.mkdir(parents=True,exist_ok=True)
    path=directory/'data.json'
    if path.exists() and path.read_bytes()!=raw:raise ValueError('immutable_release_conflict')
    path.write_bytes(raw)
    audit={'release':release,'summaries':summary_modes,'baseline_unchanged':True,'same_strategy_members':True,
           'facility_primary_node':{fs[i]['id']:n for i,n in enumerate(owners) if n},'ambiguous_footprint_facilities':ambiguous,
           'flow_audit':flow['audit'],'pins':pins}
    ap=directory/'audit.json'
    if ap.exists() and ap.read_bytes()!=encode(audit):raise ValueError('immutable_audit_conflict')
    ap.write_bytes(encode(audit));temp=OUT/'latest.tmp'
    temp.write_bytes(encode({'release':release,'sha256':sha(path),'audit_sha256':sha(ap)}));temp.replace(OUT/'latest.json')
    print({'release':release,'summaries':summary_modes,'ambiguous_footprints':ambiguous},flush=True)
