"""Read-only destination-score inputs over the unchanged commercial release.

Weights are user scenarios, not fitted success/revenue coefficients. Exact
municipality matching is candidate-specific, including cross-boundary groups.
"""
import hashlib
import math
from bisect import bisect_left,bisect_right
from collections import Counter,defaultdict
from pathlib import Path
from clusters import RegionMatcher,encode,sha
from cluster_flows import build_flows

DEFAULTS={'w':.5,'weights':{'dental':45,'medical':15,'local':30,'flow':10}}

def reference_values(groups,candidates):
    eligible=[g for g in groups.values() if not g['pending']]
    if not eligible:raise ValueError('no_growth_reference')
    raw=sorted(c['one'] for c in candidates.values() if not c['pending'])
    if not raw or not all(math.isfinite(x) and x>=0 for x in raw):raise ValueError('invalid_base_reference')
    dmax=max(g['dental_count'] for g in eligible);tmax=max(g['facility_count'] for g in eligible)
    regional=defaultdict(list)
    for g in eligible:regional[g['province']].append(g['dental_count'])
    for xs in regional.values():xs.sort()
    refs={'growth_count':len(eligible),'candidate_count':len(raw),'dental_max':dmax,'medical_max':tmax}
    factors={}
    for gid,g in groups.items():
        d,t=g['dental_count'],g['facility_count'];xs=regional.get(g['province'],[])
        factors[gid]={
            'dental':math.log1p(d)/math.log1p(dmax) if dmax and not g['pending'] else None,
            'medical':math.log1p(t)/math.log1p(tmax) if tmax and not g['pending'] else None,
            'local':(bisect_right(xs,d)/len(xs) if d else 0) if xs and not g['pending'] else None,
            'regional_reference_count':len(xs),'regional_at_or_below':bisect_right(xs,d) if xs else 0}
        # All-zero references are real observations, not missing cells.
        if not g['pending']:
            if dmax==0:factors[gid]['dental']=0.
            if tmax==0:factors[gid]['medical']=0.
    base={node:{'base_percentile':100*bisect_right(raw,c['one'])/len(raw),
                'base_rank':len(raw)-bisect_right(raw,c['one'])+1} if not c['pending'] else
               {'base_percentile':None,'base_rank':None} for node,c in candidates.items()}
    return refs,factors,base


def build_inputs(store,flow=None):
    if store.data.get('version')!='commercial-living-area-medical-v3':raise ValueError('living_area_release_required')
    flow=build_flows() if flow is None else flow
    if flow['source_hashes']!=store.data['pins']['kosis_sources']:raise ValueError('destination_flow_generation_mismatch')
    groups={r['id']:r for r in store.data['rows'] if r['mode']=='growth'}
    candidates=store.data['candidates'];refs,factors,base=reference_values(groups,candidates)
    matcher=RegionMatcher(flow['regions']);rows=[];statuses=Counter()
    for node,c in sorted(candidates.items()):
        gid=store.data['node_map']['growth'][node];g=groups[gid]
        if node not in g['members']:raise ValueError('destination_membership_mismatch')
        key=matcher.match(c['label']);f=flow['regions'].get(key)
        status=f['status'] if f else 'unmapped';statuses[status]+=1
        rows.append({k:c[k] for k in ('node','label','source_region','one','three','shock','pending','lat','lon')}|
                    base[node]|{'growth_id':gid,'local_id':store.data['node_map']['local'][node],
                                'region_key':key,'flow_status':status})
    group_rows={gid:{k:g[k] for k in ('id','label','province','dental_count','facility_count','pending','candidate_count')}|
                {'factors':factors[gid]} for gid,g in groups.items()}
    regions={key:{k:f[k] for k in ('province','district','period','status','inside_krw','inbound_krw',
                                  'outbound_krw','inbound_share','net_inflow_krw','source_tables')}
             for key,f in flow['regions'].items()}
    meta={'defaults':DEFAULTS,'reference':refs,'flow_status_counts':dict(statuses),
          'candidate_total':len(rows),'pending_total':sum(r['pending'] for r in rows),
          'vintages':store.data['definitions']['vintages'],
          'method':{
              'base':'B = 100 × 보류 제외 전국 후보 중 원점수 이하인 후보 수 / 후보 수. 원점수는 기존 신규 치과의사 1명 몫. 개별 후보 단위이므로 기존 생활권 대표 목록의 순위와 구분.',
              'dental':'Ad = ln(1 + 성장형 내부 치과기관 수) / ln(1 + 전국 비교 대상 최댓값)',
              'medical':'At = ln(1 + 성장형 내부 전체 8종 의료기관 수) / ln(1 + 전국 비교 대상 최댓값)',
              'local':'L = 같은 시도에서 치과기관 수 이하인 성장형 생활권 수 / 비교 대상 수. 치과기관 0곳이면 L=0.',
              'flow':'F = 후보 주소를 정확 매칭한 시군구의 외부 주민 외래 진료비 / (관내 + 외부 주민 외래 진료비). 약국·의료급여 포함.',
              'destination':'H = 100 × Σ 정규화된 요소 가중치 × 요소값. 기본 가중치는 45/15/30/10. 학습된 계수가 아닌 평가 가정.',
              'composite':'S = (1 − w) × B + w × H. 순점수 변화 = S − B = w × (H − B). 0~100 평가 지수이며 환자수·매출 아님.',
              'scope':'내부 기관은 기존 점포 위치별 25m 표시 영역 기준. 주변 750m 기관은 점수에 넣지 않음. 상권·생활권 경계와 기존 점수를 변경하지 않음.',
              'missing':'양의 가중치를 준 요소가 미확인이면 H 보류. w=0이면 S=B로 모든 기존 평가 가능 후보를 유지. 해당 요소 가중치가 0이면 그 요소는 미사용.',
              'rank':'동점은 공동순위(1,1,3). 지역·검색 필터를 적용하기 전에 전국 순위 계산. 순위변화는 현재 종합점수 평가 가능 후보군 안에서 원점수 순위와 비교.',
              'limits':'브랜드·비급여 전문성·실제 목적 방문은 미측정. 시군구 유입을 생활권·기관별 금액이나 치과 환자수로 환산하지 않음.'}}
    pins={'cluster_release':store.release,'baseline_release_sha256':store.data['pins']['baseline_release_sha256'],
          'strategy_pointer_sha256':store.data['pins']['strategy_pointer_sha256'],
          'code_sha256':sha(Path(__file__)),'calculator_sha256':sha(Path(__file__).with_name('destination-math.js')),
          'flow_source_hashes':flow['source_hashes']}
    release=hashlib.sha256(encode(pins)).hexdigest()[:20]
    return {'release':release,'meta':meta,'rows':rows,'groups':group_rows,'regions':regions,'pins':pins}
