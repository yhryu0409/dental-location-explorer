"""Independent, hash-pinned historical context. Never feeds location scores.

Build explicitly with --build. HTTP reads only the published context release.
Geography uses full source province/district names and KOSIS parent metadata;
no fuzzy matching, population allocation, or inferred patient OD.
"""
import argparse
import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from pathlib import Path

from data_paths import ROOT
SOURCE = ROOT / 'research/medical_utilization_acquisition_20261027'
OUT = ROOT / 'research/medical_utilization_context'
VERSION = 'medical-context-v1'
PROVINCES = dict(zip(
    ['서울','부산','대구','인천','광주','대전','울산','세종','경기','강원','충북','충남','전북','전남','경북','경남','제주'],
    ['서울특별시','부산광역시','대구광역시','인천광역시','광주광역시','대전광역시','울산광역시','세종특별자치시','경기도','강원도','충청북도','충청남도','전라북도','전라남도','경상북도','경상남도','제주특별자치도']))
ALIASES = {'강원특별자치도':'강원도', '전북특별자치도':'전라북도'}
KINDS = ['치과의원','치과병원','의원','한의원','한방병원','병원','종합병원','상급종합병원','요양병원','정신병원','보건소','보건지소','보건진료소','보건의료원(병원화보건소)','조산소','약국']
REASONS = {
    'duplicate': '같은 연월·지역·기관종별에 값이 다른 행이 있어 보류',
    'boundary_review': '과거 지역 구분·경계 연결 확인 필요',
    'missing': '원본에 해당 월 자료 없음 (0으로 간주하지 않음)',
    'suppressed': '소수 건수 비공개',
    'invalid': '원본 값 형식 확인 필요',
}

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def read(path):
    return json.loads(path.read_text())

def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',',':'), allow_nan=False).encode()

def province(name):
    return ALIASES.get(name, PROVINCES.get(name, name))

def region_key(p, city):
    return province(p) + '|' + city

def boundary_hold(city):
    # Source has mixed old/new Bucheon subdivisions and Gunwi province changes.
    # Do not map these onto the present candidate from name equality alone.
    return city.startswith('부천시') or city == '군위군'

def number(value):
    if value == '*':
        return None, 'suppressed'
    try:
        result = float(value)
        if not math.isfinite(result) or result < 0:
            raise ValueError()
        return int(result) if result.is_integer() else result, 'ok'
    except (TypeError, ValueError):
        return None, 'invalid'

def month_value(rows, held=False):
    status = 'boundary_review' if held else 'missing' if not rows else 'duplicate' if len(rows) != 1 else None
    if status:
        return {'cost_krw':None,'claims':None,'cost_status':status,'claims_status':status}
    cost, cs = number(rows[0]['진료비(천원)'])
    count, ns = number(rows[0]['진료건수(건)'])
    return {'cost_krw':None if cost is None else cost*1000, 'claims':count,
            'cost_status':cs,'claims_status':ns}

def annual(months):
    """No partial annual totals; monetary and count completeness are separate."""
    result = []
    for year in range(2019,2024):
        rows = [r for r in months if r['period'].startswith(str(year))]
        item = {'year':year}
        for field, status in [('cost_krw','cost_status'),('claims','claims_status')]:
            valid = len(rows)==12 and all(r[status]=='ok' for r in rows)
            item[field] = sum(r[field] for r in rows) if valid else None
            item[status] = 'ok' if valid else 'incomplete'
            item[field+'_months'] = sum(r[status]=='ok' for r in rows)
        result.append(item)
    return result

def mapping(meta):
    entries = {r['ITM_ID']:r for r in meta if r.get('OBJ_NM')=='시군구별'}
    mapped = {}
    for code, r in entries.items():
        parent = entries.get(r.get('UP_ITM_ID'))
        if not parent:
            # Explicit single-level municipality: KOSIS '세종', NHIS '세종시'.
            # Other province totals are never treated as municipal observations.
            if r['ITM_NM']=='세종':
                mapped[code]={'key':'세종특별자치시|세종시','province':'세종특별자치시',
                              'district':'세종시','table_region_code':code}
            continue
        if parent.get('UP_ITM_ID'):
            continue
        p = province(parent['ITM_NM'])
        if p not in PROVINCES.values():
            continue
        mapped[code] = {'key':region_key(p,r['ITM_NM']), 'province':p,
                        'district':r['ITM_NM'],'table_region_code':code}
    return mapped

def outflow(inside, outside):
    if len(inside)!=1 or len(outside)!=1:
        return {'status':'incomplete','share':None}
    a, sa = number(inside[0]['DT']); b, sb = number(outside[0]['DT'])
    if sa!='ok' or sb!='ok':
        return {'status':'invalid','share':None}
    return {'status':'ok' if a+b>0 else 'zero_denominator',
            'inside_krw':a*1000,'outside_krw':b*1000,
            'share':b/(a+b) if a+b>0 else None,
            'tables':[inside[0]['TBL_ID'],outside[0]['TBL_ID']]}

def build():
    pinned = {r['path']:r['sha256'] for r in read(SOURCE/'manifest.json')}
    used = {}
    def source(relative):
        path = SOURCE/relative
        actual = digest(path)
        if pinned.get(relative)!=actual:
            raise ValueError('source_hash_mismatch: '+relative)
        used[relative] = actual
        return path
    def csv_rows(relative):
        with source(relative).open(encoding='cp949',newline='') as f:
            return list(csv.DictReader(f))
    costs = csv_rows('raw/nhis_15141213_monthly_cost_2019_2023.csv')
    specialty = csv_rows('raw/nhis_15129861_specialty_2022_2023.csv')
    logs = [json.loads(s) for s in (SOURCE/'kosis/retrievals.jsonl').read_text().splitlines()]
    used['kosis/retrievals.jsonl'] = digest(SOURCE/'kosis/retrievals.jsonl')
    events = {e['label']:e for e in logs if e.get('outcome')=='retrieved'}
    def captured(e):
        path = source('kosis/'+e['raw_path'])
        if digest(path)!=e['sha256']:
            raise ValueError('retrieval_hash_mismatch')
        return read(path)
    regions = {}; flows = defaultdict(lambda:defaultdict(list)); geo_audit=[]
    for e in events.values():
        p = e.get('public_params',{})
        if p.get('method')!='getList' or not p.get('tblId','').startswith('TX_'):
            continue
        table = p['tblId']
        meta_event = events[table+'_ITM']
        geo = mapping(captured(meta_event))
        captured(events[table+'_CMMT'])
        for r in captured(e):
            if r['PRD_DE']!='2024' or r['ITM_NM']!='진료비':
                continue
            if r.get('UNIT_NM')!='천원':
                raise ValueError('unexpected_kosis_cost_unit')
            g = geo.get(r['C1'])
            if not g:
                geo_audit.append({'table':table,'code':r['C1'],'name':r['C1_NM'],'status':'subtotal_or_unmapped'})
                continue
            key = g['key']
            regions.setdefault(key, {'province':g['province'],'district':g['district'],
                                    'geography_basis':'patient_residence','geography_reference':'KOSIS 2024 table names'})
            side = 'outside' if '관외' in r['TBL_NM'] else 'inside'
            flows[key][side].append(r)
            geo_audit.append(dict(g,table=table,status='exact_hierarchy_name'))
    grouped = defaultdict(list); unmapped = Counter(); duplicate_rows=[]
    for n,r in enumerate(costs,2):
        key = region_key(r['시도'],r['시군구'])
        if r['요양기관종별'] not in KINDS:
            raise ValueError('unknown_institution_kind')
        if key not in regions:
            unmapped[(r['시도'],r['시군구'])]+=1
            continue
        period = r['진료년도']+'-'+r['진료월'].zfill(2)
        grouped[(key,r['요양기관종별'],period)].append(dict(r,source_line=n))
    for key, rows in grouped.items():
        if len(rows)>1:
            duplicate_rows.append({'key':list(key),'rows':rows})
    for key,g in regions.items():
        held = boundary_hold(g['district'])
        g['mapping_status'] = 'boundary_review' if held else 'exact_full_name'
        g['series'] = {}
        for kind in KINDS:
            months=[]
            for year in range(2019,2024):
                for month in range(1,13):
                    period=f'{year}-{month:02}'
                    months.append(dict(period=period,**month_value(grouped.get((key,kind,period),[]),held)))
            g['series'][kind]={'monthly':months,'annual':annual(months)}
        g['outflow'] = {'status':'boundary_review','share':None} if held else outflow(flows[key]['inside'],flows[key]['outside'])
    # Classify the mixed code namespace, but do not publish specialty counts.
    pairs = Counter((r['진료과목코드'],r['진료과목명']) for r in specialty)
    pair_report=[]
    for (code,name),count in sorted(pairs.items()):
        status = 'korean_medicine_metadata_conflict' if ('한방' in name or name=='침구과') else 'not_published_pending_codebook'
        pair_report.append({'raw_code':code,'raw_name':name,'rows':count,'status':status})
    codekeys = Counter((r['진료년도'],r['진료월'],r['시도'],r['시군구'],r['진료과목코드']) for r in specialty)
    rawkeys=Counter((r['진료년도'],r['진료월'],r['시도'],r['시군구'],r['요양기관종별']) for r in costs)
    report = {'version':VERSION,'transform_sha256':digest(Path(__file__)), 'source_hashes':used,'cost_rows':len(costs),
              'specialty_code_only_collisions':sum(n>1 for n in codekeys.values()),
              'korean_medicine_rows_held':sum(r['rows'] for r in pair_report if r['status']=='korean_medicine_metadata_conflict'),
              'raw_duplicate_keys':sum(n>1 for n in rawkeys.values()),
              'normalized_duplicate_keys':len(duplicate_rows),
              'duplicate_rows':duplicate_rows,
              'unmapped_cost_regions':[{'province':p,'district':c,'rows':n} for (p,c),n in sorted(unmapped.items())],
              'specialty_classification':pair_report,'geography_mapping':geo_audit,
              'notes':['Name-only rename aliases are explicit; no spatial redistribution.',
                       'Parent city records are not assumed to be subtotals or allocated to child districts.',
                       'Missing and suppressed counts never become zero. Annual totals require 12 valid months.',
                       'Specialty dataset is audited but deliberately not served in this release.']}
    data={'version':VERSION,'transform_sha256':digest(Path(__file__)),'regions':regions,'kinds':KINDS,'reasons':REASONS,
          'periods':{'cost':'2019–2023','outflow':'2024'},
          'sources':{'cost':'https://www.data.go.kr/data/15141213/fileData.do',
                     'outflow':'https://kosis.kr/statHtml/statHtml.do?orgId=350&tblId=TX_35003_A115'},
          'source_hashes':used}
    body=encode(data); release_id=hashlib.sha256(body).hexdigest()[:20]
    target=OUT/release_id;target.mkdir(parents=True,exist_ok=True)
    for filename,value in [('data.json',body),('audit.json',encode(report))]:
        path=target/filename
        if path.exists() and path.read_bytes()!=value:
            raise ValueError('immutable_release_conflict')
        path.write_bytes(value)
    pointer={'release':release_id,'sha256':digest(target/'data.json'),'audit_sha256':digest(target/'audit.json')}
    temp=OUT/'latest.tmp';temp.write_bytes(encode(pointer));temp.replace(OUT/'latest.json')
    print(json.dumps({'release':release_id,'regions':len(regions),
        'cost_regions':sum(any(r['cost_krw'] is not None for s in g['series'].values() for r in s['annual']) for g in regions.values()),
        'outflow_regions':sum(g['outflow']['status']=='ok' for g in regions.values()),
        'duplicates':len(duplicate_rows)},ensure_ascii=False))

class MedicalContext:
    def __init__(self, root=OUT):
        pointer=read(root/'latest.json'); release=pointer['release']
        if len(release)!=20 or any(c not in '0123456789abcdef' for c in release):
            raise ValueError('invalid_context_release')
        path=root/release/'data.json'
        if digest(path)!=pointer['sha256']:
            raise ValueError('medical_context_hash_mismatch')
        self.data=read(path);self.release=release
        self.prefixes=[]
        for key,g in self.data['regions'].items():
            for p in [g['province']]+[a for a,b in ALIASES.items() if b==g['province']]:
                self.prefixes.append((p+' '+g['district'],key))
            if key=='세종특별자치시|세종시':
                self.prefixes.append(('세종특별자치시',key))

    def lookup(self,label):
        matches=[(p,k) for p,k in self.prefixes if label==p or label.startswith(p+' ')]
        if not matches:
            return {'status':'unmapped','reason':'후보 주소와 과거 시군구의 정확한 연결을 확인하지 못했습니다.','release':self.release}
        longest=max(len(p) for p,k in matches)
        keys={k for p,k in matches if len(p)==longest}
        if len(keys)!=1:
            return {'status':'unmapped','reason':'지역 연결이 여러 개여서 보류했습니다.','release':self.release}
        key=keys.pop();g=self.data['regions'][key]
        return {'status':'ok','release':self.release,'region_key':key, 'region':g,
                'kinds':self.data['kinds'],'reasons':REASONS,'sources':self.data['sources'],
                'mapping_method':'후보 전체 주소의 시도·시군구 명칭과 원본 지역명 정확 일치',
                'score_effect':'none'}

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--build',action='store_true')
    if parser.parse_args().build:build()
