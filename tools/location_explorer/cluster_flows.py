"""Hash-verified 2024 outpatient inflow evidence at the source municipality level.

These are all-care administrative-area statistics, never a clinic/cluster's
revenue, specialty-specific flow, or an origin-destination matrix. No files are
written and no location score is read or changed by this module.
"""
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
import json

from medical_context import SOURCE, boundary_hold, digest, mapping, read


TABLES = {
    'inside': ('TX_35003_A028', 'TX_35003_A057', 'TX_35003_A086', 'TX_35003_A115'),
    'outbound': ('TX_35003_A031', 'TX_35003_A060', 'TX_35003_A089', 'TX_35003_A118'),
    'inbound': ('DT_35003_A13', 'DT_35003_A21', 'DT_35003_A29', 'DT_35003_A38'),
}
PERIOD = '2024'
DEFINITIONS = {
    'period': PERIOD,
    'scope': '시군구 전체 외래 진료비, 약국 진료비·의료급여 포함; 비급여 전체 소비 아님',
    'inside': '거주지와 의료기관 소재지가 같은 시군구인 진료비',
    'inbound': '다른 시군구 주민이 이 시군구 의료기관에서 이용한 진료비',
    'outbound': '이 시군구 주민이 다른 시군구 의료기관에서 이용한 진료비',
    'inbound_share': '외부 주민 진료비 / (관내 주민 진료비 + 외부 주민 진료비)',
    'net_inflow': '외부 주민 진료비 - 해당 지역 주민의 관외 진료비',
    'comparison': ('같은 2024년·진료일 기준·보험 포괄범위·시도/시군구 명칭을 대조한 파생값. '
                   '거주지 기준 관내 진료비가 동일 경계의 기관 소재지 관내 진료비와 같다는 '
                   '회계적 관계를 사용하며, 별도 경계 재배분은 하지 않음'),
    'limits': ('의과·치과·한의 합산 지역 맥락. 특정 의료클러스터·기관·진료과에 금액을 '
               '배분하거나 합산하여 집객력 또는 환자수로 해석할 수 없음. '
               '원천은 지역 이동에 따른 인원 중복을 경고하므로 환자수 비율은 계산하지 않음'),
}


def cost_value(row):
    """Validate a monetary source cell, preserving zero and suppression."""
    if row.get('PRD_DE') != PERIOD or row.get('ITM_NM') != '진료비' or row.get('UNIT_NM') != '천원':
        return None, 'invalid_definition'
    value = row.get('DT')
    if value == '*':
        return None, 'suppressed'
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number < 0:
            raise InvalidOperation()
        won = number * 1000
        return int(won) if won == won.to_integral_value() else float(won), 'ok'
    except (InvalidOperation, ValueError, TypeError):
        return None, 'invalid_value'


def combine_region(province, district, sides):
    """A strict three-cell join; duplicates are never silently selected/summed."""
    records = [r for side in TABLES for r in sides.get(side, [])]
    result = {
        'province': province, 'district': district, 'period': PERIOD,
        'status': 'ok', 'inside_krw': None, 'inbound_krw': None,
        'outbound_krw': None, 'inbound_share': None, 'net_inflow_krw': None,
        'source_tables': sorted({r['TBL_ID'] for r in records if 'TBL_ID' in r}),
        'geography_basis': 'source_province_district_names_2024',
        'geography_status': 'same_hierarchical_names',
        'derived_share_basis': 'same_boundary_inside_plus_provider_outside',
        'cell_status': {},
    }
    if boundary_hold(district):
        result.update(status='boundary_review', geography_status='boundary_review')
        return result
    values = {}
    for side in TABLES:
        rows = sides.get(side, [])
        if len(rows) != 1:
            result['cell_status'][side] = 'missing' if not rows else 'duplicate'
        else:
            values[side], result['cell_status'][side] = cost_value(rows[0])
    problems = set(result['cell_status'].values()) - {'ok'}
    if problems:
        result['status'] = next(s for s in ('duplicate','missing','suppressed','invalid_definition','invalid_value') if s in problems)
        return result
    result.update({side+'_krw': value for side, value in values.items()})
    denominator = values['inside'] + values['inbound']
    if denominator <= 0:
        result['status'] = 'zero_denominator'
        return result
    result['inbound_share'] = values['inbound']/denominator
    result['net_inflow_krw'] = values['inbound']-values['outbound']
    return result


def verified_capture(source, manifest, event, used):
    """Check both immutable acquisition manifest and retrieval receipt."""
    relative = 'kosis/' + event['raw_path']
    path = (source / relative).resolve()
    if not path.is_relative_to(source.resolve()):
        raise ValueError('capture_path_outside_source')
    sha = digest(path)
    if manifest.get(relative) != sha or event.get('sha256') != sha:
        raise ValueError('capture_hash_mismatch: '+relative)
    used[relative] = sha
    return read(path)


def build_flows(source=SOURCE):
    """Return evidence and audit only; no publication, network or model mutation."""
    source = Path(source)
    manifest = {r['path']:r['sha256'] for r in read(source/'manifest.json')}
    used = {'manifest.json':digest(source/'manifest.json'),
            'kosis/retrievals.jsonl':digest(source/'kosis/retrievals.jsonl')}
    events = {}
    for line in (source/'kosis/retrievals.jsonl').read_text().splitlines():
        event = json.loads(line)
        if event.get('outcome') != 'retrieved':
            continue
        label = event['label']
        if label in events and events[label]['sha256'] != event['sha256']:
            raise ValueError('conflicting_capture_generation: '+label)
        events[label] = event
    grouped = defaultdict(lambda:defaultdict(list))
    geographies = {}; ignored=[]; join_cells=[]; table_audit=[]
    for side, tables in TABLES.items():
        for table in tables:
            labels=[table+'_2024', table+'_ITM', table+'_CMMT']
            if any(label not in events for label in labels):
                raise ValueError('missing_required_table_capture: '+table)
            data, meta, comments = [verified_capture(source, manifest, events[label], used) for label in labels]
            comment_text = ''.join(r.get('CMMT_DC','') for r in comments).replace(' ','')
            if not all(s in comment_text for s in ('진료일기준','의료급여포함','약국포함')):
                raise ValueError('incompatible_coverage_definition: '+table)
            if side=='inbound' and '요양기관소재지기준' not in comment_text:
                raise ValueError('provider_geography_not_confirmed: '+table)
            raw_geo = [r for r in meta if r.get('OBJ_NM')=='시군구별']
            if len({r['ITM_ID'] for r in raw_geo}) != len(raw_geo):
                raise ValueError('duplicate_geography_metadata: '+table)
            geo = mapping(meta)
            n = 0
            for row in data:
                if row.get('PRD_DE') != PERIOD or row.get('ITM_NM') != '진료비':
                    continue
                if row.get('TBL_ID') != table or '외래' not in row.get('TBL_NM',''):
                    raise ValueError('unexpected_table_scope: '+table)
                if (side=='inside') != ('관내' in row['TBL_NM']):
                    raise ValueError('unexpected_flow_direction: '+table)
                g = geo.get(row['C1'])
                if not g:
                    ignored.append({'table':table, 'code':row['C1'],'name':row.get('C1_NM'), 'reason':'subtotal_or_unmapped'})
                    continue
                if row.get('C1_NM') != g['district'] and not (g['district']=='세종시' and row.get('C1_NM')=='세종'):
                    raise ValueError('data_metadata_name_mismatch: '+table)
                key=g['key'];geographies[key]=g;grouped[key][side].append(row);n+=1
                join_cells.append({'region_key':key,'side':side,'table':table,'code':row['C1'],
                                   'period':row['PRD_DE'],'raw_value':row['DT'],'raw_unit':row['UNIT_NM']})
            table_audit.append({'table':table,'direction':side,'mapped_cost_cells':n,
                                'comments':comments})
    regions = {key:combine_region(g['province'], g['district'], grouped[key]) for key,g in sorted(geographies.items())}
    return {
        'regions':regions, 'source_hashes':used, 'definitions':DEFINITIONS,
        'audit':{
            'period':PERIOD, 'table_count':len(table_audit), 'region_count':len(regions),
            'status_counts':dict(Counter(r['status'] for r in regions.values())),
            'table_definitions':table_audit, 'excluded_rows':ignored, 'join_cells':join_cells,
            'join_key':'2024 + explicit parent province + exact district name',
            'boundary_policy':'Bucheon/Gunwi held; no historical boundary redistribution',
            'clinic_cluster_allocation':'not_performed',
        },
    }
