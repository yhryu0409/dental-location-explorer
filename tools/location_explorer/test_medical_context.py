import json
import tempfile
import unittest
from pathlib import Path
from medical_context import MedicalContext, annual, month_value, outflow, mapping, OUT, read


class ValidationTests(unittest.TestCase):
    def test_conflicting_duplicates_are_never_added_or_chosen(self):
        a={'진료비(천원)':'207','진료건수(건)':'*'}
        b={'진료비(천원)':'103','진료건수(건)':'*'}
        result=month_value([a,b])
        self.assertIsNone(result['cost_krw'])
        self.assertEqual(result['cost_status'],'duplicate')

    def test_suppression_preserves_observed_cost_and_real_zero(self):
        r=month_value([{'진료비(천원)':'13','진료건수(건)':'*'}])
        self.assertEqual(r['cost_krw'],13000)
        self.assertIsNone(r['claims'])
        self.assertEqual(r['claims_status'],'suppressed')
        r=month_value([{'진료비(천원)':'0','진료건수(건)':'0'}])
        self.assertEqual((r['cost_krw'],r['claims']),(0,0))
        self.assertIsNone(month_value([])['cost_krw'])

    def test_annual_missing_month_is_not_a_smaller_total(self):
        months=[dict(period=f'2023-{m:02}',**month_value([{'진료비(천원)':'1','진료건수(건)':'2'}])) for m in range(1,13)]
        self.assertEqual(annual(months)[-1]['cost_krw'],12000)
        months[-1]['claims']=None;months[-1]['claims_status']='suppressed'
        r=annual(months)[-1]
        self.assertEqual(r['cost_krw'],12000)
        self.assertIsNone(r['claims'])
        self.assertIsNone(annual(months[:-1])[-1]['cost_krw'])

    def test_outflow_cost_ratio_and_zero_or_duplicate(self):
        a=[{'DT':'30','TBL_ID':'inside'}];b=[{'DT':'10','TBL_ID':'outside'}]
        self.assertEqual(outflow(a,b)['share'],.25)
        self.assertIsNone(outflow(a+a,b)['share'])
        self.assertIsNone(outflow([{'DT':'0','TBL_ID':'a'}],[{'DT':'0','TBL_ID':'b'}])['share'])

    def test_parent_metadata_disambiguates_junggu(self):
        meta=[{'ITM_ID':'P1','ITM_NM':'서울','OBJ_NM':'시군구별'},
              {'ITM_ID':'P2','ITM_NM':'부산','OBJ_NM':'시군구별'},
              {'ITM_ID':'C1','ITM_NM':'중구','UP_ITM_ID':'P1','OBJ_NM':'시군구별'},
              {'ITM_ID':'C2','ITM_NM':'중구','UP_ITM_ID':'P2','OBJ_NM':'시군구별'}]
        g=mapping(meta)
        self.assertEqual(set(g),{'C1','C2'})
        self.assertNotEqual(g['C1']['key'],g['C2']['key'])


@unittest.skipUnless((OUT / "latest.json").is_file(), "requires the captured medical release")
class CapturedReleaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.context=MedicalContext()
        p=read(OUT/'latest.json');cls.audit=read(OUT/p['release']/'audit.json')

    def test_actual_conflicts_are_accounted_for(self):
        self.assertEqual(self.audit['raw_duplicate_keys'],52)
        self.assertGreater(self.audit['normalized_duplicate_keys'],52)
        self.assertEqual(self.audit['specialty_code_only_collisions'],125)
        self.assertEqual(self.audit['korean_medicine_rows_held'],308)

    def test_exact_full_region_and_unmapped_address(self):
        a=self.context.lookup('서울특별시 중구 세종대로 1')
        b=self.context.lookup('부산광역시 중구 중앙대로 1')
        self.assertNotEqual(a['region_key'],b['region_key'])
        self.assertEqual(self.context.lookup('중구 중앙대로 1')['status'],'unmapped')
        self.assertEqual(self.context.lookup('부산광역시 중구가짜 중앙대로 1')['status'],'unmapped')

    def test_ulsan_baseline_and_korean_institution_cost(self):
        d=self.context.lookup('울산광역시 북구 명촌8길 8')
        self.assertEqual(d['score_effect'],'none')
        self.assertEqual(d['region']['outflow']['status'],'ok')
        for kind in ['치과의원','의원','한의원']:
            rows=d['region']['series'][kind]['annual']
            self.assertEqual(len(rows),5)
            self.assertTrue(all(r['cost_krw']>0 and r['claims']>0 for r in rows))
        self.assertNotIn('specialty',d)

    def test_bucheon_and_province_alias_collision_remain_unavailable(self):
        d=self.context.lookup('경기도 부천시 소사구 경인로 1')
        self.assertEqual(d['region']['mapping_status'],'boundary_review')
        self.assertTrue(all(r['cost_krw'] is None for r in d['region']['series']['치과의원']['monthly']))
        d=self.context.lookup('강원특별자치도 강릉시 경강로 1')
        self.assertTrue(any(r['cost_status']=='duplicate' for r in d['region']['series']['의원']['monthly']))

    def test_sejong_explicit_single_level_municipality(self):
        d=self.context.lookup('세종특별자치시 세종특별자치시 한누리대로 1')
        self.assertEqual(d['region_key'],'세종특별자치시|세종시')
        self.assertEqual(d['region']['outflow']['status'],'ok')
        self.assertTrue(all(r['cost_krw'] is not None for r in d['region']['series']['치과의원']['annual']))

    def test_release_tampering_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);p=read(OUT/'latest.json')
            (root/'latest.json').write_text(json.dumps(p));(root/p['release']).mkdir()
            (root/p['release']/'data.json').write_text('{}')
            with self.assertRaises(ValueError):MedicalContext(root)

if __name__=='__main__':unittest.main()
