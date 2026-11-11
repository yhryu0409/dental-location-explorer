import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from cluster_flows import build_flows, combine_region, cost_value, verified_capture
from medical_context import SOURCE, mapping


def cell(value, table='table'):
    return {'DT':str(value),'TBL_ID':table,'PRD_DE':'2024','ITM_NM':'진료비','UNIT_NM':'천원'}


class FlowRulesTest(unittest.TestCase):
    def sides(self):
        return {'inside':[cell(30,'inside')],'inbound':[cell(10,'inbound')],'outbound':[cell(15,'outbound')]}

    def test_inbound_denominator_is_inside_plus_inbound(self):
        r=combine_region('서울특별시','강남구',self.sides())
        self.assertEqual(r['inbound_share'],.25)
        self.assertEqual(r['net_inflow_krw'],-5000)
        self.assertEqual(r['inbound_krw'],10000)

    def test_duplicate_is_not_summed_even_when_values_identical(self):
        s=self.sides();s['inbound']*=2
        r=combine_region('서울특별시','강남구',s)
        self.assertEqual(r['status'],'duplicate')
        self.assertIsNone(r['inbound_krw'])
        self.assertIsNone(r['inbound_share'])

    def test_missing_and_suppression_fail_closed(self):
        for value,status in [([], 'missing'), ([cell('*')], 'suppressed')]:
            s=self.sides();s['outbound']=value
            r=combine_region('서울특별시','강남구',s)
            self.assertEqual(r['status'],status)
            self.assertIsNone(r['net_inflow_krw'])
            self.assertIsNone(r['inbound_share'])

    def test_non_cost_wrong_period_unit_and_invalid_values_rejected(self):
        for key,value in [('ITM_NM','진료실인원수'),('PRD_DE','2023'),('UNIT_NM','원')]:
            r=cell(10);r[key]=value
            self.assertEqual(cost_value(r),(None,'invalid_definition'))
        for value in ['nan','Infinity','-1','']:
            self.assertEqual(cost_value(cell(value)),(None,'invalid_value'))

    def test_real_zero_is_preserved_but_zero_denominator_not_a_rate(self):
        s={k:[cell(0)] for k in self.sides()}
        r=combine_region('서울특별시','강남구',s)
        self.assertEqual(r['inside_krw'],0)
        self.assertEqual(r['status'],'zero_denominator')
        self.assertIsNone(r['inbound_share'])

    def test_boundary_hold_and_parent_geography(self):
        for district in ['부천시 원미구','군위군']:
            self.assertEqual(combine_region('경기도',district,self.sides())['status'],'boundary_review')
        meta=[{'OBJ_NM':'시군구별','ITM_ID':'s','ITM_NM':'서울'},
              {'OBJ_NM':'시군구별','ITM_ID':'b','ITM_NM':'부산'},
              {'OBJ_NM':'시군구별','ITM_ID':'sc','ITM_NM':'중구','UP_ITM_ID':'s'},
              {'OBJ_NM':'시군구별','ITM_ID':'bc','ITM_NM':'중구','UP_ITM_ID':'b'}]
        m=mapping(meta)
        self.assertEqual(set(m),{'sc','bc'})
        self.assertNotEqual(m['sc']['key'],m['bc']['key'])

    def test_capture_hash_checks_both_manifest_and_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'kosis/raw').mkdir(parents=True)
            p=root/'kosis/raw/example.json';p.write_text('[]')
            sha=hashlib.sha256(p.read_bytes()).hexdigest()
            event={'raw_path':'raw/example.json','sha256':sha}
            used={};self.assertEqual(verified_capture(root,{'kosis/raw/example.json':sha},event,used),[])
            with self.assertRaises(ValueError):verified_capture(root,{'kosis/raw/example.json':'bad'},event,{})
            p.write_text('{}')
            with self.assertRaises(ValueError):verified_capture(root,{'kosis/raw/example.json':sha},event,{})


@unittest.skipUnless((SOURCE / "manifest.json").is_file(), "requires the archived KOSIS tables")
class ActualCapturesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.flows=build_flows()

    def test_all_twelve_tables_and_held_geographies(self):
        self.assertEqual(self.flows['audit']['table_count'],12)
        self.assertGreater(len(self.flows['regions']),200)
        self.assertEqual(self.flows['regions']['경기도|부천시 소사구']['status'],'boundary_review')

    def test_gangnam_and_provincial_hubs_have_source_bound_evidence(self):
        for key in ['서울특별시|강남구','부산광역시|부산진구','대구광역시|중구','울산광역시|북구','세종특별자치시|세종시']:
            r=self.flows['regions'][key]
            self.assertEqual(r['status'],'ok',key)
            self.assertGreater(r['inbound_krw'],0)
            self.assertEqual(len(r['source_tables']),3)
            self.assertAlmostEqual(r['inbound_share'],r['inbound_krw']/(r['inside_krw']+r['inbound_krw']))
        self.assertTrue(all(0<=r['inbound_share']<=1 for r in self.flows['regions'].values() if r['status']=='ok'))

    def test_coverage_and_scope_are_explicit(self):
        text=self.flows['definitions']['scope']
        self.assertIn('의료급여',text);self.assertIn('약국',text)
        self.assertEqual(self.flows['audit']['clinic_cluster_allocation'],'not_performed')
        self.assertGreater(len(self.flows['source_hashes']),20)


if __name__=='__main__':unittest.main()
