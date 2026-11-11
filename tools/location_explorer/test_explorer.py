"""Small read-only integration audit against the pinned national release."""
import unittest
from data_paths import ROOT
from server import Model, sha

@unittest.skipUnless((ROOT / "research/refresh_20261013/latest.json").is_file(), "requires the national release")
class ExplorerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = Model()
        cls.original_sha = sha(cls.m.release)

    def test_all_candidates_preserve_score_and_shock_direction(self):
        m = self.m
        self.assertEqual(len(m.rows),len(m.data['bundles']))
        self.assertEqual(len(m.byid),len(m.rows))
        for r in m.rows:
            self.assertGreaterEqual(r['shock_score'],0)
            self.assertLessEqual(r['shock_score'],r['entry_market_population_equivalent']+1e-8)
        self.assertEqual(sum(not r['pending'] for r in m.rows),m.meta()['ranked'])

    def test_cross_region_origin_attribution(self):
        m = self.m
        chosen = [m.baseline,next(r for r in m.rows if '점촌3길 10-4' in r['label'])]
        for province in ['서울특별시','경기도','경상남도']:
            chosen.append(max((r for r in m.rows if r['province']==province and not r['pending']),key=lambda r:r['entry_market_population_equivalent']))
        for r in chosen:
            d=m.detail(r['id'])
            self.assertAlmostEqual(sum(c[3] for c in d['origins']),r['entry_market_population_equivalent'],places=5)
            self.assertTrue(all(0<=c[3]<=c[2]+1e-6 for c in d['origins']))
            self.assertAlmostEqual(sum(c['supply_to_representative'] for c in d['dental']),r['representative_existing_dentist_supply'],places=5)
            self.assertAlmostEqual(sum(x['contribution'] for x in d['destinations']),d['origin_total'],places=5)

    def test_missing_age_is_not_zero(self):
        self.assertTrue(all(r['age_score'] is None for r in self.m.rows if r['province']!='울산광역시'))
        self.assertTrue(any(r['age_score'] is not None for r in self.m.rows if r['province']=='울산광역시'))

    def test_source_unchanged(self):
        self.assertEqual(sha(self.m.release),self.original_sha)

    def test_neighborhood_union_conserves_population_and_supply(self):
        m=self.m; b=m.baseline
        gi=m.ix[b['representative']]
        near=sorted(range(len(m.ids)),key=lambda i:float(((m.anchors[i]-m.anchors[gi])**2).sum()))[:5]
        keys=[m.ids[i] for i in near]
        d=m.neighborhood(keys+keys,b['id'])
        self.assertEqual(len(d['members']),len(keys))
        self.assertAlmostEqual(d['population'],sum(m.population[i] for i in near))
        self.assertAlmostEqual(sum(c[3] for c in d['origins']),d['population'],places=5)
        self.assertTrue(all(c[3]<=c[2]+1e-6 for c in d['origins']))
        self.assertTrue(all(r['allocated_supply']<=r['doctors']+1e-6 for r in d['dental']))
        self.assertAlmostEqual(sum(r['allocated_supply'] for r in d['dental']),d['supply'],places=5)
        self.assertAlmostEqual(d['candidate']['within_entry']+d['candidate']['outside_entry'],b['entry_market_population_equivalent'],places=5)
        single=m.neighborhood([keys[0]],b['id'])
        self.assertLessEqual(single['population'],d['population'])

    def test_neighborhood_invalid_selection(self):
        with self.assertRaises(ValueError): self.m.neighborhood([],self.m.baseline['id'])

if __name__=='__main__':unittest.main()
