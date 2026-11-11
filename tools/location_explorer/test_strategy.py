import unittest
import json
import numpy as np
from scipy.spatial.distance import cdist
from data_paths import ROOT
from server import Model, sha
from strategy import Strategies, local_groups, growth_groups, team_parts, representative


class RulesTest(unittest.TestCase):
    def test_team_is_capacity_not_score_multiplication(self):
        p=np.array([1000.,2000.]);s=np.array([1.,4.]);w=np.array([.6,.4])
        scores=np.array([team_parts(p,s,w,n).sum() for n in (1,2,3)])
        self.assertTrue(np.all(np.diff(scores)>0))
        self.assertTrue(np.all(np.diff(np.diff(np.r_[0.,scores]))<=0))
        self.assertLess(scores[2],3*scores[0])
        np.testing.assert_allclose(team_parts(p,s,w,2)/2,p*w/(s+2*w))
        with self.assertRaises(ValueError):team_parts(p,s,w,0)

    def test_local_all_pairs_prevents_chain(self):
        d=cdist([[0],[600],[1200]],[[0],[600],[1200]])
        parts=local_groups([{0},{1},{2}],d,np.ones((3,3)),['a','b','c'])
        self.assertEqual(len(parts),2)
        self.assertTrue(all(d[np.ix_(list(x),list(x))].max()<=750 for x in parts))

    def test_growth_fixed_anchor_prevents_chain(self):
        d=cdist([[0],[1400],[2800]],[[0],[1400],[2800]])
        com=[{'life':10-i,'stores':20} for i in range(3)]
        parts=growth_groups([{0},{1},{2}],d,np.ones((3,3)),['a','b','c'],com)
        self.assertEqual(len(parts),2)
        self.assertTrue(all(d[np.ix_(list(x),list(x))].max()<=2000 for x,_ in parts))

    def test_confirmed_representative_not_maximum_score(self):
        com=[{'life':3,'stores':12,'preferred':True},{'life':100,'stores':300}]
        self.assertEqual(representative({0,1},['current','other'],com),0)


@unittest.skipUnless((ROOT / "research/location_strategy/latest.json").is_file(), "requires regional strategy releases")
class LiveBaselineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m=Model();cls.s=Strategies(cls.m);cls.d=cls.s.regions['울산광역시'];cls.before=sha(cls.m.release)

    def test_frozen_scores_and_nested_mass(self):
        d=self.d
        for b in self.m.rows:
            if b['province']=='울산광역시':
                self.assertAlmostEqual(d['nodes'][b['representative']]['team']['1'],b['entry_market_population_equivalent'],places=6)
                for mode in ['local','growth']:
                    self.assertEqual(len({d['owners'][mode][k] for k in b['members']}),1)
        for mode,groups in d['modes'].items():
            flat=[k for g in groups for k in g['members']]
            self.assertEqual(len(flat),len(set(flat)))
            self.assertEqual(set(flat),set(d['nodes']))
            self.assertAlmostEqual(sum(g['population'] for g in groups),d['checks']['population_preserved'],places=5)
            self.assertAlmostEqual(sum(g['supply'] for g in groups),d['checks']['supply_preserved'],places=6)
            for g in groups:
                self.assertEqual(g['team'],d['nodes'][g['representative']]['team'])
        for g in d['modes']['local']:
            self.assertEqual(len({d['owners']['growth'][k] for k in g['members']}),1)
        for r in d['nodes'].values():
            self.assertLessEqual(r['near_1km'],r['team']['1']+.02)
        self.assertEqual(sha(self.m.release),self.before)

    def test_case_grouping_and_rep(self):
        d=self.d
        for terms,mode in [(('문현로 61','문현로 26'),'local'),(('점촌3길 10-4','점촌5길 42','구영앞길 2'),'growth')]:
            ids=[next(k for k,r in d['nodes'].items() if term in r['label']) for term in terms]
            self.assertEqual(len({d['owners'][mode][k] for k in ids}),1)
        k=self.m.baseline['representative']
        for mode in ['local','growth']:
            group=next(g for g in d['modes'][mode] if k in g['members'])
            self.assertEqual(group['representative'],k)

    def test_detail_origin_team_and_mode_invariance(self):
        d=self.d;k=next(k for k,r in d['nodes'].items() if '점촌3길 10-4' in r['label'])
        totals=[]
        for mode,n in [('local',1),('growth',1),('growth',3)]:
            group=d['owners'][mode][k]
            detail=self.s.detail('울산광역시',mode,group,k,n)
            total=sum(r[3] for r in detail['origins']);totals.append(total)
            self.assertAlmostEqual(total,d['nodes'][k]['team'][str(n)],places=5)
            self.assertAlmostEqual(sum(r[4] for r in detail['origins']),detail['group']['population'],places=5)
            self.assertAlmostEqual(sum(f['allocated'] for f in detail['dental']),detail['group']['supply'],places=5)
            for t in detail['team']:self.assertAlmostEqual(t['inside']+t['outside'],t['total'],places=6)
            self.assertTrue(all(r[3]<=r[2]+1e-7 and r[4]<=r[2]+1e-7 for r in detail['origins']))
        self.assertAlmostEqual(totals[0],totals[1],places=6)

    def test_national_coverage_and_disjoint_groups(self):
        meta=self.s.meta()
        self.assertTrue(meta['national_ready'])
        self.assertEqual(set(meta['built_regions']),{r['province'] for r in self.m.rows})
        expected={k for b in self.m.rows for k in b['members']}
        for mode in ['local','growth']:
            data=self.s.region('전국',mode)
            flat=[k for g in data['rows'] for k in g['members']]
            self.assertEqual(len(flat),len(set(flat)))
            self.assertEqual(set(flat),expected)
            self.assertEqual(set(data['nodes']),expected)
            for g in data['rows']:
                if g['pending']:self.assertNotIn('rank',g)
                else:self.assertFalse(data['nodes'][g['representative']]['pending'])
            self.assertAlmostEqual(sum(g['population'] for g in data['rows']),meta['checks']['전국']['population_preserved'],places=5)
            self.assertAlmostEqual(sum(g['supply'] for g in data['rows']),meta['checks']['전국']['supply_preserved'],places=5)
        self.assertEqual(sha(self.m.release),self.before)

    def test_national_detail_routes_to_original_region(self):
        for region in ['서울특별시','경기도','부산광역시','경상남도']:
            data=self.s.regions[region]
            g=next(r for r in data['modes']['growth'] if not r['pending'])
            d=self.s.detail('전국','growth',g['id'],g['representative'],3)
            self.assertEqual(d['group'],g)
            self.assertAlmostEqual(sum(p[3] for p in d['origins']),g['team']['3'],places=5)
            self.assertAlmostEqual(sum(p[4] for p in d['origins']),g['population'],places=5)
            self.assertAlmostEqual(sum(f['allocated'] for f in d['dental']),g['supply'],places=5)
            self.assertTrue(all(p[3]<=p[2]+1e-7 and p[4]<=p[2]+1e-7 for p in d['origins']))

    def test_compact_index_preserves_ranking_and_hydrates_detail(self):
        for mode in ('local','growth'):
            full=self.s.region('전국',mode)
            compact=self.s.region('전국',mode,compact=True)
            self.assertEqual(full['rows'],compact['rows'])
            self.assertEqual(full['release'],compact['release'])
            self.assertEqual(set(full['nodes']),set(compact['nodes']))
            self.assertNotIn('owners',compact)
            for key,node in compact['nodes'].items():
                self.assertEqual(node,{k:full['nodes'][key][k] for k in ('id','label','life','pending')})
                self.assertIn('team',full['nodes'][key])
            self.assertLess(len(json.dumps(compact)),len(json.dumps(full))*.65)
            g=next(r for r in compact['rows'] if not r['pending'])
            detail=self.s.detail('전국',mode,g['id'],g['representative'],1)
            self.assertEqual(detail['nodes'],{k:full['nodes'][k] for k in g['members']})
            self.assertEqual(detail['local_group_count'],len({full['owners']['local'][k] for k in g['members']}))

    def test_national_modes_preserve_location_score(self):
        for region,data in self.s.regions.items():
            for g in data['modes']['local']:
                self.assertEqual(len({data['owners']['growth'][k] for k in g['members']}),1)
            for b in self.m.rows:
                if b['province']==region:
                    self.assertAlmostEqual(data['nodes'][b['representative']]['team']['1'],b['entry_market_population_equivalent'],places=6)

if __name__=='__main__':unittest.main()
