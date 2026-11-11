import unittest
import numpy as np
from opportunity import qualifies, kernel, entry_parts, Opportunity, OUT
from server import Model, read, sha


class OriginUnitTest(unittest.TestCase):
    def test_medical_presence_is_not_a_candidate_requirement(self):
        com={'source_eligible':True,'candidate_supported':True}
        floors={'residents':500,'life':3}
        screen={'features':{'residents':3000,'life':8},'status':'housing_evidence_pending',
                'medical_owned':0,'dental_owned':0}
        self.assertTrue(qualifies(com,screen,floors))
        self.assertFalse(qualifies(dict(com,source_eligible=False),screen,floors))
        screen['features']['life']=0
        self.assertFalse(qualifies(com,screen,floors))

    def test_new_closer_candidate_reclaims_existing_allocated_demand(self):
        p=np.array([4000.,4000.]);k=kernel(np.array([2000.,0.]),6000)
        existing=k/(p@k)
        part,w,reserve=entry_parts(p,np.array([0.,2000.]),existing,6000)
        self.assertGreater(part[0],part[1])
        self.assertGreater(part[0],2000)
        self.assertAlmostEqual(p@w+reserve,1)
        self.assertTrue(np.all(part<=p))
        # Before entry every covered resident went to the incumbent. Afterwards
        # the old assignment is recomputed, not subtracted from available demand.
        remaining=p*existing/(existing+w)
        np.testing.assert_allclose(part+remaining,p)

    def test_resident_refinement_and_market_labels_do_not_change_score(self):
        p=np.array([3000.,1000.]);d=np.array([200.,1200.]);existing=np.array([.0002,.0003])
        a,w,_=entry_parts(p,d,existing,6000)
        split,ws,_=entry_parts(np.repeat(p/2,2),np.repeat(d,2),np.repeat(existing,2),6000)
        self.assertAlmostEqual(a.sum(),split.sum())
        self.assertAlmostEqual(p@w,np.repeat(p/2,2)@ws)

    def test_no_supply_is_unallocated_until_independent_entry(self):
        p=np.array([100.,200.]);d=np.array([100.,6500.])
        part,w,reserve=entry_parts(p,d,np.zeros(2),6000)
        np.testing.assert_allclose(part,[100.,0.])
        self.assertAlmostEqual(p@w+reserve,1.)
        part,w,reserve=entry_parts(np.array([100.]),np.array([6500.]),np.zeros(1),6000)
        self.assertEqual(reserve,1.)
        self.assertEqual(part.sum(),0.)


@unittest.skipUnless((OUT/'latest.json').exists(),'national build not yet complete')
class NationalIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m=Model();cls.o=Opportunity(cls.m);cls.original=sha(cls.m.release)

    def test_national_invariants(self):
        d=self.o.data;c=d['checks']
        self.assertLess(c['physician_unit_error'],1e-8)
        self.assertLess(c['entrant_unit_error'],1e-8)
        self.assertLess(c['origin_mass_error'],1e-7)
        self.assertGreater(c['recovered_count'],0)
        self.assertEqual(len({r['id'] for r in d['rows']}),len(d['rows']))
        self.assertTrue(all(r['score']>=0 and r['gap_population_1km']<=r['population_1km']+1e-7 for r in d['rows']))
        self.assertTrue(c['base_release_unchanged'])

    def test_actual_cases_and_loss_accounting(self):
        rows=self.o.data['rows']
        chosen=[next(r for r in rows if s in r['label']) for s in ('명촌8길 8','천상중앙길 90','점촌3길 10-4')]
        chosen.append(next(r for r in rows if r['province']=='울산광역시' and r['recovered_candidate']))
        chosen.append(next(r for r in rows if r['province']=='서울특별시' and r['recovered_candidate']))
        for r in chosen:
            d=self.o.detail(r['id'])
            self.assertAlmostEqual(sum(p[3] for p in d['origins']),r['score'],places=5)
            self.assertAlmostEqual(sum(f['shifted_equivalent'] for f in d['dental'])+d['previously_unallocated_entry'],r['score'],places=5)
            self.assertTrue(all(p[3]<=p[2]+1e-6 for p in d['origins']))
        self.assertEqual(sha(self.m.release),self.original)


if __name__=='__main__':unittest.main()
