import unittest
import threading
from collections import OrderedDict
import numpy as np
from scipy import sparse
from destination_wide import config,choice,combine,attraction,DEFAULTS,origin_labels,team_fractions,WideDemand
class WideTests(unittest.TestCase):
    def test_zero_and_zero_purpose_exact(self):
        raw=np.array([.1,1234.567890123,0.]);r=np.array([80.,200.,5.]);n=np.array([500.,3.,2.])
        for c in ({**DEFAULTS,'effect':0},{**DEFAULTS,'purpose':0}):
            total,local,regional,national=combine(raw,r,n,c)
            np.testing.assert_array_equal(total,raw);np.testing.assert_array_equal(local,raw)
            np.testing.assert_array_equal(regional,np.zeros(3));np.testing.assert_array_equal(national,np.zeros(3))
    def test_choice_conserves_every_origin_and_obeys_distance(self):
        p=choice(np.array([[0.,100.],[100.,0.],[20.,20.]]),np.array([1.,1.]),40,3)
        np.testing.assert_allclose(p.sum(axis=1),1,atol=1e-14)
        self.assertGreater(p[0,0],p[0,1]);np.testing.assert_allclose(p[2],[.5,.5])
    def test_destination_mass_moves_not_adds(self):
        residents=np.array([100.,200.]);outside=np.array([10.,40.]);allocated=residents-outside
        pr=choice(np.array([[0.,100.],[100.,0.]]),attraction([10,50]),40,3)
        pn=choice(np.array([[0.,100.],[100.,0.]]),attraction([10,50],False),180,2)
        t=.2;rho=.3;local=(1-t)*allocated
        reg=t*(1-rho)*(allocated@pr);nat=t*rho*(allocated@pn)
        self.assertAlmostEqual(local.sum()+reg.sum()+nat.sum()+outside.sum(),residents.sum())
        # A newcomer's fixed 5% capture does not inherit the whole hub volume.
        self.assertAlmostEqual(float((reg+nat)@np.array([.05,.05])),.05*t*allocated.sum())
    def test_size_is_continuous_and_national_more_selective(self):
        r=attraction([1,5,20,80],True);n=attraction([1,5,20,80],False)
        self.assertTrue((np.diff(r)>0).all());self.assertTrue((np.diff(n)>0).all())
        self.assertGreater(n[-1]/n[0],r[-1]/r[0]);self.assertEqual(attraction([0])[0],0)
    def test_path_sum_and_reduction_possible(self):
        raw=np.array([100.,100.]);regional=np.array([20.,500.]);national=np.array([10.,1000.])
        total,a,b,c=combine(raw,regional,national,DEFAULTS)
        np.testing.assert_allclose(total,a+b+c);self.assertLess(total[0],100);self.assertGreater(total[1],100)
    def test_split_origin_labels_preserve_frozen_boundary(self):
        rows=[{'district':'a','name':'인천광역시 서구 가동','current_names':['인천광역시 검단구 가동']},{'district':'a','name':'인천광역시 서구 나동','current_names':['인천광역시 서해구 나동']},{'district':'b','name':'서울특별시 강남구 다동','current_names':['서울특별시 강남구 다동']}]
        labels=origin_labels(rows,['a','b'])
        self.assertEqual(labels['a'],'인천광역시 서구 (원천 경계)')
        self.assertEqual(labels['b'],'서울특별시 강남구')
    def test_config(self):
        for key,value in [('effect',float('nan')),('purpose',-1),('national',1.01),('regionalKm',0),('nationalKm',501),('team',2),('team',1.5),('team',float('nan'))]:
            with self.assertRaises(ValueError):config({key:value})
        with self.assertRaises(ValueError):config({'regionalKm':100,'nationalKm':50})
        self.assertEqual(config({}),DEFAULTS)
        self.assertIsInstance(config({'team':'3'})['team'],int)

    def test_team_capture_and_origin_decomposition(self):
        # Two independent candidate scenarios, two local markets and two hubs.
        weights=sparse.csr_matrix([[1.,.5],[.2,1.]])
        supply=np.array([2.,4.]);population=np.array([100.,200.])
        engine=WideDemand.__new__(WideDemand)
        engine.fractions={n:team_fractions(weights,supply,n) for n in (1,3)}
        np.testing.assert_allclose(engine.fractions[3].toarray(),3*weights.toarray()/(supply+3*weights.toarray()))
        self.assertFalse(np.allclose(engine.fractions[3].toarray(),3*engine.fractions[1].toarray()))
        engine.raw_by_team={n:np.asarray(f@population).ravel() for n,f in engine.fractions.items()}
        engine.raw=engine.raw_by_team[1];engine.captures=engine.fractions
        engine.rows=[{'node':'a','pending':False,'growth_id':'h1'},{'node':'b','pending':True,'growth_id':'h2'}]
        engine.rowix={'a':0,'b':1};engine.hubix={'h1':0,'h2':1}
        engine.hubs=[{'id':'h1'},{'id':'h2'}];engine.codes=['x','y'];engine.names={'x':'X','y':'Y'}
        engine.district_p=np.diag(population);engine.mass=population;engine.source=np.array([0,1])
        engine.source_population=300.;engine.outside_total=0.;engine.baseline_error=0.
        engine.release='release';engine.inputs={'release':'input'};engine.pins={}
        engine.lock=threading.RLock();engine.cache=OrderedDict()
        pr=np.array([[.8,.2],[.3,.7]]);pn=np.array([[.6,.4],[.1,.9]])
        engine.route=lambda c:(pr,pn,population@pr,population@pn,0.)
        one=engine.evaluate({});three=engine.evaluate({'team':3})
        self.assertEqual(one['config']['team'],1);self.assertEqual(three['config']['team'],3)
        self.assertEqual(len(engine.cache),2)
        self.assertGreater(three['rows'][0]['raw'],one['rows'][0]['raw'])
        self.assertLess(three['rows'][0]['raw'],3*one['rows'][0]['raw'])
        self.assertIsNone(three['rows'][1]['demand']);self.assertGreater(three['rows'][1]['raw'],0)
        for team,result in [(1,one),(3,three)]:
            row=result['rows'][0];origins=engine.origins('a',{'team':team})
            self.assertAlmostEqual(row['demand'],sum(row['parts'].values()))
            self.assertAlmostEqual(row['demand'],sum(x['total'] for x in origins['candidate_sources']))
            self.assertAlmostEqual(row['delta'],row['demand']-row['raw'])
            zero=engine.evaluate({'team':team,'effect':0})
            self.assertEqual(zero['rows'][0]['demand'],zero['rows'][0]['raw'])
            self.assertTrue(zero['checks']['zero_exact'])
            self.assertAlmostEqual(sum(x['total'] for x in engine.origins('a',{'team':team,'effect':0})['candidate_sources']),zero['rows'][0]['raw'])
if __name__=='__main__':unittest.main()
