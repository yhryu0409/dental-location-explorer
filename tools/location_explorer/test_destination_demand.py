import unittest
import numpy as np
from scipy import sparse
from destination_demand import config,reallocate

class DemandTests(unittest.TestCase):
    def fixture(self):
        B=sparse.csr_matrix([[.4,.4,0],[0,.3,.5],[0,0,0]])
        return B,np.array([100.,200.,50.]),np.array([.2,.2,1.]),np.array([[1.,.3,.8,.2],[0.,.7,.1,.8],[.5,.2,.4,.3]])
    def test_zero_identity_all_component_weights(self):
        B,p,o,f=self.fixture()
        for weights in ([.45,.15,.3,.1],[0,0,0,0],[1,0,0,0]):
            new,out,delta,_=reallocate(B,p,o,f,np.array(weights),0)
            np.testing.assert_array_equal(new,B.T@p);np.testing.assert_array_equal(delta,np.zeros((3,4)))
            np.testing.assert_array_equal(out,p*o)
    def test_hand_calculation_redistributes_not_creates(self):
        B=sparse.csr_matrix([[.4,.4]]);p=np.array([100.]);o=np.array([.2]);f=np.array([[1.,0,0,0],[0.,0,0,0]])
        new,out,delta,error=reallocate(B,p,o,f,np.array([1.,0,0,0]),1)
        np.testing.assert_allclose(new,[80/1.4,40/1.4]);np.testing.assert_allclose(out,[20/1.4])
        self.assertLess(delta[1].sum(),0);self.assertAlmostEqual(new.sum()+out.sum(),100);self.assertLess(error,1e-10)
    def test_component_sum_and_origin_conservation(self):
        B,p,o,f=self.fixture();w=np.array([.45,.15,.3,.1]);effect=.8
        new,out,parts,err=reallocate(B,p,o,f,w,effect)
        h=f@w;den=1+effect*(B@h);matrix=B.toarray()*(1+effect*h)/den[:,None]
        np.testing.assert_allclose(matrix.sum(axis=1)+o/den,1)
        np.testing.assert_allclose(parts.sum(axis=1),new-B.T@p,atol=1e-12)
        self.assertAlmostEqual(new.sum()+out.sum(),p.sum());self.assertLess(err,1e-10)
        self.assertEqual(matrix[0,2],0);self.assertEqual(out[2],50)
    def test_fixed_competition_shares(self):
        B,p,o,f=self.fixture();new,out,parts,_=reallocate(B,p,o,f,np.array([.45,.15,.3,.1]),.7)
        entrant=np.array([.2,.4,.1]);supply=np.array([2.,1.,4.]);share=entrant/(supply+entrant)
        before=float((B.T@p)@share);after=float(new@share)
        self.assertAlmostEqual(after-before,float(share@parts.sum(axis=1)))
        self.assertLess(after,new.sum())
    def test_config_validation_and_zero_effect(self):
        for effect,weights in [(float('nan'),[1]*4),(-1,[1]*4),(1.01,[1]*4),(.5,[0]*4),(.5,[float('nan')]*4),(.5,[101,0,0,0])]:
            with self.assertRaises(ValueError):config(effect,weights)
        np.testing.assert_array_equal(config(0,[0]*4)[1],[0]*4)
        np.testing.assert_allclose(config(.5,[45,15,30,10])[1],[.45,.15,.3,.1])
if __name__=='__main__':unittest.main()
