import unittest
import numpy as np
from commercial_clusters import footprint_owners,counts,type_label

class FootprintTests(unittest.TestCase):
    def test_circle_is_not_used_as_polygon_boundary(self):
        a=np.pi/16
        xy=np.array([[0.,0.],[25.,0.],[24.9*np.cos(a),24.9*np.sin(a)],[26.,0.]])
        owners,ambiguous=footprint_owners(np.array([[0.,0.]]),[{'a'}],xy)
        self.assertEqual(owners,['a','a',None,None]);self.assertEqual(ambiguous,0)
    def test_overlap_nearest_and_stable_tie(self):
        sites=np.array([[0.,0.],[20.,0.]])
        points=np.array([[10.,0.],[2.,0.],[18.,0.]])
        owners,n=footprint_owners(sites,[{'b'},{'a'}],points)
        self.assertEqual(owners,['a','b','a']);self.assertEqual(n,3)
        reverse,_=footprint_owners(sites[::-1],[{'a'},{'b'}],points)
        self.assertEqual(owners,reverse)
    def test_empty_evidence_remains_empty(self):
        self.assertEqual(type_label([]),'내부 관측기관 없음')
        self.assertEqual(counts([])['facility_count'],0)
    def test_dental_workforce_scope_and_missing(self):
        r=[{'code':'31','doctors':99},{'code':'93','doctors':20},{'code':'51','doctors':3}]
        c=counts(r);self.assertEqual(c['dental_workforce'],3);self.assertEqual(c['medical_count'],1);self.assertEqual(c['korean_count'],1)
        self.assertIsNone(counts([{'code':'51','doctors':None}])['dental_workforce'])
if __name__=='__main__':unittest.main()
