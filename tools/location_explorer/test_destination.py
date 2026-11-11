import unittest
from destination import reference_values,build_inputs

class DestinationReferences(unittest.TestCase):
    def setUp(self):
        self.groups={'a':{'province':'A','dental_count':2,'facility_count':4,'pending':False},'b':{'province':'A','dental_count':8,'facility_count':20,'pending':False},'c':{'province':'B','dental_count':0,'facility_count':3,'pending':False},'held':{'province':'A','dental_count':1000,'facility_count':1000,'pending':True}}
        self.nodes={str(i):{'one':x,'pending':False} for i,x in enumerate([0,10,10,20])};self.nodes['held']={'one':1000,'pending':True}
    def test_reference_excludes_held_and_preserves_ties(self):
        ref,f,b=reference_values(self.groups,self.nodes)
        self.assertEqual(ref,{'growth_count':3,'candidate_count':4,'dental_max':8,'medical_max':20})
        self.assertEqual(b['0'],{'base_percentile':25,'base_rank':4});self.assertEqual(b['1'],{'base_percentile':75,'base_rank':2});self.assertEqual(b['1'],b['2']);self.assertEqual(b['3']['base_rank'],1);self.assertIsNone(b['held']['base_rank'])
    def test_regional_reference_and_zero_are_explicit(self):
        _,f,_=reference_values(self.groups,self.nodes)
        self.assertEqual(f['a']['local'],.5);self.assertEqual(f['b']['local'],1);self.assertEqual(f['c']['local'],0);self.assertIsNone(f['held']['local'])
    def test_empty_size_reference_is_zero_not_division_by_zero(self):
        _,f,_=reference_values({'z':{'province':'A','dental_count':0,'facility_count':0,'pending':False}},self.nodes)
        self.assertEqual(f['z']['dental'],0);self.assertEqual(f['z']['medical'],0)
    def test_bad_base_values_fail_closed(self):
        for value in (float('nan'),float('inf'),-1):
            with self.assertRaises(ValueError):reference_values(self.groups,{'x':{'one':value,'pending':False}})
    def test_radial_release_cannot_be_used(self):
        class Store:data={'version':'national-medical-clusters-v2-overlap-observation'}
        with self.assertRaisesRegex(ValueError,'living_area_release_required'):build_inputs(Store())
if __name__=='__main__':unittest.main()
