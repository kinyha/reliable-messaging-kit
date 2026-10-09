import math,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from benchmark import quantile

class HistogramDeltaTest(unittest.TestCase):
    def buckets(self,*counts):
        return {('timer_bucket',(('le',str(bound)),)):count for bound,count in zip([1,2,math.inf],counts)}
    def test_uses_new_observations_instead_of_lifetime_distribution(self):
        before=self.buckets(100,100,100);after=self.buckets(100,110,110)
        self.assertAlmostEqual(quantile(before,after,'timer_bucket',.5),1500)
        self.assertAlmostEqual(quantile(before,after,'timer_bucket',.99),1990)
    def test_unbounded_tail_is_unknown(self):
        self.assertIsNone(quantile(self.buckets(0,0,0),self.buckets(0,0,10),'timer_bucket',.99))
    def test_missing_histogram_is_unknown(self):
        self.assertIsNone(quantile({}, {},'timer_bucket',.99))
    def test_counter_reset_invalidates_measurement(self):
        with self.assertRaisesRegex(RuntimeError,'reset'):
            quantile(self.buckets(10,10,10),self.buckets(1,2,3),'timer_bucket',.99)

if __name__=='__main__':unittest.main()
