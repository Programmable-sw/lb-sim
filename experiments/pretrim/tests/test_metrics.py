import unittest
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from metrics import six_metrics,paired_stats

class MetricsTests(unittest.TestCase):
    def test_average_not_completion_or_max(self):
        flows=[]
        for group,values in [('A2A-A',[10,30]),('A2A-B',[5,15]),('H-long',[10,90]),
                             ('H-short',[2,4]),('R-long',[20,40]),('R-short',[6,10])]:
            for value in values:flows.append(dict(communication_group=group,start_us=100,complete_us=100+value,fct_us=value))
        r=six_metrics(flows)
        self.assertEqual([r[k] for k in r],[30,15,50,3,30,8])
    def test_censored_category_not_dropped(self):
        f=[dict(communication_group='R-long',fct_us=10,start_us=20,complete_us=30),
           dict(communication_group='R-long',fct_us=None,start_us=20,complete_us=None)]
        self.assertIsNone(six_metrics(f)['blocked_long_mean_us'])
    def test_pair_first_then_mean(self):
        r=paired_stats([10,100],[5,90])
        self.assertEqual(r['mean_improvement_pct'],30)
        self.assertEqual(r['wins'],2)
        self.assertNotEqual(r['mean_improvement_pct'],100*(110-95)/110)

if __name__=='__main__':unittest.main()
