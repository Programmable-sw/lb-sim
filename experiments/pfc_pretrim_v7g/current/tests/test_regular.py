import argparse,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from traffic import inputs
from run_regular import command
class RegularTests(unittest.TestCase):
 def test_b3_inventory(self):
  r=inputs('B3',41)
  self.assertEqual(len(r),658);self.assertEqual(sum(x['payload_bytes']for x in r),246435840)
  self.assertEqual(len({n for x in r for n in(x['src'],x['dst'])}),117)
  self.assertEqual(sum(x['communication_group']=='A2A-A'for x in r),306)
  self.assertEqual(sum(x['communication_group']=='A2A-B'for x in r),240)
 def test_w3_mapping(self):
  for case,n in(('W1',1),('W3-2',2),('W3-4',4)):
   r=inputs(case,13);self.assertEqual(len(r),160)
   self.assertEqual({x['dst']for x in r if x['communication_group']=='R-short'},set(range(112,112+n)))
 def test_no_special_workloads(self):
  with self.assertRaises(AssertionError):inputs('C1',13)
 def test_only_gamma_changes(self):
  a=argparse.Namespace(workload='B3',lb='ops',seed=41,policy='IR',transport='unordered',sender_cap=0,rx_window=128,gamma_bytes=514352,normal=False)
  old=command(a,Path('/tmp/input.csv'));a.gamma_bytes=204800;new=command(a,Path('/tmp/input.csv'))
  self.assertEqual([i for i in range(len(old))if old[i]!=new[i]],[old.index('--trim-gamma-bytes')+1])
if __name__=='__main__':unittest.main()
