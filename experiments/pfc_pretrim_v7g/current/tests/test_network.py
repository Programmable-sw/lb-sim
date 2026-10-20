"""Small real-link regression, not a formal performance matrix."""
import json,subprocess,unittest,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from build import BASE,BIN,ROOT
class NetworkTests(unittest.TestCase):
 def test_three_protocols_unchanged(self):
  self.assertTrue(BIN.exists(),'Build with current/build.py first')
  for proto in('ops','mprdma','reps'):
   with self.subTest(protocol=proto):
    fixture=json.loads((BASE/'tests/fixtures'/f'{proto}.json').read_text())
    cmd=[str(BIN)]+fixture['args'];i=cmd.index('--traffic-csv');cmd[i+1]=str(BASE/'tests/fixtures/reliability.csv')
    p=subprocess.run(cmd,cwd=ROOT,capture_output=True,text=True,timeout=120)
    self.assertEqual(p.returncode,0,p.stderr)
    events=[json.loads(x)for x in p.stdout.splitlines()]
    for kind,expected in fixture['expected'].items():
     self.assertEqual([x for x in events if x['event']==kind],expected,kind)
    self.assertTrue(next(x for x in events if x['event']=='budget_status')['final_drained'])
if __name__=='__main__':unittest.main()
