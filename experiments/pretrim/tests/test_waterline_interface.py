import json,subprocess,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from build import BASE,BIN,ROOT

class WaterlineInterfaceTests(unittest.TestCase):
 def args(self):
  c=json.loads((BASE/'tests/fixtures/ops.json').read_text())['args']
  c[c.index('--traffic-csv')+1]=str(BASE/'tests/fixtures/reliability.csv')
  return c
 def test_removed_switches_rejected(self):
  for flag,value in [('--policy','paused'),('--trim-risk-mode','enforce'),('--trim-gamma-bytes','32768'),('--trim-threshold-kib','100')]:
   p=subprocess.run([str(BIN),flag,value],cwd=ROOT,capture_output=True,text=True)
   self.assertEqual(p.returncode,2);self.assertIn('unknown or incomplete option',p.stderr)
 def test_default_off_and_zero(self):
  for threshold in ('default','off','0'):
   with self.subTest(waterline=threshold):
    c=self.args();i=c.index('--trim-threshold');del c[i:i+2]
    if threshold!='default':c+=['--trim-threshold',threshold]
    p=subprocess.run([str(BIN)]+c,cwd=ROOT,capture_output=True,text=True,timeout=120)
    self.assertEqual(p.returncode,0,p.stderr)
    events=[json.loads(l) for l in p.stdout.splitlines()]
    resolved=next(e for e in events if e['event']=='resolved_config')
    budget=next(e for e in events if e['event']=='budget_status')
    self.assertTrue(budget['run_valid']);self.assertTrue(budget['final_drained'])
    trims=[e for e in events if e['event']=='trim']
    if threshold=='off':
     self.assertEqual(resolved['trim_mode'],'off');self.assertFalse(trims)
    else:
     self.assertEqual(resolved['trim_threshold_bytes'],102400 if threshold=='default' else 0)
     self.assertTrue(trims)
     self.assertTrue(all(e.get('gate_open',True) for e in trims))
     self.assertTrue(all(e.get('trim_switch','leaf').startswith('leaf') for e in trims))
    self.assertFalse(any(e['event'].startswith('GLOBAL_TRIM') or e['event']=='trim_risk_decision' for e in events))
