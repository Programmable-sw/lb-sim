import json,subprocess,sys,unittest,tempfile
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from build import BASE,BIN,ROOT
class ArtifactNetworkTests(unittest.TestCase):
 def test_zero_time_first_tx_has_real_rtt(self):
  fixture=json.loads((BASE/'tests/fixtures/mprdma.json').read_text())
  with tempfile.TemporaryDirectory(prefix='zero-time-rtt-')as d:
   traffic=Path(d)/'traffic.csv'
   traffic.write_text((BASE/'tests/fixtures/reliability.csv').read_text().replace('2097152,20,100','2097152,0,100'))
   cmd=[str(BIN)]+fixture['args'];cmd[cmd.index('--traffic-csv')+1]=str(traffic)
   cmd+=['--lb-implementation','artifact','--a2a-cc','nscc','--rx-mode','normal','--trim-threshold','off']
   p=subprocess.run(cmd,cwd=ROOT,capture_output=True,text=True,timeout=120)
   self.assertEqual(p.returncode,0,p.stderr)
   events=[json.loads(x)for x in p.stdout.splitlines()]
   first=next(x for x in events if x['event']=='artifact_ack_feedback'and x['flow']=='native-to-R')
   self.assertGreater(first['rtt_us'],0,'TX at time zero is valid, not a missing timestamp')
 def test_loss_and_ack_blackout_fallback(self):
  for proto in ('mprdma','reps'):
   for fault in ('payload-loss','ack-blackout'):
    with self.subTest(protocol=proto,fault=fault):
     fixture=json.loads((BASE/'tests/fixtures'/f'{proto}.json').read_text())
     cmd=[str(BIN)]+fixture['args'];cmd[cmd.index('--traffic-csv')+1]=str(BASE/'tests/fixtures/reliability.csv')
     cmd+=['--lb-implementation','artifact','--a2a-cc','dctcp_variant','--trim-threshold','off','--rx-mode','normal','--unordered-test-fault',fault]
     p=subprocess.run(cmd,cwd=ROOT,capture_output=True,text=True,timeout=120)
     self.assertEqual(p.returncode,0,p.stderr)
     events=[json.loads(x)for x in p.stdout.splitlines()]
     budget=next(x for x in events if x['event']=='budget_status')
     self.assertTrue(budget['run_valid']);self.assertTrue(budget['final_drained'])
     summary=next(x for x in events if x['event']=='summary')
     self.assertGreater(summary['rto_events'],0);self.assertEqual(summary['incomplete_flows'],0)
     if proto=='reps':
      self.assertTrue(any(x['event']=='artifact_reps_freezing'and x['entered']for x in events))
 def test_real_feedback_and_delivery(self):
  for proto in ('ops','mprdma','reps'):
   for cc in ('dctcp_variant','nscc'):
    with self.subTest(protocol=proto,cc=cc):
     fixture=json.loads((BASE/'tests/fixtures'/f'{proto}.json').read_text())
     cmd=[str(BIN)]+fixture['args'];cmd[cmd.index('--traffic-csv')+1]=str(BASE/'tests/fixtures/reliability.csv')
     cmd+=['--lb-implementation','artifact','--a2a-cc',cc]
     p=subprocess.run(cmd,cwd=ROOT,capture_output=True,text=True,timeout=120)
     self.assertEqual(p.returncode,0,p.stderr)
     events=[json.loads(x) for x in p.stdout.splitlines()]
     budget=next(x for x in events if x['event']=='budget_status')
     self.assertTrue(budget['run_valid']);self.assertTrue(budget['final_drained'])
     self.assertEqual(next(x for x in events if x['event']=='receiver_audit_summary')['rx_accounting_violations'],0)
     self.assertEqual(sum(x['event']=='flow_complete' for x in events),4)
     c=next(x for x in events if x['event']=='artifact_alignment_config')
     self.assertEqual(c['a2a_cc'],cc)
     self.assertTrue(any(x['event']=='artifact_ack_feedback' for x in events))
     if proto in ('mprdma','reps'):
      paths=[x for x in events if x['event']=='artifact_entropy_select']
      self.assertTrue(paths);self.assertTrue(all(0<=x['entropy']<65535 for x in paths))
      self.assertTrue(all(0<=x['path']<8 for x in paths))
      self.assertTrue(any(x['entropy']>=8 for x in paths),'Must cache entropy, not physical Spine IDs')
      self.assertTrue(any(x['reuse'] for x in paths),'Real ACKs must drive entropy reuse')
     for x in events:
      if x['event']=='artifact_nack_feedback' and x['pretrim'] and x['accepted']:
       self.assertEqual(x['old_cwnd_bytes'],x['new_cwnd_bytes'])
