"""Execute one frozen current experiment with delivery/resource audits."""
import csv,gzip,json,subprocess,time
from pathlib import Path
from build import ROOT,sha
from run_regular import analyze,save
from trace_audit import audit_trace

def write_csv(path,rows):
 if not rows:return
 keys=list(dict.fromkeys(k for r in rows for k in r))
 with path.open("w",newline="") as f:
  w=csv.DictWriter(f,fieldnames=keys);w.writeheader();w.writerows(rows)

def run(t):
 d=Path(t['directory'])
 if (d/'PILOT_RESULT.json').exists():return json.loads((d/'PILOT_RESULT.json').read_text())
 assert not (d/'RUN.json').exists(),'Partial run preserved: '+str(d)
 assert sha(t['argv'][0])==t['binary_sha256'] and sha(d/'TRAFFIC.csv')==t['traffic_sha256']
 save(d/'RUN.json',t);start=time.monotonic()
 with gzip.open(d/'trace.jsonl.gz','wt',compresslevel=1) as dst,(d/'stderr.txt').open('w') as err:
  c=['/usr/bin/time','-o',str(d/'RESOURCE.txt'),'-f','cpu_user_s=%U\ncpu_system_s=%S\nwall_s=%e\nmax_rss_kib=%M']+t['argv']
  p=subprocess.Popen(c,cwd=ROOT,stdout=subprocess.PIPE,stderr=err,text=True)
  for line in p.stdout:dst.write(line)
  code=p.wait()
 save(d/'PROCESS_STATUS.json',dict(exit_code=code,wall_seconds=time.monotonic()-start));assert code==0
 with (d/'TRAFFIC.csv').open() as f:rows=list(csv.DictReader(f))
 r=analyze(d,rows);a=audit_trace(d);s=r['native']['summary'];b=r['native']['budget_status']
 result={k:t[k] for k in ('case','lb','trim_threshold_kib','seed','normal','binary_sha256','traffic_sha256')}
 result.update(status=r['status'],**r['six_metrics'],**a)
 result.update({k:s.get(k) for k in ('fabric_pfc_events','trimmed_packets','duplicate_trims','retransmit_bytes','rto_events')})
 result.update(raw_directory=str(d),unique_delivery=b['reliable_delivery'] and b['receiver_delivery_valid'],physical_valid=r['physical_valid'],final_drained=r['final_drained'],incomplete_flows=r['incomplete_flows'])
 links=[];pfc=[]
 with gzip.open(d/'trace.jsonl.gz','rt') as f:
  for line in f:
   if not any(k in line for k in ('light_qp_link_summary','nic_pfc_xoff','nic_pfc_xon','switch_mmu_xoff','PFC_TX','PFC_RX')):continue
   e=json.loads(line)
   if e['event']=='light_qp_link_summary':links.append(e)
   elif e['event'] in ('nic_pfc_xoff','nic_pfc_xon','switch_mmu_xoff','PFC_TX','PFC_RX'):pfc.append(e)
 write_csv(d/'QP_LINKS.csv',links)
 save(d/'PFC_EVENTS.json',pfc)
 result['nic_pfc_nics']=';'.join(map(str,sorted({e['nic'] for e in pfc if e['event']=='nic_pfc_xoff' and 'nic' in e})))
 for name,prefix in [('healthy_a2a','A2A-B-'),('victim','H-')]:
  result[name+'_packet_pause_wait_sum_us']=sum(e['packet_pause_wait_sum_us'] for e in links if e['flow'].startswith(prefix))
 save(d/'PILOT_RESULT.json',result)
 assert r['status']=='complete_valid' and result['unique_delivery'] and s['delivered_unique_bytes']==s['offered_bytes'] and not a['pretrim_direct_decreases'],result
 return result

