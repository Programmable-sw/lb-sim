#!/usr/bin/env python3
"""Reproducible four-LB comparison; refuses to report an incomplete collective."""
import argparse, csv, hashlib, json, os, random, re, subprocess, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/'experiments'))
from clos_topology import build_two_tier_clos
p=argparse.ArgumentParser()
p.add_argument('--out', type=Path, default=ROOT/'experiments/hybrid-lb/output/10pct-paper-defaults')
p.add_argument('--load', type=float, default=.1)
p.add_argument('--seed', type=int, default=13)
p.add_argument('--schemes', default='ecmp,conga,mprdma,reps')
p.add_argument('--binary', type=Path, default=ROOT/'build-hybrid/scratch/network-load-balance')
p.add_argument('--leaves', type=int, default=2)
p.add_argument('--active-hosts', type=int, default=16)
a=p.parse_args(); out=a.out.resolve();out.mkdir(parents=True,exist_ok=True)
r=random.Random(a.seed)
host_count=a.leaves*64
if a.active_hosts < 2 or a.active_hosts > host_count:
 raise ValueError('--active-hosts must be in [2, leaves * 64]')
pairs=[(s,d) for s in range(a.active_hosts) for d in range(a.active_hosts) if s!=d]
flows=[(s,d,1048576,0,'collective') for s,d in pairs]
target=int(len(pairs)*1048576*a.load);remaining=target;i=0
while remaining:
 s,d=pairs[i%len(pairs)];size=min(r.randrange(4096,16385),remaining)
 # htsim connection-matrix 'start' is picoseconds; preserve its actual 0..0.5 us burst.
 start=r.randrange(0,500000);flows.append((s,d,size,start,'short'));remaining-=size;i+=1
flows.sort(key=lambda x:x[3])
(out/'traffic.json').write_text(json.dumps(flows))
(out/'flow.txt').write_text(str(len(flows))+'\n'+''.join(f'{s} {d} 3 {size} {1+ps/1e12:.12f}\n' for s,d,size,ps,kind in flows))
(out/'topology.txt').write_text(build_two_tier_clos(a.leaves, rate='400Gbps', delay='500ns'))
modes={'ecmp':0,'conga':3,'mprdma':14,'reps':11};rows=[]
for name in a.schemes.split(','):
 d=out/name;d.mkdir(exist_ok=True)
 config={
 'TOPOLOGY_FILE':out/'topology.txt','FLOW_FILE':out/'flow.txt',
 'FLOW_INPUT_FILE':d/'input.txt','CNP_OUTPUT_FILE':d/'cnp.txt','FCT_OUTPUT_FILE':d/'fct.txt',
 'PFC_OUTPUT_FILE':d/'pfc.txt','QLEN_MON_FILE':d/'qlen.txt',
 'VOQ_MON_FILE':d/'voq.txt','VOQ_MON_DETAIL_FILE':d/'voq_detail.txt',
 'UPLINK_MON_FILE':d/'uplink.txt','CONN_MON_FILE':d/'conn.txt','EST_ERROR_MON_FILE':d/'error.txt',
 'FLOWGEN_START_TIME':1.0,'FLOWGEN_STOP_TIME':1.01,
 'QLEN_MON_START':1000000000,'QLEN_MON_END':1010000000,'SW_MONITORING_INTERVAL':10000,
 'CONGA_FLOWLET_US':500,'CONGA_DRE_US':32,'ONE_HOP_DELAY_NS':500,'CC_MODE':14,'LB_MODE':modes[name],'HYBRID_COLLECTIVE_BYTES':1048576,
 'ENABLE_PFC':1,'ENABLE_IRN':1,'USE_DYNAMIC_PFC_THRESHOLD':1,'BUFFER_SIZE':9,
 'ENABLE_QCN':1,'PACKET_PAYLOAD_SIZE':4096,'L2_CHUNK_SIZE':4096,'L2_ACK_INTERVAL':1,'L2_BACK_TO_ZERO':0,
 'HAS_WIN':1,'VAR_WIN':0,'GLOBAL_T':1,'RATE_BOUND':1,'ERROR_RATE_PER_LINK':0,
 'ALPHA_RESUME_INTERVAL':1,'RATE_DECREASE_INTERVAL':4,'CLAMP_TARGET_RATE':0,'RP_TIMER':300,
 'FAST_RECOVERY_TIMES':1,'EWMA_GAIN':0.0625,'RATE_AI':'50Mb/s','RATE_HAI':'100Mb/s',
 'MIN_RATE':'100Mb/s','DCTCP_RATE_AI':'1000Mb/s','FAST_REACT':1,'MI_THRESH':5,'INT_MULTI':1,
 'U_TARGET':0.95,'MULTI_RATE':0,'SAMPLE_FEEDBACK':0,'LINK_DOWN':'0 0 0','LOAD':10,
 'KMIN_MAP':'1 400000000000 20','KMAX_MAP':'1 400000000000 20','PMAX_MAP':'1 400000000000 1',
 'RANDOM_SEED':a.seed,'OOO_INTERVAL':15,'OOO_WINDOW_RATIO':1}
 (d/'config.txt').write_text(''.join(f'{k} {v}\n' for k,v in config.items()))
 env=os.environ.copy();env['LD_LIBRARY_PATH']=str(ROOT/'build-hybrid')+':'+env.get('LD_LIBRARY_PATH','')
 cmd=[str(a.binary.resolve()),str(d/'config.txt')]
 (d/'command.json').write_text(json.dumps(cmd));print('Running',name,flush=True)
 with (d/'run.log').open('w') as log:
  proc=subprocess.run(cmd,cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=180)
 if proc.returncode: raise RuntimeError(f'{name} failed: {proc.returncode}; see {d}/run.log')
 completed=[list(map(int,line.split())) for line in (d/'fct.txt').read_text().splitlines() if line.strip()]
 collective=[x for x in completed if x[4]==1048576]
 if len(collective)!=len(pairs) or len(completed)!=len(flows):
  raise RuntimeError(f'{name}: incomplete: {len(collective)}/{len(pairs)} collective, {len(completed)}/{len(flows)} total')
 logtext=(d/'run.log').read_text()
 diagnostics=[dict((k,int(v)) for k,v in re.findall(r'(\w+)=(\d+)', line))
              for line in logtext.splitlines() if line.startswith('PaperLbDiag ')]
 if len(diagnostics)!=len(flows): raise RuntimeError('Missing per-flow diagnostics')
 for info in diagnostics:
  expected=modes[name] if info['size']==1048576 else 0
  if info['lb']!=expected: raise RuntimeError('Hybrid LB isolation failed')
  if info['unique_acks']!=(info['size']+4095)//4096: raise RuntimeError('Per-packet ACK accounting failed')
 queue_diag=re.search(r'PaperQueueDiag ingress_drops=(\d+) egress_drops=(\d+)',logtext)
 if not queue_diag or any(int(v) for v in queue_diag.groups()): raise RuntimeError('Missing or nonzero switch drop counters')
 if 'WARNING: Drop' in logtext: raise RuntimeError('Lossless experiment dropped packets; inspect buffers/headroom')
 pfc=[line.split() for line in (d/'pfc.txt').read_text().splitlines() if line.strip()]
 row={'scheme':name,'collective_completed':len(collective),'total_completed':len(completed),
      'collective_jct_us':(max(x[5]+x[6] for x in collective)-1000000000)/1000,
      'pfc_event_rows':len(pfc), 'pause_events':sum(x[-1]=='1' for x in pfc), 'resume_events':sum(x[-1]=='0' for x in pfc), 'ecn_acks':sum(x['ecn'] for x in diagnostics), 'retransmission_timeouts':sum(x['rtos'] for x in diagnostics)}
 rows.append(row);print(row,flush=True)
 with (out/'results.csv').open('w') as f:
  w=csv.DictWriter(f,fieldnames=list(row));w.writeheader();w.writerows(rows)
(out/'manifest.json').write_text(json.dumps({'seed':a.seed,'leaves':a.leaves,'nics_per_leaf':64,'spines':64,'active_hosts':a.active_hosts,'short_bytes':target,'short_fraction_of_collective_bytes':a.load,
 'binary_sha256':hashlib.sha256(a.binary.read_bytes()).hexdigest(), 'point_to_point_sha256':hashlib.sha256((ROOT/'build-hybrid/libns3.19-point-to-point-optimized.so').read_bytes()).hexdigest(),'flow_sha256':hashlib.sha256((out/'flow.txt').read_bytes()).hexdigest(),
 'ecn_bytes':20000,'buffer_bytes_per_switch':9*1024*1024,'pfc':'dynamic','cc_mode':14},indent=2))
