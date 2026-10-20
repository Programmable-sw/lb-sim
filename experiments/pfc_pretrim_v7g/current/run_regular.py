"""One explicit regular point; prepare-only by default, never a hidden matrix."""
import argparse,collections,csv,gzip,json,shlex,subprocess,time
from pathlib import Path
from build import BASE,BIN,ROOT,sha
from traffic import inputs,write
from metrics import six_metrics
POLICIES={'B0':('off','shadow',100,None),
 'G64':('global-paused','shadow',64,None),'G80':('global-paused','shadow',80,None),'G100':('global-paused','shadow',100,None),
 'S100':('paused','shadow',100,None),'S200':('paused','shadow',200,None),'S400':('paused','shadow',400,None),
 'IR':('paused','enforce',100,None),'T32':('paused','enforce',100,32768),'T128':('paused','enforce',100,131072)}
def command(a,path):
 assert a.workload=='B3'or a.lb!='ops','OPS is Hybrid-only; use ECMP/MP-RDMA/REPS in pure W workloads'
 assert a.workload!='B3'or a.lb!='ecmp','B3 A2A uses OPS/MP-RDMA/REPS; its background remains ECMP'
 policy,risk,q,gamma=POLICIES[a.policy];proto='dcqcn'if a.lb=='ecmp'else a.lb
 pauses='112,113'if a.workload in('B3','W3-2')else'112,113,114,115'if a.workload=='W3-4'else'112'
 return [str(BIN),'--topology-profile','leaf8x16','--scenario','v5_a2a_probe',
  '--seed',str(a.seed),'--protocol',proto,'--cc','dcqcn','--lb','ecmp',
  '--hybrid-qp','on'if a.workload=='B3'else'off','--hybrid-observation','on'if a.workload=='B3'else'off',
  '--transport-profile',a.transport,'--sender-window-psn',str(a.sender_cap),'--rx-window-psn',str(a.rx_window),
  '--reliable-transport','mprdma_native_recovery1'if a.workload!='B3'and proto=='mprdma'else'finite_rx_selective_v1','--trim-feedback','missing_ce_path_v1','--trim-ce','preserve','--trim-induced-ce','off',
  '--leaf-buffer-mib','20','--spine-buffer-mib','20','--leaf-ecn-kmin-kib','100','--leaf-ecn-kmax-kib','400',
  '--spine-ecn-kmin-kib','100','--spine-ecn-kmax-kib','400','--ecn-pmax','0.2','--pfc-hysteresis-bytes','32768',
  '--rx-shared-entries','4096','--nic-rx-capacity-kib','512','--nic-control-queue-packets','4096',
  '--nic-control-servers','1','--nic-control-service-ns','7.8125','--fault-start-us','50','--pause-us','200',
  '--fault-consume-gbps','0','--pause-nics',pauses,'--rx-mode','normal'if a.normal else'pause',
  '--policy',policy,'--trim-risk-mode',risk,'--trim-threshold-kib',str(q),'--trim-gamma-bytes',str(gamma or a.gamma_bytes),
  '--traffic-csv',str(path),'--group',f'{a.workload}-{a.lb}-{a.policy}-s{a.seed}-normal{int(a.normal)}',
  '--study-end-us','20000','--trace-level','summary']
def save(path,z):path.write_text(json.dumps(z,indent=2)+'\n')
def analyze(d,rows):
 native={};done={};reliable={};configs=[]
 for line in gzip.open(d/'trace.jsonl.gz','rt'):
  e=json.loads(line);kind=e['event']
  if kind in('summary','budget_status','receiver_audit_summary','unordered_summary'):native[kind]=e
  if kind in('resolved_config','config','common_cc_config'):configs.append(e)
  if kind=='flow_complete':done[e['flow']]=e['time_us']
  if kind=='light_qp_reliable_summary':reliable[e['flow']]=e
 flows=[dict(r,complete_us=done.get(r['flow']),fct_us=done[r['flow']]-float(r['start_us'])if r['flow']in done else None)for r in rows]
 with (d/'FLOWS.csv').open('w',newline='')as f:
  w=csv.DictWriter(f,fieldnames=list(flows[0]));w.writeheader();w.writerows(flows)
 save(d/'NATIVE_SUMMARIES.json',native);save(d/'RESOLVED_CONFIG.json',configs)
 b=native['budget_status'];physical=all(b[k]==0 for k in('mmu_budget_violations','headroom_violations','serializer_overlap_violations','paused_tx_start_violations'))and native['receiver_audit_summary']['rx_accounting_violations']==0
 groupstats={}
 for g in dict.fromkeys(r['communication_group']for r in rows):
  fs=[f for f in flows if f['communication_group']==g];complete=all(f['fct_us']is not None for f in fs)
  groupstats[g]=dict(qps=len(fs),incomplete=sum(f['fct_us']is None for f in fs),mean_fct_us=sum(f['fct_us']for f in fs)/len(fs)if complete else None,
   communication_us=max(f['complete_us']for f in fs)-min(float(f['start_us'])for f in fs)if complete else None)
  for label,q in(('p50',.5),('p95',.95),('p99',.99)):
   vs=sorted(f['fct_us']for f in fs)if complete else[];pos=q*(len(vs)-1)if vs else 0;i=int(pos)
   groupstats[g]['fct_'+label+'_us']=vs[i]+(vs[min(i+1,len(vs)-1)]-vs[i])*(pos-i)if vs else None
 status='complete_valid'if b['run_valid']and b['final_drained']and physical else'incomplete'if physical and any(f['fct_us']is None for f in flows)else'invalid'
 result=dict(status=status,physical_valid=physical,final_drained=b['final_drained'],incomplete_flows=sum(f['fct_us']is None for f in flows),six_metrics=six_metrics(flows),groups=groupstats,native=native)
 save(d/'RESULT.json',result);return result
def main():
 p=argparse.ArgumentParser();p.add_argument('--workload',choices=['B3','W1','W2','W3-2','W3-4'],default='B3')
 p.add_argument('--lb',choices=['ops','ecmp','mprdma','reps'],default='ops');p.add_argument('--policy',choices=list(POLICIES),default='B0');p.add_argument('--seed',type=int,default=41)
 p.add_argument('--normal',action='store_true');p.add_argument('--transport',choices=['unordered','limited-ooo'],default='unordered');p.add_argument('--sender-cap',type=int,default=0);p.add_argument('--rx-window',type=int,default=128);p.add_argument('--gamma-bytes',type=int,default=514352)
 p.add_argument('--output',type=Path,required=True);p.add_argument('--execute',action='store_true');a=p.parse_args()
 assert a.sender_cap>=0 and a.rx_window>0 and a.gamma_bytes>=0
 d=a.output.resolve();d.mkdir(parents=True,exist_ok=True);assert not(d/'RUN.json').exists(),'Existing execution preserved'
 rows=inputs(a.workload,a.seed);path=d/'TRAFFIC.csv';write(path,rows);cmd=command(a,path)
 save(d/'PLANNED_RUN.json',dict(argv=cmd,command=shlex.join(cmd),workload=a.workload,lb=a.lb,seed=a.seed,policy=a.policy,normal=a.normal,traffic_sha256=sha(path),execute=a.execute))
 if not a.execute:print(shlex.join(cmd));return
 assert BIN.exists(),'Run build.py first'
 save(d/'RUN.json',dict(argv=cmd,command=shlex.join(cmd),binary_sha256=sha(BIN),traffic_sha256=sha(path)))
 start=time.monotonic()
 with gzip.open(d/'trace.jsonl.gz','wt',compresslevel=1)as out,(d/'stderr.txt').open('w')as err:
  proc=subprocess.Popen(cmd,cwd=ROOT,stdout=subprocess.PIPE,stderr=err,text=True)
  for line in proc.stdout:out.write(line)
  code=proc.wait()
 save(d/'PROCESS_STATUS.json',dict(exit_code=code,wall_seconds=time.monotonic()-start));assert code==0
 result=analyze(d,rows);print(json.dumps(result['six_metrics']));assert result['status']=='complete_valid',result['status']
if __name__=='__main__':main()
