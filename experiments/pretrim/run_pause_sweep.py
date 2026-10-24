"""Explicit pause-duration/waterline matrix; prepare-only unless --execute."""
import argparse,json,shlex,shutil
from concurrent.futures import ThreadPoolExecutor,as_completed
from types import SimpleNamespace
from build import BASE,BIN,sha
from traffic import inputs,write
from run_regular import command,save,waterline
from experiment_io import run,write_csv
from pathlib import Path

def main():
 p=argparse.ArgumentParser()
 p.add_argument('--output',type=Path,required=True)
 p.add_argument('--seed',type=int,default=41)
 p.add_argument('--lbs',nargs='+',choices=['ops','reps','mprdma'],default=['ops','reps','mprdma'])
 p.add_argument('--pause-us',nargs='+',type=int,default=[50,100,200])
 p.add_argument('--trim-threshold',dest='trim_threshold_kib',nargs='+',type=waterline,default=['100'])
 p.add_argument('--jobs',type=int,default=3)
 p.add_argument('--execute',action='store_true');a=p.parse_args()
 assert a.jobs>0 and all(t>0 for t in a.pause_us)
 out=a.output.resolve();out.mkdir(parents=True,exist_ok=True);manifest=out/'FROZEN.json'
 requested=dict(seed=a.seed,lbs=a.lbs,pause_us=a.pause_us,waterlines=a.trim_threshold_kib)
 if manifest.exists():
  plan=json.loads(manifest.read_text());assert plan['requested']==requested,'Frozen matrix differs'
 else:
  assert BIN.exists(),'Run build.py first'
  binary=out/'htsim_pretrim';shutil.copy2(BIN,binary);digest=sha(binary)
  tasks=[]
  for lb in a.lbs:
   for duration in a.pause_us:
    for threshold in a.trim_threshold_kib:
     name=f'{lb}-waterline{threshold}-pause{duration}-s{a.seed}'
     d=out/name;d.mkdir();traffic=d/'TRAFFIC.csv';write(traffic,inputs('B3',a.seed))
     args=SimpleNamespace(workload='B3',lb=lb,trim_threshold_kib=threshold,seed=a.seed,transport='unordered',sender_cap=0,rx_window=128,normal=False)
     c=command(args,traffic);c[0]=str(binary);c[c.index('--pause-us')+1]=str(duration)
     t=dict(case='B3',lb=lb,trim_threshold_kib=threshold,seed=a.seed,normal=False,directory=str(d),argv=c,command=shlex.join(c),binary_sha256=digest,traffic_sha256=sha(traffic),pause_duration_us=duration)
     save(d/'TASK.json',t);tasks.append(t)
  plan=dict(requested=requested,tasks=tasks,binary_sha256=digest);save(manifest,plan)
 if not a.execute:print('Prepared',len(plan['tasks']),'points');return
 results=[]
 with ThreadPoolExecutor(max_workers=a.jobs) as pool:
  pending={pool.submit(run,t):t for t in plan['tasks']}
  for future in as_completed(pending):
   t=pending[future];r=future.result();r['pause_duration_us']=t['pause_duration_us']
   results.append(r);write_csv(out/'ABSOLUTE_RESULTS.csv',results)
   save(out/'PROGRESS.json',dict(completed=len(results),total=len(pending),status='complete' if len(results)==len(pending) else 'running'))
if __name__=='__main__':main()
