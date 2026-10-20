"""Frozen regular B3 and W1/W2/W3 inputs; no C1/C2 or Gamma sweeps."""
import argparse,csv
from pathlib import Path
FIELDS=['flow','role','communication_group','src','dst','spine','payload_bytes','start_us','qp_wire_pacing_gbps']
def ecmp(seed,s,d):
 mask=(1<<64)-1
 x=((seed*0x9e3779b97f4a7c15)^((s+1)*0xbf58476d1ce4e5b9)^((d+1)*0x94d049bb133111eb))&mask
 x^=x>>30;x=x*0xbf58476d1ce4e5b9&mask;x^=x>>27
 return (x^(x>>31))%8
def members():
 return {'A2A-A':[16*l+o for l in range(8)for o in(0,1)]+[114,115],
         'A2A-B':[16*l+o for l in range(8)for o in(4,5)]}
def b3(seed):
 # Background source selection intentionally uses the original 16-member A.
 jobs=members();originalA=jobs['A2A-A'][:-2]
 occupied=set(originalA+jobs['A2A-B'])|{104,118,119}
 remote=[16*l+o for o in range(16)for l in range(7)if 16*l+o not in occupied]
 rsrc=remote[:32];remaining=[n for n in remote if n not in rsrc]
 hsrc=remaining[:8];remaining=[n for n in remaining if n not in hsrc]
 osrc=[n for n in remaining if n//16!=6][:8]
 remaining=[n for n in remote if n not in set(rsrc+hsrc+osrc)]
 oshort=[n for n in remaining if n//16!=6][:8]
 rh=[n for n in remaining if n not in oshort][:24]
 short=[rh[:4]+rh[4:12]+oshort[:4],rh[12:16]+rh[16:24]+oshort[4:]]
 rows=[]
 def add(name,g,s,d,size,start,rate):
  rows.append(dict(flow=name,role='healthy'if g.startswith('A2A')else'background',communication_group=g,src=s,dst=d,spine=ecmp(seed,s,d),payload_bytes=size,start_us=start,qp_wire_pacing_gbps=rate))
 for job,ns in jobs.items():
  for s in ns:
   for d in ns:
    if s!=d:add(f'{job}-{s}-{d}',job,s,d,262144,100,100)
 for prefix,ss in(('R',rsrc),('H',hsrc),('O',osrc)):
  for k,s in enumerate(ss):
   d=112+k%2 if prefix=='R'else 118+k%2 if prefix=='H'else 104
   add(f'{prefix}-long-{s}-{d}',prefix+'-long',s,d,2097152,20,25)
 for rnd,start in enumerate((100,160,220,280)):
  for k,s in enumerate(short[rnd%2]):
   prefix='R'if k<4 else'H'if k<12 else'O'
   d=112+k%2 if prefix=='R'else 118+k%2 if prefix=='H'else 104
   size=(4,8,16,32,64,128)[(rnd*16+k)%6]*1024
   add(f'{prefix}-short-{s}-{d}-b{rnd}',prefix+'-short',s,d,size,start,100)
 return rows
def victim(case,seed):
 rows=[];n=2 if case=='W3-2'else 4 if case=='W3-4'else 1
 def row(name,g,s,d,size,start,rate):
  return dict(flow=name,role='background'if g in('R-long','R-short')else'healthy',communication_group=g,src=s,dst=d,spine=ecmp(seed,s,d),payload_bytes=size,start_us=start,qp_wire_pacing_gbps=rate)
 if case=='W2':
  ns=[0,1,16,17,32,33,48,49,64,65,80,81,96,97,112,116]
  rows=[row(f'a2a-healthy-{s}-{d}','R-A2A'if d==112 else'normal-A2A',s,d,524288,100,100)for s in ns for d in ns if s!=d]
  return rows+[r for r in victim('W1',seed)if r['communication_group']=='victim-short'and r['src']not in ns]
 assert case in('W1','W3-2','W3-4')
 sources=[16*(k//8)+k%8 for k in range(32)]
 for i,s in enumerate(sources):rows.append(row(f'R-long-{s}-to-{112+i%n}','R-long',s,112+i%n,4*1048576,20,25))
 for burst,start in enumerate((100,160,220,280)):
  for i,s in enumerate(range(64,72)):
   d=112+(s-64)%n
   rows.append(row(f'R-short-{s}-b{burst}','R-short',s,d,(4,8,16,32,64,128,16,4)[i]*1024,start,100))
  for i,s in enumerate(range(80,96)):
   d=116+i%2
   rows.append(row(f'victim-short-healthy-{s}-{d}-b{burst}','victim-short',s,d,(4,8,16,32,64,128)[i%6]*1024,start,100))
  for i,s in enumerate(range(96,104)):
   rows.append(row(f'control-short-healthy-{s}-b{burst}','control-short',s,104,(4,8,16,32,64,128,16,4)[i]*1024,start,100))
 return rows
def inputs(case,seed):return b3(seed)if case=='B3'else victim(case,seed)
def write(path,rows):
 path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
 if path.exists():
  with path.open()as f:assert list(csv.DictReader(f))==[{k:str(r[k])for k in FIELDS}for r in rows],'Existing input differs'
  return
 with path.open('w',newline='')as f:
  w=csv.DictWriter(f,fieldnames=FIELDS,lineterminator='\n');w.writeheader();w.writerows(rows)
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--workload',choices=['B3','W1','W2','W3-2','W3-4'],default='B3');p.add_argument('--seed',type=int,default=41);p.add_argument('--output',type=Path,required=True);a=p.parse_args();write(a.output,inputs(a.workload,a.seed))
