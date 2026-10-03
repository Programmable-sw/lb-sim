#!/usr/bin/env python3
"""Compare ECMP, MP-RDMA, CONGA and REPS on all-to-all with bursty short flows."""
import argparse, json, random, subprocess, re
from pathlib import Path

SCHEMES = {"ecmp": "ecmp", "mprdma": "mprdma",
           "conga": "conga", "reps": "reps"}

def main():
    p=argparse.ArgumentParser(); p.add_argument('--nodes',type=int,default=16); p.add_argument('--long-size',type=int,default=1024*1024)
    p.add_argument('--short-load',type=float,default=.10, help='short-flow byte fraction (0.00-0.20)')
    p.add_argument('--seed',type=int,default=13); p.add_argument('--cc',default='dctcp_variant', choices=['dcqcn','dctcp_variant'])
    p.add_argument('--traffic-json', type=Path, default=None,
                   help='reuse an ns-3 hybrid-lb traffic.json exactly')
    p.add_argument('--binary',default=None); p.add_argument('--out',default='output/mprdma_bursty_a2a'); a=p.parse_args()
    if a.binary is None: a.binary=str(Path(__file__).resolve().parents[2]/'sim/datacenter/htsim_roce')
    if not 0<=a.short_load<=.20: p.error('short-load must be 0.00..0.20')
    out=Path(a.out); out.mkdir(parents=True,exist_ok=True); tm=out/'traffic.cm'; rng=random.Random(a.seed); flows=[]
    pairs=[(s,d) for s in range(a.nodes) for d in range(a.nodes) if s != d]
    fid=1
    if a.traffic_json:
        records=json.loads(a.traffic_json.read_text())
        for s,d,size,start,kind in records:
            suffix=' lb ecmp' if kind != 'collective' else ''
            flows.append(f'{s}->{d} id {fid} start {start} size {size}{suffix}')
            fid += 1
    else:
      for s,d in pairs:
        flows.append(f'{s}->{d} id {fid} start 0 size {a.long_size}'); fid+=1
    # Add bursty 4-16 KiB flows until their offered bytes reach the requested
    # fraction of the long-flow bytes.  Rotate source/destination pairs so the
    # short load is collective rather than concentrated on one pair.
    target_short_bytes = int(len(pairs) * a.long_size * a.short_load)
    short_bytes = 0
    pair_index = 0
    while not a.traffic_json and short_bytes < target_short_bytes:
        s,d = pairs[pair_index % len(pairs)]
        size = min(rng.randrange(4096, 16385), target_short_bytes - short_bytes)
        if size == 0:
            break
        # Hybrid workload semantics: only collective flows use the scheme under
        # test. Storage/control short flows always use per-flow ECMP.
        flows.append(f'{s}->{d} id {fid} start {rng.randrange(0,500000)} size {size} lb ecmp')
        fid += 1
        short_bytes += size
        pair_index += 1
    tm.write_text(f'Nodes {a.nodes}\nConnections {len(flows)}\n'+'\n'.join(flows)+'\n')
    rows=[]
    for name,lb in SCHEMES.items():
      cmd=[a.binary,'-tm',str(tm),'-nodes',str(a.nodes),'-conns',str(len(flows)),
           '-tiers','2','-linkspeed','400000','-mtu','4096',
           '-hop_latency','0.5','-switch_latency','0',
           '-lb',lb,'-cc',a.cc,'-paths','4','-queue_type','shared_buffer_ecn',
           '-shared_buffer_mb','9','-shared_ingress_alpha','0.0625',
           '-shared_egress_alpha','1.0',
           '-pfc','on','-lossless_ecn_bytes','20000','20000','-end','2000000','-seed',str(a.seed)]
      r=subprocess.run(cmd,text=True,capture_output=True,check=True); text=r.stdout+r.stderr
      (out/f'{name}.log').write_text(text)
      completed=len(re.findall(r' finished at ',text))
      if completed != len(flows):
          raise RuntimeError(f'{name}: incomplete flows {completed}/{len(flows)}')
      diag=re.search(r'RoceDiag .* rtos=([0-9]+)',text)
      if not diag or int(diag.group(1)):
          raise RuntimeError(f'{name}: missing diagnostics or nonzero RTO count')
      queue=re.search(r'QueueDiag .* lossy_drops=([0-9]+).* composite_drops=([0-9]+)',text)
      if not queue or any(int(value) for value in queue.groups()):
          raise RuntimeError(f'{name}: missing queue diagnostics or lossy drops')
      shared=re.search(r'SharedBufferDiag .* overflows=([0-9]+)',text)
      if not shared or int(shared.group(1)):
          raise RuntimeError(f'{name}: missing shared-buffer diagnostics or headroom overflow')
      vals=[float(finish) for finish,size in re.findall(
          r'finished at ([0-9]+(?:\.[0-9]+)?) total bytes ([0-9]+)',text)
          if int(size)==a.long_size]
      m=max(vals) if vals else None
      rows.append(f'{name},{m if m is not None else "NA"}')
    (out/'summary.csv').write_text('scheme,job_ct_us\n'+'\n'.join(rows)+'\n'); print(out/'summary.csv')
if __name__=='__main__': main()
