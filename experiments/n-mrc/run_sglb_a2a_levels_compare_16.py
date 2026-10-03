#!/usr/bin/env python3
import concurrent.futures, json, shutil
from pathlib import Path
from types import SimpleNamespace
import run_sglb_min_choices_64path_scan_256 as base

OUT = base.SCRIPT_DIR / 'output/sglb_a2a_levels_compare_16'
WORKLOAD = 'periodic_background_a2a_p16_256mib'
SEEDS = (13, 29, 47)
VARIANTS = (('best8', 8, 'whole_grade_min'), ('best16', 16, 'whole_grade_min'),
            ('min20', 4, 'whole_grade_min'), ('exact24', 4, 'exact_min'))

def main():
    OUT.mkdir(parents=True, exist_ok=True)
    sim = OUT / 'htsim_roce'; shutil.copy2(base.DEFAULT_SIM, sim)
    sha = base.metrics.file_sha256(sim); args=SimpleNamespace(force=False, timeout=7200)
    def config_ok(text, spec, code):
        return code == 0 and 'SGLB effective config:' in text and all(x in text for x in (
            'GCN update 15us, aging 30us,', 'candidate policy'))
    base._config_ok = config_ok
    specs=[]
    for asym in (False, True):
      for seed in SEEDS:
       traffic,n,flows=base.materialize_traffic(OUT,WORKLOAD,seed)
       for name,levels,policy in VARIANTS:
        d=OUT/'runs'/('asym' if asym else 'symmetric')/f'{name}_seed{seed}'
        cmd=list(base.build_command(sim,traffic,d/'logout.dat',n,seed,WORKLOAD,1))
        cmd += ['-sglb_candidate_policy',policy,'-sglb_nmrc_levels',str(levels)]
        if name in ('min20','exact24'):
            cmd[cmd.index('-sglb_min_choices')+1] = '20' if name=='min20' else '24'
        if asym:
            cmd += ['-slow_tor_uplinks','16','-slow_tor_uplink_divisor','2','-slow_tor_uplink_select','random-sparse']
        specs.append((asym,name,base.CellSpec(WORKLOAD,seed,levels,traffic,base.metrics.file_sha256(traffic),n,flows,d,d/'logout.dat',tuple(cmd))))
    base.atomic_write_text(OUT/'manifest.json',json.dumps(dict(total=len(specs),seeds=SEEDS,variants=VARIANTS,asymmetric_slow_uplinks=16,simulator_sha256=sha),indent=2))
    def one(item):
      asym,name,s=item; row=base.run_cell(s,args,sha); row.update(asymmetric=asym,variant=name); return row
    rows=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
      for row in ex.map(one,specs):
        rows.append(row); base.write_csv(OUT/'cells.csv',rows)
        base.atomic_write_text(OUT/'progress.json',json.dumps(dict(completed=len(rows),total=len(specs))))
    return 0
if __name__=='__main__': raise SystemExit(main())
