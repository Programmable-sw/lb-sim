"""Two-factor passive 4/8-level pilot; not the five-factor active experiment."""
from pathlib import Path
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor
import json
import shutil
import run_sglb_min_choices_64path_scan_256 as base

out = base.SCRIPT_DIR / 'output/sglb_replica_levels_pilot'
out.mkdir(parents=True, exist_ok=True)
sha = base.metrics.file_sha256(base.DEFAULT_SIM)
sim = out / 'binaries' / sha / 'htsim_roce'
sim.parent.mkdir(parents=True, exist_ok=True)
if not sim.exists():
    shutil.copy2(base.DEFAULT_SIM, sim)
args = SimpleNamespace(force=False, timeout=7200)
specs = []
workload = 'fixed_hotspot_permutation_16mib'
for seed in (13, 29, 47):
    traffic, n, flows = base.materialize_traffic(out, workload, seed)
    for levels in (4, 8):
        d = out / f'seed{seed}_levels{levels}'
        cmd = list(base.build_command(sim, traffic, d/'logout.dat', n, seed, workload, 1))
        cmd += ['-sglb_candidate_policy', 'whole_grade_min', '-sglb_nmrc_levels', str(levels)]
        specs.append((levels, base.CellSpec(workload, seed, 1, traffic,
            base.metrics.file_sha256(traffic), n, flows, d, d/'logout.dat', tuple(cmd))))
# Parent parser hard-codes four levels. Check the actual requested level instead.
def config_ok(text, spec, code):
    level = spec.command[spec.command.index('-sglb_nmrc_levels')+1]
    return code == 0 and all(s in text for s in (
        'OFAT factor real_gcn_raw_linear', 'min choices 1,',
        'candidate policy whole_grade_min', f'nmrc_levels {level},',
        'local quality update 1us, GCN update 15us, aging 30us,'))
base._config_ok = config_ok
base.atomic_write_text(out/'manifest.json', json.dumps(dict(
    factors=2, refresh='passive', levels=[4,8], weights=[0.5,0.5],
    selection='best_grade_only', workload=workload, seeds=[13,29,47],
    simulator_sha256=sha, note='Existing thresholds; no tuning or superiority claim yet'),indent=2))
def run(item):
    levels, spec = item
    row = base.run_cell(spec, args, sha)
    row['levels'] = levels
    return row
with ThreadPoolExecutor(max_workers=2) as pool:
    rows = []
    for row in pool.map(run, specs):
        rows.append(row)
        base.write_csv(out/'cells.csv', rows)
        base.atomic_write_text(out/'progress.json', json.dumps(dict(
            completed=len(rows), total=6, valid=sum(base._valid_row(x) for x in rows))))
