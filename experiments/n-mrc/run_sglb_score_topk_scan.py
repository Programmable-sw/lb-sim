#!/usr/bin/env python3
"""Score-only top-K and graded controls, with resumable per-cell evidence."""
import argparse
import concurrent.futures
import dataclasses
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

import run_sglb_min_choices_64path_scan_256 as base

KS = (1, 2, 4, 8, 16, 24, 32, 48, 64)

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--out', type=Path, default=base.SCRIPT_DIR / 'output/sglb_score_topk_final_20260913')
    p.add_argument('--sim', type=Path, default=base.DEFAULT_SIM)
    p.add_argument('--workers', type=int, default=2)
    p.add_argument('--timeout', type=int, default=7200)
    p.add_argument('--ks', default=','.join(map(str, KS)))
    p.add_argument('--seeds', default='13,29,47')
    p.add_argument('--workloads', default=','.join(base.WORKLOADS))
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--force', action='store_true')
    args = p.parse_args()
    ks = tuple(int(x) for x in args.ks.split(','))
    seeds = tuple(int(x) for x in args.seeds.split(','))
    workloads = args.workloads.split(',')
    if not ks or any(k < 1 or k > 64 for k in ks) or len(set(ks)) != len(ks):
        p.error('K must be distinct integers in [1,64]')
    if args.workers < 1 or any(w not in base.WORKLOADS for w in workloads):
        p.error('invalid workers or workload')
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=True)
    # Freeze the tested executable so concurrent rebuilds cannot change a matrix.
    source_sim = args.sim.resolve()
    sha = base.metrics.file_sha256(source_sim)
    sim = args.out / 'binaries' / sha / 'htsim_roce'
    sim.parent.mkdir(parents=True, exist_ok=True)
    if not sim.exists():
        shutil.copy2(source_sim, sim)
    assert base.metrics.file_sha256(sim) == sha
    variants = [('score_topk', k) for k in ks]
    variants += [('whole_grade_min', 20), ('exact_min', 24),
                 ('whole_grade_min', 1)]
    specs = []
    for w in workloads:
        for seed in seeds:
            traffic, connections, flows = base.materialize_traffic(args.out, w, seed)
            digest = base.metrics.file_sha256(traffic)
            for policy, k in variants:
                directory = args.out / 'runs' / w / f'seed{seed}_{policy}_{k}'
                output = directory / 'logout.dat'
                cmd = list(base.build_command(sim, traffic, output, connections, seed, w, k))
                cmd += ['-sglb_topk', str(k)] if policy == 'score_topk' else ['-sglb_candidate_policy', policy]
                spec = base.CellSpec(w, seed, k, traffic, digest, connections, flows,
                                     directory, output, tuple(cmd))
                specs.append((policy, spec))
    base.write_csv(args.out / 'commands.tsv', [dict(policy=policy, **r)
                   for policy, spec in specs for r in base._commands_rows([spec])])
    manifest = dict(simulator=str(sim), sha256=sha, cells=len(specs), ks=ks,
                    seeds=seeds, workloads=workloads,
                    baselines=['whole_grade_min20', 'exact_min24', 'whole_grade_min1'],
                    source_revision=subprocess.check_output(['git','rev-parse','HEAD'], cwd=base.ROOT, text=True).strip())
    base.atomic_write_text(args.out / 'manifest.json', json.dumps(manifest, indent=2)+'\n')
    base.atomic_write_text(args.out / 'source.patch', subprocess.check_output(
        ['git','diff','--','sim/datacenter'], cwd=base.ROOT, text=True))
    shutil.copy2(base.ROOT / 'sim/datacenter/sglb_score_topk.h', sim.parent / 'sglb_score_topk.h')
    if args.dry_run:
        print(f'validated {len(specs)} cells in {args.out}', flush=True)
        return 0
    rows, errors = [], []
    started = time.monotonic()
    def run(policy, spec):
        row = base.run_cell(spec, args, sha)
        # Cached rows are fingerprinted by full command + traffic + binary hash.
        import gzip
        with gzip.open(spec.case_dir / 'stdout.log.gz', 'rt') as stream:
            log = stream.read()
        if f'candidate policy {policy}' not in log:
            raise ValueError(f'wrong effective policy: {policy}')
        if policy == 'score_topk' and f'continuous topk {spec.min_choices},' not in log:
            raise ValueError('wrong effective K')
        row.update(policy=policy, k=spec.min_choices if policy == 'score_topk' else '',
                   case_dir=str(spec.case_dir))
        if not base._valid_row(row):
            raise ValueError(f'incomplete or invalid cell: {spec.case_dir}')
        return row
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run, policy, spec): (policy, spec) for policy, spec in specs}
        for f in concurrent.futures.as_completed(futures):
            policy, spec = futures[f]
            try:
                rows.append(f.result())
            except Exception as exc:
                errors.append(f'{policy}/{spec.workload}/{spec.seed}/{spec.min_choices}: {exc}')
            base.write_csv(args.out / 'cells.csv', sorted(rows, key=lambda r:(r['workload'],r['policy'],r['seed'],r['min_choices'])))
            base.atomic_write_text(args.out / 'progress.json', json.dumps(dict(
                complete=len(rows), errors=errors, total=len(specs), elapsed_s=time.monotonic()-started), indent=2)+'\n')
    summaries = []
    for w in workloads:
        metric = 'cct_us' if w == base.WORKLOADS[3] else 'p99_fct_us'
        for policy, k in variants:
            group = [r for r in rows if r['workload']==w and r['policy']==policy and r['min_choices']==k]
            if len(group) == len(seeds):
                summaries.append(dict(workload=w, policy=policy, choice=k, metric=metric,
                                      geometric_mean=base.geometric_mean([r[metric] for r in group])))
    base.write_csv(args.out / 'summary.csv', summaries)
    print(f'finished {len(rows)}/{len(specs)} valid cells; errors={len(errors)}', flush=True)
    return 1 if errors else 0

if __name__ == '__main__':
    sys.exit(main())
