#!/usr/bin/env python3
"""Compare graded n-MRC reroute/cooldown policies on A2A and WebSearch."""

import argparse
import concurrent.futures
import gzip
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import time
from typing import NamedTuple

import run_final_512_comparison as base


ROOT = Path(__file__).resolve().parents[2]
BASELINE = ROOT / "experiments/n-mrc/output/n-mrc-results"
DEFAULT_OUT = (
    ROOT / "experiments/n-mrc/output/"
    "nmrc_routing_cooldown_seed13_20260726")
NODES = 128


class Variant(NamedTuple):
    name: str
    lb: str
    args: tuple
    required_tokens: tuple


class MatrixCase(NamedTuple):
    scenario: base.Scenario
    variant: Variant
    seed: int


GRADED_COMMON = (
    "-nmrc_reroute_policy", "better_ge3",
    "-nmrc_fastcnp", "on",
)
VARIANTS = (
    Variant(
        "graded_selective", "n-mrc",
        GRADED_COMMON + (
            "-nmrc_graded_cooldown", "selective",
            "-nmrc_graded_reroute_delta", "0",
        ),
        (
            "reroute_policy=better_ge3 fastcnp=on",
            "graded_cooldown=selective graded_reroute_delta=0",
        ),
    ),
    Variant(
        "graded_full", "n-mrc",
        GRADED_COMMON + (
            "-nmrc_graded_cooldown", "full",
            "-nmrc_graded_reroute_delta", "0",
        ),
        (
            "reroute_policy=better_ge3 fastcnp=on",
            "graded_cooldown=full graded_reroute_delta=0",
        ),
    ),
    Variant(
        "graded_none", "n-mrc",
        GRADED_COMMON + (
            "-nmrc_graded_cooldown", "none",
            "-nmrc_graded_reroute_delta", "0",
        ),
        (
            "reroute_policy=better_ge3 fastcnp=on",
            "graded_cooldown=none graded_reroute_delta=0",
        ),
    ),
    Variant(
        "graded_delta025", "n-mrc",
        GRADED_COMMON + (
            "-nmrc_graded_cooldown", "full",
            "-nmrc_graded_reroute_delta", "0.25",
        ),
        (
            "reroute_policy=better_ge3 fastcnp=on",
            "graded_cooldown=full graded_reroute_delta=0.25",
        ),
    ),
    Variant(
        "fixed05", "n-mrc-fixed0.5",
        (
            "-nmrc_fastcnp", "on",
            "-nmrc_absolute_threshold", "0.5",
            "-nmrc_relative_delta", "0.25",
        ),
        (
            "preset=n-mrc-fixed0.5",
            "absolute_threshold=0.5 relative_delta=0.25",
        ),
    ),
    Variant(
        "delta025", "n-mrc-delta",
        (
            "-nmrc_fastcnp", "on",
            "-nmrc_relative_delta", "0.25",
        ),
        (
            "preset=n-mrc-delta",
            "relative_delta=0.25",
        ),
    ),
)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=13)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--sim", type=Path, default=base.DEFAULT_SIM)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--accept-valid-cache", action="store_true",
        help=(
            "Reuse complete validated summaries even when the binary "
            "fingerprint changed because an equivalent pinned build uses "
            "a different absolute debug path."))
    parser.add_argument("--list-only", action="store_true")
    return parser.parse_args()


def selected_scenarios():
    return tuple(
        scenario for scenario in base.scenario_catalog(NODES)
        if (
            scenario.kind == "all_to_all" and
            scenario.family in (
                "healthy_alltoall",
                "asymmetric_alltoall_background",
            )
        ) or (
            scenario.traffic == "websearch" and
            scenario.websearch_load in (40, 60, 80, 100)
        )
    )


def materialize_verified_traffic(out, scenarios, seed):
    artifacts = {}
    for scenario in scenarios:
        artifact = base.materialize_traffic(
            out / "traffic", scenario, seed, NODES)
        archived_path = (
            BASELINE / "traffic" /
            f"n{NODES}_{scenario.name}_seed{seed}.cm")
        if not archived_path.exists():
            raise FileNotFoundError(
                f"missing archived traffic: {archived_path}")
        archived_sha = base.file_sha256(archived_path)
        if artifact.sha256 != archived_sha:
            raise ValueError(
                f"traffic hash mismatch for {scenario.name}: "
                f"generated={artifact.sha256} archived={archived_sha}")
        artifacts[scenario.name] = artifact
    return artifacts


def build_command(case, artifact, sim, case_dir):
    spec = base.CaseSpec(case.scenario, "n-mrc", case.seed)
    command = base.build_command(
        spec, sim, artifact.path, case_dir, NODES, artifact.connections)
    lb_index = command.index("-lb") + 1
    command[lb_index] = case.variant.lb
    del command[-len(base.SCHEME_ARGS["n-mrc"]):]
    command.extend(case.variant.args)
    return command


def config_ok(text, case, returncode):
    required = (
        f"lb mode {case.variant.lb}",
        "cc mode dctcp_variant",
        "RoCE receive mode sp",
        "RoCE SACK bitmap 64 bits",
        "RoCE TRIM recovery mode exact",
        "RoceTransportConfig semantics=mrc_exact_bounded",
        "HybridNmrcConfig ev_mode=encoded ",
    ) + case.variant.required_tokens
    condition_required = []
    if case.scenario.background:
        condition_required.append("SGLB fixed-link background enabled")
    if case.scenario.degraded:
        condition_required.append(
            "Slow ToR-to-agg uplinks "
            f"{base.common.slow_uplink_count(base.common.TOPOLOGIES[NODES])}")
    return int(
        returncode == 0 and
        all(token in text for token in required + tuple(condition_required))
    )


def write_gzip(path, text):
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=6) as handle:
        handle.write(text)


def run_case(case, artifact, args):
    case_dir = (
        args.out / "raw" / case.scenario.name / case.variant.name /
        f"seed_{case.seed}")
    case_dir.mkdir(parents=True, exist_ok=True)
    command = build_command(case, artifact, args.sim, case_dir)
    command_text = shlex.join(command)
    fingerprint = hashlib.sha256((
        base.file_sha256(args.sim) + "\0" + artifact.sha256 + "\0" +
        command_text
    ).encode()).hexdigest()
    summary_path = case_dir / "summary.json"
    if not args.force and summary_path.exists():
        cached = json.loads(summary_path.read_text(encoding="utf-8"))
        if (
                (
                    cached.get("fingerprint") == fingerprint or
                    args.accept_valid_cache
                ) and
                cached.get("returncode") == 0 and cached.get("config_ok") and
                cached.get("all_flows_completed")):
            print(
                f"cached {case.scenario.name} {case.variant.name}",
                flush=True)
            return cached

    print(f"run {case.scenario.name} {case.variant.name}", flush=True)
    started = time.monotonic()
    try:
        process = subprocess.run(
            command, cwd=case_dir, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, timeout=args.timeout,
            check=False)
        returncode = process.returncode
        stdout = process.stdout
    except subprocess.TimeoutExpired as error:
        returncode = 124
        stdout = error.stdout or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode(errors="replace")
        stdout += f"\nTIMEOUT after {args.timeout}s\n"
    runtime_s = time.monotonic() - started

    write_gzip(case_dir / "stdout.log.gz", stdout)
    (case_dir / "command.txt").write_text(
        command_text + "\n", encoding="utf-8")
    parsed = base.parse_completions(
        stdout, artifact.starts_by_flowid, artifact.connections, False)
    _, diagnostics = base.parse_diagnostics(stdout)
    metric_name, primary_us = base.primary_metric(case.scenario, parsed)
    row = {
        "fingerprint": fingerprint,
        "nodes": NODES,
        "paths": base.common.TOPOLOGIES[NODES].paths,
        "scenario": case.scenario.name,
        "family": case.scenario.family,
        "kind": case.scenario.kind,
        "traffic": case.scenario.traffic,
        "size_mib": case.scenario.size_mib,
        "websearch_load_pct": case.scenario.websearch_load,
        "parallel": case.scenario.parallel,
        "degraded": int(case.scenario.degraded),
        "periodic_background": int(case.scenario.background),
        "variant": case.variant.name,
        "lb": case.variant.lb,
        "seed": case.seed,
        "primary_metric": metric_name,
        "primary_us": primary_us,
        "returncode": returncode,
        "config_ok": config_ok(stdout, case, returncode),
        "runtime_s": runtime_s,
        "traffic_sha256": artifact.sha256,
        "traffic_bytes": artifact.serialized_bytes,
        "stdout_gz": str(case_dir / "stdout.log.gz"),
        "command": command_text,
        "diagnostics_json": json.dumps(diagnostics, sort_keys=True),
        **parsed,
    }
    summary_path.write_text(
        json.dumps(row, indent=2, sort_keys=True) + "\n",
        encoding="utf-8")
    for disposable in (case_dir / "idmap.txt", case_dir / "logout.dat"):
        disposable.unlink(missing_ok=True)
    print(
        f"done {case.scenario.name} {case.variant.name} "
        f"{metric_name}={primary_us:.3f}us "
        f"flows={parsed['completed']}/{artifact.connections} "
        f"runtime={runtime_s:.1f}s",
        flush=True)
    return row


def validate(rows, cases):
    expected = {
        (case.scenario.name, case.variant.name, case.seed) for case in cases}
    actual = {
        (row["scenario"], row["variant"], row["seed"]) for row in rows}
    if actual != expected or len(rows) != len(expected):
        raise ValueError(
            f"result matrix mismatch: missing={expected - actual}, "
            f"extra={actual - expected}")
    bad = [
        row for row in rows
        if row["returncode"] != 0 or not row["config_ok"] or
        not row["all_flows_completed"] or row["primary_us"] <= 0]
    if bad:
        raise ValueError(
            "invalid cells: " + ", ".join(
                f"{row['scenario']}:{row['variant']}" for row in bad))


def main():
    args = parse_args()
    args.out = args.out.resolve()
    args.sim = args.sim.resolve()
    if (
            args.seed not in base.CANONICAL_SEEDS or
            args.workers < 1 or args.timeout < 1):
        raise ValueError("unsupported seed or non-positive worker/timeout")
    args.out.mkdir(parents=True, exist_ok=True)
    output_lock = base.acquire_output_lock(args.out)
    try:
        scenarios = selected_scenarios()
        cases = tuple(
            MatrixCase(scenario, variant, args.seed)
            for scenario in scenarios for variant in VARIANTS)
        artifacts = materialize_verified_traffic(
            args.out, scenarios, args.seed)
        manifest = {
            "nodes": NODES,
            "paths": base.common.TOPOLOGIES[NODES].paths,
            "seed": args.seed,
            "scenario_count": len(scenarios),
            "variant_count": len(VARIANTS),
            "case_count": len(cases),
            "baseline_traffic": str(BASELINE / "traffic"),
            "sim": str(args.sim),
            "sim_sha256": base.file_sha256(args.sim),
            "accept_valid_cache": args.accept_valid_cache,
            "variants": [
                {
                    "name": variant.name,
                    "lb": variant.lb,
                    "args": list(variant.args),
                }
                for variant in VARIANTS
            ],
            "scenarios": [scenario.name for scenario in scenarios],
        }
        (args.out / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8")
        if len(cases) != 156:
            raise ValueError(f"expected 156 cases, got {len(cases)}")
        if args.list_only:
            print(
                f"validated manifest: {len(scenarios)} scenarios x "
                f"{len(VARIANTS)} variants = {len(cases)} cases")
            return 0

        rows = []
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=min(args.workers, len(cases))) as executor:
            futures = [
                executor.submit(
                    run_case, case, artifacts[case.scenario.name], args)
                for case in cases]
            for future in concurrent.futures.as_completed(futures):
                rows.append(future.result())
                base.write_csv(args.out / "results.partial.csv", rows)

        validate(rows, cases)
        rows.sort(key=lambda row: (
            row["scenario"], row["variant"], row["seed"]))
        base.write_csv(args.out / "results.csv", rows)
        (args.out / "COMPLETE").write_text(
            f"{len(rows)} validated cells\n", encoding="ascii")
        print(
            f"validated {len(rows)} cells; "
            f"results={args.out / 'results.csv'}")
        return 0
    finally:
        output_lock.close()


if __name__ == "__main__":
    raise SystemExit(main())
