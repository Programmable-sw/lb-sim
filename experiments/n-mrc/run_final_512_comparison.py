#!/usr/bin/env python3
"""Run the approved final OPS/REPS/MRC/SGLB/n-MRC comparison matrix.

The runner attempts one 512-node resource gate.  If that gate cannot finish,
the entire experiment is rebuilt at 256 nodes; result sets never mix scales.
"""

import argparse
import concurrent.futures
import csv
import fcntl
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
from typing import NamedTuple


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

import feedback_eval_common as common  # noqa: E402
import experiment_metrics as metrics  # noqa: E402


CANONICAL_SEEDS = (13, 29, 47)
TEST_SCHEMES = ("ops", "reps", "mrc", "sglb", "n-mrc")
HEALTHY_SCHEMES = ("ecmp",) + TEST_SCHEMES
LB_NAMES = {scheme: scheme for scheme in HEALTHY_SCHEMES}
SCHEME_ARGS = {
    "ecmp": (),
    "ops": (),
    "reps": ("-reps_buffer", "8"),
    "mrc": (),
    "sglb": (),
    "n-mrc": (
        "-nmrc_reroute_policy", "better_ge3",
        "-nmrc_fastcnp", "on",
    ),
}
DEFAULT_SIM = ROOT / "sim/datacenter/htsim_roce"
DEFAULT_OUT = ROOT / "experiments/n-mrc/output/n-mrc-results"
P2P_END_US = 10000
WEBSEARCH_DURATION_US = 10000
WEBSEARCH_END_US = 40000
ALLTOALL_END_US = 100000

COMPLETION_RE = re.compile(
    r"^\.*Flow Roce_(\d+)_(\d+)\s+(\d+) finished at ([0-9.]+) "
    r"total bytes (\d+) bg traffic ([01]) flowid (\d+)$",
    re.MULTILINE,
)
DIAG_LINE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9]*Diag\b.*$", re.MULTILINE)


class Scenario(NamedTuple):
    name: str
    family: str
    kind: str
    traffic: str
    size_mib: int = 0
    websearch_load: int = 0
    parallel: int = 0
    degraded: bool = False
    background: bool = False
    mixed: bool = False


class CaseSpec(NamedTuple):
    scenario: Scenario
    scheme: str
    seed: int


class TrafficArtifact(NamedTuple):
    path: Path
    connections: int
    starts_by_flowid: dict
    sha256: str
    serialized_bytes: int


def scenario_catalog(nodes):
    if nodes not in (256, 512):
        raise ValueError("final comparison scale must be 256 or 512")
    scenarios = []
    for family, degraded in (
            ("healthy_p2p", False), ("asymmetric_p2p", True)):
        for traffic in ("permutation", "tornado"):
            for size_mib in (4, 8, 16):
                scenarios.append(Scenario(
                    f"{family}_{traffic}_{size_mib}mib", family,
                    "point_to_point", traffic, size_mib=size_mib,
                    degraded=degraded,
                ))
        for load in (40, 60, 80, 100):
            scenarios.append(Scenario(
                f"{family}_websearch_{load}pct", family,
                "point_to_point", "websearch", websearch_load=load,
                degraded=degraded,
            ))

    for family, degraded, background in (
            ("healthy_alltoall", False, False),
            ("asymmetric_alltoall_background", True, True)):
        for size_mib in (64, 256, 1024):
            for parallel in (4, 8, 16):
                scenarios.append(Scenario(
                    f"{family}_{size_mib}mib_p{parallel}", family,
                    "all_to_all", "alltoall", size_mib=size_mib,
                    parallel=parallel, degraded=degraded,
                    background=background,
                ))

    for traffic in ("permutation", "tornado"):
        for size_mib in (4, 8, 16):
            scenarios.append(Scenario(
                f"mixed_deployment_{traffic}_{size_mib}mib",
                "mixed_deployment", "point_to_point", traffic,
                size_mib=size_mib, mixed=True,
            ))
    return tuple(scenarios)


def make_case_specs(nodes, seeds):
    specs = []
    for scenario in scenario_catalog(nodes):
        schemes = (
            HEALTHY_SCHEMES
            if scenario.family == "healthy_p2p" else TEST_SCHEMES
        )
        for seed in seeds:
            specs.extend(CaseSpec(scenario, scheme, seed) for scheme in schemes)
    return tuple(specs)


def choose_scale(gate_succeeded):
    return 512 if gate_succeeded else 256


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def traffic_key(scenario, seed, nodes):
    return f"n{nodes}_{scenario.name}_seed{seed}"


def materialize_traffic(traffic_dir, scenario, seed, nodes):
    topology = common.TOPOLOGIES[nodes]
    traffic_dir = Path(traffic_dir)
    traffic_dir.mkdir(parents=True, exist_ok=True)
    path = traffic_dir / f"{traffic_key(scenario, seed, nodes)}.cm"
    starts = {}

    if scenario.traffic == "permutation":
        flows = common.permutation_flows(
            topology, seed, scenario.size_mib * common.MIB)
        common.write_traffic_matrix(path, topology, flows)
        starts = {index: flow.start_us for index, flow in enumerate(flows, 1)}
        connections = len(flows)
    elif scenario.traffic == "tornado":
        flows = common.tornado_flows(
            topology, scenario.size_mib * common.MIB)
        common.write_traffic_matrix(path, topology, flows)
        starts = {index: flow.start_us for index, flow in enumerate(flows, 1)}
        connections = len(flows)
    elif scenario.traffic == "websearch":
        flows = common.websearch_flows(
            topology, seed, scenario.websearch_load / 100.0,
            WEBSEARCH_DURATION_US)
        common.write_traffic_matrix(path, topology, flows)
        starts = {index: flow.start_us for index, flow in enumerate(flows, 1)}
        connections = len(flows)
    elif scenario.traffic == "alltoall":
        message_bytes = scenario.size_mib * common.MIB
        plan = common.alltoall_plan(
            nodes, nodes, scenario.parallel, message_bytes // nodes, 0, seed)
        manifest = common.write_alltoall_matrix(path, plan)
        connections = manifest.connections
    else:
        raise ValueError(f"unsupported traffic {scenario.traffic}")

    return TrafficArtifact(
        path=path,
        connections=connections,
        starts_by_flowid=starts,
        sha256=file_sha256(path),
        serialized_bytes=path.stat().st_size,
    )


def scenario_end_us(scenario):
    if scenario.traffic == "websearch":
        return WEBSEARCH_END_US
    if scenario.kind == "all_to_all":
        return ALLTOALL_END_US
    return P2P_END_US


def scenario_args(scenario, nodes):
    result = []
    if scenario.degraded:
        result.extend((
            "-slow_tor_uplinks", str(common.slow_uplink_count(
                common.TOPOLOGIES[nodes])),
            "-slow_tor_uplink_divisor", "2",
            "-slow_tor_uplink_select", "random-sparse",
        ))
    if scenario.background:
        result.extend(common.alltoall_background_args(
            scenario.size_mib * common.MIB, True))
    if scenario.mixed:
        result.append("-mixed_lb_traffic")
    return result


def build_command(spec, sim, traffic_file, case_dir, nodes,
                  connections=None):
    topology = common.TOPOLOGIES[nodes]
    if connections is None:
        if spec.scenario.kind == "all_to_all":
            connections = nodes * (nodes - 1)
        elif spec.scenario.traffic in ("permutation", "tornado"):
            connections = nodes
        else:
            connections = 1
    command = [
        str(sim),
        "-o", str(Path(case_dir) / "logout.dat"),
        "-tm", str(traffic_file),
        "-nodes", str(nodes),
        "-conns", str(connections),
        "-tiers", "2",
        "-lb", LB_NAMES[spec.scheme],
        "-linkspeed", "400000",
        "-queue_type", "composite_ecn_lb",
        "-host_queue_type", "prio",
        "-mtu", "4096",
        "-end", str(scenario_end_us(spec.scenario)),
        "-paths", str(topology.paths),
        "-seed", str(spec.seed),
        "-cc", "dctcp_variant",
        "-roce_rx_mode", "sp",
        "-roce_sack_bitmap_bits", "64",
        "-roce_transport_semantics", "mrc_exact_bounded",
        "-roce_trim_recovery", "exact",
        "-hop_latency", "0.5",
        "-switch_latency", "0.5",
        "-queue_cv_sample_us", "100",
    ]
    command.extend(scenario_args(spec.scenario, nodes))
    command.extend(SCHEME_ARGS[spec.scheme])
    return command


def parse_completions(text, starts_by_flowid, expected_flows, mixed):
    seen = set()
    all_fct = []
    main_fct = []
    background_fct = []
    for match in COMPLETION_RE.finditer(text):
        flow_id = int(match.group(7))
        if flow_id in seen:
            continue
        seen.add(flow_id)
        finish_us = float(match.group(4))
        fct_us = finish_us - starts_by_flowid.get(flow_id, 0.0)
        all_fct.append(fct_us)
        if match.group(6) == "1":
            background_fct.append(fct_us)
        else:
            main_fct.append(fct_us)

    def value(values, quantile):
        return metrics.percentile(values, quantile) if values else 0.0

    return {
        "completed": len(seen),
        "unique_completed": len(seen),
        "expected_flows": expected_flows,
        "all_flows_completed": int(len(seen) == expected_flows),
        "mean_fct_us": sum(all_fct) / len(all_fct) if all_fct else 0.0,
        "p50_fct_us": value(all_fct, 0.50),
        "p95_fct_us": value(all_fct, 0.95),
        "p99_fct_us": value(all_fct, 0.99),
        "p999_fct_us": value(all_fct, 0.999),
        "max_fct_us": max(all_fct) if all_fct else 0.0,
        "main_completed": len(main_fct),
        "main_mean_fct_us": (
            sum(main_fct) / len(main_fct) if main_fct else 0.0),
        "main_p99_fct_us": value(main_fct, 0.99),
        "main_max_fct_us": max(main_fct) if main_fct else 0.0,
        "ecmp_background_completed": len(background_fct),
        "ecmp_background_mean_fct_us": (
            sum(background_fct) / len(background_fct)
            if background_fct else 0.0),
        "ecmp_background_p99_fct_us": value(background_fct, 0.99),
        "ecmp_background_max_fct_us": (
            max(background_fct) if background_fct else 0.0),
        "mixed_labels_ok": int(
            not mixed or (
                len(background_fct) == (expected_flows + 9) // 10 and
                len(main_fct) == expected_flows - (expected_flows + 9) // 10
            )
        ),
    }


def parse_diagnostics(text):
    lines = DIAG_LINE_RE.findall(text)
    flattened = {}
    for line in lines:
        prefix = line.split(None, 1)[0]
        for key, raw in metrics.KEY_VALUE_RE.findall(line):
            value = (
                float(raw) if any(marker in raw for marker in ".eE")
                else int(raw)
            )
            flattened[f"{prefix}.{key}"] = value
    return lines, flattened


def config_ok(text, spec, returncode, nodes):
    required = (
        f"lb mode {LB_NAMES[spec.scheme]}",
        "cc mode dctcp_variant",
        "RoCE receive mode sp",
        "RoCE SACK bitmap 64 bits",
        "RoCE TRIM recovery mode exact",
        "RoceTransportConfig semantics=mrc_exact_bounded",
    )
    scheme_required = {
        "ecmp": (),
        "ops": (),
        "reps": ("reps buffer size 8",),
        "mrc": ("MrcPolicyDiag policy=skip_once",),
        "sglb": ("SglbRouteDiag ",),
        "n-mrc": (
            "HybridNmrcConfig ev_mode=encoded ",
            "reroute_policy=better_ge3 fastcnp=on",
        ),
    }[spec.scheme]
    condition_required = []
    if spec.scenario.mixed:
        condition_required.append("MixedLbDiag enabled=on")
    if spec.scenario.background:
        condition_required.append("SGLB fixed-link background enabled")
    if spec.scenario.degraded:
        condition_required.append(
            "Slow ToR-to-agg uplinks "
            f"{common.slow_uplink_count(common.TOPOLOGIES[nodes])}")
    return int(
        returncode == 0 and
        all(token in text for token in required + scheme_required + tuple(condition_required))
    )


def primary_metric(scenario, parsed):
    if scenario.kind == "all_to_all":
        return "all_to_all_cct_us", parsed["max_fct_us"]
    if scenario.mixed:
        return "main_max_fct_us", parsed["main_max_fct_us"]
    return "p99_fct_us", parsed["p99_fct_us"]


def write_csv(path, rows):
    rows = list(rows)
    if not rows:
        return
    fields = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with Path(path).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def acquire_output_lock(out):
    """Hold an advisory lock so two runners cannot corrupt one result tree."""
    lock_path = Path(out) / ".runner.lock"
    handle = lock_path.open("a+", encoding="ascii")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise RuntimeError(f"another runner owns {lock_path}") from None
    handle.seek(0)
    handle.truncate()
    handle.write(f"pid={os.getpid()}\n")
    handle.flush()
    return handle


def _write_gzip(path, text):
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=6) as handle:
        handle.write(text)


def run_case(spec, artifact, args, nodes):
    case_dir = (
        Path(args.out) / "raw" / spec.scenario.name / spec.scheme /
        f"seed_{spec.seed}"
    )
    case_dir.mkdir(parents=True, exist_ok=True)
    command = build_command(
        spec, args.sim, artifact.path, case_dir, nodes, artifact.connections)
    command_text = shlex.join(command)
    fingerprint = hashlib.sha256((
        file_sha256(args.sim) + "\0" + artifact.sha256 + "\0" + command_text
    ).encode()).hexdigest()
    summary_path = case_dir / "summary.json"
    if not args.force and summary_path.exists():
        cached = json.loads(summary_path.read_text(encoding="utf-8"))
        if (
                cached.get("fingerprint") == fingerprint and
                cached.get("returncode") == 0 and cached.get("config_ok") and
                cached.get("all_flows_completed") and
                cached.get("mixed_labels_ok")):
            print(
                f"cached {spec.scenario.name} {spec.scheme} seed={spec.seed}",
                flush=True,
            )
            return cached

    print(
        f"run {spec.scenario.name} {spec.scheme} seed={spec.seed}",
        flush=True,
    )
    started = time.monotonic()
    try:
        process = subprocess.run(
            command, cwd=case_dir, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, timeout=args.timeout,
            check=False,
        )
        returncode = process.returncode
        stdout = process.stdout
    except subprocess.TimeoutExpired as error:
        returncode = 124
        partial = error.stdout or ""
        if isinstance(partial, bytes):
            partial = partial.decode(errors="replace")
        stdout = partial + f"\nTIMEOUT after {args.timeout}s\n"
    runtime_s = time.monotonic() - started
    _write_gzip(case_dir / "stdout.log.gz", stdout)
    (case_dir / "command.txt").write_text(command_text + "\n", encoding="utf-8")

    parsed = parse_completions(
        stdout, artifact.starts_by_flowid, artifact.connections,
        spec.scenario.mixed)
    diag_lines, diag_values = parse_diagnostics(stdout)
    metric_name, primary_us = primary_metric(spec.scenario, parsed)
    row = {
        "fingerprint": fingerprint,
        "nodes": nodes,
        "paths": common.TOPOLOGIES[nodes].paths,
        "scenario": spec.scenario.name,
        "family": spec.scenario.family,
        "kind": spec.scenario.kind,
        "traffic": spec.scenario.traffic,
        "size_mib": spec.scenario.size_mib,
        "websearch_load_pct": spec.scenario.websearch_load,
        "parallel": spec.scenario.parallel,
        "degraded": int(spec.scenario.degraded),
        "periodic_background": int(spec.scenario.background),
        "mixed_deployment": int(spec.scenario.mixed),
        "scheme": spec.scheme,
        "seed": spec.seed,
        "primary_metric": metric_name,
        "primary_us": primary_us,
        "returncode": returncode,
        "config_ok": config_ok(stdout, spec, returncode, nodes),
        "runtime_s": runtime_s,
        "traffic_sha256": artifact.sha256,
        "traffic_bytes": artifact.serialized_bytes,
        "stdout_gz": str(case_dir / "stdout.log.gz"),
        "command": command_text,
        "diagnostics_json": json.dumps(diag_values, sort_keys=True),
        **parsed,
    }
    summary_path.write_text(
        json.dumps(row, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for disposable in (case_dir / "idmap.txt", case_dir / "logout.dat"):
        disposable.unlink(missing_ok=True)
    print(
        f"done {spec.scenario.name} {spec.scheme} seed={spec.seed} "
        f"{metric_name}={primary_us:.3f}us "
        f"flows={parsed['completed']}/{artifact.connections} "
        f"runtime={runtime_s:.1f}s",
        flush=True,
    )
    return row


def validate_result_rows(rows, nodes, seeds):
    expected_specs = make_case_specs(nodes, seeds)
    expected = {
        (item.scenario.name, item.scheme, item.seed) for item in expected_specs
    }
    identities = [
        (row["scenario"], row["scheme"], row["seed"]) for row in rows
    ]
    if len(identities) != len(set(identities)):
        raise ValueError("duplicate result identity")
    if set(identities) != expected:
        raise ValueError(
            f"result matrix mismatch: missing={expected - set(identities)} "
            f"extra={set(identities) - expected}")
    bad = [
        row for row in rows
        if row["returncode"] != 0 or not row["config_ok"] or
        not row["all_flows_completed"] or not row["mixed_labels_ok"] or
        not math.isfinite(row["primary_us"]) or row["primary_us"] <= 0
    ]
    if bad:
        names = [
            f"{row['scenario']}:{row['scheme']}:seed{row['seed']}"
            for row in bad
        ]
        raise ValueError(f"invalid result cells: {names}")


def geometric_mean(values):
    values = tuple(values)
    if not values or any(value <= 0 for value in values):
        return 0.0
    return math.exp(sum(math.log(value) for value in values) / len(values))


def aggregate_rows(rows):
    cells = {}
    for row in rows:
        key = (row["scenario"], row["scheme"])
        cells.setdefault(key, []).append(row)
    summary = []
    for (scenario, scheme), selected in sorted(cells.items()):
        first = selected[0]
        primary = [row["primary_us"] for row in selected]
        summary.append({
            "scenario": scenario,
            "family": first["family"],
            "kind": first["kind"],
            "scheme": scheme,
            "primary_metric": first["primary_metric"],
            "primary_geomean_us": geometric_mean(primary),
            "primary_min_us": min(primary),
            "primary_max_us": max(primary),
            "seed_values_us": ";".join(
                f"{row['seed']}:{row['primary_us']:.6f}"
                for row in sorted(selected, key=lambda item: item["seed"])),
            "main_max_geomean_us": geometric_mean(
                row["main_max_fct_us"] for row in selected),
            "ecmp_background_max_geomean_us": geometric_mean(
                row["ecmp_background_max_fct_us"] for row in selected),
            "raw_cells": len(selected),
        })

    ecmp = {
        item["scenario"]: item["primary_geomean_us"]
        for item in summary if item["scheme"] == "ecmp"
    }
    for item in summary:
        baseline = ecmp.get(item["scenario"], 0.0)
        item["speedup_vs_ecmp"] = (
            baseline / item["primary_geomean_us"]
            if baseline and item["primary_geomean_us"] else 0.0)
    return summary


def build_report(rows, summary, nodes, gate):
    lines = [
        "# Final packet load-balancing comparison",
        "",
        f"Scale: {nodes} nodes / {common.TOPOLOGIES[nodes].paths} paths. "
        f"Seeds: {', '.join(map(str, CANONICAL_SEEDS))}.",
        "",
        f"512-node resource gate: {gate['status']}.",
        "",
        "Primary metrics: healthy/asymmetric P2P uses p99 FCT; "
        "All-to-All uses CCT; mixed deployment uses bg=0 maximum FCT. "
        "Every raw row also retains mean/p50/p95/p99/p99.9/max, and mixed "
        "bg=0/bg=1 statistics separately.",
        "",
        "## Three-seed results",
        "",
        "| family | scenario | scheme | metric | geomean us | min | max | ECMP speedup |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for item in summary:
        speedup = (
            f"{item['speedup_vs_ecmp']:.4f}x"
            if item["speedup_vs_ecmp"] else ""
        )
        lines.append(
            f"| {item['family']} | {item['scenario']} | {item['scheme']} | "
            f"{item['primary_metric']} | {item['primary_geomean_us']:.3f} | "
            f"{item['primary_min_us']:.3f} | {item['primary_max_us']:.3f} | "
            f"{speedup} |")
    lines += [
        "",
        "## Mixed deployment: separate maxima",
        "",
        "| scenario | scheme | main bg=0 max geomean us | ECMP bg=1 max geomean us |",
        "| --- | --- | ---: | ---: |",
    ]
    for item in summary:
        if item["family"] == "mixed_deployment":
            lines.append(
                f"| {item['scenario']} | {item['scheme']} | "
                f"{item['main_max_geomean_us']:.3f} | "
                f"{item['ecmp_background_max_geomean_us']:.3f} |")
    lines += [
        "",
        f"Validated raw cells: {len(rows)}.",
        "",
        "WebSearch uses the repository's digitized proxy CDF, not an exact "
        "machine-readable trace extracted from the REPS paper.",
    ]
    return "\n".join(lines) + "\n"


def run_resource_gate(args):
    gate_root = Path(args.out).parent / (Path(args.out).name + "_512_gate")
    gate_root.mkdir(parents=True, exist_ok=True)
    scenario = next(
        item for item in scenario_catalog(512)
        if item.family == "healthy_alltoall" and item.size_mib == 64 and
        item.parallel == 4)
    spec = CaseSpec(scenario, "ops", CANONICAL_SEEDS[0])
    artifact = materialize_traffic(gate_root / "traffic", scenario, spec.seed, 512)
    gate_args = argparse.Namespace(
        out=gate_root, sim=args.sim, force=args.force,
        timeout=min(args.gate_timeout, args.gate_runtime_budget),
    )
    try:
        row = run_case(spec, artifact, gate_args, 512)
        succeeded = bool(
            row["returncode"] == 0 and row["config_ok"] and
            row["all_flows_completed"] and row["primary_us"] > 0)
        status = "passed" if succeeded else "failed"
        reason = "complete" if succeeded else "invalid_or_incomplete_result"
    except (OSError, subprocess.SubprocessError, MemoryError) as error:
        succeeded = False
        status = "failed"
        reason = f"{type(error).__name__}: {error}"
        row = {}
    result = {
        "status": status,
        "reason": reason,
        "selected_nodes": choose_scale(succeeded),
        "gate_case": row,
    }
    (gate_root / "gate.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def validate_specs(specs, nodes, seeds):
    expected = make_case_specs(nodes, seeds)
    if tuple(specs) != expected:
        raise ValueError("case specification matrix changed")
    if len(specs) != (230 * len(seeds)):
        raise ValueError(f"expected {230 * len(seeds)} cases, got {len(specs)}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sim", type=Path, default=DEFAULT_SIM)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--seeds", default="13,29,47")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--gate-timeout", type=int, default=1800)
    parser.add_argument(
        "--gate-runtime-budget", type=int, default=300,
        help="maximum practical runtime in seconds for the smallest 512 A2A gate",
    )
    parser.add_argument("--nodes", choices=("auto", "256", "512"), default="auto")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    args.seeds = tuple(int(value) for value in args.seeds.split(",") if value)
    if not args.seeds or args.workers < 1 or args.timeout <= 0:
        raise ValueError("seeds, workers and timeout must be positive")
    if args.nodes == "auto" and tuple(args.seeds) != CANONICAL_SEEDS:
        raise ValueError("automatic final run requires canonical seeds 13,29,47")
    if not args.sim.exists() and not args.dry_run:
        raise FileNotFoundError(args.sim)

    args.out.mkdir(parents=True, exist_ok=True)
    output_lock = acquire_output_lock(args.out)
    if args.nodes == "auto" and not args.dry_run:
        gate = run_resource_gate(args)
        nodes = gate["selected_nodes"]
    else:
        nodes = 512 if args.nodes in ("auto", "512") else 256
        gate = {
            "status": "skipped_dry_run" if args.dry_run else "forced",
            "reason": f"nodes={nodes}", "selected_nodes": nodes,
        }

    specs = make_case_specs(nodes, args.seeds)
    validate_specs(specs, nodes, args.seeds)
    manifest = {
        "nodes": nodes,
        "paths": common.TOPOLOGIES[nodes].paths,
        "seeds": args.seeds,
        "schemes": TEST_SCHEMES,
        "scheme_rules": {
            "healthy_p2p": HEALTHY_SCHEMES,
            "all_other_families": TEST_SCHEMES,
        },
        "case_count": len(specs),
        "gate": gate,
    }
    (args.out / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8")
    if args.dry_run:
        print(f"validated {len(specs)} commands at {nodes} nodes")
        return 0

    artifacts = {}
    traffic_dir = args.out / "traffic"
    for spec in specs:
        key = (spec.scenario.name, spec.seed)
        if key not in artifacts:
            artifacts[key] = materialize_traffic(
                traffic_dir, spec.scenario, spec.seed, nodes)

    rows = []
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(args.workers, len(specs))) as executor:
        futures = [
            executor.submit(
                run_case, spec, artifacts[(spec.scenario.name, spec.seed)],
                args, nodes)
            for spec in specs
        ]
        for future in concurrent.futures.as_completed(futures):
            rows.append(future.result())
            write_csv(args.out / "results.partial.csv", rows)

    validate_result_rows(rows, nodes, args.seeds)
    rows.sort(key=lambda row: (row["family"], row["scenario"],
                               row["scheme"], row["seed"]))
    summary = aggregate_rows(rows)
    write_csv(args.out / "results.csv", rows)
    write_csv(args.out / "summary.csv", summary)
    (args.out / "report.md").write_text(
        build_report(rows, summary, nodes, gate), encoding="utf-8")
    (args.out / "COMPLETE").write_text(
        f"{len(rows)} validated cells\n", encoding="ascii")
    print(f"validated {len(rows)} cells; report={args.out / 'report.md'}")
    output_lock.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
