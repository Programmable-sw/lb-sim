#!/usr/bin/env python3
"""Reproducible 64-path SGLB min-choice scan on the 256-host topology."""

import argparse
import concurrent.futures
import csv
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
from dataclasses import dataclass


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

import experiment_metrics as metrics  # noqa: E402
import feedback_eval_common as common  # noqa: E402


NODES = 256
PATHS = 64
TOPOLOGY = common.TOPOLOGIES[NODES]
MIN_CHOICES = (16, 20, 24, 28, 32, 36, 40)
SEEDS = (13, 29, 47)
WORKLOADS = (
    "healthy_permutation_16mib",
    "healthy_websearch_100",
    "fixed_hotspot_permutation_16mib",
    "periodic_background_a2a_p16_256mib",
)
DEFAULT_SIM = ROOT / "sim/datacenter/htsim_roce"
DEFAULT_OUT = SCRIPT_DIR / "output/sglb_min_choices_64path_scan_256"
RANKING_METRICS = (
    "a2a_cct_us", "a2a_mean_fct_us", "a2a_p95_fct_us",
    "a2a_p99_fct_us", "a2a_max_fct_us",
    "healthy_p2p_p99_ratio", "healthy_websearch_p99_ratio",
    "fixed_hotspot_cct_us", "retransmissions", "rtos", "trims",
    "ecn_marks", "queue_p99_fraction", "spine_queue_cv",
    "avg_candidate_choices", "nonbest_fraction", "avoid_fraction",
)
PARETO_METRICS = (
    "a2a_cct_us", "a2a_p99_fct_us", "retransmissions", "trims",
    "ecn_marks", "healthy_p2p_p99_ratio", "healthy_websearch_p99_ratio",
)
COMPLETION_RE = re.compile(
    r"^\.*Flow Roce_(\d+)_(\d+)\s+(\d+) finished at ([0-9.]+) "
    r"total bytes (\d+) bg traffic ([01]) flowid (\d+)$",
    re.MULTILINE,
)


@dataclass(frozen=True)
class CellSpec:
    workload: str
    seed: int
    min_choices: int
    traffic: Path
    traffic_sha256: str
    connections: int
    flows_data: object
    case_dir: Path
    output: Path
    command: tuple


def atomic_write_text(path, content):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + path.name + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def deterministic_gzip(path, content):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + path.name + ".tmp")
    temporary.write_bytes(gzip.compress(content.encode("utf-8"), mtime=0))
    os.replace(temporary, path)


def write_csv(path, rows):
    rows = list(rows)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    temporary = path.with_name("." + path.name + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fields,
            delimiter="\t" if path.suffix == ".tsv" else ",")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def _selection(value, allowed, name):
    result = tuple(int(item) for item in value.split(",") if item)
    if not result or any(item not in allowed for item in result):
        raise ValueError(f"{name} must be selected from {','.join(map(str, allowed))}")
    return result


def materialize_traffic(out, workload, seed, sample_scale=1.0):
    traffic_dir = Path(out) / "traffic"
    traffic_dir.mkdir(parents=True, exist_ok=True)
    permutation_path = traffic_dir / f"permutation_16mib_seed{seed}.cm"
    if workload in (WORKLOADS[0], WORKLOADS[2]):
        flows = common.permutation_flows(TOPOLOGY, seed, 16 * common.MIB, 0)
        if not permutation_path.exists():
            common.write_traffic_matrix(permutation_path, TOPOLOGY, flows)
        return permutation_path, len(flows), flows
    if workload == WORKLOADS[1]:
        suffix = "" if sample_scale == 1.0 else f"_sample{sample_scale:g}"
        path = traffic_dir / f"websearch_100{suffix}_seed{seed}.cm"
        flows = common.websearch_flows(
            TOPOLOGY, seed, 1.0, 10000 * sample_scale)
        if not path.exists():
            common.write_traffic_matrix(path, TOPOLOGY, flows)
        return path, len(flows), flows
    if workload == WORKLOADS[3]:
        suffix = "" if sample_scale == 1.0 else f"_sample{sample_scale:g}"
        path = traffic_dir / f"a2a_p16_256mib{suffix}_seed{seed}.cm"
        sampled_message_bytes = max(
            NODES, int(round(256 * common.MIB * sample_scale)))
        plan = common.alltoall_plan(
            NODES, NODES, 16, sampled_message_bytes // NODES, 0, seed)
        if not path.exists():
            common.write_alltoall_matrix(path, plan)
        return path, plan.connection_count, None
    raise ValueError(f"unknown workload {workload}")


def _end_us(workload, sample_scale=1.0):
    if workload == WORKLOADS[0] or workload == WORKLOADS[2]:
        return 10000
    if workload == WORKLOADS[1]:
        return 40000 if sample_scale == 1.0 else max(
            5000, int(round(40000 * sample_scale)))
    return 100000 if sample_scale == 1.0 else max(
        5000, int(round(100000 * sample_scale)))


def build_command(sim, traffic, output, connections, seed, workload,
                  min_choices, sample_scale=1.0):
    end_us = _end_us(workload, sample_scale)
    command = [
        str(sim), "-o", str(output), "-tm", str(traffic),
        "-nodes", str(NODES), "-conns", str(connections), "-tiers", "2",
        "-lb", "sglb", "-linkspeed", "400000",
        "-queue_type", "composite_ecn_lb", "-host_queue_type", "prio",
        "-mtu", "4096", "-end", str(end_us), "-paths", str(PATHS),
        "-seed", str(seed), "-cc", "dctcp_variant", "-roce_rx_mode", "sp",
        "-roce_sack_bitmap_bits", "64", "-roce_transport_semantics",
        "mrc_exact_bounded", "-roce_trim_recovery", "exact",
        "-hop_latency", "0.5", "-switch_latency", "0.5",
        "-queue_cv_sample_us", "100", "-sglb_min_choices",
        str(min_choices), "-sglb_update_us", "1",
        "-sglb_gcn_update_us", "15", "-sglb_gcn_aging_us", "30",
    ]
    if workload == WORKLOADS[2]:
        command.extend([
            "-path_hotspot_spines", "16",
            "-path_hotspot_bg_rate_gbps", "390",
            "-path_hotspot_bg_on_us", str(end_us),
            "-path_hotspot_bg_off_us", "0",
        ])
    elif workload == WORKLOADS[3]:
        command.extend(common.alltoall_background_args(256 * common.MIB, True))
    return tuple(command)


def make_specs(args):
    seeds = _selection(args.seeds, SEEDS, "seeds")
    choices = _selection(args.min_choices, MIN_CHOICES, "min choices")
    workloads = tuple(item for item in args.workloads.split(",") if item)
    if not workloads or any(item not in WORKLOADS for item in workloads):
        raise ValueError("workloads contain an unsupported value")
    args.out = Path(args.out).resolve()
    args.sim = Path(args.sim).resolve()
    materials = {}
    specs = []
    for workload in workloads:
        for seed in seeds:
            key = (workload, seed)
            if key not in materials:
                traffic, connections, flows = materialize_traffic(
                    args.out, workload, seed, args.sample_scale)
                materials[key] = (
                    traffic, metrics.file_sha256(traffic), connections, flows)
            traffic, digest, connections, flows = materials[key]
            for choice in choices:
                case_dir = args.out / "runs" / workload / f"seed{seed}_min{choice}"
                output = case_dir / "logout.dat"
                command = build_command(
                    args.sim, traffic, output, connections, seed, workload,
                    choice, args.sample_scale)
                specs.append(CellSpec(
                    workload, seed, choice, traffic, digest, connections,
                    flows, case_dir, output, command))
    return specs


def validate_specs(specs, args):
    seen = set()
    simulator = str(Path(args.sim).resolve())
    for spec in specs:
        key = (spec.workload, spec.seed, spec.min_choices)
        if key in seen:
            raise ValueError(f"duplicate cell {key}")
        seen.add(key)
        command = list(spec.command)
        if command[0] != simulator:
            raise ValueError("simulator path mismatch")
        for option, expected in (
                ("-nodes", "256"), ("-tiers", "2"), ("-lb", "sglb"),
                ("-paths", "64"), ("-sglb_update_us", "1"),
                ("-sglb_gcn_update_us", "15"),
                ("-sglb_gcn_aging_us", "30")):
            if command[command.index(option) + 1] != expected:
                raise ValueError(f"cell {key}: incorrect {option}")
        if command[command.index("-sglb_min_choices") + 1] != str(
                spec.min_choices):
            raise ValueError(f"cell {key}: min-choice mismatch")
        if (spec.traffic.exists()
                and metrics.file_sha256(spec.traffic) != spec.traffic_sha256):
            raise ValueError(f"cell {key}: traffic changed")


def _diag(text, prefix):
    match = re.search(rf"^{re.escape(prefix)} (.*)$", text, re.MULTILINE)
    if match is None:
        raise ValueError(f"missing {prefix}")
    return {
        key: float(value) if "." in value else int(value)
        for key, value in metrics.KEY_VALUE_RE.findall(match.group(1))
    }


def _config_ok(text, spec, returncode):
    config_prefix = (
        "SGLB effective config: score mode nmrc_quantized_topk, "
        "OFAT factor real_gcn_raw_linear, local quality update 1us, "
        "GCN update 15us, aging 30us,"
    )
    required = (
        "Standard 2-tier leaf-spine: nodes 256 leaves 4 spines 64 ",
        "RoceTransportConfig semantics=mrc_exact_bounded",
        "topology_path_combo=64",
        config_prefix,
        f"min choices {spec.min_choices},",
        "nmrc_levels 4,",
    )
    return returncode == 0 and all(item in text for item in required)


def parse_run(spec, text, returncode, runtime_s):
    finishes = {}
    for match in COMPLETION_RE.finditer(text):
        finishes[int(match.group(7))] = float(match.group(4))
    fcts = []
    for flow_id, finish_us in finishes.items():
        if spec.flows_data is None:
            fcts.append(finish_us)
        elif 1 <= flow_id <= len(spec.flows_data):
            fcts.append(finish_us - spec.flows_data[flow_id - 1].start_us)
    roce = _diag(text, "RoceDiag")
    queue = _diag(text, "QueueDiag")
    cv = _diag(text, "QueueCvDiag")
    route = _diag(text, "SglbRouteDiag")
    paper = _diag(text, "PaperSglbDiag")
    new_retx = metrics.NEW_RETX_RE.search(text)
    if new_retx is None:
        raise ValueError("missing packet retransmission summary")
    selected = sum(route.get(f"selected_{level}", 0) for level in (
        "good", "degraded", "bad", "avoid"))
    route_calls = route.get("route_calls", 0)
    complete = len(finishes) == spec.connections and len(fcts) == spec.connections
    row = {
        "workload": spec.workload, "seed": spec.seed,
        "min_choices": spec.min_choices, "traffic_sha256": spec.traffic_sha256,
        "runtime_s": runtime_s, "returncode": returncode,
        "expected_flows": spec.connections, "completed_flows": len(finishes),
        "all_flows_completed": int(complete),
        "config_ok": int(_config_ok(text, spec, returncode)),
        "cct_us": max(finishes.values()) if finishes else 0.0,
        "mean_fct_us": sum(fcts) / len(fcts) if fcts else 0.0,
        "p50_fct_us": metrics.percentile(fcts, 0.50),
        "p95_fct_us": metrics.percentile(fcts, 0.95),
        "p99_fct_us": metrics.percentile(fcts, 0.99),
        "p999_fct_us": metrics.percentile(fcts, 0.999),
        "max_fct_us": max(fcts) if fcts else 0.0,
        "new_packets": int(new_retx.group(1)),
        "retransmissions": int(new_retx.group(2)),
        "rtos": roce.get("rtos", 0),
        "trims": queue.get("composite_trims", 0),
        "ecn_marks": queue.get("composite_ecn_marks", 0),
        "spine_queue_cv": cv.get("spine_queue_cv", 0.0),
        "spine_queue_avg": cv.get("spine_queue_avg", 0.0),
        "queue_p95_fraction": cv.get("spine_queue_p95_fraction", 0.0),
        "queue_p99_fraction": cv.get("spine_queue_p99_fraction", 0.0),
        "avg_available_choices": route.get("avg_available_choices", 0.0),
        "avg_candidate_choices": route.get("avg_candidate_choices", 0.0),
        "avg_best_quality_choices": route.get("avg_best_quality_choices", 0.0),
        "selected_nonbest_quality": route.get("selected_nonbest_quality", 0),
        "nonbest_fraction": (
            route.get("selected_nonbest_quality", 0) / route_calls
            if route_calls else 0.0),
        "selected_good": route.get("selected_good", 0),
        "selected_degraded": route.get("selected_degraded", 0),
        "selected_bad": route.get("selected_bad", 0),
        "selected_avoid": route.get("selected_avoid", 0),
        "avoid_fraction": (
            route.get("selected_avoid", 0) / selected if selected else 0.0),
        "remote_snapshot_used": route.get("remote_snapshot_used", 0),
        "remote_snapshot_missing": route.get("remote_snapshot_missing", 0),
        "gcn_updates": paper.get("gcn_updates", 0),
        "gcn_packets": paper.get("gcn_packets", 0),
        "gcn_bytes": paper.get("gcn_bytes", 0),
        "gcn_deliveries": paper.get("gcn_deliveries", 0),
        "gcn_profile_updates": paper.get("gcn_profile_updates", 0),
        "gcn_stale": paper.get("gcn_stale", 0),
    }
    return row


def _fingerprint(spec, sim_sha):
    payload = sim_sha + "\0" + spec.traffic_sha256 + "\0" + shlex.join(
        spec.command)
    return hashlib.sha256(payload.encode()).hexdigest()


def _valid_row(row):
    return bool(row.get("config_ok") and row.get("all_flows_completed"))


def run_cell(spec, args, simulator_sha):
    spec.case_dir.mkdir(parents=True, exist_ok=True)
    fingerprint = _fingerprint(spec, simulator_sha)
    result_path = spec.case_dir / "parsed.json"
    fingerprint_path = spec.case_dir / "fingerprint.txt"
    if (not args.force and result_path.exists() and fingerprint_path.exists()
            and fingerprint_path.read_text().strip() == fingerprint):
        row = json.loads(result_path.read_text())
        if _valid_row(row):
            print(f"cached {spec.workload} seed={spec.seed} min={spec.min_choices}",
                  flush=True)
            return row
    atomic_write_text(spec.case_dir / "command.txt", shlex.join(spec.command) + "\n")
    atomic_write_text(fingerprint_path, fingerprint + "\n")
    print(f"run {spec.workload} seed={spec.seed} min={spec.min_choices}",
          flush=True)
    started = time.monotonic()
    try:
        process = subprocess.run(
            spec.command, cwd=spec.case_dir, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, timeout=args.timeout,
            check=False)
        text = process.stdout
        returncode = process.returncode
    except subprocess.TimeoutExpired as error:
        text = (error.stdout or "")
        if isinstance(text, bytes):
            text = text.decode(errors="replace")
        text += f"\nTIMEOUT after {args.timeout}s\n"
        returncode = 124
    elapsed = time.monotonic() - started
    deterministic_gzip(spec.case_dir / "stdout.log.gz", text)
    atomic_write_text(spec.case_dir / "returncode.txt", f"{returncode}\n")
    atomic_write_text(spec.case_dir / "runtime.txt", f"{elapsed:.6f}\n")
    try:
        row = parse_run(spec, text, returncode, elapsed)
    except Exception as error:
        atomic_write_text(spec.case_dir / "parse_error.txt", repr(error) + "\n")
        raise
    atomic_write_text(result_path, json.dumps(row, indent=2, sort_keys=True) + "\n")
    if _valid_row(row):
        spec.output.unlink(missing_ok=True)
    print(f"done {spec.workload} seed={spec.seed} min={spec.min_choices} "
          f"flows={row['completed_flows']}/{row['expected_flows']} "
          f"cct={row['cct_us']:.3f}us runtime={elapsed:.1f}s", flush=True)
    return row


def geometric_mean(values):
    values = list(values)
    if not values:
        return 0.0
    if any(value < 0 for value in values):
        raise ValueError("geometric mean requires nonnegative values")
    if any(value == 0 for value in values):
        return 0.0
    return math.exp(sum(math.log(value) for value in values) / len(values))


def _aggregate(values):
    values = list(values)
    return geometric_mean(values) if values and all(value > 0 for value in values) \
        else sum(values) / len(values) if values else 0.0


def summarize(rows):
    rows = list(rows)
    baselines = {
        (row["workload"], row["seed"]): row
        for row in rows if row["min_choices"] == 24
    }
    seed_ratios = []
    for row in rows:
        baseline = baselines[(row["workload"], row["seed"])]
        enriched = dict(row)
        for field in ("cct_us", "mean_fct_us", "p95_fct_us", "p99_fct_us",
                      "p999_fct_us", "max_fct_us"):
            denominator = baseline[field]
            enriched[field + "_ratio_vs_min24"] = (
                row[field] / denominator if denominator else 1.0)
        seed_ratios.append(enriched)

    summary = []
    for choice in sorted({row["min_choices"] for row in rows}):
        candidate = [row for row in rows if row["min_choices"] == choice]
        by_workload = {
            workload: [row for row in candidate if row["workload"] == workload]
            for workload in WORKLOADS
        }
        a2a = by_workload[WORKLOADS[3]]
        hotspot = by_workload[WORKLOADS[2]]
        def agg(source, field):
            return _aggregate(row[field] for row in source)
        def ratio(workload):
            values = []
            for row in by_workload[workload]:
                base = baselines[(workload, row["seed"])]["p99_fct_us"]
                values.append(row["p99_fct_us"] / base if base else 1.0)
            return geometric_mean(values)
        summary.append({
            "min_choices": choice,
            "nominal_floor_fraction": choice / PATHS,
            "healthy_p2p_p99_ratio": ratio(WORKLOADS[0]),
            "healthy_websearch_p99_ratio": ratio(WORKLOADS[1]),
            "a2a_cct_us": agg(a2a, "cct_us"),
            "a2a_mean_fct_us": agg(a2a, "mean_fct_us"),
            "a2a_p95_fct_us": agg(a2a, "p95_fct_us"),
            "a2a_p99_fct_us": agg(a2a, "p99_fct_us"),
            "a2a_max_fct_us": agg(a2a, "max_fct_us"),
            "fixed_hotspot_cct_us": agg(hotspot, "cct_us"),
            **{
                field: agg(candidate, field) for field in (
                    "retransmissions", "rtos", "trims", "ecn_marks",
                    "queue_p99_fraction", "spine_queue_cv",
                    "avg_candidate_choices", "nonbest_fraction",
                    "avoid_fraction")
            },
        })
    return summary, seed_ratios


def rank_candidates(summary):
    ranked = [dict(row) for row in summary]
    for row in ranked:
        row["eligible"] = (
            row["healthy_p2p_p99_ratio"] <= 1.01 and
            row["healthy_websearch_p99_ratio"] <= 1.01)
        row["selected"] = False
    eligible = [row for row in ranked if row["eligible"]]
    if eligible:
        best_cct = min(row["a2a_cct_us"] for row in eligible)
        tied = [row for row in eligible
                if row["a2a_cct_us"] <= best_cct * 1.001]
        winner = min(tied, key=lambda row: (
            row["a2a_p99_fct_us"], row["fixed_hotspot_cct_us"],
            row["trims"], row["retransmissions"], row["ecn_marks"],
            row["min_choices"]))
        winner["selected"] = True
    ranked.sort(key=lambda row: (
        not row["selected"], not row["eligible"], row["a2a_cct_us"]))

    rankings = []
    for metric in RANKING_METRICS:
        ordered = sorted(summary, key=lambda row: (row[metric], row["min_choices"]))
        for rank, row in enumerate(ordered, 1):
            rankings.append({
                "metric": metric, "rank": rank,
                "min_choices": row["min_choices"], "value": row[metric],
            })

    pareto = []
    for row in summary:
        dominators = []
        for other in summary:
            if other is row:
                continue
            no_worse = all(other[field] <= row[field] for field in PARETO_METRICS)
            better = any(other[field] < row[field] for field in PARETO_METRICS)
            if no_worse and better:
                dominators.append(str(other["min_choices"]))
        pareto.append({
            "min_choices": row["min_choices"],
            "pareto": int(not dominators),
            "dominated_by": ";".join(dominators),
        })
    return ranked, rankings, pareto


def _markdown_table(rows, columns):
    lines = ["| " + " | ".join(columns) + " |",
             "| " + " | ".join("---" for _ in columns) + " |"]
    for row in rows:
        values = []
        for column in columns:
            value = row[column]
            values.append(f"{value:.6g}" if isinstance(value, float) else str(value))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def write_report(path, ranked, rankings, pareto, sample_scale=1.0):
    winner = next((row for row in ranked if row["selected"]), None)
    if winner is None:
        lead = "No tested candidate met both healthy-workload guardrails."
    else:
        lead = (
            f"The selected lower bound is **min{winner['min_choices']}** "
            f"({winner['nominal_floor_fraction']:.1%} of 64 paths).")
    summary_columns = (
        "min_choices", "eligible", "selected", "a2a_cct_us",
        "a2a_p99_fct_us", "healthy_p2p_p99_ratio",
        "healthy_websearch_p99_ratio", "fixed_hotspot_cct_us",
        "avg_candidate_choices", "retransmissions", "trims", "ecn_marks")
    ranking_rows = sorted(rankings, key=lambda row: (row["metric"], row["rank"]))
    sampling_note = (
        "Full-scale workload matrix."
        if sample_scale == 1.0 else
        f"Quick screening matrix (`sample_scale={sample_scale:g}`): P2P and "
        "fixed-hotspot traffic remain full; WebSearch uses the scaled arrival "
        "window and A2A uses the scaled per-source message volume."
    )
    content = f"""# SGLB min-choice scan (64 paths, 256 hosts)

{lead}

{sampling_note}

Eligibility requires the paired three-seed geometric-mean p99-FCT ratio to
min24 to be at most 1.01 for both healthy P2P and healthy WebSearch. The winner
minimizes paired three-seed A2A CCT among eligible candidates.

## Candidate summary

{_markdown_table(ranked, summary_columns)}

## Complete metric rankings

{_markdown_table(ranking_rows, ("metric", "rank", "min_choices", "value"))}

## Pareto status

{_markdown_table(pareto, ("min_choices", "pareto", "dominated_by"))}

The configured value is a lower bound, not a fixed spraying fraction; use the
observed `avg_candidate_choices` when interpreting how many paths were exposed.
The three traffic seeds are the independent replication unit.
"""
    atomic_write_text(path, content)


def _commands_rows(specs):
    return [{
        "workload": spec.workload, "seed": spec.seed,
        "min_choices": spec.min_choices, "traffic_sha256": spec.traffic_sha256,
        "command": shlex.join(spec.command),
    } for spec in specs]


def parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--sim", type=Path, default=DEFAULT_SIM)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--seeds", default=",".join(map(str, SEEDS)))
    parser.add_argument("--min-choices", default=",".join(map(str, MIN_CHOICES)))
    parser.add_argument("--workloads", default=",".join(WORKLOADS))
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=7200)
    parser.add_argument("--sample-scale", type=float, default=1.0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--gate", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if (args.workers < 1 or args.timeout < 1
            or not 0 < args.sample_scale <= 1):
        raise ValueError("workers/timeout must be positive and scale in (0,1]")
    args.out = args.out.resolve()
    args.sim = args.sim.resolve()
    if not args.dry_run and not args.sim.exists():
        raise FileNotFoundError(args.sim)
    specs = make_specs(args)
    validate_specs(specs, args)
    if args.gate:
        specs = [spec for spec in specs if (
            spec.workload == WORKLOADS[0] and spec.seed == 13 and
            spec.min_choices == 24)]
        if len(specs) != 1:
            raise ValueError("gate selection is absent from requested matrix")
    args.out.mkdir(parents=True, exist_ok=True)
    write_csv(args.out / "commands.tsv", _commands_rows(specs))
    simulator_sha = metrics.file_sha256(args.sim) if args.sim.exists() else "dry-run"
    manifest = {
        "schema": 1, "nodes": NODES, "paths": PATHS,
        "sample_scale": args.sample_scale,
        "sample_semantics": {
            "p2p_and_fixed_hotspot": "full",
            "websearch_arrival_window": args.sample_scale,
            "a2a_per_source_message_volume": args.sample_scale,
        },
        "min_choices": sorted({spec.min_choices for spec in specs}),
        "seeds": sorted({spec.seed for spec in specs}),
        "workloads": sorted({spec.workload for spec in specs}),
        "cells": len(specs), "simulator": str(args.sim),
        "simulator_sha256": simulator_sha,
        "source_revision": subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True,
            stdout=subprocess.PIPE, check=False).stdout.strip(),
        "traffic": {str(spec.traffic): spec.traffic_sha256 for spec in specs},
    }
    atomic_write_text(args.out / "manifest.json",
                      json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    if args.dry_run:
        print(f"validated {len(specs)} cells; outputs in {args.out}")
        return 0
    rows = []
    errors = []
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(args.workers, len(specs))) as executor:
        futures = {executor.submit(run_cell, spec, args, simulator_sha): spec
                   for spec in specs}
        for future in concurrent.futures.as_completed(futures):
            spec = futures[future]
            try:
                row = future.result()
                rows.append(row)
                if not _valid_row(row):
                    errors.append(f"invalid cell {spec.workload}/{spec.seed}/min{spec.min_choices}")
            except Exception as error:
                errors.append(
                    f"{spec.workload}/{spec.seed}/min{spec.min_choices}: {error!r}")
    rows.sort(key=lambda row: (row["workload"], row["seed"], row["min_choices"]))
    write_csv(args.out / "cells.csv", rows)
    if args.gate and rows:
        row = rows[0]
        if (not _valid_row(row) or row["gcn_packets"] <= 0 or
                row["gcn_stale"] != 0 or row["remote_snapshot_missing"] != 0):
            errors.append("gate diagnostics failed")
    if errors:
        atomic_write_text(args.out / "errors.txt", "\n".join(errors) + "\n")
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    (args.out / "errors.txt").unlink(missing_ok=True)
    if args.gate:
        print("gate passed")
        return 0
    summary, seed_ratios = summarize(rows)
    ranked, rankings, pareto = rank_candidates(summary)
    write_csv(args.out / "seed_ratios.csv", seed_ratios)
    write_csv(args.out / "summary.csv", ranked)
    write_csv(args.out / "rankings.csv", rankings)
    write_csv(args.out / "pareto.csv", pareto)
    write_report(
        args.out / "report.md", ranked, rankings, pareto,
        args.sample_scale)
    winner = next((row for row in ranked if row["selected"]), None)
    print("selected " + (f"min{winner['min_choices']}" if winner else "none"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
