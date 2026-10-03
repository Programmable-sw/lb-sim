#!/usr/bin/env python3
"""Test short-flow MRC versus SGLB after fabric-state warm-up."""

import argparse
import concurrent.futures
import csv
import gzip
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import random
import shlex
import subprocess
import tempfile
import time


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[1]
DEFAULT_SIM = ROOT / "sim/datacenter/htsim_roce"
DEFAULT_OUT = SCRIPT_DIR / "output/mrc_sglb_warmed_short_flow"
FLOW_SIZES = (6144, 13312, 19456, 33792, 54272, 136192)
SEEDS = (13, 29, 47)
SCHEMES = ("mrc", "sglb")
FLOWS_PER_SIZE = 1000

_HELPER_SPEC = importlib.util.spec_from_file_location(
    "mrc_sglb_transition_helper",
    SCRIPT_DIR / "run_mrc_sglb_pressure_transition.py")
helper = importlib.util.module_from_spec(_HELPER_SPEC)
_HELPER_SPEC.loader.exec_module(helper)


def build_balanced_flows(seed, flows_per_size=FLOWS_PER_SIZE):
    rng = random.Random(seed)
    flows = []
    for flow_size in FLOW_SIZES:
        for _ in range(flows_per_size):
            src = rng.randrange(128)
            dst = rng.randrange(127)
            if dst >= src:
                dst += 1
            start_ps = rng.randrange(100_000_000, 600_000_001)
            flows.append({
                "src": src,
                "dst": dst,
                "start_us": start_ps / 1_000_000.0,
                "start_ps": start_ps,
                "flow_size": flow_size,
            })
    flows.sort(key=lambda row: (
        row["start_ps"], row["src"], row["dst"], row["flow_size"]))
    for flow_id, row in enumerate(flows, 1):
        row["flow_id"] = flow_id
    return flows


def write_traffic(path, flows):
    lines = ["Nodes 128", "Connections {}".format(len(flows))]
    lines.extend(
        "{src}->{dst} id {flow_id} start {start_ps} size {flow_size}".format(
            **row) for row in flows)
    helper.atomic_write_text(path, "\n".join(lines) + "\n")


def build_command(sim, scheme, traffic, output, connections, seed):
    return [
        str(sim),
        "-o", str(output),
        "-tm", str(traffic),
        "-nodes", "128",
        "-conns", str(connections),
        "-tiers", "2",
        "-lb", scheme,
        "-linkspeed", "400000",
        "-queue_type", "composite_ecn_lb",
        "-host_queue_type", "prio",
        "-mtu", "4096",
        "-end", "10000",
        "-paths", "64",
        "-seed", str(seed),
        "-cc", "dctcp_variant",
        "-roce_rx_mode", "sp",
        "-roce_sack_bitmap_bits", "64",
        "-roce_transport_semantics", "mrc_exact_bounded",
        "-roce_trim_recovery", "exact",
        "-hop_latency", "0.5",
        "-switch_latency", "0.5",
        "-path_hotspot_spines", "16",
        "-path_hotspot_bg_rate_gbps", "380",
        "-path_hotspot_bg_on_us", "1000",
        "-path_hotspot_bg_off_us", "0",
    ]


def parse_mrc_flow_diags(text):
    rows = {}
    required = {
        "flow_id", "src", "dst", "flow_size", "actionable_feedback",
        "unique_active_evs", "full_sweeps",
        "quality_feedback_before_done", "effective_state_updates",
        "new_selections_after_first_update",
    }
    for line in text.splitlines():
        if not line.startswith("MrcFlowDiag "):
            continue
        raw = dict(token.split("=", 1) for token in line.split()[1:])
        missing = required - set(raw)
        if missing:
            raise ValueError("MRC diagnostic missing " + repr(sorted(missing)))
        flow_id = int(raw["flow_id"])
        if flow_id in rows:
            raise ValueError("duplicate MRC diagnostic flow_id")
        rows[flow_id] = {
            "flow_id": flow_id,
            "src": int(raw["src"]),
            "dst": int(raw["dst"]),
            "flow_size": int(raw["flow_size"]),
            "actionable_feedback": int(raw["actionable_feedback"]),
            "unique_active_evs": int(raw["unique_active_evs"]),
            "full_sweeps": int(raw["full_sweeps"]),
            "quality_feedback_before_done":
                int(raw["quality_feedback_before_done"]),
            "effective_state_updates": int(raw["effective_state_updates"]),
            "new_selections_after_first_update":
                int(raw["new_selections_after_first_update"]),
        }
    return rows


def parse_sglb_route_diag(text):
    selected = None
    for line in text.splitlines():
        if line.startswith("SglbRouteDiag "):
            selected = dict(
                token.split("=", 1) for token in line.split()[1:])
    if selected is None:
        raise ValueError("missing SglbRouteDiag")
    integer_fields = (
        "route_calls", "all_same_quality_calls", "all_zero_quality_calls",
        "remote_snapshot_used", "remote_snapshot_missing",
    )
    result = {field: int(selected[field]) for field in integer_fields}
    result["avg_candidate_choices"] = float(
        selected["avg_candidate_choices"])
    result["avg_distinct_qualities"] = float(
        selected["avg_distinct_qualities"])
    calls = result["route_calls"]
    result["all_zero_fraction"] = (
        result["all_zero_quality_calls"] / calls if calls else 0.0)
    result["all_same_fraction"] = (
        result["all_same_quality_calls"] / calls if calls else 0.0)
    return result


def pair_flows(seed, traffic, completions, mrc_diags):
    expected = set(traffic)
    for scheme in SCHEMES:
        if set(completions[scheme]) != expected:
            raise ValueError("{} completion IDs mismatch".format(scheme))
    if set(mrc_diags) != expected:
        raise ValueError("MRC diagnostic IDs mismatch")
    rows = []
    for flow_id in sorted(expected):
        spec = traffic[flow_id]
        for scheme in SCHEMES:
            item = completions[scheme][flow_id]
            if item["src"] != spec["src"] or item["dst"] != spec["dst"]:
                raise ValueError(
                    "{} identity mismatch flow {}".format(scheme, flow_id))
        diag = mrc_diags[flow_id]
        if (
                diag["src"] != spec["src"] or
                diag["dst"] != spec["dst"] or
                diag["flow_size"] != spec["flow_size"]):
            raise ValueError("MRC diagnostic identity mismatch")
        mrc_fct = (
            completions["mrc"][flow_id]["finish_us"] - spec["start_us"])
        sglb_fct = (
            completions["sglb"][flow_id]["finish_us"] - spec["start_us"])
        if mrc_fct <= 0 or sglb_fct <= 0:
            raise ValueError("non-positive paired FCT")
        rows.append({
            "seed": seed,
            "flow_id": flow_id,
            "src": spec["src"],
            "dst": spec["dst"],
            "start_us": spec["start_us"],
            "flow_size": spec["flow_size"],
            "mrc_fct_us": mrc_fct,
            "sglb_fct_us": sglb_fct,
            "fct_ratio": mrc_fct / sglb_fct,
            "actionable_feedback": diag["actionable_feedback"],
            "unique_active_evs": diag["unique_active_evs"],
            "full_sweeps": diag["full_sweeps"],
            "quality_feedback_before_done":
                diag["quality_feedback_before_done"],
            "effective_state_updates": diag["effective_state_updates"],
            "new_selections_after_first_update":
                diag["new_selections_after_first_update"],
        })
    return rows


def aggregate_by_seed_and_size(rows):
    groups = {}
    for row in rows:
        groups.setdefault(
            (int(row["seed"]), int(row["flow_size"])), []).append(row)
    result = []
    for (seed, flow_size), selected in sorted(groups.items()):
        count = len(selected)
        mrc_fcts = [row["mrc_fct_us"] for row in selected]
        sglb_fcts = [row["sglb_fct_us"] for row in selected]
        ratios = [row["fct_ratio"] for row in selected]
        result.append({
            "seed": seed,
            "flow_size": flow_size,
            "flow_size_kib": flow_size / 1024.0,
            "flow_count": count,
            "fct_ratio_geomean": math.exp(
                sum(math.log(value) for value in ratios) / count),
            "mrc_fct_p50_us": helper.percentile(mrc_fcts, 0.50),
            "sglb_fct_p50_us": helper.percentile(sglb_fcts, 0.50),
            "mrc_fct_p99_us": helper.percentile(mrc_fcts, 0.99),
            "sglb_fct_p99_us": helper.percentile(sglb_fcts, 0.99),
            "actionable_fraction": sum(
                row["actionable_feedback"] > 0
                for row in selected) / count,
            "quality_feedback_fraction": sum(
                row["quality_feedback_before_done"] > 0
                for row in selected) / count,
            "effective_updates_mean": sum(
                row["effective_state_updates"]
                for row in selected) / count,
            "new_selections_after_update_mean": sum(
                row["new_selections_after_first_update"]
                for row in selected) / count,
            "ev_coverage_mean": sum(
                row["unique_active_evs"] / 8.0
                for row in selected) / count,
            "complete_sweep_fraction": sum(
                row["full_sweeps"] > 0
                for row in selected) / count,
        })
    return result


def aggregate_across_seeds(rows, seed_rows):
    groups = {}
    for row in rows:
        groups.setdefault(int(row["flow_size"]), []).append(row)
    by_size_seed = {}
    for row in seed_rows:
        by_size_seed.setdefault(int(row["flow_size"]), []).append(row)
    result = []
    for flow_size, selected in sorted(groups.items()):
        count = len(selected)
        ratios = [row["fct_ratio"] for row in selected]
        seed_ratios = [
            row["fct_ratio_geomean"] for row in by_size_seed[flow_size]]
        mrc_fcts = [row["mrc_fct_us"] for row in selected]
        sglb_fcts = [row["sglb_fct_us"] for row in selected]
        result.append({
            "flow_size": flow_size,
            "flow_size_kib": flow_size / 1024.0,
            "flow_count": count,
            "fct_ratio_geomean": math.exp(
                sum(math.log(value) for value in ratios) / count),
            "seed_ratio_min": min(seed_ratios),
            "seed_ratio_max": max(seed_ratios),
            "improving_seeds": sum(value < 1 for value in seed_ratios),
            "mrc_fct_p50_us": helper.percentile(mrc_fcts, 0.50),
            "sglb_fct_p50_us": helper.percentile(sglb_fcts, 0.50),
            "mrc_fct_p99_us": helper.percentile(mrc_fcts, 0.99),
            "sglb_fct_p99_us": helper.percentile(sglb_fcts, 0.99),
            "actionable_fraction": sum(
                row["actionable_feedback"] > 0
                for row in selected) / count,
            "quality_feedback_fraction": sum(
                row["quality_feedback_before_done"] > 0
                for row in selected) / count,
            "effective_updates_mean": sum(
                row["effective_state_updates"]
                for row in selected) / count,
            "new_selections_after_update_mean": sum(
                row["new_selections_after_first_update"]
                for row in selected) / count,
            "ev_coverage_mean": sum(
                row["unique_active_evs"] / 8.0
                for row in selected) / count,
            "complete_sweep_fraction": sum(
                row["full_sweeps"] > 0
                for row in selected) / count,
        })
    return result


def deterministic_gzip_text(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + path.name + ".tmp")
    temporary.write_bytes(gzip.compress(text.encode("utf-8"), mtime=0))
    os.replace(temporary, path)


def run_cell(seed, scheme, sim, out, traffic_path, connections, force):
    cell = Path(out) / "raw" / "seed_{}".format(seed) / scheme
    cell.mkdir(parents=True, exist_ok=True)
    output = (cell / "logout.dat").resolve()
    command = build_command(
        Path(sim).resolve(), scheme, traffic_path.resolve(), output,
        connections, seed)
    command_text = shlex.join(command)
    sim_sha = helper.file_sha256(sim)
    traffic_sha = helper.file_sha256(traffic_path)
    command_sha = hashlib.sha256(command_text.encode()).hexdigest()
    summary_path = cell / "summary.json"
    stdout_path = cell / "stdout.log.gz"
    if not force and summary_path.exists() and stdout_path.exists():
        cached = json.loads(summary_path.read_text(encoding="utf-8"))
        if (
                cached.get("valid") == 1 and
                cached.get("sim_sha256") == sim_sha and
                cached.get("traffic_sha256") == traffic_sha and
                cached.get("command_sha256") == command_sha):
            with gzip.open(stdout_path, "rt", encoding="utf-8") as handle:
                text = handle.read()
            return text, cached

    helper.atomic_write_text(cell / "command.txt", command_text + "\n")
    raw = cell / ".stdout.running"
    started = time.monotonic()
    with raw.open("w", encoding="utf-8") as handle:
        process = subprocess.run(
            command, cwd=cell, stdout=handle, stderr=subprocess.STDOUT,
            text=True)
    runtime = time.monotonic() - started
    text = raw.read_text(encoding="utf-8")
    completions = helper.parse_completion_text(text)
    required = [
        "lb mode " + scheme,
        "RoceTransportConfig semantics=mrc_exact_bounded",
        "Path hotspot background installed",
        "hot_spines 16 rate 380Gbps on 1000us off 0us",
    ]
    if scheme == "mrc":
        required.append("MRC: paths 64")
        if len(parse_mrc_flow_diags(text)) != connections:
            raise ValueError("MRC diagnostic count mismatch")
    else:
        required.extend([
            "SGLB effective config: score mode nmrc_quantized_topk",
            "local quality update 1us, GCN update 5us",
        ])
        parse_sglb_route_diag(text)
    missing = [token for token in required if token not in text]
    if process.returncode or missing or len(completions) != connections:
        raise RuntimeError(
            "invalid cell seed={} scheme={} rc={} missing={} completions={}/{}"
            .format(
                seed, scheme, process.returncode, missing,
                len(completions), connections))
    deterministic_gzip_text(stdout_path, text)
    raw.unlink()
    metadata = {
        "seed": seed,
        "scheme": scheme,
        "connections": connections,
        "returncode": process.returncode,
        "valid": 1,
        "runtime_s": runtime,
        "sim_sha256": sim_sha,
        "traffic_sha256": traffic_sha,
        "command_sha256": command_sha,
    }
    helper.atomic_write_text(
        summary_path, json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    return text, metadata


def write_csv(path, rows):
    helper.atomic_write_csv(path, rows, list(rows[0]))


def write_paired_gzip(path, rows):
    import io
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer, fieldnames=list(rows[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    deterministic_gzip_text(path, buffer.getvalue())


def plot_results(summary, seed_rows, sglb_routes, out):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    sizes = [row["flow_size_kib"] for row in summary]
    ratios = [row["fct_ratio_geomean"] for row in summary]
    coverages = [row["ev_coverage_mean"] for row in summary]
    actionable = [row["actionable_fraction"] for row in summary]
    fig, axes = plt.subplots(2, 1, figsize=(10.8, 8.4), sharex=True)
    for seed in SEEDS:
        selected = [row for row in seed_rows if row["seed"] == seed]
        axes[0].plot(
            sizes, [row["fct_ratio_geomean"] for row in selected],
            color="#A9B5C2", linewidth=1.1, marker="o", markersize=3,
            alpha=0.8)
    axes[0].plot(
        sizes, ratios, color="#B6403A", linewidth=2.5, marker="o",
        markersize=7, label="3-seed paired geomean")
    axes[0].axhline(
        1.0, color="#50555A", linestyle="--", linewidth=1.2)
    axes[0].set_ylabel("MRC / warmed-SGLB FCT\n(paired geometric mean)")
    axes[0].set_title(
        "Short flows after 100 μs fabric-state warm-up",
        fontweight="bold")
    axes[0].grid(axis="y", alpha=0.25)
    axes[0].legend(frameon=False)
    axes[1].plot(
        sizes, coverages, color="#2878B5", marker="s", linewidth=2.2,
        label="MRC EV coverage")
    axes[1].plot(
        sizes, actionable, color="#D97706", marker="o", linewidth=2.2,
        label="MRC actionable-flow fraction")
    axes[1].set_ylabel("Fraction")
    axes[1].yaxis.set_major_formatter(PercentFormatter(1.0))
    axes[1].set_xlabel("Flow size (KiB)")
    axes[1].set_xticks(sizes, [str(int(value)) for value in sizes])
    axes[1].set_ylim(-0.03, 1.05)
    axes[1].grid(axis="y", alpha=0.25)
    axes[1].legend(frameon=False)
    avg_candidates = sum(
        row["avg_candidate_choices"] for row in sglb_routes
    ) / len(sglb_routes)
    all_zero = sum(
        row["all_zero_fraction"] for row in sglb_routes
    ) / len(sglb_routes)
    fig.text(
        0.5, 0.015,
        "SGLB route diagnostics: mean candidates {:.2f}/8; "
        "all-zero quality {:.1%}. Ratio >1 favors SGLB.".format(
            avg_candidates, all_zero),
        ha="center", fontsize=9.5, color="#4A5057")
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    for suffix in ("png", "pdf"):
        fig.savefig(
            Path(out) / ("focused_proof." + suffix),
            dpi=220 if suffix == "png" else None,
            facecolor="white", bbox_inches="tight",
            metadata=(
                {"CreationDate": None, "ModDate": None}
                if suffix == "pdf" else None))
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sim", type=Path, default=DEFAULT_SIM)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--flows-per-size", type=int, default=FLOWS_PER_SIZE)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--no-plot", action="store_true")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    traffic_by_seed = {}
    for seed in SEEDS:
        flows = build_balanced_flows(seed, args.flows_per_size)
        path = args.out / "traffic" / "seed_{}.cm".format(seed)
        write_traffic(path, flows)
        traffic_by_seed[seed] = path

    results = {}
    specs = [
        (seed, scheme) for seed in SEEDS for scheme in SCHEMES]
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(
                run_cell, seed, scheme, args.sim, args.out,
                traffic_by_seed[seed], args.flows_per_size * len(FLOW_SIZES),
                args.force): (seed, scheme)
            for seed, scheme in specs
        }
        for future in concurrent.futures.as_completed(futures):
            spec = futures[future]
            results[spec] = future.result()
            print("validated seed={} scheme={}".format(*spec), flush=True)

    all_paired = []
    cells = []
    sglb_routes = []
    for seed in SEEDS:
        traffic = helper.parse_traffic_text(
            traffic_by_seed[seed].read_text(encoding="utf-8"))
        completions = {}
        for scheme in SCHEMES:
            text, metadata = results[(seed, scheme)]
            completions[scheme] = helper.parse_completion_text(text)
            cells.append(metadata)
        mrc_diags = parse_mrc_flow_diags(results[(seed, "mrc")][0])
        all_paired.extend(
            pair_flows(seed, traffic, completions, mrc_diags))
        route = parse_sglb_route_diag(results[(seed, "sglb")][0])
        route["seed"] = seed
        sglb_routes.append(route)

    seed_rows = aggregate_by_seed_and_size(all_paired)
    summary = aggregate_across_seeds(all_paired, seed_rows)
    write_csv(args.out / "cells.csv", cells)
    write_csv(args.out / "seed_summary.csv", seed_rows)
    write_csv(args.out / "summary.csv", summary)
    write_csv(args.out / "sglb_route_summary.csv", sglb_routes)
    write_paired_gzip(args.out / "paired_flow_metrics.csv.gz", all_paired)
    if not args.no_plot:
        plot_results(summary, seed_rows, sglb_routes, args.out)
    print(
        "PASS cells={} paired={} seed_rows={} summary_rows={}".format(
            len(cells), len(all_paired), len(seed_rows), len(summary)))


if __name__ == "__main__":
    main()
