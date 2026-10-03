#!/usr/bin/env python3
"""Measure MRC's transition from cold feedback to a useful closed loop."""

import argparse
import concurrent.futures
import csv
import gzip
import hashlib
import importlib.util
import io
import json
import math
import os
from pathlib import Path
import random
import shlex
import subprocess
import time


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[1]
DEFAULT_SIM = ROOT / "sim/datacenter/htsim_roce"
DEFAULT_OUT = SCRIPT_DIR / "output/mrc_sglb_feedback_transition"
SIZE_COUNTS = {
    6144: 1000,
    13312: 1000,
    19456: 1000,
    33792: 1000,
    54272: 1000,
    136192: 1000,
    683008: 300,
    1364992: 300,
    3412992: 100,
    6827008: 60,
    20480000: 30,
    30720000: 20,
}
SEEDS = (13, 29, 47)
SCHEMES = ("mrc", "sglb", "rr")

_WARMED_SPEC = importlib.util.spec_from_file_location(
    "mrc_sglb_warmed_helper",
    SCRIPT_DIR / "run_mrc_sglb_warmed_short_flow.py")
warmed = importlib.util.module_from_spec(_WARMED_SPEC)
_WARMED_SPEC.loader.exec_module(warmed)
helper = warmed.helper


def scaled_count(count, sample_scale):
    if sample_scale <= 0:
        raise ValueError("sample_scale must be positive")
    return max(1, math.ceil(count * sample_scale))


def build_transition_flows(
        seed, sample_scale=1.0, arrival_window_us=500.0):
    if sample_scale > 1.0:
        raise ValueError("sample_scale must not exceed one")
    rng = random.Random(seed)
    flows = []
    end_ps = 100_000_000 + round(
        arrival_window_us * 1_000_000 * sample_scale)
    for flow_size, base_count in SIZE_COUNTS.items():
        for _ in range(scaled_count(base_count, sample_scale)):
            src = rng.randrange(128)
            dst = rng.randrange(127)
            if dst >= src:
                dst += 1
            start_ps = rng.randrange(100_000_000, end_ps + 1)
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


def format_number(value):
    return "{:g}".format(value)


def build_command(
        sim, scheme, traffic, output, connections, seed, hotspot_rate,
        hotspot_spines=16, hotspot_on_us=1000.0):
    if scheme not in SCHEMES:
        raise ValueError("unsupported scheme " + repr(scheme))
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
        "-end", "40000",
        "-paths", "64",
        "-seed", str(seed),
        "-cc", "dctcp_variant",
        "-roce_rx_mode", "sp",
        "-roce_sack_bitmap_bits", "64",
        "-roce_transport_semantics", "mrc_exact_bounded",
        "-roce_trim_recovery", "exact",
        "-hop_latency", "0.5",
        "-switch_latency", "0.5",
        "-path_hotspot_spines", str(hotspot_spines),
        "-path_hotspot_bg_rate_gbps", format_number(hotspot_rate),
        "-path_hotspot_bg_on_us", format_number(hotspot_on_us),
        "-path_hotspot_bg_off_us", "0",
    ]


def parse_mrc_phase_diags(text):
    rows = {}
    required = {
        "flow_id", "src", "dst", "flow_size",
        "new_data_selections", "packets_before_first_update",
        "new_selections_after_first_update", "actionable_feedback",
        "quality_feedback_before_done", "effective_state_updates",
        "unique_active_evs", "full_sweeps",
    }
    for line in text.splitlines():
        if not line.startswith("MrcFlowDiag "):
            continue
        raw = dict(token.split("=", 1) for token in line.split()[1:])
        missing = required - set(raw)
        if missing:
            raise ValueError(
                "MRC phase diagnostic missing " + repr(sorted(missing)))
        flow_id = int(raw["flow_id"])
        if flow_id in rows:
            raise ValueError("duplicate MRC diagnostic flow_id")
        row = {
            field: int(raw[field])
            for field in required
            if field not in {"src", "dst", "flow_size"}
        }
        row.update({
            "src": int(raw["src"]),
            "dst": int(raw["dst"]),
            "flow_size": int(raw["flow_size"]),
        })
        rows[flow_id] = row
    return rows


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
        fcts = {}
        for scheme in SCHEMES:
            item = completions[scheme][flow_id]
            if item["src"] != spec["src"] or item["dst"] != spec["dst"]:
                raise ValueError(
                    "{} identity mismatch flow {}".format(scheme, flow_id))
            fct = item["finish_us"] - spec["start_us"]
            if fct <= 0:
                raise ValueError("non-positive paired FCT")
            fcts[scheme] = fct
        diag = mrc_diags[flow_id]
        if (
                diag["src"] != spec["src"] or
                diag["dst"] != spec["dst"] or
                diag["flow_size"] != spec["flow_size"]):
            raise ValueError("MRC diagnostic identity mismatch")
        selections = diag["new_data_selections"]
        after = diag["new_selections_after_first_update"]
        if after > selections:
            raise ValueError("post-update selections exceed total selections")
        rows.append({
            "seed": seed,
            "flow_id": flow_id,
            "src": spec["src"],
            "dst": spec["dst"],
            "start_us": spec["start_us"],
            "flow_size": spec["flow_size"],
            "mrc_fct_us": fcts["mrc"],
            "sglb_fct_us": fcts["sglb"],
            "rr_fct_us": fcts["rr"],
            "mrc_sglb_ratio": fcts["mrc"] / fcts["sglb"],
            "mrc_rr_ratio": fcts["mrc"] / fcts["rr"],
            "new_data_selections": selections,
            "packets_before_first_update":
                diag["packets_before_first_update"],
            "new_selections_after_first_update": after,
            "post_update_selection_fraction":
                after / selections if selections else 0.0,
            "actionable_feedback": diag["actionable_feedback"],
            "quality_feedback_before_done":
                diag["quality_feedback_before_done"],
            "effective_state_updates": diag["effective_state_updates"],
            "unique_active_evs": diag["unique_active_evs"],
            "full_sweeps": diag["full_sweeps"],
        })
    return rows


def geomean(values):
    return math.exp(sum(math.log(value) for value in values) / len(values))


def summarize_group(selected):
    count = len(selected)
    mrc = [row["mrc_fct_us"] for row in selected]
    sglb = [row["sglb_fct_us"] for row in selected]
    rr = [row["rr_fct_us"] for row in selected]
    return {
        "flow_count": count,
        "mrc_sglb_ratio_geomean": geomean(
            [row["mrc_sglb_ratio"] for row in selected]),
        "mrc_rr_ratio_geomean": geomean(
            [row["mrc_rr_ratio"] for row in selected]),
        "mrc_fct_p50_us": helper.percentile(mrc, 0.50),
        "sglb_fct_p50_us": helper.percentile(sglb, 0.50),
        "rr_fct_p50_us": helper.percentile(rr, 0.50),
        "mrc_fct_p99_us": helper.percentile(mrc, 0.99),
        "sglb_fct_p99_us": helper.percentile(sglb, 0.99),
        "rr_fct_p99_us": helper.percentile(rr, 0.99),
        "mrc_sglb_p99_ratio":
            helper.percentile(mrc, 0.99) / helper.percentile(sglb, 0.99),
        "mrc_rr_p99_ratio":
            helper.percentile(mrc, 0.99) / helper.percentile(rr, 0.99),
        "quality_feedback_fraction": sum(
            row["quality_feedback_before_done"] > 0
            for row in selected) / count,
        "actionable_fraction": sum(
            row["actionable_feedback"] > 0
            for row in selected) / count,
        "post_update_selection_fraction_mean": sum(
            row["post_update_selection_fraction"]
            for row in selected) / count,
        "effective_updates_mean": sum(
            row["effective_state_updates"]
            for row in selected) / count,
        "ev_coverage_mean": sum(
            row["unique_active_evs"] / 8.0
            for row in selected) / count,
        "complete_sweep_fraction": sum(
            row["full_sweeps"] > 0 for row in selected) / count,
    }


def aggregate_by_seed_and_size(rows):
    groups = {}
    for row in rows:
        groups.setdefault(
            (int(row["seed"]), int(row["flow_size"])), []).append(row)
    result = []
    for (seed, flow_size), selected in sorted(groups.items()):
        summary = summarize_group(selected)
        result.append({
            "seed": seed,
            "flow_size": flow_size,
            "flow_size_kib": flow_size / 1024.0,
            **summary,
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
        summary = summarize_group(selected)
        per_seed = by_size_seed[flow_size]
        sglb_ratios = [
            row["mrc_sglb_ratio_geomean"] for row in per_seed]
        rr_ratios = [row["mrc_rr_ratio_geomean"] for row in per_seed]
        result.append({
            "flow_size": flow_size,
            "flow_size_kib": flow_size / 1024.0,
            **summary,
            "mrc_sglb_seed_min": min(sglb_ratios),
            "mrc_sglb_seed_max": max(sglb_ratios),
            "mrc_sglb_improving_seeds": sum(
                value < 1.0 for value in sglb_ratios),
            "mrc_rr_seed_min": min(rr_ratios),
            "mrc_rr_seed_max": max(rr_ratios),
            "mrc_rr_improving_seeds": sum(
                value < 1.0 for value in rr_ratios),
        })
    return result


def deterministic_gzip_text(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + path.name + ".tmp")
    temporary.write_bytes(gzip.compress(text.encode("utf-8"), mtime=0))
    os.replace(temporary, path)


def stable_metadata(metadata):
    return {key: metadata[key] for key in sorted(metadata)}


def run_cell(
        seed, scheme, sim, out, traffic_path, connections, hotspot_rate,
        hotspot_spines, hotspot_on_us, force):
    cell = Path(out) / "raw" / "seed_{}".format(seed) / scheme
    cell.mkdir(parents=True, exist_ok=True)
    output = (cell / "logout.dat").resolve()
    command = build_command(
        Path(sim).resolve(), scheme, traffic_path.resolve(), output,
        connections, seed, hotspot_rate, hotspot_spines, hotspot_on_us)
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
                return handle.read(), stable_metadata(cached)

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
        "hot_spines {} rate {}Gbps on {}us off 0us".format(
            hotspot_spines, format_number(hotspot_rate),
            format_number(hotspot_on_us)),
    ]
    if scheme == "mrc":
        required.append("MRC: paths 64")
        if len(parse_mrc_phase_diags(text)) != connections:
            raise ValueError("MRC diagnostic count mismatch")
    elif scheme == "sglb":
        required.extend([
            "SGLB effective config: score mode nmrc_quantized_topk",
            "local quality update 1us, GCN update 5us",
        ])
        warmed.parse_sglb_route_diag(text)
    else:
        required.append(
            "RR: stateless_mrc true, physical_path_space 64, "
            "active_evs 64, ev_path_mapping encoded_identity")
    missing = [token for token in required if token not in text]
    if process.returncode or missing or len(completions) != connections:
        raise RuntimeError(
            "invalid cell seed={} scheme={} rc={} missing={} "
            "completions={}/{}".format(
                seed, scheme, process.returncode, missing,
                len(completions), connections))
    deterministic_gzip_text(stdout_path, text)
    raw.unlink()
    metadata = {
        "seed": seed,
        "scheme": scheme,
        "connections": connections,
        "hotspot_rate_gbps": hotspot_rate,
        "hotspot_spines": hotspot_spines,
        "hotspot_on_us": hotspot_on_us,
        "returncode": process.returncode,
        "valid": 1,
        "runtime_s": runtime,
        "sim_sha256": sim_sha,
        "traffic_sha256": traffic_sha,
        "command_sha256": command_sha,
    }
    helper.atomic_write_text(
        summary_path, json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    return text, stable_metadata(metadata)


def write_csv(path, rows):
    helper.atomic_write_csv(path, rows, list(rows[0]))


def write_paired_gzip(path, rows):
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
    fig, axes = plt.subplots(2, 1, figsize=(11.2, 8.7), sharex=True)
    for seed in sorted({row["seed"] for row in seed_rows}):
        selected = [row for row in seed_rows if row["seed"] == seed]
        axes[0].plot(
            sizes,
            [row["mrc_sglb_ratio_geomean"] for row in selected],
            color="#B9C2CC", linewidth=1.0, alpha=0.75)
    axes[0].plot(
        sizes,
        [row["mrc_sglb_ratio_geomean"] for row in summary],
        color="#B6403A", marker="o", linewidth=2.4,
        label="MRC / warmed SGLB")
    axes[0].plot(
        sizes,
        [row["mrc_rr_ratio_geomean"] for row in summary],
        color="#2878B5", marker="s", linewidth=2.2,
        label="MRC / RR (feedback ablation)")
    axes[0].axhline(
        1.0, color="#50555A", linestyle="--", linewidth=1.2)
    axes[0].set_ylabel("Paired FCT ratio\n(geometric mean)")
    axes[0].set_title(
        "MRC feedback transition under warmed path asymmetry",
        fontweight="bold")
    axes[0].grid(axis="y", alpha=0.25)
    axes[0].legend(frameon=False)

    axes[1].plot(
        sizes, [row["actionable_fraction"] for row in summary],
        color="#D97706", marker="o", linewidth=2.2,
        label="Flows with actionable MRC feedback")
    axes[1].plot(
        sizes,
        [row["post_update_selection_fraction_mean"] for row in summary],
        color="#5B4B9A", marker="^", linewidth=2.2,
        label="New-data selections after first update")
    axes[1].set_ylabel("Fraction")
    axes[1].yaxis.set_major_formatter(PercentFormatter(1.0))
    axes[1].set_xlabel("Flow size (KiB, log scale)")
    axes[1].set_xscale("log")
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
        0.5, 0.014,
        "SGLB: mean candidates {:.2f}/8, all-zero quality {:.1%}. "
        "Ratio >1 favors the denominator.".format(
            avg_candidates, all_zero),
        ha="center", fontsize=9.3, color="#4A5057")
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    for suffix in ("png", "pdf"):
        fig.savefig(
            Path(out) / ("focused_transition." + suffix),
            dpi=220 if suffix == "png" else None,
            facecolor="white", bbox_inches="tight",
            metadata=(
                {"CreationDate": None, "ModDate": None}
                if suffix == "pdf" else None))
    plt.close(fig)


def parse_seed_list(value):
    seeds = tuple(int(token) for token in value.split(",") if token)
    if not seeds:
        raise argparse.ArgumentTypeError("at least one seed is required")
    return seeds


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sim", type=Path, default=DEFAULT_SIM)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--hotspot-rate", type=float, default=340.0)
    parser.add_argument("--hotspot-spines", type=int, default=16)
    parser.add_argument("--hotspot-on-us", type=float, default=1000.0)
    parser.add_argument("--arrival-window-us", type=float, default=500.0)
    parser.add_argument("--sample-scale", type=float, default=1.0)
    parser.add_argument("--seeds", type=parse_seed_list, default=SEEDS)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--no-plot", action="store_true")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    traffic_by_seed = {}
    connections_by_seed = {}
    for seed in args.seeds:
        flows = build_transition_flows(
            seed, args.sample_scale, args.arrival_window_us)
        traffic_path = args.out / "traffic" / "seed_{}.cm".format(seed)
        write_traffic(traffic_path, flows)
        traffic_by_seed[seed] = traffic_path
        connections_by_seed[seed] = len(flows)

    results = {}
    specs = [
        (seed, scheme) for seed in args.seeds for scheme in SCHEMES]
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(
                run_cell, seed, scheme, args.sim, args.out,
                traffic_by_seed[seed], connections_by_seed[seed],
                args.hotspot_rate, args.hotspot_spines,
                args.hotspot_on_us, args.force): (seed, scheme)
            for seed, scheme in specs
        }
        for future in concurrent.futures.as_completed(futures):
            spec = futures[future]
            results[spec] = future.result()
            print("validated seed={} scheme={}".format(*spec), flush=True)

    all_paired = []
    cells = []
    sglb_routes = []
    for seed in args.seeds:
        traffic = helper.parse_traffic_text(
            traffic_by_seed[seed].read_text(encoding="utf-8"))
        completions = {}
        for scheme in SCHEMES:
            text, metadata = results[(seed, scheme)]
            completions[scheme] = helper.parse_completion_text(text)
            cells.append(metadata)
        diags = parse_mrc_phase_diags(results[(seed, "mrc")][0])
        all_paired.extend(pair_flows(
            seed, traffic, completions, diags))
        route = warmed.parse_sglb_route_diag(
            results[(seed, "sglb")][0])
        route["seed"] = seed
        sglb_routes.append(route)

    seed_rows = aggregate_by_seed_and_size(all_paired)
    summary = aggregate_across_seeds(all_paired, seed_rows)
    write_csv(args.out / "cells.csv", cells)
    write_csv(args.out / "seed_summary.csv", seed_rows)
    write_csv(args.out / "summary.csv", summary)
    write_csv(args.out / "sglb_route_summary.csv", sglb_routes)
    write_paired_gzip(
        args.out / "paired_flow_metrics.csv.gz", all_paired)
    if not args.no_plot:
        plot_results(summary, seed_rows, sglb_routes, args.out)
    print(
        "PASS cells={} paired={} seed_rows={} summary_rows={}".format(
            len(cells), len(all_paired), len(seed_rows), len(summary)))


if __name__ == "__main__":
    main()
