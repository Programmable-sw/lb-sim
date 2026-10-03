#!/usr/bin/env python3
"""Run the focused 5 ms MRC-versus-SGLB pressure-transition comparison."""

import argparse
import concurrent.futures
import csv
import gzip
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import time


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[1]
DEFAULT_SIM = ROOT / "sim/datacenter/htsim_roce"
SOURCE_OUT = SCRIPT_DIR / "output/mrc_pressure_transition_examples_5ms"
DEFAULT_OUT = SCRIPT_DIR / "output/mrc_sglb_pressure_transition_examples_5ms"
LOADS = (60, 80)
SEED = 13

TRAFFIC_RE = re.compile(
    r"^(\d+)->(\d+) id (\d+) start (\d+) size (\d+)$", re.MULTILINE
)
COMPLETION_RE = re.compile(
    r"^\.*Flow Roce_(\d+)_(\d+)\s+\d+ finished at ([0-9.]+) "
    r"total bytes (\d+) bg traffic ([01]) flowid (\d+)$",
    re.MULTILINE,
)


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_traffic_text(text):
    rows = {}
    for match in TRAFFIC_RE.finditer(text):
        src, dst, flow_id, start_ps, flow_size = map(int, match.groups())
        if flow_id in rows:
            raise ValueError("duplicate traffic flow_id {}".format(flow_id))
        rows[flow_id] = {
            "flow_id": flow_id,
            "src": src,
            "dst": dst,
            "start_us": start_ps / 1_000_000.0,
            "flow_size": flow_size,
        }
    header = re.search(r"^Connections (\d+)$", text, re.MULTILINE)
    if not header:
        raise ValueError("traffic matrix is missing Connections header")
    expected = int(header.group(1))
    if len(rows) != expected:
        raise ValueError(
            "traffic flow count {} != {}".format(len(rows), expected)
        )
    return rows


def parse_completion_text(text):
    rows = {}
    for match in COMPLETION_RE.finditer(text):
        src, dst = int(match.group(1)), int(match.group(2))
        finish_us = float(match.group(3))
        completed_bytes = int(match.group(4))
        background = int(match.group(5))
        flow_id = int(match.group(6))
        if background:
            continue
        if flow_id in rows:
            raise ValueError("duplicate completion flow_id {}".format(flow_id))
        rows[flow_id] = {
            "flow_id": flow_id,
            "src": src,
            "dst": dst,
            "finish_us": finish_us,
            "completed_bytes": completed_bytes,
        }
    return rows


def percentile(values, quantile):
    ordered = sorted(values)
    if not ordered:
        return 0.0
    position = (len(ordered) - 1) * quantile
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def pair_flows(traffic, mrc_rows, sglb_completions):
    mrc_by_id = {int(row["flow_id"]): row for row in mrc_rows}
    expected = set(traffic)
    if set(mrc_by_id) != expected:
        raise ValueError("MRC flow IDs do not match traffic")
    if set(sglb_completions) != expected:
        missing = sorted(expected - set(sglb_completions))
        extra = sorted(set(sglb_completions) - expected)
        raise ValueError(
            "SGLB flow IDs do not match traffic: missing={} extra={}".format(
                missing[:10], extra[:10]
            )
        )
    paired = []
    for flow_id in sorted(expected):
        spec = traffic[flow_id]
        mrc = mrc_by_id[flow_id]
        sglb = sglb_completions[flow_id]
        for name, row in (("MRC", mrc), ("SGLB", sglb)):
            if int(row["src"]) != spec["src"] or int(row["dst"]) != spec["dst"]:
                raise ValueError(
                    "{} identity mismatch for flow {}".format(name, flow_id)
                )
        if int(mrc["flow_size"]) != spec["flow_size"]:
            raise ValueError("MRC size mismatch for flow {}".format(flow_id))
        if not math.isclose(
                float(mrc["start_us"]), spec["start_us"],
                rel_tol=0.0, abs_tol=0.006):
            raise ValueError("MRC start mismatch for flow {}".format(flow_id))
        sglb_fct = float(sglb["finish_us"]) - spec["start_us"]
        mrc_fct = float(mrc["fct_us"])
        if sglb_fct <= 0 or mrc_fct <= 0:
            raise ValueError("non-positive FCT for flow {}".format(flow_id))
        paired.append({
            "flow_id": flow_id,
            "src": spec["src"],
            "dst": spec["dst"],
            "start_us": spec["start_us"],
            "flow_size": spec["flow_size"],
            "mrc_fct_us": mrc_fct,
            "sglb_fct_us": sglb_fct,
            "fct_ratio": mrc_fct / sglb_fct,
            "actionable_feedback": int(mrc["actionable_feedback"]),
            "quality_feedback_before_done":
                int(mrc["quality_feedback_before_done"]),
        })
    return paired


def aggregate_pairs(rows, load_pct):
    groups = {}
    for row in rows:
        groups.setdefault(int(row["flow_size"]), []).append(row)
    summary = []
    for flow_size, selected in sorted(groups.items()):
        ratios = [row["fct_ratio"] for row in selected]
        mrc_fcts = [row["mrc_fct_us"] for row in selected]
        sglb_fcts = [row["sglb_fct_us"] for row in selected]
        count = len(selected)
        summary.append({
            "load_pct": load_pct,
            "flow_size": flow_size,
            "flow_size_kib": flow_size / 1024.0,
            "flow_count": count,
            "quality_feedback_fraction": sum(
                row["quality_feedback_before_done"] > 0
                for row in selected) / count,
            "actionable_fraction": sum(
                row["actionable_feedback"] > 0
                for row in selected) / count,
            "actionable_feedback_mean": sum(
                row["actionable_feedback"] for row in selected) / count,
            "fct_ratio_geomean": math.exp(
                sum(math.log(value) for value in ratios) / count),
            "mrc_fct_mean_us": sum(mrc_fcts) / count,
            "sglb_fct_mean_us": sum(sglb_fcts) / count,
            "mrc_fct_p99_us": percentile(mrc_fcts, 0.99),
            "sglb_fct_p99_us": percentile(sglb_fcts, 0.99),
        })
    return summary


def build_sglb_command(sim, traffic, output, connections, seed=SEED):
    return [
        str(sim),
        "-o", str(output),
        "-tm", str(traffic),
        "-nodes", "128",
        "-conns", str(connections),
        "-tiers", "2",
        "-lb", "sglb",
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
        "-slow_tor_uplinks", "4",
        "-slow_tor_uplink_divisor", "2",
        "-slow_tor_uplink_select", "random-sparse",
    ]


def atomic_write_text(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", dir=path.parent,
                prefix="." + path.name + ".", suffix=".tmp",
                delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        raise


def atomic_write_csv(path, rows, fields):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", newline="", dir=path.parent,
                prefix="." + path.name + ".", suffix=".tmp",
                delete=False) as handle:
            temporary = Path(handle.name)
            writer = csv.DictWriter(
                handle, fieldnames=fields, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        raise


def traffic_path(load):
    return SOURCE_OUT / "traffic" / (
        "asymmetric_websearch_{}pct_seed13_5ms.cm".format(load)
    )


def mrc_cell_dir(load):
    return SOURCE_OUT / "raw/flow_lifetime" / (
        "focused_5ms_asymmetric_websearch_{}pct".format(load)
    ) / "mrc/seed_13"


def load_validated_mrc(load, sim_sha, traffic_sha, connections):
    cell = mrc_cell_dir(load)
    metadata = json.loads(
        (cell / "summary.json").read_text(encoding="utf-8")
    )
    expected = {
        "sim_sha256": sim_sha,
        "traffic_sha256": traffic_sha,
        "connections": connections,
        "valid": 1,
    }
    actual = {key: metadata.get(key) for key in expected}
    if actual != expected:
        raise ValueError(
            "MRC cache validation failed for load {}: {} != {}".format(
                load, actual, expected
            )
        )
    with gzip.open(cell / "flow_metrics.json.gz", "rt", encoding="utf-8") as f:
        rows = json.load(f)
    if len(rows) != connections:
        raise ValueError("MRC cached flow count mismatch")
    return rows, metadata


def sglb_cell_dir(out, load):
    return Path(out) / "raw" / (
        "focused_5ms_asymmetric_websearch_{}pct".format(load)
    ) / "sglb/seed_13"


def validate_sglb_text(text, connections):
    required = (
        "lb mode sglb",
        "cc mode dctcp_variant",
        "RoCE receive mode sp",
        "RoceTransportConfig semantics=mrc_exact_bounded",
        "SGLB effective config: score mode nmrc_quantized_topk, "
        "local quality update 1us, GCN update 5us",
        "nmrc_levels 4",
        "min choices 3",
    )
    missing = [token for token in required if token not in text]
    completions = parse_completion_text(text)
    if missing:
        raise ValueError("SGLB config diagnostics missing: " + repr(missing))
    if len(completions) != connections:
        raise ValueError(
            "SGLB completions {} != {}".format(len(completions), connections)
        )
    return completions


def run_sglb_cell(load, sim, out, force=False):
    traffic = traffic_path(load).resolve()
    traffic_text = traffic.read_text(encoding="utf-8")
    traffic_rows = parse_traffic_text(traffic_text)
    connections = len(traffic_rows)
    traffic_sha = file_sha256(traffic)
    sim_sha = file_sha256(sim)
    cell = sglb_cell_dir(out, load)
    cell.mkdir(parents=True, exist_ok=True)
    command = build_sglb_command(
        Path(sim).resolve(), traffic, (cell / "logout.dat").resolve(),
        connections,
    )
    command_text = shlex.join(command)
    command_sha = hashlib.sha256(command_text.encode()).hexdigest()
    summary_path = cell / "summary.json"
    stdout_path = cell / "stdout.log.gz"
    if not force and summary_path.exists() and stdout_path.exists():
        cached = json.loads(summary_path.read_text(encoding="utf-8"))
        if (
                cached.get("valid") == 1 and
                cached.get("sim_sha256") == sim_sha and
                cached.get("traffic_sha256") == traffic_sha and
                cached.get("command_sha256") == command_sha and
                cached.get("connections") == connections):
            with gzip.open(stdout_path, "rt", encoding="utf-8") as handle:
                text = handle.read()
            completions = validate_sglb_text(text, connections)
            return completions, cached

    atomic_write_text(cell / "command.txt", command_text + "\n")
    raw_stdout = cell / ".stdout.running"
    started = time.monotonic()
    with raw_stdout.open("w", encoding="utf-8") as handle:
        process = subprocess.run(
            command, cwd=cell, stdout=handle, stderr=subprocess.STDOUT,
            text=True,
        )
    runtime = time.monotonic() - started
    text = raw_stdout.read_text(encoding="utf-8")
    completions = validate_sglb_text(text, connections)
    if process.returncode != 0:
        raise RuntimeError(
            "SGLB load {} returned {}".format(load, process.returncode)
        )
    temporary_gzip = cell / ".stdout.log.gz.tmp"
    with gzip.open(temporary_gzip, "wt", encoding="utf-8") as handle:
        handle.write(text)
    os.replace(temporary_gzip, stdout_path)
    raw_stdout.unlink()
    metadata = {
        "load_pct": load,
        "seed": SEED,
        "scheme": "sglb",
        "connections": connections,
        "returncode": process.returncode,
        "valid": 1,
        "runtime_s": runtime,
        "sim_sha256": sim_sha,
        "traffic_sha256": traffic_sha,
        "command_sha256": command_sha,
    }
    atomic_write_text(
        summary_path, json.dumps(metadata, indent=2, sort_keys=True) + "\n"
    )
    return completions, metadata


def write_paired_gzip(path, rows):
    fields = list(rows[0])
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + path.name + ".tmp")
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    temporary.write_bytes(gzip.compress(
        buffer.getvalue().encode("utf-8"), mtime=0))
    os.replace(temporary, path)


def plot_summary(rows, out):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    colors = {60: "#2878B5", 80: "#D97706"}
    fig, axes = plt.subplots(2, 1, figsize=(11.5, 8.5), sharex=True)
    for load in LOADS:
        selected = [row for row in rows if row["load_pct"] == load]
        sizes = [row["flow_size_kib"] for row in selected]
        ratios = [row["fct_ratio_geomean"] for row in selected]
        actionable = [row["actionable_fraction"] for row in selected]
        axes[0].plot(
            sizes, ratios, marker="o", linewidth=2.2,
            color=colors[load], label="{}% load".format(load))
        axes[1].plot(
            sizes, actionable, marker="o", linewidth=2.2,
            color=colors[load], label="{}% load".format(load))
    axes[0].axhline(1.0, color="#50555A", linestyle="--", linewidth=1.2)
    axes[0].set_ylabel("MRC / SGLB paired FCT\n(geometric mean)")
    axes[0].set_title(
        "MRC versus SGLB under sustained path asymmetry", fontweight="bold")
    axes[0].grid(alpha=0.25)
    axes[0].legend(frameon=False)
    axes[1].set_ylabel("MRC flows with actionable feedback")
    axes[1].yaxis.set_major_formatter(PercentFormatter(1.0))
    axes[1].set_xlabel("Flow size (KiB, log scale)")
    axes[1].set_xscale("log")
    axes[1].set_ylim(-0.03, 1.05)
    axes[1].grid(alpha=0.25)
    axes[1].legend(frameon=False)
    fig.text(
        0.5, 0.015,
        "Ratios pair identical flows; below 1 favors MRC. "
        "Actionable feedback is an MRC-only mechanism metric.",
        ha="center", fontsize=9.5, color="#4A5057")
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    out = Path(out)
    for suffix in ("png", "pdf"):
        fig.savefig(
            out / ("focused_transition." + suffix),
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
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--no-plot", action="store_true")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    sim_sha = file_sha256(args.sim)

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(
                run_sglb_cell, load, args.sim, args.out, args.force): load
            for load in LOADS
        }
        for future in concurrent.futures.as_completed(futures):
            load = futures[future]
            results[load] = future.result()
            print("validated SGLB {}%".format(load), flush=True)

    all_summary = []
    cells = []
    all_paired = []
    for load in LOADS:
        traffic = traffic_path(load)
        traffic_sha = file_sha256(traffic)
        traffic_rows = parse_traffic_text(
            traffic.read_text(encoding="utf-8"))
        mrc_rows, mrc_metadata = load_validated_mrc(
            load, sim_sha, traffic_sha, len(traffic_rows))
        completions, sglb_metadata = results[load]
        paired = pair_flows(traffic_rows, mrc_rows, completions)
        for row in paired:
            row["load_pct"] = load
        all_paired.extend(paired)
        all_summary.extend(aggregate_pairs(paired, load))
        cells.extend([
            {
                "load_pct": load,
                "scheme": "mrc",
                "connections": len(mrc_rows),
                "valid": int(mrc_metadata["valid"]),
                "sim_sha256": sim_sha,
                "traffic_sha256": traffic_sha,
            },
            {
                "load_pct": load,
                "scheme": "sglb",
                "connections": len(completions),
                "valid": int(sglb_metadata["valid"]),
                "sim_sha256": sim_sha,
                "traffic_sha256": traffic_sha,
            },
        ])

    atomic_write_csv(
        args.out / "focused_summary.csv", all_summary,
        list(all_summary[0]))
    atomic_write_csv(args.out / "cells.csv", cells, list(cells[0]))
    write_paired_gzip(args.out / "paired_flow_metrics.csv.gz", all_paired)
    if not args.no_plot:
        plot_summary(all_summary, args.out)
    print(
        "PASS cells={} paired_flows={} summary_rows={}".format(
            len(cells), len(all_paired), len(all_summary)
        )
    )


if __name__ == "__main__":
    main()
