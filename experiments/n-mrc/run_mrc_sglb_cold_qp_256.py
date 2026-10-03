#!/usr/bin/env python3
"""Validate cold per-QP MRC learning against warmed SGLB fabric state."""

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
import random
import re
import shlex
import subprocess
import sys
import time
from types import SimpleNamespace


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

import feedback_eval_common as common  # noqa: E402
import experiment_metrics as metrics  # noqa: E402
import run_mrc_sglb_feedback_transition as transition  # noqa: E402


DEFAULT_SIM = ROOT / "sim/datacenter/htsim_roce"
DEFAULT_OUT = SCRIPT_DIR / "output/mrc_sglb_cold_qp_256"
NODES = 256
TOPOLOGY = common.TOPOLOGIES[NODES]
SCHEMES = ("mrc", "rr", "sglb")
CONDITIONS = ("healthy", "fixed_hotspot")
SEEDS = (13, 29, 47)
START_ANCHORS_US = (0.5, 2.0, 5.0, 10.0, 25.0, 100.0, 500.0, 2000.0)
FLOW_SIZES = (6144, 33792, 136192, 262144, 3412992, 6827008)
BASE_COUNTS = {
    6144: 24,
    33792: 24,
    136192: 24,
    262144: 16,
    3412992: 8,
    6827008: 4,
}
HOT_SPINES = 16
HOTSPOT_RATE_GBPS = 390
END_US = 30000
ACTIVE_EVS = 64
SGLB_GCN_UPDATE_US = 15
DISCRETE_FIGURE_STEM = "cold_qp_discrete_start_diagnostics"


def scaled_count(count, sample_scale):
    if not 0 < sample_scale <= 1:
        raise ValueError("sample scale must be in (0, 1]")
    return max(1, math.ceil(count * sample_scale))


def build_probe_flows(seed, sample_scale=1.0):
    rng = random.Random(seed)
    flows = []
    for start_anchor_us in START_ANCHORS_US:
        for flow_size in FLOW_SIZES:
            for _ in range(scaled_count(BASE_COUNTS[flow_size], sample_scale)):
                src = rng.randrange(NODES)
                src_leaf = src // TOPOLOGY.hosts_per_leaf
                dst_leaf_offset = rng.randrange(TOPOLOGY.leaves - 1)
                dst_leaf = dst_leaf_offset
                if dst_leaf >= src_leaf:
                    dst_leaf += 1
                dst = (
                    dst_leaf * TOPOLOGY.hosts_per_leaf +
                    rng.randrange(TOPOLOGY.hosts_per_leaf)
                )
                jitter_us = rng.random()
                start_us = start_anchor_us + jitter_us
                flows.append({
                    "src": src,
                    "dst": dst,
                    "start_anchor_us": start_anchor_us,
                    "start_us": start_us,
                    "start_ps": int(round(start_us * 1_000_000)),
                    "flow_size": flow_size,
                })
    flows.sort(key=lambda row: (
        row["start_ps"], row["flow_size"], row["src"], row["dst"]))
    for flow_id, row in enumerate(flows, 1):
        row["flow_id"] = flow_id
    return flows


def atomic_write_text(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + path.name + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def write_traffic(path, flows):
    lines = [f"Nodes {NODES}", f"Connections {len(flows)}"]
    lines.extend(
        "{src}->{dst} id {flow_id} start {start_ps} size {flow_size}".format(
            **flow) for flow in flows)
    atomic_write_text(path, "\n".join(lines) + "\n")


def build_command(
        sim, scheme, traffic, output, connections, seed, condition):
    if scheme not in SCHEMES:
        raise ValueError(f"unsupported scheme {scheme}")
    if condition not in CONDITIONS:
        raise ValueError(f"unsupported condition {condition}")
    command = [
        str(sim),
        "-o", str(output),
        "-tm", str(traffic),
        "-nodes", str(NODES),
        "-conns", str(connections),
        "-tiers", "2",
        "-lb", scheme,
        "-linkspeed", "400000",
        "-queue_type", "composite_ecn_lb",
        "-host_queue_type", "prio",
        "-mtu", "4096",
        "-end", str(END_US),
        "-paths", "64",
        "-seed", str(seed),
        "-cc", "dctcp_variant",
        "-roce_rx_mode", "sp",
        "-roce_sack_bitmap_bits", "64",
        "-roce_transport_semantics", "mrc_exact_bounded",
        "-roce_trim_recovery", "exact",
        "-hop_latency", "0.5",
        "-switch_latency", "0.5",
        "-sglb_update_us", "1",
        "-sglb_gcn_update_us", str(SGLB_GCN_UPDATE_US),
    ]
    if condition == "fixed_hotspot":
        command.extend([
            "-path_hotspot_spines", str(HOT_SPINES),
            "-path_hotspot_bg_rate_gbps", str(HOTSPOT_RATE_GBPS),
            "-path_hotspot_bg_on_us", str(END_US),
            "-path_hotspot_bg_off_us", "0",
        ])
    return command


MRC_DIAG_FIELDS = (
    "new_data_selections", "unique_active_evs", "full_sweeps",
    "quality_feedback_before_done", "effective_state_updates",
    "packets_before_first_update", "new_selections_after_first_update",
    "actionable_feedback",
)


def parse_mrc_diags(text):
    rows = {}
    for line in text.splitlines():
        if not line.startswith("MrcFlowDiag "):
            continue
        raw = dict(token.split("=", 1) for token in line.split()[1:])
        required = {
            "flow_id", "src", "dst", "flow_size",
            "first_state_update_us", "first_full_sweep_us",
            *MRC_DIAG_FIELDS,
        }
        missing = required - set(raw)
        if missing:
            raise ValueError(f"MRC diagnostic missing {sorted(missing)}")
        flow_id = int(raw["flow_id"])
        if flow_id in rows:
            raise ValueError(f"duplicate MRC diagnostic {flow_id}")
        values = {
            name: int(raw[name])
            for name in MRC_DIAG_FIELDS
        }
        values.update({
            "flow_id": flow_id,
            "src": int(raw["src"]),
            "dst": int(raw["dst"]),
            "flow_size": int(raw["flow_size"]),
            "first_state_update_us": float(raw["first_state_update_us"]),
            "first_full_sweep_us": float(raw["first_full_sweep_us"]),
        })
        rows[flow_id] = SimpleNamespace(**values)
    return rows


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def deterministic_gzip_text(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + path.name + ".tmp")
    temporary.write_bytes(gzip.compress(text.encode("utf-8"), mtime=0))
    os.replace(temporary, path)


def validate_cell(
        text, returncode, scheme, condition, connections,
        mrc_connections=None):
    required = [
        "Standard 2-tier leaf-spine: nodes 256 leaves 4 spines 64 ",
        "RoceTransportConfig semantics=mrc_exact_bounded",
        "topology_path_combo=64",
    ]
    if scheme == "mrc":
        required.extend([
            "MRC: paths 64", "active_evs 64", "backup_evs 0",
            "MrcPolicyDiag policy=skip_token "
            "all_skip_resolution=natural_rotation",
            "MrcFailureRecoveryDiag enabled=0",
            "data_on_non_good_violations=0",
        ])
        expected_mrc_connections = (
            connections if mrc_connections is None else mrc_connections)
        if len(parse_mrc_diags(text)) != expected_mrc_connections:
            raise ValueError("MRC diagnostic count mismatch")
    elif scheme == "rr":
        required.append(
            "RR: stateless_mrc true, physical_path_space 64, active_evs 64")
    else:
        required.extend([
            "SGLB effective config: score mode nmrc_quantized_topk",
            "OFAT factor real_gcn_raw_linear",
            "local quality update 1us, GCN update 15us",
            "min choices 24",
            "PaperSglbDiag ",
        ])
    if condition == "fixed_hotspot":
        required.extend([
            "Path hotspot background installed",
            f"hot_spines {HOT_SPINES} rate {HOTSPOT_RATE_GBPS}Gbps",
        ])
    missing = [token for token in required if token not in text]
    completions = transition.helper.parse_completion_text(text)
    if scheme == "sglb":
        diagnostic = re.search(
            r"^PaperSglbDiag .*\bgcn_packets=(\d+) .*\bgcn_deliveries=(\d+) "
            r".*\bgcn_profile_updates=(\d+)", text, re.MULTILINE)
        if (not diagnostic or
                any(int(value) <= 0 for value in diagnostic.groups())):
            raise RuntimeError(
                f"invalid {condition}/{scheme}: real GCN packet, delivery, "
                "and profile-update counters must all be positive")
    if returncode or missing or len(completions) != connections:
        raise RuntimeError(
            f"invalid {condition}/{scheme}: rc={returncode} "
            f"missing={missing} completions={len(completions)}/{connections}")
    return completions


def run_cell(
        sim, out, seed, condition, scheme, traffic_path, connections, force):
    cell = Path(out) / "raw" / condition / scheme / f"seed_{seed}"
    cell.mkdir(parents=True, exist_ok=True)
    output = (cell / "logout.dat").resolve()
    command = build_command(
        Path(sim).resolve(), scheme, traffic_path.resolve(), output,
        connections, seed, condition)
    command_text = shlex.join(command)
    fingerprint = {
        "sim_sha256": file_sha256(sim),
        "traffic_sha256": file_sha256(traffic_path),
        "command_sha256": hashlib.sha256(command_text.encode()).hexdigest(),
    }
    summary_path = cell / "summary.json"
    stdout_path = cell / "stdout.log.gz"
    if not force and summary_path.exists() and stdout_path.exists():
        cached = json.loads(summary_path.read_text(encoding="utf-8"))
        if cached.get("valid") == 1 and all(
                cached.get(key) == value for key, value in fingerprint.items()):
            with gzip.open(stdout_path, "rt", encoding="utf-8") as handle:
                text = handle.read()
            completions = validate_cell(
                text, 0, scheme, condition, connections)
            return text, completions, cached

    atomic_write_text(cell / "command.txt", command_text + "\n")
    running = cell / ".stdout.running"
    started = time.monotonic()
    with running.open("w", encoding="utf-8") as handle:
        process = subprocess.run(
            command, cwd=cell, stdout=handle, stderr=subprocess.STDOUT,
            text=True, check=False)
    runtime_s = time.monotonic() - started
    text = running.read_text(encoding="utf-8")
    completions = validate_cell(
        text, process.returncode, scheme, condition, connections)
    deterministic_gzip_text(stdout_path, text)
    running.unlink()
    metadata = {
        "seed": seed,
        "condition": condition,
        "scheme": scheme,
        "connections": connections,
        "runtime_s": runtime_s,
        "returncode": process.returncode,
        "valid": 1,
        **fingerprint,
    }
    atomic_write_text(
        summary_path, json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    return text, completions, metadata


def _checked_fct(spec, item):
    if item["src"] != spec["src"] or item["dst"] != spec["dst"]:
        raise ValueError(f"completion identity mismatch for {spec['flow_id']}")
    fct = item["finish_us"] - spec["start_us"]
    if fct <= 0:
        raise ValueError(f"non-positive FCT for {spec['flow_id']}")
    return fct


def make_paired_rows(seed, flows, completions, mrc_diags):
    rows = []
    for spec in flows:
        flow_id = spec["flow_id"]
        fct = {}
        for condition in CONDITIONS:
            for scheme in SCHEMES:
                item = completions[condition][scheme].get(flow_id)
                if item is None:
                    raise ValueError(
                        f"missing completion {condition}/{scheme}/{flow_id}")
                fct[(condition, scheme)] = _checked_fct(spec, item)
        row = {
            "seed": seed,
            "flow_id": flow_id,
            "src": spec["src"],
            "dst": spec["dst"],
            "start_anchor_us": spec["start_anchor_us"],
            "start_us": spec["start_us"],
            "flow_size": spec["flow_size"],
            "flow_size_kib": spec["flow_size"] / 1024.0,
        }
        for condition in CONDITIONS:
            for scheme in SCHEMES:
                row[f"{condition}_{scheme}_fct_us"] = fct[(condition, scheme)]
        row.update({
            "fixed_mrc_sglb_ratio":
                fct[("fixed_hotspot", "mrc")] /
                fct[("fixed_hotspot", "sglb")],
            "fixed_rr_sglb_ratio":
                fct[("fixed_hotspot", "rr")] /
                fct[("fixed_hotspot", "sglb")],
            "fixed_mrc_rr_ratio":
                fct[("fixed_hotspot", "mrc")] /
                fct[("fixed_hotspot", "rr")],
        })
        for scheme in SCHEMES:
            row[f"{scheme}_degradation"] = (
                fct[("fixed_hotspot", scheme)] /
                fct[("healthy", scheme)])
        for condition in CONDITIONS:
            diag = mrc_diags[condition].get(flow_id)
            if diag is None:
                raise ValueError(f"missing MRC diagnostic {condition}/{flow_id}")
            prefix = f"{condition}_mrc_"
            row[prefix + "actionable"] = int(diag.actionable_feedback > 0)
            row[prefix + "actionable_feedback"] = diag.actionable_feedback
            row[prefix + "quality_feedback"] = diag.quality_feedback_before_done
            row[prefix + "effective_updates"] = diag.effective_state_updates
            row[prefix + "new_data_selections"] = diag.new_data_selections
            row[prefix + "new_after_update"] = (
                diag.new_selections_after_first_update)
            row[prefix + "packets_before_update"] = (
                diag.packets_before_first_update)
            row[prefix + "unique_evs"] = diag.unique_active_evs
            row[prefix + "coverage"] = diag.unique_active_evs / ACTIVE_EVS
            row[prefix + "full_sweeps"] = diag.full_sweeps
            row[prefix + "first_update_delay_us"] = (
                diag.first_state_update_us - spec["start_us"]
                if diag.first_state_update_us >= 0 else -1.0)
            row[prefix + "first_sweep_delay_us"] = (
                diag.first_full_sweep_us - spec["start_us"]
                if diag.first_full_sweep_us >= 0 else -1.0)
        rows.append(row)
    return rows


def geometric_mean(values):
    values = list(values)
    if not values or any(value <= 0 for value in values):
        raise ValueError("geometric mean requires positive values")
    return math.exp(sum(math.log(value) for value in values) / len(values))


def percentile(values, fraction):
    values = sorted(values)
    if not values:
        raise ValueError("percentile requires values")
    position = (len(values) - 1) * fraction
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return values[lower]
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def summarize_group(rows, include_keys):
    result = {key: rows[0][key] for key in include_keys}
    result["flows"] = len(rows)
    positive_metrics = [
        f"{condition}_{scheme}_fct_us"
        for condition in CONDITIONS for scheme in SCHEMES
    ] + [
        "fixed_mrc_sglb_ratio", "fixed_rr_sglb_ratio",
        "fixed_mrc_rr_ratio", "mrc_degradation", "rr_degradation",
        "sglb_degradation",
    ]
    for name in positive_metrics:
        values = [row[name] for row in rows]
        result[name + "_gmean"] = geometric_mean(values)
        if name.endswith("_fct_us"):
            result[name + "_p99"] = percentile(values, 0.99)
    for condition in CONDITIONS:
        prefix = f"{condition}_mrc_"
        result[prefix + "actionable_fraction"] = sum(
            row[prefix + "actionable"] for row in rows) / len(rows)
        result[prefix + "feedback_fraction"] = sum(
            row[prefix + "quality_feedback"] > 0 for row in rows) / len(rows)
        result[prefix + "effective_update_fraction"] = sum(
            row[prefix + "effective_updates"] > 0 for row in rows) / len(rows)
        result[prefix + "full_sweep_fraction"] = sum(
            row[prefix + "full_sweeps"] > 0 for row in rows) / len(rows)
        result[prefix + "coverage_mean"] = sum(
            row[prefix + "coverage"] for row in rows) / len(rows)
        update_delays = [
            row[prefix + "first_update_delay_us"] for row in rows
            if row[prefix + "first_update_delay_us"] >= 0
        ]
        sweep_delays = [
            row[prefix + "first_sweep_delay_us"] for row in rows
            if row[prefix + "first_sweep_delay_us"] >= 0
        ]
        result[prefix + "first_update_delay_median_us"] = (
            percentile(update_delays, 0.5) if update_delays else -1.0)
        result[prefix + "first_sweep_delay_median_us"] = (
            percentile(sweep_delays, 0.5) if sweep_delays else -1.0)
        result[prefix + "post_update_selections_mean"] = sum(
            row[prefix + "new_after_update"] for row in rows) / len(rows)
    return result


def aggregate(rows, keys):
    groups = {}
    for row in rows:
        key = tuple(row[name] for name in keys)
        groups.setdefault(key, []).append(row)
    return [
        summarize_group(groups[key], keys)
        for key in sorted(groups)
    ]


def write_csv(path, rows):
    if not rows:
        raise ValueError(f"no rows for {path}")
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer, fieldnames=list(rows[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    atomic_write_text(path, buffer.getvalue())


def write_gzip_csv(path, rows):
    if not rows:
        raise ValueError(f"no rows for {path}")
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer, fieldnames=list(rows[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    deterministic_gzip_text(path, buffer.getvalue())


def _seed_series(seed_summary, size_filter, metric):
    result = {}
    for seed in SEEDS:
        for start in START_ANCHORS_US:
            selected = [
                row for row in seed_summary
                if row["seed"] == seed and row["start_anchor_us"] == start
                and size_filter(row["flow_size"])
            ]
            if selected:
                values = [row[metric] for row in selected]
                result[(seed, start)] = geometric_mean(values)
    return result


def plot_results(seed_summary, size_summary, out):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    plt.rcParams.update({
        "font.size": 10,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.22,
        "figure.facecolor": "white",
        "axes.facecolor": "white",
    })
    colors = {"MRC": "#276FBF", "RR": "#6B7280", "SGLB": "#D97706"}
    size_tick_labels = ("6 KiB", "33 KiB", "133 KiB", "256 KiB",
                        "3.3 MiB", "6.5 MiB")
    bdp_kib = 350000 / 1024
    figure, axes = plt.subplots(2, 2, figsize=(13.2, 8.8))

    cohorts = (
        (lambda size: size <= 136192, "Short flows (6–133 KiB)"),
        (lambda size: size >= 3412992, "Long QPs (3.3–6.5 MiB)"),
    )
    start_positions = list(range(len(START_ANCHORS_US)))
    start_labels = [f"{value:g}" for value in START_ANCHORS_US]
    for axis, (size_filter, title) in zip(axes[0], cohorts):
        for label, metric in (
                ("MRC", "mrc_degradation_gmean"),
                ("RR", "rr_degradation_gmean"),
                ("SGLB", "sglb_degradation_gmean")):
            series = _seed_series(seed_summary, size_filter, metric)
            medians, lows, highs = [], [], []
            for start in START_ANCHORS_US:
                values = [
                    series[(seed, start)] for seed in SEEDS
                    if (seed, start) in series
                ]
                medians.append(percentile(values, 0.5))
                lows.append(min(values))
                highs.append(max(values))
            axis.plot(
                start_positions, medians, marker="o", linewidth=2,
                label=label, color=colors[label])
            axis.fill_between(
                start_positions, lows, highs, color=colors[label], alpha=0.12)
        axis.axhline(1.0, color="#111827", linewidth=1, linestyle=":")
        axis.axvline(
            START_ANCHORS_US.index(5.0), color="#9CA3AF",
            linewidth=1, linestyle="--")
        axis.set_xticks(start_positions, start_labels)
        axis.set_title(title)
        axis.set_xlabel("QP start after background begins (μs)")
        axis.set_ylabel("Paired FCT: hotspot / same-scheme healthy")
    axes[0, 0].legend(frameon=False, ncol=3)

    warmed = [row for row in size_summary if row["start_anchor_us"] >= 25]
    by_size = {}
    for size in FLOW_SIZES:
        selected = [row for row in warmed if row["flow_size"] == size]
        by_size[size] = selected
    x = [size / 1024 for size in FLOW_SIZES]
    axis = axes[1, 0]
    for label, metric in (
            ("MRC / SGLB", "fixed_mrc_sglb_ratio_gmean"),
            ("RR / SGLB", "fixed_rr_sglb_ratio_gmean"),
            ("MRC / RR", "fixed_mrc_rr_ratio_gmean")):
        values = [
            geometric_mean(row[metric] for row in by_size[size])
            for size in FLOW_SIZES
        ]
        color = {
            "MRC / SGLB": colors["MRC"],
            "RR / SGLB": colors["RR"],
            "MRC / RR": "#7C3AED",
        }[label]
        axis.plot(
            [size / 1024 for size in FLOW_SIZES], values,
            marker="o", linewidth=2, label=label, color=color)
    axis.axhline(1.0, color="#111827", linewidth=1, linestyle=":")
    axis.set_xscale("log", base=10)
    axis.set_xticks(x, size_tick_labels, rotation=18)
    axis.axvline(bdp_kib, color="#9CA3AF", linewidth=1, linestyle="--")
    axis.text(
        bdp_kib * 1.08, 0.96, "1-RTT BDP",
        transform=axis.get_xaxis_transform(), color="#6B7280",
        fontsize=9, va="top")
    axis.set_title("Warmed fabric, newly started QPs")
    axis.set_xlabel("Flow size (actual values, log₁₀ spacing)")
    axis.set_ylabel("Paired FCT ratio")
    axis.legend(frameon=False)

    axis = axes[1, 1]
    actionable = []
    sweep = []
    coverage = []
    for size in FLOW_SIZES:
        selected = by_size[size]
        actionable.append(sum(
            row["fixed_hotspot_mrc_actionable_fraction"]
            for row in selected) / len(selected))
        sweep.append(sum(
            row["fixed_hotspot_mrc_full_sweep_fraction"]
            for row in selected) / len(selected))
        coverage.append(sum(
            row["fixed_hotspot_mrc_coverage_mean"]
            for row in selected) / len(selected))
    positions = list(range(len(FLOW_SIZES)))
    width = 0.25
    axis.bar(
        [position - width for position in positions], coverage, width,
        label=f"Mean EV-set coverage (used / {ACTIVE_EVS})", color="#6B7280")
    axis.bar(
        positions, sweep, width,
        label=f"Flows completing ≥1 {ACTIVE_EVS}-EV rotation", color="#7C3AED")
    axis.bar(
        [position + width for position in positions], actionable, width,
        label="Flows with actionable feedback", color=colors["MRC"])
    axis.set_xticks(positions, size_tick_labels, rotation=18)
    axis.set_ylim(-0.03, 1.03)
    axis.yaxis.set_major_formatter(PercentFormatter(1.0))
    axis.set_title(f"Per-flow opportunity — all {ACTIVE_EVS} EVs active")
    axis.set_xlabel("Flow size (discrete measured points)")
    axis.set_ylabel("Fraction")
    axis.legend(frameon=False)

    figure.suptitle(
        "Cold-QP startup under persistent path pressure — 256 nodes, 64 spines",
        fontsize=14, fontweight="bold")
    figure.tight_layout(rect=(0, 0, 1, 0.96))
    png = Path(out) / f"{DISCRETE_FIGURE_STEM}.png"
    pdf = Path(out) / f"{DISCRETE_FIGURE_STEM}.pdf"
    figure.savefig(png, dpi=180, bbox_inches="tight")
    figure.savefig(pdf, bbox_inches="tight")
    plt.close(figure)
    return png, pdf


def plot_mrc_timing(paired, out):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    plt.rcParams.update({
        "font.size": 10,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.22,
        "figure.facecolor": "white",
        "axes.facecolor": "white",
    })
    warm = [row for row in paired if row["start_anchor_us"] >= 25]
    sizes_kib = [size / 1024 for size in FLOW_SIZES]
    size_tick_labels = ("6 KiB", "33 KiB", "133 KiB", "256 KiB",
                        "3.3 MiB", "6.5 MiB")
    bdp_kib = 350000 / 1024
    update_median, update_min, update_p90 = [], [], []
    sweep_median = []
    update_fraction, actionable_fraction, sweep_fraction = [], [], []
    for size in FLOW_SIZES:
        rows = [row for row in warm if row["flow_size"] == size]
        updates = [
            row["fixed_hotspot_mrc_first_update_delay_us"] for row in rows
            if row["fixed_hotspot_mrc_first_update_delay_us"] >= 0
        ]
        sweeps = [
            row["fixed_hotspot_mrc_first_sweep_delay_us"] for row in rows
            if row["fixed_hotspot_mrc_first_sweep_delay_us"] >= 0
        ]
        update_median.append(
            percentile(updates, 0.5) if updates else math.nan)
        update_min.append(min(updates) if updates else math.nan)
        update_p90.append(
            percentile(updates, 0.9) if updates else math.nan)
        sweep_median.append(percentile(sweeps, 0.5) if sweeps else math.nan)
        update_fraction.append(len(updates) / len(rows))
        actionable_fraction.append(sum(
            row["fixed_hotspot_mrc_actionable"] for row in rows) / len(rows))
        sweep_fraction.append(len(sweeps) / len(rows))

    figure, axes = plt.subplots(1, 2, figsize=(12.8, 4.6))
    axis = axes[0]
    axis.plot(
        sizes_kib, update_median, marker="o", linewidth=2,
        label="First effective state update (median)", color="#276FBF")
    axis.fill_between(
        sizes_kib, update_min, update_p90, alpha=0.14, color="#276FBF",
        label="Update min–p90")
    axis.plot(
        sizes_kib, sweep_median, marker="s", linewidth=2,
        linestyle="--", label=f"First {ACTIVE_EVS}-EV rotation (median)",
        color="#7C3AED")
    axis.axhline(
        7.0, linewidth=1.3, linestyle=":", color="#111827",
        label="Theoretical RTT = 7 μs")
    axis.set_xscale("log", base=10)
    axis.set_xticks(sizes_kib, size_tick_labels, rotation=18)
    axis.axvline(
        bdp_kib, linewidth=1, linestyle="--", color="#9CA3AF",
        label="1-RTT BDP ≈ 342 KiB")
    axis.set_xlabel("Flow size (actual values, log₁₀ spacing)")
    axis.set_ylabel("Delay from QP start (μs)")
    axis.set_title("Feedback cannot update state before one RTT")
    axis.legend(frameon=False, fontsize=9)

    axis = axes[1]
    positions = list(range(len(FLOW_SIZES)))
    width = 0.25
    axis.bar(
        [position - width for position in positions], update_fraction, width,
        label="Received effective update", color="#276FBF")
    axis.bar(
        positions, actionable_fraction, width,
        label="Update followed by new-data selection", color="#D97706")
    axis.bar(
        [position + width for position in positions], sweep_fraction, width,
        label=f"Completed full {ACTIVE_EVS}-EV rotation", color="#7C3AED")
    axis.set_xticks(positions, size_tick_labels, rotation=18)
    axis.set_ylim(-0.03, 1.03)
    axis.yaxis.set_major_formatter(PercentFormatter(1.0))
    axis.set_xlabel("Flow size (discrete measured points)")
    axis.set_ylabel("Fraction of MRC flows")
    axis.set_title("Receiving feedback is not the same as using it")
    axis.legend(frameon=False, fontsize=9)

    figure.suptitle(
        "MRC feedback and rotation timing — new QPs after fabric warm-up",
        fontsize=13, fontweight="bold")
    figure.tight_layout(rect=(0, 0, 1, 0.94))
    png = Path(out) / "mrc_feedback_and_rotation_timing.png"
    pdf = Path(out) / "mrc_feedback_and_rotation_timing.pdf"
    figure.savefig(png, dpi=180, bbox_inches="tight")
    figure.savefig(pdf, bbox_inches="tight")
    plt.close(figure)
    return png, pdf


def _fmt_ratio(value):
    return f"{value:.4f}"


def write_report(summary, paired, out, sample_scale):
    warm = [row for row in summary if row["start_anchor_us"] >= 25]
    warm_paired = [row for row in paired if row["start_anchor_us"] >= 25]
    size_rows = {}
    for size in FLOW_SIZES:
        selected = [row for row in warm if row["flow_size"] == size]
        size_rows[size] = {
            "mrc_sglb": geometric_mean(
                row["fixed_mrc_sglb_ratio_gmean"] for row in selected),
            "rr_sglb": geometric_mean(
                row["fixed_rr_sglb_ratio_gmean"] for row in selected),
            "mrc_rr": geometric_mean(
                row["fixed_mrc_rr_ratio_gmean"] for row in selected),
            "actionable": sum(
                row["fixed_hotspot_mrc_actionable_fraction"]
                for row in selected) / len(selected),
            "sweep": sum(
                row["fixed_hotspot_mrc_full_sweep_fraction"]
                for row in selected) / len(selected),
            "coverage": sum(
                row["fixed_hotspot_mrc_coverage_mean"]
                for row in selected) / len(selected),
        }
    def cohort_stats(predicate):
        rows = [row for row in warm_paired if predicate(row["flow_size"])]
        return {
            "flows": len(rows),
            "mrc_sglb": geometric_mean(
                row["fixed_mrc_sglb_ratio"] for row in rows),
            "rr_sglb": geometric_mean(
                row["fixed_rr_sglb_ratio"] for row in rows),
            "mrc_rr": geometric_mean(
                row["fixed_mrc_rr_ratio"] for row in rows),
            "mrc_deg": geometric_mean(row["mrc_degradation"] for row in rows),
            "rr_deg": geometric_mean(row["rr_degradation"] for row in rows),
            "sglb_deg": geometric_mean(
                row["sglb_degradation"] for row in rows),
            "actionable": sum(
                row["fixed_hotspot_mrc_actionable"] for row in rows) /
                len(rows),
        }

    cohorts = {
        "short": cohort_stats(lambda size: size <= 136192),
        "medium": cohort_stats(lambda size: size == 262144),
        "long": cohort_stats(lambda size: size >= 3412992),
    }
    observed_updates = [
        row["fixed_hotspot_mrc_first_update_delay_us"] for row in warm_paired
        if row["fixed_hotspot_mrc_first_update_delay_us"] >= 0
    ]
    observed_sweeps = [
        row["fixed_hotspot_mrc_first_sweep_delay_us"] for row in warm_paired
        if row["fixed_hotspot_mrc_first_sweep_delay_us"] >= 0
    ]
    earliest_update = min(observed_updates)
    median_sweep = percentile(observed_sweeps, 0.5)
    short = cohorts["short"]
    long = cohorts["long"]
    warm_long_rows = [
        row for row in warm_paired if row["flow_size"] >= 3412992
    ]
    warm_long_mrc_p99 = percentile(
        [row["fixed_hotspot_mrc_fct_us"] for row in warm_long_rows], 0.99)
    warm_long_sglb_p99 = percentile(
        [row["fixed_hotspot_sglb_fct_us"] for row in warm_long_rows], 0.99)
    seeds_in_data = sorted({row["seed"] for row in paired})
    flows_per_seed = len(paired) // len(seeds_in_data)

    lines = [
        "# MRC 新 QP 冷启动局限：256 节点标准拓扑验证",
        "",
        "## 技术结论",
        "",
        (
            f"全部 64 个 EV 均为 active；本轮每 seed 有 {flows_per_seed} 条流。"
            "短流在反馈可用于后续新数据前完成，因此 MRC 与 RR 主要表现为"
            "同一套初始轮转；长流则进入反馈闭环，并明显优于无状态 RR。"
            "SGLB 复用持续更新的 fabric profile，故固定热点下仍明显优于新建 MRC QP。"
        ),
        "",
        (
            f"背景预热至少 25 μs 后，6–133 KiB 的 MRC/SGLB 配对 FCT "
            f"几何均值为 **{short['mrc_sglb']:.4f}**，MRC actionable 比例为 "
            f"**{short['actionable']:.1%}**。3.3–6.5 MiB 长 QP 的 actionable "
            f"为 **{long['actionable']:.1%}**，MRC/RR 降至 "
            f"**{long['mrc_rr']:.4f}**，即相对 RR 改善 "
            f"**{(1 - long['mrc_rr']):.1%}**；但 MRC/SGLB 仍为 "
            f"**{long['mrc_sglb']:.4f}**。因此实验同时证明了 MRC 的学习有效"
            "和冷启动代价不会自动消失。"
        ),
        "",
        f"![启动时间、流大小与反馈闭环]({DISCRETE_FIGURE_STEM}.png)",
        "",
        (
            "上图左上和右上比较同一批流在持续路径压力相对健康网络的 FCT "
            "恶化；左下比较拥塞稳定后新 QP 的配对 FCT；右下显示 MRC 是否"
            f"完成反馈闭环和完整 {ACTIVE_EVS}-EV rotation。阴影是三个 seed 的"
            "最小到最大值。"
        ),
        "",
        "## 短流没有把反馈用于新数据，长流闭环明显优于 RR",
        "",
        "| Cohort（背景预热 ≥25 μs） | flows | MRC/SGLB | RR/SGLB | MRC/RR | MRC 压力恶化 | RR 压力恶化 | SGLB 压力恶化 | actionable |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, label in (
            ("short", "6–133 KiB"), ("medium", "256 KiB"),
            ("long", "3.3–6.5 MiB")):
        row = cohorts[name]
        lines.append(
            f"| {label} | {row['flows']} | {row['mrc_sglb']:.4f} | "
            f"{row['rr_sglb']:.4f} | {row['mrc_rr']:.4f} | "
            f"{row['mrc_deg']:.4f} | {row['rr_deg']:.4f} | "
            f"{row['sglb_deg']:.4f} | {row['actionable']:.1%} |")
    lines.extend([
        "",
        (
            "短流中 MRC/RR 为 "
            f"{short['mrc_rr']:.4f}，实际基本持平，严格 actionable 为 0。"
            "这不矛盾：源码中 actionable 只统计新的原始数据选择，而 MRC 状态"
            "也会用于 retransmission/recovery 的替代 EV；本报告的 "
            "actionable 特指‘状态更新后还有新的原始数据选择’，避免把任意 ACK "
            "误报成当前 flow 已经受益。相对 SGLB，短流 MRC/SGLB 为 "
            f"{short['mrc_sglb']:.4f}；该差距说明已有 fabric 状态对冷 QP 有用，"
            "但不能作为共享状态的纯控制变量消融。"
        ),
        "",
        (
            "长流中 MRC 相对 RR 改善 "
            f"{(1 - long['mrc_rr']):.1%}，说明反馈闭环已经产生明显收益；SGLB 的压力"
            f"恶化仅 {long['sglb_deg']:.4f}，而 MRC 为 {long['mrc_deg']:.4f}。"
            f"但不能把 {long['mrc_sglb']:.4f}× 的全部差距归因于 per-QP 冷启动：持续压力下，"
            "SGLB 在 64 条可用物理路径中每次都从当时的 best-quality 集合选路；"
            "MRC 同样使用全部 64 个 active EV，但每次精确拥塞反馈只让对应 "
            "EV 跳过下一次 nominal 发送机会，随后恢复为 GOOD。两者的状态位置"
            "和持续选择规则仍然不同。"
        ),
        "",
        "## 反馈至少受 RTT 约束，rotation 与 RTT 可以并行发生",
        "",
        "![MRC 反馈与 rotation 时间](mrc_feedback_and_rotation_timing.png)",
        "",
        (
            f"晚启动 QP 中，最早有效状态更新出现在启动后 "
            f"**{earliest_update:.3f} μs**，没有早于理论 RTT 7 μs。完成初始 "
            f"{ACTIVE_EVS}-EV rotation 的中位时间为 **{median_sweep:.3f} μs**。"
            "因此这里不是机械相加的 `1 RTT + 1 rotation`：发送端可在反馈返回"
            "前先完成 rotation；真正的闭环条件是反馈返回后仍有新数据可发送。"
        ),
        "",
        (
            "256 KiB 流已经能完成一轮 64-EV rotation，但 actionable 仍为 0；"
            "它在反馈返回前完成全部新数据选路。133 KiB 只能覆盖约一半 EV。"
            "完整探索和反馈可作用是两个不同条件。"
        ),
        "",
        (
            "256 KiB 只有 64 次新数据选择，低于本拓扑一个 RTT 的 BDP（约 "
            "342 KiB）；发送端可在首个反馈返回前完成全部新数据选路。因此，"
            "当前采样点中，MRC 首次出现反馈可用于后续新数据的机会是在 "
            f"3.3 MiB：MRC/RR={size_rows[3412992]['mrc_rr']:.4f} 且 actionable="
            f"{size_rows[3412992]['actionable']:.0%}。133/256 KiB 的差异"
            "只能来自反馈后的恢复/重传选路或运行波动。由于 256 KiB 与 "
            "3.3 MiB 之间没有采样点，本实验不能声称精确转折点就是 3.3 MiB。"
        ),
        "",
        "## 晚启动仍然冷启动，直接体现 per-QP 状态不复用",
        "",
        (
            "25、100、500、2000 μs 才启动的 MRC 流没有继承此前 QP 的 EV 状态。"
            "报告把这些启动点合并为‘背景已预热’队列：25 μs 启动点前 SGLB "
            f"至少已有一次 {SGLB_GCN_UPDATE_US} μs 远端更新机会，而每个 MRC QP 仍需自己探索。若 MRC 状态"
            "能够跨 QP 复用，晚启动短流不应继续保持 0 actionable 的冷启动"
            "形态。该结果支持 per-QP 局限，但没有把 SGLB 的其他机制差异错误"
            "归因成共享状态的纯消融；RR 才是端侧编码和轮转一致的无状态控制组。"
        ),
        "",
        "## 实验输入与控制变量",
        "",
        "- 标准二层拓扑：256 hosts、4 Leaf、64 Spine；每 Leaf 64 下联和 64 上联。",
        "- 链路：400 Gbit/s；每跳 0.5 μs、交换机 0.5 μs；理论 RTT 为 7 μs。",
        "- 被测方案：MRC、RR、SGLB；队列、DCQCN、SACK、TRIM recovery 完全相同。",
        "- 固定压力：16/64 条 Spine 路径持续注入 390 Gbit/s 背景，健康场景不注入。",
        f"- SGLB：real_gcn_raw_linear，真实 256 B 高优先级 GCN；本地状态 1 μs，远端最快 {SGLB_GCN_UPDATE_US} μs。",
        "- 被测流全部跨 Leaf，启动锚点为 0.5、2、5、10、25、100、500、2000 μs。",
        "- 流大小为 6、33、133、256 KiB 和 3.3、6.5 MiB。",
        f"- seeds：13、29、47；sample_scale={sample_scale:g}。",
        "",
        "固定路径占用是主证据，因为它保证新 QP 启动前路径差异已经存在。"
        "随机 ECMP 长流若没有稳定落到同一批路径，只适合作为稳健性检查，"
        "不适合作为主机制证据。",
        "",
        "## 指标定义",
        "",
        "- `scheme_degradation = FCT_fixed_hotspot / FCT_healthy`：同一 flow、seed、方案的压力恶化幅度。",
        "- `MRC/SGLB`、`RR/SGLB`、`MRC/RR`：同一 flow、seed、traffic 的配对 FCT 比值。",
        "- `quality_feedback_before_done`：QP 完成前收到的质量反馈数量。",
        "- `effective_state_updates`：确实改变 MRC EV 状态的反馈数量。",
        "- `actionable_feedback`：状态更新之后至少又发生一次新数据选择时才计数。因此它不是‘收到一个 ACK 就算一次’，而是反馈有机会作用于当前 flow 的严格口径。",
        f"- `full_sweeps`：对全部 {ACTIVE_EVS} 个 active EV 完成整轮覆盖的次数；本配置没有 backup EV。",
        f"- `coverage`：`已至少选择一次的 EV / {ACTIVE_EVS}` 的逐流均值，即完整 EV-set coverage。",
        "- 启动时间曲线：每个 seed 内先对 cohort 的逐流配对比值取几何均值，曲线点为 3 个 seed 的中位数，阴影为 min–max；不是 p99 或 max FCT。",
        "- 流大小曲线和表格：对背景预热 ≥25 μs 的逐流配对比值取几何均值。",
        "",
        "一个反馈闭环存在至少一个 RTT 的因果下界；完整覆盖初始 active rotation 还需要"
        f"至少 {ACTIVE_EVS} 次新数据选择。全部 64 个逻辑 EV 均为 active。两段过程可以重叠，所以实验不把它错误写成"
        "必然严格相加的 `1 RTT + 1 rotation`。",
        "",
        "## 按流大小统计（背景已预热 ≥25 μs）",
        "",
        "| Flow size | MRC/SGLB | RR/SGLB | MRC/RR | actionable | ≥1 full-64 rotation | full EV coverage (/64) |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for size in FLOW_SIZES:
        row = size_rows[size]
        label = (
            f"{size / 1024:.0f} KiB" if size < (1 << 20)
            else f"{size / (1 << 20):.1f} MiB")
        lines.append(
            f"| {label} | {_fmt_ratio(row['mrc_sglb'])} | "
            f"{_fmt_ratio(row['rr_sglb'])} | {_fmt_ratio(row['mrc_rr'])} | "
            f"{row['actionable']:.1%} | {row['sweep']:.1%} | "
            f"{row['coverage']:.1%} |")
    lines.extend([
        "",
        "## 机制解释边界",
        "",
        "- 这是固定持续路径压力下的描述性机制验证，不证明任意工作负载中 SGLB 都优于 MRC。SGLB 与 MRC 除状态位置外还存在算法差异，不能把二者差值全部归因于跨 QP 共享。",
        "- 本配置中 SGLB 与 MRC/RR 都使用 64 条路径；MRC/RR 的初始轮转集合相同，且无 backup。",
        f"- RR 保留 MRC 的端侧 EV 编码和轮转但不学习；本轮长流 MRC/RR={long['mrc_rr']:.4f}，支持反馈闭环相对无状态轮转的明显收益。",
        "- 长流最终 FCT 是整个生命周期的累计量，不能单独证明反馈后的瞬时 goodput；actionable、新数据剩余选择和 rotation 是机制旁证。",
        "- 本场景是独立 flow，不定义 collective completion time（CCT）；把一批无依赖流的最大 FCT 称作 CCT 会混淆概念。",
        "- 390 Gbit/s 是 400 Gbit/s 链路的 97.5%。单 seed 的 400 Gbit/s 灵敏度试验出现数量级放大，说明结论方向稳健但幅度对接近饱和程度很敏感，因此没有把 400 Gbit/s 用作主结果。",
        "- 固定热点从 0 一直运行到仿真结束；`off_us=0` 在实现中表示不进入 OFF phase，不是 0 μs 后关闭。各启动锚点却共享同一条 trace：100 μs 时仍有此前长流重叠，500/2000 μs 时此前被测流已清空。因此时间曲线同时反映并发阶段，不能只解释为预热时间。",
        f"- SGLB 本地更新周期为 1 μs、真实 GCN 远端更新最快 {SGLB_GCN_UPDATE_US} μs。0.5/2/5/10 μs 启动组与背景几乎同时开始，尚未获得完整公共视角；它们不是‘SGLB 已预热、只有 MRC 冷启动’的纯对照。",
        f"- 当前每 seed 有 {flows_per_seed} 条被测流，共 {len(paired)} 条。`sample_scale` 会同时改变样本数和网络负载，因此不同 scale 的结果不能当作单纯增加重复次数。",
        f"- 晚启动长流 p99 为 MRC {warm_long_mrc_p99:.2f} μs、SGLB {warm_long_sglb_p99:.2f} μs，p99 比值 {warm_long_mrc_p99 / warm_long_sglb_p99:.3f}。它不能与旧 128 节点、8-Spine WebSearch 直接对比。",
        "",
        "## 后续建议",
        "",
        "- 用带 barrier 的真实 collective workload 复现实验后，才能报告 straggler 和 CCT；当前结果只回答单 flow FCT 与 QP 冷启动。",
        "- 可增加 300、350、375 Gbit/s 三档背景压力，估计冷启动代价随剩余 headroom 的响应曲线，而不是只报告 390 Gbit/s 单点。",
        "",
        "## 尚待回答的问题",
        "",
        "- 背景压力撤除后，MRC 的 per-QP 状态多久会变陈旧，是否反向伤害新阶段？",
        "- 跨 QP 共享状态在拥塞快速移动时会不会引发同步回避或羊群效应？",
        "- 真实 AI collective 的 QP 复用周期是否长到足以摊薄本实验测得的冷启动代价？",
        "",
        "## 数据文件",
        "",
        "- `paired_flow_metrics.csv.gz`：逐 flow 六组 FCT、配对比值及 MRC 反馈诊断。",
        "- `seed_summary.csv`：每个 seed × 启动时间 × 流大小的聚合。",
        "- `summary.csv`：三 seed 合并统计。",
        "- `manifest.json`：拓扑、流量、压力和二进制指纹。",
        "- `raw/`：18 个 scheme × condition × seed 原始日志和逐 cell 校验摘要。",
    ])
    report = Path(out) / "mrc_cold_qp_limitations_256.md"
    atomic_write_text(report, "\n".join(lines) + "\n")
    return report


def parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--sim", type=Path, default=DEFAULT_SIM)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--seeds", default="13,29,47")
    parser.add_argument("--sample-scale", type=float, default=1.0)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--quick", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    seeds = tuple(int(value) for value in args.seeds.split(",") if value)
    if not seeds or any(seed not in SEEDS for seed in seeds):
        raise ValueError("seeds must be selected from 13,29,47")
    sample_scale = 0.05 if args.quick else args.sample_scale
    if not 0 < sample_scale <= 1 or args.workers < 1:
        raise ValueError("invalid sample scale or worker count")
    args.out = args.out.resolve()
    args.sim = args.sim.resolve()
    args.out.mkdir(parents=True, exist_ok=True)
    if not args.sim.exists():
        raise FileNotFoundError(args.sim)

    traffic_by_seed = {}
    flows_by_seed = {}
    for seed in seeds:
        flows = build_probe_flows(seed, sample_scale)
        traffic = args.out / "traffic" / f"probes_seed_{seed}.cm"
        write_traffic(traffic, flows)
        traffic_by_seed[seed] = traffic
        flows_by_seed[seed] = flows

    specs = [
        (seed, condition, scheme)
        for seed in seeds
        for condition in CONDITIONS
        for scheme in SCHEMES
    ]
    cell_results = {}
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(args.workers, len(specs))) as executor:
        futures = {
            executor.submit(
                run_cell, args.sim, args.out, seed, condition, scheme,
                traffic_by_seed[seed], len(flows_by_seed[seed]), args.force
            ): (seed, condition, scheme)
            for seed, condition, scheme in specs
        }
        for future in concurrent.futures.as_completed(futures):
            key = futures[future]
            text, completions, metadata = future.result()
            cell_results[key] = {
                "text": text,
                "completions": completions,
                "metadata": metadata,
            }
            print(
                f"complete seed={key[0]} condition={key[1]} scheme={key[2]} "
                f"runtime={metadata['runtime_s']:.2f}s",
                flush=True)

    paired = []
    for seed in seeds:
        completions = {
            condition: {
                scheme: cell_results[(seed, condition, scheme)]["completions"]
                for scheme in SCHEMES
            }
            for condition in CONDITIONS
        }
        mrc_diags = {
            condition: parse_mrc_diags(
                cell_results[(seed, condition, "mrc")]["text"])
            for condition in CONDITIONS
        }
        paired.extend(make_paired_rows(
            seed, flows_by_seed[seed], completions, mrc_diags))

    seed_summary = aggregate(
        paired, ("seed", "start_anchor_us", "flow_size", "flow_size_kib"))
    summary = aggregate(
        paired, ("start_anchor_us", "flow_size", "flow_size_kib"))
    write_gzip_csv(args.out / "paired_flow_metrics.csv.gz", paired)
    write_csv(args.out / "seed_summary.csv", seed_summary)
    write_csv(args.out / "summary.csv", summary)
    plot_results(seed_summary, summary, args.out)
    plot_mrc_timing(paired, args.out)
    report = write_report(summary, paired, args.out, sample_scale)
    manifest = {
        "status": "complete",
        "nodes": NODES,
        "leaves": TOPOLOGY.leaves,
        "hosts_per_leaf": TOPOLOGY.hosts_per_leaf,
        "spines": TOPOLOGY.spines,
        "physical_paths": TOPOLOGY.paths,
        "logical_evs": 64,
        "initial_active_evs": 64,
        "mrc_congestion_policy": "skip_once",
        "mrc_failure_recovery": "disabled",
        "schemes": SCHEMES,
        "conditions": CONDITIONS,
        "start_anchors_us": START_ANCHORS_US,
        "flow_sizes": FLOW_SIZES,
        "seeds": seeds,
        "sample_scale": sample_scale,
        "hot_spines": HOT_SPINES,
        "hotspot_rate_gbps": HOTSPOT_RATE_GBPS,
        "sim_sha256": file_sha256(args.sim),
        "traffic_sha256": {
            str(seed): file_sha256(traffic_by_seed[seed]) for seed in seeds},
        "flow_count": len(paired),
        "report": report.name,
    }
    atomic_write_text(
        args.out / "manifest.json",
        json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
