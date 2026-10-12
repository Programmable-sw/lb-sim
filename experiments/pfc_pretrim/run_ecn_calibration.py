#!/usr/bin/env python3
"""Run the v6 ECN calibration matrix without mutating v5 outputs."""
import argparse
import concurrent.futures
import csv
import hashlib
import json
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[2]
BIN = ROOT / "sim/datacenter/htsim_pfc_pretrim"
PROFILES = {
    "reference": (8, 16, 8, 16),
    "A": (32, 128, 16, 64),
    "B": (64, 256, 32, 128),
    "C": (128, 512, 32, 128),
}
CASES = {
    "permutation": ("permutation", 0, 1024),
    "a2a": ("a2a", 0, 1024),
    "incast": ("permutation", 12, 1024),
    "long-background": ("jct_permutation", 12, 8192),
}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_one(job):
    output, seed, profile, thresholds, pmax, case, full_trace = job
    leaf_min, leaf_max, spine_min, spine_max = thresholds
    scenario, senders, background_kib = CASES[case]
    label = f"seed-{seed}-{profile}-pmax-{pmax:g}-{case}"
    cmd = [str(BIN), "--group", label, "--seed", str(seed), "--scenario", scenario,
           "--cc", "dcqcn", "--lb", "ecmp", "--policy", "off",
           "--rx-mode", "normal", "--background-senders", str(senders),
           "--background-kib", str(background_kib),
           "--leaf-ecn-kmin-kib", str(leaf_min),
           "--leaf-ecn-kmax-kib", str(leaf_max),
           "--spine-ecn-kmin-kib", str(spine_min),
           "--spine-ecn-kmax-kib", str(spine_max), "--ecn-pmax", str(pmax),
           "--compact" if full_trace else "--metrics-only"]
    proc = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=True)
    events = [json.loads(line) for line in proc.stdout.splitlines()]
    summary = next(e for e in events if e["event"] == "summary")
    budget = next(e for e in events if e["event"] == "budget_status")
    config = next(e for e in events if e["event"] == "resolved_config")
    summary.update({k: v for k, v in budget.items() if k not in ("event", "time_us")})
    if scenario.startswith("jct_"):
        jct = next(e for e in events if e["event"] == "jct_summary")
        summary.update({k: v for k, v in jct.items() if k not in ("event", "time_us")})
        healthy_bytes = 16 * 64 * 4096 * 20
        completion_us = jct["healthy_jct_us"]
    else:
        healthy_bytes = (16 if scenario == "permutation" else 16 * 15) * 64 * 4096
        completion_us = summary["healthy_cct_us"]
    summary["healthy_goodput_gbps"] = (healthy_bytes * 8 / completion_us / 1000
                                          if completion_us else 0)
    background_bytes = senders * background_kib * 1024
    summary["background_goodput_gbps"] = (
        background_bytes * 8 / summary["background_fct_us"] / 1000
        if summary["background_fct_us"] else 0)
    summary.update(seed=seed, profile=profile, pmax=pmax, case=case,
                   leaf_kmin_kib=leaf_min, leaf_kmax_kib=leaf_max,
                   spine_kmin_kib=spine_min, spine_kmax_kib=spine_max,
                   model_version=config["model_version"], binary_sha256=sha256(BIN))
    (output / f"{label}.command.txt").write_text(" ".join(cmd) + "\n")
    (output / f"{label}.stderr").write_text(proc.stderr)
    (output / f"{label}.stdout.jsonl").write_text(proc.stdout)
    (output / f"{label}.config.json").write_text(json.dumps(config, indent=2) + "\n")
    if not budget["run_valid"]:
        raise RuntimeError(f"invalid run: {label}")
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--seed", type=int, default=13)
    parser.add_argument("--profiles", default=",".join(PROFILES))
    parser.add_argument("--pmax", type=float, default=1.0)
    parser.add_argument("--cases", default=",".join(CASES))
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--full-trace-cases", default="")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    profiles = args.profiles.split(",")
    cases = args.cases.split(",")
    traces = set(filter(None, args.full_trace_cases.split(",")))
    jobs = [(args.output, args.seed, profile, PROFILES[profile], args.pmax, case,
             case in traces) for profile in profiles for case in cases]
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        rows = list(executor.map(run_one, jobs))
    fields = sorted({key for row in rows for key in row})
    with (args.output / "summary.csv").open("w", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    manifest = {
        "model_version": "queue-fixed-v6-ecn-calibration",
        "binary": str(BIN),
        "binary_sha256": sha256(BIN),
        "source_sha256": sha256(ROOT / "sim/datacenter/main_pfc_pretrim.cpp"),
        "seed": args.seed,
        "profiles": {name: PROFILES[name] for name in profiles},
        "pmax": args.pmax,
        "cases": cases,
        "cc_model": "dcqcn_subset",
    }
    (args.output / "BUILD_CONFIG.json").write_text(json.dumps(manifest, indent=2) + "\n")
    diff = subprocess.run(["git", "diff", "--", "sim/datacenter/main_pfc_pretrim.cpp",
                           "experiments/pfc_pretrim"], cwd=ROOT, text=True,
                          capture_output=True, check=True).stdout
    (args.output / "SOURCE.diff").write_text(diff)


if __name__ == "__main__":
    main()
