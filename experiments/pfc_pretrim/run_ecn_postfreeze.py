#!/usr/bin/env python3
"""Run v6 post-freeze policy, CE-preservation, and trim-threshold comparisons."""
import argparse, concurrent.futures, csv, hashlib, json, pathlib, subprocess

ROOT = pathlib.Path(__file__).resolve().parents[2]
BIN = ROOT / "sim/datacenter/htsim_pfc_pretrim"
ECN = ["--leaf-ecn-kmin-kib", "32", "--leaf-ecn-kmax-kib", "128",
       "--spine-ecn-kmin-kib", "16", "--spine-ecn-kmax-kib", "64",
       "--ecn-pmax", "0.01"]


def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()


def one(job):
    out, seed, lb, workload, policy, trim_ce, threshold, full = job
    scenario = "jct_" + workload if workload.startswith("jct-") else workload
    if workload.startswith("jct-"): scenario = "jct_" + workload[4:]
    jct = scenario.startswith("jct_")
    label = f"seed-{seed}-{lb}-{workload}-{policy}-ce-{trim_ce}-t{threshold}"
    cmd = [str(BIN), "--group", label, "--seed", str(seed), "--scenario", scenario,
           "--cc", "dcqcn", "--lb", lb, "--policy", policy, "--trim-ce", trim_ce,
           "--rx-mode", "pause", "--pause-us", "200",
           "--fault-start-us", "700" if jct else "50",
           "--background-kib", "8192" if jct else "1024",
           "--trim-threshold-kib", str(threshold)] + ECN + ["--compact" if full else "--metrics-only"]
    proc = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=True)
    events = [json.loads(line) for line in proc.stdout.splitlines()]
    summary = next(e for e in events if e["event"] == "summary")
    budget = next(e for e in events if e["event"] == "budget_status")
    config = next(e for e in events if e["event"] == "resolved_config")
    summary.update({k: v for k, v in budget.items() if k not in ("event", "time_us")})
    if jct:
        report = next(e for e in events if e["event"] == "jct_summary")
        summary.update({k: v for k, v in report.items() if k not in ("event", "time_us")})
    summary.update(seed=seed, lb=lb, workload=workload, policy=policy,
                   trim_ce=trim_ce, threshold_kib=threshold,
                   model_version=config["model_version"], binary_sha256=digest(BIN))
    (out / f"{label}.command.txt").write_text(" ".join(cmd) + "\n")
    (out / f"{label}.stderr").write_text(proc.stderr)
    (out / f"{label}.stdout.jsonl").write_text(proc.stdout)
    if not budget["run_valid"]: raise RuntimeError(label)
    return summary


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output", type=pathlib.Path, required=True)
    p.add_argument("--seeds", default="13")
    p.add_argument("--lbs", default="ecmp,ops,hybrid")
    p.add_argument("--workloads", default="jct-a2a,jct-permutation")
    p.add_argument("--policies", default="off,paused")
    p.add_argument("--trim-ces", default="off")
    p.add_argument("--thresholds", default="32")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--full", action="store_true")
    a = p.parse_args(); a.output.mkdir(parents=True, exist_ok=True)
    jobs = [(a.output, seed, lb, w, policy, ce, int(t), a.full)
            for seed in map(int, a.seeds.split(",")) for lb in a.lbs.split(",")
            for w in a.workloads.split(",") for policy in a.policies.split(",")
            for ce in a.trim_ces.split(",") for t in a.thresholds.split(",")
            if policy != "off" or ce == "off"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=a.workers) as ex:
        rows = list(ex.map(one, jobs))
    fields = sorted({k for r in rows for k in r})
    with (a.output / "summary.csv").open("w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=fields); wr.writeheader(); wr.writerows(rows)
    manifest = {"model_version": "queue-fixed-v6-ecn-calibration",
                "binary_sha256": digest(BIN), "source_sha256": digest(ROOT / "sim/datacenter/main_pfc_pretrim.cpp"),
                "ecn": {"leaf_kmin_kib": 32, "leaf_kmax_kib": 128,
                        "spine_kmin_kib": 16, "spine_kmax_kib": 64, "pmax": 0.01},
                "cc_model": "dcqcn_subset"}
    (a.output / "BUILD_CONFIG.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__": main()
