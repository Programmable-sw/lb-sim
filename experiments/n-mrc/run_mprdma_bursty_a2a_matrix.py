#!/usr/bin/env python3
"""Run the MP-RDMA collective-communication short-flow load matrix."""
import argparse
import csv
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", type=int, default=16)
    parser.add_argument("--long-size", type=int, default=1024 * 1024)
    parser.add_argument("--loads", default="0.00,0.05,0.10,0.15,0.20")
    parser.add_argument("--seeds", default="13,29,47")
    parser.add_argument("--out", default="output/mprdma_bursty_a2a_matrix")
    args = parser.parse_args()
    root = Path(args.out)
    root.mkdir(parents=True, exist_ok=True)
    script = Path(__file__).with_name("run_mprdma_bursty_a2a.py")
    rows = []
    for load_text in args.loads.split(","):
        load = float(load_text)
        for seed_text in args.seeds.split(","):
            seed = int(seed_text)
            run_dir = root / f"load_{load:.2f}" / f"seed_{seed}"
            subprocess.run([
                "python3", str(script), "--nodes", str(args.nodes),
                "--long-size", str(args.long_size), "--short-load", str(load),
                "--seed", str(seed), "--out", str(run_dir),
            ], check=True)
    for summary in sorted(root.glob("load_*/seed_*/summary.csv")):
        load = summary.parent.parent.name.split("_", 1)[1]
        seed = int(summary.parent.name.split("_", 1)[1])
        with summary.open(newline="") as handle:
            for row in csv.DictReader(handle):
                rows.append({"load": load, "seed": seed,
                             "scheme": row["scheme"],
                             "job_ct_us": row["job_ct_us"]})
    with (root / "results.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["load", "seed", "scheme", "job_ct_us"])
        writer.writeheader()
        writer.writerows(rows)
    print(root / "results.csv")


if __name__ == "__main__":
    main()
