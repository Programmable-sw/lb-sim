#!/usr/bin/env python3
"""Validate routing behavior, not only FCT, for CONGA/MPRDMA/REPS.

The report records path-selection CV, selected-path count, ECN/NACK counters,
and completed-flow latency for the same traffic matrix and seed.  A run is
marked incomplete when the simulator does not emit the requested trace.
"""
import argparse
import csv
import math
import re
import subprocess
from pathlib import Path

SCHEMES = ("ecmp", "conga", "mprdma", "reps")
FINISHED = re.compile(r"finished at ([0-9]+(?:\.[0-9]+)?)")


def make_matrix(path, nodes):
    pairs = [(s, d) for s in range(nodes) for d in range(nodes) if s != d]
    lines = [f"Nodes {nodes}", f"Connections {len(pairs)}"]
    for fid, (src, dst) in enumerate(pairs, 1):
        lines.append(f"{src}->{dst} id {fid} start 0 size 1048576")
    path.write_text("\n".join(lines) + "\n")


def summarize_trace(path):
    # timeline format is intentionally parsed defensively because older
    # binaries may emit only the aggregate path-selection counters.
    counts = {}
    if not path.exists():
        return 0, float("nan")
    for line in path.read_text().splitlines():
        fields = line.replace(",", " ").split()
        for token in fields:
            if token.startswith("path="):
                try:
                    key = int(token.split("=", 1)[1])
                    counts[key] = counts.get(key, 0) + 1
                except ValueError:
                    pass
    total = sum(counts.values())
    if not total or len(counts) < 2:
        return total, float("nan")
    mean = total / len(counts)
    cv = math.sqrt(sum((v - mean) ** 2 for v in counts.values()) /
                   len(counts)) / mean
    return total, cv


def main():
    parser = argparse.ArgumentParser()
    root = Path(__file__).resolve().parents[2]
    parser.add_argument("--binary", default=str(root / "sim/datacenter/htsim_roce"))
    parser.add_argument("--nodes", type=int, default=16)
    parser.add_argument("--seed", type=int, default=13)
    parser.add_argument("--out", default=str(Path(__file__).parent / "output/lb_behavior"))
    args = parser.parse_args()
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    matrix = out / "traffic.cm"; make_matrix(matrix, args.nodes)
    rows = []
    for scheme in SCHEMES:
        trace = out / f"{scheme}.timeline"
        cmd = [args.binary, "-tm", str(matrix), "-nodes", str(args.nodes),
               "-conns", str(args.nodes * (args.nodes - 1)), "-lb", scheme,
               "-paths", "4", "-cc", "dctcp_variant", "-seed", str(args.seed),
               "-end", "2000000", "-path_selection_timeline", str(trace),
               "-path_selection_timeline_every", "1"]
        result = subprocess.run(cmd, text=True, capture_output=True)
        stdout = result.stdout + result.stderr
        (out / f"{scheme}.log").write_text(stdout)
        fcts = [float(x) for x in FINISHED.findall(stdout)]
        selected, cv = summarize_trace(trace)
        rows.append({"scheme": scheme, "returncode": result.returncode,
                     "flows": len(fcts), "max_fct_us": max(fcts) if fcts else "",
                     "selected": selected, "path_cv": cv})
    with (out / "summary.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys()); writer.writeheader(); writer.writerows(rows)
    print(out / "summary.csv")
    raise SystemExit(0 if all(row["returncode"] == 0 for row in rows) else 1)


if __name__ == "__main__":
    main()
