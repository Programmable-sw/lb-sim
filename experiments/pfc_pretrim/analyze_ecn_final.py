#!/usr/bin/env python3
"""Create paired v6 ECN/JCT and trim-threshold summaries."""
import csv, math, pathlib, statistics

ROOT = pathlib.Path(__file__).resolve().parent / "results/packet_sim/queue-fixed-v6-ecn-calibration"


def read(path):
    with path.open() as source: return list(csv.DictReader(source))


def paired(rows, keys, key_values):
    result = []
    for values in key_values:
        selected = [r for r in rows if all(r[k] == v for k, v in zip(keys, values))]
        pairs = []
        for seed in range(1, 21):
            by_policy = {r["policy"]: r for r in selected if int(r["seed"]) == seed}
            pairs.append((by_policy["off"], by_policy["paused"]))
        completion = lambda row: float(row.get("healthy_jct_us") or row["healthy_cct_us"])
        savings = [completion(off) - completion(paused)
                   for off, paused in pairs]
        ci = 2.093 * statistics.stdev(savings) / math.sqrt(20)
        metric = lambda policy, field: statistics.mean(float(pair[policy][field]) for pair in pairs)
        result.append(dict(zip(keys, values), n=20,
            off_completion_us=statistics.mean(completion(pair[0]) for pair in pairs),
            paused_completion_us=statistics.mean(completion(pair[1]) for pair in pairs),
            paired_saving_mean_us=statistics.mean(savings), paired_saving_ci95_us=ci,
            paired_saving_min_us=min(savings), paired_saving_max_us=max(savings),
            off_background_fct_us=metric(0, "background_fct_us"),
            paused_background_fct_us=metric(1, "background_fct_us"),
            paused_retransmit_bytes=metric(1, "retransmit_bytes")))
    return result


def write(path, rows):
    with path.open("w", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=list(rows[0])); writer.writeheader(); writer.writerows(rows)


jct = read(ROOT / "jct-paired-20-final/summary.csv")
jct_rows = paired(jct, ("lb", "workload"),
                  [(lb, workload) for lb in ("ecmp", "ops", "hybrid")
                   for workload in ("jct-a2a", "jct-permutation")])
write(ROOT / "jct-paired-20-final/paired-summary.csv", jct_rows)

trim = read(ROOT / "trim-paired-20-final/summary.csv")
trim_rows = paired(trim, ("threshold_kib",), [(str(t),) for t in (8, 32, 128)])
write(ROOT / "trim-paired-20-final/paired-summary.csv", trim_rows)

