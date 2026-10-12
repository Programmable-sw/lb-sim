#!/usr/bin/env python3
"""Stream representative v6 traces to disk instead of buffering stdout."""
import hashlib, json, pathlib, subprocess

ROOT = pathlib.Path(__file__).resolve().parents[2]
BIN = ROOT / "sim/datacenter/htsim_pfc_pretrim"
OUT = ROOT / "experiments/pfc_pretrim/results/packet_sim/queue-fixed-v6-ecn-calibration/representative-trace-seed13"
OUT.mkdir(parents=True, exist_ok=True)

for policy in ("off", "paused"):
    label = f"seed-13-ecmp-jct-a2a-{policy}"
    cmd = [str(BIN), "--group", label, "--seed", "13", "--scenario", "jct_a2a",
           "--cc", "dcqcn", "--lb", "ecmp", "--policy", policy,
           "--trim-ce", "off", "--rx-mode", "pause", "--pause-us", "200",
           "--fault-start-us", "700", "--background-kib", "8192",
           "--trim-threshold-kib", "32", "--leaf-ecn-kmin-kib", "32",
           "--leaf-ecn-kmax-kib", "128", "--spine-ecn-kmin-kib", "16",
           "--spine-ecn-kmax-kib", "64", "--ecn-pmax", "0.01", "--compact"]
    with (OUT / f"{label}.trace.jsonl").open("w") as stdout, \
         (OUT / f"{label}.stderr").open("w") as stderr:
        subprocess.run(cmd, cwd=ROOT, text=True, stdout=stdout, stderr=stderr, check=True)
    (OUT / f"{label}.command.txt").write_text(" ".join(cmd) + "\n")

manifest = {"model_version": "queue-fixed-v6-ecn-calibration-trace-v2",
            "network_behavior": "identical_to_queue-fixed-v6-ecn-calibration",
            "binary_sha256": hashlib.sha256(BIN.read_bytes()).hexdigest(),
            "source_sha256": hashlib.sha256((ROOT / "sim/datacenter/main_pfc_pretrim.cpp").read_bytes()).hexdigest(),
            "trace_only_changes": ["flow roles on pacing events", "CC-low and window interval events"]}
(OUT / "BUILD_CONFIG.json").write_text(json.dumps(manifest, indent=2) + "\n")
