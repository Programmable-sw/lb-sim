#!/usr/bin/env python3

import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BINARY = ROOT / "sim/datacenter/htsim_roce"


SUPPORTED_SCALES = {
    256: 4,
    512: 8,
    1024: 16,
    2048: 32,
    4096: 64,
    8192: 128,
}
SUPPORTED_SCALE_TEXT = "256, 512, 1024, 2048, 4096, 8192"


def run(nodes, include_nodes=True):
    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        traffic = temp / "empty.cm"
        traffic.write_text(
            f"Nodes {nodes}\nConnections 0\n", encoding="utf-8")
        command = [
            str(BINARY), "-o", str(temp / "out.dat"),
            "-tm", str(traffic), "-conns", "0", "-tiers", "2",
            "-lb", "rr", "-paths", "64",
            "-queue_type", "composite_ecn_lb",
            "-host_queue_type", "prio", "-cc", "dctcp_variant",
            "-end", "1", "-linkspeed", "400000",
        ]
        if include_nodes:
            command.extend(["-nodes", str(nodes)])
        return subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )


def verify(nodes, expected_leaves, include_nodes=True):
    result = run(nodes, include_nodes)
    if result.returncode != 0:
        raise AssertionError(result.stdout)
    expected = (
        f"Standard 2-tier leaf-spine: nodes {nodes} leaves {expected_leaves} "
        "spines 64 downlinks_per_leaf 64 uplinks_per_leaf 64"
    )
    if expected not in result.stdout:
        raise AssertionError(
            f"missing fixed 64-spine topology diagnostic:\n{result.stdout}"
        )


def verify_rejected(nodes):
    result = run(nodes)
    if result.returncode == 0:
        raise AssertionError(
            f"noncanonical generated two-tier scale {nodes} was accepted")
    expected = (
        "Generated 2-tier leaf-spine supports node counts: "
        f"{SUPPORTED_SCALE_TEXT}"
    )
    if expected not in result.stdout:
        raise AssertionError(
            f"missing supported-scale diagnostic:\n{result.stdout}")


def main():
    verify(256, 4, include_nodes=False)
    for nodes, leaves in SUPPORTED_SCALES.items():
        verify(nodes, leaves)
    verify_rejected(128)
    verify_rejected(432)


if __name__ == "__main__":
    main()
