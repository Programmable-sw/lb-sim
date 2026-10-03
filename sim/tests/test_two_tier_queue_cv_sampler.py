#!/usr/bin/env python3

import re
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BINARY = ROOT / "sim/datacenter/htsim_roce"


def main():
    if not BINARY.exists():
        raise AssertionError(f"missing simulator binary: {BINARY}")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        traffic = temp / "traffic.cm"
        traffic.write_text(
            "\n".join((
                "Nodes 256",
                "Connections 4",
                "0->64 id 1 start 0 size 4194304",
                "1->65 id 2 start 0 size 4194304",
                "2->66 id 3 start 0 size 4194304",
                "3->67 id 4 start 0 size 4194304",
                "",
            )),
            encoding="ascii",
        )
        command = [
            str(BINARY),
            "-o", str(temp / "logout.dat"),
            "-tm", str(traffic),
            "-nodes", "256",
            "-conns", "4",
            "-tiers", "2",
            "-lb", "ecmp_rr",
            "-linkspeed", "400000",
            "-queue_type", "composite_ecn_lb",
            "-host_queue_type", "prio",
            "-mtu", "4096",
            "-end", "1000",
            "-paths", "8",
            "-seed", "13",
            "-cc", "dctcp_variant",
            "-roce_rx_mode", "sp",
            "-roce_sack_bitmap_bits", "64",
            "-hop_latency", "0.5",
            "-switch_latency", "0.5",
            "-queue_cv_sample_us", "100",
        ]
        completed = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=60,
            check=False,
        )
        if completed.returncode != 0:
            raise AssertionError(
                f"simulator exited {completed.returncode}:\n{completed.stdout}"
            )
        match = re.search(
            r"QueueCvDiag .*spine_queue_count=(\d+)", completed.stdout
        )
        if not match:
            raise AssertionError(
                f"missing QueueCvDiag output:\n{completed.stdout[-4000:]}"
            )
        count = int(match.group(1))
        if count <= 0:
            raise AssertionError(
                "two-tier QueueCvDiag must sample spine-to-leaf queues; "
                f"observed spine_queue_count={count}"
            )


if __name__ == "__main__":
    main()
