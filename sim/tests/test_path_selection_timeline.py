#!/usr/bin/env python3

import csv
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
        trace = temp / "path_timeline.csv"
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
            "-lb", "avail",
            "-linkspeed", "400000",
            "-queue_type", "composite_ecn_lb",
            "-host_queue_type", "prio",
            "-mtu", "4096",
            "-end", "1000",
            "-paths", "64",
            "-seed", "13",
            "-cc", "dctcp_variant",
            "-roce_rx_mode", "sp",
            "-roce_sack_bitmap_bits", "64",
            "-queue_cv_sample_us", "100",
            "-path_selection_timeline", str(trace),
            "-path_selection_timeline_every", "1000",
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
        if not trace.exists():
            raise AssertionError("path-selection timeline was not created")

        rows = list(csv.DictReader(trace.open(encoding="utf-8")))
        if len(rows) < 2:
            raise AssertionError(f"expected multiple timeline rows, got {len(rows)}")
        expected = {"time_us", "selected_total", "cumulative_cv"}
        expected.update(f"path_{index}" for index in range(64))
        if set(rows[0]) != expected:
            raise AssertionError(f"unexpected timeline columns: {rows[0].keys()}")

        previous_total = 0
        for row in rows:
            total = int(row["selected_total"])
            counts = [int(row[f"path_{index}"]) for index in range(64)]
            if total <= previous_total:
                raise AssertionError("selected_total must increase monotonically")
            if sum(counts) != total:
                raise AssertionError(
                    f"path counts {sum(counts)} do not equal total {total}"
                )
            if float(row["cumulative_cv"]) < 0:
                raise AssertionError("cumulative CV must be non-negative")
            previous_total = total
        if float(rows[-1]["time_us"]) >= 1000:
            raise AssertionError(
                "final partial snapshot must use the last selection time, "
                "not the simulation end time"
            )

        baseline_command = command[:-4]
        baseline_command[baseline_command.index("-o") + 1] = str(
            temp / "baseline_logout.dat"
        )
        baseline = subprocess.run(
            baseline_command,
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=60,
            check=False,
        )
        if baseline.returncode != 0:
            raise AssertionError(baseline.stdout)
        traced_flows = [
            line for line in completed.stdout.splitlines()
            if line.startswith("Flow ") and " finished at " in line
        ]
        baseline_flows = [
            line for line in baseline.stdout.splitlines()
            if line.startswith("Flow ") and " finished at " in line
        ]
        if traced_flows != baseline_flows:
            raise AssertionError("timeline observer changed flow completion output")
        traced_diag = next(
            line for line in completed.stdout.splitlines()
            if line.startswith("PathSelectDiag ")
        )
        baseline_diag = next(
            line for line in baseline.stdout.splitlines()
            if line.startswith("PathSelectDiag ")
        )
        if traced_diag != baseline_diag:
            raise AssertionError("timeline observer changed path selections")

    print("Path-selection timeline test passed")


if __name__ == "__main__":
    main()
