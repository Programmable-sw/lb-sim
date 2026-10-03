#!/usr/bin/env python3

import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BINARY = ROOT / "sim/datacenter/htsim_roce"


def run(traffic, output, scheme, extra_args=()):
    command = [
        str(BINARY), "-o", str(output), "-tm", str(traffic),
        "-nodes", "256", "-conns", "0", "-tiers", "2",
        "-lb", scheme, "-queue_type", "composite_ecn_lb",
        "-host_queue_type", "prio", "-roce_rx_mode", "sp",
        "-cc", "dctcp_variant", "-end", "1",
        "-linkspeed", "400000", "-paths", "64", *extra_args,
    ]
    return subprocess.run(
        command, cwd=ROOT, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, check=False,
    )


def require(result, text):
    if result.returncode != 0:
        raise AssertionError(result.stdout)
    if text not in result.stdout:
        raise AssertionError(f"missing {text!r} in:\n{result.stdout}")


def reject(result, text, context):
    if result.returncode == 0:
        raise AssertionError(f"{context} was unexpectedly accepted")
    if text not in result.stdout:
        raise AssertionError(
            f"{context} did not report {text!r}:\n{result.stdout}"
        )


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        traffic = temp / "empty.cm"
        traffic.write_text("Nodes 256\nConnections 0\n", encoding="utf-8")

        canonical = run(traffic, temp / "nmrc.dat", "n-mrc")
        require(
            canonical,
            "preset=n-mrc endpoint_policy=rr_cooldown "
            "all_cooling_policy=earliest network_decision=graded",
        )

        fixed = run(
            traffic, temp / "fixed.dat", "n-mrc-fixed0.5",
            ("-nmrc_absolute_threshold", "0.5",
             "-nmrc_relative_delta", "0.25"),
        )
        require(
            fixed,
            "preset=n-mrc-fixed0.5 endpoint_policy=rr_cooldown "
            "all_cooling_policy=earliest network_decision=fixed_threshold",
        )
        require(fixed, "absolute_threshold=0.5 relative_delta=0.25")

        delta = run(
            traffic, temp / "delta.dat", "n-mrc-delta",
            ("-nmrc_relative_delta", "0.25"),
        )
        require(
            delta,
            "preset=n-mrc-delta endpoint_policy=rr_cooldown "
            "all_cooling_policy=earliest network_decision=delta",
        )
        require(delta, "relative_delta=0.25")

        reject(
            run(
                traffic, temp / "bad_scope.dat", "n-mrc",
                ("-nmrc_relative_delta", "0.25"),
            ),
            "-nmrc_relative_delta requires -lb n-mrc-fixed0.5 or n-mrc-delta",
            "relative delta on canonical N-MRC",
        )
        for removed in (
            "n-mrc-allcool-rr-reset", "n-mrc1", "n-mrc2", "n-mrc4",
            "n-mrc5", "n-mrc6", "n-mrc7",
        ):
            reject(
                run(traffic, temp / f"{removed}.dat", removed),
                "Usage ",
                f"removed preset {removed}",
            )

    print("N-MRC public preset runtime compatibility passed")


if __name__ == "__main__":
    main()
