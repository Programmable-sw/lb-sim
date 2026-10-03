#!/usr/bin/env python3
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SIM = ROOT / "sim/datacenter/htsim_roce"


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        traffic = temp / "empty.cm"
        traffic.write_text("Nodes 256\nConnections 0\n", encoding="utf-8")
        command = [
            str(SIM),
            "-o", str(temp / "logout.dat"),
            "-tm", str(traffic),
            "-nodes", "256",
            "-conns", "0",
            "-tiers", "2",
            "-lb", "ops",
            "-queue_type", "composite_ecn_lb",
            "-host_queue_type", "prio",
            "-cc", "dctcp_variant",
            "-roce_rx_mode", "sp",
            "-roce_sack_bitmap_bits", "64",
            "-linkspeed", "400000",
            "-paths", "128",
            "-end", "2",
            "-path_hotspot_spines", "2",
            "-path_hotspot_bg_rate_gbps", "1",
            "-path_hotspot_bg_on_us", "1",
            "-path_hotspot_bg_off_us", "1",
        ]
        result = subprocess.run(
            command, cwd=temp, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if result.returncode != 0:
            raise AssertionError(result.stdout)
        expected = (
            "Path hotspot background installed 16 fixed-link sources "
            "hot_spines 2 rate 1Gbps on 1us off 1us")
        if expected not in result.stdout:
            raise AssertionError(f"missing {expected!r}\n{result.stdout}")

        three_tier = list(command)
        three_tier[three_tier.index("-tiers") + 1] = "3"
        three_tier[three_tier.index("-nodes") + 1] = "128"
        result = subprocess.run(
            three_tier, cwd=temp, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if result.returncode == 0 or "only supports 2-tier" not in result.stdout:
            raise AssertionError(result.stdout)

        zero_rate = list(command)
        zero_rate[zero_rate.index("-path_hotspot_bg_rate_gbps") + 1] = "0"
        result = subprocess.run(
            zero_rate, cwd=temp, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if result.returncode == 0 or "requires positive rate and ON time" not in result.stdout:
            raise AssertionError(result.stdout)

    print("path-hotspot background CLI test passed")


if __name__ == "__main__":
    main()
