#!/usr/bin/env python3

import re
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BINARY = ROOT / "sim/datacenter/htsim_roce"


def require(text, needle, context):
    if needle not in text:
        raise AssertionError(f"missing {needle!r} in {context}")


def run_case(temp, mixed):
    traffic = temp / ("mixed.cm" if mixed else "plain.cm")
    flows = []
    for flow_id in range(1, 21):
        src = flow_id - 1
        dst = 64 + src
        flows.append(
            f"{src}->{dst} id {flow_id} start 0 size 4096"
        )
    traffic.write_text(
        "Nodes 256\nConnections 20\n" + "\n".join(flows) + "\n",
        encoding="utf-8",
    )
    command = [
        str(BINARY), "-o", str(temp / ("mixed.dat" if mixed else "plain.dat")),
        "-tm", str(traffic), "-nodes", "256", "-conns", "20",
        "-tiers", "2", "-lb", "reps", "-linkspeed", "400000",
        "-queue_type", "composite_ecn_lb", "-host_queue_type", "prio",
        "-mtu", "4096", "-end", "1000", "-paths", "8", "-seed", "13",
        "-cc", "dctcp_variant", "-roce_rx_mode", "sp",
        "-roce_sack_bitmap_bits", "64",
    ]
    if mixed:
        command.append("-mixed_lb_traffic")
    return subprocess.run(
        command, cwd=ROOT, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, check=False,
    )


def main():
    sources = {
        "network.h": (ROOT / "sim/network.h").read_text(encoding="utf-8"),
        "roce.h": (ROOT / "sim/roce.h").read_text(encoding="utf-8"),
        "roce.cpp": (ROOT / "sim/roce.cpp").read_text(encoding="utf-8"),
        "main_roce.cpp": (
            ROOT / "sim/datacenter/main_roce.cpp"
        ).read_text(encoding="utf-8"),
        "fat_tree_switch.cpp": (
            ROOT / "sim/datacenter/fat_tree_switch.cpp"
        ).read_text(encoding="utf-8"),
    }
    require(sources["network.h"], "set_background_traffic", "PacketFlow API")
    require(sources["network.h"], "background_traffic()", "PacketFlow API")
    require(sources["roce.h"], "set_flow_ecmp_override", "RoceSrc API")
    require(sources["roce.h"], "_flow_lb_mode", "per-flow LB state")
    require(sources["main_roce.cpp"], "-mixed_lb_traffic", "mixed CLI")
    require(
        sources["main_roce.cpp"],
        "roceSrc->set_flow_ecmp_override(c % 10 == 0)",
        "deterministic one-in-ten assignment",
    )
    require(
        sources["fat_tree_switch.cpp"],
        "pkt.flow().background_traffic()",
        "SGLB background ECMP bypass",
    )

    if not BINARY.exists():
        raise AssertionError("build sim/datacenter/htsim_roce before this test")
    with tempfile.TemporaryDirectory(prefix="mixed-lb-test-") as directory:
        temp = Path(directory)
        plain = run_case(temp, mixed=False)
        if plain.returncode != 0:
            raise AssertionError(plain.stdout)
        if "MixedLbDiag" in plain.stdout:
            raise AssertionError("disabled mode emitted mixed-LB diagnostics")
        plain_labels = re.findall(r"bg traffic (\d+)", plain.stdout)
        if plain_labels != ["0"] * 20:
            raise AssertionError(f"plain labels changed: {plain_labels!r}")

        mixed = run_case(temp, mixed=True)
        if mixed.returncode != 0:
            raise AssertionError(mixed.stdout)
        require(
            mixed.stdout,
            "MixedLbDiag enabled=on total_flows=20 main_flows=18 "
            "ecmp_background_flows=2",
            "mixed runtime diagnostics",
        )
        labels = re.findall(r"bg traffic (\d+)", mixed.stdout)
        if labels.count("1") != 2 or labels.count("0") != 18:
            raise AssertionError(f"mixed labels changed: {labels!r}")
        flow_ids = re.findall(r"bg traffic [01] flowid (\d+)", mixed.stdout)
        if sorted(map(int, flow_ids)) != list(range(1, 21)):
            raise AssertionError(f"completion flow IDs changed: {flow_ids!r}")

    print("final mixed-LB test passed")


if __name__ == "__main__":
    main()
