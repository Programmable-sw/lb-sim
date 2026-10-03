#!/usr/bin/env python3
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SIM = ROOT / "sim/datacenter/htsim_roce"
MAIN_ROCE = ROOT / "sim/datacenter/main_roce.cpp"


def run_simulator(command, cwd):
    return subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )


def require_installed(result, expected, links_per_direction):
    if result.returncode != 0:
        raise AssertionError(result.stdout)
    message = f"SGLB background installed {expected} fixed-link sources"
    if message not in result.stdout:
        raise AssertionError(f"missing {message!r}\n{result.stdout}")
    resolved = (
        f"SGLB background links per direction {links_per_direction}"
    )
    if resolved not in result.stdout:
        raise AssertionError(f"missing {resolved!r}\n{result.stdout}")
    details = [
        line
        for line in result.stdout.splitlines()
        if line.startswith("SGLB background ") and " on " in line
    ]
    labels = [line.split(" on ", 1)[0] for line in details]
    if len(labels) != expected or len(set(labels)) != expected:
        raise AssertionError(f"background labels are not unique: {labels!r}")
    queues = [
        line.split(" on ", 1)[1].rsplit(" rate ", 1)[0]
        for line in details
    ]
    if len(queues) != expected or len(set(queues)) != expected:
        raise AssertionError(f"background queues are not unique: {queues!r}")
    if links_per_direction == 1:
        expected_labels = [
            "SGLB background tor_to_leaf",
            "SGLB background leaf_to_tor",
        ]
        if labels != expected_labels:
            raise AssertionError(
                f"default background labels changed: {labels!r}"
            )


def main():
    source = MAIN_ROCE.read_text(encoding="utf-8")
    option = "-sglb_bg_links_per_direction"
    if source.count(option) < 2:
        raise AssertionError(f"{option} must be parsed and included in usage")
    helper = source.split("static uint32_t add_sglb_background_links", 1)[1]
    helper = helper.split("static bool add_fixed_link_background", 1)[0]
    if "set<pair<BaseQueue*, Pipe*> > selected" not in helper:
        raise AssertionError("background link membership must use std::set")
    if "for (size_t link = 0; link < selected.size(); link++)" in helper:
        raise AssertionError("background link membership must not scan a vector")
    parser = source.split(
        '} else if (!strcmp(argv[i],"-sglb_bg_links_per_direction")){', 1
    )[1].split('} else if (!strcmp(argv[i],"-sglb_bg_rate_gbps")){', 1)[0]
    minus_guard = parser.find("argv[i+1][0] == '-'")
    unsigned_parse = parser.find("strtoul")
    if minus_guard < 0 or unsigned_parse < 0 or minus_guard > unsigned_parse:
        raise AssertionError("leading '-' must be rejected before strtoul")
    if not SIM.exists():
        raise AssertionError("build sim/datacenter/htsim_roce before running this test")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        traffic = temp / "empty.cm"
        traffic.write_text("Nodes 256\nConnections 0\n", encoding="utf-8")
        base_command = [
            str(SIM),
            "-o", str(temp / "logout.dat"),
            "-tm", str(traffic),
            "-nodes", "256",
            "-conns", "0",
            "-tiers", "2",
            "-lb", "sglb",
            "-queue_type", "composite_ecn_lb",
            "-host_queue_type", "prio",
            "-cc", "dctcp_variant",
            "-roce_rx_mode", "sp",
            "-roce_sack_bitmap_bits", "64",
            "-linkspeed", "1000",
            "-paths", "8",
            "-end", "2",
        ]
        command = base_command + [
            "-sglb_background",
            "-sglb_bg_rate_gbps", "1",
            "-sglb_bg_on_us", "1",
            "-sglb_bg_off_us", "1",
        ]

        require_installed(run_simulator(command, temp), 2, 1)

        scaled = command + [option, "2"]
        require_installed(run_simulator(scaled, temp), 4, 2)

        invalid_values = (
            "0",
            "-1",
            "-18446744073709551615",
            "abc",
            "4294967296",
            "18446744073709551616",
        )
        for value in invalid_values:
            result = run_simulator(command + [option, value], temp)
            if result.returncode == 0:
                raise AssertionError(f"invalid value {value!r} must fail")
            if (
                "must be an integer >= 1" not in result.stdout
                or "Usage " not in result.stdout
            ):
                raise AssertionError(result.stdout)

        result = run_simulator(base_command, temp)
        if result.returncode != 0:
            raise AssertionError(result.stdout)
        if "SGLB background links per direction" in result.stdout:
            raise AssertionError(result.stdout)

    print("SGLB background links-per-direction CLI test passed")


if __name__ == "__main__":
    main()
