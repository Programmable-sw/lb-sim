#!/usr/bin/env python3
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SIM = ROOT / "sim/datacenter/htsim_roce"
MAIN_ROCE = ROOT / "sim/datacenter/main_roce.cpp"
MRC_PHYSICAL_HISTOGRAMS = (
    "ecn_physical_hist",
    "trim_physical_hist",
    "nack_physical_hist",
    "ooo_nack_physical_hist",
    "loss_nack_physical_hist",
)


def diag_fields(output, prefix):
    lines = [line for line in output.splitlines() if line.startswith(prefix + " ")]
    if len(lines) != 1:
        raise AssertionError(f"expected one {prefix} line, found {len(lines)}\n{output}")
    return dict(token.split("=", 1) for token in lines[0].split()[1:])


def assert_formatter_contract():
    source = MAIN_ROCE.read_text(encoding="utf-8")
    formatter = source.split("static string format_u32_u64_hist", 1)[1]
    formatter = formatter.split("static string format_top_u32_u64_hist", 1)[0]
    if 'return "none";' not in formatter:
        raise AssertionError("empty histograms must format as literal 'none'")
    if 'out << "/";' not in formatter or '<< ":" <<' not in formatter:
        raise AssertionError("nonempty histogram grammar must remain key:value/key:value")

    expected_callers = (
        "buffer_occupancy_hist=\" << format_u32_u64_hist(reps_buffer_occupancy_hist)",
        "ecn_physical_hist=\" << format_u32_u64_hist(mrc_ecn_physical_hist)",
        "trim_physical_hist=\" << format_u32_u64_hist(mrc_trim_physical_hist)",
        "nack_physical_hist=\" << format_u32_u64_hist(mrc_nack_physical_hist)",
        "ooo_nack_physical_hist=\" << format_u32_u64_hist(mrc_ooo_nack_physical_hist)",
        "loss_nack_physical_hist=\" << format_u32_u64_hist(mrc_loss_nack_physical_hist)",
        "physical_mod_hist=\" << format_u32_u64_hist(selected_physical_hist)",
    )
    for caller in expected_callers:
        if caller not in source:
            raise AssertionError(f"diagnostic histogram bypasses shared formatter: {caller}")


def main():
    if not SIM.exists():
        raise AssertionError("build sim/datacenter/htsim_roce before running this test")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        traffic = temp / "empty.cm"
        traffic.write_text("Nodes 256\nConnections 0\n", encoding="utf-8")
        result = subprocess.run(
            [
                str(SIM),
                "-o", str(temp / "logout.dat"),
                "-tm", str(traffic),
                "-nodes", "256",
                "-conns", "0",
                "-tiers", "2",
                "-lb", "mrc",
                "-queue_type", "composite_ecn_lb",
                "-host_queue_type", "prio",
                "-cc", "dctcp_variant",
                "-roce_rx_mode", "sp",
                "-roce_sack_bitmap_bits", "64",
                "-linkspeed", "1000",
                "-paths", "8",
                "-end", "2",
            ],
            cwd=temp,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
            timeout=30,
        )
        if result.returncode != 0:
            raise AssertionError(result.stdout)

        fields = diag_fields(result.stdout, "MrcDiag")
        for name in MRC_PHYSICAL_HISTOGRAMS:
            if fields.get(name) != "none":
                raise AssertionError(
                    f"expected MrcDiag {name}=none, got {fields.get(name)!r}\n"
                    f"{result.stdout}"
                )
        reps_fields = diag_fields(result.stdout, "RepsLikeDiag")
        if reps_fields.get("buffer_occupancy_hist") != "none":
            raise AssertionError(result.stdout)
        path_fields = diag_fields(result.stdout, "PathSelectDiag")
        if path_fields.get("physical_mod_hist") != "none":
            raise AssertionError(result.stdout)
        if path_fields.get("ev_hist_top") != "none":
            raise AssertionError(result.stdout)

    assert_formatter_contract()
    print("Diagnostic histogram output test passed")


if __name__ == "__main__":
    main()
