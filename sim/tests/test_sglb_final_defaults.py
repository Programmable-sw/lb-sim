#!/usr/bin/env python3

import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BINARY = ROOT / "sim/datacenter/htsim_roce"


def run(traffic, output, extra_args):
    command = [
        str(BINARY), "-o", str(output), "-tm", str(traffic),
        "-nodes", "256", "-conns", "0", "-tiers", "2", "-lb", "sglb",
        "-queue_type", "composite_ecn_lb", "-host_queue_type", "prio",
        "-cc", "dctcp_variant", "-end", "1", "-linkspeed", "400000",
        "-paths", "64",
        *extra_args,
    ]
    return subprocess.run(
        command, cwd=ROOT, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, check=False)


def require_config(result, score_mode, levels, min_choices=24):
    if result.returncode != 0:
        raise AssertionError(result.stdout)
    expected = f"SGLB effective config: score mode {score_mode}"
    if expected not in result.stdout:
        raise AssertionError(f"missing score mode {expected!r}")
    if f"nmrc_levels {levels}" not in result.stdout:
        raise AssertionError(f"missing n-MRC level count {levels}")
    if f"min choices {min_choices}" not in result.stdout:
        raise AssertionError(
            f"SGLB candidate minimum is not {min_choices}")


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        traffic = temp / "empty.cm"
        traffic.write_text("Nodes 256\nConnections 0\n", encoding="utf-8")

        default = run(traffic, temp / "default.dat", [])
        require_config(default, "nmrc_quantized_topk", 4)

        legacy = run(
            traffic, temp / "legacy.dat",
            ["-sglb_score_mode", "legacy"])
        require_config(legacy, "legacy", 4)

        level8 = run(
            traffic, temp / "level8.dat", ["-sglb_nmrc_levels", "8"])
        require_config(level8, "nmrc_quantized_topk", 8)

        for index, removed_mode in enumerate(
                ("nmrc_grade", "nmrc_grade_ranked")):
            removed = run(
                traffic, temp / f"removed_{index}.dat",
                ["-sglb_score_mode", removed_mode])
            if removed.returncode == 0:
                raise AssertionError(
                    f"removed SGLB mode was accepted: {removed_mode}")


if __name__ == "__main__":
    main()
