#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path


def test_short_flows_are_forced_to_ecmp(tmp_path: Path) -> None:
    repo = Path(__file__).resolve().parents[2]
    fake_binary = tmp_path / "fake_htsim.py"
    fake_binary.write_text(
        "#!/usr/bin/env python3\n"
        "import pathlib, re, sys\n"
        "tm = pathlib.Path(sys.argv[sys.argv.index('-tm') + 1])\n"
        "for line in tm.read_text().splitlines():\n"
        "    m = re.search(r' size (\\d+)', line)\n"
        "    if m: print('flow finished at 1 total bytes ' + m.group(1))\n"
        "print('RoceDiag acks=0 rtos=0')\n"
        "print('QueueDiag lossless_overflows=0 lossy_drops=0 composite_drops=0')\n"
        "print('SharedBufferDiag total_bytes=0 overflows=0')\n"
    )
    fake_binary.chmod(0o755)
    out = tmp_path / "out"

    subprocess.run(
        [
            sys.executable,
            str(repo / "experiments/n-mrc/run_mprdma_bursty_a2a.py"),
            "--nodes", "4",
            "--long-size", "16384",
            "--short-load", "0.05",
            "--binary", str(fake_binary),
            "--out", str(out),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    flow_lines = (out / "traffic.cm").read_text().splitlines()[2:]
    collective_count = 4 * 3
    assert all(" lb ecmp" not in line for line in flow_lines[:collective_count])
    assert flow_lines[collective_count:]
    assert all(line.endswith(" lb ecmp") for line in flow_lines[collective_count:])


if __name__ == "__main__":
    from tempfile import TemporaryDirectory

    with TemporaryDirectory() as directory:
        test_short_flows_are_forced_to_ecmp(Path(directory))
