#!/usr/bin/env python3
"""Run comparable REPS, destination-ToR bitmap, and GLB smoke experiments."""
import argparse
import csv
import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCHEMES = ("reps", "dtor", "glb")
DEFAULT_TOPOLOGY = "leaf_spine_128_100G_OS2"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path,
                        default=ROOT / "experiments/smoke/output/reps-dtor-glb")
    parser.add_argument("--seed", type=int, default=13)
    parser.add_argument("--netload", type=int, default=50)
    parser.add_argument("--topo", default=DEFAULT_TOPOLOGY)
    parser.add_argument("--simul-time", default="0.1")
    parser.add_argument(
        "--waf-python-bin", type=Path,
        default=Path(os.environ.get("NS3_WAF_PYTHON_BIN", ROOT.parents[1] / ".deps/python2/bin")),
        help="directory containing the Python interpreter required by this ns-3.19 waf build",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def waf_environment(environment, python_bin):
    """Return an environment in which the legacy waf launcher can find Python 2."""
    result = dict(environment)
    result["PATH"] = str(python_bin) + os.pathsep + result.get("PATH", "")
    return result


def build_specs(args):
    specs = []
    for scheme in SCHEMES:
        run_id = "smoke-{scheme}-seed{seed}-load{load}".format(
            scheme=scheme, seed=args.seed, load=args.netload)
        command = [
            "python3", "run.py", "--lb", scheme,
            "--irn", "1", "--pfc", "1",
            "--seed", str(args.seed), "--netload", str(args.netload), "--topo", args.topo,
            "--simul_time", args.simul_time, "--id", run_id,
        ]
        specs.append({"scheme": scheme, "run_id": run_id, "command": command})
    return specs


def parse_summary(path):
    rows = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("<1BDP,") or line.startswith(">1BDP,"):
            fields = [field.strip() for field in line.split(",")]
            rows[fields[0]] = {"avg": float(fields[1]), "p99": float(fields[4])}
    if set(rows) != {"<1BDP", ">1BDP"}:
        raise RuntimeError("missing small/large rows in {}".format(path))
    return rows


def main(argv=None):
    args = parse_args(argv)
    output = args.out.resolve()
    specs = build_specs(args)
    if args.dry_run:
        for spec in specs:
            print(" ".join(spec["command"]))
        return

    if not (args.waf_python_bin / "python").is_file():
        raise RuntimeError(
            "cannot find the Python interpreter required by waf: {}; pass "
            "--waf-python-bin or set NS3_WAF_PYTHON_BIN".format(
                args.waf_python_bin / "python"))
    environment = waf_environment(os.environ, args.waf_python_bin)
    output.mkdir(parents=True, exist_ok=True)
    rows = []
    for spec in specs:
        log_path = output / (spec["scheme"] + ".log")
        with log_path.open("w", encoding="utf-8") as log:
            result = subprocess.run(spec["command"], cwd=ROOT, text=True,
                                    stdout=log, stderr=subprocess.STDOUT,
                                    env=environment)
        if result.returncode:
            raise RuntimeError("{} failed; see {}".format(spec["scheme"], log_path))
        summary = ROOT / "mix/output" / spec["run_id"] / (
            spec["run_id"] + "_out_fct_summary.txt")
        values = parse_summary(summary)
        rows.append({
            "scheme": spec["scheme"],
            "small_avg_slowdown": values["<1BDP"]["avg"],
            "small_p99_slowdown": values["<1BDP"]["p99"],
            "large_avg_slowdown": values[">1BDP"]["avg"],
            "large_p99_slowdown": values[">1BDP"]["p99"],
        })
    with (output / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
