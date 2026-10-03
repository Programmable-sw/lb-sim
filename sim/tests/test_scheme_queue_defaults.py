#!/usr/bin/env python3

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "experiments" / "n-mrc" / "run_literature_metric_compare.py"


def load_script():
    spec = importlib.util.spec_from_file_location("run_literature_metric_compare", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def scheme_by_label(module, label):
    for scheme in module.ALL_SCHEMES:
        if scheme[0] == label:
            return scheme
    raise AssertionError(f"missing scheme {label!r}")


def queue_types_in_command(cmd):
    values = []
    for index, token in enumerate(cmd):
        if token == "-queue_type":
            values.append(cmd[index + 1])
    return values


def command_queue_type(module, label):
    scenario = {
        "nodes": 128,
        "tiers": 2,
        "queue_type": "lossless_input_ecn",
        "end_us": 1000,
        "paths": 128,
    }
    cmd = module.command_for(
        scenario,
        scheme_by_label(module, label),
        Path("traffic.cm"),
        Path("logout.dat"),
        1,
        0,
    )
    values = queue_types_in_command(cmd)
    if len(values) != 1:
        raise AssertionError(f"{label} should emit exactly one -queue_type, got {values}")
    return values[0]


def main():
    module = load_script()
    main_roce = (ROOT / "sim" / "datacenter" / "main_roce.cpp").read_text(
        encoding="utf-8")

    if "RoceSrc::cc_mode_t roce_cc_mode = RoceSrc::CC_DCTCP_VARIANT;" not in main_roce:
        raise AssertionError("global RoCE CC default should be dctcp_variant")
    for marker in (
            'lb_scheme_name << " default cc dctcp_variant"',
            'MRC default cc dctcp_variant',
            'n-MRC default cc dctcp_variant'):
        if marker not in main_roce:
            raise AssertionError(f"missing {marker!r}")
    if "MRC default cc none" in main_roce:
        raise AssertionError("MRC must not override the shared dctcp_variant default")

    for label in ("mrc", "avail", "grade", "netaware"):
        actual = command_queue_type(module, label)
        if actual != "composite_ecn_lb":
            raise AssertionError(f"{label} should default to composite_ecn_lb, got {actual}")

    for label in ("ecmp_rr", "ops", "rr", "reps", "adaptive-routing", "drill", "sglb"):
        actual = command_queue_type(module, label)
        if actual != "lossless_input_ecn":
            raise AssertionError(f"{label} should keep lossless_input_ecn, got {actual}")


if __name__ == "__main__":
    main()
