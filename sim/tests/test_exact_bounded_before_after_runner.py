#!/usr/bin/env python3
import importlib.util
import math
import tempfile
from argparse import Namespace
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "experiments/n-mrc/run_exact_bounded_before_after_schemes.py"


def load_runner():
    spec = importlib.util.spec_from_file_location(
        "exact_bounded_before_after_schemes", SCRIPT)
    if spec is None or spec.loader is None:
        raise AssertionError(f"cannot load {SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def option_value(command, option):
    index = command.index(option)
    return command[index + 1]


def main():
    runner = load_runner()
    assert runner.SCENARIOS == (
        "healthy_permutation_16mib",
        "asymmetric_permutation_16mib",
        "standard_websearch_proxy_80pct",
        "full_global_p16_256mib_background_off",
        "full_global_p16_256mib_background_on",
        "full_global_p4_64mib_background_off",
    )
    assert tuple(runner.SCHEMES) == (
        "sglb", "mrc", "ar", "avail", "grade", "netaware")

    with tempfile.TemporaryDirectory() as temp_dir:
        args = Namespace(
            out=Path(temp_dir),
            sim=ROOT / "sim/datacenter/htsim_roce",
        )
        specs = runner.make_legacy_specs(args)
        assert len(specs) == 36
        runner.validate_legacy_specs(specs)
        for spec in specs:
            command = spec["command"]
            assert spec["variant"] == "natural_cumulative"
            assert option_value(command, "-roce_transport_semantics") == "legacy"
            assert option_value(command, "-roce_trim_recovery") == "cumulative"
            assert option_value(command, "-cc") == "dctcp_variant"
            assert option_value(command, "-roce_rx_mode") == "sp"
            assert option_value(command, "-queue_type") == "composite_ecn_lb"
            if spec["scheme"] == "netaware":
                assert option_value(
                    command, "-netaware_weight_adaptation") == "good_share_cap"

    base = {"primary_us": 100.0, "nacks": 20, "retx_ratio": 0.1}
    exact = {"primary_us": 80.0, "nacks": 10, "retx_ratio": 0.05}
    comparison = runner.comparison_row("healthy", "netaware", base, exact)
    assert math.isclose(comparison["primary_delta_pct"], -20.0)
    assert math.isclose(comparison["nacks_delta_pct"], -50.0)
    assert math.isclose(comparison["retx_ratio_delta_pct"], -50.0)

    legacy_stdout = (
        "RoceTransportConfig semantics=legacy awnd=legacy "
        "exact_trim_attempt_id=off recovery_reserve_bytes=0\n"
        "FinalCcMrcConfig dctcp_variant_inflate=natural "
        "mrc_ecn_trim_penalty=mode_uniform "
        "roce_trim_recovery=cumulative\n"
    )
    assert runner.legacy_transport_config_ok(legacy_stdout)
    assert runner.classify_delta(-6.0) == "material_improvement"
    assert runner.classify_delta(1.5) == "near_neutral"
    assert runner.classify_delta(7.0) == "material_regression"
    assert math.isnan(runner.pct_change(0, 0))

    normalized = runner.normalized_exact_rows([
        {"scenario": "s", "scheme": "a", "exact_primary_us": 10.0},
        {"scenario": "s", "scheme": "b", "exact_primary_us": 12.5},
    ])
    assert normalized == [
        {"scenario": "s", "scheme": "a", "exact_primary_us": 10.0,
         "best_exact_primary_us": 10.0, "ratio_to_best": 1.0},
        {"scenario": "s", "scheme": "b", "exact_primary_us": 12.5,
         "best_exact_primary_us": 10.0, "ratio_to_best": 1.25},
    ]

    print("exact bounded before/after runner tests passed")


if __name__ == "__main__":
    main()
