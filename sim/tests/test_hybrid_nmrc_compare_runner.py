#!/usr/bin/env python3

import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "experiments/n-mrc/run_hybrid_nmrc_compare.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("hybrid_nmrc_compare", RUNNER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    runner = load_runner()

    def selection_row(variant, scenario, valid, p99):
        return {
            "variant": variant,
            "scenario": scenario,
            "seed": 13,
            "config_ok": int(valid),
            "all_flows_completed": int(valid),
            "p99_fct_us": p99,
            "goodput_gbps": 100.0,
            "rtos": 0,
            "fastcnp_packet_amplification": 0.01,
        }

    incomplete_rows = [
        selection_row("nmrc_a", "healthy_permutation", True, 10.0),
        selection_row("nmrc_b", "healthy_permutation", True, 11.0),
        selection_row("nmrc_a", "path_hotspot", False, 0.0),
        selection_row("nmrc_b", "path_hotspot", False, 0.0),
    ]
    selected, audit = runner.guardrail_selection(incomplete_rows)
    assert selected is None
    assert audit and not any(item["passed"] for item in audit)

    with tempfile.TemporaryDirectory() as temp:
        case_dir = Path(temp)
        for name in ("command.txt", "stdout.log", "returncode.txt",
                     "runtime.txt", "fingerprint.txt", "idmap.txt"):
            (case_dir / name).write_text("cached\n", encoding="utf-8")
        (case_dir / "command.txt").write_text("same command\n", encoding="utf-8")
        (case_dir / "fingerprint.txt").write_text("same fingerprint\n", encoding="utf-8")
        assert runner.reusable_result(
            SimpleNamespace(force=False), case_dir,
            "same command", "same fingerprint")
        assert not runner.reusable_result(
            SimpleNamespace(force=True), case_dir,
            "same command", "same fingerprint")

    variants8 = runner.variant_matrix(8)
    variant_keys8 = {variant.key for variant in variants8}
    assert len(variants8) == 11
    assert {
        "nmrc_encoded_any_better",
        "nmrc_encoded_better_ge3",
        "nmrc_random_matched_any_better",
        "nmrc_random_matched_better_ge3",
        "nmrc_random32_any_better",
        "nmrc_random32_better_ge3",
        "sglb",
        "adaptive_routing",
        "netaware",
        "mrc",
        "reps",
    } == variant_keys8

    variants32 = runner.variant_matrix(32)
    variant_keys32 = {variant.key for variant in variants32}
    assert len(variants32) == 9
    assert not any("random32" in key for key in variant_keys32)

    scenario_keys = {scenario.key for scenario in runner.SCENARIOS}
    assert scenario_keys == {
        "healthy_permutation",
        "healthy_tornado",
        "asymmetric_uplink",
        "path_hotspot",
        "mixed_background",
        "incast_recovery",
    }

    with tempfile.TemporaryDirectory() as temp:
        selected_args = runner.parse_args([
            "--dry-run",
            "--quick",
            "--nodes", "128",
            "--seeds", "13",
            "--variants",
            ("nmrc_encoded_better_ge3,netaware,sglb,adaptive_routing,"
             "mrc,reps"),
            "--out", str(Path(temp) / "selected"),
        ])
        selected_specs = runner.make_specs(selected_args)
        assert len(selected_specs) == 36
        expected_order = [
            "nmrc_encoded_better_ge3",
            "sglb",
            "adaptive_routing",
            "netaware",
            "mrc",
            "reps",
        ]
        expected_pairs = [
            (scenario.key, variant)
            for scenario in runner.SCENARIOS
            for variant in expected_order
        ]
        assert [
            (spec["scenario"].key, spec["variant"].key)
            for spec in selected_specs
        ] == expected_pairs
        runner.validate_specs(selected_specs)

        for lb, forbidden in (("reps", {"-reps_buffer"}),):
            original = next(
                spec for spec in selected_specs if spec["variant"].lb == lb)
            broken = dict(original)
            command = original["command"]
            broken["command"] = [
                token for index, token in enumerate(command)
                if token not in forbidden and
                (index == 0 or command[index - 1] not in forbidden)
            ]
            try:
                runner.validate_specs([broken])
            except ValueError:
                pass
            else:
                raise AssertionError(f"validate_specs accepted incomplete {lb}")

    with tempfile.TemporaryDirectory() as temp:
        output = Path(temp) / "compare"
        command = [
            sys.executable,
            str(RUNNER),
            "--dry-run",
            "--quick",
            "--nodes", "128",
            "--seeds", "13",
            "--out", str(output),
        ]
        subprocess.run(command, cwd=ROOT, check=True)

        commands = (output / "commands.tsv").read_text(encoding="utf-8")
        report = (output / "hybrid_nmrc_compare_for_gpt.md").read_text(
            encoding="utf-8")
        assert b"\r\n" not in (output / "summary.csv").read_bytes()
        assert (output / "traffic").is_dir()
        assert "healthy_permutation\tnmrc_encoded_any_better\t13\t" in commands
        for required in (
            "-queue_type composite_ecn_lb",
            "-host_queue_type prio",
            "-cc dctcp_variant",
            "-roce_rx_mode sp",
            "-roce_transport_semantics mrc_exact_bounded",
            "-roce_trim_recovery exact",
            "-seed 13",
            "-tm ",
            "-queue_cv_sample_us 1",
        ):
            assert required in commands, required
        assert "-lb n-mrc -nmrc_ev_mode encoded " in commands
        assert "-nmrc_reroute_policy better_ge3 -nmrc_fastcnp on" in commands
        assert "-lb sglb" in commands
        assert "-lb adaptive-routing" in commands
        assert "-lb netaware" in commands
        assert "-lb mrc" in commands
        assert "-mrc_cooldown_mode" not in commands
        assert "-mrc_all_cooling_fallback" not in commands
        assert "-lb reps -reps_buffer 8" in commands
        assert "dry-run" in report
        assert "random32" in report
        assert "P >= 32" in report
        assert "Jain" in report
        assert "guardrail" in report
        assert "queue p99" in report
        assert "at or below 10%" in report


if __name__ == "__main__":
    main()
