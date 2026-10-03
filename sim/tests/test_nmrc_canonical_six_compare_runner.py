#!/usr/bin/env python3

import importlib.util
import math
from pathlib import Path
import tempfile
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "experiments/n-mrc/run_nmrc_canonical_six_compare.py"


def load_runner():
    spec = importlib.util.spec_from_file_location(
        "nmrc_canonical_six_compare", RUNNER)
    if spec is None or spec.loader is None:
        raise AssertionError(f"cannot load {RUNNER}")
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
        "nmrc_encoded_better_ge3",
        "netaware",
        "sglb",
        "ar",
        "mrc",
        "reps",
    )
    assert runner.parse_args(["--seeds", "13,29,47"]).seeds == (13, 29, 47)

    canonical_hashes = {
        "healthy_permutation_16mib":
            "1d32164f81bc51706685126f9d1cc694093e1e6c821cd1d47385414384b288d6",
        "standard_websearch_proxy_80pct":
            "008da4ad9615dce108cb046b2ccd82a5a1fad0be6f1ecb7a6c61e55710bd6903",
        "full_global_p16_256mib_background_off":
            "20427d53cb40a34632ad4b93722726c89b4713dc6c0bc86529e0861b6a0da1ed",
        "full_global_p4_64mib_background_off":
            "cd66f7cc1c0f0fe7e6813b9bbb02c23813611479a17c4ae0dc62747f28a66945",
    }
    with tempfile.TemporaryDirectory() as temp_dir:
        traffic_dir = Path(temp_dir)
        for scenario in runner.SCENARIOS:
            materialized = runner.materialize_traffic(
                traffic_dir, scenario, seed=13)
            assert materialized["traffic_file"].exists()
            assert materialized["connections"] > 0
            if scenario in canonical_hashes:
                assert materialized["traffic_sha256"] == canonical_hashes[scenario]

    with tempfile.TemporaryDirectory() as temp_dir:
        output = Path(temp_dir)
        args = SimpleNamespace(
            sim=ROOT / "sim/datacenter/htsim_roce",
            out=output,
            seeds=(13, 29, 47),
        )
        specs = runner.make_specs(args)
        assert len(specs) == 108
        runner.validate_specs(specs)
        hashes_by_block = {}
        for spec in specs:
            assert spec["variant"] == "mrc_exact_bounded"
            assert spec["cc_mode"] == "dctcp_variant"
            assert spec["inflate_diag"] == "disabled"
            assert spec["trim_mode"] == "exact"
            block = (spec["scenario"], spec["seed"])
            hashes_by_block.setdefault(block, set()).add(
                spec["traffic_sha256"])
            command = spec["command"]
            for option, expected in (
                    ("-nodes", "128"),
                    ("-tiers", "2"),
                    ("-linkspeed", "400000"),
                    ("-queue_type", "composite_ecn_lb"),
                    ("-host_queue_type", "prio"),
                    ("-mtu", "4096"),
                    ("-paths", "64"),
                    ("-cc", "dctcp_variant"),
                    ("-roce_rx_mode", "sp"),
                    ("-roce_sack_bitmap_bits", "64"),
                    ("-roce_transport_semantics", "mrc_exact_bounded"),
                    ("-roce_trim_recovery", "exact"),
                    ("-hop_latency", "0.5"),
                    ("-switch_latency", "0.5")):
                assert option_value(command, option) == expected
            assert not any(
                token.startswith("-nmrc_feedback_") or
                token.startswith("-netaware_feedback_")
                for token in command)
            if spec["kind"] == "all_to_all":
                assert option_value(command, "-end") == "100000"

            scheme = spec["scheme"]
            expected_lb = {
                "nmrc_encoded_better_ge3": "n-mrc",
                "netaware": "netaware",
                "sglb": "sglb",
                "ar": "adaptive-routing",
                "mrc": "mrc",
                "reps": "reps",
            }[scheme]
            assert option_value(command, "-lb") == expected_lb
            if scheme == "nmrc_encoded_better_ge3":
                assert option_value(command, "-nmrc_ev_mode") == "encoded"
                assert option_value(
                    command, "-nmrc_reroute_policy") == "better_ge3"
                assert option_value(command, "-nmrc_fastcnp") == "on"
            elif scheme == "netaware":
                assert option_value(
                    command,
                    "-netaware_weight_adaptation") == "good_share_cap"
            elif scheme == "ar":
                assert option_value(command, "-ar_granularity") == "packet"
            elif scheme == "mrc":
                assert "-mrc_cooldown_mode" not in command
                assert "-mrc_all_cooling_fallback" not in command
            elif scheme == "reps":
                assert option_value(command, "-reps_buffer") == "8"
        assert len(hashes_by_block) == 18
        assert all(len(hashes) == 1 for hashes in hashes_by_block.values())

        original = specs[0]
        broken = dict(original)
        broken["command"] = list(original["command"])
        broken["command"].remove("-nmrc_fastcnp")
        broken["command"].remove("on")
        broken_specs = list(specs)
        broken_specs[0] = broken
        try:
            runner.validate_specs(broken_specs)
        except ValueError:
            pass
        else:
            raise AssertionError("validate_specs accepted incomplete n-MRC")

        wrong_seed_specs = []
        for item in specs:
            changed = dict(item)
            changed["command"] = list(item["command"])
            if item["seed"] == 13:
                changed["seed"] = 1
                seed_index = changed["command"].index("-seed") + 1
                changed["command"][seed_index] = "1"
            wrong_seed_specs.append(changed)
        try:
            runner.validate_specs(wrong_seed_specs)
        except ValueError:
            pass
        else:
            raise AssertionError("validate_specs accepted non-canonical seeds")

        common_runtime = (
            "cc mode dctcp_variant\n"
            "queue_type 11\n"
            "host queue_type 4\n"
            "RoCE receive mode sp\n"
            "RoCE SACK bitmap 64 bits\n"
            "RoCE TRIM recovery mode exact\n"
            "RoceTransportConfig semantics=mrc_exact_bounded "
            "awnd=cwnd_minus_inflight exact_trim_attempt_id=on "
            "recovery_reserve_bytes=4096\n"
            "FinalCcMrcConfig dctcp_variant_inflate=disabled "
            "roce_trim_recovery=exact\n"
            "BoundedRecoveryDiag semantics=mrc_exact_bounded "
            "inflight_final=0 unique_acks=2 "
            "recovery_inflight_final_bytes=0 "
            "recovery_inflight_max_bytes=4096 stale_attempt_nacks=0 "
            "duplicate_failure_nacks=0 "
            "duplicate_confirmations_suppressed=0 "
            "acked_revival_rejected=0 attempt_wraps=0 "
            "exact_trim_recoveries=0 sack_loss_recoveries=0\n"
        )
        scheme_runtime = {
            "nmrc_encoded_better_ge3": (
                "HybridNmrcConfig ev_mode=encoded "
                "reroute_policy=better_ge3 fastcnp=on\n"
                "HybridNmrcDiag route_checks=10 reroutes=2 "
                "threshold_blocked=0 "
                "level_transitions=0,0,0,0/0,0,0,0/0,0,0,0/2,0,0,0 "
                "fastcnp_generated=2 fastcnp_arrived=2 "
                "fastcnp_after_done=0 "
                "fastcnp_bytes=128 fastcnp_arrived_bytes=128 "
                "fastcnp_latency_avg_us=1.5 fastcnp_route_missing=0 "
                "fastcnp_unknown_qp=0 fastcnp_unknown_ev=0 "
                "cooldown_starts=2 cooling_skips=0 "
                "cooling_recoveries=0 duplicate_notifications=0 "
                "all_cooling_fallbacks=0 observed_evs=8 "
                "observed_first_hop_choices=8 "
                "observed_first_hop_alias_ratio=0.0\n"),
            "netaware": (
                "weight_adaptation good_share_cap\n"
                "NetawareDiag samples=10 feedbacks=2\n"),
            "sglb": "SglbRouteDiag route_calls=10\n",
            "ar": "Adaptive routing granularity packet\n",
            "mrc": (
                "MrcPolicyDiag policy=skip_once "
                "all_skip_resolution=ordinary_rotation\n"
                "MrcDiag ecn_cooldown_events=0\n"),
            "reps": (
                "reps buffer size 8\n"
                "RepsLikeDiag random_sends=10 cached_sends=2\n"),
        }
        for scheme, diagnostic in scheme_runtime.items():
            spec = next(item for item in specs if item["scheme"] == scheme)
            runtime = (
                f"lb mode {spec['lb_name']}\n" + common_runtime + diagnostic)
            assert runner.config_ok(runtime, spec, returncode=0) == 1
            assert runner.config_ok(
                runtime.replace("BoundedRecoveryDiag", "MissingDiag"),
                spec, returncode=0) == 0
            assert runner.config_ok(
                runtime.replace(diagnostic, ""), spec, returncode=0) == 0
            assert runner.config_ok(
                runtime.replace("exact_trim_attempt_id=on",
                                "exact_trim_attempt_id=off"),
                spec, returncode=0) == 0
            assert runner.config_ok(
                runtime.replace(" recovery_reserve_bytes=4096",
                                " recovery_reserve_bytes=0"),
                spec, returncode=0) == 0
            assert runner.config_ok(
                runtime.replace(" unique_acks=2", ""),
                spec, returncode=0) == 0
            if scheme == "nmrc_encoded_better_ge3":
                assert runner.config_ok(
                    runtime.replace(
                        "0,0,0,0/0,0,0,0/0,0,0,0/2,0,0,0",
                        ",,,/,,,/,,,/,,,"),
                    spec, returncode=0) == 0

    with tempfile.TemporaryDirectory() as temp_dir:
        case_dir = Path(temp_dir) / "point"
        case_dir.mkdir()
        flows = [
            runner.common.Flow(0, 1, 4096, start_us=1),
            runner.common.Flow(1, 0, 4096, start_us=2),
        ]
        point_spec = {
            "scenario": "healthy_permutation_16mib",
            "kind": "point_to_point",
            "scheme": "ar",
            "lb_name": "adaptive-routing",
            "variant": "mrc_exact_bounded",
            "cc_mode": "dctcp_variant",
            "inflate_diag": "disabled",
            "trim_mode": "exact",
            "seed": 13,
            "traffic_sha256": "a" * 64,
            "case_dir": case_dir,
            "command": ["sim", "-lb", "adaptive-routing"],
            "expected_flows": 2,
            "flows_data": flows,
        }
        point_runtime = (
            "lb mode adaptive-routing\n" + common_runtime +
            scheme_runtime["ar"] +
            "Flow Roce_0_1 1 finished at 11\n"
            "Flow Roce_1_0 2 finished at 22\n"
        )
        (case_dir / "stdout.log").write_text(
            point_runtime, encoding="utf-8")
        (case_dir / "idmap.txt").write_text(
            "1 Roce_0_1\n2 Roce_1_0\n", encoding="utf-8")
        point_row = runner.parse_run(point_spec, 0, 0.25)
        assert point_row["primary_metric"] == "p99_fct_us"
        assert math.isclose(point_row["primary_us"], 19.9)
        assert point_row["all_flows_completed"] == 1

        (case_dir / "stdout.log").write_text(
            point_runtime.replace(
                "Flow Roce_1_0 2 finished at 22",
                "Flow Roce_0_1 1 finished at 22"),
            encoding="utf-8")
        duplicate_point_row = runner.parse_run(point_spec, 0, 0.25)
        assert duplicate_point_row["completed"] == 2
        assert duplicate_point_row["unique_completed"] == 1
        assert duplicate_point_row["all_flows_completed"] == 0

        alltoall_dir = Path(temp_dir) / "alltoall"
        alltoall_dir.mkdir()
        alltoall_spec = dict(point_spec)
        alltoall_spec.update({
            "scenario": "full_global_p16_256mib_background_off",
            "kind": "all_to_all",
            "case_dir": alltoall_dir,
            "flows_data": None,
        })
        (alltoall_dir / "stdout.log").write_text(
            "lb mode adaptive-routing\n" + common_runtime +
            scheme_runtime["ar"] +
            "Flow Roce_0_1 1 finished at 5\n"
            "Flow Roce_1_0 2 finished at 8\n",
            encoding="utf-8")
        (alltoall_dir / "idmap.txt").write_text(
            "1 Roce_0_1\n2 Roce_1_0\n", encoding="utf-8")
        alltoall_row = runner.parse_run(alltoall_spec, 0, 0.5)
        assert alltoall_row["primary_metric"] == "all_to_all_cct_us"
        assert alltoall_row["primary_us"] == 8.0
        assert alltoall_row["all_flows_completed"] == 1
        (alltoall_dir / "stdout.log").write_text(
            "lb mode adaptive-routing\n" + common_runtime +
            scheme_runtime["ar"] +
            "Flow Roce_0_1 1 finished at 5\n"
            "Flow Roce_0_1 1 finished at 8\n",
            encoding="utf-8")
        duplicate_alltoall_row = runner.parse_run(
            alltoall_spec, 0, 0.5)
        assert duplicate_alltoall_row["completed"] == 2
        assert duplicate_alltoall_row["unique_completed"] == 1
        assert duplicate_alltoall_row["all_flows_completed"] == 0

    rows = []
    for scenario_index, scenario in enumerate(runner.SCENARIOS):
        for seed_index, seed in enumerate((13, 29, 47)):
            block_best = 100.0 + 10 * scenario_index + seed_index
            for scheme_index, scheme in enumerate(runner.SCHEMES):
                rows.append({
                    "scenario": scenario,
                    "kind": (
                        "point_to_point" if scenario in runner.P2P_SCENARIOS
                        else "all_to_all"),
                    "scheme": scheme,
                    "seed": seed,
                    "primary_metric": (
                        "p99_fct_us" if scenario in runner.P2P_SCENARIOS
                        else "all_to_all_cct_us"),
                    "primary_us": block_best * (scheme_index + 1),
                    "returncode": 0,
                    "config_ok": 1,
                    "all_flows_completed": 1,
                })
    aggregate = runner.aggregate(rows)
    assert len(aggregate) == 6
    for scheme_index, item in enumerate(aggregate):
        assert item["scheme"] == runner.SCHEMES[scheme_index]
        assert item["raw_cells"] == 18
        assert math.isclose(
            item["normalized_geometric_mean"], scheme_index + 1)

    cells = runner.summarize_three_seed_cells(rows)
    assert len(cells) == 36
    target = next(
        item for item in cells
        if item["scenario"] == runner.SCENARIOS[0] and
        item["scheme"] == runner.SCHEMES[0])
    assert target["seed_values"] == "13:100;29:101;47:102"
    assert math.isclose(
        target["primary_geometric_mean_us"],
        (100.0 * 101.0 * 102.0) ** (1.0 / 3.0))
    assert target["primary_min_us"] == 100.0
    assert target["primary_max_us"] == 102.0
    assert target["raw_cells"] == 3

    for malformed in (rows[:-1], [dict(row) for row in rows]):
        if len(malformed) == len(rows):
            malformed[0]["primary_us"] = 0.0
        try:
            runner.validate_result_matrix(malformed)
        except ValueError:
            pass
        else:
            raise AssertionError("accepted incomplete result matrix")

    report = runner.build_report(rows, revision="deadbeef")
    assert "Per-seed primary results (108 raw cells)" in report
    assert "geometric mean / min / max" in report
    assert "normalized geometric mean" in report
    assert "median" not in report.lower()


if __name__ == "__main__":
    main()
