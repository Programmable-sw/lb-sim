#!/usr/bin/env python3
import importlib.util
import os
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "experiments/n-mrc/run_packet_lb_common_workloads_512.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("packet_lb_common_workloads", SCRIPT)
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

    assert runner.NODES == 512
    assert runner.TIERS == 2
    assert runner.HOSTS_PER_LEAF == 64
    assert runner.LEAVES == 8
    assert runner.SPINES == 64
    assert runner.HOT_SPINES == 16
    assert runner.expected_hotspot_background_sources() == 256
    original_leaves = runner.LEAVES
    original_hot_spines = runner.HOT_SPINES
    runner.LEAVES = 16
    runner.HOT_SPINES = 2
    assert runner.expected_hotspot_background_sources() == 64
    runner.LEAVES = original_leaves
    runner.HOT_SPINES = original_hot_spines

    assert [scheme.key for scheme in runner.SCHEMES] == [
        "ecmp_rr", "ops", "reps", "mrc", "avail", "grade", "netaware"
    ]
    expected_workloads = [
        "healthy_permutation_1m",
        "healthy_tornado_1m",
        "degraded_permutation_1m",
        "degraded_tornado_1m",
        "mixed_1m",
        "path_hotspot_1m",
        "incast_32x1m",
    ]
    assert [workload.name for workload in runner.WORKLOADS] == expected_workloads
    assert len(runner.run_specs()) == 49

    by_name = {workload.name: workload for workload in runner.WORKLOADS}
    healthy = runner.make_flows(by_name["healthy_permutation_1m"])
    degraded = runner.make_flows(by_name["degraded_permutation_1m"])
    assert len(healthy) == 512
    assert all(flow.src != flow.dst and flow.role == "target" for flow in healthy)
    assert [(flow.src, flow.dst, flow.size) for flow in degraded] == [
        (flow.src, flow.dst, flow.size) for flow in healthy
    ]

    mixed = runner.make_flows(by_name["mixed_1m"])
    mixed_target = [flow for flow in mixed if flow.role == "target"]
    mixed_background = [flow for flow in mixed if flow.role == "background"]
    assert len(mixed_target) == runner.LEAVES * (runner.HOSTS_PER_LEAF - 1)
    assert len(mixed_background) == runner.LEAVES * runner.MIXED_BACKGROUND_BURSTS
    assert all(flow.rate_mbps == 300000 for flow in mixed_background)
    assert all(flow.start_us == 250.0 for flow in mixed_target)

    incast = runner.make_flows(by_name["incast_32x1m"])
    assert len(incast) == 32
    assert len({flow.src for flow in incast}) == 32
    assert {flow.dst for flow in incast} == {0}

    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        tm = temp / "traffic.cm"
        dat = temp / "logout.dat"
        scheme = next(s for s in runner.SCHEMES if s.key == "mrc")
        default_cmd = runner.build_command(
            by_name["healthy_permutation_1m"], scheme,
            tm, dat,
            len(runner.make_flows(by_name["healthy_permutation_1m"])))
        assert option_value(default_cmd, "-lb") == "mrc"
        assert "-mrc_cooldown_mode" not in default_cmd

        degraded_cmd = runner.build_command(
            by_name["degraded_tornado_1m"], scheme, tm, dat,
            len(runner.make_flows(by_name["degraded_tornado_1m"])))
        assert option_value(degraded_cmd, "-slow_tor_uplinks") == "16"
        assert option_value(degraded_cmd, "-slow_tor_uplink_divisor") == "2"
        assert option_value(degraded_cmd, "-slow_tor_uplink_select") == "random-sparse"

        hotspot_cmd = runner.build_command(
            by_name["path_hotspot_1m"], scheme, tm, dat,
            len(runner.make_flows(by_name["path_hotspot_1m"])))
        assert option_value(hotspot_cmd, "-path_hotspot_spines") == "16"
        assert option_value(hotspot_cmd, "-path_hotspot_bg_rate_gbps") == "300"
        assert option_value(hotspot_cmd, "-path_hotspot_bg_on_us") == "1000"
        assert option_value(hotspot_cmd, "-path_hotspot_bg_off_us") == "0"

        incast_cmd = runner.build_command(
            by_name["incast_32x1m"], scheme, tm, dat,
            len(runner.make_flows(by_name["incast_32x1m"])))
        assert option_value(incast_cmd, "-end") == "20000"

        for workload in runner.WORKLOADS:
            for scheme in runner.SCHEMES:
                flows = runner.make_flows(workload)
                command = runner.build_command(workload, scheme, tm, dat, len(flows))
                assert option_value(command, "-nodes") == "512"
                assert option_value(command, "-tiers") == "2"
                assert option_value(command, "-paths") == "64"
                assert option_value(command, "-linkspeed") == "400000"
                assert option_value(command, "-queue_type") == "composite_ecn_lb"
                assert option_value(command, "-host_queue_type") == "prio"
                assert option_value(command, "-roce_rx_mode") == "sp"
                assert option_value(command, "-roce_sack_bitmap_bits") == "64"
                assert option_value(command, "-cc") == "dctcp_variant"
                assert option_value(
                    command, "-roce_transport_semantics") == "legacy"
                assert option_value(
                    command, "-roce_trim_recovery") == "cumulative"
                assert option_value(command, "-lb") == scheme.lb

        synthetic_flows = [
            runner.make_flow(1, 2, start_us=0.0),
            runner.make_flow(3, 4, start_us=10.0),
            runner.make_flow(5, 6, size=3750000, start_us=0.0,
                             role="background", rate_mbps=300000),
        ]
        stdout_file = temp / "synthetic.stdout"
        stdout_file.write_text(
            "lb mode mrc\n"
            "queue_type 11\n"
            "cc mode dctcp_variant\n"
            "RoCE receive mode sp\n"
            "RoCE SACK bitmap 64 bits\n"
            "FinalCcMrcConfig dctcp_variant_inflate=natural "
            "mrc_ecn_trim_penalty=mode_uniform\n"
            "MrcPolicyDiag policy=skip_once "
            "all_skip_resolution=ordinary_rotation\n"
            "Flow Roce_1_2 101 finished at 10\n"
            "Flow Roce_3_4 102 finished at 30\n"
            "Flow Roce_5_6 103 finished at 40\n"
            "New: 100 Rtx: 5\n"
            "RoceDiag acks=90 nacks=6 nacks_ooo=3 nacks_trim=2 "
            "nacks_loss=1 rtos=1 ecn_echo_acks=8 feedback_acks=7\n"
            "QueueDiag lossless_overflows=0 lossless_ecn_marks=0 "
            "lossy_drops=0 lossy_ecn_marks=0 composite_trims=2 "
            "composite_drops=1 composite_ecn_marks=8\n",
            encoding="utf-8")
        idmap_file = temp / "idmap.txt"
        idmap_file.write_text(
            "101 Roce_1_2\n102 Roce_3_4\n103 Roce_5_6\n",
            encoding="utf-8")
        tm.write_text("original traffic\n", encoding="utf-8")
        fingerprint_before = runner.cache_fingerprint(tm, ["sim", "-x"])
        tm.write_text("changed traffic\n", encoding="utf-8")
        fingerprint_after = runner.cache_fingerprint(tm, ["sim", "-x"])
        assert fingerprint_before != fingerprint_after
        bad_idmap = temp / "bad_idmap.txt"
        bad_idmap.write_text("101 Roce_99_100\n102 Roce_3_4\n103 Roce_5_6\n")
        assert runner.source_id_map(bad_idmap, synthetic_flows) == {}
        synthetic_workload = by_name["mixed_1m"]
        synthetic_scheme = next(s for s in runner.SCHEMES if s.key == "mrc")
        synthetic_command = runner.build_command(
            synthetic_workload, synthetic_scheme, tm, dat,
            len(synthetic_flows))
        row = runner.parse_run(
            synthetic_workload, synthetic_scheme, synthetic_flows,
            stdout_file, idmap_file, 0, synthetic_command)
        assert row["completed"] == 2 and row["flows"] == 2
        assert row["background_completed"] == 1
        assert row["avg_fct_us"] == 15.0
        assert row["p50_fct_us"] == 15.0
        assert row["max_fct_us"] == 20.0
        assert row["nacks"] == 6
        assert row["nacks_ooo"] == 3
        assert row["nacks_trim"] == 2
        assert row["nacks_loss"] == 1
        assert row["rtos"] == 1
        assert row["new_packets"] == 100
        assert row["retx_packets"] == 5
        assert row["retx_ratio"] == 0.05
        assert row["composite_trims"] == 2
        assert row["composite_drops"] == 1
        assert row["composite_ecn_marks"] == 8
        assert row["config_ok"] == 1

        output_dir = temp / "report"
        report = runner.write_outputs([row], output_dir)
        assert report == output_dir / "packet_lb_common_workloads_512_for_gpt.md"
        assert (output_dir / "summary.csv").exists()
        assert (output_dir / "commands.tsv").exists()
        report_text = report.read_text(encoding="utf-8")
        assert "512-Node Packet-LB Common-Workload Comparison" in report_text
        assert "single-seed transport/load-balancing sanity comparison" in report_text
        assert "## Findings" in report_text

        dry_output = temp / "dry"
        environment = dict(os.environ)
        environment["PACKET_LB_COMMON_DRY_RUN"] = "1"
        environment["PACKET_LB_COMMON_OUT"] = str(dry_output)
        dry = subprocess.run(
            [sys.executable, str(SCRIPT)], cwd=ROOT, env=environment,
            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if dry.returncode != 0:
            raise AssertionError(dry.stdout)
        assert sum(1 for _ in (dry_output / "summary.csv").open()) == 50
        assert sum(1 for _ in (dry_output / "commands.tsv").open()) == 50

    print("packet LB common-workload runner tests passed")


if __name__ == "__main__":
    main()
