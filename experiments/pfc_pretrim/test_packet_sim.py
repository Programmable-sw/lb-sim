import json
import pathlib
import subprocess
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
BIN = ROOT / "sim" / "datacenter" / "htsim_pfc_pretrim"


class PacketSimulatorTest(unittest.TestCase):
    def run_group(self, group):
        proc = subprocess.run(
            [str(BIN), "--group", group, "--seed", "13", "--scenario", "minimal"],
            cwd=ROOT, text=True, capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines() if line.startswith("{")]
        return events

    def test_p0_has_endogenous_pfc_chain(self):
        events = self.run_group("P0")
        names = [e["event"] for e in events]
        chain = [names.index(x) for x in (
            "nic_rx_enqueue", "nic_pfc_xoff", "leaf_data_pause",
            "switch_mmu_xoff", "upstream_data_pause", "flow_complete")]
        self.assertEqual(chain, sorted(chain))
        checks = [e for e in events if e["event"] == "switch_mmu_check"]
        self.assertTrue(checks)
        self.assertTrue(all("occupancy" in e and "xoff" in e for e in checks))
        summary = next(e for e in events if e["event"] == "summary")
        self.assertEqual(summary["offered_bytes"], summary["delivered_unique_bytes"])
        self.assertEqual(summary["incomplete_flows"], 0)
        self.assertGreater(summary["victim_fct_us"], summary["reference_fct_us"])
        self.assertLessEqual(summary["max_outstanding_bytes"], 512 * 1024)
        self.assertIn("ack_send", names)
        self.assertIn("ack_arrive", names)

    def test_p1_has_real_trim_recovery_chain(self):
        events = self.run_group("P1")
        trimmed = next(e for e in events if e["event"] == "trim")
        psn = trimmed["psn"]
        chain_names = [e["event"] for e in events if e.get("psn") == psn]
        for name in ("data_enqueue", "trim", "header_arrive", "nack_arrive",
                     "retransmit", "payload_deliver"):
            self.assertIn(name, chain_names)
        summary = next(e for e in events if e["event"] == "summary")
        self.assertEqual(summary["offered_bytes"], summary["delivered_unique_bytes"])
        self.assertEqual(summary["physical_bytes_final"], 0)

    def test_a2a_completes_and_reports_collective_cct(self):
        proc = subprocess.run(
            [str(BIN), "--group", "P1", "--seed", "13", "--scenario", "a2a"],
            cwd=ROOT, text=True, capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines()]
        summary = next(e for e in events if e["event"] == "summary")
        self.assertEqual(summary["incomplete_flows"], 0)
        self.assertGreater(summary["healthy_cct_us"], 0)

    def test_dcqcn_uses_ecn_cnp_and_changes_rate(self):
        proc = subprocess.run(
            [str(BIN), "--group", "P0", "--seed", "13", "--scenario", "minimal",
             "--cc", "dcqcn"], cwd=ROOT, text=True, capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines()]
        names = [e["event"] for e in events]
        self.assertIn("ecn_mark", names)
        self.assertIn("cnp_arrive", names)
        self.assertIn("rate_change", names)
        summary = next(e for e in events if e["event"] == "summary")
        self.assertGreater(summary["cnp_events"], 0)

    def test_healthy_receiver_generates_cnp_for_ce_data(self):
        proc = subprocess.run(
            [str(BIN), "--group", "healthy-cnp", "--seed", "13", "--scenario", "a2a",
             "--cc", "dcqcn", "--policy", "off", "--rx-mode", "normal",
             "--background-senders", "0", "--leaf-ecn-kmin-kib", "8",
             "--leaf-ecn-kmax-kib", "16", "--spine-ecn-kmin-kib", "8",
             "--spine-ecn-kmax-kib", "16", "--ecn-pmax", "1", "--metrics-only"],
            cwd=ROOT, text=True, capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines()]
        summary = next(e for e in events if e["event"] == "summary")
        self.assertGreater(summary["ecn_marks"], 0)
        self.assertGreater(summary["cnp_events"], 0)
        self.assertGreater(summary["low_rate_total_us"], 0)

    def test_retransmission_work_is_deduplicated_and_drained(self):
        events = self.run_group("P1")
        summary = next(e for e in events if e["event"] == "summary")
        self.assertGreater(summary["retransmit_requests_coalesced"], 0)
        self.assertEqual(summary["pending_retransmissions_final"], 0)
        self.assertEqual(summary["inflight_retransmissions_final"], 0)
        self.assertEqual(summary["outstanding_psns_final"], 0)
        self.assertEqual(summary["rx_queue_bytes_final"], 0)
        self.assertEqual(summary["control_queue_bytes_final"], 0)
        self.assertLess(summary["duplicate_full_arrivals"], 3446)

    def test_pretrim_ce_header_can_drive_cnp(self):
        proc = subprocess.run(
            [str(BIN), "--group", "P1CE", "--seed", "13", "--scenario", "minimal",
             "--cc", "dcqcn", "--trim-ce", "preserve"],
            cwd=ROOT, text=True, capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines()]
        names = [e["event"] for e in events]
        self.assertIn("trim_ce_mark", names)
        self.assertIn("cnp_arrive", names)

    def test_sustained_congestion_changes_later_dcqcn_pacing(self):
        proc = subprocess.run(
            [str(BIN), "--group", "H0", "--seed", "13", "--scenario", "cc_probe",
             "--cc", "dcqcn"], cwd=ROOT, text=True, capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines()]
        sends = [e for e in events if e["event"] == "pacing_send"]
        self.assertTrue(any(e["actual_pacing_gbps"] < e["application_limit_gbps"]
                            for e in sends))
        self.assertTrue(any(e["inter_send_us"] > 0.2 for e in sends))
        summary = next(e for e in events if e["event"] == "summary")
        self.assertGreater(summary["cnp_events"], 0)

    def test_late_and_paused_target_leaf_controls_are_runnable(self):
        for group in ("P2", "P3"):
            proc = subprocess.run(
                [str(BIN), "--group", group, "--seed", "13", "--scenario", "minimal",
                 "--cc", "dcqcn"], cwd=ROOT, text=True, capture_output=True, check=True)
            events = [json.loads(line) for line in proc.stdout.splitlines()]
            config = next(e for e in events if e["event"] == "config")
            summary = next(e for e in events if e["event"] == "summary")
            self.assertEqual(summary["incomplete_flows"], 0)
            self.assertIn(config["trim_mode"], ("late", "pre"))

    def test_full_topology_exports_physical_queues_serializers_and_mmus(self):
        events = self.run_group("H0")
        inventory = [e for e in events if e["event"] == "link_inventory"]
        self.assertEqual(len(inventory), 128)
        switch_links = [e for e in inventory if e["mmu"] != "endpoint"]
        self.assertTrue(all(e["queue"] and e["serializer"] for e in switch_links))
        self.assertEqual({e["mmu"] for e in switch_links},
                         {f"leaf{i}" for i in range(4)} | {f"spine{i}" for i in range(8)})

    def test_explicit_shared_spine_egress_competes_more_than_split(self):
        summaries = {}
        for scenario in ("path_compete_shared", "path_compete_split"):
            proc = subprocess.run(
                [str(BIN), "--group", "H0", "--seed", "13", "--scenario", scenario,
                 "--cc", "fixed"], cwd=ROOT, text=True, capture_output=True, check=True)
            events = [json.loads(line) for line in proc.stdout.splitlines()]
            summaries[scenario] = next(e for e in events if e["event"] == "summary")
        self.assertGreater(summaries["path_compete_shared"]["max_switch_queue_bytes"],
                           summaries["path_compete_split"]["max_switch_queue_bytes"])
        self.assertGreater(summaries["path_compete_shared"]["competition_cct_us"],
                           summaries["path_compete_split"]["competition_cct_us"])

    def test_route_seed_changes_flow_path_without_changing_traffic(self):
        mappings = []
        for seed in (1, 2):
            proc = subprocess.run(
                [str(BIN), "--group", "H0", "--seed", str(seed), "--scenario", "permutation",
                 "--cc", "dcqcn"], cwd=ROOT, text=True, capture_output=True, check=True)
            events = [json.loads(line) for line in proc.stdout.splitlines()]
            mappings.append([(e["flow"], e["spine"]) for e in events
                             if e["event"] == "path" and e["flow"].startswith("healthy-")])
        self.assertEqual([name for name, _ in mappings[0]], [name for name, _ in mappings[1]])
        self.assertNotEqual(mappings[0], mappings[1])

    def test_ops_sprays_packets_and_hybrid_keeps_background_on_ecmp(self):
        proc = subprocess.run(
            [str(BIN), "--group", "diag", "--seed", "13", "--scenario", "permutation",
             "--cc", "dcqcn", "--lb", "hybrid", "--policy", "off",
             "--rx-mode", "normal"], cwd=ROOT, text=True, capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines()]
        by_flow = {}
        for event in events:
            if event["event"] == "packet_path":
                by_flow.setdefault(event["flow"], set()).add(event["spine"])
        self.assertTrue(any(flow.startswith("healthy-") for flow in by_flow))
        self.assertTrue(any(flow.startswith("background-") for flow in by_flow))
        self.assertTrue(all(len(spines) > 1 for flow, spines in by_flow.items()
                            if flow.startswith("healthy-")))
        self.assertTrue(all(len(spines) == 1 for flow, spines in by_flow.items()
                            if flow.startswith("background-")))

    def test_hybrid_jct_uses_explicit_healthy_role_for_ops(self):
        proc = subprocess.run(
            [str(BIN), "--group", "jct-route", "--seed", "13",
             "--scenario", "jct_permutation", "--cc", "dcqcn", "--lb", "hybrid",
             "--policy", "off", "--rx-mode", "normal", "--background-kib", "64",
             "--compact"], cwd=ROOT, text=True, capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines()]
        by_flow = {}
        roles = {}
        for event in events:
            if event["event"] == "packet_path":
                by_flow.setdefault(event["flow"], set()).add(event["spine"])
                roles[event["flow"]] = event.get("flow_role")
        healthy = {f: paths for f, paths in by_flow.items() if "-healthy-" in f}
        background = {f: paths for f, paths in by_flow.items() if f.startswith("background-")}
        self.assertTrue(healthy)
        self.assertTrue(background)
        self.assertTrue(all(roles[f] == "healthy" and len(paths) > 1
                            for f, paths in healthy.items()))
        self.assertTrue(all(roles[f] == "background" and len(paths) == 1
                            for f, paths in background.items()))

    def test_ecn_thresholds_and_probability_are_runtime_configured(self):
        proc = subprocess.run(
            [str(BIN), "--group", "ecn-config", "--seed", "13",
             "--scenario", "minimal", "--cc", "dcqcn", "--policy", "off",
             "--rx-mode", "normal", "--leaf-ecn-kmin-kib", "64",
             "--leaf-ecn-kmax-kib", "256", "--spine-ecn-kmin-kib", "32",
             "--spine-ecn-kmax-kib", "128", "--ecn-pmax", "0.1",
             "--metrics-only"], cwd=ROOT, text=True, capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines()]
        config = next(e for e in events if e["event"] == "resolved_config")
        self.assertEqual(config["model_version"], "queue-fixed-v6-ecn-calibration")
        self.assertEqual(config["leaf_ecn_kmin_bytes"], 64 * 1024)
        self.assertEqual(config["leaf_ecn_kmax_bytes"], 256 * 1024)
        self.assertEqual(config["spine_ecn_kmin_bytes"], 32 * 1024)
        self.assertEqual(config["spine_ecn_kmax_bytes"], 128 * 1024)
        self.assertAlmostEqual(config["ecn_pmax"], 0.1)
        self.assertEqual(config["ecn_queue_sample"], "pre_admission_data_memory_bytes")

    def test_ecn_probability_matches_piecewise_function_and_is_deterministic(self):
        cmd = [str(BIN), "--group", "ecn-function", "--seed", "13",
               "--scenario", "cc_probe", "--cc", "dcqcn", "--policy", "off",
               "--rx-mode", "normal", "--leaf-ecn-kmin-kib", "8",
               "--leaf-ecn-kmax-kib", "16", "--spine-ecn-kmin-kib", "8",
               "--spine-ecn-kmax-kib", "16", "--ecn-pmax", "0.1"]
        decisions = []
        for _ in range(2):
            proc = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=True)
            events = [json.loads(line) for line in proc.stdout.splitlines()]
            current = [e for e in events if e["event"] == "ecn_decision"]
            self.assertTrue(any(e["queue_bytes"] < e["kmin_bytes"] for e in current))
            self.assertTrue(any(e["queue_bytes"] >= e["kmax_bytes"] for e in current))
            for event in current:
                q, kmin, kmax = (event["queue_bytes"], event["kmin_bytes"],
                                 event["kmax_bytes"])
                expected = (0.0 if q < kmin else 1.0 if q >= kmax else
                            0.1 * (q - kmin) / (kmax - kmin))
                self.assertAlmostEqual(event["mark_probability"], expected, places=6)
            decisions.append([(e["psn"], e["switch"], e["marked"], e["sample"])
                              for e in current])
        self.assertEqual(decisions[0], decisions[1])

    def test_spine_dynamic_pfc_stops_upstream_and_uses_headroom(self):
        proc = subprocess.run(
            [str(BIN), "--group", "diag", "--seed", "13",
             "--scenario", "pfc_chain_probe", "--cc", "fixed",
             "--policy", "off", "--rx-mode", "normal"],
            cwd=ROOT, text=True, capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines()]
        names = [e["event"] for e in events]
        self.assertIn("switch_mmu_xoff", names)
        self.assertIn("upstream_data_pause", names)
        self.assertIn("headroom_allocate", names)
        self.assertIn("switch_mmu_xon", names)
        self.assertIn("upstream_data_resume", names)
        summary = next(e for e in events if e["event"] == "summary")
        budget = next(e for e in events if e["event"] == "budget_status")
        self.assertEqual(summary["mmu_budget_violations"], 0)
        self.assertEqual(budget["admission_drops"], 0)
        self.assertTrue(budget["run_valid"])

    def test_seed13_a2a_off_and_paused_stay_within_every_switch_budget(self):
        for policy in ("off", "paused"):
            proc = subprocess.run(
                [str(BIN), "--group", policy, "--seed", "13", "--scenario", "a2a",
                 "--cc", "dcqcn", "--policy", policy, "--rx-mode", "pause"],
                cwd=ROOT, text=True, capture_output=True, check=True)
            events = [json.loads(line) for line in proc.stdout.splitlines()]
            summary = next(e for e in events if e["event"] == "summary")
            budget = next(e for e in events if e["event"] == "budget_status")
            self.assertEqual(summary["mmu_budget_violations"], 0)
            self.assertEqual(budget["admission_drops"], 0)
            self.assertEqual(summary["offered_bytes"], summary["delivered_unique_bytes"])
            self.assertTrue(budget["run_valid"])

    def test_real_port_scheduler_honors_pause_and_pool_eligibility(self):
        proc = subprocess.run(
            [str(BIN), "--group", "diag", "--seed", "13",
             "--scenario", "pfc_chain_probe", "--cc", "fixed",
             "--policy", "off", "--rx-mode", "normal"],
            cwd=ROOT, text=True, capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines()]
        budgets = {e["switch"]: e for e in events if e["event"] == "switch_budget"}
        self.assertEqual(budgets["leaf0"]["data_shared"], 6272 * 1024)
        self.assertEqual(budgets["spine0"]["data_shared"], 1376 * 1024)

        starts = [e for e in events if e["event"] == "tx_start"]
        ends = [e for e in events if e["event"] == "tx_end"]
        self.assertTrue(starts)
        self.assertEqual(len(starts), len(ends))
        by_port = {}
        for event in starts + ends:
            by_port.setdefault(event["port"], []).append(event)
        for port, timeline in by_port.items():
            timeline.sort(key=lambda e: (e["time_us"], e["event"] == "tx_start"))
            active = 0
            for event in timeline:
                active += 1 if event["event"] == "tx_start" else -1
                self.assertIn(active, (0, 1), (port, event, active))
            self.assertEqual(active, 0, port)

        budget = next(e for e in events if e["event"] == "budget_status")
        self.assertEqual(budget["serializer_overlap_violations"], 0)
        self.assertEqual(budget["paused_tx_start_violations"], 0)
        self.assertEqual(budget["headroom_violations"], 0)
        self.assertLessEqual(budget["max_pg_headroom_used"], 96 * 1024)
        self.assertTrue(budget["run_valid"])

    def test_runtime_pause_threshold_and_buffer_totals_are_resolved(self):
        proc = subprocess.run(
            [str(BIN), "--group", "P3", "--seed", "13", "--scenario", "minimal",
             "--cc", "dcqcn", "--policy", "paused", "--rx-mode", "pause",
             "--pause-us", "20", "--trim-threshold-kib", "16",
             "--leaf-buffer-mib", "4", "--spine-buffer-mib", "1"],
            cwd=ROOT, text=True, capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines()]
        config = next(e for e in events if e["event"] == "resolved_config")
        self.assertEqual(config["pause_duration_us"], 20)
        self.assertEqual(config["trim_threshold_bytes"], 16 * 1024)
        self.assertEqual(config["leaf_total_bytes"], 4 * 1024 * 1024)
        self.assertEqual(config["spine_total_bytes"], 1024 * 1024)
        resume = next(e for e in events if e["event"] == "nic_consume_resume")
        self.assertAlmostEqual(resume["time_us"], 70.0)
        budgets = {e["switch"]: e for e in events if e["event"] == "switch_budget"}
        self.assertEqual(budgets["leaf0"]["data_shared"], 2176 * 1024)
        self.assertEqual(budgets["spine0"]["data_shared"], 352 * 1024)

    def test_paused_gate_bound_uses_runtime_trim_threshold(self):
        for threshold in (8, 64):
            proc = subprocess.run(
                [str(BIN), "--group", "gate-bound", "--seed", "13",
                 "--scenario", "a2a", "--cc", "dcqcn", "--policy", "paused",
                 "--rx-mode", "pause", "--trim-threshold-kib", str(threshold),
                 "--metrics-only"], cwd=ROOT, text=True, capture_output=True,
                check=True)
            events = [json.loads(line) for line in proc.stdout.splitlines()]
            summary = next(e for e in events if e["event"] == "summary")
            self.assertEqual(summary["paused_gate_bound_base_bytes"],
                             max(summary["paused_gate_q0_bytes"], threshold * 1024))

    def test_incast_shape_is_runtime_configured(self):
        proc = subprocess.run(
            [str(BIN), "--group", "incast", "--seed", "13", "--scenario", "permutation",
             "--cc", "dcqcn", "--policy", "off", "--rx-mode", "normal",
             "--background-senders", "4", "--background-kib", "64", "--metrics-only"],
            cwd=ROOT, text=True, capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines()]
        config = next(e for e in events if e["event"] == "resolved_config")
        self.assertEqual(config["background_senders"], 4)
        self.assertEqual(config["background_bytes_per_sender"], 64 * 1024)

    def test_normal_calibration_can_disable_background_traffic(self):
        proc = subprocess.run(
            [str(BIN), "--group", "no-background", "--seed", "13",
             "--scenario", "permutation", "--cc", "dcqcn", "--policy", "off",
             "--rx-mode", "normal", "--background-senders", "0", "--metrics-only"],
            cwd=ROOT, text=True, capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines()]
        config = next(e for e in events if e["event"] == "resolved_config")
        summary = next(e for e in events if e["event"] == "summary")
        self.assertEqual(config["background_senders"], 0)
        self.assertEqual(summary["background_fct_us"], 0)

    def test_paused_spines_trims_at_real_spine_egress_admission(self):
        proc = subprocess.run(
            [str(BIN), "--group", "spine-trim", "--seed", "13",
             "--scenario", "permutation", "--cc", "fixed",
             "--policy", "paused-spines", "--rx-mode", "normal",
             "--trim-threshold-kib", "8", "--background-source-rate-gbps", "400",
             "--background-kib", "1024", "--compact"], cwd=ROOT, text=True,
            capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines()]
        trims = [e for e in events if e["event"] == "trim" and
                 e.get("trim_switch", "").startswith("spine")]
        self.assertTrue(trims)
        self.assertTrue(all(e["gate_open"] for e in trims))
        psn = trims[0]["psn"]
        names = [e["event"] for e in events if e.get("psn") == psn]
        for name in ("header_arrive", "nack_arrive", "retransmit", "payload_deliver"):
            self.assertIn(name, names)

    def test_jct_rounds_are_completion_dependent(self):
        proc = subprocess.run(
            [str(BIN), "--group", "jct", "--seed", "13", "--scenario", "jct_permutation",
             "--cc", "dcqcn", "--policy", "off", "--rx-mode", "normal",
             "--background-kib", "8192", "--metrics-only"], cwd=ROOT,
            text=True, capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines()]
        jct = next(e for e in events if e["event"] == "jct_summary")
        self.assertEqual(jct["completed_rounds"], 20)
        self.assertEqual(len(jct["round_cct_us"]), 20)
        self.assertGreaterEqual(jct["healthy_jct_us"], sum(jct["round_cct_us"]) + 19 * 50)
        self.assertEqual(len([e for e in events if e["event"] == "round_start"]), 20)
        self.assertEqual(len([e for e in events if e["event"] == "round_complete"]), 20)
        budget = next(e for e in events if e["event"] == "budget_status")
        self.assertTrue(budget["run_valid"])

    def test_seed2_long_background_runs_until_reliable_completion(self):
        proc = subprocess.run(
            [str(BIN), "--group", "seed2-long", "--seed", "2",
             "--scenario", "jct_a2a", "--cc", "dcqcn", "--lb", "ecmp",
             "--policy", "off", "--rx-mode", "pause", "--fault-start-us", "700",
             "--pause-us", "200", "--background-kib", "8192",
             "--leaf-ecn-kmin-kib", "32", "--leaf-ecn-kmax-kib", "128",
             "--spine-ecn-kmin-kib", "16", "--spine-ecn-kmax-kib", "64",
             "--ecn-pmax", "0.01", "--metrics-only"], cwd=ROOT, text=True,
            capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines()]
        budget = next(e for e in events if e["event"] == "budget_status")
        summary = next(e for e in events if e["event"] == "summary")
        self.assertTrue(budget["reliable_delivery"])
        self.assertTrue(budget["final_drained"])
        self.assertTrue(budget["run_valid"])
        self.assertEqual(summary["incomplete_flows"], 0)

    def test_rto_is_armed_only_after_actual_pacing_send(self):
        proc = subprocess.run(
            [str(BIN), "--group", "slow-pacing", "--seed", "13",
             "--scenario", "permutation", "--cc", "fixed", "--policy", "off",
             "--rx-mode", "normal", "--background-senders", "1",
             "--background-kib", "64", "--background-source-rate-gbps", "0.01"],
            cwd=ROOT, text=True, capture_output=True, check=True)
        events = [json.loads(line) for line in proc.stdout.splitlines()]
        sent = {e["psn"]: e["time_us"] for e in events if e["event"] == "pacing_send"}
        for event in (e for e in events if e["event"] == "rto"):
            self.assertIn(event["psn"], sent)
            self.assertGreaterEqual(event["time_us"], sent[event["psn"]] + 1000)


if __name__ == "__main__":
    unittest.main()
