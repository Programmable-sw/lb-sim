#!/usr/bin/env python3

import copy
import csv
import hashlib
import importlib.util
import json
import math
import re
import shutil
import struct
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "experiments/n-mrc/build_feedback_cadence_report.py"
SIMULATOR = ROOT / "sim/datacenter/htsim_roce"
SIMULATOR_HASH = hashlib.sha256(SIMULATOR.read_bytes()).hexdigest()
SEEDS = (13, 29, 47)
SUBJECTS = ("avail", "grade", "netaware")
RUNNER_SCHEMES = (
    "ecmp_rr", "ops", "reps", "mrc", "sglb", "ar", "drill",
    "avail", "grade", "netaware",
)
CADENCES = ("fixed5", "fixed_rtt", "bdp_triggered")
EVALUATION_STAGES = ("pilot-time", "pilot-bdp", "simple", "alltoall")
FIXTURE_TOPOLOGIES = {
    128: {
        "nodes": 128, "hosts_per_leaf": 64, "leaves": 2,
        "spines": 64, "paths": 64, "tiers": 2,
    },
    512: {
        "nodes": 512, "hosts_per_leaf": 64, "leaves": 8,
        "spines": 64, "paths": 64, "tiers": 2,
    },
}
SCENARIOS = (
    ("healthy_permutation_4mib", "guardrail", False),
    ("incast_8to1_4mib", "guardrail", False),
    ("asymmetric_permutation_4mib", "path_opportunity", False),
    ("mixed_fixed_background_target_4mib", "path_opportunity", False),
    ("periodic_hotspot_target_4mib", "path_opportunity", False),
    ("full_global_p4_64mib_background_off", "path_opportunity", True),
)
FIGURE_STEMS = (
    "normalized_tail_heatmap", "key_fct_cdf", "alltoall_cct",
    "topology_scaling", "queue_cv_cdf", "recovery_cost",
    "feedback_pareto", "seed_ranges",
)
DERIVED_ARTIFACTS = (
    "normalized_metrics.csv", "seed_metrics.csv",
    "empirical_seed_ecdf.csv", "feedback_pareto.csv",
    "runtime_config_audit.csv",
)


def load_report():
    spec = importlib.util.spec_from_file_location(
        "build_feedback_cadence_report", SCRIPT
    )
    if spec is None or spec.loader is None:
        raise AssertionError(f"cannot load {SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def manifest_fingerprint(manifest):
    payload = json.dumps(
        manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def seal_invocation(invocation):
    sealed = copy.deepcopy(invocation)
    sealed.pop("invocation_sha256", None)
    sealed["invocation_sha256"] = manifest_fingerprint(sealed)
    return sealed


def selection_fingerprint(document):
    payload = {
        key: value for key, value in document.items()
        if key != "selection_sha256"
    }
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def cadence_args(scheme, cadence):
    prefix = "netaware" if scheme == "netaware" else "stor"
    values = {
        "fixed5": (1, 5, 5),
        "fixed_rtt": (1, 7, 7),
        "bdp_triggered": (86, 5, 14),
    }[cadence]
    args = [
        f"-{prefix}_feedback_pkts", str(values[0]),
        f"-{prefix}_feedback_min_us", str(values[1]),
        f"-{prefix}_feedback_max_us", str(values[2]),
    ]
    if prefix == "stor":
        args += [
            "-stor_trim_feedback_min_us",
            str(values[2] if cadence == "bdp_triggered" else values[1]),
        ]
    return args


def command_for(seed, scheme, cadence, nodes=128):
    topology = FIXTURE_TOPOLOGIES[nodes]
    return [
        "sim/datacenter/htsim_roce",
        "-nodes", str(nodes), "-tiers", "2",
        "-paths", str(topology["paths"]),
        "-linkspeed", "400000", "-queue_type", "composite_ecn_lb",
        "-host_queue_type", "prio", "-mtu", "4096",
        "-cc", "dctcp_variant", "-roce_rx_mode", "sp",
        "-roce_sack_bitmap_bits", "64", "-queue_cv_sample_us", "100",
        "-end", "20000",
        "-seed", str(seed), "-lb", scheme,
    ] + cadence_args(scheme, cadence)


def case_config_identity(manifest):
    omitted = {"-seed", "-o", "-tm", "-conns"}
    argv = []
    index = 0
    while index < len(manifest["argv"]):
        token = manifest["argv"][index]
        if token in omitted and index + 1 < len(manifest["argv"]):
            index += 2
            continue
        argv.append(token)
        index += 1
    scenario = dict(manifest["scenario"])
    scenario.pop("seed", None)
    return manifest_fingerprint({
        "topology": manifest["topology"],
        "stage": manifest["stage"],
        "scenario": scenario,
        "scheme": manifest["scheme"],
        "cadence": manifest["cadence"],
        "alltoall": manifest["alltoall"],
        "timing": manifest["timing"],
        "flow_identity_mode": manifest["flow_identity"]["mode"],
        "argv": argv,
    })


def write_fixture_run_index(root):
    root = Path(root)
    stages = {}
    for stage in EVALUATION_STAGES:
        stage_dir = root / "raw" / stage
        ledger_path = stage_dir / "expected_cases.jsonl"
        intent_path = stage_dir / "run_intent.json"
        if not ledger_path.is_file() or not intent_path.is_file():
            continue
        ledger = [
            json.loads(line) for line in ledger_path.read_text().splitlines()
        ]
        intent = json.loads(intent_path.read_text(encoding="utf-8"))
        case_keys = [row["case_key"] for row in ledger]
        invocation_id = f"fixture-{stage}-invocation"
        candidates = sorted({
            key["scheme"] if key["cadence"] == "none" else
            f"{key['scheme']}_{key['cadence']}"
            for key in case_keys
        })
        stages[stage] = {
            "invocation_id": invocation_id,
            "ledger_path": ledger_path.relative_to(root).as_posix(),
            "ledger_sha256": hashlib.sha256(ledger_path.read_bytes()).hexdigest(),
            "run_intent_path": intent_path.relative_to(root).as_posix(),
            "run_intent_sha256": hashlib.sha256(
                intent_path.read_bytes()
            ).hexdigest(),
            "scenario_patterns": intent["scenario_patterns"],
            "results_path": f"raw/{stage}/results.jsonl",
            "case_count": len(ledger),
            "case_identity_sha256": manifest_fingerprint(case_keys),
            "nodes": sorted({key["nodes"] for key in case_keys}),
            "seeds": sorted({key["seed"] for key in case_keys}),
            "scenarios": sorted({key["scenario"] for key in case_keys}),
            "candidates": candidates,
        }
    required = [stage for stage in EVALUATION_STAGES if stage in stages]
    invocations = []
    for stage in required:
        identities = [
            (
                candidate[:-len(f"_{cadence}")], cadence
            )
            for candidate in stages[stage]["candidates"]
            for cadence in CADENCES
            if candidate.endswith(f"_{cadence}")
        ]
        schemes = {
            scheme for scheme, unused_cadence in identities
        } | {
            candidate for candidate in stages[stage]["candidates"]
            if not any(candidate.endswith(f"_{cadence}") for cadence in CADENCES)
        }
        invocations.append(seal_invocation({
            "invocation_id": stages[stage]["invocation_id"],
            "requested_stage": stage,
            "expanded_required_stages": [stage],
            "requested_nodes": stages[stage]["nodes"],
            "requested_seeds": stages[stage]["seeds"],
            "scenario_patterns": stages[stage]["scenario_patterns"],
            "requested_schemes": [
                scheme for scheme in RUNNER_SCHEMES if scheme in schemes
            ],
            "requested_cadences": [
                cadence for cadence in CADENCES
                if any(item_cadence == cadence for unused_scheme, item_cadence in identities)
            ],
            "stages": {stage: stages[stage]},
        }))
    (root / "run_index.json").write_text(
        json.dumps({
            "schema_version": 1,
            "required_stages": required,
            "stages": stages,
            "invocations": invocations,
        }, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _opportunity_ratio(scheme, cadence):
    if scheme == "avail":
        return {"fixed5": 1.0, "fixed_rtt": 0.90, "bdp_triggered": 0.92}[cadence]
    return 1.0


class Fixture:
    def __init__(self, root):
        self.root = Path(root)
        self.rows = []
        self.cases = {}

    def add(self, seed=13, scheme="avail", cadence="fixed5",
            scenario="healthy_permutation_4mib", opportunity="guardrail",
            alltoall=False, nodes=128, stage=None):
        topology = FIXTURE_TOPOLOGIES[nodes]
        candidate = f"{scheme}_{cadence}"
        stage = stage or (
            "alltoall" if alltoall else
            "pilot-bdp" if cadence == "bdp_triggered" else "pilot-time"
        )
        case_dir = (
            self.root / "raw" / stage / f"nodes_{nodes}" / f"seed_{seed}"
            / scenario / candidate
        )
        case_dir.mkdir(parents=True, exist_ok=True)
        expected_target = (
            nodes * (nodes - 1) if alltoall
            else 8 if scenario.startswith("incast_")
            else nodes
        )
        expected_background = 48 if scenario.startswith("mixed_") else 0
        scenario_config = {
            "name": scenario,
            "traffic": "alltoall" if alltoall else "permutation",
            "opportunity": opportunity,
            "size_bytes": 64 << 20 if alltoall else 4 << 20,
        }
        if alltoall:
            scenario_config["seed"] = seed
        alltoall_config = None
        if alltoall:
            alltoall_config = {
                "group_size": nodes,
                "parallel": 4,
                "flow_size_bytes": 64 << 20,
                "background_enabled": False,
            }
        manifest = {
            "simulator_sha256": SIMULATOR_HASH,
            "traffic_sha256": "c" * 64,
            "argv": command_for(seed, scheme, cadence, nodes),
            "parser_version": 10,
            "topology": dict(topology),
            "stage": stage,
            "seed": seed,
            "scenario": scenario_config,
            "scheme": scheme,
            "cadence": cadence,
            "expected_flows": {
                "target": expected_target, "background": expected_background,
            },
            "alltoall": alltoall_config,
            "timing": {"simulation_end_us": 20000},
            "flow_identity": {"mode": "strict_idmap"},
        }
        fingerprint = manifest_fingerprint(manifest)
        seed_offset = {13: -1.0, 29: 0.0, 47: 1.0}[seed]
        topology_factor = {128: 1.0, 512: 1.25}[nodes]
        healthy_p99 = (100.0 + seed_offset) * topology_factor
        ratio = _opportunity_ratio(scheme, cadence)
        if alltoall:
            p99_ratio = {
                "fixed5": 1.0, "fixed_rtt": 1.4, "bdp_triggered": 0.7,
            }[cadence]
            p99 = 250.0 * topology_factor * p99_ratio
            cct = 1000.0 * topology_factor * ratio
        else:
            p99 = (
                healthy_p99 if opportunity == "guardrail"
                else 100.0 * topology_factor * ratio
            )
            cct = 0.0
        feedback_acks = (
            {"fixed5": 1000, "fixed_rtt": 900, "bdp_triggered": 500}[cadence]
            if scheme == "avail" else 500
        )
        nacks = {13: 0, 29: 10, 47: 10}[seed]
        retx_packets = {13: 10, 29: 0, 47: 10}[seed]
        feedback_nacks = nacks if scheme in ("avail", "grade") else 0
        result = {
            "stage": stage,
            "nodes": nodes,
            "seed": seed,
            "scenario": scenario,
            "candidate": candidate,
            "scheme": scheme,
            "cadence": cadence,
            "returncode": 0,
            "config_ok": True,
            "all_flows_completed": True,
            "target_completed": expected_target,
            "target_flows": expected_target,
            "background_completed": expected_background,
            "background_flows": expected_background,
            "avg_fct_us": p99 * 0.60,
            "p50_fct_us": p99 * 0.50,
            "p95_fct_us": p99 * 0.95,
            "p99_fct_us": p99,
            "p999_fct_us": p99 * 1.05,
            "max_fct_us": p99 * 1.10,
            "all_to_all_cct_us": cct,
            "feedback_acks": feedback_acks,
            "feedback_nacks": feedback_nacks,
            "feedback_messages_total": feedback_acks + feedback_nacks,
            "nacks": nacks,
            "nacks_ooo": 0,
            "nacks_trim": nacks,
            "nacks_loss": 0,
            "rtos": 0,
            "retx_packets": retx_packets,
            "retx_ratio": retx_packets / 100000.0,
            "queue_cv_spine_queue_cv": 0.125 + seed_offset / 1000,
            "nmrc_feedbacks": feedback_acks if scheme == "netaware" else 0,
            "nmrc_packet_feedbacks": feedback_acks // 2 if scheme == "netaware" else 0,
            "nmrc_time_feedbacks": feedback_acks // 2 if scheme == "netaware" else 0,
            "fingerprint": fingerprint,
        }
        manifest_path = case_dir / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        key = (
            (seed, scenario, scheme, cadence) if nodes == 128 else
            (nodes, seed, scenario, scheme, cadence)
        )
        self.cases[key] = {
            "manifest": manifest,
            "manifest_path": manifest_path,
            "result": result,
        }
        self.rows.append(result)
        return key

    def rewrite(self):
        path = self.root / "results.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for row in self.rows:
                handle.write(json.dumps(row, sort_keys=True) + "\n")
        result_stages = {case["result"]["stage"] for case in self.cases.values()}
        result_stages.update(row["stage"] for row in self.rows)
        for stage in result_stages:
            stage_path = self.root / "raw" / stage / "results.jsonl"
            stage_path.parent.mkdir(parents=True, exist_ok=True)
            with stage_path.open("w", encoding="utf-8") as handle:
                for row in self.rows:
                    if row["stage"] == stage:
                        handle.write(json.dumps(row, sort_keys=True) + "\n")
        by_stage = {}
        for case in self.cases.values():
            row = case["result"]
            by_stage.setdefault(row["stage"], []).append(case)
        for stage, cases in by_stage.items():
            ordered = sorted(
                cases,
                key=lambda case: (
                    case["result"]["nodes"], case["result"]["seed"],
                    case["result"]["scenario"],
                    case["result"]["candidate"],
                ),
            )
            case_keys = [{
                "stage": case["result"]["stage"],
                "nodes": case["result"]["nodes"],
                "scenario": case["result"]["scenario"],
                "seed": case["result"]["seed"],
                "scheme": case["result"]["scheme"],
                "cadence": case["result"]["cadence"],
            } for case in ordered]
            stage_dir = self.root / "raw" / stage
            with (stage_dir / "expected_cases.jsonl").open(
                    "w", encoding="utf-8") as handle:
                for case, key in zip(ordered, case_keys):
                    manifest = case["manifest"]
                    handle.write(json.dumps({
                        "ledger_version": 1,
                        "case_key": key,
                        "fingerprint": manifest_fingerprint(manifest),
                        "traffic_sha256": manifest["traffic_sha256"],
                        "config_identity": case_config_identity(manifest),
                    }, sort_keys=True) + "\n")
            intent = {
                "ledger_version": 1,
                "stage": stage,
                "scenario_patterns": [],
                "requested_nodes": sorted({
                    key["nodes"] for key in case_keys
                }),
                "requested_scenarios": sorted({
                    key["scenario"] for key in case_keys
                }),
                "requested_seeds": sorted({
                    key["seed"] for key in case_keys
                }),
                "requested_candidates": list(dict.fromkeys(
                    (
                        case["result"]["scheme"]
                        if case["result"]["cadence"] == "none" else
                        f"{case['result']['scheme']}_{case['result']['cadence']}"
                    )
                    for case in ordered
                )),
                "expected_case_keys": case_keys,
            }
            (stage_dir / "run_intent.json").write_text(
                json.dumps(intent, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
        write_fixture_run_index(self.root)

    def mutate(self, key, manifest=None, result=None, refresh=True):
        case = self.cases[key]
        if manifest is not None:
            manifest(case["manifest"])
            case["manifest_path"].write_text(
                json.dumps(case["manifest"], sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            if refresh:
                case["result"]["fingerprint"] = manifest_fingerprint(
                    case["manifest"]
                )
        if result is not None:
            result(case["result"])
        self.rewrite()


def write_full_fixture(root):
    fixture = Fixture(root)
    for nodes in FIXTURE_TOPOLOGIES:
        for seed in SEEDS:
            for scenario, opportunity, alltoall in SCENARIOS:
                for scheme in SUBJECTS:
                    for cadence in CADENCES:
                        fixture.add(
                            seed, scheme, cadence, scenario, opportunity,
                            alltoall, nodes,
                        )
    fixture.rewrite()
    return fixture


def write_single_fixture(root):
    fixture = Fixture(root)
    key = fixture.add()
    fixture.rewrite()
    return fixture, key


def add_cross_stage_duplicate(fixture, key, stage="simple", mutate=None):
    original = fixture.cases[key]
    manifest = copy.deepcopy(original["manifest"])
    manifest["stage"] = stage
    result = copy.deepcopy(original["result"])
    result["stage"] = stage
    if mutate is not None:
        mutate(result)
    result["fingerprint"] = manifest_fingerprint(manifest)
    case_dir = (
        fixture.root / "raw" / stage / f"nodes_{result['nodes']}"
        / f"seed_{result['seed']}" / result["scenario"] / result["candidate"]
    )
    case_dir.mkdir(parents=True, exist_ok=True)
    (case_dir / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    stage_dir = fixture.root / "raw" / stage
    (stage_dir / "results.jsonl").write_text(
        json.dumps(result, sort_keys=True) + "\n", encoding="utf-8"
    )
    case_key = {
        "stage": stage, "nodes": result["nodes"],
        "scenario": result["scenario"], "seed": result["seed"],
        "scheme": result["scheme"], "cadence": result["cadence"],
    }
    (stage_dir / "expected_cases.jsonl").write_text(
        json.dumps({
            "ledger_version": 1,
            "case_key": case_key,
            "fingerprint": result["fingerprint"],
            "traffic_sha256": manifest["traffic_sha256"],
            "config_identity": case_config_identity(manifest),
        }, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    candidate = (
        result["scheme"] if result["cadence"] == "none" else
        f"{result['scheme']}_{result['cadence']}"
    )
    (stage_dir / "run_intent.json").write_text(
        json.dumps({
            "ledger_version": 1,
            "stage": stage,
            "scenario_patterns": [],
            "requested_nodes": [result["nodes"]],
            "requested_scenarios": [result["scenario"]],
            "requested_seeds": [result["seed"]],
            "requested_candidates": [candidate],
            "expected_case_keys": [case_key],
        }, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    write_fixture_run_index(fixture.root)
    return result


def read_csv(path):
    with Path(path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path, rows):
    fields = list(rows[0])
    with Path(path).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def assert_publication_artifacts(output):
    output = Path(output)
    expected_figures = {
        f"{stem}.{suffix}" for stem in FIGURE_STEMS
        for suffix in ("png", "pdf")
    }
    actual_figures = {
        path.name for path in output.iterdir()
        if path.is_file() and path.suffix in {".png", ".pdf"}
    }
    assert actual_figures == expected_figures, (
        sorted(actual_figures), sorted(expected_figures)
    )
    for stem in FIGURE_STEMS:
        png = output / f"{stem}.png"
        pdf = output / f"{stem}.pdf"
        assert png.stat().st_size > 10_000, png
        assert pdf.stat().st_size > 1_000, pdf
        payload = png.read_bytes()
        assert payload.startswith(b"\x89PNG\r\n\x1a\n"), png
        width, height = struct.unpack(">II", payload[16:24])
        assert width >= 1200 and height >= 700, (png, width, height)
        assert payload[24] == 8, (png, "expected 8-bit PNG")
        assert payload[25] in (2, 3, 6), (png, "expected color PNG")
        assert pdf.read_bytes().startswith(b"%PDF"), pdf

        try:
            from PIL import Image
        except ImportError:
            continue
        with Image.open(png) as image:
            colors = image.convert("RGB").resize((160, 100)).getcolors(16_001)
        assert colors is not None and len(colors) >= 8, (png, colors)

    for name in DERIVED_ARTIFACTS:
        path = output / name
        assert path.stat().st_size > 100, path
        assert read_csv(path), path

    report_path = output / "report.md"
    report = report_path.read_text(encoding="utf-8")
    assert report.startswith("# Feedback Cadence Evaluation")
    lowered = report.lower()
    assert not re.search(r"(?<![a-z])[+-]?(?:nan|inf(?:inity)?)(?![a-z])", lowered)
    required_sections = (
        "## Technical Summary", "## Key Findings and Visual Evidence",
        "## Scope, Data, and Metric Definitions", "## Methodology",
        "## Limitations and Robustness", "## Recommended Next Steps",
        "## Further Questions", "### Runtime Configuration Audit",
        "### RTT and BDP", "### Cadence Decisions",
        "### Ordinary Workloads", "### All-to-All CCT",
        "### Feedback Overhead", "### Data Quality and Rejections",
    )
    for section in required_sections:
        assert section in report, section
    for phrase in (
            "microseconds (us)", "feedback bandwidth (Mbps)",
            "simulated run duration", "feedback bytes (bytes)",
            "denominator", "fixed5", "empirical seed/cell distribution",
            "median and min/max", "2K resource caveat"):
        assert phrase in report, phrase
    for stem in FIGURE_STEMS:
        assert f"]({stem}.png)" in report, stem
        assert f"]({stem}.pdf)" in report, stem


def test_publication_ready_visualizations_and_report():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")
        output = Path(temp_dir) / "report"
        built = module.build_all(output, input_dir=fixture.root)
        assert_publication_artifacts(output)
        assert set(built["selected"]) == set(SUBJECTS)
        normalized = read_csv(output / "normalized_metrics.csv")
        assert normalized
        assert len(normalized) == (
            len(FIXTURE_TOPOLOGIES) * len(SCENARIOS)
            * len(SUBJECTS) * len(CADENCES)
        )
        assert {row["nodes"] for row in normalized} == {"128", "512"}
        assert {
            row["metric"] for row in normalized
            if row["scenario"] == "full_global_p4_64mib_background_off"
        } == {"all_to_all_cct_us"}
        assert {
            row["metric"] for row in normalized
            if row["scenario"] != "full_global_p4_64mib_background_off"
        } == {"p99_fct_us"}
        assert all(row["denominator"] for row in normalized)
        ecdf = read_csv(output / "empirical_seed_ecdf.csv")
        assert {row["metric"] for row in ecdf} == {
            "p99_fct_us", "queue_cv"
        }
        assert {row["grain"] for row in ecdf} == {
            "validated seed/cell"
        }
        pareto = read_csv(output / "feedback_pareto.csv")
        assert pareto
        assert len(pareto) == len(SUBJECTS) * len(CADENCES) * 2
        assert all(row["x_unit"] == "Mbps" for row in pareto)
        assert all(float(row["feedback_bandwidth_mbps"]) > 0 for row in pareto)
        assert all(float(row["simulation_duration_us_median"]) == 20000
                   for row in pareto)
        assert all(row["duration_definition"] for row in pareto)
        assert all(float(row["feedback_bytes_per_run"]) > 0 for row in pareto)
        assert all("fixed5" in row["denominator"] for row in pareto)
        seed_metrics = read_csv(output / "seed_metrics.csv")
        assert len(seed_metrics) == (
            len(FIXTURE_TOPOLOGIES) * len(SEEDS) * len(SCENARIOS)
            * len(SUBJECTS) * len(CADENCES)
        )
        crossed = [
            int(row["recovery_packets"]) for row in seed_metrics
            if row["scheme"] == "avail" and row["cadence"] == "fixed5"
            and row["all_to_all_cct_us"] == "0.0"
        ]
        assert crossed.count(10) == 20
        assert crossed.count(20) == 10
        assert all(float(row["simulation_duration_us"]) == 20000
                   for row in seed_metrics)
        assert all(row["simulation_duration_source"] for row in seed_metrics)
        assert all(
            math.isclose(
                float(row["feedback_bandwidth_mbps"]),
                float(row["feedback_bytes"]) * 8 / 20000,
            ) for row in seed_metrics
        )
        (output / "unexpected-figure.png").write_bytes(b"not a report figure")
        try:
            assert_publication_artifacts(output)
        except AssertionError:
            pass
        else:
            raise AssertionError("extra PNG must fail exact figure inventory")


def test_pareto_aggregates_seed_cell_bandwidth_before_median():
    module = load_report()
    normalized = [{
        "nodes": 128, "scenario": "ordinary", "scheme": "avail",
        "cadence": "fixed5", "metric": "p99_fct_us",
        "normalized_value": 1.0,
    }]
    seed_rows = [
        {
            "nodes": 128, "scenario": "ordinary", "scheme": "avail",
            "cadence": "fixed5", "seed": seed,
            "feedback_bytes": feedback_bytes,
            "simulation_duration_us": duration,
            "simulation_duration_source": "manifest timing + command -end",
            "feedback_bandwidth_mbps": feedback_bytes * 8 / duration,
        }
        for seed, feedback_bytes, duration in (
            (13, 1.0, 1.0), (29, 100.0, 2.0), (47, 101.0, 100.0)
        )
    ]
    derived, fields = module._derived_pareto_rows(normalized, seed_rows)
    assert len(derived) == 1
    assert "feedback_bytes_per_run" in fields
    assert "feedback_bandwidth_mbps" in fields
    assert derived[0]["feedback_bytes_per_run"] == 100.0
    assert derived[0]["simulation_duration_us_median"] == 2.0
    assert math.isclose(derived[0]["feedback_bandwidth_mbps"], 8.08)
    assert derived[0]["seed_cell_count"] == 3


def test_feedback_overhead_requires_and_uses_total_feedback_messages():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture, key = write_single_fixture(Path(temp_dir) / "valid")
        fixture.mutate(
            key,
            result=lambda row: (
                row.__setitem__("feedback_acks", 7),
                row.__setitem__("feedback_nacks", 2),
                row.__setitem__("feedback_messages_total", 9),
                row.__setitem__("nacks", 2),
                row.__setitem__("nacks_trim", 2),
            ),
        )
        rows = module.load_rows(fixture.root)
        assert rows.rejections == []
        assert rows[0]["feedback_count"] == 9
        assert rows[0]["feedback_bytes"] == 72

        missing, missing_key = write_single_fixture(Path(temp_dir) / "missing")
        missing.mutate(
            missing_key,
            result=lambda row: row.pop("feedback_messages_total"),
        )
        rejected = module.load_rows(missing.root)
        assert not rejected
        assert any(
            "missing_metric:feedback_messages_total" in item["reason"]
            for item in rejected.rejections
        )

        inconsistent, inconsistent_key = write_single_fixture(
            Path(temp_dir) / "inconsistent"
        )
        inconsistent.mutate(
            inconsistent_key,
            result=lambda row: row.__setitem__("feedback_messages_total", 999),
        )
        rejected = module.load_rows(inconsistent.root)
        assert not rejected
        assert any(
            "feedback_message_split_mismatch" in item["reason"]
            for item in rejected.rejections
        )


def test_normalized_rows_choose_one_scenario_metric_and_label_it():
    module = load_report()

    def record(nodes, scenario, cadence, p99, cct):
        return {
            "nodes": nodes, "scenario": scenario, "scheme": "avail",
            "cadence": cadence, "seed_count": 3,
            "p99_fct_us_median": p99,
            "all_to_all_cct_us_median": cct,
        }

    ordinary = "healthy_permutation_4mib"
    alltoall = "full_global_p4_64mib_background_off"
    rows = [
        record(128, ordinary, "fixed5", 100, 0),
        record(128, ordinary, "fixed_rtt", 80, 0),
        record(128, alltoall, "fixed5", 200, 1000),
        record(128, alltoall, "fixed_rtt", 500, 800),
    ]
    summary = {
        (row["nodes"], row["scenario"], row["scheme"], row["cadence"]): row
        for row in rows
    }
    derived, fields = module._derived_normalized_rows(summary)
    assert "metric_key" in fields
    assert "metric_label" in fields
    by_scenario = {}
    for row in derived:
        by_scenario.setdefault(row["scenario"], set()).add(row["metric"])
        assert row["metric"] in row["metric_key"]
        assert row["metric_label"] in {"p99 FCT", "All-to-All CCT"}
    assert by_scenario == {
        ordinary: {"p99_fct_us"},
        alltoall: {"all_to_all_cct_us"},
    }
    fixed_rtt = {
        (row["scenario"], row["cadence"]): row for row in derived
    }
    assert fixed_rtt[(ordinary, "fixed_rtt")]["normalized_value"] == 0.8
    assert fixed_rtt[(alltoall, "fixed_rtt")]["normalized_value"] == 0.8


def test_recovery_cost_is_summed_per_run_before_median_and_minmax():
    module = load_report()
    rows = [
        {
            "scheme": "avail", "cadence": "fixed5",
            "all_to_all_cct_us": 0, "nacks": nacks,
            "retx_packets": retx,
        }
        for nacks, retx in ((0, 100), (100, 0), (100, 100))
    ]
    stats = module._recovery_cost_statistics(rows)
    assert stats[("avail", "fixed5")] == {
        "median": 100, "min": 100, "max": 200, "run_count": 3,
    }


def test_topology_scaling_excludes_alltoall_and_has_insufficient_state():
    module = load_report()
    rows = [
        {
            "nodes": nodes, "scenario": scenario, "scheme": "avail",
            "cadence": "fixed5", "metric": "p99_fct_us",
            "metric_key": f"{scenario}|p99_fct_us",
            "workload_family": (
                "alltoall" if scenario.startswith("full_global_") else "ordinary"
            ),
            "normalized_value": value,
        }
        for nodes, scenario, value in (
            (128, "healthy_permutation_4mib", 1.0),
            (512, "healthy_permutation_4mib", 1.1),
            (2048, "full_global_p4_64mib_background_off", 9.0),
        )
    ]
    with tempfile.TemporaryDirectory() as temp_dir:
        output = Path(temp_dir)
        write_csv(output / "normalized_metrics.csv", rows)
        captured = []
        module._save_figure = lambda fig, unused_output, unused_stem: captured.append(fig)
        module._plot_topology_scaling(output)
        axis = captured.pop().axes[0]
        assert len(axis.lines) == 1
        assert list(axis.lines[0].get_xdata()) == [128, 512]
        module.plt.close(axis.figure)

        write_csv(output / "normalized_metrics.csv", rows[:1])
        module._plot_topology_scaling(output)
        axis = captured.pop().axes[0]
        assert not axis.lines
        assert any("Insufficient topology coverage" in text.get_text()
                   for text in axis.texts)
        module.plt.close(axis.figure)


def test_pareto_facets_and_offsets_coincident_categories_deterministically():
    module = load_report()
    rows = []
    for family in ("p99 FCT", "All-to-All CCT"):
        for scheme in ("avail", "grade"):
            rows.append({
                "scheme": scheme, "cadence": "fixed5",
                "metric_family": family, "feedback_bandwidth_mbps": 1.0,
                "normalized_tail_median": 1.0,
            })
    with tempfile.TemporaryDirectory() as temp_dir:
        output = Path(temp_dir)
        write_csv(output / "feedback_pareto.csv", rows)
        captured = []
        module._save_figure = lambda fig, unused_output, unused_stem: captured.append(fig)
        module._plot_feedback_pareto(output)
        fig = captured.pop()
        assert [axis.get_title() for axis in fig.axes] == [
            "p99 FCT", "All-to-All CCT",
        ]
        for axis in fig.axes:
            offsets = [
                tuple(collection.get_offsets()[0])
                for collection in axis.collections
            ]
            assert len(offsets) == 2
            assert len(set(offsets)) == 2
            assert len(axis.lines) >= 2
            labels = {text.get_text() for text in axis.texts}
            assert {"avail / fixed5", "grade / fixed5"} <= labels
        assert any("offset" in text.get_text().lower() for text in fig.texts)
        module.plt.close(fig)


def test_report_followups_are_derived_from_actual_topologies():
    module = load_report()
    next_steps, questions = module._topology_followups([128, 512])
    text = "\n".join(next_steps + questions)
    assert "2048-host" in text
    assert "512- and 2048-host" not in text
    assert "128 and 512 hosts" in text

    next_steps, questions = module._topology_followups([128, 512, 2048])
    text = "\n".join(next_steps + questions)
    assert "Add " not in text
    assert "128, 512, and 2048 hosts" in text


def test_simulation_duration_is_finite_positive_and_audited():
    module = load_report()

    def remove_end(manifest):
        index = manifest["argv"].index("-end")
        del manifest["argv"][index:index + 2]

    mutations = (
        lambda manifest: (
            manifest["timing"].pop("simulation_end_us"),
            remove_end(manifest),
        ),
        lambda manifest: (
            manifest["timing"].__setitem__("simulation_end_us", 0),
            manifest["argv"].__setitem__(
                manifest["argv"].index("-end") + 1, "0"
            ),
        ),
        lambda manifest: manifest["timing"].__setitem__(
            "simulation_end_us", 10000
        ),
        lambda manifest: (
            manifest["timing"].__setitem__("simulation_end_us", float("inf")),
            manifest["argv"].__setitem__(
                manifest["argv"].index("-end") + 1, "inf"
            ),
        ),
    )
    for mutate in mutations:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture, key = write_single_fixture(Path(temp_dir) / "input")
            fixture.mutate(key, manifest=mutate)
            rows = module.load_rows(fixture.root)
            assert not rows
            assert any(
                "timing_config:" in row["reason"]
                for row in rows.rejections
            ), rows.rejections

    duration, source = module._validated_simulation_duration({
        "timing": {"simulation_end_us": 1234}, "argv": ["simulator"],
    })
    assert duration == 1234
    assert source == "manifest timing.simulation_end_us"
    duration, source = module._validated_simulation_duration({
        "timing": None, "argv": ["simulator", "-end", "5678"],
    })
    assert duration == 5678
    assert source == "command -end"

    duration, source = module._validated_simulation_duration({
        "timing": {"simulation_end_us": 31474.83648},
        "argv": ["simulator", "-end", "31474.8"],
    })
    assert duration == 31474.8
    assert source == (
        "manifest timing.simulation_end_us audited against command -end"
    )


def test_fixture_cli_is_self_contained():
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--fixture-test"], cwd=ROOT,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        check=False,
    )
    assert result.returncode == 0, result.stdout
    assert "fixture-test: PASS" in result.stdout


def test_derived_ecdf_excludes_non_cadence_baselines():
    module = load_report()
    subject = {
        "scheme": "avail", "cadence": "fixed5",
        "all_to_all_cct_us": 0.0, "p99_fct_us": 100.0,
        "queue_cv": 0.1,
    }
    baseline = dict(subject, scheme="ecmp_rr", cadence="none")
    derived, unused_fields = module._derived_ecdf_rows([subject, baseline])
    assert {row["scheme"] for row in derived} == {"avail"}
    assert {row["cadence"] for row in derived} == {"fixed5"}


def expect_failure(action, text):
    try:
        action()
    except (RuntimeError, ValueError) as error:
        assert text in str(error), str(error)
        return
    raise AssertionError(f"expected failure containing {text!r}")


def test_load_rows_and_aggregate_complete_fixture():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(temp_dir)
        rows = module.load_rows(fixture.root)
        assert len(rows) == (
            len(FIXTURE_TOPOLOGIES) * len(SEEDS) * len(SCENARIOS)
            * len(SUBJECTS) * len(CADENCES)
        )
        assert rows.rejections == []
        summary = module.aggregate(rows)
        expected_groups = (
            len(FIXTURE_TOPOLOGIES) * len(SCENARIOS)
            * len(SUBJECTS) * len(CADENCES)
        )
        assert len(summary) == expected_groups
        assert isinstance(summary, dict)
        healthy = summary[(
            128, "healthy_permutation_4mib", "avail", "fixed5"
        )]
        assert healthy["seed_count"] == 3
        assert healthy["p99_fct_us_median"] == 100.0
        assert healthy["p99_fct_us_min"] == 99.0
        assert healthy["p99_fct_us_max"] == 101.0
        assert healthy["feedback_count_median"] == 1010
        assert healthy["feedback_bytes_median"] == 8080
        assert healthy["queue_cv_median"] == 0.125
        alltoall = summary[(
            128, "full_global_p4_64mib_background_off",
            "avail", "fixed_rtt",
        )]
        assert alltoall["all_to_all_cct_us_median"] == 900.0


def test_simple_selected_cadence_does_not_expand_recommendation_grid():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(temp_dir)
        for seed in SEEDS:
            for scheme in SUBJECTS:
                fixture.add(
                    seed=seed,
                    scheme=scheme,
                    cadence="fixed5",
                    scenario="simple_selected_only",
                    opportunity="guardrail",
                    stage="simple",
                )
        fixture.rewrite()

        built = module.build_all(
            Path(temp_dir) / "report", input_dir=fixture.root
        )

        assert built["selected"] == {
            "avail": "bdp_triggered",
            "grade": "fixed5",
            "netaware": "fixed5",
        }
        assert (
            128, "simple_selected_only", "avail", "fixed5"
        ) in built["summary"]


def test_recursive_stage_results_dedupe_identical_and_reject_conflicts():
    module = load_report()
    duplicate_key = (13, "healthy_permutation_4mib", "avail", "fixed5")
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "identical")
        fixture.mutate(
            duplicate_key,
            result=lambda row: (
                row.__setitem__("stdout", "/pilot-time/stdout.log"),
                row.__setitem__(
                    "command", ["sim", "-o", "/pilot-time/logout.dat"]
                ),
            ),
        )
        add_cross_stage_duplicate(
            fixture, duplicate_key,
            mutate=lambda row: (
                row.__setitem__("stdout", "/simple/stdout.log"),
                row.__setitem__(
                    "command", ["sim", "-o", "/simple/logout.dat"]
                ),
            ),
        )
        (fixture.root / "results.jsonl").unlink()
        rows = module.load_rows(fixture.root)
        assert len(rows) == len(fixture.rows)
        audits = [
            row for row in rows.rejections
            if row["reason"].startswith("duplicate_dedup:")
        ]
        assert len(audits) == 1
        assert "pilot-time" in audits[0]["reason"]
        assert "simple" in audits[0]["reason"]
        output = Path(temp_dir) / "report"
        built = module.build_all(output, input_dir=fixture.root)
        assert set(built["selected"]) == set(SUBJECTS)
        assert built["summary"][
            (128, "healthy_permutation_4mib", "avail", "fixed5")
        ]["seed_count"] == 3

    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "conflict")
        add_cross_stage_duplicate(
            fixture, duplicate_key,
            mutate=lambda row: row.__setitem__(
                "p99_fct_us", row["p99_fct_us"] + 1.0
            ),
        )
        (fixture.root / "results.jsonl").unlink()
        rows = module.load_rows(fixture.root)
        assert any(
            row["reason"].startswith("duplicate_conflict:")
            for row in rows.rejections
        )
        output = Path(temp_dir) / "report"
        expect_failure(
            lambda: module.build_all(output, input_dir=fixture.root),
            "recommendation blocked",
        )
        assert not (output / "selected-cadences.json").exists()


def _append_stage_result(fixture, payload):
    row = next(item for item in fixture.rows if item["stage"] == "pilot-time")
    path = fixture.root / "raw/pilot-time/results.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        if isinstance(payload, str):
            handle.write(payload + "\n")
        else:
            handle.write(json.dumps(payload(row), sort_keys=True) + "\n")


def _assert_result_set_blocks_selection(mutation, expected_reason):
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")
        _append_stage_result(fixture, mutation)
        output = Path(temp_dir) / "report"
        expect_failure(
            lambda: module.build_all(output, input_dir=fixture.root),
            "recommendation blocked",
        )
        assert not (output / "selected-cadences.json").exists()
        assert any(
            expected_reason in row["reason"]
            for row in read_csv(output / "data_quality_rejections.csv")
        )


def test_stage_result_set_rejects_conflicting_duplicate():
    def conflicting(row):
        duplicate = copy.deepcopy(row)
        duplicate["p99_fct_us"] += 10.0
        return duplicate

    _assert_result_set_blocks_selection(
        conflicting, "result_set_duplicate"
    )


def test_stage_result_set_rejects_identical_duplicate():
    _assert_result_set_blocks_selection(
        lambda row: copy.deepcopy(row), "result_set_duplicate"
    )


def test_stage_result_set_rejects_malformed_json():
    _assert_result_set_blocks_selection(
        '{"malformed":', "result_set_invalid:invalid_json"
    )


def test_stage_result_set_rejects_non_object_record():
    _assert_result_set_blocks_selection(
        "[]", "result_set_invalid:row_type"
    )


def test_stage_result_set_rejects_undeclared_record():
    def undeclared(row):
        extra = copy.deepcopy(row)
        extra["scheme"] = "undeclared"
        extra["cadence"] = "none"
        extra["candidate"] = "undeclared"
        return extra

    _assert_result_set_blocks_selection(
        undeclared, "result_set_undeclared_case"
    )


def test_strict_row_validation_records_each_reason():
    module = load_report()
    mutations = (
        ("topology_mismatch", lambda m: m["topology"].__setitem__("paths", 9), None, True),
        ("queue_config", lambda m: m["argv"].__setitem__(m["argv"].index("composite_ecn_lb"), "lossless"), None, True),
        ("rx_config", lambda m: m["argv"].__setitem__(m["argv"].index("sp"), "gbn"), None, True),
        ("cc_config", lambda m: m["argv"].__setitem__(m["argv"].index("dctcp_variant"), "none"), None, True),
        ("mtu_config", lambda m: m["argv"].__setitem__(m["argv"].index("4096"), "1500"), None, True),
        ("link_rate_config", lambda m: m["argv"].__setitem__(m["argv"].index("400000"), "100000"), None, True),
        ("cadence_config", lambda m: m["argv"].__setitem__(m["argv"].index("5", m["argv"].index("-stor_feedback_min_us")), "6"), None, True),
        ("flow_count_mismatch", None, lambda r: r.__setitem__("target_completed", 127), True),
        ("config_not_ok", None, lambda r: r.__setitem__("config_ok", False), True),
        ("incomplete_flows", None, lambda r: r.__setitem__("all_flows_completed", False), True),
        ("parser_config", lambda m: m.__setitem__("parser_version", 11), None, True),
        ("fingerprint_mismatch", lambda m: m.__setitem__("traffic_sha256", "d" * 64), None, False),
        ("non_finite_numeric", None, lambda r: r.__setitem__("p99_fct_us", math.inf), True),
        ("integer_counter", None, lambda r: r.__setitem__("retx_packets", -1), True),
        ("integer_counter", None, lambda r: r.__setitem__("feedback_acks", 1.5), True),
        ("integer_counter", None, lambda r: r.__setitem__("nacks", True), True),
        ("integer_counter", None, lambda r: r.__setitem__("target_flows", 128.0), True),
        ("integer_counter", None, lambda r: r.__setitem__("new_packets", 1.5), True),
        ("integer_counter", None, lambda r: r.__setitem__("rtos", 1.5), True),
        ("integer_counter", None, lambda r: r.__setitem__("queue_cv_spine_queue_count", 1.5), True),
        ("integer_counter", None, lambda r: r.__setitem__("composite_ecn_marks", 1.5), True),
        ("non_finite_numeric", None, lambda r: r.__setitem__("p99_fct_us", 10 ** 1000), True),
    )
    for reason, manifest_mutation, result_mutation, refresh in mutations:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture, key = write_single_fixture(temp_dir)
            fixture.mutate(
                key, manifest=manifest_mutation, result=result_mutation,
                refresh=refresh,
            )
            rows = module.load_rows(fixture.root)
            assert len(rows) == 0, reason
            matching = [
                item for item in rows.rejections
                if reason in item["reason"]
                and str(item["source"]).endswith("results.jsonl")
            ]
            assert len(matching) == 1, (reason, rows.rejections)
            rejection = matching[0]
            assert rejection["source"].endswith("results.jsonl")
            assert rejection["row"] == 1
            assert rejection["case"].startswith("128/13/")


def test_ar_scheme_uses_runner_cli_lb_mapping_strictly():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture, key = write_single_fixture(temp_dir)

        def make_ar_manifest(manifest):
            manifest["scheme"] = "ar"
            manifest["cadence"] = "none"
            argv = manifest["argv"]
            argv[argv.index("-lb") + 1] = "adaptive-routing"
            feedback_options = {
                "-stor_feedback_pkts", "-stor_feedback_min_us",
                "-stor_feedback_max_us", "-stor_trim_feedback_min_us",
            }
            manifest["argv"] = [
                token
                for index, token in enumerate(argv)
                if not (
                    token in feedback_options
                    or (index > 0 and argv[index - 1] in feedback_options)
                )
            ]

        fixture.mutate(
            key,
            manifest=make_ar_manifest,
            result=lambda row: (
                row.__setitem__("scheme", "ar"),
                row.__setitem__("cadence", "none"),
            ),
        )
        valid = module.load_rows(fixture.root)
        assert len(valid) == 1, valid.rejections
        assert valid[0]["scheme"] == "ar"

        fixture.mutate(
            key,
            manifest=lambda manifest: manifest["argv"].__setitem__(
                manifest["argv"].index("-lb") + 1, "ar"
            ),
        )
        invalid = module.load_rows(fixture.root)
        assert len(invalid) == 0
        matching = [
            item for item in invalid.rejections
            if "scheme_config" in item["reason"]
        ]
        assert len(matching) == 1
        assert "adaptive-routing" in matching[0]["reason"]


def test_duplicate_seed_and_mixed_provenance_are_rejected():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture, _ = write_single_fixture(temp_dir)
        fixture.rows.append(copy.deepcopy(fixture.rows[0]))
        fixture.rewrite()
        rows = module.load_rows(fixture.root)
        assert len(rows) == 0
        assert any(
            "result_set_duplicate" in item["reason"]
            for item in rows.rejections
        )

    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = Fixture(temp_dir)
        first = fixture.add(seed=13)
        second = fixture.add(seed=29)
        fixture.rewrite()
        fixture.mutate(
            second,
            manifest=lambda m: m.__setitem__("simulator_sha256", "b" * 64),
        )
        rows = module.load_rows(fixture.root)
        assert len(rows) == 1
        expected_hash = min(SIMULATOR_HASH, "b" * 64)
        expected_seed = 13 if expected_hash == SIMULATOR_HASH else 29
        assert rows[0]["seed"] == expected_seed
        assert rows[0]["simulator_sha256"] == expected_hash
        assert any("mixed_simulator_hash" in item["reason"] for item in rows.rejections)
        assert fixture.cases[first]["manifest"]["simulator_sha256"] == SIMULATOR_HASH


def test_expected_flow_counts_may_vary_across_seeds_but_not_cadences():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(temp_dir)
        scenario = "asymmetric_permutation_4mib"

        for cadence in CADENCES:
            fixture.mutate(
                (29, scenario, "avail", cadence),
                manifest=lambda manifest: manifest["expected_flows"].__setitem__(
                    "target", manifest["expected_flows"]["target"] + 17
                ),
                result=lambda result: (
                    result.__setitem__("target_flows", result["target_flows"] + 17),
                    result.__setitem__(
                        "target_completed", result["target_completed"] + 17
                    ),
                ),
            )
        rows = module.load_rows(fixture.root)
        assert not any(
            "mixed_case_comparability" in item["reason"]
            for item in rows.rejections
        ), rows.rejections
        accepted_cells = {
            (row["nodes"], row["seed"], row["scenario"],
             row["scheme"], row["cadence"])
            for row in rows
        }
        assert all(
            (128, seed, scenario, "avail", cadence) in accepted_cells
            for seed in SEEDS for cadence in CADENCES
        )
        accepted_output = Path(temp_dir) / "accepted-report"
        built = module.build_all(accepted_output, input_dir=fixture.root)
        assert set(built["selected"]) == set(SUBJECTS)

        fixture.mutate(
            (47, scenario, "avail", "fixed_rtt"),
            manifest=lambda manifest: manifest["expected_flows"].__setitem__(
                "background", manifest["expected_flows"]["background"] + 1
            ),
            result=lambda result: (
                result.__setitem__(
                    "background_flows", result["background_flows"] + 1
                ),
                result.__setitem__(
                    "background_completed", result["background_completed"] + 1
                ),
            ),
        )
        rows = module.load_rows(fixture.root)
        rejected = [
            item for item in rows.rejections
            if "mixed_case_comparability" in item["reason"]
        ]
        assert len(rejected) == 1, rejected
        assert rejected[0]["seed"] == 47
        assert rejected[0]["cadence"] == "fixed_rtt"
        assert all("expected_flow_counts" in item["reason"] for item in rejected)
        assert (128, 47, scenario, "avail", "fixed_rtt") not in {
            (row["nodes"], row["seed"], row["scenario"],
             row["scheme"], row["cadence"])
            for row in rows
        }
        expect_failure(
            lambda: module.build_all(
                Path(temp_dir) / "rejected-report", input_dir=fixture.root
            ),
            "recommendation blocked",
        )

    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")
        baseline_output = Path(temp_dir) / "baseline-report"
        module.build_all(baseline_output, input_dir=fixture.root)
        baseline = json.loads(
            (baseline_output / "selected-cadences.json").read_text(
                encoding="utf-8"
            )
        )["provenance"]["case_config_sha256"]
        for seed in SEEDS:
            for cadence in CADENCES:
                fixture.mutate(
                    (seed, scenario, "avail", cadence),
                    manifest=lambda manifest: manifest["expected_flows"].__setitem__(
                        "target", manifest["expected_flows"]["target"] + 1
                    ),
                    result=lambda result: (
                        result.__setitem__("target_flows", result["target_flows"] + 1),
                        result.__setitem__(
                            "target_completed", result["target_completed"] + 1
                        ),
                    ),
                )
        changed_output = Path(temp_dir) / "changed-report"
        module.build_all(changed_output, input_dir=fixture.root)
        changed = json.loads(
            (changed_output / "selected-cadences.json").read_text(
                encoding="utf-8"
            )
        )["provenance"]["case_config_sha256"]
        assert changed != baseline


def test_traffic_hash_is_same_seed_workload_identity_across_candidates():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(temp_dir)
        key = (29, "asymmetric_permutation_4mib", "grade", "fixed_rtt")
        fixture.mutate(
            key,
            manifest=lambda manifest: manifest.__setitem__(
                "traffic_sha256", "d" * 64
            ),
        )
        rows = module.load_rows(fixture.root)
        rejected = [
            item for item in rows.rejections
            if "mixed_workload_identity" in item["reason"]
        ]
        assert len(rejected) == 1, rows.rejections
        assert rejected[0]["seed"] == 29
        assert rejected[0]["scheme"] == "grade"
        assert rejected[0]["cadence"] == "fixed_rtt"
        assert "traffic_sha256" in rejected[0]["reason"]
        assert not any(
            row["nodes"] == 128
            and row["seed"] == 29
            and row["scenario"] == "asymmetric_permutation_4mib"
            and row["scheme"] == "grade"
            and row["cadence"] == "fixed_rtt"
            for row in rows
        )


def test_same_seed_geomean_and_feedback_tiebreak_recommendations():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")
        output = Path(temp_dir) / "report"
        built = module.build_all(output, input_dir=fixture.root)
        assert built["selected"] == {
            "avail": "bdp_triggered",
            "grade": "fixed5",
            "netaware": "fixed5",
        }
        selected = json.loads(
            (output / "selected-cadences.json").read_text(encoding="utf-8")
        )
        assert selected["schema_version"] == 1
        assert selected["selection_sha256"] == selection_fingerprint(selected)
        assert selected["selected_cadences"] == built["selected"]
        assert selected["cadence_flags"] == {
            scheme: cadence_args(scheme, cadence)
            for scheme, cadence in built["selected"].items()
        }
        assert selected["provenance"]["simulator_sha256"] == SIMULATOR_HASH
        assert selected["provenance"]["parser_version"] == 10
        assert len(selected["provenance"]["case_config_sha256"]) == 64
        assert len(selected["provenance"]["result_set_sha256"]) == 64
        decisions = read_csv(output / "cadence_decisions.csv")
        fixed_rtt = next(
            row for row in decisions
            if row["subject"] == "avail"
            and row["cadence"] == "fixed_rtt"
            and row["rule"] == "opportunity_geomean"
        )
        assert int(fixed_rtt["paired_cases"]) == 24
        assert math.isclose(float(fixed_rtt["value"]), 0.90)
        assert fixed_rtt["baseline"] == "fixed5 same subject/nodes/scenario/seed"
        assert "ordinary=p99_fct_us" in fixed_rtt["evidence"]
        assert "alltoall=all_to_all_cct_us" in fixed_rtt["evidence"]
        feedback_choice = next(
            row for row in decisions
            if row["subject"] == "avail"
            and row["cadence"] == "bdp_triggered"
            and row["rule"] == "feedback_tiebreak"
        )
        assert feedback_choice["status"] == "pass"
        assert read_csv(output / "data_quality_rejections.csv") == []
        assert not list(output.glob("*.tmp"))


def test_result_set_hash_covers_measurements_but_excludes_paths():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")
        key = (13, "healthy_permutation_4mib", "avail", "fixed5")

        def result_hash(output):
            module.build_all(output, input_dir=fixture.root)
            document = json.loads(
                (output / "selected-cadences.json").read_text(encoding="utf-8")
            )
            return document["provenance"]["result_set_sha256"]

        baseline = result_hash(Path(temp_dir) / "baseline")
        original_p95 = fixture.cases[key]["result"]["p95_fct_us"]
        fixture.mutate(
            key,
            result=lambda row: row.__setitem__("p95_fct_us", original_p95 + 1),
        )
        changed_metric = result_hash(Path(temp_dir) / "changed-metric")
        assert changed_metric != baseline

        fixture.mutate(
            key,
            result=lambda row: (
                row.__setitem__("p95_fct_us", original_p95),
                row.__setitem__("stdout", "/different/nonsemantic/path/stdout.log"),
            ),
        )
        changed_path = result_hash(Path(temp_dir) / "changed-path")
        assert changed_path == baseline


def test_stable_and_cascade_require_cross_seed_majority_and_cost_thresholds():
    module = load_report()
    assert module.MIN_GUARDRAIL_PAIRED_SEEDS == 3
    assert module.STABLE_P99_RATIO_THRESHOLD == 1.03
    assert module.RECOVERY_BDP_PACKETS == 86
    assert module.RECOVERY_RETX_RATIO_THRESHOLD == 0.001
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")
        healthy = "healthy_permutation_4mib"

        def set_p99(subject, cadence, seed, ratio):
            baseline = fixture.cases[(seed, healthy, subject, "fixed5")][
                "result"
            ]["p99_fct_us"]
            fixture.mutate(
                (seed, healthy, subject, cadence),
                result=lambda row: row.__setitem__(
                    "p99_fct_us", baseline * ratio
                ),
            )

        set_p99("avail", "fixed_rtt", 13, 1.50)
        set_p99("avail", "bdp_triggered", 13, 1.04)
        set_p99("avail", "bdp_triggered", 29, 1.04)
        set_p99("netaware", "fixed_rtt", 13, 1.03)
        set_p99("netaware", "fixed_rtt", 29, 1.04)
        set_p99("netaware", "fixed_rtt", 47, 1.03)

        def set_recovery(subject, cadence, seed, nacks, retx, retx_ratio):
            fixture.mutate(
                (seed, healthy, subject, cadence),
                result=lambda row: (
                    row.__setitem__("nacks", nacks),
                    row.__setitem__("nacks_ooo", 0),
                    row.__setitem__("nacks_trim", 0),
                    row.__setitem__("nacks_loss", nacks),
                    row.__setitem__("feedback_nacks", 0),
                    row.__setitem__(
                        "feedback_messages_total", row["feedback_acks"]
                    ),
                    row.__setitem__("retx_packets", retx),
                    row.__setitem__("retx_ratio", retx_ratio),
                ),
            )

        set_recovery("netaware", "bdp_triggered", 47, 100, 100, 1.0)
        for seed, nacks, retx in ((13, 43, 53), (29, 53, 43)):
            set_recovery("grade", "fixed_rtt", seed, nacks, retx, 0.0005)
            fixture.mutate(
                (seed, healthy, "grade", "fixed5"),
                result=lambda row: row.__setitem__("retx_ratio", 0.0005),
            )
        set_recovery("grade", "bdp_triggered", 13, 0, 11, 0.001)
        set_recovery("grade", "bdp_triggered", 29, 10, 1, 0.001)

        output = Path(temp_dir) / "report"
        module.build_all(output, input_dir=fixture.root)
        decisions = read_csv(output / "cadence_decisions.csv")

        def decision(subject, cadence, rule):
            return next(
                row for row in decisions
                if row["subject"] == subject and row["cadence"] == cadence
                and row["rule"] == rule
            )

        single_stable = decision(
            "avail", "fixed_rtt", "healthy_p99_guardrail"
        )
        assert single_stable["status"] == "pass"
        assert "warning" in single_stable["evidence"]
        stable = decision("avail", "bdp_triggered", "healthy_p99_guardrail")
        assert stable["status"] == "fail"
        assert "median_ratio=1.04" in stable["evidence"]
        assert "regressed_seeds=2/3" in stable["evidence"]
        exact_boundary = decision(
            "netaware", "fixed_rtt", "healthy_p99_guardrail"
        )
        assert exact_boundary["status"] == "pass"
        assert "median_ratio=1.03" in exact_boundary["evidence"]

        single_cascade = decision(
            "netaware", "bdp_triggered", "healthy_nack_retx_guardrail"
        )
        assert single_cascade["status"] == "pass"
        assert "warning" in single_cascade["evidence"]
        bdp_cascade = decision(
            "grade", "fixed_rtt", "healthy_nack_retx_guardrail"
        )
        assert bdp_cascade["status"] == "fail"
        assert "median_recovery_packets=86" in bdp_cascade["evidence"]
        assert "BDP=86 packets" in bdp_cascade["evidence"]
        ratio_cascade = decision(
            "grade", "bdp_triggered", "healthy_nack_retx_guardrail"
        )
        assert ratio_cascade["status"] == "fail"
        assert "median_retx_ratio=0.001" in ratio_cascade["evidence"]
        assert "retx_ratio_threshold=0.001" in ratio_cascade["evidence"]


def test_extreme_same_seed_ratio_cannot_underflow_into_best_score():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")
        scenario = "asymmetric_permutation_4mib"
        fixture.mutate(
            (13, scenario, "avail", "fixed5"),
            result=lambda row: row.__setitem__("p99_fct_us", 1e300),
        )
        fixture.mutate(
            (13, scenario, "avail", "bdp_triggered"),
            result=lambda row: row.__setitem__("p99_fct_us", 1e-300),
        )
        output = Path(temp_dir) / "report"
        built = module.build_all(output, input_dir=fixture.root)
        assert built["selected"]["avail"] != "bdp_triggered"
        decisions = read_csv(output / "cadence_decisions.csv")
        pairing = next(
            row for row in decisions
            if row["subject"] == "avail"
            and row["cadence"] == "bdp_triggered"
            and row["rule"] == "same_seed_pairing"
        )
        assert pairing["status"] == "fail"
        assert "derived same-seed ratio" in pairing["evidence"]
        geomean = next(
            row for row in decisions
            if row["subject"] == "avail"
            and row["cadence"] == "bdp_triggered"
            and row["rule"] == "opportunity_geomean"
        )
        assert geomean["status"] == "fail"
        assert geomean["value"] == ""


def test_opportunity_direction_uses_three_seed_neutral_band_per_scenario():
    module = load_report()
    assert module.OPPORTUNITY_IMPROVEMENT_THRESHOLD == 0.97
    assert module.OPPORTUNITY_REGRESSION_THRESHOLD == 1.03
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")

        def set_ratios(subject, cadence, scenario, ratios):
            for seed, ratio in zip(SEEDS, ratios):
                baseline = fixture.cases[(seed, scenario, subject, "fixed5")][
                    "result"
                ]["p99_fct_us"]
                fixture.mutate(
                    (seed, scenario, subject, cadence),
                    result=lambda row, value=baseline * ratio: row.__setitem__(
                        "p99_fct_us", value
                    ),
                )

        regressed_scenario = "asymmetric_permutation_4mib"
        neutral_scenario = "periodic_hotspot_target_4mib"
        improved_scenario = "mixed_fixed_background_target_4mib"
        set_ratios(
            "avail", "bdp_triggered", regressed_scenario, (2.0, 2.0, 0.01)
        )
        set_ratios(
            "avail", "fixed_rtt", neutral_scenario, (1.02, 1.00, 0.99)
        )
        set_ratios(
            "grade", "bdp_triggered", improved_scenario, (0.96, 0.96, 0.96)
        )
        output = Path(temp_dir) / "report"
        built = module.build_all(output, input_dir=fixture.root)
        assert built["selected"]["avail"] != "bdp_triggered"
        decisions = read_csv(output / "cadence_decisions.csv")

        def direction(subject, cadence, scenario):
            return next(
                row for row in decisions
                if row["subject"] == subject and row["cadence"] == cadence
                and row["scenario"] == scenario
                and row["rule"] == "opportunity_direction_guardrail"
            )

        regressed = direction("avail", "bdp_triggered", regressed_scenario)
        assert regressed["status"] == "fail"
        assert "classification=regressed" in regressed["evidence"]
        assert "improved=1 neutral=0 regressed=2" in regressed["evidence"]
        neutral = direction("avail", "fixed_rtt", neutral_scenario)
        assert neutral["status"] == "pass"
        assert "classification=neutral" in neutral["evidence"]
        assert "improved=0 neutral=3 regressed=0" in neutral["evidence"]
        improved = direction("grade", "bdp_triggered", improved_scenario)
        assert improved["status"] == "pass"
        assert "classification=improved" in improved["evidence"]
        assert "improved=3 neutral=0 regressed=0" in improved["evidence"]


def test_opportunity_recovery_guardrail_is_per_scenario_and_cross_seed():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")
        rto_scenario = "periodic_hotspot_target_4mib"
        recovery_scenario = "asymmetric_permutation_4mib"
        warning_scenario = "mixed_fixed_background_target_4mib"

        for seed, nacks, retx in ((13, 43, 53), (29, 53, 43)):
            fixture.mutate(
                (seed, rto_scenario, "avail", "bdp_triggered"),
                result=lambda row: row.__setitem__("rtos", 1),
            )
            fixture.mutate(
                (seed, recovery_scenario, "grade", "fixed_rtt"),
                result=lambda row: (
                    row.__setitem__("nacks", nacks),
                    row.__setitem__("nacks_ooo", 0),
                    row.__setitem__("nacks_trim", 0),
                    row.__setitem__("nacks_loss", nacks),
                    row.__setitem__("feedback_nacks", 0),
                    row.__setitem__(
                        "feedback_messages_total", row["feedback_acks"]
                    ),
                    row.__setitem__("retx_packets", retx),
                ),
            )
        fixture.mutate(
            (47, warning_scenario, "netaware", "bdp_triggered"),
            result=lambda row: (
                row.__setitem__("rtos", 100),
                row.__setitem__("nacks", 100),
                row.__setitem__("nacks_ooo", 0),
                row.__setitem__("nacks_trim", 0),
                row.__setitem__("nacks_loss", 100),
                row.__setitem__("feedback_nacks", 0),
                row.__setitem__(
                    "feedback_messages_total", row["feedback_acks"]
                ),
                row.__setitem__("retx_packets", 100),
                row.__setitem__("retx_ratio", 1.0),
            ),
        )
        output = Path(temp_dir) / "report"
        built = module.build_all(output, input_dir=fixture.root)
        assert built["selected"]["avail"] != "bdp_triggered"
        assert built["selected"]["grade"] != "fixed_rtt"
        decisions = read_csv(output / "cadence_decisions.csv")

        def recovery(subject, cadence, scenario):
            return next(
                row for row in decisions
                if row["subject"] == subject and row["cadence"] == cadence
                and row["scenario"] == scenario
                and row["rule"] == "opportunity_recovery_guardrail"
            )

        rto = recovery("avail", "bdp_triggered", rto_scenario)
        assert rto["status"] == "fail"
        assert "rto_increase_seeds=2/3" in rto["evidence"]
        assert "median_rto_delta=1" in rto["evidence"]
        packet_recovery = recovery(
            "grade", "fixed_rtt", recovery_scenario
        )
        assert packet_recovery["status"] == "fail"
        assert "new_recovery_seeds=2/3" in packet_recovery["evidence"]
        assert "median_recovery_packets=86" in packet_recovery["evidence"]
        warning = recovery("netaware", "bdp_triggered", warning_scenario)
        assert warning["status"] == "pass"
        assert "warning" in warning["evidence"]
        assert "new_recovery_seeds=1/3" in warning["evidence"]


def test_incast_has_distinct_cross_seed_guardrails_and_no_geomean_weight():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")
        scenario = "incast_8to1_4mib"
        for seed, ratio in zip(SEEDS, (1.04, 1.04, 1.0)):
            baseline = fixture.cases[(seed, scenario, "avail", "fixed5")][
                "result"
            ]["p99_fct_us"]
            fixture.mutate(
                (seed, scenario, "avail", "bdp_triggered"),
                result=lambda row, value=baseline * ratio: row.__setitem__(
                    "p99_fct_us", value
                ),
            )
        for seed, nacks, retx in ((13, 43, 53), (29, 53, 43)):
            fixture.mutate(
                (seed, scenario, "grade", "fixed_rtt"),
                result=lambda row: (
                    row.__setitem__("nacks", nacks),
                    row.__setitem__("nacks_ooo", 0),
                    row.__setitem__("nacks_trim", 0),
                    row.__setitem__("nacks_loss", nacks),
                    row.__setitem__("feedback_nacks", 0),
                    row.__setitem__(
                        "feedback_messages_total", row["feedback_acks"]
                    ),
                    row.__setitem__("retx_packets", retx),
                ),
            )
            fixture.mutate(
                (seed, scenario, "netaware", "bdp_triggered"),
                result=lambda row: row.__setitem__("rtos", 1),
            )
            fixture.mutate(
                (seed, "healthy_permutation_4mib", "avail", "fixed_rtt"),
                result=lambda row: row.__setitem__("rtos", 1),
            )
        output = Path(temp_dir) / "report"
        built = module.build_all(output, input_dir=fixture.root)
        assert built["selected"]["avail"] != "bdp_triggered"
        assert built["selected"]["avail"] != "fixed_rtt"
        assert built["selected"]["grade"] != "fixed_rtt"
        assert built["selected"]["netaware"] != "bdp_triggered"
        decisions = read_csv(output / "cadence_decisions.csv")

        def incast(subject, cadence):
            return next(
                row for row in decisions
                if row["subject"] == subject and row["cadence"] == cadence
                and row["scenario"] == scenario
                and row["rule"] == "incast_guardrail"
            )

        assert incast("avail", "bdp_triggered")["status"] == "fail"
        assert "stable_p99=true" in incast(
            "avail", "bdp_triggered"
        )["evidence"]
        assert incast("grade", "fixed_rtt")["status"] == "fail"
        assert "recovery_cascade=true" in incast(
            "grade", "fixed_rtt"
        )["evidence"]
        assert incast("netaware", "bdp_triggered")["status"] == "fail"
        assert "stable_rto=true" in incast(
            "netaware", "bdp_triggered"
        )["evidence"]
        healthy_rto = next(
            row for row in decisions
            if row["subject"] == "avail" and row["cadence"] == "fixed_rtt"
            and row["rule"] == "healthy_rto_guardrail"
        )
        assert healthy_rto["status"] == "fail"
        assert "rto_increase_seeds=2/3" in healthy_rto["evidence"]
        geomean_rows = [
            row for row in decisions
            if row["rule"] == "opportunity_direction_guardrail"
            and row["scenario"] == scenario
        ]
        assert geomean_rows == []


def test_zero_incast_reference_is_explicit_evidence_invalid_for_subject():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")
        scenario = "incast_8to1_4mib"
        for seed in SEEDS:
            fixture.mutate(
                (seed, scenario, "avail", "fixed5"),
                result=lambda row: row.__setitem__("p99_fct_us", 0.0),
            )
        rows = module.load_rows(fixture.root)
        selected, decisions, unused_blockers = module._recommend(
            rows, rows.rejections, rows.expected_cases
        )
        assert "avail" not in selected
        invalid = [
            row for row in decisions
            if row["subject"] == "avail" and row["rule"] == "evidence_invalid"
        ]
        assert {row["cadence"] for row in invalid} == set(CADENCES)
        assert all(row["status"] == "fail" for row in invalid)
        assert all("finite positive" in row["evidence"] for row in invalid)
        assert any(
            "evidence_invalid" in row["reason"]
            and row["scenario"] == scenario
            for row in rows.rejections
        )
        output = Path(temp_dir) / "report"
        expect_failure(
            lambda: module.build_all(output, input_dir=fixture.root),
            "recommendation blocked",
        )
        assert not (output / "selected-cadences.json").exists()


def test_decisions_record_every_candidate_stage_and_only_real_lower_tail_ties():
    module = load_report()
    required_stages = {
        "guardrail", "geomean_eligibility", "three_percent_cutoff",
        "feedback_bytes_tiebreak", "final_tiebreak", "final_recommendation",
    }
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")
        output = Path(temp_dir) / "report"
        module.build_all(output, input_dir=fixture.root)
        decisions = read_csv(output / "cadence_decisions.csv")
        for subject in SUBJECTS:
            for cadence in CADENCES:
                stages = {
                    row["rule"] for row in decisions
                    if row["subject"] == subject and row["cadence"] == cadence
                }
                assert required_stages <= stages, (subject, cadence, stages)
        assert not any(
            row["rule"] == "avail_lower_tail_tiebreak" for row in decisions
        )
        cutoff = {
            row["cadence"]: row["status"] for row in decisions
            if row["subject"] == "avail"
            and row["rule"] == "three_percent_cutoff"
        }
        assert cutoff == {
            "fixed5": "fail", "fixed_rtt": "pass", "bdp_triggered": "pass",
        }

        for nodes in FIXTURE_TOPOLOGIES:
            for seed in SEEDS:
                for scenario, unused_opportunity, unused_alltoall in SCENARIOS:
                    key = (seed, scenario, "avail", "bdp_triggered")
                    if nodes != 128:
                        key = (nodes, *key)
                    fixture.mutate(
                        key,
                        result=lambda row: (
                            row.__setitem__("feedback_acks", 900),
                            row.__setitem__(
                                "feedback_messages_total",
                                900 + row["feedback_nacks"],
                            ),
                        ),
                    )
        tied_output = Path(temp_dir) / "tied-report"
        built = module.build_all(tied_output, input_dir=fixture.root)
        tied = read_csv(tied_output / "cadence_decisions.csv")
        lower_tail = [
            row for row in tied
            if row["subject"] == "avail"
            and row["rule"] == "avail_lower_tail_tiebreak"
        ]
        assert {row["cadence"] for row in lower_tail} == {
            "fixed_rtt", "bdp_triggered"
        }
        assert {
            row["cadence"]: row["status"] for row in lower_tail
        } == {"fixed_rtt": "pass", "bdp_triggered": "fail"}
        final_rows = [
            row for row in tied
            if row["subject"] == "avail"
            and row["rule"] == "final_recommendation"
        ]
        assert len(final_rows) == 3
        assert [
            row["cadence"] for row in final_rows if row["status"] == "pass"
        ] == ["fixed_rtt"]
        assert built["selected"]["avail"] == "fixed_rtt"


def test_missing_pair_zero_denominator_and_mixed_case_config_block_selection():
    module = load_report()
    for mutation, expected in (
        ("missing", "missing same-seed baseline"),
        ("all_missing_seed", "missing required evidence"),
        ("missing_subject", "no opportunity cases"),
        ("zero", "zero denominator"),
        ("config", "mixed_case_config"),
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = write_full_fixture(Path(temp_dir) / "input")
            key = (13, "asymmetric_permutation_4mib", "avail", "fixed5")
            if mutation == "missing":
                fixture.rows.remove(fixture.cases[key]["result"])
                fixture.rewrite()
            elif mutation == "all_missing_seed":
                fixture.rows = [
                    row for row in fixture.rows if row["seed"] != 47
                ]
                fixture.rewrite()
            elif mutation == "missing_subject":
                fixture.rows = [
                    row for row in fixture.rows if row["scheme"] != "grade"
                ]
                fixture.rewrite()
            elif mutation == "zero":
                fixture.mutate(
                    key, result=lambda r: r.__setitem__("p99_fct_us", 0.0)
                )
            else:
                fixture.mutate(
                    key,
                    manifest=lambda m: m["scenario"].__setitem__(
                        "size_bytes", 8 << 20
                    ),
                )
            output = Path(temp_dir) / "report"
            expect_failure(
                lambda: module.build_all(output, input_dir=fixture.root), expected
            )
            assert not (output / "selected-cadences.json").exists()
            assert not (output / "cadence_decisions.csv").exists()
            assert not (output / "summary.csv").exists()


def test_required_evidence_registry_blocks_any_missing_class_seed():
    module = load_report()
    assert module.REQUIRED_EVIDENCE_CLASSES == (
        "healthy_guardrail", "incast_guardrail", "asymmetric", "mixed",
        "periodic_background", "alltoall",
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")
        missing = fixture.cases[
            (29, "mixed_fixed_background_target_4mib", "avail", "bdp_triggered")
        ]["result"]
        fixture.rows.remove(missing)
        fixture.rewrite()
        output = Path(temp_dir) / "report"
        expect_failure(
            lambda: module.build_all(output, input_dir=fixture.root),
            "missing required evidence",
        )
        assert not (output / "selected-cadences.json").exists()
        rejections = read_csv(output / "data_quality_rejections.csv")
        missing_rows = [
            row for row in rejections
            if "missing_required_evidence" in row["reason"]
        ]
        assert len(missing_rows) == 1
        assert missing_rows[0]["scheme"] == "avail"
        assert missing_rows[0]["cadence"] == "bdp_triggered"
        assert missing_rows[0]["seed"] == "29"
        assert "mixed" in missing_rows[0]["reason"]


def test_expected_ledger_detects_deleted_scenario_and_entire_node():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "missing-scenario")
        fixture.rows = [
            row for row in fixture.rows
            if row["scenario"] != "asymmetric_permutation_4mib"
        ]
        fixture.rewrite()
        for stage in ("pilot-time", "pilot-bdp"):
            ledger_path = fixture.root / "raw" / stage / "expected_cases.jsonl"
            ledger = [
                json.loads(line) for line in ledger_path.read_text().splitlines()
            ]
            ledger_path.write_text("".join(
                json.dumps(row, sort_keys=True) + "\n" for row in ledger
                if row["case_key"]["scenario"] != "asymmetric_permutation_4mib"
            ), encoding="utf-8")
            intent_path = fixture.root / "raw" / stage / "run_intent.json"
            intent = json.loads(intent_path.read_text(encoding="utf-8"))
            intent["expected_case_keys"] = [
                key for key in intent["expected_case_keys"]
                if key["scenario"] != "asymmetric_permutation_4mib"
            ]
            intent_path.write_text(
                json.dumps(intent, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
        output = Path(temp_dir) / "scenario-report"
        expect_failure(
            lambda: module.build_all(output, input_dir=fixture.root),
            "recommendation blocked",
        )
        rejections = read_csv(output / "data_quality_rejections.csv")
        assert any(
            "missing_run_intent_case" in row["reason"]
            and row["scenario"] == "asymmetric_permutation_4mib"
            for row in rejections
        )
        assert not (output / "selected-cadences.json").exists()

    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "missing-node")
        for intent_path in fixture.root.glob("raw/*/run_intent.json"):
            intent = json.loads(intent_path.read_text(encoding="utf-8"))
            intent["requested_nodes"] = [128, 512, 2048]
            intent_path.write_text(
                json.dumps(intent, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
        output = Path(temp_dir) / "node-report"
        expect_failure(
            lambda: module.build_all(output, input_dir=fixture.root),
            "recommendation blocked",
        )
        rejections = read_csv(output / "data_quality_rejections.csv")
        assert any(
            "missing_run_intent_case" in row["reason"]
            and row["nodes"] == "2048"
            for row in rejections
        )
        assert not (output / "selected-cadences.json").exists()


def test_root_run_index_detects_deleted_required_stage_directories():
    module = load_report()
    for stage in ("pilot-time", "pilot-bdp"):
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = write_full_fixture(Path(temp_dir) / stage)
            shutil.rmtree(fixture.root / "raw" / stage)
            output = Path(temp_dir) / "report"
            expect_failure(
                lambda: module.build_all(output, input_dir=fixture.root),
                "recommendation blocked",
            )
            rejections = read_csv(output / "data_quality_rejections.csv")
            assert any(
                "run_index_missing_required_stage" in row["reason"]
                and row["scenario"] == stage
                for row in rejections
            )
            assert not (output / "selected-cadences.json").exists()


def test_root_run_index_validates_stage_hash_count_and_identity():
    module = load_report()
    mutations = (
        (
            "run_index_ledger_hash_mismatch",
            lambda entry: entry.__setitem__("ledger_sha256", "d" * 64),
        ),
        (
            "run_index_case_count_mismatch",
            lambda entry: entry.__setitem__(
                "case_count", entry["case_count"] + 1
            ),
        ),
        (
            "run_index_nodes_intent_mismatch",
            lambda entry: entry.__setitem__("nodes", [512]),
        ),
    )
    for expected, mutate in mutations:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = write_full_fixture(Path(temp_dir) / expected)
            index_path = fixture.root / "run_index.json"
            index = json.loads(index_path.read_text(encoding="utf-8"))
            entry = index["stages"]["pilot-time"]
            mutate(entry)
            invocation = next(
                item for item in index["invocations"]
                if item["invocation_id"] == entry["invocation_id"]
            )
            invocation["stages"]["pilot-time"] = copy.deepcopy(entry)
            if expected == "run_index_nodes_intent_mismatch":
                invocation["requested_nodes"] = list(entry["nodes"])
            invocation.update(seal_invocation(invocation))
            index_path.write_text(
                json.dumps(index, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            output = Path(temp_dir) / "report"
            expect_failure(
                lambda: module.build_all(output, input_dir=fixture.root),
                "recommendation blocked",
            )
            assert any(
                expected in row["reason"]
                for row in read_csv(output / "data_quality_rejections.csv")
            )


def test_root_run_index_binds_invocation_metadata_to_stage_union():
    module = load_report()

    def invalid_stage(invocation):
        invocation["requested_stage"] = "invalid-stage"

    def extra_node(invocation):
        invocation["requested_nodes"] = [2048]

    def extra_seed(invocation):
        invocation["requested_seeds"] = [999]

    def invalid_node_type(invocation):
        invocation["requested_nodes"] = ["128"]

    def unknown_scheme(invocation):
        invocation["requested_schemes"] = ["unknown-scheme"]

    def duplicate_scheme(invocation):
        invocation["requested_schemes"].append(
            invocation["requested_schemes"][0]
        )

    def unknown_cadence(invocation):
        invocation["requested_cadences"] = ["unknown-cadence"]

    def noncanonical_cadence_order(invocation):
        invocation["requested_cadences"].reverse()

    def missing_field(invocation):
        del invocation["requested_seeds"]

    probes = (
        ("invalid requested_stage", invalid_stage),
        ("requested_nodes do not match invocation stage union", extra_node),
        ("requested_seeds do not match invocation stage union", extra_seed),
        ("invalid requested_nodes", invalid_node_type),
        ("invalid requested_schemes", unknown_scheme),
        ("invalid requested_schemes", duplicate_scheme),
        ("invalid requested_cadences", unknown_cadence),
        ("invalid requested_cadences", noncanonical_cadence_order),
        ("invalid invocation fields", missing_field),
    )
    for expected, mutate in probes:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = write_full_fixture(Path(temp_dir) / "input")
            index_path = fixture.root / "run_index.json"
            index = json.loads(index_path.read_text(encoding="utf-8"))
            invocation = index["invocations"][0]
            mutate(invocation)
            invocation.update(seal_invocation(invocation))
            index_path.write_text(
                json.dumps(index, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            output = Path(temp_dir) / "report"
            expect_failure(
                lambda: module.build_all(output, input_dir=fixture.root),
                "no accepted evaluation rows",
            )
            rejections = read_csv(output / "data_quality_rejections.csv")
            assert any(expected in row["reason"] for row in rejections), (
                expected, rejections
            )
            assert not (output / "selected-cadences.json").exists()


def test_root_run_index_rejects_invocation_digest_tampering():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")
        index_path = fixture.root / "run_index.json"
        index = json.loads(index_path.read_text(encoding="utf-8"))
        index["invocations"][0]["invocation_sha256"] = "d" * 64
        index_path.write_text(
            json.dumps(index, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        output = Path(temp_dir) / "report"
        expect_failure(
            lambda: module.build_all(output, input_dir=fixture.root),
            "no accepted evaluation rows",
        )
        assert any(
            "invocation digest mismatch" in row["reason"]
            for row in read_csv(output / "data_quality_rejections.csv")
        )


def test_root_run_index_binds_scenario_patterns_bidirectionally():
    module = load_report()
    for mode in ("unmatched_scenarios", "dead_pattern"):
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = write_full_fixture(Path(temp_dir) / "input")
            index_path = fixture.root / "run_index.json"
            index = json.loads(index_path.read_text(encoding="utf-8"))
            invocation = index["invocations"][0]
            stage = invocation["expanded_required_stages"][0]
            scenarios = invocation["stages"][stage]["scenarios"]
            patterns = (
                ["does-not-match"]
                if mode == "unmatched_scenarios" else
                list(scenarios) + ["dead-pattern"]
            )
            intent_path = fixture.root / f"raw/{stage}/run_intent.json"
            intent = json.loads(intent_path.read_text(encoding="utf-8"))
            intent["scenario_patterns"] = patterns
            intent_path.write_text(
                json.dumps(intent, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            entry = invocation["stages"][stage]
            entry["scenario_patterns"] = patterns
            entry["run_intent_sha256"] = hashlib.sha256(
                intent_path.read_bytes()
            ).hexdigest()
            index["stages"][stage] = copy.deepcopy(entry)
            invocation["scenario_patterns"] = patterns
            invocation.update(seal_invocation(invocation))
            index_path.write_text(
                json.dumps(index, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            output = Path(temp_dir) / "report"
            expect_failure(
                lambda: module.build_all(output, input_dir=fixture.root),
                "no accepted evaluation rows",
            )
            rejections = read_csv(output / "data_quality_rejections.csv")
            assert any(
                "scenario_patterns do not match staged scenarios"
                in row["reason"] for row in rejections
            ), (mode, rejections)
            assert not (output / "selected-cadences.json").exists()


def test_report_accepts_runner_normalized_scenario_patterns():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture, unused_key = write_single_fixture(Path(temp_dir) / "input")
        intent_path = fixture.root / "raw/pilot-time/run_intent.json"
        intent = json.loads(intent_path.read_text(encoding="utf-8"))
        intent["scenario_patterns"] = ["healthy"]
        intent_path.write_text(
            json.dumps(intent, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        write_fixture_run_index(fixture.root)
        rows = module.load_rows(fixture.root)
        assert len(rows) == 1
        assert rows.rejections == []


def test_run_index_path_boundaries_reject_nested_traversal_and_symlink_escape():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "nested")
        canonical = fixture.root / "raw/pilot-time"
        nested = fixture.root / "raw/archive/pilot-time"
        nested.mkdir(parents=True)
        for name in (
                "expected_cases.jsonl", "run_intent.json", "results.jsonl"):
            shutil.copyfile(canonical / name, nested / name)
        output = Path(temp_dir) / "nested-report"
        expect_failure(
            lambda: module.build_all(output, input_dir=fixture.root),
            "recommendation blocked",
        )
        assert any(
            "run_index_noncanonical_duplicate" in row["reason"]
            for row in read_csv(output / "data_quality_rejections.csv")
        )

    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "traversal")
        index_path = fixture.root / "run_index.json"
        index = json.loads(index_path.read_text(encoding="utf-8"))
        entry = index["stages"]["pilot-time"]
        entry["ledger_path"] = "../escape/expected_cases.jsonl"
        invocation = next(
            item for item in index["invocations"]
            if item["invocation_id"] == entry["invocation_id"]
        )
        invocation["stages"]["pilot-time"] = copy.deepcopy(entry)
        invocation.update(seal_invocation(invocation))
        index_path.write_text(
            json.dumps(index, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        output = Path(temp_dir) / "traversal-report"
        expect_failure(
            lambda: module.build_all(output, input_dir=fixture.root),
            "recommendation blocked",
        )
        assert any(
            "run_index_path_boundary" in row["reason"]
            for row in read_csv(output / "data_quality_rejections.csv")
        )

    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "symlink")
        ledger_path = fixture.root / "raw/pilot-time/expected_cases.jsonl"
        outside = Path(temp_dir) / "outside-ledger.jsonl"
        shutil.copyfile(ledger_path, outside)
        ledger_path.unlink()
        try:
            ledger_path.symlink_to(outside)
        except (NotImplementedError, OSError):
            return
        output = Path(temp_dir) / "symlink-report"
        expect_failure(
            lambda: module.build_all(output, input_dir=fixture.root),
            "recommendation blocked",
        )
        assert any(
            "run_index_path_boundary" in row["reason"]
            for row in read_csv(output / "data_quality_rejections.csv")
        )


def test_canonical_results_symlink_to_in_root_file_is_rejected():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")
        results_path = fixture.root / "raw/pilot-time/results.jsonl"
        undeclared = fixture.root / "raw/pilot-time-results-shadow.jsonl"
        shutil.copyfile(results_path, undeclared)
        results_path.unlink()
        try:
            results_path.symlink_to(undeclared)
        except (NotImplementedError, OSError):
            return
        output = Path(temp_dir) / "report"
        expect_failure(
            lambda: module.build_all(output, input_dir=fixture.root),
            "recommendation blocked",
        )
        assert any(
            "symlink_path_component" in row["reason"]
            and row["scenario"] == "pilot-time"
            for row in read_csv(output / "data_quality_rejections.csv")
        )


def test_external_run_index_symlink_is_rejected():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")
        index_path = fixture.root / "run_index.json"
        external = Path(temp_dir) / "external-run-index.json"
        shutil.copyfile(index_path, external)
        index_path.unlink()
        try:
            index_path.symlink_to(external)
        except (NotImplementedError, OSError):
            return
        output = Path(temp_dir) / "report"
        expect_failure(
            lambda: module.build_all(output, input_dir=fixture.root),
            "no accepted evaluation rows",
        )
        assert any(
            "symlink_path_component" in row["reason"]
            and row["scenario"] == "run-index"
            for row in read_csv(output / "data_quality_rejections.csv")
        )


def test_manifest_symlink_is_rejected():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")
        case = next(iter(fixture.cases.values()))
        manifest_path = case["manifest_path"]
        shadow = fixture.root / "manifest-shadow.json"
        shutil.copyfile(manifest_path, shadow)
        manifest_path.unlink()
        try:
            manifest_path.symlink_to(shadow)
        except (NotImplementedError, OSError):
            return
        output = Path(temp_dir) / "report"
        expect_failure(
            lambda: module.build_all(output, input_dir=fixture.root),
            "recommendation blocked",
        )
        assert any(
            "symlink_path_component" in row["reason"]
            and int(row["row"]) > 0
            for row in read_csv(output / "data_quality_rejections.csv")
        )


def test_expected_ledger_rejects_identity_mismatch_and_unexpected_result():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "mismatch")
        ledger_path = fixture.root / "raw/pilot-time/expected_cases.jsonl"
        ledger = [
            json.loads(line) for line in ledger_path.read_text().splitlines()
        ]
        ledger[0]["fingerprint"] = "d" * 64
        ledger_path.write_text("".join(
            json.dumps(row, sort_keys=True) + "\n" for row in ledger
        ), encoding="utf-8")
        rows = module.load_rows(fixture.root)
        assert any(
            "expected_result_mismatch:fingerprint" in row["reason"]
            for row in rows.rejections
        )
        assert any(
            "run_index_ledger_hash_mismatch" in row["reason"]
            for row in rows.rejections
        )
        assert len(rows) == len(fixture.rows) - 1

    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "unexpected")
        ledger_path = fixture.root / "raw/pilot-time/expected_cases.jsonl"
        lines = ledger_path.read_text(encoding="utf-8").splitlines()
        ledger_path.write_text(
            "\n".join(lines[1:]) + "\n", encoding="utf-8"
        )
        rows = module.load_rows(fixture.root)
        assert any(
            "result_set_undeclared_case" in row["reason"]
            for row in rows.rejections
        )
        assert any(
            "missing_expected_ledger_case" in row["reason"]
            for row in rows.rejections
        )

    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "malformed")
        ledger_path = fixture.root / "raw/pilot-time/expected_cases.jsonl"
        with ledger_path.open("a", encoding="utf-8") as handle:
            handle.write("{}\n")
        output = Path(temp_dir) / "report"
        expect_failure(
            lambda: module.build_all(
                output, input_dir=fixture.root, nodes=[128]
            ),
            "recommendation blocked",
        )
        assert any(
            "invalid_expected_ledger" in row["reason"]
            for row in read_csv(output / "data_quality_rejections.csv")
        )


def test_cli_nodes_filter_is_deterministic_and_does_not_edit_defaults():
    with tempfile.TemporaryDirectory() as temp_dir:
        fixture = write_full_fixture(Path(temp_dir) / "input")
        output = Path(temp_dir) / "report"
        watched = ROOT / "sim/datacenter/main_roce.cpp"
        before = hashlib.sha256(watched.read_bytes()).hexdigest()
        command = [
            sys.executable, str(SCRIPT), "--input", str(fixture.root),
            "--output", str(output), "--nodes", "128",
        ]
        first = subprocess.run(
            command, cwd=ROOT, text=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, check=False,
        )
        assert first.returncode == 0, first.stdout
        artifacts = {
            path.name: path.read_bytes() for path in output.iterdir()
            if path.is_file()
        }
        second = subprocess.run(
            command, cwd=ROOT, text=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, check=False,
        )
        assert second.returncode == 0, second.stdout
        assert artifacts == {
            path.name: path.read_bytes() for path in output.iterdir()
            if path.is_file()
        }
        assert hashlib.sha256(watched.read_bytes()).hexdigest() == before
        expected = {
            "summary.csv", "data_quality_rejections.csv",
            "cadence_decisions.csv", "selected-cadences.json", "report.md",
            *DERIVED_ARTIFACTS,
            *(f"{stem}.{suffix}" for stem in FIGURE_STEMS
              for suffix in ("png", "pdf")),
        }
        assert set(artifacts) == expected


def test_empty_input_is_explicitly_rejected():
    module = load_report()
    with tempfile.TemporaryDirectory() as temp_dir:
        expect_failure(lambda: module.load_rows(temp_dir), "results.jsonl")


def test_failed_rebuild_removes_all_stale_success_artifacts():
    module = load_report()
    success_artifacts = (
        "summary.csv", "cadence_decisions.csv", "selected-cadences.json",
        "report.md", *DERIVED_ARTIFACTS,
        *(f"{stem}.{suffix}" for stem in FIGURE_STEMS
          for suffix in ("png", "pdf")),
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        input_dir = Path(temp_dir) / "input"
        output = Path(temp_dir) / "report"
        fixture = write_full_fixture(input_dir)
        output.mkdir()
        user_artifact = output / "user-owned-plot.png"
        user_artifact.write_bytes(b"user-owned")
        module.build_all(output, input_dir=input_dir)
        assert all((output / name).is_file() for name in success_artifacts)
        assert user_artifact.read_bytes() == b"user-owned"

        fixture.rows = []
        fixture.rewrite()
        expect_failure(
            lambda: module.build_all(output, input_dir=input_dir),
            "no accepted evaluation rows",
        )
        assert all(not (output / name).exists() for name in success_artifacts)
        assert user_artifact.read_bytes() == b"user-owned"

        fixture = write_full_fixture(input_dir)
        module.build_all(output, input_dir=input_dir)
        fixture.rows = [row for row in fixture.rows if row["seed"] != 47]
        fixture.rewrite()
        expect_failure(
            lambda: module.build_all(output, input_dir=input_dir),
            "recommendation blocked",
        )
        assert all(not (output / name).exists() for name in success_artifacts)
        assert user_artifact.read_bytes() == b"user-owned"


def main():
    test_load_rows_and_aggregate_complete_fixture()
    test_simple_selected_cadence_does_not_expand_recommendation_grid()
    test_publication_ready_visualizations_and_report()
    test_pareto_aggregates_seed_cell_bandwidth_before_median()
    test_feedback_overhead_requires_and_uses_total_feedback_messages()
    test_normalized_rows_choose_one_scenario_metric_and_label_it()
    test_recovery_cost_is_summed_per_run_before_median_and_minmax()
    test_topology_scaling_excludes_alltoall_and_has_insufficient_state()
    test_pareto_facets_and_offsets_coincident_categories_deterministically()
    test_report_followups_are_derived_from_actual_topologies()
    test_simulation_duration_is_finite_positive_and_audited()
    test_fixture_cli_is_self_contained()
    test_derived_ecdf_excludes_non_cadence_baselines()
    test_recursive_stage_results_dedupe_identical_and_reject_conflicts()
    test_stage_result_set_rejects_conflicting_duplicate()
    test_stage_result_set_rejects_identical_duplicate()
    test_stage_result_set_rejects_malformed_json()
    test_stage_result_set_rejects_non_object_record()
    test_stage_result_set_rejects_undeclared_record()
    test_strict_row_validation_records_each_reason()
    test_ar_scheme_uses_runner_cli_lb_mapping_strictly()
    test_duplicate_seed_and_mixed_provenance_are_rejected()
    test_expected_flow_counts_may_vary_across_seeds_but_not_cadences()
    test_traffic_hash_is_same_seed_workload_identity_across_candidates()
    test_same_seed_geomean_and_feedback_tiebreak_recommendations()
    test_result_set_hash_covers_measurements_but_excludes_paths()
    test_stable_and_cascade_require_cross_seed_majority_and_cost_thresholds()
    test_extreme_same_seed_ratio_cannot_underflow_into_best_score()
    test_opportunity_direction_uses_three_seed_neutral_band_per_scenario()
    test_opportunity_recovery_guardrail_is_per_scenario_and_cross_seed()
    test_incast_has_distinct_cross_seed_guardrails_and_no_geomean_weight()
    test_zero_incast_reference_is_explicit_evidence_invalid_for_subject()
    test_decisions_record_every_candidate_stage_and_only_real_lower_tail_ties()
    test_missing_pair_zero_denominator_and_mixed_case_config_block_selection()
    test_required_evidence_registry_blocks_any_missing_class_seed()
    test_expected_ledger_detects_deleted_scenario_and_entire_node()
    test_root_run_index_detects_deleted_required_stage_directories()
    test_root_run_index_validates_stage_hash_count_and_identity()
    test_root_run_index_binds_invocation_metadata_to_stage_union()
    test_root_run_index_rejects_invocation_digest_tampering()
    test_root_run_index_binds_scenario_patterns_bidirectionally()
    test_report_accepts_runner_normalized_scenario_patterns()
    test_run_index_path_boundaries_reject_nested_traversal_and_symlink_escape()
    test_canonical_results_symlink_to_in_root_file_is_rejected()
    test_external_run_index_symlink_is_rejected()
    test_manifest_symlink_is_rejected()
    test_expected_ledger_rejects_identity_mismatch_and_unexpected_result()
    test_cli_nodes_filter_is_deterministic_and_does_not_edit_defaults()
    test_empty_input_is_explicitly_rejected()
    test_failed_rebuild_removes_all_stale_success_artifacts()


if __name__ == "__main__":
    main()
