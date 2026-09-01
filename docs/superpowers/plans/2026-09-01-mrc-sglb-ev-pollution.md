# MRC+SGLB EV Pollution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a real combined MRC+SGLB mode and produce event-level and 64-path evidence for or against the proposed EV-pollution mechanism.

**Architecture:** Keep endpoint selection and switch forwarding orthogonal: `mrc-sglb` uses the existing `LB_MRC` sender and default SGLB switch strategy. Add flow-scoped trace records at the sender and source ToR, then use one Python runner to construct identical four-scheme cells, validate the RTT/update premise, compute causal timing, and aggregate performance.

**Tech Stack:** C++ htsim simulator, Python 3 standard library/matplotlib, pytest, CSV/Markdown.

## Global Constraints

- All four schemes use default `dcqcn_variant`; no `cc=none` mechanism run.
- Preserve MRC's canonical 64 active EVs, identity mapping, and `SKIP_ONCE`.
- Use the existing 64-path two-tier topology for both controlled and scale runs.
- SGLB local update is 1 us and remote update is 15 us.
- Conclusions are conditional on measured feedback RTT being below 15 us.
- Do not alter existing `mrc`, `sglb`, or `rr` behavior.

---

### Task 1: Combined mode contract

**Files:**
- Modify: `sim/datacenter/main_roce.cpp`
- Test: `sim/tests/test_mrc_sglb_combined_mode.py`

**Interfaces:**
- Consumes: existing `RoceSrc::LB_MRC` and `FatTreeSwitch::SGLB`.
- Produces: CLI scheme `mrc-sglb` with MRC defaults and SGLB effective config.

- [ ] Add a failing CLI test that runs the smallest valid 64-path traffic and requires `lb=mrc-sglb`, `MrcEvModelDiag`, `MrcPolicyDiag`, and `SGLB effective config` in stdout.
- [ ] Run `python3 -m pytest -q sim/tests/test_mrc_sglb_combined_mode.py` and confirm failure because the scheme is unknown.
- [ ] Add the `mrc-sglb` parser branch, usage text, and reuse every MRC-default condition currently keyed by `LB_MRC`.
- [ ] Re-run the focused test and existing MRC/SGLB interface tests until green.

### Task 2: Event trace contract

**Files:**
- Modify: `sim/roce.h`
- Modify: `sim/roce.cpp`
- Modify: `sim/datacenter/fat_tree_switch.h`
- Modify: `sim/datacenter/fat_tree_switch.cpp`
- Modify: `sim/datacenter/main_roce.cpp`
- Test: `sim/tests/test_mrc_sglb_event_trace.py`

**Interfaces:**
- Produces line records `MrcSglbTrace event=<send|route|feedback|cc> time_us=... flow_id=... seq=... ev=... packet_path=... actual_path=... ecn=... cwnd=... profile_version=... profile_age_us=...` gated by the existing debug-flow selection.

- [ ] Add a failing parser/integration test requiring correlated send, source-ToR route, ECN feedback, state transition, and cwnd records for one debug flow.
- [ ] Run the test and confirm it fails on missing `MrcSglbTrace` records.
- [ ] Add only the fields required to join events; derive the source-ToR actual physical path from the chosen uplink rather than overwriting packet `pathid`.
- [ ] Re-run focused trace, MRC skip semantics, and SGLB routing tests.

### Task 3: Controlled four-scheme runner

**Files:**
- Create: `experiments/n-mrc/run_mrc_sglb_ev_pollution.py`
- Test: `sim/tests/test_mrc_sglb_ev_pollution_runner.py`

**Interfaces:**
- Produces `build_command`, `parse_trace`, `derive_causal_metrics`, `validate_cell`, and deterministic artifacts under `experiments/n-mrc/output/mrc_sglb_ev_pollution/`.

- [ ] Add failing unit tests with a synthetic trace proving mismatch count, distinct polluted EV count, first feedback RTT, last post-feedback bad-path hit, and effective-avoidance timing.
- [ ] Run the tests and confirm import/behavior failure.
- [ ] Implement identical `rr`, `mrc`, `sglb`, `mrc-sglb` commands with 64 paths, `dcqcn_variant`, 1 us local update, 15 us remote update, one controlled hotspot, plus an unloaded control.
- [ ] Add strict validation for hashes, completion, effective config, measured RTT `<15 us`, and the event-order proof.
- [ ] Run unit tests and a one-seed smoke matrix.

### Task 4: Performance expansion and report

**Files:**
- Modify: `experiments/n-mrc/run_mrc_sglb_ev_pollution.py`
- Create: `experiments/n-mrc/mrc_sglb_ev_pollution_report.md`
- Generate: `experiments/n-mrc/output/mrc_sglb_ev_pollution/{events.csv,cells.csv,summary.csv,paired_flows.csv.gz,causal_timeline.png,performance.png,manifest.json}`

**Interfaces:**
- Consumes the controlled proof and a fixed three-seed 64-path workload.
- Produces paired FCT, goodput, ECN/MiB, TRIM/retransmission, cwnd, pollution, and steering-delay results.

- [ ] Add failing aggregation tests for paired ratios, percentiles, throughput, ECN normalization, and empty/incomplete cells.
- [ ] Implement the three-seed scale matrix without changing parameters after observing outcomes.
- [ ] Run the controlled proof, then the full matrix only if its premise passes.
- [ ] Independently recompute headline metrics from raw CSV and assert equality with `summary.csv`.
- [ ] Render and inspect both figures; write a report that distinguishes observed mechanism, performance consequence, null results, and limitations.
- [ ] Run focused tests, broader simulator tests, build, `py_compile`, `git diff --check`, cached replay, and artifact hash checks before claiming completion.
