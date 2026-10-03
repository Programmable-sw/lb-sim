# CONGA Default Mainline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the current optimized CONGA implementations the clean default baseline and align HTSim feedback cadence and aging with corrected ns-3.19 CONGA.

**Architecture:** Preserve the current switch-owned HTSim DRE, flowlet, and leaf-pair tables. Replace the dirty-entry feedback cursor with a small CONGA helper that prunes entries at 500 microseconds and selects one current entry by a supplied random value. Commit existing CONGA-only changes independently from the new semantic correction.

**Tech Stack:** C++11, HTSim event simulation, ns-3.19 C++, GNU make, Python hybrid workload runner.

## Global Constraints

- Do not change ECMP, MP-RDMA, REPS, congestion control, topology, queueing, or hybrid traffic generation.
- Keep random choices driven by the existing `-seed` RNG.
- Keep all unrelated dirty worktree changes unstaged.
- Require zero RTO, lossy/composite drops, and shared-buffer overflow in the workload run.

---

### Task 1: Preserve the optimized CONGA baselines in clean commits

**Files:**
- Modify/commit only existing CONGA-related HTSim source, tests, experiment runner, and documentation.
- Modify/commit only existing CONGA-related ns-3.19 source, hybrid experiment, and documentation.

**Interfaces:**
- Produces: a committed baseline containing Q=3 DRE, 32 us/alpha 0.2 DRE decay, 500 us flowlet timeout, short-flow DRE accounting, and corrected ns-3 flowlet-key/metric behavior.

- [ ] **Step 1: Inspect each candidate diff hunk**

Run: `git diff -- <candidate files>` in each repository.

Expected: stage only hunks directly implementing CONGA or its focused tests/docs; do not stage MRC/SGLB/queue/transport work.

- [ ] **Step 2: Run baseline focused tests**

Run: `make -C sim/tests htsim_conga_model && ./sim/tests/htsim_conga_model` and `g++ -std=c++11 experiments/hybrid-lb/test_conga_helpers.cc -o /tmp/test-conga-helpers && /tmp/test-conga-helpers`.

Expected: both commands exit 0.

- [ ] **Step 3: Create separate baseline commits**

Run: `git commit -m "feat: establish optimized CONGA baseline"` in each repository after reviewing the staged patch.

Expected: each commit contains only its repository's CONGA baseline.

### Task 2: Specify and test HTSim feedback-table selection

**Files:**
- Modify: `sim/conga_model.h`
- Modify: `sim/tests/main_conga_model.cpp`

**Interfaces:**
- Produces: `conga_select_feedback(...)`, accepting a mutable path-to-info table, current simulation time, aging interval, and a random value; returns a selected path/metric only when a fresh entry remains.

- [ ] **Step 1: Write the failing unit test**

Add entries with timestamps `now-499us` and `now-501us`; assert that selection removes the stale entry, returns the fresh path/metric, and returns that same fresh entry on a second call.

- [ ] **Step 2: Verify RED**

Run: `make -C sim/tests htsim_conga_model && ./sim/tests/htsim_conga_model`.

Expected: compilation fails because `conga_select_feedback` is undefined.

- [ ] **Step 3: Implement the helper**

Add a small templated/helper representation in `conga_model.h` that erases entries older than `timeFromUs(500.0)` at the caller boundary, indexes the remaining entries by `random_value % size`, and never mutates a selected fresh entry.

- [ ] **Step 4: Verify GREEN**

Run: `make -C sim/tests htsim_conga_model && ./sim/tests/htsim_conga_model`.

Expected: exit 0.

### Task 3: Wire the helper into HTSim CONGA transmission and validate the workload

**Files:**
- Modify: `sim/datacenter/fat_tree_switch.cpp`
- Modify: `sim/datacenter/fat_tree_switch.h`
- Modify: `experiments/n-mrc/README.md`

**Interfaces:**
- Consumes: Task 2 feedback selection behavior.
- Produces: every outgoing source-ToR CONGA data packet can piggyback a fresh, randomly selected FromLeaf feedback record; stale FromLeaf and ToLeaf state expires at 500 us.

- [ ] **Step 1: Replace the dirty/cursor send path**

At the source-ToR CONGA path, prune `FromLeaf[destination_leaf]` at 500 us, choose one surviving entry with `random()`, set `set_conga_feedback(path, metric)`, and do not clear any entry. Remove the now-unused dirty flag/cursor state.

- [ ] **Step 2: Align remote feedback aging**

When reading `ToLeaf[destination_leaf][path]`, treat entries older than 500 us as zero/remove them. Keep local DRE and the 500 us flowlet gap unchanged.

- [ ] **Step 3: Build and run focused checks**

Run: `make -C sim/tests htsim_conga_model && ./sim/tests/htsim_conga_model`.

Expected: exit 0.

- [ ] **Step 4: Run unchanged hybrid workload**

Run: `python3 experiments/n-mrc/run_mprdma_bursty_a2a.py --traffic-json ../ns-allinone-3.19/ns-3.19/experiments/hybrid-lb/output/10pct-conga-corrected/traffic.json --out output/conga-feedback-align-10pct`.

Expected: all 2712 flows complete for every scheme; diagnostics report zero prohibited failures; preserve generated logs and summary.

- [ ] **Step 5: Commit the focused correction**

Run: `git add sim/conga_model.h sim/tests/main_conga_model.cpp sim/datacenter/fat_tree_switch.cpp sim/datacenter/fat_tree_switch.h experiments/n-mrc/README.md && git commit -m "fix: align CONGA feedback cadence and aging"`.

Expected: one commit containing the test and correction only.
