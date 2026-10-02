# CONGA Default Mainline Design

## Goal

Promote the current optimized CONGA implementations in HTSim and ns-3.19 as
the default comparison baseline, then align HTSim's feedback cadence and
aging with the corrected ns-3.19 behavior without changing ECMP, MP-RDMA,
REPS, transport, queueing, topology, or the hybrid traffic artifact.

## Scope

The clean CONGA-only history includes the existing optimized changes: Q=3
DRE quantization, 32 microsecond DRE period with alpha 0.2, 500 microsecond
flowlet stickiness, ECMP short-flow DRE accounting, full-flow flowlet keys in
ns-3, and deterministic-seed tie selection. It excludes unrelated dirty MRC,
SGLB, shared-buffer, transport, and experiment changes.

HTSim will make its source-ToR feedback behavior equivalent in cadence to
ns-3: each outgoing new CONGA data packet chooses one current FromLeaf entry
uniformly and piggybacks it. A selected feedback entry remains available; it
is not consumed by transmission. FromLeaf and ToLeaf stale entries age at 500
microseconds, matching ns-3's CONGA aging event.

## Design

`FatTreeSwitch` remains the owner of CONGA state. On a data arrival at a
destination ToR, it overwrites the `(source leaf, path)` FromLeaf metric and
timestamp. When a source ToR sends a data packet toward a remote leaf, it
removes stale entries, randomly selects one remaining FromLeaf entry for that
destination, and copies its path and metric into the packet feedback fields.
No dirty bit or feedback cursor participates in this selection.

DRE accounting, flowlet selection, path label assignment, and receiver ACK
processing are unchanged. Existing use of `random()` remains seeded through
the experiment's `-seed`, consistent with the present ns-3 comparison.

## Verification

Add a focused unit test for the feedback-selection helper/state semantics:
fresh entries are selectable repeatedly and entries older than 500
microseconds are excluded. Run the CONGA helper test and the existing hybrid
10 percent workload with the unchanged traffic input. The runner must
complete every flow with no RTO, lossy/composite drops, or shared-buffer
overflow. Compare CONGA JCT and PFC events against HTSim ECMP and the ns-3
corrected reference; the result is diagnostic evidence, not a required
bit-for-bit reproduction.

## Commit Boundaries

1. Existing optimized CONGA-only files and their existing tests/docs are
   committed separately for HTSim and ns-3.19.
2. The HTSim feedback-cadence/aging correction and its regression test are a
   subsequent focused commit.
