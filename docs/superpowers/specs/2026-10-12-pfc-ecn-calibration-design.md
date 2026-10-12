# PFC pre-trim ECN calibration design

## Scope and versioning

Keep `queue-fixed-v5-spine-admission` and all existing outputs immutable.  The
new simulator identity is `queue-fixed-v6-ecn-calibration`.  V5's JCT hybrid
results are explicitly classified as ECMP behavior because JCT flow names did
not satisfy the old healthy-flow prefix test.  The old always-pretrim P1 phase
rate table remains a diagnostic artifact and is not evidence for v5 paused JCT.

## Routing correction

Each flow carries an explicit role (`healthy`, `background`, or `diagnostic`).
Routing policy is selected from that role, never inferred from its display
name.  In hybrid mode healthy packets use OPS packet spraying while background
packets retain their seeded ECMP path.  Resolved configuration and trace events
record both role and selected path.  Tests cover ordinary and multi-round JCT
flow names.

## ECN marking

Leaf and Spine data egress admission use one marking function.  Queue `q` is
sampled immediately before admitting the arriving packet and excludes that
packet.  It is the data-memory occupancy of the selected physical egress queue;
headers and control frames are excluded.  Thresholds use memory bytes, not wire
bytes.

For switch class `s`:

* `q < Kmin_s`: no mark.
* `Kmin_s <= q < Kmax_s`: mark with probability
  `Pmax * (q-Kmin_s)/(Kmax_s-Kmin_s)`.
* `q >= Kmax_s`: mark with probability one.

Mark decisions use a dedicated deterministic PRNG stream derived from the
route seed but independent of traffic generation and routing.  Retransmissions
pass through the same marking function.  A trimmed header either preserves the
packet's CE bit or clears it according to the explicit ablation option.

The first threshold sweep fixes `Pmax=1.0` and compares, in KiB:

| profile | Leaf Kmin/Kmax | Spine Kmin/Kmax |
|---|---:|---:|
| reference | 8/16 | 8/16 |
| A | 32/128 | 16/64 |
| B | 64/256 | 32/128 |
| C | 128/512 | 32/128 |

The second sweep evaluates `Pmax` 0.01, 0.1, and 1.0 on candidate intervals.
The common profile is selected using no-trim, normal-consumption throughput,
latency, queue, CNP, actual-rate, pause, low-rate, and window-block metrics.

## Workloads and experiment order

Seed 13 runs first with trimming disabled and normal NIC consumption for
permutation, A2A, ordinary incast, and a long background flow covering the
job.  Representative points keep full traces; grids use metrics-only output.
After freezing ECN, compare off and paused trimming, header CE preservation,
and trim thresholds derived from the dynamic buffer bound.  Only validated
key points proceed to 20 paired seeds.

## Metrics and attribution

Every run retains existing `run_valid` checks and reports absolute CCT/JCT,
background/incast FCT, goodput, queue peaks/integrals, ECN/CNP counts, actual
send-service rates, PFC intervals, retransmission cost, low-rate duration, and
window-block duration.  Blocking intervals are unioned per cause.  The FCT
decomposition uses an explicit priority order—PFC, serializer/queue, CC pacing,
window, then other—and separately reports overlaps and raw per-cause unions;
exclusive components sum to elapsed FCT.  CC influence is checked against real
`tx_start` service events.  All reports label the implementation as a DCQCN
subset.

