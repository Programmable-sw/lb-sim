# Hybrid load-balancing comparison in ns-3.19

This experiment compares routing/load-balancing algorithms over one common OOO-capable transport and per-packet ECN window controller. It is not a reproduction of all MP-RDMA transport/recovery mechanisms, nor a reproduction of the Argus slide's undisclosed traffic generator.

## Reproduce

From the ns-3.19 repository root:

```sh
/home/user/workspace/.deps/python2/bin/python2.7 waf configure --out=build-hybrid --build-profile=optimized --disable-python
/home/user/workspace/.deps/python2/bin/python2.7 waf build -j4
python3 experiments/hybrid-lb/run.py
```

Python 2.7.18 was built locally under the workspace's `.deps/python2` because the original waf requires it. `build-hybrid` is separate from the pre-existing `build` directory. Never use the old `build/scratch/network-load-balance` for this experiment.

## Workload and reporting

- 16 hosts, 4 leaves with 4 hosts each, 4 spines; all links 400 Gbps and 500 ns propagation.
- 240 all-to-all collective flows of 1 MiB, starting together.
- Short-flow bytes = 10% of collective bytes; 2472 short flows with seed 13. This is a byte ratio, not a calibrated fraction of link capacity.
- Short sizes/arrival draws preserve the htsim runner's RNG sequence. Its `start` field is picoseconds: arrivals occupy 0–0.5 microseconds. ns-3 rounds timestamps to nanoseconds. Absolute start is shifted to 1 second.
- All data uses the same priority group (3). Collective flows use the selected LB; short flows explicitly select ECMP. ACK/control routing remains ECMP.
- JCT = last collective completion minus common collective start. Short flow completion is checked but not included in JCT.
- Every run must finish all 2712 flows, return one unique data acknowledgment per packet, preserve short-flow LB=ECMP, and have zero ingress/egress admission drops. Otherwise the runner fails instead of publishing a complete comparison.

## Network/CC configuration

- Existing Broadcom-style shared switch buffer: 9 MiB **per switch**, dynamic PFC enabled. This is not equivalent to htsim's 135-packet per-output buffer.
- ECN Kmin=Kmax=20,000 bytes, Pmax=1 (the project's ConfigEcn uses decimal kB). The earlier htsim command used 20,480 bytes; that difference is explicit here.
- Headroom uses configured payload plus 48 header bytes, not the prior fixed 1000-byte payload assumption.
- 4096-byte data payload. The simulated RTT/BDP derived by this topology are 4324 ns / 216200 bytes. This ns-3 setup has no added 500 ns switch processing latency. It therefore differs from the former three-tier htsim topology's 11 us estimate.
- Experimental `CC_MODE 14`: initial window one calculated BDP; each unique clean ACK increases cwnd by 1/cwnd, each unique ECN ACK decreases it by 0.5 packet, lower bound 1. Actual outstanding data count gates the window. All four schemes and their short flows use this controller.
- Existing DCQCN/HPCC/TIMELY/DCTCP modes are retained. New selection logic is opt-in to mode 14.

## Schemes

- ECMP (`LB_MODE 0`): stable five-tuple routing.
- CONGA (`3`): upstream DRE/leaf-feedback/flowlet implementation with explicit paper defaults Q=3, flowlet timeout=500 us, DRE period=32 us and alpha=0.2 (time constant=160 us). The flowlet table now uses source/destination IP, source/destination port and priority group without overlapping fields; the Q-bit metric saturates at `2^Q-1`; an expired flowlet retains its old path when that path ties for the minimum; and ECMP short-flow bytes contribute to the DRE of the links they use. Existing metric aging remains 500 us.
- MP-RDMA (`14`): random UDP source-port VPs, echoed VP and delivered sequence, out-of-order pruning with delta=32 packets, ACK-clock budget at most two packets and subject to available cwnd, at most one 1% probe trial per base RTT, retransmissions reuse their recorded VP. A base-RTT timer releases random-VP traffic when no clock credits remain. VPs are ECMP hash inputs, **not guaranteed one-to-one physical paths**.
- REPS (`11`, mode 14): 16-bit entropy, eight-entry clean-ACK ring, consume oldest valid entry, random exploration when empty, initialized-entry-only frozen recycling, 100 us freeze deadline, clean-ACK-triggered exit, and cwnd-sized exploration after exit. Initial exploration happens naturally while there is no returning clean feedback.

## Corrected matrix

Collective JCT in microseconds (seed 13):

| Short-flow load | ECMP | CONGA | MP-RDMA | REPS |
| ---: | ---: | ---: | ---: | ---: |
| 0% | 484.417 | 437.856 | 364.566 | 356.523 |
| 5% | 497.688 | 464.780 | 397.739 | 383.622 |
| 10% | 514.617 | 476.738 | 412.427 | 391.663 |
| 15% | 532.350 | 494.582 | 435.244 | 413.101 |
| 20% | 542.347 | 506.849 | 449.646 | 424.265 |

All runs completed every flow with zero admission drops and zero retransmission timeouts. At 10%, correcting CONGA changes JCT from 598.274 to 476.738 microseconds and pause events from 2310 to 34; the other schemes retain their previous results.

## Limits

- Common receiver uses simulation-side OOO bookkeeping, not MP-RDMA's bounded 64-bit receive bitmap and complete memory-registration/synchronization implementation. MP-RDMA's FUSO early recovery is not implemented. Burst timeout uses base RTT rather than an outstanding-ACK-dependent estimate. Loss recovery uses the repository's selective retransmission infrastructure; this is not a complete paper transport artifact.
- `PaperLbTag` carries original flow identity and precise data feedback as simulation metadata, without adding wire bytes. Header-overhead comparisons are outside this experiment.
- PFC columns count received pause/resume trace events (refresh pauses count again), not independent congestion episodes or aggregate pause duration.
- Single seed and one load are a smoke comparison, not a statistically established ranking. REPS being newer does not imply that it must win every workload.

## Validation

```sh
g++ -std=c++11 experiments/hybrid-lb/test_state.cc -o /tmp/test-paper-lb
/tmp/test-paper-lb
g++ -std=c++11 experiments/hybrid-lb/test_conga_helpers.cc -o /tmp/test-conga-helpers
/tmp/test-conga-helpers
g++ -std=c++11 -Ibuild-hybrid experiments/hybrid-lb/test_tag.cc -Lbuild-hybrid -lns3.19-network-optimized -lns3.19-core-optimized -o /tmp/test-paper-tag
LD_LIBRARY_PATH="$PWD/build-hybrid" /tmp/test-paper-tag
python3 -m py_compile experiments/hybrid-lb/run.py
git diff --check
```

Each output directory contains the shared traffic/topology, per-scheme configuration, command, complete run log, flow completions, raw PFC traces, results CSV and executable/library hashes. The corrected matrix is in `output/{0pct,5pct,10pct,15pct,20pct}-conga-corrected`; `output/10pct-paper-defaults` preserves the result before the CONGA corrections.
