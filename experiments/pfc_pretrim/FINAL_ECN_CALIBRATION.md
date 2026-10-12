# ECN calibration and corrected hybrid validation

## Frozen version

The network-data build is `queue-fixed-v6-ecn-calibration`, binary SHA-256
`dad9e7b9607867af049e2a0bac957e69a2d239823d6655d7689675d087ff4424`.
The later representative-trace build only adds interval events; its binary is
`8c76b2e3f9ca1c7adf3d7605f9d947fb129b7d94376751b5c20cbb6b6697609c`
and does not change network decisions.  Both builds use the same DCQCN subset.

V5 is retained.  Its hybrid JCT healthy flows were named `round-N-healthy-*`,
which failed the old `startswith("healthy-")` test, so those old hybrid JCT
points actually used ECMP.  V6 carries an explicit flow role: healthy traffic
uses OPS in hybrid and background traffic uses seeded ECMP.  The old P1 phase
rate table used always pre-trim and cannot explain v5 paused JCT.

ECN samples the selected physical data-egress queue immediately before packet
admission.  `q` is resident data memory bytes and excludes the arriving packet,
wire overhead, headers, and control frames.  The mark probability is zero below
Kmin, `Pmax*(q-Kmin)/(Kmax-Kmin)` in the interval, and one at or above Kmax.
The deterministic ECN random stream is independent of routing and traffic RNG.

## Seed 13 threshold calibration, Pmax=100%

All runs use trimming off and normal NIC consumption.

| profile | permutation CCT (us) | A2A CCT (us) | incast FCT (us) | long-bg JCT / FCT (us) | A2A CNP | long-bg CNP |
|---|---:|---:|---:|---:|---:|---:|
| 8/16; 8/16 | 26.440 | 92.168 | 370.156 | 1478.795 / 6440.815 | 472 | 97 |
| A: 32/128; 16/64 | 26.440 | 92.153 | 335.354 | 1479.061 / 2767.579 | 40 | 7 |
| B: 64/256; 32/128 | 26.440 | 92.153 | 329.477 | 1479.145 / 2603.525 | 0 | 0 |
| C: 128/512; 32/128 | 26.440 | 92.153 | 329.477 | 1479.145 / 2603.525 | 0 | 0 |

B and C are no-feedback points under these loads.  The original thresholds
over-react: they leave normal collective latency essentially unchanged but make
the long background FCT 6.441 ms.

For profile A, Pmax 1%, 10%, and 100% give long-background FCTs 2665.659,
2702.316, and 2767.579 us, with 3, 4, and 7 CNPs.  A2A CCT remains 92.153 us;
ordinary-incast FCT is 329.477, 334.117, and 335.354 us.  The common frozen
setting is therefore A plus Pmax=1%: it retains a measured feedback path without
the normal-load penalty of stronger marking.  Selection did not use trim speedup.

## Seed 13 corrected JCT, frozen ECN

| LB | workload | off JCT / bg FCT (us) | paused JCT / bg FCT (us) | paused retransmit (B) |
|---|---|---:|---:|---:|
| ECMP | A2A | 3125.965 / 4871.981 | 2793.055 / 2811.003 | 7,471,104 |
| ECMP | permutation | 1780.120 / 4869.261 | 1479.145 / 2813.084 | 7,471,104 |
| OPS | A2A | 3048.946 / 5767.738 | 2721.517 / 2830.026 | 7,467,008 |
| OPS | permutation | 1736.399 / 4902.644 | 1478.976 / 2824.366 | 7,471,104 |
| hybrid | A2A | 3025.005 / 4890.611 | 2721.940 / 2811.114 | 7,475,200 |
| hybrid | permutation | 1749.592 / 4829.041 | 1479.074 / 2819.491 | 7,471,104 |

Corrected hybrid is no longer numerically identical to ECMP.  Header CE
preserve/clear is a measured no-effect point at seed 13: A2A is 92.153/543.092
us and permutation is 26.440/541.353 us in both cases, because the packets
trimmed after the paused gate did not carry CE under A plus 1%.

## Twenty-seed paired JCT

Values are absolute means.  CI is the paired saving's 95% t interval.

| LB | workload | off JCT | paused JCT | saving [95% CI] us | saving range | off / paused bg FCT us |
|---|---|---:|---:|---:|---:|---:|
| ECMP | A2A | 3095.665 | 2784.355 | 311.310 [298.068,324.553] | 258.113–367.731 | 8897.326 / 5576.035 |
| ECMP | permutation | 1738.258 | 1479.725 | 258.533 [230.708,286.358] | 74.427–325.254 | 4706.370 / 2811.912 |
| OPS | A2A | 3028.137 | 2721.047 | 307.090 [300.458,313.723] | 279.416–333.579 | 5327.537 / 2837.558 |
| OPS | permutation | 1737.359 | 1478.372 | 258.987 [257.963,260.010] | 254.738–262.734 | 4942.301 / 2813.219 |
| hybrid | A2A | 3045.638 | 2721.531 | 324.107 [314.340,333.873] | 276.609–351.211 | 4964.454 / 2833.767 |
| hybrid | permutation | 1757.278 | 1478.463 | 278.815 [266.607,291.023] | 231.719–323.002 | 4826.600 / 2811.700 |

Paused retransmission averages 7.33–7.47 MB depending on LB/workload.  Every
point passed reliable delivery, final drain, MMU/headroom, serializer, and
pause-service validity checks.

## Trim threshold and recovery cost

The paused bound is `max(q0,T)+overshoot`; low T never claims recovery of old
resident packets.  At seed 13 one-round A2A, q0 is zero and T=8/16/32/64/128
KiB all give 92.153 us healthy CCT.  Background FCT rises monotonically from
541.738 to 547.839 us while retransmission falls from 7,516,160 to 7,393,280 B.
These are retained no-additional-benefit points.

Across 20 paired seeds, T=8/32/128 KiB gives mean off CCT 373.487 us and paused
CCT 92.674/92.744/92.674 us.  Paired savings are 280.813±7.643,
280.743±7.626, and 280.813±7.643 us.  Paused background FCT is
541.567/542.644/546.169 us.  T=32 KiB remains the common default because it is
inside the dynamic-buffer safety bound and avoids selecting an equally fast but
more aggressive 8 KiB point.

## Background FCT decomposition

For seed 13 ECMP JCT-A2A, background FCT falls 4871.981→2811.003 us.  Raw
unions overlap: PFC is 503.161→193.461 us, CC-low is 3984.184→774.813 us,
window-blocked is 4221.443→2444.387 us, and actual target-egress serialization
is 2079.130 us in both groups.  They must not be added.

Using the declared priority PFC, actual serializer service, CC-low, window,
other, the exclusive partitions are respectively:

| policy | PFC | serializer | CC-low | window | other | sum/FCT |
|---|---:|---:|---:|---:|---:|---:|
| off | 503.161 | 1769.417 | 2459.490 | 100.535 | 39.378 | 4871.981 |
| paused | 193.461 | 2079.117 | 164.645 | 333.512 | 40.267 | 2811.003 |

Thus the observed reduction is chiefly the disappearance of CC-low time plus
309.700 us less PFC; window-exclusive time increases by 232.977 us.  Actual
background pacing/service confirms the mechanism: pre-fault arrivals are
320.087 Gbit/s in both groups; during the fault they are 272.750 off versus
320.803 paused; after recovery they are 140.142 versus 316.704 Gbit/s.  The
paused group pays 7.47 MB of controlled retransmission.  This is an interval
attribution, not a claim that overlapping raw durations are independently
causal.

## Reproduction

```sh
make -C sim/datacenter htsim_pfc_pretrim -j2
python3 experiments/pfc_pretrim/run_ecn_calibration.py --output <dir> --seed 13 --pmax 1 --workers 4
python3 experiments/pfc_pretrim/run_ecn_postfreeze.py --output <dir> --seeds 1,2,...,20 --lbs ecmp,ops,hybrid --workloads jct-a2a,jct-permutation --policies off,paused --trim-ces off --thresholds 32 --workers 8
python3 experiments/pfc_pretrim/analyze_ecn_final.py
```

Complete commands, resolved configs, stdout/stderr, manifests, raw CSVs, and
compressed representative traces are under
`results/packet_sim/queue-fixed-v6-ecn-calibration/`.
