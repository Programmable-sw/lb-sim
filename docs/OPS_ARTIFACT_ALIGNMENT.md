# OPS artifact alignment

Reference: REPS_EuroSys_Artifact, commit b751588745b57e8264ce22d9a40df67bdedd62c0.

The author's `uec.cpp:2358` OBLIVIOUS branch selects `rand()%_no_of_paths`.
The experiment scripts (including `artifact_scripts/fig_11_ack_compression.py`)
explicitly set `-paths 65535`; the source default 256 is not the experiment setting.
New packets and retransmissions both select fresh entropy. Switch ECMP hashes
the flow identifier, entropy and switch salt; it does not choose the emptiest path.

Current OPS now uses 65535 entropy values (0–65534), replacing the previous
65536-value mask. It continues to hash entropy to the eight physical Spines.
No CC, recovery, traffic, PFC, trimming, MP-RDMA or REPS behavior was changed.
An `ops_config` event at QP initialization explicitly records the entropy range.

This is algorithm/entropy-parameter alignment, not bitwise reproduction of the
author's simulator: this model retains independent per-QP SplitMix64 streams,
QP-ID hash input, and deterministic Leaf salt instead of the author's global
rand(), source/destination flow identifier and random switch salt. Full topology,
packet representation and UEC transport are not replaced. Performance differences
must be measured anew; a different random sequence alone is not an improvement.

Validation: the million-draw entropy boundary regression failed with the old
mask and passes after the modulo change. Existing hash mapping, seed repeatability,
independent-QP and non-round-robin distribution tests remain enabled.

Historical fixed-waterline results retain their original build hashes and commands.
Retired mode executables and snapshots have been removed during mode cleanup.
Current runs use the single Leaf PAUSE-gated waterline interface; do not mix
performance results from different binary hashes.
