# PFC ECN calibration implementation plan

1. Add failing tests for explicit flow roles in hybrid JCT, Leaf/Spine ECN
   command-line resolution, boundary marking, deterministic independent ECN
   sampling, and unchanged `run_valid` invariants.
2. Add explicit `FlowRole`, per-switch ECN configuration, a dedicated ECN PRNG,
   and the shared marking function to `main_pfc_pretrim.cpp`.
3. Invoke marking at real Leaf and Spine data-egress admission, preserve/clear
   CE on trim as configured, and emit resolved parameters and mark samples.
4. Add interval accounting for PFC, queue/serializer, CC pacing, and window
   blocking plus actual `tx_start` service-rate summaries.
5. Build and run focused unit/integration tests, then the complete packet
   simulator test suite and a short v5-regression comparison.
6. Add a calibration runner and parser that freeze commands, config, build hash,
   stdout/stderr, summaries, representative traces, and paired statistics.
7. Run seed-13 no-trim calibration for all four ECN profiles and workloads,
   followed by the Pmax sweep.  Select and freeze the common profile from normal
   workload metrics.
8. Run seed-13 off/paused, header-CE ablation, and dynamic-budget trim-threshold
   sweep.  Verify every run and decompose representative background FCTs.
9. Run 20 paired seeds for validated key points, update reports and plots, and
   publish old/new configuration and corrected hybrid comparisons.

