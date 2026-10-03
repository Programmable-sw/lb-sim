#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def read(path):
    return (ROOT / path).read_text(encoding="utf-8")


def assert_contains(text, needle, context):
    if needle not in text:
        raise AssertionError(f"missing {needle!r} in {context}")


def assert_absent(text, needle, context):
    if needle in text:
        raise AssertionError(f"unexpected {needle!r} in {context}")


def main():
    main_roce = read("sim/datacenter/main_roce.cpp")
    roce_h = read("sim/roce.h")
    roce_cpp = read("sim/roce.cpp")
    switch_h = read("sim/datacenter/fat_tree_switch.h")
    switch_cpp = read("sim/datacenter/fat_tree_switch.cpp")
    experiment_readme = read("experiments/n-mrc/README.md")

    assert_contains(main_roce, '"n-mrc-fixed0.5"', "fixed-threshold N-MRC preset")
    assert_contains(main_roce, '"n-mrc-delta"', "pure-delta N-MRC preset")
    assert_contains(main_roce, "-nmrc_absolute_threshold requires -lb n-mrc-fixed0.5", "fixed-only absolute threshold validation")
    assert_contains(main_roce, "double nmrc_relative_delta = 0.25", "N-MRC delta default")
    assert_contains(main_roce, "NMRC_NETWORK_FIXED_THRESHOLD", "fixed-threshold decision mode")
    assert_contains(main_roce, "NMRC_NETWORK_DELTA", "pure-delta decision mode")
    assert_contains(switch_cpp, "_nmrc_piecewise_delta_below = 0.25", "piecewise lower-score delta")
    assert_contains(switch_cpp, "_nmrc_piecewise_delta_above = 0.15", "piecewise higher-score delta")

    assert_contains(main_roce, "netaware", "main_roce NetAware CLI")
    assert_contains(main_roce, "LB_NETAWARE", "main_roce NetAware mode")
    assert_contains(main_roce, "NetAware default queue_type composite_ecn_lb", "NetAware queue default")
    assert_contains(main_roce, "NetAware default receive mode sp", "NetAware RX default")
    assert_contains(main_roce, "NetAware default cc dctcp_variant", "NetAware CC default")
    assert_absent(main_roce, "-netaware_feedback_pkts", "fixed NetAware feedback cadence")
    assert_absent(main_roce, "-netaware_feedback_min_us", "fixed NetAware feedback cadence")
    assert_absent(main_roce, "-netaware_feedback_max_us", "fixed NetAware feedback cadence")
    assert_contains(main_roce, "-netaware_queue_threshold", "NetAware queue threshold CLI")
    assert_contains(main_roce, "-netaware_queue_level_thresholds", "NetAware queue level threshold CLI")
    assert_contains(main_roce, "-netaware_degraded_util_thresholds", "NetAware utilization degradation threshold CLI")
    assert_contains(main_roce, "-netaware_score_mode gated|worst_hop|sglb_quantized", "NetAware score mode CLI")
    assert_contains(main_roce, "-netaware_score_q_range kmin kmax", "NetAware queue pressure range CLI")
    assert_contains(main_roce, "-netaware_score_util_range low high", "NetAware utilization pressure range CLI")
    assert_contains(main_roce, "-netaware_score_weights w_lq w_rq w_lu w_ru", "NetAware SGLB-style score weights CLI")
    assert_contains(main_roce, "-netaware_score_level_thresholds degraded bad avoid", "NetAware score quantization thresholds CLI")
    assert_contains(main_roce, "-netaware_level_weights good degraded bad avoid", "NetAware endpoint level weights CLI")
    assert_contains(main_roce, "-netaware_wrr_mode shuffled_bucket|bucket|direct|topk", "NetAware WRR mode CLI")
    assert_contains(main_roce, "-netaware_topk k", "NetAware hard top-k size CLI")
    assert_contains(main_roce, "-netaware_weight_adaptation off|good_share_cap", "consolidated NetAware weight adaptation CLI")
    assert_absent(main_roce, "-netaware_max_path_share_multiplier", "removed ShareCap parameter")
    assert_absent(main_roce, "-netaware_soft_weight_beta_min", "removed ProfileSoft parameter")
    assert_contains(main_roce, "weight_adaptation ", "NetAware canonical adaptation mode")
    assert_absent(main_roce, "max_path_share_multiplier ", "removed ShareCap diagnostic")
    assert_absent(main_roce, "max_path_share_tickets ", "removed ShareCap diagnostic")
    assert_contains(main_roce, "NETAWARE_SCORE_SGLB_QUANTIZED", "NetAware SGLB-style score mode default")
    assert_contains(main_roce, "_netaware_score_mode = netaware_score_mode", "NetAware score mode assignment")
    assert_contains(main_roce, "_netaware_score_q_min = netaware_score_q_min", "NetAware queue pressure range assignment")
    assert_contains(main_roce, "_netaware_score_util_high = netaware_score_util_high", "NetAware utilization pressure range assignment")
    assert_contains(main_roce, "_netaware_score_weight_remote_util = netaware_score_weight_remote_util", "NetAware score weight assignment")
    assert_contains(main_roce, "_netaware_score_avoid_threshold = netaware_score_avoid_threshold", "NetAware score threshold assignment")
    assert_contains(main_roce, "_netaware_degraded_queue_fraction = netaware_degraded_queue_threshold", "NetAware degraded queue threshold assignment")
    assert_contains(main_roce, "_netaware_bad_queue_fraction = netaware_bad_queue_threshold", "NetAware bad queue threshold assignment")
    assert_contains(main_roce, "_netaware_util_queue_floor_fraction = netaware_util_queue_floor_threshold", "NetAware utilization queue floor assignment")
    assert_contains(main_roce, "_netaware_degraded_utilization_fraction = netaware_degraded_utilization_threshold", "NetAware degraded utilization assignment")
    assert_contains(main_roce, "_netaware_enabled = roce_lb_mode == RoceSrc::LB_NETAWARE", "NetAware switch enable")

    assert_contains(roce_h, "LB_NETAWARE", "RoceSrc lb enum")
    assert_contains(roce_cpp, "if (_flow_lb_mode == LB_NETAWARE", "NetAware endpoint level path")
    assert_contains(roce_cpp, "choose_netaware_path(priority, path_space)", "NetAware endpoint selector call")
    assert_contains(roce_cpp, "void RoceSrc::update_netaware", "NetAware endpoint feedback update")
    assert_contains(roce_cpp, "_lb_mode != LB_NETAWARE || !ack.has_netaware_feedback()", "NetAware endpoint feedback update")
    assert_contains(roce_cpp, "apply_netaware_snapshot(ack.netaware_feedback(), path_space)", "NetAware full snapshot replacement")
    assert_contains(roce_cpp, "_netaware_shared_profiles", "NetAware shared profile storage")
    assert_contains(roce_cpp, "virtual_shuffle_index", "NetAware virtual per-flow permutation")
    assert_contains(roce_h, "resetNetawareSharedState", "NetAware shared state reset")
    assert_contains(roce_cpp, "_netaware_level_weights = {{4, 2, 1, 0}}", "NetAware tuned default weights")
    assert_contains(switch_cpp, "_netaware_score_degraded_threshold = 0.10", "NetAware tuned DEGRADED threshold")
    assert_contains(switch_cpp, "_netaware_score_bad_threshold = 0.40", "NetAware tuned BAD threshold")
    assert_contains(switch_cpp, "_netaware_score_avoid_threshold = 0.60", "NetAware tuned AVOID threshold")
    assert_contains(roce_cpp, "_netaware_wrr_mode = NETAWARE_WRR_SHUFFLED_BUCKET", "NetAware shuffled bucket WRR default")
    assert_contains(
        roce_cpp,
        "_netaware_weight_adaptation =\n    NETAWARE_WEIGHT_ADAPTATION_GOOD_SHARE_CAP",
        "NetAware GoodCap adaptation default",
    )
    assert_contains(roce_h, "setNetawareLevelWeights", "independent NetAware level weight setter")
    assert_contains(roce_h, "setNetawareWrrMode", "NetAware WRR mode setter")
    assert_contains(roce_h, "setNetawareWeightAdaptation", "NetAware adaptation mode setter")
    assert_contains(roce_cpp, "NETAWARE_WEIGHT_ADAPTATION_GOOD_SHARE_CAP", "NetAware GOOD-share-cap implementation")
    runtime_sources = {
        "sim/datacenter/main_roce.cpp": main_roce,
        "sim/roce.h": roce_h,
        "sim/roce.cpp": roce_cpp,
    }
    obsolete_runtime_tokens = (
        "NETAWARE_WEIGHT_ADAPTATION_SHARE_CAP",
        "NETAWARE_WEIGHT_ADAPTATION_PROFILE_SOFT",
        "NETAWARE_WEIGHT_ADAPTATION_RANK_FILL",
        "NETAWARE_WEIGHT_ADAPTATION_ORDINAL_GOOD_CAP",
        "NETAWARE_WEIGHT_ADAPTATION_CONFIDENCE_4210_CAP",
        "NETAWARE_WEIGHT_ADAPTATION_THRESHOLD_HEADROOM",
        "NETAWARE_WEIGHT_ADAPTATION_BREADTH_BLEND",
        '"share_cap"',
        '"profile_soft"',
        '"rank_fill"',
        '"ordinal_good_cap"',
        '"confidence_4210_cap"',
        '"threshold_headroom"',
        '"breadth_blend"',
        "_netaware_max_path_share_multiplier",
        "_netaware_soft_weight_beta_min",
        "setNetawareMaxPathShareMultiplier",
        "setNetawareSoftWeightBetaMin",
        "setNetawareScoreLevelThresholds",
    )
    for path, text in runtime_sources.items():
        for obsolete in obsolete_runtime_tokens:
            assert_absent(text, obsolete, path)
    assert_contains(roce_cpp, "choose_netaware_direct_path", "NetAware direct per-path WRR")
    assert_contains(roce_cpp, "choose_netaware_virtual_bucket_path", "NetAware virtual shuffled bucket WRR")
    assert_contains(roce_cpp, "choose_netaware_topk_path", "NetAware hard top-k selector")
    assert_contains(roce_h, "weightedShuffledBucketSize", "dynamic K=4P shuffled bucket size")
    assert_contains(roce_cpp, "_netaware_level_weights[level] * (uint64_t)level_counts[level]", "NetAware count-weighted buckets")
    assert_contains(roce_h, "netawareLevelWeight", "NetAware level weight access")
    assert_contains(main_roce, "RoceSrc::setNetawareLevelWeights", "main_roce NetAware level weight parser")
    for forbidden in ("_netaware_shuffled_bucket", "_netaware_shuffled_tickets",
                      "_dtor_path_bitmap", "_stor_levels", "_netaware_levels",
                      "_netaware_latched_profiles",
                      "NetawareProfilePtr"):
        if forbidden in roce_h:
            raise AssertionError(
                f"per-QP path-profile state must be removed: {forbidden}")

    assert_contains(switch_h, "NetawareState", "NetAware switch state")
    assert_contains(switch_h, "collect_netaware_diag", "NetAware switch diagnostics")
    assert_contains(switch_h, "NETAWARE_SCORE_GATED", "NetAware old gated score mode")
    assert_contains(switch_h, "NETAWARE_SCORE_WORST_HOP", "NetAware worst-hop score mode")
    assert_contains(switch_h, "NETAWARE_SCORE_SGLB_QUANTIZED", "NetAware SGLB-style score mode")
    assert_contains(switch_h, "NetawarePathScore", "NetAware path score detail")
    assert_contains(switch_h, "NetawarePortSnapshot", "NetAware periodic port snapshot")
    assert_contains(switch_h, "_netaware_remote_update_interval", "NetAware 5us spine export interval")
    assert_contains(switch_h, "_netaware_score_weight_local_q", "NetAware local queue score weight")
    assert_contains(switch_h, "_netaware_score_weight_remote_q", "NetAware remote queue score weight")
    assert_contains(switch_h, "_netaware_score_weight_local_util", "NetAware local utilization score weight")
    assert_contains(switch_h, "_netaware_score_weight_remote_util", "NetAware remote utilization score weight")
    assert_contains(switch_h, "_netaware_queue_threshold_fraction", "NetAware threshold state")
    assert_contains(switch_h, "netaware_port_level", "NetAware port grading")
    assert_contains(switch_h, "netaware_gated_level_from_inputs", "NetAware old gated level helper")
    assert_contains(switch_h, "netaware_path_score", "NetAware path score calculation")
    assert_contains(switch_h, "netaware_level_from_score", "NetAware score quantization")
    assert_contains(switch_cpp, "maybe_update_netaware_feedback", "NetAware ACK feedback hook")
    assert_contains(switch_cpp, "netaware_compute_levels", "NetAware level computation")
    assert_contains(switch_cpp, "NETAWARE_SCORE_GATED", "NetAware gated level computation")
    assert_contains(switch_cpp, "NETAWARE_SCORE_SGLB_QUANTIZED", "NetAware SGLB-style level computation")
    assert_contains(switch_cpp, "netaware_neighbor_snapshot", "NetAware cached spine state lookup")
    if "netaware_spine_queue_for_ev" in switch_cpp:
        raise AssertionError("NetAware leaf must not directly read the live spine queue")
    assert_contains(switch_cpp, "netaware_local_queue_for_ev", "NetAware local queue lookup")
    assert_contains(switch_cpp, "netaware_trace_spine_queue_for_ev", "NetAware trace-only live spine queue lookup")
    assert_contains(switch_cpp, "ack->set_netaware_feedback(state.levels)", "NetAware level piggyback")
    assert_contains(rocepacket_h := read("sim/rocepacket.h"), "set_netaware_feedback", "NetAware packet feedback field")
    assert_contains(rocepacket_h, "has_netaware_feedback", "NetAware packet feedback presence")
    assert_contains(rocepacket_h, "netaware_feedback", "NetAware packet feedback accessor")
    assert_contains(switch_cpp, "STOR_LEVEL_DEGRADED", "NetAware degraded level")
    assert_contains(switch_cpp, "STOR_LEVEL_BAD", "NetAware bad level")
    assert_contains(switch_cpp, "STOR_LEVEL_AVOID", "NetAware avoid level")
    assert_contains(switch_cpp, "if (slow_port)\n        return STOR_LEVEL_BAD;", "old gated NetAware slow-link direct BAD baseline")
    assert_contains(switch_cpp, "packet_feedbacks", "NetAware feedback trigger diagnostics")
    assert_contains(switch_cpp, "time_feedbacks", "NetAware feedback trigger diagnostics")
    assert_contains(switch_h, "STOR_SCORE_PROFILE_SIMPLE", "simple 4-bit STOR score profile")
    assert_contains(switch_h, "STOR_SCORE_PROFILE_BINARY", "binary source-ToR STOR profile")
    assert_contains(main_roce, "-stor_simple_score_params", "simple STOR score CLI")
    assert_contains(main_roce, "NetawareDiag", "main_roce NetAware diagnostic output")
    assert_contains(main_roce, "RoceSrc::netawareLevelWeight", "main_roce NetAware weight output")
    assert_contains(main_roce, "local_q_pressure,remote_q_pressure,local_util_pressure,remote_util_pressure,path_score,netaware_level_reason", "NetAware trace score fields")
    if "ack->set_stor_feedback(state.levels)" in switch_cpp:
        raise AssertionError("NetAware must not piggyback via stor_feedback")

    trace_start = switch_cpp.index(
        "bool FatTreeSwitch::netaware_trace_path_state"
    )
    trace_end = switch_cpp.index(
        "void FatTreeSwitch::maybe_update_netaware_feedback", trace_start
    )
    trace_body = switch_cpp[trace_start:trace_end]
    assert_contains(
        trace_body, "peek_average_utilization", "read-only NetAware trace"
    )
    assert_absent(
        trace_body, "netaware_local_snapshot(", "trace local-cache isolation"
    )
    assert_absent(
        trace_body, "netaware_neighbor_snapshot(", "trace remote-cache isolation"
    )
    assert_absent(
        trace_body, "netaware_compute_levels(", "trace level-cache isolation"
    )

    assert_contains(experiment_readme, "## `netaware`", "experiment README NetAware section")
    assert_contains(experiment_readme, "leaf uplink", "experiment README NetAware queue model")
    assert_contains(experiment_readme, "spine downlink", "experiment README NetAware queue model")
    assert_absent(main_roce, "-mrc_active_evs", "removed MRC active-EV CLI")
    assert_absent(
        main_roce, "RoceSrc::setMrcActiveEvs",
        "removed MRC active-EV runtime assignment",
    )
    assert_contains(
        main_roce,
        "mrc_active_paths = 64",
        "fixed MRC active-EV diagnostics",
    )


if __name__ == "__main__":
    main()
