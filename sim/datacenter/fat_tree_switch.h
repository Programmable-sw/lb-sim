// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#ifndef _FATTREESWITCH_H
#define _FATTREESWITCH_H

#include "switch.h"
#include "callback_pipe.h"
#include "../shared_buffer.h"
#include "../broadcom_mmu.h"
#include "rocepacket.h"
#include <set>
#include <map>
#include <unordered_map>
#include <vector>
#include <limits>

class FatTreeTopology;
class SglbGcnTimer;
class SglbRealGcnTimer;
class NetawareExportTimer;
class SglbGcnPacket;
struct SglbGcnRecord;

/*
 * Copyright (C) 2013-2014 Universita` di Pisa. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *   1. Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *   2. Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 * Shamelessly copied from FreeBSD
 */

/* ----- FreeBSD if_bridge hash function ------- */

/*
 * The following hash function is adapted from "Hash Functions" by Bob Jenkins
 * ("Algorithm Alley", Dr. Dobbs Journal, September 1997).
 *
 * http://www.burtleburtle.net/bob/hash/spooky.html
 */

#define MIX(a, b, c)                            \
    do {                                        \
        a -= b; a -= c; a ^= (c >> 13);         \
        b -= c; b -= a; b ^= (a << 8);          \
        c -= a; c -= b; c ^= (b >> 13);         \
        a -= b; a -= c; a ^= (c >> 12);         \
        b -= c; b -= a; b ^= (a << 16);         \
        c -= a; c -= b; c ^= (b >> 5);          \
        a -= b; a -= c; a ^= (c >> 3);          \
        b -= c; b -= a; b ^= (a << 10);         \
        c -= a; c -= b; c ^= (b >> 15);         \
    } while (/*CONSTCOND*/0)

static inline uint32_t freeBSDHash(uint32_t target1, uint32_t target2 = 0, uint32_t target3 = 0)
{
    uint32_t a = 0x9e3779b9, b = 0x9e3779b9, c = 0; // hask key
        
    b += target3;
    c += target2;
    a += target1;        
    MIX(a, b, c);
    return c;
}

#undef MIX

class FlowletInfo {
public:
    uint32_t _egress;
    simtime_picosec _last;

    FlowletInfo(uint32_t egress,simtime_picosec lasttime) {_egress = egress; _last = lasttime;};

};

class FatTreeSwitch : public Switch, public SharedBufferSwitch {
public:
    static uint64_t _shared_buffer_bytes;
    static double _shared_ingress_alpha;
    static double _shared_egress_alpha;
    static uint64_t _shared_headroom_bytes;
    static uint64_t _shared_buffer_overflows_total;
    static uint64_t _shared_buffer_peak_bytes;
    void shared_register_ingress(const void* queue);
    void shared_register_egress(const void* queue);
    Admission shared_admit(const void* ingress, uint32_t pg,
                            const void* egress, uint32_t queue, uint64_t bytes);
    void shared_release(const void* ingress, uint32_t pg,
                        const void* egress, uint32_t queue, uint64_t bytes);
    bool shared_should_pause(const void* ingress, uint32_t pg) const;
    bool shared_should_resume(const void* ingress, uint32_t pg) const;
    uint64_t shared_pause_threshold(uint32_t pg) const;
    bool shared_should_mark_ecn(const void* egress, uint32_t queue,
                                uint64_t kmin, uint64_t kmax,
                                double sample01) const;
    uint64_t shared_buffer_used() const;
    uint64_t shared_buffer_overflows() const { return _shared_buffer_overflows; }
    enum switch_type {
        NONE = 0, TOR = 1, AGG = 2, CORE = 3
    };

    enum routing_strategy {
        NIX = 0, ECMP = 1, ADAPTIVE_ROUTING = 2, ECMP_ADAPTIVE = 3, RR = 4, RR_ECMP = 5, SGLB = 6, DRILL = 7,
        PAPER_SGLB = 8
    };

    enum SglbEcnMode {
        SGLB_ECN_OFF = 0,
        SGLB_ECN_NEUTRAL = 1,
        SGLB_ECN_CLEAR = 2
    };

    struct SglbCandidateState {
        bool valid;
        uint64_t epoch;
        vector<uint32_t> members;
        std::map<uint32_t, simtime_picosec> excluded_at;

        SglbCandidateState() : valid(false), epoch(0) {}
    };

    static bool sglb_update_candidate_state(
        SglbCandidateState& state, const vector<uint32_t>& candidates,
        simtime_picosec now);
    static bool sglb_stale_excluded(
        const SglbCandidateState& state, uint64_t tx_epoch,
        uint32_t selected_path);
    static void set_sglb_ecn_mode(SglbEcnMode mode) {_sglb_ecn_mode = mode;}
    static SglbEcnMode sglb_ecn_mode() {return _sglb_ecn_mode;}

    enum sticky_choices {
        PER_PACKET = 0, PER_FLOWLET = 1
    };

    FatTreeSwitch(EventList& eventlist, string s, switch_type t, uint32_t id,simtime_picosec switch_delay, FatTreeTopology* ft);
  
    virtual void receivePacket(Packet& pkt);
    virtual Route* getNextHop(Packet& pkt, BaseQueue* ingress_port);
    virtual uint32_t getType() {return _type;}

    uint32_t adaptive_route(vector<FibEntry*>* ecmp_set, int8_t (*cmp)(FibEntry*,FibEntry*));
    uint32_t replace_worst_choice(vector<FibEntry*>* ecmp_set, int8_t (*cmp)(FibEntry*,FibEntry*),uint32_t my_choice);
    uint32_t adaptive_route_p2c(vector<FibEntry*>* ecmp_set, int8_t (*cmp)(FibEntry*,FibEntry*));
    uint32_t sglb_route(vector<FibEntry*>* ecmp_set, uint32_t dst,
                        vector<uint32_t>* candidates_out = NULL);
    uint32_t sglb_best_score(uint32_t dst, uint32_t depth);
    uint32_t drill_route(vector<FibEntry*>* ecmp_set, uint32_t dst);

    struct PaperSglbRemoteState {
        uint8_t level;
        uint64_t version;
        bool link_up;
        double remote_queue;
        double remote_utilization;
        double remote_busyness;
        simtime_picosec received_at;
        bool valid;

        PaperSglbRemoteState()
            : level(0), version(0), link_up(true), remote_queue(0.0),
              remote_utilization(0.0), remote_busyness(0.0),
              received_at(0), valid(false) {}
    };

    struct PaperSglbFactors {
        double local_queue;
        double local_utilization;
        double remote_queue;
        double remote_utilization;
        double remote_busyness;

        PaperSglbFactors()
            : local_queue(0.0), local_utilization(0.0),
              remote_queue(0.0), remote_utilization(0.0),
              remote_busyness(0.0) {}
    };

    static double paper_sglb_value(const PaperSglbFactors& factors);
    static uint8_t paper_sglb_ar_level(double value);
    static vector<uint32_t> paper_sglb_best_level(
        const vector<uint8_t>& levels, const vector<bool>& available,
        uint32_t min_choices);
    static vector<uint32_t> paper_sglb_exact_min_by_level(
        const vector<uint8_t>& levels, const vector<bool>& available,
        const vector<uint64_t>& tie_keys, uint32_t min_choices);
    static vector<uint32_t> paper_sglb_strict_k_by_level(
        const vector<uint8_t>& levels, const vector<bool>& available,
        const vector<uint64_t>& tie_keys, uint32_t choices);

    struct SglbShuffledRrState {
        vector<uint32_t> members;
        vector<uint32_t> order;
        size_t cursor;
        uint64_t generation;
        uint64_t quality_signature;
        bool valid;

        SglbShuffledRrState()
            : cursor(0), generation(0), quality_signature(0), valid(false) {}
    };

    static uint32_t sglb_shuffled_rr_select(
        const vector<uint32_t>& candidates, uint32_t switch_id,
        uint32_t dst_tor, uint64_t quality_signature,
        SglbShuffledRrState& state);
    static vector<uint32_t> paper_sglb_topk_by_level(
        const vector<uint8_t>& levels, const vector<bool>& available,
        const vector<uint64_t>& tie_keys, uint32_t k);
    static bool paper_sglb_snapshot_refresh_due(
        bool valid, simtime_picosec last_update, simtime_picosec now,
        simtime_picosec interval);
    static bool paper_sglb_emit_due(
        bool has_sent, simtime_picosec last_sent, simtime_picosec now,
        simtime_picosec interval);

    static double paper_sglb_noisy_or(double local, double remote);
    static double paper_sglb_queue_pressure(double queue_fraction);
    static uint8_t paper_sglb_quantized_level(double score, uint32_t levels);
    static vector<uint32_t> paper_sglb_strict_topk(
        const vector<uint8_t>& levels, const vector<double>& scores,
        const vector<bool>& available, const vector<uint64_t>& stable_keys,
        uint32_t k);
    static bool paper_sglb_accept_gcn(
        PaperSglbRemoteState& state, bool link_up, double remote_queue,
        double remote_utilization, double remote_busyness, uint8_t level,
        uint64_t version, simtime_picosec received_at);
    static void initialize_paper_sglb(FatTreeTopology* topology,
                                      simtime_picosec propagation_delay);
    void receive_paper_sglb_gcn(SglbGcnPacket& packet);
    uint32_t paper_sglb_route(vector<FibEntry*>* ecmp_set, Packet& pkt);

    enum PaperSglbSelectionMode {
        PAPER_SGLB_BEST_LEVEL = 0,
        PAPER_SGLB_TOPK = 1
    };

    enum PaperSglbRemoteMode {
        PAPER_SGLB_REMOTE_GCN_PROFILE = 0,
        PAPER_SGLB_REMOTE_DIRECT = 1
    };

    enum PaperSglbAblation {
        PAPER_SGLB_ABLATION_NONE = 0,
        PAPER_SGLB_ABLATION_LEGACY_CANDIDATES,
        PAPER_SGLB_ABLATION_LEGACY_NOISY_OR,
        PAPER_SGLB_ABLATION_LEGACY_QUEUE_PRESSURE,
        PAPER_SGLB_ABLATION_LEGACY_LEVELS,
        PAPER_SGLB_ABLATION_LEGACY_REMOTE_SEMANTICS,
        PAPER_SGLB_ABLATION_LEGACY_TRANSPORT,
        PAPER_SGLB_ABLATION_LAZY_INIT,
        PAPER_SGLB_ABLATION_NO_VERSION,
        PAPER_SGLB_ABLATION_ALL_SWITCH_DECISIONS,
        PAPER_SGLB_ABLATION_LEGACY_BACKGROUND,
        PAPER_SGLB_ABLATION_NO_CONTROL_BANDWIDTH
    };

    static const char* paper_sglb_remote_mode_name();
    static const char* paper_sglb_ablation_name();

    enum SglbScoreMode {
        SGLB_SCORE_LEGACY = 0,
        SGLB_SCORE_NMRC_QUANTIZED_TOPK = 1
    };

    enum SglbCandidatePolicy {
        SGLB_CANDIDATE_STRICT_K = 0,
        SGLB_CANDIDATE_WHOLE_GRADE_MIN = 1,
        SGLB_CANDIDATE_EXACT_MIN = 2,
        SGLB_CANDIDATE_SCORE_TOPK = 3
    };

    enum SglbCandidateDispatch {
        SGLB_DISPATCH_RANDOM = 0,
        SGLB_DISPATCH_SHUFFLED_RR = 1
    };

    enum SglbGcnCadence {
        SGLB_GCN_INDEPENDENT = 0,
        SGLB_GCN_SYNCHRONIZED = 1
    };

    enum SglbOfatFactor {
        SGLB_OFAT_BASELINE = 0,
        SGLB_OFAT_TOPK8,
        SGLB_OFAT_LINEAR_SCORE,
        SGLB_OFAT_RAW_QUEUE,
        SGLB_OFAT_PAPER_LEVELS,
        SGLB_OFAT_REMOTE_MEAN,
        SGLB_OFAT_CHANGE_TRIGGERED,
        SGLB_OFAT_DELAYED_MESSAGE,
        SGLB_OFAT_EAGER_INIT,
        SGLB_OFAT_VERSIONED,
        SGLB_OFAT_NO_AGING,
        SGLB_OFAT_SOURCE_LEAF_ONLY,
        SGLB_OFAT_BACKGROUND_SGLB,
        SGLB_OFAT_SHADOW_GCN,
        SGLB_OFAT_RAW_QUEUE_TOPK8,
        SGLB_OFAT_RAW_PAPER_LEVELS_TOPK8,
        SGLB_OFAT_RAW_LINEAR_PAPER_LEVELS_TOPK8,
        SGLB_OFAT_REAL_GCN_PROFILES,
        SGLB_OFAT_REAL_GCN_RAW_LINEAR
    };

    enum NmrcReroutePolicy {
        NMRC_REROUTE_ANY_BETTER = 0,
        NMRC_REROUTE_BETTER_GE3 = 1
    };

    enum NmrcNetworkDecisionMode {
        NMRC_NETWORK_GRADED = 0,
        NMRC_NETWORK_BINARY_SCORE = 1,
        NMRC_NETWORK_FIXED_THRESHOLD = 2,
        NMRC_NETWORK_PIECEWISE_DELTA = 3,
        NMRC_NETWORK_TWO_STAGE_DELTA = 4,
        NMRC_NETWORK_ABSOLUTE_REROUTE = 5,
        NMRC_NETWORK_DELTA = 6
    };

    enum NmrcRelativeDecisionReason {
        NMRC_RELATIVE_SELECTED = 0,
        NMRC_RELATIVE_INVALID_INPUT,
        NMRC_RELATIVE_ORIGINAL_UNKNOWN,
        NMRC_RELATIVE_ORIGINAL_UNAVAILABLE,
        NMRC_RELATIVE_ORIGINAL_BELOW_ABSOLUTE,
        NMRC_RELATIVE_NO_SAFE_CANDIDATE,
        NMRC_RELATIVE_NO_DELTA_CANDIDATE
    };

    enum NmrcBinaryClass {
        NMRC_BINARY_UNKNOWN = 0,
        NMRC_BINARY_SAFE = 1,
        NMRC_BINARY_CONGESTED = 2
    };

    enum NmrcGradedCooldownMode {
        NMRC_GRADED_COOLDOWN_SELECTIVE = 0,
        NMRC_GRADED_COOLDOWN_FULL = 1,
        NMRC_GRADED_COOLDOWN_NONE = 2
    };

    struct NmrcRerouteDecision {
        bool reroute;
        uint32_t selected_index;
        uint32_t better_count;
        uint32_t candidate_count;
        uint8_t original_level;
        uint8_t selected_level;

        NmrcRerouteDecision()
            : reroute(false), selected_index(UINT32_MAX), better_count(0),
              candidate_count(0), original_level(STOR_LEVEL_GOOD),
              selected_level(STOR_LEVEL_GOOD) {}
    };

    struct NmrcRelativeDecision {
        bool reroute;
        uint32_t selected_index;
        uint32_t candidate_count;
        double original_score;
        double selected_score;
        double best_gap;
        double selected_gap;
        bool request_cooldown;
        NmrcRelativeDecisionReason reason;

        NmrcRelativeDecision()
            : reroute(false), selected_index(UINT32_MAX), candidate_count(0),
              original_score(std::numeric_limits<double>::quiet_NaN()),
              selected_score(std::numeric_limits<double>::quiet_NaN()),
              best_gap(0.0), selected_gap(0.0), request_cooldown(true),
              reason(NMRC_RELATIVE_INVALID_INPUT) {}
    };

    static NmrcRerouteDecision nmrc_select_better_path(
        uint32_t original_index,
        const vector<uint8_t>& levels,
        const vector<bool>& available,
        NmrcReroutePolicy policy,
        uint32_t min_choices,
        uint32_t selection_value);
    static NmrcRerouteDecision nmrc_select_graded_delta_path(
        uint32_t original_index,
        const vector<uint8_t>& levels,
        const vector<double>& scores,
        const vector<bool>& available,
        NmrcReroutePolicy policy,
        uint32_t min_choices,
        uint32_t selection_value,
        double delta);
    static bool nmrc_graded_requests_cooldown(
        double original_score, double selected_score, double threshold);
    static bool nmrc_graded_cooldown_requested(
        NmrcGradedCooldownMode mode,
        double original_score, double selected_score, double threshold);

    static bool nmrc_binary_safe(double score, bool two_hop_valid);
    static NmrcBinaryClass nmrc_binary_classify(double score,
                                                 bool two_hop_valid);
    static NmrcRerouteDecision nmrc_select_binary_path(
        uint32_t original_index,
        const vector<double>& scores,
        const vector<bool>& two_hop_valid,
        const vector<bool>& available,
        uint32_t selection_value);
    static NmrcRelativeDecision nmrc_select_fixed_threshold_path(
        uint32_t original_index,
        const vector<double>& scores,
        const vector<bool>& two_hop_valid,
        const vector<bool>& available,
        double absolute_threshold,
        double delta,
        uint32_t selection_value);
    static NmrcRelativeDecision nmrc_select_delta_path(
        uint32_t original_index,
        const vector<double>& scores,
        const vector<bool>& two_hop_valid,
        const vector<bool>& available,
        double delta,
        uint32_t selection_value);
    static NmrcRelativeDecision nmrc_select_piecewise_delta_path(
        uint32_t original_index,
        const vector<double>& scores,
        const vector<bool>& two_hop_valid,
        const vector<bool>& available,
        double breakpoint,
        double delta_below,
        double delta_above,
        uint32_t selection_value);
    static NmrcRelativeDecision nmrc_select_two_stage_delta_path(
        uint32_t original_index,
        const vector<double>& scores,
        const vector<bool>& two_hop_valid,
        const vector<bool>& available,
        double route_delta,
        double cooldown_delta,
        uint32_t selection_value);
    static NmrcRelativeDecision nmrc_select_absolute_reroute_path(
        uint32_t original_index,
        const vector<double>& scores,
        const vector<bool>& two_hop_valid,
        const vector<bool>& available,
        double absolute_threshold,
        double cooldown_delta,
        uint32_t selection_value);
    static uint64_t nmrc_relative_action_key(
        uint32_t flow_id, RocePacket::seq_t psn, uint8_t attempt,
        uint32_t original_ev, uint32_t original_egress,
        uint32_t selected_egress);
    static uint32_t nmrc_relative_hist_bin(double value);

    struct SglbPathState {
        double score;
        double best_score;
        double avg_busy;
        double queue_fraction;
        double queue_pressure;
        uint32_t candidate_count;
        uint8_t quality;
        uint64_t version;
        simtime_picosec last_update;
        bool valid;
        bool link_available;

        SglbPathState()
            : score(0.0), best_score(0.0), avg_busy(0.0),
              queue_fraction(0.0), queue_pressure(0.0), candidate_count(0),
              quality(0), version(0), last_update(0),
              valid(false), link_available(true) {}
    };

    struct SglbGcnProducerState {
        SglbPathState current;
        SglbPathState advertised;
        simtime_picosec last_sample;
        simtime_picosec last_sent;
        uint64_t version;
        bool valid;
        bool dirty;
        bool has_sent;
        bool timer_pending;

        SglbGcnProducerState()
            : last_sample(0), last_sent(0), version(0), valid(false),
              dirty(false), has_sent(false), timer_pending(false) {}
    };

    static bool sglb_gcn_export_changed(
        const SglbPathState& advertised,
        const SglbPathState& observed);
    static bool sglb_gcn_observe_export(
        SglbGcnProducerState& producer,
        const SglbPathState& observed,
        simtime_picosec now,
        simtime_picosec sample_interval);
    static void sglb_gcn_mark_advertised(
        SglbGcnProducerState& producer,
        simtime_picosec now);

    struct SglbQualitySnapshot {
        double score;
        uint8_t quality;
        simtime_picosec last_update;
        simtime_picosec downstream_last_update;
        bool valid;
        bool local_valid;
        bool downstream_valid;

        SglbQualitySnapshot()
            : score(0.0), quality(0), last_update(0),
              downstream_last_update(0), valid(false),
              local_valid(false), downstream_valid(false) {}

        bool two_hop_valid() const {
            return local_valid && downstream_valid;
        }
    };

    enum StorSignal {
        STOR_SIGNAL_CLEAN = 0,
        STOR_SIGNAL_ECN = 1,
        STOR_SIGNAL_TRIM = 2
    };

    enum StorScoreProfile {
        STOR_SCORE_PROFILE_ORIGINAL = 0,
        STOR_SCORE_PROFILE_BALANCED = 1,
        STOR_SCORE_PROFILE_CUSTOM = 2,
        STOR_SCORE_PROFILE_SIMPLE = 3,
        STOR_SCORE_PROFILE_BINARY = 4
    };

    enum StorAgingProfile {
        STOR_AGING_PACKET = 0,
        STOR_AGING_TIME_EWMA = 1,
        STOR_AGING_HYBRID = 2
    };

    enum NetawareScoreMode {
        NETAWARE_SCORE_GATED = 0,
        NETAWARE_SCORE_WORST_HOP = 1,
        NETAWARE_SCORE_SGLB_QUANTIZED = 2
    };

    enum NetawarePathCoupling {
        NETAWARE_PATH_COUPLING_ADDITIVE = 0,
        NETAWARE_PATH_COUPLING_BOTTLENECK = 1,
        NETAWARE_PATH_COUPLING_NOISY_OR = 2
    };

    struct NetawarePathScore {
        double local_q_pressure;
        double remote_q_pressure;
        double local_util_pressure;
        double remote_util_pressure;
        double path_score;
        uint8_t level;
        const char* reason;

        NetawarePathScore()
            : local_q_pressure(0.0), remote_q_pressure(0.0),
              local_util_pressure(0.0), remote_util_pressure(0.0),
              path_score(0.0), level(STOR_LEVEL_GOOD), reason("good") {}
    };

    struct NetawarePortSnapshot {
        double queue_fraction;
        double utilization_fraction;
        linkspeed_bps bitrate;
        simtime_picosec last_update;
        uint64_t bytes_sent;
        double dre_pressure;
        bool paused;
        bool valid;

        NetawarePortSnapshot()
            : queue_fraction(0.0), utilization_fraction(0.0), bitrate(0),
              last_update(0), bytes_sent(0), dre_pressure(0.0),
              paused(false), valid(false) {}
    };

    struct StorEvState {
        uint8_t score;
        uint8_t ecn_acc;
        uint8_t trim_acc;
        simtime_picosec last_update;
        simtime_picosec last_bad_time;
        simtime_picosec last_probe_time;
        uint64_t last_probe_packet;
        uint8_t probe_clean_streak;
        uint8_t min_score_seen;
        uint8_t last_level;
        bool has_bad_time;
        uint64_t clean_signals;
        uint64_t ecn_signals;
        uint64_t trim_signals;
        uint64_t avoid_entries;
        uint64_t avoid_exits;

        StorEvState()
            : score(255), ecn_acc(0), trim_acc(0), last_update(0),
              last_bad_time(0), last_probe_time(0), last_probe_packet(0),
              probe_clean_streak(0), min_score_seen(255),
              last_level(STOR_LEVEL_GOOD), has_bad_time(false),
              clean_signals(0), ecn_signals(0),
              trim_signals(0), avoid_entries(0), avoid_exits(0) {}
    };

    static uint8_t stor_level_from_score(uint8_t score);
    static void stor_apply_signal(StorEvState& state, StorSignal signal);
    static void set_stor_score_profile(StorScoreProfile profile);
    static const char* stor_score_profile_name();
    static void set_stor_aging_profile(StorAgingProfile profile);
    static const char* stor_aging_profile_name();
    StorFeedbackLevels stor_feedback_after_signal(uint32_t src_host,
                                                  uint32_t peer_host,
                                                  uint32_t pathid,
                                                  uint32_t path_count,
                                                  StorSignal signal);
    void collect_stor_diag(uint8_t& min_score,
                           uint64_t& avoid_entries,
                           uint64_t& avoid_exits,
                           uint64_t& clean_signals,
                           uint64_t& ecn_signals,
                           uint64_t& trim_signals) const;
    void collect_netaware_diag(uint64_t& samples,
                           uint64_t& sample_zero_bits,
                           uint64_t& feedbacks,
                           uint64_t& packet_feedbacks,
                           uint64_t& time_feedbacks,
                           uint64_t& feedback_packets_sum,
                           uint64_t& feedback_zero_bits,
                           uint64_t& all_good_feedbacks) const;
    struct NetawarePathTraceSample {
        uint32_t spine_id;
        mem_b q_leaf_to_spine;
        mem_b q_spine_to_dst_leaf;
        mem_b q_leaf_to_spine_max;
        mem_b q_spine_to_dst_leaf_max;
        double util_leaf_to_spine;
        double util_spine_to_dst_leaf;
        linkspeed_bps link_rate_leaf_to_spine;
        linkspeed_bps link_rate_spine_to_dst_leaf;
        bool link_down;
        uint64_t ecn_marks_on_path;
        uint64_t trimmed_packets_on_path;
        uint64_t dropped_packets_on_path;
        uint64_t bytes_sent_on_path;
        double local_q_pressure;
        double remote_q_pressure;
        double local_util_pressure;
        double remote_util_pressure;
        double path_score;
        const char* netaware_level_reason;
        uint8_t path_grade;

        NetawarePathTraceSample()
            : spine_id(UINT32_MAX), q_leaf_to_spine(0),
              q_spine_to_dst_leaf(0), q_leaf_to_spine_max(0),
              q_spine_to_dst_leaf_max(0), util_leaf_to_spine(0.0),
              util_spine_to_dst_leaf(0.0), link_rate_leaf_to_spine(0),
              link_rate_spine_to_dst_leaf(0), link_down(true),
              ecn_marks_on_path(0), trimmed_packets_on_path(0),
              dropped_packets_on_path(0), bytes_sent_on_path(0),
              local_q_pressure(0.0), remote_q_pressure(0.0),
              local_util_pressure(0.0), remote_util_pressure(0.0),
              path_score(0.0), netaware_level_reason("missing_queue"),
              path_grade(STOR_LEVEL_GOOD) {}
    };
    bool netaware_trace_path_state(uint32_t dst,
                               uint32_t ev,
                               uint32_t path_count,
                               NetawarePathTraceSample& sample);
    const StorEvState* stor_state_for_test(uint32_t peer_host,
                                           uint32_t pathid) const;
    void stor_apply_time_aging_for_test(uint32_t peer_host,
                                        simtime_picosec now);

    static uint8_t sglb_quality_from_score(double score, double bucket, uint32_t levels) {
        if (levels == 0)
            levels = 1;
        if (bucket <= 0.0)
            bucket = 1.0;
        if (score <= 0.0)
            return 0;

        uint32_t max_quality = levels - 1;
        if (max_quality == 0)
            return 0;
        if (score >= bucket * (double)max_quality)
            return (uint8_t)max_quality;

        uint32_t quality = (uint32_t)(score / bucket);
        return (uint8_t)quality;
    }

    static double sglb_downstream_scale_for_score(double local_score,
                                                 double bucket,
                                                 bool normalized) {
        if (local_score <= 0.0)
            return 1.0;
        if (bucket <= 0.0)
            bucket = 1.0;

        double pressure = normalized ? local_score : local_score / bucket;
        if (pressure <= 0.0)
            return 1.0;
        return 1.0 / (1.0 + pressure);
    }

    static double sglb_nmrc_queue_pressure(double queue_fraction);
    static double sglb_nmrc_noisy_or(double local, double remote);
    static double sglb_nmrc_queue_signal(double queue_fraction);
    static double sglb_nmrc_couple(double local, double remote);
    static uint8_t sglb_nmrc_level(double score);
    static uint8_t sglb_nmrc_quantized_level(double score, uint32_t levels);
    static const char* sglb_score_mode_name();
    static const char* sglb_ofat_factor_name();
    static void configure_sglb_scheme_defaults(bool legacy);
    static bool sglb_ofat_uses_topk8();
    static bool sglb_ofat_uses_raw_queue();
    static bool sglb_ofat_uses_linear_score();
    static bool sglb_ofat_uses_paper_levels();
    static bool sglb_ofat_uses_real_gcn_profiles();
    static bool sglb_accept_real_gcn_profile(
        SglbPathState& state, const SglbGcnRecord& record,
        uint64_t version, simtime_picosec received_at);
    static void initialize_sglb_ofat(FatTreeTopology* topology);
    static void initialize_sglb_real_gcn_profiles(
        FatTreeTopology* topology);

    static const char* netaware_score_mode_name();
    static const char* netaware_path_coupling_name();
    static uint32_t netaware_default_feedback_pkts(uint32_t path_count);
    static double netaware_default_feedback_min_us(uint32_t path_count);
    static double netaware_default_feedback_max_us(uint32_t path_count);
    static uint32_t avail_default_feedback_pkts(uint32_t path_count);
    static uint32_t grade_default_feedback_pkts(uint32_t path_count);
    static bool netaware_snapshot_refresh_due(const NetawarePortSnapshot& snapshot,
                                          simtime_picosec now,
                                          simtime_picosec interval) {
        if (!snapshot.valid || interval == 0)
            return true;
        if (now < snapshot.last_update)
            return true;
        return now - snapshot.last_update >= interval;
    }
    static double netaware_pressure_from_range(double value, double low, double high);
    static double netaware_composite_score(double local_q_pressure,
                                       double remote_q_pressure,
                                       double local_util_pressure,
                                       double remote_util_pressure);
    static double netaware_couple_hop_scores(double local_q_pressure,
                                         double remote_q_pressure,
                                         double local_util_pressure,
                                         double remote_util_pressure);
    static uint8_t netaware_level_from_score(double score);
    static uint8_t netaware_gated_level_from_inputs(bool paused,
                                                double rate_ratio,
                                                double queue_fraction,
                                                double util_fraction,
                                                bool grade_queues);

    static bool sglb_snapshot_usable(const SglbPathState& state,
                                    simtime_picosec now,
                                    simtime_picosec aging_interval) {
        if (!state.valid || !state.link_available)
            return false;
        if (aging_interval == 0)
            return true;
        if (now < state.last_update)
            return false;
        return now - state.last_update <= aging_interval;
    }

    void sglb_mark_neighbor_link(uint32_t neighbor_id, bool available);

    static int8_t compare_flow_count(FibEntry* l, FibEntry* r);
    static int8_t compare_pause(FibEntry* l, FibEntry* r);
    static int8_t compare_bandwidth(FibEntry* l, FibEntry* r);
    static int8_t compare_queuesize(FibEntry* l, FibEntry* r);
    static int8_t compare_pqb(FibEntry* l, FibEntry* r);//compare pause,queue, bw.
    static int8_t compare_pq(FibEntry* l, FibEntry* r);//compare pause, queue
    static int8_t compare_pb(FibEntry* l, FibEntry* r);//compare pause, bandwidth
    static int8_t compare_qb(FibEntry* l, FibEntry* r);//compare pause, bandwidth

    static int8_t (*fn)(FibEntry*,FibEntry*);

    virtual void addHostPort(int addr, int flowid, PacketSink* transport);

    virtual void permute_paths(vector<FibEntry*>* uproutes);

    static void set_strategy(routing_strategy s) { assert (_strategy==NIX); _strategy = s; }
    static void set_ar_fraction(uint16_t f) { assert(f>=1);_ar_fraction = f;} 

    static routing_strategy _strategy;
    static uint16_t _ar_fraction;
    static uint16_t _ar_sticky;
    static simtime_picosec _sticky_delta;
    static double _ecn_threshold_fraction;
    // Optional absolute ECN thresholds for lossless_input_ecn queues.
    // Zero keeps the historical 20%/80% defaults.
    static uint64_t _lossless_ecn_kmin_bytes;
    static uint64_t _lossless_ecn_kmax_bytes;
    static double _speculative_threshold_fraction;
    static double _sglb_downstream_weight;
    static double _sglb_queue_weight;
    static double _sglb_util_weight;
    static double _sglb_remote_queue_weight;
    static double _sglb_remote_util_weight;
    static double _sglb_remote_busy_weight;
    static double _sglb_quality_bucket;
    static uint32_t _sglb_max_quality;
    static simtime_picosec _sglb_update_interval;
    static uint32_t _sglb_quality_levels;
    static uint32_t _sglb_min_choices;
    static uint32_t _sglb_topk;
    static SglbCandidatePolicy _sglb_candidate_policy;
    static SglbCandidateDispatch _sglb_candidate_dispatch;
    static SglbGcnCadence _sglb_gcn_cadence;
    static simtime_picosec _sglb_gcn_update_interval;
    static simtime_picosec _sglb_gcn_aging_interval;
    static simtime_picosec _paper_sglb_sample_interval;
    static simtime_picosec _paper_sglb_gcn_interval;
    static simtime_picosec _paper_sglb_gcn_delay;
    static double _paper_sglb_weight_local_queue;
    static double _paper_sglb_weight_local_util;
    static double _paper_sglb_weight_remote_queue;
    static double _paper_sglb_weight_remote_util;
    static double _paper_sglb_weight_remote_busy;
    static PaperSglbSelectionMode _paper_sglb_selection_mode;
    static PaperSglbRemoteMode _paper_sglb_remote_mode;
    static PaperSglbAblation _paper_sglb_ablation;
    static uint32_t _paper_sglb_levels;
    static uint32_t _paper_sglb_k;
    static double _paper_sglb_q_low;
    static double _paper_sglb_q_high;
    static uint64_t _paper_sglb_diag_route_calls;
    static uint64_t _paper_sglb_diag_candidate_sum;
    static uint64_t _paper_sglb_diag_remote_missing;
    static uint64_t _paper_sglb_diag_gcn_updates;
    static uint64_t _paper_sglb_diag_gcn_deliveries;
    static uint64_t _paper_sglb_diag_gcn_packets;
    static uint64_t _paper_sglb_diag_gcn_bytes;
    static uint64_t _paper_sglb_diag_gcn_stale;
    static uint64_t _paper_sglb_diag_gcn_profile_updates;
    static bool _sglb_normalize_scores;
    static bool _sglb_local_damping;
    static SglbScoreMode _sglb_score_mode;
    static SglbOfatFactor _sglb_ofat_factor;
    static simtime_picosec _sglb_ofat_message_delay;
    static double _sglb_nmrc_q_min;
    static double _sglb_nmrc_q_max;
    static double _sglb_nmrc_degraded_threshold;
    static double _sglb_nmrc_bad_threshold;
    static double _sglb_nmrc_avoid_threshold;
    static uint32_t _sglb_nmrc_levels;
    static uint64_t _sglb_diag_route_calls;
    static uint64_t _sglb_diag_available_choices;
    static uint64_t _sglb_diag_candidate_choices;
    static uint64_t _sglb_diag_best_quality_choices;
    static uint64_t _sglb_diag_distinct_qualities;
    static uint64_t _sglb_diag_all_same_quality_calls;
    static uint64_t _sglb_diag_all_zero_quality_calls;
    static uint64_t _sglb_diag_selected_nonbest_quality;
    static uint64_t _sglb_diag_remote_snapshot_used;
    static uint64_t _sglb_diag_remote_snapshot_missing;
    static uint64_t _sglb_diag_observed_levels[4];
    static uint64_t _sglb_diag_selected_levels[4];
    static double _sglb_diag_score_spread_sum;
    static void reset_sglb_route_diag();
    static bool _nmrc_hybrid_enabled;
    static bool _nmrc_fastcnp_enabled;
    static NmrcReroutePolicy _nmrc_reroute_policy;
    static NmrcNetworkDecisionMode _nmrc_network_decision_mode;
    static NmrcGradedCooldownMode _nmrc_graded_cooldown_mode;
    static double _nmrc_graded_reroute_delta;
    static double _nmrc_graded_cooldown_delta;
    static double _nmrc_absolute_threshold;
    static double _nmrc_relative_delta;
    static double _nmrc_piecewise_delta_below;
    static double _nmrc_piecewise_delta_above;
    static double _nmrc_route_delta;
    static double _nmrc_cooldown_delta;
    static const double NMRC_RELATIVE_EPSILON;
    static uint64_t _nmrc_diag_route_checks;
    static uint64_t _nmrc_diag_reroutes;
    static uint64_t _nmrc_diag_threshold_blocked;
    static uint64_t _nmrc_diag_fastcnp_generated;
    static uint64_t _nmrc_diag_fastcnp_route_missing;
    static uint64_t _nmrc_diag_graded_cooldown_requested;
    static uint64_t _nmrc_diag_graded_cooldown_suppressed;
    static uint64_t _nmrc_diag_better_count[33];
    static uint64_t _nmrc_diag_level_transitions[4][4];
    static uint64_t _nmrc_diag_binary_original_safe;
    static uint64_t _nmrc_diag_binary_original_congested;
    static uint64_t _nmrc_diag_binary_no_safe;
    static uint64_t _nmrc_diag_binary_route_missing;
    static uint64_t _nmrc_diag_binary_paired_actions;
    static double _nmrc_diag_binary_original_score_sum;
    static double _nmrc_diag_binary_original_score_max;
    static double _nmrc_diag_binary_selected_score_sum;
    static double _nmrc_diag_binary_selected_score_max;
    static uint64_t _nmrc_diag_binary_actual_egress[33];
    static uint64_t _nmrc_diag_relative_checks;
    static uint64_t _nmrc_diag_relative_original_unknown;
    static uint64_t _nmrc_diag_relative_original_unavailable;
    static uint64_t _nmrc_diag_relative_original_below_absolute;
    static uint64_t _nmrc_diag_relative_no_safe_candidate;
    static uint64_t _nmrc_diag_relative_no_delta_candidate;
    static uint64_t _nmrc_diag_relative_reverse_path_blocked;
    static uint64_t _nmrc_diag_relative_paired_actions;
    static uint64_t _nmrc_diag_relative_reroutes;
    static uint64_t _nmrc_diag_two_stage_reroute_only;
    static uint64_t _nmrc_diag_two_stage_cooldown_requested;
    static uint64_t _nmrc_diag_relative_selected_gap_violations;
    static uint64_t _nmrc_diag_relative_decision_ce_set;
    static uint64_t _nmrc_diag_relative_decision_ce_cleared;
    static uint64_t _nmrc_diag_relative_reroute_key_count;
    static uint64_t _nmrc_diag_relative_reroute_key_sum;
    static uint64_t _nmrc_diag_relative_reroute_key_xor;
    static uint64_t _nmrc_diag_relative_generated_key_count;
    static uint64_t _nmrc_diag_relative_generated_key_sum;
    static uint64_t _nmrc_diag_relative_generated_key_xor;
    static uint64_t _nmrc_diag_relative_original_score_count;
    static double _nmrc_diag_relative_original_score_sum;
    static double _nmrc_diag_relative_original_score_max;
    static uint64_t _nmrc_diag_relative_selected_score_count;
    static double _nmrc_diag_relative_selected_score_sum;
    static double _nmrc_diag_relative_selected_score_max;
    static uint64_t _nmrc_diag_relative_candidate_count[33];
    static uint64_t _nmrc_diag_relative_best_gap[21];
    static uint64_t _nmrc_diag_relative_selected_gap[21];
    static uint64_t _nmrc_diag_relative_original_score[21];
    static uint64_t _nmrc_diag_relative_selected_score[21];
    static uint64_t _nmrc_diag_relative_actual_egress[33];
    static std::set<uint64_t> _nmrc_diag_observed_flow_evs;
    static std::set<uint64_t> _nmrc_diag_observed_flow_paths;
    static void reset_nmrc_hybrid_diag();
    static uint64_t nmrc_diag_observed_evs() {
        return _nmrc_diag_observed_flow_evs.size();
    }
    static uint64_t nmrc_diag_observed_paths() {
        return _nmrc_diag_observed_flow_paths.size();
    }
    static bool _netaware_enabled;
    static uint32_t _netaware_path_count;
    static uint32_t _netaware_feedback_pkts;
    static simtime_picosec _netaware_feedback_min_interval;
    static simtime_picosec _netaware_feedback_max_interval;
    static double _netaware_queue_threshold_fraction;
    static double _netaware_degraded_queue_fraction;
    static double _netaware_bad_queue_fraction;
    static double _netaware_degraded_utilization_fraction;
    static double _netaware_util_queue_floor_fraction;
    static double _netaware_slow_link_fraction;
    static simtime_picosec _netaware_state_update_interval;
    static simtime_picosec _netaware_remote_update_interval;
    static NetawareScoreMode _netaware_score_mode;
    static NetawarePathCoupling _netaware_path_coupling;
    static double _netaware_score_q_min;
    static double _netaware_score_q_max;
    static double _netaware_score_util_low;
    static double _netaware_score_util_high;
    static double _netaware_score_weight_local_q;
    static double _netaware_score_weight_remote_q;
    static double _netaware_score_weight_local_util;
    static double _netaware_score_weight_remote_util;
    static double _netaware_score_degraded_threshold;
    static double _netaware_score_bad_threshold;
    static double _netaware_score_avoid_threshold;
    static bool _stor_enabled;
    static uint32_t _stor_path_count;
    static uint32_t _stor_feedback_pkts;
    static simtime_picosec _stor_feedback_min_interval;
    static simtime_picosec _stor_feedback_max_interval;
    static simtime_picosec _stor_trim_feedback_min_interval;
    static bool _stor_feedback_on_trim;
    static bool _stor_binary_trim_bad;
    static uint8_t _stor_clean_gain;
    static uint8_t _stor_ecn_acc_add;
    static uint8_t _stor_trim_acc_add;
    static uint8_t _stor_ecn_base_penalty;
    static uint8_t _stor_trim_base_penalty;
    static uint8_t _stor_ecn_decay_shift;
    static uint8_t _stor_trim_decay_shift;
    static uint8_t _stor_ecn_penalty_shift;
    static uint8_t _stor_trim_penalty_shift;
    static uint8_t _stor_good_threshold;
    static uint8_t _stor_degraded_threshold;
    static uint8_t _stor_bad_threshold;
    static uint8_t _stor_simple_max_score;
    static uint8_t _stor_simple_clean_gain;
    static uint8_t _stor_simple_congestion_penalty;
    static StorScoreProfile _stor_score_profile;
    static StorAgingProfile _stor_aging_profile;
    static simtime_picosec _stor_time_ecn_tau;
    static simtime_picosec _stor_time_trim_tau;
    static simtime_picosec _stor_time_score_tau;
    static simtime_picosec _stor_hybrid_bad_hold;
    static simtime_picosec _stor_hybrid_avoid_hold;
    static uint32_t _stor_hybrid_probe_interval_pkts;
    static uint32_t _stor_hybrid_probe_clean_promote;
    static bool _pathid_only_hash;
    static SglbEcnMode _sglb_ecn_mode;
    static uint64_t _sglb_ecn_total;
    static uint64_t _sglb_ecn_stale;
    static uint64_t _sglb_ecn_neutralized;
    static uint64_t _sglb_ecn_cleared;
    static uint64_t _sglb_ecn_missing_metadata;
private:
    std::map<uint32_t, SglbCandidateState> _sglb_candidate_states;
    struct NetawareState {
        StorFeedbackLevels levels;
        bool levels_valid;
        uint32_t packets;
        simtime_picosec last_level_update;
        simtime_picosec last_feedback;
        uint64_t samples;
        uint64_t sample_zero_bits;
        uint64_t feedbacks;
        uint64_t packet_feedbacks;
        uint64_t time_feedbacks;
        uint64_t feedback_packets_sum;
        uint64_t feedback_zero_bits;
        uint64_t all_good_feedbacks;

        NetawareState()
            : levels_valid(false), packets(0), last_level_update(0),
              last_feedback(0), samples(0),
              sample_zero_bits(0), feedbacks(0), packet_feedbacks(0),
              time_feedbacks(0), feedback_packets_sum(0),
              feedback_zero_bits(0), all_good_feedbacks(0) {}
    };

    struct NetawareExportState {
        std::vector<NetawarePortSnapshot> ports;
        simtime_picosec last_update;
        bool valid;

        NetawareExportState() : last_update(0), valid(false) {}
    };

    struct StorState {
        std::vector<StorEvState> evs;
        uint32_t packets;
        simtime_picosec last_feedback;
        uint64_t signals_seen;
        bool dirty;

        StorState() : packets(0), last_feedback(0), signals_seen(0), dirty(false) {}
    };

    switch_type _type;
    BroadcomMmu* _shared_mmu;
    unordered_map<const void*,uint32_t> _shared_ingress_ports;
    unordered_map<const void*,uint32_t> _shared_egress_ports;
    uint64_t _shared_buffer_overflows;
    Pipe* _pipe;
    FatTreeTopology* _ft;
    
    //CAREFUL: can't always have a single FIB for all up destinations when there are failures!
    vector<FibEntry*>* _uproutes;

    unordered_map<uint32_t,FlowletInfo*> _flowlet_maps;
    // CONGA flowlets are source-leaf state. They must be created when a
    // packet reaches the leaf, after host-link serialization, rather than at
    // endpoint packet creation time.
    unordered_map<uint32_t,FlowletInfo*> _conga_flowlet_maps;
    unordered_map<uint32_t,NetawareState> _netaware_states;
    unordered_map<uint32_t,StorState> _stor_states;
    unordered_map<uint32_t,uint32_t> _drill_memory;
    unordered_map<uint32_t,SglbPathState> _sglb_exported_state;
    unordered_map<uint32_t,SglbPathState> _sglb_previous_exported_state;
    unordered_map<uint32_t,unordered_map<FibEntry*,SglbQualitySnapshot> > _sglb_quality_table;
    unordered_map<uint32_t,SglbShuffledRrState> _sglb_shuffled_rr_states;
    unordered_map<uint64_t,SglbPathState> _sglb_received_profiles;
    unordered_map<uint32_t,bool> _sglb_neighbor_available;
    unordered_map<uint64_t,PaperSglbRemoteState> _paper_sglb_remote;
    struct PaperSglbLocalState {
        double queue;
        double utilization;
        simtime_picosec last_update;
        bool valid;
        PaperSglbLocalState()
            : queue(0.0), utilization(0.0), last_update(0), valid(false) {}
    };
    struct PaperSglbProducerState {
        PaperSglbFactors current;
        PaperSglbFactors advertised;
        simtime_picosec last_sample;
        bool valid;
        PaperSglbProducerState()
            : last_sample(0), valid(false) {}
    };
    unordered_map<BaseQueue*,PaperSglbLocalState> _paper_sglb_local;
    unordered_map<uint32_t,PaperSglbProducerState> _paper_sglb_producers;
    unordered_map<uint32_t,vector<Route*> > _paper_sglb_gcn_routes;
    unordered_map<uint32_t,SglbGcnProducerState> _sglb_gcn_producers;
    unordered_map<uint32_t,SglbRealGcnTimer*> _sglb_real_gcn_timers;
    PacketFlow* _paper_sglb_gcn_flow;
    simtime_picosec _paper_sglb_gcn_last_sent;
    uint64_t _paper_sglb_gcn_version;
    bool _paper_sglb_gcn_dirty;
    bool _paper_sglb_gcn_has_sent;
    SglbGcnTimer* _sglb_gcn_timer;
    bool _sglb_gcn_timer_pending;
    unordered_map<BaseQueue*,NetawarePortSnapshot> _netaware_local_state;
    struct CongaDreState {
        uint64_t bytes;
        simtime_picosec last_decay;
        CongaDreState() : bytes(0), last_decay(0) {}
    };
    unordered_map<BaseQueue*,CongaDreState> _conga_dre;
    struct CongaPathInfo {
        uint8_t metric;
        simtime_picosec updated;
        bool dirty;
        CongaPathInfo() : metric(0), updated(0), dirty(false) {}
        CongaPathInfo(uint8_t value, simtime_picosec when, bool changed = false)
            : metric(value), updated(when), dirty(changed) {}
    };
    // CONGA paper tables, local to each ToR. FromLeaf contains congestion
    // learned from arriving data; ToLeaf contains reverse-data feedback used
    // to choose a path to a destination leaf.
    unordered_map<uint32_t, unordered_map<uint32_t,CongaPathInfo> >
        _conga_from_leaf;
    unordered_map<uint32_t, unordered_map<uint32_t,CongaPathInfo> >
        _conga_to_leaf;
    unordered_map<uint32_t,uint32_t> _conga_feedback_cursor;
    unordered_map<uint32_t,NetawareExportState> _netaware_exported_state;
    NetawareExportTimer* _netaware_export_timer;
    bool _netaware_export_timer_pending;

    static unordered_map<BaseQueue*,uint32_t> _port_flow_counts;

    uint32_t _crt_route;
    uint32_t _hash_salt;
    simtime_picosec _last_choice;

    unordered_map<Packet*,bool> _packets;

    void sglb_candidate_queues(uint32_t dst, vector<BaseQueue*>& queues);
    void paper_sglb_observe_remote_on_lookup(uint32_t destination_tor);
    void paper_sglb_emit_if_due();
    uint32_t sglb_queue_kbytes(BaseQueue* q);
    double sglb_queue_fraction(BaseQueue* q);
    uint32_t sglb_utilization_percent(BaseQueue* q);
    double sglb_port_score(BaseQueue* q, double queue_weight, double util_weight);
    SglbPathState sglb_compute_export_state(uint32_t dst);
    void sglb_maybe_refresh_export(uint32_t dst);
    bool sglb_uses_leaf_profiles() const;
    uint32_t sglb_profile_destination(uint32_t dst) const;
    uint32_t sglb_profile_observation_destination(uint32_t profile) const;
    void sglb_schedule_periodic_gcn();
    void sglb_periodic_refresh_exports();
    void sglb_observe_real_gcn_export(uint32_t destination);
    void sglb_schedule_real_gcn(uint32_t destination,
                                simtime_picosec delay);
    void sglb_real_gcn_timer_fired(uint32_t destination);
    void sglb_emit_real_gcn(uint32_t destination);
    void receive_sglb_real_gcn(SglbGcnPacket& packet);
    const SglbPathState* sglb_neighbor_snapshot(FibEntry* entry, uint32_t dst) const;
    uint32_t sglb_next_hop_id(FibEntry* entry) const;
    bool sglb_entry_available(FibEntry* entry) const;
    double sglb_compute_score(FibEntry* entry, uint32_t dst, uint32_t depth);
    double sglb_compute_nmrc_score(FibEntry* entry, uint32_t dst,
                                   uint32_t depth,
                                   bool* local_valid = NULL,
                                   bool* downstream_valid = NULL,
                                   simtime_picosec* downstream_last_update = NULL);
    const SglbQualitySnapshot& sglb_quality_snapshot(FibEntry* entry, uint32_t dst, uint32_t depth);
    uint8_t sglb_quality(double score);
    uint32_t pathid_ecmp_choice(Packet& pkt, uint32_t hop_count, packet_direction direction);
    uint32_t nmrc_maybe_reroute(Packet& pkt,
                                vector<FibEntry*>* available_hops,
                                uint32_t original_choice);
    bool nmrc_is_source_leaf_data(Packet& pkt,
                                  vector<FibEntry*>* available_hops) const;
    bool nmrc_inject_fastcnp(const RocePacket& data,
                             uint32_t original_egress,
                             uint32_t selected_egress,
                             uint8_t original_level,
                             uint8_t selected_level,
                             double original_score,
                             double selected_score,
                             bool need_endpoint_cooldown);
    bool nmrc_inject_relative_fastcnp(
        RocePacket& data, uint32_t original_egress,
        uint32_t selected_egress, double original_score,
        double selected_score, uint64_t action_key,
        bool need_endpoint_cooldown = true);
    void maybe_update_netaware_feedback(Packet& pkt);
    void maybe_update_stor_feedback(Packet& pkt);
    BaseQueue* netaware_local_queue_for_ev(uint32_t dst, uint32_t ev);
    BaseQueue* netaware_trace_spine_queue_for_ev(uint32_t dst, uint32_t ev);
    NetawarePortSnapshot netaware_read_port_snapshot(BaseQueue* q);
    NetawarePortSnapshot netaware_local_snapshot(BaseQueue* q);
    NetawareExportState netaware_compute_export_state(uint32_t dst);
    void netaware_maybe_refresh_export(uint32_t dst);
    void netaware_schedule_periodic_export();
    void netaware_periodic_refresh_exports();
    const NetawarePortSnapshot* netaware_neighbor_snapshot(uint32_t dst, uint32_t ev);
    uint8_t netaware_port_level(const NetawarePortSnapshot& snapshot,
                            linkspeed_bps normal_bitrate,
                            bool grade_queues);
    uint8_t netaware_gated_port_level(const NetawarePortSnapshot& snapshot,
                                  linkspeed_bps normal_bitrate,
                                  bool grade_queues);
    NetawarePathScore netaware_path_score(const NetawarePortSnapshot& local,
                                  const NetawarePortSnapshot& remote);
    StorFeedbackLevels netaware_compute_levels(uint32_t dst, uint32_t path_count);
    void netaware_refresh_levels(NetawareState& state, uint32_t dst,
                             uint32_t path_count, simtime_picosec now);
    static void stor_apply_time_aging(StorState& state, simtime_picosec now);
    static void stor_note_level_transition(StorEvState& ev, uint8_t old_level);

    friend class SglbGcnTimer;
    friend class SglbRealGcnTimer;
    friend class NetawareExportTimer;
};

#endif
    
