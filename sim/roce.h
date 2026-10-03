// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-        


#ifndef ROCE_H
#define ROCE_H

/*
 * A ROCEv2 source and sink
 */

#include <list>
#include <map>
#include <set>
#include <vector>
#include <deque>
#include <array>
#include <ostream>
#include <tuple>
//#include "util.h"
#include "math.h"
#include "config.h"
#include "network.h"
#include "rocepacket.h"
#include "queue.h"
#include "eventlist.h"
#include "eth_pause_packet.h"
#include "trigger.h"

#define timeInf 0

//min RTO bound in us
// *** don't change this default - override it by calling RoceSrc::setMinRTO()
#define DEFAULT_RTO_MIN 5000
#define ROCE_SACK_BITMAP_BITS_DEFAULT 64
#define ROCE_SACK_BITMAP_BITS_MAX 128

class RoceSink;
class Switch;

class RoceSrc : public BaseQueue, public TriggerTarget {
    friend class RoceSink;
public:
    typedef enum {LB_ECMP = 0, LB_REPS = 1, LB_CONWEAVE = 2, LB_NDP = 3, LB_RR = 4, LB_OPS = 5, LB_MRC = 6, LB_STOR = 7, LB_NETAWARE = 8, LB_NMRC = 9, LB_MPRDMA = 10, LB_CONGA = 11} lb_mode_t;
    typedef enum {
        CC_NONE = 0,
        CC_DCTCP_VARIANT = 1,
        CC_DCQCN = 2,
        CC_DCTCP_VARIANT_NODUP_OLD = 3
    } cc_mode_t;
    typedef enum {RX_GBN = 0, RX_SP_RETX_QUEUE = 1} rx_mode_t;
    typedef enum {
        TRANSPORT_LEGACY = 0,
        TRANSPORT_MRC_EXACT_BOUNDED = 1
    } transport_semantics_t;
    typedef enum {
        TRIM_RECOVERY_CUMULATIVE = 0,
        TRIM_RECOVERY_EXACT_PSN = 1
    } trim_recovery_mode_t;
    enum {MRC_EV_COUNT = 64};
    typedef enum {
        MRC_CONGESTION_ECN = 0,
        MRC_CONGESTION_TRIM = 1
    } mrc_congestion_signal_t;
    typedef enum {
        NETAWARE_WRR_BUCKET = 0,
        NETAWARE_WRR_DIRECT = 1,
        NETAWARE_WRR_SHUFFLED_BUCKET = 2,
        NETAWARE_WRR_TOPK = 3
    } netaware_wrr_mode_t;
    typedef enum {
        NETAWARE_WEIGHT_ADAPTATION_OFF = 0,
        NETAWARE_WEIGHT_ADAPTATION_GOOD_SHARE_CAP = 1
    } netaware_weight_adaptation_t;
    typedef enum {
        NMRC_ENDPOINT_RR_COOLDOWN = 0,
        NMRC_ENDPOINT_RANDOM_STATELESS = 1
    } nmrc_endpoint_policy_t;
    typedef enum {
        NMRC_ALL_COOLING_EARLIEST = 0,
        NMRC_ALL_COOLING_RR_RESET = 1
    } nmrc_all_cooling_policy_t;
    enum {
        WEIGHTED_SHUFFLED_BUCKET_PATH_MULTIPLIER = 4
    };
    typedef enum {
        DCQCN_NACK_AS_CNP = 0,
        DCQCN_NACK_CNP = DCQCN_NACK_AS_CNP,
        DCQCN_NACK_IGNORE = 1,
        DCQCN_NACK_RATE_CUT = 2
    } dcqcn_nack_reaction_t;

    RoceSrc(RoceLogger* logger, TrafficLogger* pktlogger, EventList &eventlist, linkspeed_bps rate);

    virtual void connect(Route* routeout, Route* routeback, RoceSink& sink, simtime_picosec startTime);
    void set_src(uint32_t src) {_srcaddr = src;}
    void set_dst(uint32_t dst) {_dstaddr = dst;}
    void set_traffic_logger(TrafficLogger* pktlogger);

    void startflow();
    void setRate(linkspeed_bps r) {_bitrate = r; update_packet_spacing(); doNextEvent();}

    inline void set_flowid(flowid_t flow_id) { _flow.set_flowid(flow_id);}
    inline void set_flow_ecmp_override(bool enabled) {
        _flow_lb_mode = enabled ? LB_ECMP : _lb_mode;
        _flow.set_background_traffic(enabled);
    }
    inline bool background_traffic() const {
        return _flow.background_traffic();
    }


    static void setMinRTO(uint32_t min_rto_in_us) {_min_rto = timeFromUs((uint32_t)min_rto_in_us);}
    static void setHighRTO(uint32_t high_rto_in_us) {_rto_high = high_rto_in_us ? timeFromUs((uint32_t)high_rto_in_us) : 0;}
    static void setReceiveMode(rx_mode_t mode) {_rx_mode = mode;}
    static void setTransportSemantics(transport_semantics_t semantics) {
        _transport_semantics = semantics;
    }
    static transport_semantics_t transportSemantics() {
        return _transport_semantics;
    }
    static const char* transportSemanticsName() {
        return _transport_semantics == TRANSPORT_MRC_EXACT_BOUNDED ?
            "mrc_exact_bounded" : "legacy";
    }
    static uint32_t normalizeSackBitmapBits(uint32_t bits) {
        return bits <= ROCE_SACK_BITMAP_BITS_DEFAULT ?
            ROCE_SACK_BITMAP_BITS_DEFAULT : ROCE_SACK_BITMAP_BITS_MAX;
    }
    static void setSackBitmapBits(uint32_t bits) {_sack_bitmap_bits = normalizeSackBitmapBits(bits);}
    static uint32_t sackBitmapBits() {return _sack_bitmap_bits;}
    static void setOooTolerance(simtime_picosec interval) {_ooo_tolerance = interval;}
    static void setOooWindowPkts(uint32_t pkts) {_ooo_window_pkts = pkts ? pkts : 1;}
    static void setNackInterval(simtime_picosec interval) {_nack_interval = interval;}
    static void setLoadBalancing(lb_mode_t mode) {_lb_mode = mode;}
    static void setPathEntropySize(uint32_t paths) {_path_entropy_size = paths ? paths : 1;}
    static void setNmrcEvSeed(uint32_t seed);
    static uint32_t nmrcEvSeed();
    static void setNmrcEndpointPolicy(nmrc_endpoint_policy_t policy);
    static nmrc_endpoint_policy_t nmrcEndpointPolicy();
    static const char* nmrcEndpointPolicyName();
    static void setNmrcAllCoolingPolicy(nmrc_all_cooling_policy_t policy);
    static nmrc_all_cooling_policy_t nmrcAllCoolingPolicy();
    static const char* nmrcAllCoolingPolicyName();
    static void setRepsBufferSize(uint32_t size) {_reps_buffer_size = size ? size : 1;}
    static void setRepsWarmupPkts(uint32_t pkts) {_reps_warmup_pkts = pkts;}
    static void setHostsPerTor(uint32_t hosts) {_hosts_per_tor = hosts ? hosts : 1;}
    static void setCongaUplinks(uint32_t uplinks) {_conga_uplinks = uplinks ? uplinks : 1;}
    static void resetCongaSharedState();
    static void updateCongaLocalMetric(uint32_t source_tor, uint32_t uplink,
                                       uint32_t metric);
    static uint32_t congaRemoteMetric(uint32_t source_tor, uint32_t destination_tor,
                                      uint32_t uplink);
    static void resetStorSharedState();
    static void resetNetawareSharedState();
    static void resetStorProfileDiag();
    static uint64_t storLevelTransition(uint8_t from, uint8_t to) {
        return from < 4 && to < 4 ? _stor_level_transitions[from][to] : 0;
    }
    static uint64_t netawareLevelTransition(uint8_t from, uint8_t to) {
        return from < 4 && to < 4 ? _netaware_level_transitions[from][to] : 0;
    }
    static uint64_t storLevelChanges() {return _stor_level_changes;}
    static uint64_t netawareLevelChanges() {return _netaware_level_changes;}
    static uint64_t storAllZeroProfiles() {return _stor_all_zero_profiles;}
    static uint64_t netawareAllZeroProfiles() {return _netaware_all_zero_profiles;}
    static uint64_t storAllZeroSelections() {return _stor_all_zero_selections;}
    static uint64_t netawareAllZeroSelections() {return _netaware_all_zero_selections;}
    static void setStorBinarySelector(bool enabled) {_stor_binary_selector = enabled;}
    static bool storBinarySelector() {return _stor_binary_selector;}
    static void setStorLevelWeights(uint32_t good, uint32_t degraded,
                                    uint32_t bad, uint32_t avoid) {
        _stor_level_weights[STOR_LEVEL_GOOD] = good;
        _stor_level_weights[STOR_LEVEL_DEGRADED] = degraded;
        _stor_level_weights[STOR_LEVEL_BAD] = bad;
        _stor_level_weights[STOR_LEVEL_AVOID] = avoid;
        resetStorSharedState();
    }
    static uint32_t storLevelWeight(uint8_t level) {
        return level <= STOR_LEVEL_AVOID ? _stor_level_weights[level] : 0;
    }
    static void setNetawareLevelWeights(uint32_t good, uint32_t degraded,
                                    uint32_t bad, uint32_t avoid) {
        _netaware_level_weights[STOR_LEVEL_GOOD] = good;
        _netaware_level_weights[STOR_LEVEL_DEGRADED] = degraded;
        _netaware_level_weights[STOR_LEVEL_BAD] = bad;
        _netaware_level_weights[STOR_LEVEL_AVOID] = avoid;
        resetNetawareSharedState();
    }
    static uint32_t netawareLevelWeight(uint8_t level) {
        return level <= STOR_LEVEL_AVOID ? _netaware_level_weights[level] : 0;
    }
    static void setNetawareWrrMode(netaware_wrr_mode_t mode) {_netaware_wrr_mode = mode;}
    static netaware_wrr_mode_t netawareWrrMode() {return _netaware_wrr_mode;}
    static const char* netawareWrrModeName();
    static void setNetawareTopK(uint32_t k) {_netaware_topk = k ? k : 1;}
    static uint32_t netawareTopK() {return _netaware_topk;}
    static void setNetawareWeightAdaptation(netaware_weight_adaptation_t mode);
    static netaware_weight_adaptation_t netawareWeightAdaptation();
    static const char* netawareWeightAdaptationName();
    static uint32_t weightedShuffledBucketSize(uint32_t path_space) {
        return WEIGHTED_SHUFFLED_BUCKET_PATH_MULTIPLIER *
            (path_space ? path_space : 1);
    }
    static uint32_t netawareShuffledBucketSize(uint32_t path_space) {
        return weightedShuffledBucketSize(path_space);
    }
    static uint32_t virtualShuffleIndexForTest(uint32_t key, uint64_t epoch,
                                               uint32_t position,
                                               uint32_t domain);
    static void setNetawareDecisionTrace(std::ostream* trace) {_netaware_decision_trace = trace;}
    static void resetPathSelectionDiag();
    static void configurePathSelectionTimeline(std::ostream* trace,
                                               uint64_t every);
    static void flushPathSelectionTimeline();
    static void setDiagPhysicalPathSpace(uint32_t paths) {_diag_physical_path_space = paths ? paths : 1;}
    static uint64_t diagSelectedTotal() {return _diag_selected_total;}
    static const std::map<uint32_t, uint64_t>& diagSelectedEvHist() {return _diag_selected_ev_hist;}
    static const std::map<uint32_t, uint64_t>& diagSelectedPhysicalHist() {return _diag_selected_physical_hist;}
    static const std::vector<uint32_t>& diagFirstSelectedEvs() {return _diag_first_selected_evs;}
    static void setMrcFailedRetry(simtime_picosec retry) {_mrc_failed_retry = retry;}
    static void setMrcProbeIntervalPkts(uint32_t pkts) {_mrc_probe_interval_pkts = pkts;}
    static void setMrcFailureRecoveryEnabled(bool enabled) {
        _mrc_failure_recovery_enabled = enabled;
    }
    static bool mrcFailureRecoveryEnabled() {
        return _mrc_failure_recovery_enabled;
    }
    static void setMrcProbeSuccessThreshold(uint32_t successes) {
        _mrc_probe_success_threshold = successes ? successes : 1;
    }
    static uint32_t mrcProbeSuccessThreshold() {
        return _mrc_probe_success_threshold;
    }
    static void setConweaveRttThreshold(simtime_picosec threshold) {_conweave_rtt_threshold = threshold;}
    static void setConweaveMinRerouteGap(simtime_picosec gap) {_conweave_min_reroute_gap = gap;}
    static void setNdpInitialWindow(uint32_t pkts) {_ndp_initial_window = pkts ? pkts : 1;}
    static void setCongestionControl(cc_mode_t mode) {_cc_mode = mode;}
    static void setTrimRecoveryMode(trim_recovery_mode_t mode) {
        _trim_recovery_mode = mode;
    }
    static trim_recovery_mode_t trimRecoveryMode() {
        return _trim_recovery_mode;
    }
    static const char* trimRecoveryModeName() {
        return _trim_recovery_mode == TRIM_RECOVERY_EXACT_PSN ?
            "exact" : "cumulative";
    }
    static void setCcInitialWindow(uint32_t pkts) {_cc_initial_cwnd_pkts = pkts ? pkts : 1;}
    static void setCcMinWindow(uint32_t pkts) {_cc_min_cwnd_pkts = pkts ? pkts : 1;}
    static void setCcMaxWindow(uint32_t pkts) {_cc_max_cwnd_pkts = pkts;}
    static void setDcqcnG(double g) {_dcqcn_g = g;}
    static void setDcqcnInitialAlpha(double alpha) {_dcqcn_initial_alpha = alpha;}
    static void setDcqcnAiRate(linkspeed_bps rate) {_dcqcn_ai_rate = rate;}
    static void setDcqcnMinRate(linkspeed_bps rate) {_dcqcn_min_rate = rate;}
    static void setDcqcnByteCounter(mem_b bytes) {_dcqcn_byte_counter = bytes;}
    static void setDcqcnFastRecoverySteps(uint32_t steps) {_dcqcn_fast_recovery_steps = steps;}
    static void setDcqcnAlphaInterval(simtime_picosec interval) {_dcqcn_alpha_interval = interval;}
    static void setDcqcnRateIncreaseInterval(simtime_picosec interval) {_dcqcn_rate_increase_interval = interval;}
    static void setDcqcnCnpInterval(simtime_picosec interval) {_dcqcn_cnp_interval = interval;}
    static void setDcqcnNackReaction(dcqcn_nack_reaction_t reaction) {_dcqcn_nack_reaction = reaction;}
    static void setDcqcnNackReaction(uint32_t reaction) {_dcqcn_nack_reaction = (dcqcn_nack_reaction_t)reaction;}
    static void printDcqcnConfiguration(std::ostream& out);

    void set_flowsize(uint64_t flow_size_in_bytes) {
        _flow_size = flow_size_in_bytes;
    }

    void set_stoptime(simtime_picosec stop_time) {
        _stop_time = stop_time;
        cout << "Setting stop time to " << timeAsSec(_stop_time) << endl;
    }

    // called from a trigger to start the flow.
    virtual void activate() {
        cout << "Activate called " << _flow._name << endl;
        startflow();
    }

    void set_end_trigger(Trigger& trigger);

    virtual void doNextEvent();
    virtual void receivePacket(Packet& pkt);

    virtual void setPath(uint32_t p) {_pathid = p;}

    virtual void processPause(const EthPausePacket& pkt);
    virtual void processAck(const RoceAck& ack);
    virtual void processNack(const RoceNack& nack);
    virtual void rtx_timer_hook(simtime_picosec now, simtime_picosec period);
    uint32_t choose_path_for_test(Packet::PktPriority priority, bool retransmitted) {
        return choose_path(priority, retransmitted);
    }
    void apply_stor_feedback_for_test(const StorFeedbackLevels& levels);
    void apply_netaware_feedback_for_test(const NetawareFeedbackLevels& levels);
    std::vector<uint32_t> netaware_bucket_tickets_for_test(Packet::PktPriority priority,
                                                       uint32_t path_space);
    void init_nmrc_evs_for_test(uint32_t path_space);
    std::vector<uint32_t> nmrc_ev_values_for_test() const;
    std::vector<uint32_t> nmrc_physical_paths_for_test() const;
    uint32_t choose_nmrc_ev_for_test(uint32_t path_space);
    bool notify_nmrc_ev_for_test(uint32_t ev);
    bool nmrc_ev_cooling_for_test(uint32_t ev) const;
    bool nmrc_ev_cooling_flag_for_test(uint32_t ev) const;
    uint64_t nmrc_ev_cool_until_for_test(uint32_t ev) const;
    uint64_t nmrc_selection_ordinal_for_test() const;
    uint64_t nmrc_all_cooling_fallbacks_for_test() const;
    bool nmrc_all_cooling_rr_active_for_test() const;
    uint32_t nmrc_all_cooling_rr_progress_for_test() const;
    uint32_t nmrc_ev_set_size_for_diag() const {
        return (uint32_t)_nmrc_evs.size();
    }
    uint32_t nmrc_unique_physical_paths_for_diag() const;
    uint64_t nmrc_cooldown_starts_for_diag() const {
        return _nmrc_cooldown_starts;
    }
    uint64_t nmrc_cooling_skips_for_diag() const {
        return _nmrc_cooling_skips;
    }
    uint64_t nmrc_cooling_recoveries_for_diag() const {
        return _nmrc_cooling_recoveries;
    }
    uint64_t nmrc_duplicate_notifications_for_diag() const {
        return _nmrc_duplicate_notifications;
    }
    uint64_t nmrc_all_cooling_fallbacks_for_diag() const {
        return _nmrc_all_cooling_fallbacks;
    }
    uint64_t nmrc_all_cooling_rr_episodes_for_diag() const {
        return _nmrc_all_cooling_rr_episodes;
    }
    uint64_t nmrc_all_cooling_rr_selections_for_diag() const {
        return _nmrc_all_cooling_rr_selections;
    }
    uint64_t nmrc_all_cooling_rr_resets_for_diag() const {
        return _nmrc_all_cooling_rr_resets;
    }
    uint64_t nmrc_fastcnp_policy_ignored_for_diag() const {
        return _nmrc_fastcnp_policy_ignored;
    }
    uint64_t nmrc_trim_policy_ignored_for_diag() const {
        return _nmrc_trim_policy_ignored;
    }
    uint64_t nmrcFastCnpCcMutations() const {
        return _nmrc_fastcnp_cc_mutations;
    }
    std::array<uint64_t, 64> fast_cnp_isolation_snapshot_for_test() const;
    uint64_t ecnNominalCeForDiag() const;
    uint64_t ecnDetourCeForDiag() const;
    std::array<uint32_t, 5> mrc_state_counts_for_diag() const;
    std::array<uint32_t, 5> mrc_state_physical_counts_for_diag() const;
    uint32_t mrc_backup_remaining_for_diag() const;

    virtual mem_b queuesize() const { return 0;};
    virtual mem_b maxsize() const { return 0;}; 

    // should really be private, but loggers want to see:
    uint64_t _highest_sent;  //seqno is in bytes
    uint64_t _packets_sent;
    uint64_t _last_acked;
    uint32_t _new_packets_sent;  // all the below reduced to 32 bits to save RAM
    uint32_t _rtx_packets_sent;
    uint32_t _acks_received;
    uint32_t _nacks_received;
    uint32_t _ooo_nacks_received;
    uint32_t _trim_nacks_received;
    uint32_t _trim_lh_nacks_received;
    uint32_t _loss_nacks_received;
    uint32_t _ecn_echo_acks_received;
    uint32_t _duplicate_acks_received;
    uint32_t _duplicate_ack_inflate_suppressed;
    uint64_t _bounded_inflight_pkts;
    uint64_t _bounded_unique_acks;
    uint32_t _bounded_recovery_inflight_bytes;
    uint32_t _bounded_recovery_inflight_max_bytes;
    uint64_t _bounded_stale_attempt_nacks;
    uint64_t _bounded_duplicate_failure_nacks;
    uint64_t _bounded_duplicate_confirmations_suppressed;
    uint64_t _bounded_acked_revival_rejected;
    uint64_t _bounded_attempt_wraps;
    uint64_t _bounded_exact_trim_recoveries;
    uint64_t _bounded_sack_loss_recoveries;
    uint32_t _feedback_acks_received;
    uint32_t _feedback_nacks_received;
    uint32_t _feedback_zero_bits_received;
    uint32_t _netaware_ev_skips;
    uint32_t _netaware_bitmap_fallbacks;
    uint64_t _stor_selected_good;
    uint64_t _stor_selected_degraded;
    uint64_t _stor_selected_bad;
    uint64_t _stor_selected_avoid;
    uint64_t _reps_random_sends;
    uint64_t _reps_cached_sends;
    uint64_t _reps_clean_ack_cached;
    uint64_t _reps_ecn_ack_discarded;
    uint64_t _reps_buffer_occupancy_samples;
    std::map<uint32_t, uint64_t> _reps_buffer_occupancy_hist;
    uint64_t _mrc_ecn_cooldown_events;
    uint64_t _mrc_trim_events;
    uint64_t _mrc_rto_fail_events;
    uint64_t _mrc_trim_cooling_events;
    uint64_t _mrc_nack_ooo_ignored_for_failure;
    uint64_t _mrc_nack_loss_fail_events;
    uint64_t _mrc_nack_unknown_ignored_for_failure;
    uint64_t _mrc_failure_backup_promotions;
    uint64_t _mrc_forced_cooling_use;
    uint64_t _mrc_forced_cooling_earliest_use;
    uint64_t _mrc_forced_cooling_round_robin_use;
    uint64_t _mrc_feedback_exact_ev_events;
    uint64_t _mrc_feedback_sequence_fallback_events;
    uint64_t _mrc_feedback_physical_fallback_events;
    uint64_t _mrc_feedback_cumulative_mismatch_events;
    uint64_t _mrc_select_counter;
    uint64_t _mrc_rotation;
    uint32_t _mrc_rotation_slot;
    uint64_t _mrc_skip_opportunities_consumed;
    uint64_t _mrc_data_on_non_good_violations;
    uint64_t _mrc_cycle_cooling_events;
    uint64_t _mrc_cycle_cooling_expiries;
    uint64_t _mrc_cooling_skip_selection_sum;
    uint64_t _mrc_cooling_skip_selection_events;
    uint64_t _mrc_duplicate_feedback_ignored;
    uint64_t _mrc_probe_events;
    uint64_t _mrc_probe_success_events;
    uint64_t _mrc_probe_fail_events;
    uint64_t _mrc_backup_replacement_events;
    uint64_t _mrc_backup_replacement_diff_physical;
    uint64_t _mrc_retx_original_physical;
    uint64_t _mrc_retx_different_physical;
    uint64_t _mrc_retx_same_physical_new_ev;
    uint64_t _mrc_retx_different_physical_new_ev;
    uint64_t _mrc_retx_unknown_original;
    uint64_t _mrc_retx_fallback_events;
    uint64_t _nmrc_fastcnp_arrived;
    uint64_t _nmrc_fastcnp_after_done;
    uint64_t _nmrc_fastcnp_bytes;
    uint64_t _nmrc_fastcnp_latency_sum;
    uint64_t _nmrc_fastcnp_unknown_qp;
    uint64_t _nmrc_fastcnp_unknown_ev;
    uint64_t _nmrc_fastcnp_cc_mutations;
    uint64_t _nmrc_trim_non_detour;
    uint64_t _nmrc_trim_detour;
    uint64_t _nmrc_trim_nominal_cooldown_starts;
    uint64_t _nmrc_trim_actual_cooldown_starts;
    uint64_t _nmrc_trim_duplicate_stale_ignored;
    uint64_t _nmrc_trim_actual_unresolved;
    uint64_t _mrc_state_samples;
    uint64_t _mrc_active_count_sum;
    uint64_t _mrc_backup_count_sum;
    uint64_t _mrc_cooling_count_sum;
    uint64_t _mrc_failed_count_sum;
    uint64_t _mrc_active_physical_count_sum;
    uint64_t _mrc_cooling_physical_count_sum;
    uint64_t _mrc_failed_physical_count_sum;
    uint32_t _mrc_active_count_max;
    uint32_t _mrc_backup_count_max;
    uint32_t _mrc_cooling_count_max;
    uint32_t _mrc_failed_count_max;
    uint32_t _mrc_active_physical_count_max;
    uint32_t _mrc_cooling_physical_count_max;
    uint32_t _mrc_failed_physical_count_max;
    std::map<uint32_t, uint64_t> _mrc_ecn_physical_hist;
    std::map<uint32_t, uint64_t> _mrc_trim_physical_hist;
    std::map<uint32_t, uint64_t> _mrc_nack_physical_hist;
    std::map<uint32_t, uint64_t> _mrc_ooo_nack_physical_hist;
    std::map<uint32_t, uint64_t> _mrc_loss_nack_physical_hist;

    uint32_t _acked_packets;
    uint32_t _pathid;
    uint32_t _mprdma_last_vp;
    RocePacket::seq_t _mprdma_snd_ooh;
    uint32_t _mprdma_ool_delta_pkts;
    uint64_t _mprdma_send_count;
    simtime_picosec _mprdma_last_probe;
    bool _mprdma_ack_valid;
    std::vector<bool> _mprdma_pruned;
    std::map<RocePacket::seq_t, uint32_t> _mprdma_seq_vp;
    std::deque<uint32_t> _mprdma_ack_clock;
    uint32_t _conga_flowlet_path;
    simtime_picosec _conga_last_send;

    enum {PAUSED,READY};

    uint32_t _dstaddr;

    void print_stats();

    //round trip time estimate, needed for RTO calculation
    simtime_picosec _rtt, _rto, _mdev,_base_rtt;

    uint16_t _mss;
    uint32_t _drops;

    RoceSink* _sink;
 
    const Route* _route;
    bool _flow_started;
    uint16_t _state_send;

    bool send_packet();

    virtual const string& nodename() { return _nodename; }
    inline uint32_t flow_id() const { return _flow.flow_id();}
 
    //debugging hack
    void log_me();
    bool _log_me;

    static uint32_t _global_node_count; 
    static uint32_t _global_rto_count;  // keep track of the total number of timeouts across all srcs
    static simtime_picosec _min_rto;
    static simtime_picosec _rto_high;
    static rx_mode_t _rx_mode;
    static transport_semantics_t _transport_semantics;
    static uint32_t _sack_bitmap_bits;
    static simtime_picosec _ooo_tolerance;
    static uint32_t _ooo_window_pkts;
    static simtime_picosec _nack_interval;
    static lb_mode_t _lb_mode;
    lb_mode_t _flow_lb_mode;
    static uint32_t _path_entropy_size;
    static uint32_t _nmrc_ev_seed;
    static nmrc_endpoint_policy_t _nmrc_endpoint_policy;
    static nmrc_all_cooling_policy_t _nmrc_all_cooling_policy;
    static uint32_t _reps_buffer_size;
    static uint32_t _reps_warmup_pkts;
    static uint32_t _hosts_per_tor;
    static uint32_t _conga_uplinks;
    // CONGA's path-congestion table belongs to the source leaf and is indexed
    // by destination leaf.  All QPs with the same leaf pair must therefore
    // learn from the same feedback rather than keeping private copies.
    static std::map<std::pair<uint32_t, uint32_t>, std::vector<uint32_t> >
        _conga_shared_uplink_scores;
    static std::map<uint32_t, std::vector<uint32_t> >
        _conga_local_uplink_scores;
    static simtime_picosec _mrc_failed_retry;
    static uint32_t _mrc_probe_interval_pkts;
    static bool _mrc_failure_recovery_enabled;
    static uint32_t _mrc_probe_success_threshold;
    static simtime_picosec _conweave_rtt_threshold;
    static simtime_picosec _conweave_min_reroute_gap;
    static uint32_t _ndp_initial_window;
    static cc_mode_t _cc_mode;
    static trim_recovery_mode_t _trim_recovery_mode;
    static uint32_t _cc_initial_cwnd_pkts;
    static uint32_t _cc_min_cwnd_pkts;
    static uint32_t _cc_max_cwnd_pkts;
    static double _dcqcn_g;
    static double _dcqcn_initial_alpha;
    static linkspeed_bps _dcqcn_ai_rate;
    static linkspeed_bps _dcqcn_min_rate;
    static mem_b _dcqcn_byte_counter;
    static uint32_t _dcqcn_fast_recovery_steps;
    static simtime_picosec _dcqcn_alpha_interval;
    static simtime_picosec _dcqcn_rate_increase_interval;
    static simtime_picosec _dcqcn_cnp_interval;
    static dcqcn_nack_reaction_t _dcqcn_nack_reaction;

    PacketFlow _flow;

private:
    // Housekeeping
    RoceLogger* _logger;
    Trigger* _end_trigger;

    TrafficLogger* _pktlogger;

    // Connectivity
    string _nodename;
    uint32_t _node_num;
    uint32_t _srcaddr;

    // Mechanism
    void clear_timer(uint64_t start,uint64_t end);

    uint64_t _flow_size;  //The flow size in bytes.  Stop sending after this amount.
    simtime_picosec _stop_time;
    simtime_picosec _packet_spacing;
    simtime_picosec _time_last_sent;
    simtime_picosec _send_event_time;
    EventList::Handle _send_event_handle;
    simtime_picosec _rtx_timeout;
    bool _send_event_pending;
    bool _done;

    void schedule_send(simtime_picosec when);
    void schedule_send_now();
    void update_packet_spacing();
    void reset_congestion_control();
    void reset_rtx_timeout();
    simtime_picosec current_rto_interval() const;
    void trace_cc_state(const char* event,
                        RocePacket::seq_t seqno = 0,
                        int reason = -1,
                        bool duplicate = false,
                        bool old_duplicate = false,
                        bool ecn = false,
                        double delta = 0.0) const;
    void update_congestion_control_on_ack(const RoceAck& ack, double newly_acked_pkts);
    void update_congestion_control_on_nack();
    bool has_retransmit_work() const;
    void clean_retransmit_queue();
    void reset_sack_recovery_state();
    simtime_picosec sack_rxtpsn_reset_interval() const;
    void maybe_reset_sack_rxtpsn();
    bool sack_seq_blocked_by_rxtpsn(RocePacket::seq_t seq) const;
    void update_sack_rxtpsn(RocePacket::seq_t psn);
    void bounded_note_new_send(RocePacket::seq_t psn);
    uint64_t bounded_mark_acked(RocePacket::seq_t psn);
    bool bounded_queue_failure(RocePacket::seq_t psn, uint8_t attempt_id);
    bool bounded_begin_retransmission(RocePacket::seq_t psn,
                                      uint8_t& attempt_id,
                                      bool& used_recovery_reserve);
    bool bounded_expire_recovery_reserve();
    uint64_t bounded_mark_cumulative_acked(RocePacket::seq_t old_cack,
                                           RocePacket::seq_t new_cack);
    uint64_t bounded_process_sack(const RoceNack& nack);
    bool congestion_window_allows_send() const;
    double congestion_window_available() const;
    void clamp_congestion_window();
    void dcqcn_update_alpha_timer();
    void dcqcn_on_cnp();
    void dcqcn_maybe_increase(double newly_acked_pkts);
    void dcqcn_increase_rate();
    void clamp_dcqcn_rate();
    double _cc_cwnd_pkts;
    double _cc_inflate_pkts;
    double _dcqcn_alpha;
    double _dcqcn_current_rate;
    double _dcqcn_target_rate;
    mem_b _dcqcn_bytes_since_increase;
    uint32_t _dcqcn_recovery_count;
    bool _dcqcn_seen_cnp;
    bool _dcqcn_marked_since_alpha;
    simtime_picosec _dcqcn_last_cnp;
    simtime_picosec _dcqcn_next_alpha_update;
    simtime_picosec _dcqcn_next_rate_increase;

    uint32_t choose_path(Packet::PktPriority priority, bool retransmitted);
    void update_reps(const RoceAck& ack);
    void update_mprdma(const RoceAck& ack);
    void update_conweave(const RoceAck& ack, simtime_picosec rtt);
    void update_netaware(const RoceAck& ack);
    void processFastCnp(const RoceFastCnp& fast_cnp);
    std::array<uint64_t, 64> fast_cnp_isolation_snapshot() const;
    void update_stor(const RoceAck& ack);
    void update_stor(const RoceNack& nack);
    bool mrc_path_state_enabled() const { return _flow_lb_mode == LB_MRC; }
    std::vector<uint32_t> build_mrc_ev_order(uint32_t path_space) const;
    void reset_rr_paths();
    void init_rr_paths(uint32_t path_space);
    uint32_t choose_rr_path(uint32_t path_space);
    void reset_mrc_paths();
    void init_mrc_paths(uint32_t path_space);
    uint32_t choose_mrc_path(uint32_t path_space);
    struct MrcEv;
    struct MrcChoice {
        uint32_t logical_ev;
        uint32_t physical_path;
        MrcChoice() : logical_ev(UINT32_MAX), physical_path(0) {}
        MrcChoice(uint32_t logical, uint32_t physical)
            : logical_ev(logical), physical_path(physical) {}
    };
    MrcChoice choose_mrc_ev(uint32_t path_space);
    MrcChoice choose_mrc_skip_ev(uint32_t path_space);
    MrcChoice choose_mrc_retx_ev(uint32_t path_space,
                                 uint32_t original_logical_ev);
    void reset_mrc_flow_metrics();
    void mrc_flow_note_new_selection(uint32_t ev);
    void mrc_flow_note_quality_feedback();
    void mrc_flow_note_effective_update(
        uint32_t ev, RocePacket::seq_t sequence,
        bool failure, bool replacement_congestion);
    simtime_picosec mrc_feedback_send_time(
        RocePacket::seq_t sequence, uint32_t ev) const;
    void emit_mrc_flow_diag();
    void update_mrc_on_ack(const RoceAck& ack);
    void update_mrc_on_nack(const RoceNack& nack);
    void update_mrc_on_rto();
    void note_mrc_packet_ev(RocePacket::seq_t seqno, uint32_t logical_ev);
    void clean_mrc_seq_evs();
    uint32_t mrc_logical_ev_count(uint32_t path_space) const;
    bool mrc_ev_selectable(uint32_t logical_ev);
    MrcChoice mrc_note_selected_choice(const MrcChoice& choice);
    void mrc_activate_ev(uint32_t logical_ev);
    bool mrc_mark_congested(
        uint32_t logical_ev,
        mrc_congestion_signal_t signal = MRC_CONGESTION_ECN);
    bool mrc_mark_failed(uint32_t logical_ev);
    bool mrc_note_probe_result(uint32_t logical_ev, bool success);
    uint32_t mrc_choose_probe_ev(uint32_t path_space);
    void mrc_note_clean_ack(RocePacket::seq_t ackno,
                            uint32_t explicit_ev = UINT32_MAX);
    uint32_t mrc_resolve_encoded_feedback_ev(uint32_t explicit_ev,
                                             RocePacket::seq_t sequence,
                                             uint32_t physical_path);
    struct NmrcEv;
    struct NmrcChoice {
        uint32_t ev;
        uint32_t physical_path;
        NmrcChoice() : ev(UINT32_MAX), physical_path(0) {}
        NmrcChoice(uint32_t value, uint32_t physical)
            : ev(value), physical_path(physical) {}
    };
    void reset_nmrc_evs();
    void reset_nmrc_all_cooling_rr_episode();
    void init_nmrc_evs(uint32_t path_space);
    NmrcChoice choose_nmrc_ev(uint32_t path_space);
    NmrcChoice choose_nmrc_all_cooling_rr();
    bool notify_nmrc_ev(uint32_t ev);
    void process_nmrc_trim_feedback(const RoceNack& nack,
                                    bool failure_accepted);
    uint32_t nmrc_ev_index(uint32_t ev) const;
    uint32_t nmrc_physical_path(uint32_t ev, uint32_t path_space) const;
    void init_selector_priority(Packet::PktPriority priority, uint32_t path_space);
    std::pair<uint32_t, uint32_t> tor_pair_cache_key() const;
    void apply_stor_feedback(const StorFeedbackLevels& feedback, uint32_t path_space);
    void apply_netaware_snapshot(const NetawareFeedbackLevels& feedback, uint32_t path_space);
    void record_netaware_selected_level(uint8_t level);
    uint32_t choose_netaware_direct_path(uint32_t prio, uint32_t path_space);
    uint32_t choose_netaware_bucket_path(uint32_t prio, uint32_t path_space);
    uint32_t choose_netaware_virtual_bucket_path(uint32_t prio, uint32_t path_space);
    uint32_t choose_netaware_topk_path(uint32_t prio, uint32_t path_space);
    uint32_t choose_virtual_binary_path(uint32_t prio, uint32_t path_space,
                                        const StorFeedbackLevels& levels,
                                        uint64_t profile_version,
                                        uint32_t mode_tag);
    uint32_t virtual_selector_key(uint32_t prio, uint64_t profile_version,
                                  uint64_t epoch, uint32_t mode_tag) const;
    static uint32_t virtual_shuffle_index(uint32_t key, uint64_t epoch,
                                          uint32_t position, uint32_t domain);
    uint32_t choose_netaware_path(Packet::PktPriority priority, uint32_t path_space);
    uint32_t choose_stor_path(Packet::PktPriority priority, uint32_t path_space);
    void trace_netaware_decision(RocePacket::seq_t seqno, uint32_t path,
                             Packet::PktPriority priority);
    void record_path_selection(uint32_t selected_ev, uint32_t physical_path);
    void sample_reps_buffer_occupancy();
    void sample_mrc_state_counts();
    void init_ndp_paths(uint32_t path_space);
    uint32_t choose_ndp_path(uint32_t path_space);
    void grant_ndp_credit(uint32_t credits = 1);

    struct RepsBufferEntry {
        uint32_t cached_ev;
        bool valid;
        bool initialized;
        RepsBufferEntry() : cached_ev(0), valid(false), initialized(false) {}
    };
    enum MrcPathState {
        MRC_EV_DENIED = 0,
        MRC_EV_GOOD = 1,
        MRC_EV_SKIP = 2,
        MRC_EV_ASSUMED_BAD = 3,
        MRC_EV_PROBING = 4,
        MRC_PATH_UNUSED = MRC_EV_DENIED,
        MRC_PATH_ACTIVE = MRC_EV_GOOD,
        MRC_PATH_COOLING = MRC_EV_SKIP,
        MRC_PATH_FAILED = MRC_EV_ASSUMED_BAD,
        MRC_PATH_PROBING = MRC_EV_PROBING
    };
    struct MrcEv {
        uint32_t logical_ev;
        uint32_t physical_path;
        uint8_t state;
        uint8_t probe_successes;
        simtime_picosec retry_after;
        bool skip_pending;
        uint64_t congestion_epoch;
        bool awaiting_post_cooldown_feedback;
        MrcEv()
            : logical_ev(0), physical_path(0), state(MRC_PATH_UNUSED),
              probe_successes(0), retry_after(0),
              skip_pending(false), congestion_epoch(0),
              awaiting_post_cooldown_feedback(false) {}
    };
    struct MrcFlowMetrics {
        simtime_picosec start_time;
        simtime_picosec first_full_sweep_time;
        simtime_picosec first_state_update_time;
        simtime_picosec feedback_age_sum;
        simtime_picosec feedback_age_max;
        uint64_t new_data_selections;
        uint64_t full_sweeps;
        uint64_t quality_feedback_before_done;
        uint64_t effective_state_updates;
        uint64_t packets_before_first_update;
        uint64_t new_selections_after_first_update;
        uint64_t actionable_feedback;
        uint64_t pending_actionable_updates;
        uint64_t cooldown_starts;
        uint64_t failure_starts;
        uint64_t max_simultaneous_cooling;
        uint64_t replacement_congestion;
        uint64_t post_cooldown_first_clean;
        uint32_t active_evs;
        bool first_full_sweep_set;
        bool first_state_update_set;
        bool emitted;
        std::set<uint32_t> initial_active;
        std::set<uint32_t> unique_active;
        std::set<uint32_t> sweep_seen;
        MrcFlowMetrics() { reset(); }
        void reset() {
            start_time = 0;
            first_full_sweep_time = 0;
            first_state_update_time = 0;
            feedback_age_sum = 0;
            feedback_age_max = 0;
            new_data_selections = 0;
            full_sweeps = 0;
            quality_feedback_before_done = 0;
            effective_state_updates = 0;
            packets_before_first_update = 0;
            new_selections_after_first_update = 0;
            actionable_feedback = 0;
            pending_actionable_updates = 0;
            cooldown_starts = 0;
            failure_starts = 0;
            max_simultaneous_cooling = 0;
            replacement_congestion = 0;
            post_cooldown_first_clean = 0;
            active_evs = 0;
            first_full_sweep_set = false;
            first_state_update_set = false;
            emitted = false;
            initial_active.clear();
            unique_active.clear();
            sweep_seen.clear();
        }
    };
    struct NmrcEv {
        uint32_t ev;
        uint32_t physical_path;
        uint64_t cool_until_select_count;
        bool cooling;
        NmrcEv()
            : ev(0), physical_path(0), cool_until_select_count(0),
              cooling(false) {}
        NmrcEv(uint32_t value, uint32_t physical)
            : ev(value), physical_path(physical),
              cool_until_select_count(0), cooling(false) {}
    };
    class SpRtxQueue {
    public:
        void clear();
        bool empty() const;
        void insert(RocePacket::seq_t seq);
        void erase(RocePacket::seq_t seq);
        void erase_acked(RocePacket::seq_t last_acked);
        bool pop_next(RocePacket::seq_t last_acked,
                      RocePacket::seq_t highest_sent,
                      uint64_t flow_size,
                      RocePacket::seq_t& seq);
        size_t size() const;
        const std::set<RocePacket::seq_t>& contents() const { return _seqs; }
    private:
        std::set<RocePacket::seq_t> _seqs;
    };
    typedef enum {
        BOUNDED_SENT = 0,
        BOUNDED_RTX_PENDING = 1,
        BOUNDED_RTX_INFLIGHT = 2,
        BOUNDED_ACKED = 3
    } bounded_tx_state_t;
    struct BoundedPacketState {
        BoundedPacketState()
            : state(BOUNDED_SENT), attempt_id(0), counted_inflight(false),
              uses_recovery_reserve(false) {}
        bounded_tx_state_t state;
        uint8_t attempt_id;
        bool counted_inflight;
        bool uses_recovery_reserve;
    };
    struct SharedWeightedProfile {
        StorFeedbackLevels levels;
        std::vector<uint32_t> tickets;
        std::vector<uint32_t> bucket;
        uint64_t version;
        SharedWeightedProfile() : version(0) {}
    };
    static void rebuild_shared_weighted_profile(
        SharedWeightedProfile& profile, uint32_t path_space,
        const std::array<uint32_t, 4>& level_weights);
    static void rebuild_netaware_weighted_profile(
        SharedWeightedProfile& profile, uint32_t path_space);
    SharedWeightedProfile& shared_stor_profile(uint32_t path_space);
    SharedWeightedProfile& shared_netaware_profile(uint32_t path_space);
    void ensure_reps_buffer();
    void reset_reps_buffer();
    void detect_reps_failure();

    std::vector<RepsBufferEntry> _reps_buffer;
    uint32_t _reps_head;
    uint32_t _reps_valid_count;
    uint32_t _reps_initialized_count;
    uint32_t _reps_explore_remaining;
    uint32_t _reps_current_ev;
    uint32_t _reps_selected_ev;
    bool _reps_freezing;
    simtime_picosec _reps_freezing_until;
    static std::map<std::pair<uint32_t, uint32_t>, SharedWeightedProfile> _stor_shared_profiles;
    static std::map<std::pair<uint32_t, uint32_t>, SharedWeightedProfile> _netaware_shared_profiles;
    static std::array<uint32_t, 4> _stor_level_weights;
    static std::array<uint32_t, 4> _netaware_level_weights;
    static uint64_t _stor_level_transitions[4][4];
    static uint64_t _netaware_level_transitions[4][4];
    static uint64_t _stor_level_changes;
    static uint64_t _netaware_level_changes;
    static uint64_t _stor_all_zero_profiles;
    static uint64_t _netaware_all_zero_profiles;
    static uint64_t _stor_all_zero_selections;
    static uint64_t _netaware_all_zero_selections;
    static netaware_wrr_mode_t _netaware_wrr_mode;
    static uint32_t _netaware_topk;
    static netaware_weight_adaptation_t _netaware_weight_adaptation;
    static bool _stor_binary_selector;
    static std::ostream* _netaware_decision_trace;
    static uint32_t _diag_physical_path_space;
    static uint64_t _diag_selected_total;
    static std::map<uint32_t, uint64_t> _diag_selected_ev_hist;
    static std::map<uint32_t, uint64_t> _diag_selected_physical_hist;
    static std::vector<uint32_t> _diag_first_selected_evs;
    static std::ostream* _path_selection_timeline;
    static uint64_t _path_selection_timeline_every;
    static uint64_t _path_selection_timeline_next;
    static uint64_t _path_selection_timeline_last;
    static simtime_picosec _path_selection_last_event_time;
    static void writePathSelectionTimeline(simtime_picosec now);
    std::vector<uint32_t> _ndp_path_ids;
    uint32_t _ndp_cursor;
    uint32_t _ndp_pull_credit;
    bool _ndp_paths_ready;
    std::vector<uint32_t> _rr_evs;
    uint32_t _rr_cursor;
    uint32_t _rr_path_space;
    bool _rr_paths_ready;
    std::vector<MrcEv> _mrc_evs;
    std::vector<uint32_t> _mrc_active;
    uint32_t _mrc_path_space;
    bool _mrc_paths_ready;
    std::map<RocePacket::seq_t, uint32_t> _mrc_seq_ev;
    std::map<RocePacket::seq_t, simtime_picosec> _mrc_seq_sent_at;
    MrcFlowMetrics _mrc_flow_metrics;
    std::vector<NmrcEv> _nmrc_evs;
    uint32_t _nmrc_cursor;
    uint32_t _nmrc_path_space;
    bool _nmrc_evs_ready;
    uint64_t _nmrc_select_ordinal;
    bool _nmrc_all_cooling_rr_active;
    uint32_t _nmrc_all_cooling_rr_episode_size;
    uint32_t _nmrc_all_cooling_rr_progress;
    uint64_t _nmrc_cooldown_starts;
    uint64_t _nmrc_cooling_skips;
    uint64_t _nmrc_cooling_recoveries;
    uint64_t _nmrc_duplicate_notifications;
    uint64_t _nmrc_all_cooling_fallbacks;
    uint64_t _nmrc_all_cooling_rr_episodes;
    uint64_t _nmrc_all_cooling_rr_selections;
    uint64_t _nmrc_all_cooling_rr_resets;
    uint64_t _nmrc_fastcnp_policy_ignored;
    uint64_t _nmrc_trim_policy_ignored;
    simtime_picosec _conweave_last_reroute;
    std::array<uint32_t, 3> _selector_cursor;
    std::array<uint32_t, 3> _selector_stride;
    std::array<bool, 3> _selector_cursor_ready;
    std::array<uint32_t, 3> _stor_weight_cursor;
    std::array<uint32_t, 3> _stor_selections_since_probe;
    std::array<uint32_t, 3> _stor_avoid_probe_cursor;
    std::array<uint64_t, 3> _virtual_selector_counter;
    SpRtxQueue _rtx_queue;
    std::map<RocePacket::seq_t, BoundedPacketState> _bounded_packets;
    RocePacket::seq_t _sack_rxt_psn;
    simtime_picosec _sack_rxt_psn_updated;
    bool _sack_rxt_psn_valid;
};

class RoceSink : public PacketSink, public DataReceiver {
    friend class RoceSrc;
public:
    RoceSink();

    enum {PAUSED,READY};

    virtual void receivePacket(Packet& pkt);
    
    RoceAck::seq_t _cumulative_ack; // the packet we have cumulatively acked
    uint32_t _drops;
    uint64_t cumulative_ack() { return _cumulative_ack;}
    uint64_t total_received() const { return _cumulative_ack;}
    uint64_t rx_rcvd_bytes() const { return _rx_rcvd_bytes;}
    uint32_t drops(){ return _src->_drops;}
    uint64_t ecnNominalCe() const { return _ecn_nominal_ce; }
    uint64_t ecnDetourCe() const { return _ecn_detour_ce; }
    virtual const string& nodename() { return _nodename; }

    void set_src(uint32_t s) {_srcaddr = s;}
 
    RoceSrc* _src;

    //debugging hack
    void log_me();
    bool _log_me;

    uint32_t _srcaddr;
    
private:
    class OooNackTimer : public EventSource {
    public:
        OooNackTimer(RoceSink& sink)
            : EventSource("roce_sink_ooo_timer"), _sink(sink) {}
        virtual void doNextEvent() {_sink.ooo_nack_timer_hook();}
    private:
        RoceSink& _sink;
    };

 
    // Connectivity
    void connect(RoceSrc& src, Route* route);

    inline uint32_t flow_id() const {
        return _src->flow_id();
    };

    const Route* _route;

    string _nodename;
 
    RocePacket::seq_t _last_packet_seqno; //sequence number of the last
    //packet in the connection (or 0 if not known)
    uint64_t _total_received;
    uint64_t _rx_rcvd_bytes;
    uint64_t _ecn_nominal_ce;
    uint64_t _ecn_detour_ce;
    RocePacket::seq_t _highest_seqno;
    struct OooPacketInfo {
        OooPacketInfo() : size(0), path_id(0), mrc_ev(UINT32_MAX) {}
        OooPacketInfo(int packet_size, uint32_t packet_path_id,
                      uint32_t packet_mrc_ev)
            : size(packet_size), path_id(packet_path_id),
              mrc_ev(packet_mrc_ev) {}

        int size;
        uint32_t path_id;
        uint32_t mrc_ev;
    };
    map<RocePacket::seq_t, OooPacketInfo> _ooo_packets;
    simtime_picosec _ooo_first_time;
    simtime_picosec _nack_silent_until;
    bool _ooo_nack_event_pending;
    simtime_picosec _ooo_nack_event_time;
    uint32_t _ooo_nack_path_id;
    uint32_t _ooo_nack_mrc_ev;
    OooNackTimer _ooo_nack_timer;
 
    // Mechanism
    void send_ack(const RocePacket& pkt, simtime_picosec ts,
                  bool duplicate_ack = false,
                  bool old_duplicate_ack = false);
    RoceNack* send_nack(simtime_picosec ts, RocePacket::seq_t ackno, uint32_t path_id = 0,
                        uint64_t sack_bitmap = 0, uint16_t sack_offset = 0,
                        bool has_sack = false,
                        RoceNack::nack_reason_t reason = RoceNack::LOSS,
                        uint64_t sack_bitmap_high = 0,
                        RocePacket::seq_t sack_bitmap_start_psn = 0,
                        uint16_t sack_bitmap_valid_length = 0,
                        uint32_t mrc_ev = UINT32_MAX,
                        RocePacket::seq_t missing_psn = 0,
                        uint8_t missing_attempt_id = 0,
                        bool nmrc_detour = false,
                        uint32_t nmrc_actual_egress = UINT32_MAX,
                        bool trim_is_lh = false);
    void build_sack_bitmap(RocePacket::seq_t ackno, uint64_t& sack_bitmap_low,
                           uint64_t& sack_bitmap_high,
                           RocePacket::seq_t& sack_bitmap_start_psn,
                           uint16_t& sack_bitmap_valid_length,
                           uint16_t& sack_offset) const;
    bool should_send_sp_nack() const;
    void refresh_ooo_nack_metadata();
    void arm_ooo_nack_timer(simtime_picosec when);
    void cancel_ooo_nack_timer();
    void ooo_nack_timer_hook();
};

class RoceRtxTimerScanner : public EventSource {
public:
    RoceRtxTimerScanner(simtime_picosec scanPeriod, EventList& eventlist);
    void doNextEvent();
    void registerRoce(RoceSrc& src);
private:
    simtime_picosec _scanPeriod;
    typedef list<RoceSrc*> roces_t;
    roces_t _roces;
};

#endif
