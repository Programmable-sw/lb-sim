// -*- c-basic-offset: 4; indent-tabs-mode: nil -*- 
#include <math.h>
#include <cmath>
#include <iostream>
#include <algorithm>
#include <cstring>
#include "roce.h"
#include "conga_model.h"
#include "queue.h"
#include <stdio.h>
#include "switch.h"
#include "trigger.h"
#include "ecn.h"
using namespace std;

static uint32_t selector_mix(uint32_t a, uint32_t b, uint32_t c) {
    a += 0x9e3779b9;
    b += 0x9e3779b9;
    c += 0x85ebca6b;
    a -= b; a -= c; a ^= (c >> 13);
    b -= c; b -= a; b ^= (a << 8);
    c -= a; c -= b; c ^= (b >> 13);
    a -= b; a -= c; a ^= (c >> 12);
    b -= c; b -= a; b ^= (a << 16);
    c -= a; c -= b; c ^= (b >> 5);
    a -= b; a -= c; a ^= (c >> 3);
    b -= c; b -= a; b ^= (a << 10);
    c -= a; c -= b; c ^= (b >> 15);
    return c;
}

static uint32_t selector_gcd(uint32_t a, uint32_t b) {
    while (b != 0) {
        uint32_t t = a % b;
        a = b;
        b = t;
    }
    return a;
}

static uint32_t selector_priority_index(Packet::PktPriority priority) {
    switch (priority) {
    case Packet::PRIO_LO:
        return 0;
    case Packet::PRIO_MID:
        return 1;
    case Packet::PRIO_HI:
        return 2;
    case Packet::PRIO_NONE:
        return 0;
    }
    return 0;
}

static uint32_t nmrc_local_next(uint32_t seed, uint32_t& counter) {
    return selector_mix(seed, counter++, 0x4e4d5243);
}

////////////////////////////////////////////////////////////////
//  ROCE SOURCE
////////////////////////////////////////////////////////////////

/* When you're debugging, sometimes it's useful to enable debugging on
   a single ROCE receiver, rather than on all of them.  Set this to the
   node ID and recompile if you need this; otherwise leave it
   alone. */
//#define LOGSINK 2332
#define LOGSINK   0 


/* keep track of RTOs.  Generally, we shouldn't see RTOs if
   return-to-sender is enabled.  Otherwise we'll see them with very
   large incasts. */
uint32_t RoceSrc::_global_node_count = 0;
uint32_t RoceSrc::_global_rto_count = 0;

/* _min_rto can be tuned using SetMinRTO. Don't change it here.  */
simtime_picosec RoceSrc::_min_rto = timeFromUs((uint32_t)DEFAULT_RTO_MIN);
simtime_picosec RoceSrc::_rto_high = 0;
RoceSrc::rx_mode_t RoceSrc::_rx_mode = RoceSrc::RX_GBN;
RoceSrc::transport_semantics_t RoceSrc::_transport_semantics =
    RoceSrc::TRANSPORT_MRC_EXACT_BOUNDED;
uint32_t RoceSrc::_sack_bitmap_bits = ROCE_SACK_BITMAP_BITS_DEFAULT;
simtime_picosec RoceSrc::_ooo_tolerance = timeFromUs(15.0);
uint32_t RoceSrc::_ooo_window_pkts = 32;
simtime_picosec RoceSrc::_nack_interval = timeFromUs(4.0);
RoceSrc::lb_mode_t RoceSrc::_lb_mode = RoceSrc::LB_ECMP;
uint32_t RoceSrc::_path_entropy_size = 256;
uint32_t RoceSrc::_nmrc_ev_seed = 1;
RoceSrc::nmrc_endpoint_policy_t RoceSrc::_nmrc_endpoint_policy =
    RoceSrc::NMRC_ENDPOINT_RR_COOLDOWN;
RoceSrc::nmrc_all_cooling_policy_t RoceSrc::_nmrc_all_cooling_policy =
    RoceSrc::NMRC_ALL_COOLING_EARLIEST;
uint32_t RoceSrc::_reps_buffer_size = 8;
uint32_t RoceSrc::_reps_warmup_pkts = 0;
// REPS stores a 16-bit entropy value (e.g. UDP source port) and relies on
// the fabric ECMP hash to map that value to one of the physical paths.
static const uint32_t REPS_EV_SPACE = 1u << 16;
uint32_t RoceSrc::_hosts_per_tor = 1;
uint32_t RoceSrc::_conga_uplinks = 1;
std::map<std::pair<uint32_t, uint32_t>, std::vector<uint32_t> >
    RoceSrc::_conga_shared_uplink_scores;
std::map<uint32_t, std::vector<uint32_t> > RoceSrc::_conga_local_uplink_scores;
simtime_picosec RoceSrc::_mrc_failed_retry = timeFromUs(100.0);
uint32_t RoceSrc::_mrc_probe_interval_pkts = 256;
bool RoceSrc::_mrc_failure_recovery_enabled = false;
uint32_t RoceSrc::_mrc_probe_success_threshold = 3;
simtime_picosec RoceSrc::_conweave_rtt_threshold = timeFromUs(16.0);
simtime_picosec RoceSrc::_conweave_min_reroute_gap = timeFromUs(4.0);
uint32_t RoceSrc::_ndp_initial_window = 256;
RoceSrc::cc_mode_t RoceSrc::_cc_mode = RoceSrc::CC_DCTCP_VARIANT;
RoceSrc::trim_recovery_mode_t RoceSrc::_trim_recovery_mode =
    RoceSrc::TRIM_RECOVERY_EXACT_PSN;
uint32_t RoceSrc::_cc_initial_cwnd_pkts = 100;
uint32_t RoceSrc::_cc_min_cwnd_pkts = 1;
uint32_t RoceSrc::_cc_max_cwnd_pkts = 0;
double RoceSrc::_dcqcn_g = 1.0 / 256.0;
double RoceSrc::_dcqcn_initial_alpha = 0.6;
linkspeed_bps RoceSrc::_dcqcn_ai_rate = 4000000000ULL;
linkspeed_bps RoceSrc::_dcqcn_min_rate = 80000000000ULL;
mem_b RoceSrc::_dcqcn_byte_counter = 1400000;
uint32_t RoceSrc::_dcqcn_fast_recovery_steps = 5;
simtime_picosec RoceSrc::_dcqcn_alpha_interval = timeFromUs(28.0);
simtime_picosec RoceSrc::_dcqcn_rate_increase_interval = timeFromUs(28.0);
simtime_picosec RoceSrc::_dcqcn_cnp_interval = timeFromUs(16.8);
RoceSrc::dcqcn_nack_reaction_t RoceSrc::_dcqcn_nack_reaction = RoceSrc::DCQCN_NACK_AS_CNP;
std::map<std::pair<uint32_t, uint32_t>, RoceSrc::SharedWeightedProfile>
    RoceSrc::_stor_shared_profiles;
std::map<std::pair<uint32_t, uint32_t>, RoceSrc::SharedWeightedProfile>
    RoceSrc::_netaware_shared_profiles;
std::array<uint32_t, 4> RoceSrc::_stor_level_weights = {{4, 2, 1, 0}};
std::array<uint32_t, 4> RoceSrc::_netaware_level_weights = {{4, 2, 1, 0}};
uint64_t RoceSrc::_stor_level_transitions[4][4] = {{0}};
uint64_t RoceSrc::_netaware_level_transitions[4][4] = {{0}};
uint64_t RoceSrc::_stor_level_changes = 0;
uint64_t RoceSrc::_netaware_level_changes = 0;
uint64_t RoceSrc::_stor_all_zero_profiles = 0;
uint64_t RoceSrc::_netaware_all_zero_profiles = 0;
uint64_t RoceSrc::_stor_all_zero_selections = 0;
uint64_t RoceSrc::_netaware_all_zero_selections = 0;
RoceSrc::netaware_wrr_mode_t RoceSrc::_netaware_wrr_mode = NETAWARE_WRR_SHUFFLED_BUCKET;
uint32_t RoceSrc::_netaware_topk = 4;
RoceSrc::netaware_weight_adaptation_t RoceSrc::_netaware_weight_adaptation =
    NETAWARE_WEIGHT_ADAPTATION_GOOD_SHARE_CAP;
bool RoceSrc::_stor_binary_selector = false;
std::ostream* RoceSrc::_netaware_decision_trace = NULL;
uint32_t RoceSrc::_diag_physical_path_space = 1;
uint64_t RoceSrc::_diag_selected_total = 0;
std::map<uint32_t, uint64_t> RoceSrc::_diag_selected_ev_hist;
std::map<uint32_t, uint64_t> RoceSrc::_diag_selected_physical_hist;
std::vector<uint32_t> RoceSrc::_diag_first_selected_evs;
std::ostream* RoceSrc::_path_selection_timeline = NULL;
uint64_t RoceSrc::_path_selection_timeline_every = 0;
uint64_t RoceSrc::_path_selection_timeline_next = 0;
uint64_t RoceSrc::_path_selection_timeline_last = 0;
simtime_picosec RoceSrc::_path_selection_last_event_time = 0;

void RoceSrc::printDcqcnConfiguration(std::ostream& out) {
    const char* nack_reaction = "cnp";
    if (_dcqcn_nack_reaction == DCQCN_NACK_IGNORE)
        nack_reaction = "ignore";
    else if (_dcqcn_nack_reaction == DCQCN_NACK_RATE_CUT)
        nack_reaction = "rate_cut";

    out << "DCQCN effective config"
        << " g=" << _dcqcn_g
        << " initial_alpha=" << _dcqcn_initial_alpha
        << " ai_mbps=" << ((double)_dcqcn_ai_rate / 1000000.0)
        << " min_rate_mbps=" << ((double)_dcqcn_min_rate / 1000000.0)
        << " alpha_us=" << timeAsUs(_dcqcn_alpha_interval)
        << " rate_us=" << timeAsUs(_dcqcn_rate_increase_interval)
        << " cnp_us=" << timeAsUs(_dcqcn_cnp_interval)
        << " byte_counter=" << _dcqcn_byte_counter
        << " fast_recovery_steps=" << _dcqcn_fast_recovery_steps
        << " nack_reaction=" << nack_reaction
        << std::endl;
}

const char* RoceSrc::netawareWrrModeName() {
    if (_netaware_wrr_mode == NETAWARE_WRR_TOPK)
        return "topk";
    if (_netaware_wrr_mode == NETAWARE_WRR_DIRECT)
        return "direct";
    if (_netaware_wrr_mode == NETAWARE_WRR_SHUFFLED_BUCKET)
        return "shuffled_bucket";
    return "bucket";
}

void RoceSrc::setNetawareWeightAdaptation(netaware_weight_adaptation_t mode) {
    if (_netaware_weight_adaptation == mode)
        return;
    _netaware_weight_adaptation = mode;
    resetNetawareSharedState();
}

RoceSrc::netaware_weight_adaptation_t RoceSrc::netawareWeightAdaptation() {
    return _netaware_weight_adaptation;
}

const char* RoceSrc::netawareWeightAdaptationName() {
    if (_netaware_weight_adaptation == NETAWARE_WEIGHT_ADAPTATION_GOOD_SHARE_CAP)
        return "good_share_cap";
    return "off";
}

void RoceSrc::setNmrcEvSeed(uint32_t seed) {
    _nmrc_ev_seed = seed;
}

uint32_t RoceSrc::nmrcEvSeed() {
    return _nmrc_ev_seed;
}

void RoceSrc::setNmrcEndpointPolicy(nmrc_endpoint_policy_t policy) {
    _nmrc_endpoint_policy = policy;
}

RoceSrc::nmrc_endpoint_policy_t RoceSrc::nmrcEndpointPolicy() {
    return _nmrc_endpoint_policy;
}

const char* RoceSrc::nmrcEndpointPolicyName() {
    return _nmrc_endpoint_policy == NMRC_ENDPOINT_RANDOM_STATELESS ?
        "random_stateless" : "rr_cooldown";
}

void RoceSrc::setNmrcAllCoolingPolicy(nmrc_all_cooling_policy_t policy) {
    _nmrc_all_cooling_policy = policy;
}

RoceSrc::nmrc_all_cooling_policy_t RoceSrc::nmrcAllCoolingPolicy() {
    return _nmrc_all_cooling_policy;
}

const char* RoceSrc::nmrcAllCoolingPolicyName() {
    return _nmrc_all_cooling_policy == NMRC_ALL_COOLING_RR_RESET ?
        "rr_reset" : "earliest";
}

void RoceSrc::resetPathSelectionDiag() {
    _diag_selected_total = 0;
    _diag_selected_ev_hist.clear();
    _diag_selected_physical_hist.clear();
    _diag_first_selected_evs.clear();
    _path_selection_timeline_next = _path_selection_timeline_every;
    _path_selection_timeline_last = 0;
    _path_selection_last_event_time = 0;
}

void RoceSrc::configurePathSelectionTimeline(std::ostream* trace,
                                             uint64_t every) {
    _path_selection_timeline = trace;
    _path_selection_timeline_every = every ? every : 1;
    _path_selection_timeline_next = _path_selection_timeline_every;
    _path_selection_timeline_last = 0;
    if (!_path_selection_timeline)
        return;
    (*_path_selection_timeline) << "time_us,selected_total";
    for (uint32_t path = 0; path < _diag_physical_path_space; path++)
        (*_path_selection_timeline) << ",path_" << path;
    (*_path_selection_timeline) << ",cumulative_cv\n";
}

void RoceSrc::writePathSelectionTimeline(simtime_picosec now) {
    if (!_path_selection_timeline || _diag_selected_total == 0 ||
        _diag_selected_total == _path_selection_timeline_last)
        return;
    double mean = (double)_diag_selected_total /
        (double)_diag_physical_path_space;
    double variance = 0.0;
    for (uint32_t path = 0; path < _diag_physical_path_space; path++) {
        std::map<uint32_t, uint64_t>::const_iterator found =
            _diag_selected_physical_hist.find(path);
        double count = found == _diag_selected_physical_hist.end() ?
            0.0 : (double)found->second;
        double delta = count - mean;
        variance += delta * delta;
    }
    variance /= (double)_diag_physical_path_space;
    (*_path_selection_timeline) << timeAsUs(now)
        << "," << _diag_selected_total;
    for (uint32_t path = 0; path < _diag_physical_path_space; path++) {
        std::map<uint32_t, uint64_t>::const_iterator found =
            _diag_selected_physical_hist.find(path);
        (*_path_selection_timeline) << ","
            << (found == _diag_selected_physical_hist.end() ? 0 : found->second);
    }
    (*_path_selection_timeline) << ","
        << (mean > 0.0 ? sqrt(variance) / mean : 0.0) << "\n";
    _path_selection_timeline_last = _diag_selected_total;
}

void RoceSrc::flushPathSelectionTimeline() {
    writePathSelectionTimeline(_path_selection_last_event_time);
    if (_path_selection_timeline)
        _path_selection_timeline->flush();
}

RoceSrc::RoceSrc(RoceLogger* logger, TrafficLogger* pktlogger, EventList &eventlist, linkspeed_bps rate)
    : BaseQueue(rate,eventlist,NULL), _flow(pktlogger), _logger(logger)
{
    _flow_lb_mode = _lb_mode;
    _mss = Packet::data_packet_size();
    _end_trigger = NULL;

    _stop_time = 0;
    _flow_started = false;
    _base_rtt = timeInf;
    _acked_packets = 0;
    _packets_sent = 0;
    _new_packets_sent = 0;
    _rtx_packets_sent = 0;
    _acks_received = 0;
    _nacks_received = 0;
    _ooo_nacks_received = 0;
    _trim_nacks_received = 0;
    _trim_lh_nacks_received = 0;
    _loss_nacks_received = 0;
    _ecn_echo_acks_received = 0;
    _duplicate_acks_received = 0;
    _duplicate_ack_inflate_suppressed = 0;
    _bounded_inflight_pkts = 0;
    _bounded_unique_acks = 0;
    _bounded_recovery_inflight_bytes = 0;
    _bounded_recovery_inflight_max_bytes = 0;
    _bounded_stale_attempt_nacks = 0;
    _bounded_duplicate_failure_nacks = 0;
    _bounded_duplicate_confirmations_suppressed = 0;
    _bounded_acked_revival_rejected = 0;
    _bounded_attempt_wraps = 0;
    _bounded_exact_trim_recoveries = 0;
    _bounded_sack_loss_recoveries = 0;
    _bounded_packets.clear();
    _feedback_acks_received = 0;
    _feedback_nacks_received = 0;
    _feedback_zero_bits_received = 0;
    _netaware_ev_skips = 0;
    _netaware_bitmap_fallbacks = 0;
    _stor_selected_good = 0;
    _stor_selected_degraded = 0;
    _stor_selected_bad = 0;
    _stor_selected_avoid = 0;
    _reps_random_sends = 0;
    _reps_cached_sends = 0;
    _reps_clean_ack_cached = 0;
    _reps_ecn_ack_discarded = 0;
    _reps_buffer_occupancy_samples = 0;
    _reps_buffer_occupancy_hist.clear();
    _mrc_ecn_cooldown_events = 0;
    _mrc_trim_events = 0;
    _mrc_rto_fail_events = 0;
    _mrc_trim_cooling_events = 0;
    _mrc_nack_ooo_ignored_for_failure = 0;
    _mrc_nack_loss_fail_events = 0;
    _mrc_nack_unknown_ignored_for_failure = 0;
    _mrc_failure_backup_promotions = 0;
    _mrc_forced_cooling_use = 0;
    _mrc_forced_cooling_earliest_use = 0;
    _mrc_forced_cooling_round_robin_use = 0;
    _mrc_feedback_exact_ev_events = 0;
    _mrc_feedback_sequence_fallback_events = 0;
    _mrc_feedback_physical_fallback_events = 0;
    _mrc_feedback_cumulative_mismatch_events = 0;
    _mrc_select_counter = 0;
    _mrc_rotation = 0;
    _mrc_rotation_slot = 0;
    _mrc_skip_opportunities_consumed = 0;
    _mrc_data_on_non_good_violations = 0;
    _mrc_cycle_cooling_events = 0;
    _mrc_cycle_cooling_expiries = 0;
    _mrc_cooling_skip_selection_sum = 0;
    _mrc_cooling_skip_selection_events = 0;
    _mrc_duplicate_feedback_ignored = 0;
    _mrc_probe_events = 0;
    _mrc_probe_success_events = 0;
    _mrc_probe_fail_events = 0;
    _mrc_backup_replacement_events = 0;
    _mrc_backup_replacement_diff_physical = 0;
    _mrc_retx_original_physical = 0;
    _mrc_retx_different_physical = 0;
    _mrc_retx_same_physical_new_ev = 0;
    _mrc_retx_different_physical_new_ev = 0;
    _mrc_retx_unknown_original = 0;
    _mrc_retx_fallback_events = 0;
    _nmrc_fastcnp_arrived = 0;
    _nmrc_fastcnp_after_done = 0;
    _nmrc_fastcnp_bytes = 0;
    _nmrc_fastcnp_latency_sum = 0;
    _nmrc_fastcnp_unknown_qp = 0;
    _nmrc_fastcnp_unknown_ev = 0;
    _nmrc_fastcnp_cc_mutations = 0;
    _nmrc_trim_non_detour = 0;
    _nmrc_trim_detour = 0;
    _nmrc_trim_nominal_cooldown_starts = 0;
    _nmrc_trim_actual_cooldown_starts = 0;
    _nmrc_trim_duplicate_stale_ignored = 0;
    _nmrc_trim_actual_unresolved = 0;
    _mrc_state_samples = 0;
    _mrc_active_count_sum = 0;
    _mrc_backup_count_sum = 0;
    _mrc_cooling_count_sum = 0;
    _mrc_failed_count_sum = 0;
    _mrc_active_physical_count_sum = 0;
    _mrc_cooling_physical_count_sum = 0;
    _mrc_failed_physical_count_sum = 0;
    _mrc_active_count_max = 0;
    _mrc_backup_count_max = 0;
    _mrc_cooling_count_max = 0;
    _mrc_failed_count_max = 0;
    _mrc_active_physical_count_max = 0;
    _mrc_cooling_physical_count_max = 0;
    _mrc_failed_physical_count_max = 0;
    _mrc_ecn_physical_hist.clear();
    _mrc_trim_physical_hist.clear();
    _mrc_nack_physical_hist.clear();
    _mrc_ooo_nack_physical_hist.clear();
    _mrc_loss_nack_physical_hist.clear();
    _nmrc_cooldown_starts = 0;
    _nmrc_cooling_skips = 0;
    _nmrc_cooling_recoveries = 0;
    _nmrc_duplicate_notifications = 0;
    _nmrc_all_cooling_fallbacks = 0;
    _nmrc_all_cooling_rr_episodes = 0;
    _nmrc_all_cooling_rr_selections = 0;
    _nmrc_all_cooling_rr_resets = 0;
    _nmrc_fastcnp_policy_ignored = 0;
    _nmrc_trim_policy_ignored = 0;

    _highest_sent = 0;
    _last_acked = 0;
    _srcaddr = UINT32_MAX;
    _dstaddr = UINT32_MAX;

    _sink = 0;
    _done = false;

    _rtt = 0;
    _rto = timeFromMs(20);
    _mdev = 0;
    _drops = 0;
    _flow_size = ((uint64_t)1)<<63;
  
    _node_num = _global_node_count++;
    _nodename = "rocesrc " + to_string(_node_num);

    _pathid = random()%256;
    _mprdma_last_vp = _pathid;
    _mprdma_snd_ooh = 0;
    _mprdma_ool_delta_pkts = 32;
    _mprdma_send_count = 0;
    _mprdma_last_probe = 0;
    _mprdma_ack_valid = false;
    _mprdma_pruned.clear();
    _mprdma_seq_vp.clear();
    _mprdma_ack_clock.clear();
    _conga_flowlet_path = UINT32_MAX;
    _conga_last_send = timeInf;
    _reps_head = 0;
    _reps_valid_count = 0;
    _reps_explore_remaining = 0;
    _reps_current_ev = random() % REPS_EV_SPACE;
    _reps_selected_ev = _reps_current_ev;
    _reps_freezing = false;
    _reps_freezing_until = 0;
    reset_reps_buffer();
    _ndp_cursor = 0;
    _ndp_pull_credit = 0;
    _ndp_paths_ready = false;
    _rr_cursor = 0;
    _rr_path_space = 0;
    _rr_paths_ready = false;
    _mrc_path_space = 0;
    _mrc_paths_ready = false;
    _nmrc_cursor = 0;
    _nmrc_path_space = 0;
    _nmrc_evs_ready = false;
    _nmrc_select_ordinal = 0;
    _nmrc_all_cooling_rr_active = false;
    _nmrc_all_cooling_rr_episode_size = 0;
    _nmrc_all_cooling_rr_progress = 0;
    _conweave_last_reroute = 0;
    _selector_cursor.fill(0);
    _selector_stride.fill(1);
    _selector_cursor_ready.fill(false);
    _stor_weight_cursor.fill(0);
    _virtual_selector_counter.fill(0);
    _stor_selections_since_probe.fill(0);
    _stor_avoid_probe_cursor.fill(0);
    _rtx_queue.clear();
    reset_sack_recovery_state();
    reset_congestion_control();
    _mprdma_last_vp = _pathid;
    _mprdma_snd_ooh = 0;
    _mprdma_send_count = 0;
    _mprdma_last_probe = 0;
    _mprdma_ack_valid = false;
    _mprdma_pruned.clear();
    _mprdma_seq_vp.clear();
    _mprdma_ack_clock.clear();
    _conga_flowlet_path = UINT32_MAX;
    _conga_last_send = timeInf;

    //cout << _nodename << " path id is " << _pathid << endl;

    // debugging hack
    _log_me = false;
    //if (get_id() == 144212)
    //    _log_me = true;

    _state_send = READY;
    _time_last_sent = 0;
    _send_event_time = 0;
    _send_event_handle = EventList::nullHandle();
    _send_event_pending = false;
    _rtx_timeout = timeInf;
    update_packet_spacing();
}

/*mem_b RoceSrc::queuesize(){
  return 0;
  }

  mem_b RoceSrc::maxsize(){
  return 0;
  }*/

void RoceSrc::set_traffic_logger(TrafficLogger* pktlogger) {
    _flow.set_logger(pktlogger);
}

void RoceSrc::schedule_send(simtime_picosec when) {
    if (_done)
        return;
    assert(when >= eventlist().now());

    if (_send_event_pending) {
        if (when >= _send_event_time)
            return;
        if (_send_event_handle != EventList::nullHandle())
            eventlist().cancelPendingSourceByHandle(*this, _send_event_handle);
        _send_event_pending = false;
        _send_event_handle = EventList::nullHandle();
        _send_event_time = 0;
    }

    _send_event_handle = eventlist().sourceIsPendingGetHandle(*this, when);
    _send_event_pending = _send_event_handle != EventList::nullHandle();
    _send_event_time = when;
}

void RoceSrc::schedule_send_now() {
    schedule_send(eventlist().now());
}

void RoceSrc::update_packet_spacing() {
    double rate = (double)_bitrate;
    if (_cc_mode == CC_DCQCN && _dcqcn_current_rate > 0 && _dcqcn_current_rate < rate)
        rate = _dcqcn_current_rate;
    if (rate < 1.0)
        rate = 1.0;

    double spacing = (Packet::data_packet_size()+RocePacket::ACKSIZE) * (pow(10.0,12.0) * 8) / rate;
    if (spacing < 1.0)
        spacing = 1.0;
    _packet_spacing = (simtime_picosec)spacing;
}

void RoceSrc::reset_congestion_control() {
    _cc_cwnd_pkts = _cc_initial_cwnd_pkts ? _cc_initial_cwnd_pkts : 1;
    _cc_inflate_pkts = 0;
    _bounded_inflight_pkts = 0;
    _bounded_unique_acks = 0;
    _bounded_recovery_inflight_bytes = 0;
    _bounded_recovery_inflight_max_bytes = 0;
    _bounded_stale_attempt_nacks = 0;
    _bounded_duplicate_failure_nacks = 0;
    _bounded_duplicate_confirmations_suppressed = 0;
    _bounded_acked_revival_rejected = 0;
    _bounded_attempt_wraps = 0;
    _bounded_exact_trim_recoveries = 0;
    _bounded_sack_loss_recoveries = 0;
    _bounded_packets.clear();
    clamp_congestion_window();

    _dcqcn_alpha = _dcqcn_initial_alpha;
    _dcqcn_current_rate = (double)_bitrate;
    _dcqcn_target_rate = (double)_bitrate;
    _dcqcn_bytes_since_increase = 0;
    _dcqcn_recovery_count = 0;
    _dcqcn_seen_cnp = false;
    _dcqcn_marked_since_alpha = false;
    _dcqcn_last_cnp = 0;
    _dcqcn_next_alpha_update = eventlist().now() + _dcqcn_alpha_interval;
    _dcqcn_next_rate_increase = eventlist().now() + _dcqcn_rate_increase_interval;
    clamp_dcqcn_rate();
    update_packet_spacing();
}

void RoceSrc::reset_rtx_timeout() {
    if (!_flow_started || _done || _highest_sent <= _last_acked) {
        _rtx_timeout = timeInf;
        return;
    }
    _rtx_timeout = eventlist().now() + current_rto_interval();
}

simtime_picosec RoceSrc::current_rto_interval() const {
    if (_rx_mode == RX_SP_RETX_QUEUE && _rto_high) {
        if (_highest_sent > _last_acked && _highest_sent - _last_acked > 3 * _mss)
            return _rto_high;
        return _min_rto;
    }
    return _rto;
}

void RoceSrc::clamp_congestion_window() {
    double min_cwnd = _cc_min_cwnd_pkts ? _cc_min_cwnd_pkts : 1;
    if (_cc_max_cwnd_pkts && _cc_cwnd_pkts > _cc_max_cwnd_pkts)
        _cc_cwnd_pkts = _cc_max_cwnd_pkts;
    if (_cc_cwnd_pkts < min_cwnd)
        _cc_cwnd_pkts = min_cwnd;
}

double RoceSrc::congestion_window_available() const {
    if (_cc_mode == CC_NONE)
        return 1.0;

    if (_transport_semantics == TRANSPORT_MRC_EXACT_BOUNDED)
        return _cc_cwnd_pkts - (double)_bounded_inflight_pkts;

    double outstanding_pkts = 0;
    if (_highest_sent > _last_acked)
        outstanding_pkts = ((double)(_highest_sent - _last_acked)) / _mss;

    if (_cc_mode == CC_DCQCN)
        return _cc_cwnd_pkts - outstanding_pkts;

    return _cc_cwnd_pkts + _cc_inflate_pkts - outstanding_pkts;
}

void RoceSrc::trace_cc_state(const char* event,
                             RocePacket::seq_t seqno,
                             int reason,
                             bool duplicate,
                             bool old_duplicate,
                             bool ecn,
                             double delta) const {
    if (!_log_me)
        return;

    double outstanding_pkts = 0.0;
    if (_highest_sent > _last_acked)
        outstanding_pkts = ((double)(_highest_sent - _last_acked)) / _mss;

    cout << "CcEvent time_us=" << timeAsUs(eventlist().now())
         << " flow_id=" << _flow.flow_id()
         << " name=" << _name
         << " event=" << event
         << " seq=" << seqno
         << " reason=" << reason
         << " duplicate=" << duplicate
         << " old_duplicate=" << old_duplicate
         << " ecn=" << ecn
         << " delta=" << delta
         << " last_acked=" << _last_acked
         << " highest_sent=" << _highest_sent
         << " outstanding=" << outstanding_pkts
         << " cwnd=" << _cc_cwnd_pkts
         << " inflate=" << _cc_inflate_pkts
         << " awnd=" << congestion_window_available()
         << " rtx_queue=" << _rtx_queue.size()
         << endl;
}

bool RoceSrc::congestion_window_allows_send() const {
    if (_cc_mode == CC_NONE)
        return true;
    return congestion_window_available() >= 1.0;
}

void RoceSrc::update_congestion_control_on_ack(const RoceAck& ack, double newly_acked_pkts) {
    if (_cc_mode == CC_NONE)
        return;

    if (_cc_mode == CC_DCQCN) {
        dcqcn_update_alpha_timer();
        if (ack.flags() & ECN_ECHO)
            dcqcn_on_cnp();
        dcqcn_maybe_increase(newly_acked_pkts);
        return;
    }

    if (ack.flags() & ECN_ECHO)
        _cc_cwnd_pkts -= 0.5;
    else if (_transport_semantics == TRANSPORT_MRC_EXACT_BOUNDED) {
        if (newly_acked_pkts > 0)
            _cc_cwnd_pkts += newly_acked_pkts / _cc_cwnd_pkts;
    } else {
        _cc_cwnd_pkts += 1.0 / _cc_cwnd_pkts;
    }

    clamp_congestion_window();

    if (_transport_semantics == TRANSPORT_MRC_EXACT_BOUNDED)
        return;

    bool suppress_duplicate_inflate =
        _cc_mode == CC_DCTCP_VARIANT_NODUP_OLD &&
        ack.is_old_duplicate_ack();
    if (suppress_duplicate_inflate)
        _duplicate_ack_inflate_suppressed++;
    else
        _cc_inflate_pkts += 1.0;
    if (newly_acked_pkts > 0) {
        if (newly_acked_pkts >= _cc_inflate_pkts)
            _cc_inflate_pkts = 0;
        else
            _cc_inflate_pkts -= newly_acked_pkts;
    }
}

void RoceSrc::update_congestion_control_on_nack() {
    if (_cc_mode == CC_NONE)
        return;

    if (_cc_mode == CC_DCQCN) {
        if (_dcqcn_nack_reaction == DCQCN_NACK_AS_CNP) {
            dcqcn_on_cnp();
        } else if (_dcqcn_nack_reaction == DCQCN_NACK_RATE_CUT) {
            simtime_picosec now = eventlist().now();
            _dcqcn_target_rate = _dcqcn_current_rate;
            _dcqcn_current_rate *= 0.875;
            _dcqcn_bytes_since_increase = 0;
            _dcqcn_recovery_count = 0;
            _dcqcn_next_rate_increase = now + _dcqcn_rate_increase_interval;
            clamp_dcqcn_rate();
            update_packet_spacing();
        }
        return;
    }

    _cc_cwnd_pkts -= 1.0;
    clamp_congestion_window();
}

void RoceSrc::clamp_dcqcn_rate() {
    double line_rate = (double)_bitrate;
    double min_rate = _dcqcn_min_rate ? (double)_dcqcn_min_rate : 1.0;

    if (_dcqcn_current_rate < min_rate)
        _dcqcn_current_rate = min_rate;
    if (_dcqcn_target_rate < min_rate)
        _dcqcn_target_rate = min_rate;
    if (_dcqcn_current_rate > line_rate)
        _dcqcn_current_rate = line_rate;
    if (_dcqcn_target_rate > line_rate)
        _dcqcn_target_rate = line_rate;

    if (_dcqcn_alpha < 0.0)
        _dcqcn_alpha = 0.0;
    if (_dcqcn_alpha > 1.0)
        _dcqcn_alpha = 1.0;
}

void RoceSrc::dcqcn_update_alpha_timer() {
    if (!_dcqcn_alpha_interval)
        return;

    simtime_picosec now = eventlist().now();
    while (now >= _dcqcn_next_alpha_update) {
        if (!_dcqcn_marked_since_alpha)
            _dcqcn_alpha *= (1.0 - _dcqcn_g);
        _dcqcn_marked_since_alpha = false;
        _dcqcn_next_alpha_update += _dcqcn_alpha_interval;
    }
    clamp_dcqcn_rate();
}

void RoceSrc::dcqcn_on_cnp() {
    simtime_picosec now = eventlist().now();
    _dcqcn_marked_since_alpha = true;

    if (_dcqcn_seen_cnp && _dcqcn_cnp_interval && now - _dcqcn_last_cnp < _dcqcn_cnp_interval)
        return;

    _dcqcn_target_rate = _dcqcn_current_rate;
    double cut = 1.0 - _dcqcn_alpha / 2.0;
    if (cut < 0.0)
        cut = 0.0;
    _dcqcn_current_rate *= cut;
    _dcqcn_alpha = (1.0 - _dcqcn_g) * _dcqcn_alpha + _dcqcn_g;

    _dcqcn_bytes_since_increase = 0;
    _dcqcn_recovery_count = 0;
    _dcqcn_seen_cnp = true;
    _dcqcn_last_cnp = now;
    _dcqcn_next_alpha_update = now + _dcqcn_alpha_interval;
    _dcqcn_next_rate_increase = now + _dcqcn_rate_increase_interval;

    clamp_dcqcn_rate();
    update_packet_spacing();
}

void RoceSrc::dcqcn_increase_rate() {
    if (_dcqcn_recovery_count < _dcqcn_fast_recovery_steps) {
        _dcqcn_current_rate = (_dcqcn_target_rate + _dcqcn_current_rate) / 2.0;
        _dcqcn_recovery_count++;
    } else {
        _dcqcn_target_rate += (double)_dcqcn_ai_rate;
        _dcqcn_current_rate = (_dcqcn_target_rate + _dcqcn_current_rate) / 2.0;
    }

    clamp_dcqcn_rate();
}

void RoceSrc::dcqcn_maybe_increase(double newly_acked_pkts) {
    if (newly_acked_pkts > 0.0)
        _dcqcn_bytes_since_increase += (mem_b)(newly_acked_pkts * _mss);

    simtime_picosec now = eventlist().now();
    bool changed = false;
    uint32_t updates = 0;

    while (updates < 64) {
        bool due_timer = _dcqcn_rate_increase_interval && now >= _dcqcn_next_rate_increase;
        bool due_bytes = _dcqcn_byte_counter > 0 && _dcqcn_bytes_since_increase >= _dcqcn_byte_counter;
        if (!due_timer && !due_bytes)
            break;

        dcqcn_increase_rate();
        changed = true;
        updates++;

        if (due_timer)
            _dcqcn_next_rate_increase += _dcqcn_rate_increase_interval;
        if (due_bytes)
            _dcqcn_bytes_since_increase -= _dcqcn_byte_counter;
    }

    if (changed)
        update_packet_spacing();
}

void RoceSrc::log_me() {
    // avoid looping
    if (_log_me == true)
        return;

    cout << "Enabling logging on RoceSrc " << _nodename << endl;
    _log_me = true;
    if (_sink)
        _sink->log_me();
}

void RoceSrc::SpRtxQueue::clear() {
    _seqs.clear();
}

bool RoceSrc::SpRtxQueue::empty() const {
    return _seqs.empty();
}

void RoceSrc::SpRtxQueue::insert(RocePacket::seq_t seq) {
    _seqs.insert(seq);
}

void RoceSrc::SpRtxQueue::erase(RocePacket::seq_t seq) {
    _seqs.erase(seq);
}

void RoceSrc::SpRtxQueue::erase_acked(RocePacket::seq_t last_acked) {
    while (!_seqs.empty() && *_seqs.begin() <= last_acked)
        _seqs.erase(_seqs.begin());
}

bool RoceSrc::SpRtxQueue::pop_next(RocePacket::seq_t last_acked,
                                   RocePacket::seq_t highest_sent,
                                   uint64_t flow_size,
                                   RocePacket::seq_t& seq) {
    while (!_seqs.empty()) {
        RocePacket::seq_t candidate = *_seqs.begin();
        _seqs.erase(_seqs.begin());
        if (candidate <= last_acked)
            continue;
        if (candidate > highest_sent)
            continue;
        if (flow_size && candidate > flow_size)
            continue;
        seq = candidate;
        return true;
    }
    return false;
}

size_t RoceSrc::SpRtxQueue::size() const {
    return _seqs.size();
}

void RoceSrc::bounded_note_new_send(RocePacket::seq_t psn) {
    BoundedPacketState& packet = _bounded_packets[psn];
    if (packet.state == BOUNDED_ACKED || packet.counted_inflight)
        return;
    packet.state = BOUNDED_SENT;
    packet.counted_inflight = true;
    _bounded_inflight_pkts++;
}

uint64_t RoceSrc::bounded_mark_acked(RocePacket::seq_t psn) {
    map<RocePacket::seq_t, BoundedPacketState>::iterator it =
        _bounded_packets.find(psn);
    if (it == _bounded_packets.end())
        return 0;
    if (it->second.state == BOUNDED_ACKED) {
        _bounded_duplicate_confirmations_suppressed++;
        return 0;
    }
    if (it->second.counted_inflight) {
        assert(_bounded_inflight_pkts > 0);
        _bounded_inflight_pkts--;
        it->second.counted_inflight = false;
    }
    if (it->second.uses_recovery_reserve) {
        assert(_bounded_recovery_inflight_bytes >= _mss);
        _bounded_recovery_inflight_bytes -= _mss;
        it->second.uses_recovery_reserve = false;
    }
    it->second.state = BOUNDED_ACKED;
    _rtx_queue.erase(psn);
    _bounded_unique_acks++;
    return 1;
}

bool RoceSrc::bounded_queue_failure(RocePacket::seq_t psn,
                                    uint8_t attempt_id) {
    map<RocePacket::seq_t, BoundedPacketState>::iterator it =
        _bounded_packets.find(psn);
    if (it == _bounded_packets.end())
        return false;
    if (it->second.state == BOUNDED_ACKED) {
        _bounded_acked_revival_rejected++;
        return false;
    }
    if (it->second.attempt_id != attempt_id) {
        _bounded_stale_attempt_nacks++;
        return false;
    }
    if (it->second.state == BOUNDED_RTX_PENDING ||
        !it->second.counted_inflight) {
        _bounded_duplicate_failure_nacks++;
        return false;
    }

    assert(_bounded_inflight_pkts > 0);
    _bounded_inflight_pkts--;
    it->second.counted_inflight = false;
    if (it->second.uses_recovery_reserve) {
        assert(_bounded_recovery_inflight_bytes >= _mss);
        _bounded_recovery_inflight_bytes -= _mss;
        it->second.uses_recovery_reserve = false;
    }
    it->second.state = BOUNDED_RTX_PENDING;
    _rtx_queue.insert(psn);
    return true;
}

bool RoceSrc::bounded_begin_retransmission(
        RocePacket::seq_t psn, uint8_t& attempt_id,
        bool& used_recovery_reserve) {
    map<RocePacket::seq_t, BoundedPacketState>::iterator it =
        _bounded_packets.find(psn);
    if (it == _bounded_packets.end() ||
        it->second.state != BOUNDED_RTX_PENDING)
        return false;

    used_recovery_reserve = false;
    if (!congestion_window_allows_send()) {
        if (_bounded_recovery_inflight_bytes != 0)
            return false;
        used_recovery_reserve = true;
        _bounded_recovery_inflight_bytes = _mss;
        if (_bounded_recovery_inflight_bytes >
            _bounded_recovery_inflight_max_bytes) {
            _bounded_recovery_inflight_max_bytes =
                _bounded_recovery_inflight_bytes;
        }
    }

    if (it->second.attempt_id == UINT8_MAX)
        _bounded_attempt_wraps++;
    it->second.attempt_id = (uint8_t)(it->second.attempt_id + 1);
    it->second.state = BOUNDED_RTX_INFLIGHT;
    it->second.counted_inflight = true;
    it->second.uses_recovery_reserve = used_recovery_reserve;
    _bounded_inflight_pkts++;
    _rtx_queue.erase(psn);
    attempt_id = it->second.attempt_id;
    return true;
}

bool RoceSrc::bounded_expire_recovery_reserve() {
    if (_bounded_recovery_inflight_bytes == 0)
        return false;
    for (map<RocePacket::seq_t, BoundedPacketState>::iterator it =
             _bounded_packets.begin();
         it != _bounded_packets.end(); ++it) {
        if (it->second.state == BOUNDED_RTX_INFLIGHT &&
            it->second.uses_recovery_reserve) {
            return bounded_queue_failure(it->first, it->second.attempt_id);
        }
    }
    assert(false && "bounded recovery reserve has no owning packet");
    return false;
}

uint64_t RoceSrc::bounded_mark_cumulative_acked(
        RocePacket::seq_t old_cack, RocePacket::seq_t new_cack) {
    if (new_cack <= old_cack)
        return 0;
    uint64_t confirmed = 0;
    for (RocePacket::seq_t psn = old_cack + 1; psn <= new_cack;
         psn += _mss) {
        confirmed += bounded_mark_acked(psn);
    }
    return confirmed;
}

uint64_t RoceSrc::bounded_process_sack(const RoceNack& nack) {
    if (!nack.has_sack())
        return 0;
    RocePacket::seq_t start = nack.sack_bitmap_start_psn();
    if (start == 0)
        start = nack.ackno() + 1 + (uint64_t)nack.sack_offset() * _mss;
    uint16_t valid = nack.sack_bitmap_valid_length();
    if (valid > _sack_bitmap_bits)
        valid = _sack_bitmap_bits;
    uint64_t confirmed = 0;
    for (uint32_t bit = 0; bit < valid; bit++) {
        if (!nack.sack_bit(bit))
            continue;
        RocePacket::seq_t psn = start + (uint64_t)bit * _mss;
        if (psn <= _highest_sent && (!_flow_size || psn <= _flow_size))
            confirmed += bounded_mark_acked(psn);
    }
    return confirmed;
}

bool RoceSrc::has_retransmit_work() const {
    if (_rx_mode != RX_SP_RETX_QUEUE || _rtx_queue.empty())
        return false;
    if (_transport_semantics != TRANSPORT_MRC_EXACT_BOUNDED ||
        _cc_mode == CC_NONE) {
        return true;
    }
    return congestion_window_available() >= 1.0 ||
        _bounded_recovery_inflight_bytes == 0;
}

void RoceSrc::clean_retransmit_queue() {
    _rtx_queue.erase_acked(_last_acked);
}

void RoceSrc::reset_sack_recovery_state() {
    _sack_rxt_psn = 0;
    _sack_rxt_psn_updated = 0;
    _sack_rxt_psn_valid = false;
}

simtime_picosec RoceSrc::sack_rxtpsn_reset_interval() const {
    if (_rtt > 0)
        return _rtt;
    if (_base_rtt != timeInf)
        return _base_rtt;
    if (_ooo_tolerance > 0)
        return _ooo_tolerance;
    return _min_rto;
}

void RoceSrc::maybe_reset_sack_rxtpsn() {
    if (!_sack_rxt_psn_valid)
        return;
    simtime_picosec threshold = sack_rxtpsn_reset_interval();
    if (threshold == 0)
        return;
    if (eventlist().now() - _sack_rxt_psn_updated >= threshold)
        _sack_rxt_psn_valid = false;
}

bool RoceSrc::sack_seq_blocked_by_rxtpsn(RocePacket::seq_t seq) const {
    return _sack_rxt_psn_valid && seq <= _sack_rxt_psn;
}

void RoceSrc::update_sack_rxtpsn(RocePacket::seq_t psn) {
    if (!_sack_rxt_psn_valid || psn > _sack_rxt_psn) {
        _sack_rxt_psn = psn;
        _sack_rxt_psn_valid = true;
        _sack_rxt_psn_updated = eventlist().now();
    }
}

void RoceSrc::startflow(){
    cout << "startflow " << _flow._name << " at " << timeAsUs(eventlist().now()) << endl;
    _flow_started = true;
    _highest_sent = 0;
    _last_acked = 0;
    _rtx_queue.clear();
    _bounded_packets.clear();
    _bounded_inflight_pkts = 0;
    _bounded_unique_acks = 0;
    _bounded_recovery_inflight_bytes = 0;
    _bounded_recovery_inflight_max_bytes = 0;
    _bounded_stale_attempt_nacks = 0;
    _bounded_duplicate_failure_nacks = 0;
    _bounded_duplicate_confirmations_suppressed = 0;
    _bounded_acked_revival_rejected = 0;
    _bounded_attempt_wraps = 0;
    _bounded_exact_trim_recoveries = 0;
    _bounded_sack_loss_recoveries = 0;
    reset_sack_recovery_state();
    _rto = _min_rto;
    _rtx_timeout = timeInf;
    
    _acked_packets = 0;
    _packets_sent = 0;
    _done = false;
    reset_mrc_flow_metrics();
    reset_congestion_control();
    if (_flow_lb_mode == LB_REPS)
        reset_reps_buffer();
    if (_flow_lb_mode == LB_RR)
        reset_rr_paths();
    if (mrc_path_state_enabled())
        reset_mrc_paths();
    if (_flow_lb_mode == LB_NMRC)
        reset_nmrc_evs();
    if (_flow_lb_mode == LB_NDP) {
        _ndp_pull_credit = _ndp_initial_window;
        _ndp_paths_ready = false;
    }
    
    schedule_send_now();
}

void RoceSrc::set_end_trigger(Trigger& end_trigger) {
    _end_trigger = &end_trigger;
}

void RoceSrc::connect(Route* routeout, Route* routeback, RoceSink& sink, simtime_picosec starttime) {
    assert(routeout);
    _route = routeout;
    
    _sink = &sink;
    _flow.set_id(get_id()); // identify the packet flow with the ROCE source that generated it
    _flow._name = _name;
    _sink->connect(*this, routeback);

    if (starttime != TRIGGER_START) {
        if (starttime == 0)
            startflow();
        else
            schedule_send(starttime);
    }
    //else cout << "TRIGGER START " << _nodename << endl; 
}

/* Process a NACK.  Generally this involves queuing the NACKed packet
   for retransmission, but then waiting for a PULL to actually resend
   it.  However, sometimes the NACK has the PULL bit set, and then we
   resend immediately */
void RoceSrc::processNack(const RoceNack& nack){
    RoceNack::seq_t ackno = nack.ackno();
    trace_cc_state("nack_pre", ackno, (int)nack.reason());
    if (nack.reason() == RoceNack::LOSS || nack.reason() == RoceNack::TRIM)
        detect_reps_failure();
    if (nack.has_stor_feedback())
        _feedback_nacks_received++;
    switch (nack.reason()) {
    case RoceNack::OOO:
        _ooo_nacks_received++;
        break;
    case RoceNack::TRIM:
        _trim_nacks_received++;
        if (nack.trim_is_lh())
            _trim_lh_nacks_received++;
        break;
    case RoceNack::LOSS:
        _loss_nacks_received++;
        break;
    }
    if (_transport_semantics == TRANSPORT_MRC_EXACT_BOUNDED &&
        _rx_mode == RX_SP_RETX_QUEUE) {
        RocePacket::seq_t old_cack = _last_acked;
        if (ackno > _last_acked)
            _last_acked = ackno;

        // Positive evidence is authoritative and must be applied before the
        // failure carried by the same feedback packet.
        uint64_t newly_confirmed =
            bounded_mark_cumulative_acked(old_cack, _last_acked);
        newly_confirmed += bounded_process_sack(nack);
        bool feedback_made_progress = _last_acked > old_cack ||
            newly_confirmed > 0;

        bool failure_accepted = false;
        if (nack.reason() == RoceNack::TRIM) {
            bool exact_is_valid = nack.has_missing_psn() &&
                nack.has_attempt_id() &&
                (nack.missing_psn() - 1) % _mss == 0 &&
                nack.missing_psn() <= _highest_sent &&
                (!_flow_size || nack.missing_psn() <= _flow_size);
            if (exact_is_valid) {
                failure_accepted = bounded_queue_failure(
                    nack.missing_psn(), nack.attempt_id());
                if (failure_accepted)
                    _bounded_exact_trim_recoveries++;
                process_nmrc_trim_feedback(nack, failure_accepted);
            }
        } else {
            // An OOO NACK is emitted only after the receiver's tolerance or
            // distance window.  Recover the cumulative first hole, not every
            // zero in a single bitmap.
            RocePacket::seq_t first_missing = _last_acked + 1;
            map<RocePacket::seq_t, BoundedPacketState>::const_iterator it =
                _bounded_packets.find(first_missing);
            if (it != _bounded_packets.end()) {
                failure_accepted = bounded_queue_failure(
                    first_missing, it->second.attempt_id);
                if (failure_accepted)
                    _bounded_sack_loss_recoveries++;
            }
        }

        if (failure_accepted)
            update_congestion_control_on_nack();
        update_stor(nack);
        update_mrc_on_nack(nack);
        if (feedback_made_progress || failure_accepted)
            reset_rtx_timeout();
        if (_state_send == READY &&
            (has_retransmit_work() || congestion_window_allows_send()))
            schedule_send_now();
        trace_cc_state("nack_post", ackno, (int)nack.reason());
        return;
    }
    const auto exact_missing_psn_is_valid = [&]() {
        return _trim_recovery_mode == TRIM_RECOVERY_EXACT_PSN &&
            nack.reason() == RoceNack::TRIM &&
            nack.has_missing_psn() &&
            (nack.missing_psn() - 1) % _mss == 0 &&
            nack.missing_psn() > _last_acked &&
            nack.missing_psn() <= _highest_sent &&
            (!_flow_size || nack.missing_psn() <= _flow_size);
    };
    if (_rx_mode == RX_SP_RETX_QUEUE && ackno < _last_acked) {
        maybe_reset_sack_rxtpsn();
        if (exact_missing_psn_is_valid() &&
            !sack_seq_blocked_by_rxtpsn(nack.missing_psn())) {
            _rtx_queue.insert(nack.missing_psn());
            if (_state_send == READY)
                schedule_send_now();
        }
        trace_cc_state("nack_stale", ackno, (int)nack.reason());
        return;
    }
    if (ackno > _last_acked)
        _last_acked = ackno;

    if (_rx_mode == RX_SP_RETX_QUEUE) {
        clean_retransmit_queue();

        RocePacket::seq_t first_missing = _last_acked + 1;

        if (nack.has_sack()) {
            maybe_reset_sack_rxtpsn();

            {
                RocePacket::seq_t sack_start = nack.sack_bitmap_start_psn();
                if (sack_start == 0)
                    sack_start = first_missing + (uint64_t)nack.sack_offset() * _mss;

                if (sack_start > first_missing) {
                    for (RocePacket::seq_t seq = first_missing; seq < sack_start; seq += _mss) {
                        if (seq <= _last_acked)
                            continue;
                        if (seq > _highest_sent)
                            break;
                        if (_flow_size && seq > _flow_size)
                            break;
                        if (!sack_seq_blocked_by_rxtpsn(seq))
                            _rtx_queue.insert(seq);
                    }
                }

                uint16_t valid_length = nack.sack_bitmap_valid_length();
                if (valid_length > _sack_bitmap_bits)
                    valid_length = _sack_bitmap_bits;

                if (valid_length == 0) {
                    if (first_missing <= _highest_sent &&
                        (!_flow_size || first_missing <= _flow_size) &&
                        !sack_seq_blocked_by_rxtpsn(first_missing)) {
                        _rtx_queue.insert(first_missing);
                    }
                }

                for (uint32_t bit = 0; bit < valid_length; bit++) {
                    RocePacket::seq_t seq = sack_start + (uint64_t)bit * _mss;
                    if (seq <= _last_acked)
                        continue;
                    if (seq > _highest_sent)
                        break;
                    if (_flow_size && seq > _flow_size)
                        break;

                    if (nack.sack_bit(bit)) {
                        _rtx_queue.erase(seq);
                    } else if (!sack_seq_blocked_by_rxtpsn(seq)) {
                        _rtx_queue.insert(seq);
                    }
                }

                if (valid_length > 0)
                    update_sack_rxtpsn(sack_start + (uint64_t)(valid_length - 1) * _mss);
            }
        } else if (first_missing <= _highest_sent &&
                   (!_flow_size || first_missing <= _flow_size)) {
            _rtx_queue.insert(first_missing);
        }

        if (exact_missing_psn_is_valid() &&
            !sack_seq_blocked_by_rxtpsn(nack.missing_psn())) {
            _rtx_queue.insert(nack.missing_psn());
        }

        if (_log_me)
            cout << "Src " << get_id() << " queued " << _rtx_queue.size()
                 << " selective retransmissions after nack " << _last_acked
                 << " sack_offset " << (nack.has_sack() ? nack.sack_offset() : 0)
                 << " at " << timeAsUs(eventlist().now()) << " us" << endl;
    } else {
        uint64_t old_highest = _highest_sent;
        if (_last_acked < _highest_sent)
            _rtx_packets_sent += (_highest_sent - _last_acked + _mss - 1) / _mss;

        if (_log_me)
            cout << "Src " << get_id() << " go back n from " << old_highest
                 << " to " << _last_acked << " at "
                 << timeAsUs(eventlist().now()) << " us" << endl;

        _highest_sent = _last_acked;
    }

    if (_flow_size && _highest_sent>=_flow_size && _last_acked < _flow_size){
        // restart pacing; it may have stopped once we passed the flow size.
        if (_log_me)
            cout << "Src " << get_id() << " restarting pacing\n";
        schedule_send_now();
    }

    update_congestion_control_on_nack();
    update_stor(nack);
    update_mrc_on_nack(nack);
    reset_rtx_timeout();
    if (_flow_lb_mode == LB_NDP) {
        grant_ndp_credit();
        if (_state_send == READY)
            schedule_send_now();
    }
    if (_flow_lb_mode != LB_NDP && _state_send == READY &&
        (has_retransmit_work() || congestion_window_allows_send()))
        schedule_send_now();
    trace_cc_state("nack_post", ackno, (int)nack.reason());

    //this packet be sent when it is time to send a new packet!
}

/* Process an ACK.  Mostly just housekeeping*/
void RoceSrc::processAck(const RoceAck& ack) {
    RoceAck::seq_t ackno = ack.ackno();
    simtime_picosec ts = ack.ts();
    uint64_t old_last_acked = _last_acked;
    trace_cc_state("ack_pre", ackno, -1, ack.is_duplicate_ack(),
                   ack.is_old_duplicate_ack(), ack.flags() & ECN_ECHO);

    // Compute rtt.  This comes originally from TCP, and may not be optimal for ROCE */
    uint64_t m = eventlist().now()-ts;

    if (m!=0){
        if (_rtt>0){
            uint64_t abs;
            if (m>_rtt)
                abs = m - _rtt;
            else
                abs = _rtt - m;

            _mdev = 3 * _mdev / 4 + abs/4;
            _rtt = 7*_rtt/8 + m/8;

            _rto = _rtt + 4*_mdev;
        } else {
            _rtt = m;
            _mdev = m/2;
            _rto = _rtt + 4*_mdev;
        }
        if (_base_rtt==timeInf || _base_rtt > m)
            _base_rtt = m;
    }

    if (_rto < _min_rto)
        _rto = _min_rto * ((drand() * 0.5) + 0.75);

    double newly_acked_pkts = 0;
    if (ackno > _last_acked) { // a brand new ack    
        // we should probably cancel the rtx timer for any acked by
        // the cumulative ack, but we'll get an ACK or NACK anyway in
        // due course.
        _last_acked = ackno;
        clean_retransmit_queue();
        _sack_rxt_psn_valid = false;
        newly_acked_pkts = ((double)(ackno - old_last_acked)) / _mss;
        reset_rtx_timeout();
    }
    if (_transport_semantics == TRANSPORT_MRC_EXACT_BOUNDED) {
        uint64_t newly_confirmed = 0;
        if (ackno > old_last_acked) {
            RocePacket::seq_t first = old_last_acked + 1;
            for (RocePacket::seq_t psn = first; psn <= ackno; psn += _mss)
                newly_confirmed += bounded_mark_acked(psn);
        }
        if (ack.has_delivered_psn())
            newly_confirmed += bounded_mark_acked(ack.delivered_psn());
        newly_acked_pkts = (double)newly_confirmed;
    }
    if (_logger) _logger->logRoce(*this, RoceLogger::ROCE_RCV);

    if (ack.flags() & ECN_ECHO)
        _ecn_echo_acks_received++;
    if (ack.is_duplicate_ack())
        _duplicate_acks_received++;
    bool has_feedback_ack = ack.has_stor_feedback() || ack.has_netaware_feedback();
    if (has_feedback_ack) {
        _feedback_acks_received++;
    }
    if (_flow_lb_mode == LB_NETAWARE && ack.has_netaware_feedback()) {
        const NetawareFeedbackLevels& levels = ack.netaware_feedback();
        for (uint32_t i = 0; i < levels.size(); i++) {
            if (levels[i] >= STOR_LEVEL_BAD)
                _feedback_zero_bits_received++;
        }
    }

    if (!ack.neutral_ecn())
        update_congestion_control_on_ack(ack, newly_acked_pkts);
    trace_cc_state("ack_post", ackno, -1, ack.is_duplicate_ack(),
                   ack.is_old_duplicate_ack(), ack.flags() & ECN_ECHO,
                   newly_acked_pkts);
    update_reps(ack);
    update_mprdma(ack);
    update_conweave(ack, m);
    update_stor(ack);
    update_netaware(ack);
    update_mrc_on_ack(ack);
    if (_flow_lb_mode == LB_NDP && !_done) {
        grant_ndp_credit();
        if (_state_send == READY)
            schedule_send_now();
    }
    if (_flow_lb_mode != LB_NDP && !_done && _state_send == READY &&
        (has_retransmit_work() || congestion_window_allows_send()))
        schedule_send_now();

    if (_log_me)
        cout << "Src " << get_id() << " ackno " << ackno << endl;
    if (ackno >= _flow_size){
        cout << "Flow " << _name << " " << get_id() << " finished at "
             << timeAsUs(eventlist().now()) << " total bytes " << ackno
             << " bg traffic " << (background_traffic() ? 1 : 0)
             << " flowid " << flow_id() << endl;
        emit_mrc_flow_diag();
        _done = true;
        reset_nmrc_all_cooling_rr_episode();
        _rtx_timeout = timeInf;
        if (_end_trigger) {
            _end_trigger->activate();
        }

        return;
    }
}

void RoceSrc::init_selector_priority(Packet::PktPriority priority, uint32_t path_space) {
    uint32_t prio = selector_priority_index(priority);
    if (_selector_cursor_ready[prio])
        return;

    uint32_t src = _srcaddr == UINT32_MAX ? _node_num : _srcaddr;
    uint32_t dst = _dstaddr == UINT32_MAX ? (_node_num ^ 0x5bd1e995) : _dstaddr;
    uint32_t flow = _flow.flow_id();
    uint32_t seed = selector_mix(src, dst, flow ^ (prio * 0x9e3779b9));

    _selector_cursor[prio] = seed % path_space;

    uint32_t stride = ((seed >> 8) % path_space) | 1;
    if (stride == 0)
        stride = 1;
    while (selector_gcd(stride, path_space) != 1)
        stride = (stride + 2) % path_space;
    if (stride == 0)
        stride = 1;
    _selector_stride[prio] = stride;
    _selector_cursor_ready[prio] = true;
}

std::pair<uint32_t, uint32_t> RoceSrc::tor_pair_cache_key() const {
    uint32_t hosts_per_tor = _hosts_per_tor ? _hosts_per_tor : 1;
    uint32_t src = _srcaddr == UINT32_MAX ? _node_num : _srcaddr;
    uint32_t dst = _dstaddr == UINT32_MAX ? 0 : _dstaddr;
    return std::make_pair(src / hosts_per_tor, dst / hosts_per_tor);
}

void RoceSrc::resetStorSharedState() {
    _stor_shared_profiles.clear();
}

void RoceSrc::resetCongaSharedState() {
    _conga_shared_uplink_scores.clear();
    _conga_local_uplink_scores.clear();
}

void RoceSrc::updateCongaLocalMetric(uint32_t source_tor, uint32_t uplink,
                                    uint32_t metric) {
    const uint32_t uplinks = _conga_uplinks ? _conga_uplinks : 1;
    std::vector<uint32_t>& scores = _conga_local_uplink_scores[source_tor];
    if (scores.size() != uplinks)
        scores.assign(uplinks, 0);
    scores[uplink % uplinks] = metric;
}

uint32_t RoceSrc::congaRemoteMetric(uint32_t source_tor, uint32_t destination_tor,
                                    uint32_t uplink) {
    const uint32_t uplinks = _conga_uplinks ? _conga_uplinks : 1;
    std::map<std::pair<uint32_t, uint32_t>, std::vector<uint32_t> >::const_iterator it =
        _conga_shared_uplink_scores.find(std::make_pair(source_tor, destination_tor));
    if (it == _conga_shared_uplink_scores.end() || it->second.size() != uplinks)
        return 0;
    return it->second[uplink % uplinks];
}

void RoceSrc::resetNetawareSharedState() {
    _netaware_shared_profiles.clear();
}

void RoceSrc::resetStorProfileDiag() {
    for (uint32_t from = 0; from < 4; from++) {
        for (uint32_t to = 0; to < 4; to++) {
            _stor_level_transitions[from][to] = 0;
            _netaware_level_transitions[from][to] = 0;
        }
    }
    _stor_level_changes = 0;
    _netaware_level_changes = 0;
    _stor_all_zero_profiles = 0;
    _netaware_all_zero_profiles = 0;
    _stor_all_zero_selections = 0;
    _netaware_all_zero_selections = 0;
}

static bool all_profile_weights_zero(
        const StorFeedbackLevels& levels,
        const std::array<uint32_t, 4>& weights,
        uint32_t path_space) {
    for (uint32_t path = 0; path < path_space; path++) {
        uint8_t level = path < levels.size() ?
            levels[path] : STOR_LEVEL_GOOD;
        if (level > STOR_LEVEL_AVOID)
            level = STOR_LEVEL_GOOD;
        if (weights[level] > 0)
            return false;
    }
    return true;
}

void RoceSrc::reset_mrc_flow_metrics() {
    _mrc_flow_metrics.reset();
    _mrc_flow_metrics.start_time = eventlist().now();
}

void RoceSrc::mrc_flow_note_new_selection(uint32_t ev) {
    MrcFlowMetrics& metrics = _mrc_flow_metrics;
    metrics.new_data_selections++;
    if (metrics.first_state_update_set) {
        metrics.new_selections_after_first_update++;
        if (metrics.pending_actionable_updates) {
            metrics.actionable_feedback +=
                metrics.pending_actionable_updates;
            metrics.pending_actionable_updates = 0;
        }
    }
    if (metrics.initial_active.count(ev)) {
        metrics.unique_active.insert(ev);
        metrics.sweep_seen.insert(ev);
        if (metrics.active_evs &&
            metrics.sweep_seen.size() >= metrics.active_evs) {
            metrics.full_sweeps++;
            if (!metrics.first_full_sweep_set) {
                metrics.first_full_sweep_set = true;
                metrics.first_full_sweep_time = eventlist().now();
            }
            metrics.sweep_seen.clear();
        }
    }

}

void RoceSrc::mrc_flow_note_quality_feedback() {
    _mrc_flow_metrics.quality_feedback_before_done++;
}

simtime_picosec RoceSrc::mrc_feedback_send_time(
        RocePacket::seq_t sequence, uint32_t ev) const {
    map<RocePacket::seq_t, uint32_t>::const_iterator it =
        _mrc_seq_ev.upper_bound(sequence);
    while (it != _mrc_seq_ev.begin()) {
        --it;
        if (it->second == ev) {
            map<RocePacket::seq_t, simtime_picosec>::const_iterator sent =
                _mrc_seq_sent_at.find(it->first);
            if (sent != _mrc_seq_sent_at.end())
                return sent->second;
        }
    }
    return eventlist().now();
}

void RoceSrc::mrc_flow_note_effective_update(
        uint32_t ev, RocePacket::seq_t sequence,
        bool failure, bool replacement_congestion) {
    MrcFlowMetrics& metrics = _mrc_flow_metrics;
    metrics.effective_state_updates++;
    metrics.pending_actionable_updates++;
    if (!metrics.first_state_update_set) {
        metrics.first_state_update_set = true;
        metrics.first_state_update_time = eventlist().now();
        metrics.packets_before_first_update =
            metrics.new_data_selections;
    }
    simtime_picosec sent_at = mrc_feedback_send_time(sequence, ev);
    simtime_picosec age =
        eventlist().now() >= sent_at ? eventlist().now() - sent_at : 0;
    metrics.feedback_age_sum += age;
    metrics.feedback_age_max = std::max(metrics.feedback_age_max, age);
    if (failure)
        metrics.failure_starts++;
    else
        metrics.cooldown_starts++;
    if (replacement_congestion)
        metrics.replacement_congestion++;
    uint64_t cooling = 0;
    for (uint32_t i = 0; i < _mrc_evs.size(); i++)
        if (_mrc_evs[i].state == MRC_PATH_COOLING)
            cooling++;
    metrics.max_simultaneous_cooling =
        std::max(metrics.max_simultaneous_cooling, cooling);
}

void RoceSrc::emit_mrc_flow_diag() {
    if (_mrc_flow_metrics.emitted || background_traffic() ||
        (_flow_lb_mode != LB_RR && !mrc_path_state_enabled()))
        return;
    _mrc_flow_metrics.emitted = true;
    const MrcFlowMetrics& metrics = _mrc_flow_metrics;
    uint32_t src = _srcaddr == UINT32_MAX ? _node_num : _srcaddr;
    uint32_t dst = _dstaddr == UINT32_MAX ? 0 : _dstaddr;
    uint32_t hosts_per_tor = _hosts_per_tor ? _hosts_per_tor : 1;
    uint64_t unused = metrics.active_evs > metrics.unique_active.size() ?
        metrics.active_evs - metrics.unique_active.size() : 0;
    cout << "MrcFlowDiag"
         << " flow_id=" << flow_id()
         << " src=" << src
         << " dst=" << dst
         << " dst_tor=" << dst / hosts_per_tor
         << " flow_size=" << _flow_size
         << " start_us=" << timeAsUs(metrics.start_time)
         << " finish_us=" << timeAsUs(eventlist().now())
         << " new_data_selections=" << metrics.new_data_selections
         << " unique_active_evs=" << metrics.unique_active.size()
         << " full_sweeps=" << metrics.full_sweeps
         << " first_full_sweep_us="
         << (metrics.first_full_sweep_set ?
             timeAsUs(metrics.first_full_sweep_time) : -1.0)
         << " unused_active_evs=" << unused
         << " quality_feedback_before_done="
         << metrics.quality_feedback_before_done
         << " effective_state_updates=" << metrics.effective_state_updates
         << " first_state_update_us="
         << (metrics.first_state_update_set ?
             timeAsUs(metrics.first_state_update_time) : -1.0)
         << " packets_before_first_update="
         << metrics.packets_before_first_update
         << " new_selections_after_first_update="
         << metrics.new_selections_after_first_update
         << " actionable_feedback=" << metrics.actionable_feedback
         << " feedback_age_sum_us=" << timeAsUs(metrics.feedback_age_sum)
         << " feedback_age_max_us=" << timeAsUs(metrics.feedback_age_max)
         << " cooldown_starts=" << metrics.cooldown_starts
         << " failure_starts=" << metrics.failure_starts
         << " forced_cooling_uses=" << _mrc_forced_cooling_use
         << " max_simultaneous_cooling="
         << metrics.max_simultaneous_cooling
         << " replacement_congestion=" << metrics.replacement_congestion
         << " post_cooldown_first_clean="
         << metrics.post_cooldown_first_clean
         << endl;
}

RoceSrc::SharedWeightedProfile& RoceSrc::shared_stor_profile(
        uint32_t path_space) {
    if (path_space == 0)
        path_space = 1;
    SharedWeightedProfile& profile = _stor_shared_profiles[tor_pair_cache_key()];
    if (profile.levels.size() != path_space) {
        profile.levels.assign(path_space, STOR_LEVEL_GOOD);
        profile.version++;
        rebuild_shared_weighted_profile(profile, path_space,
                                        _stor_level_weights);
    }
    return profile;
}

RoceSrc::SharedWeightedProfile& RoceSrc::shared_netaware_profile(
        uint32_t path_space) {
    if (path_space == 0)
        path_space = 1;
    SharedWeightedProfile& profile = _netaware_shared_profiles[tor_pair_cache_key()];
    if (profile.levels.size() != path_space) {
        profile.levels.assign(path_space, STOR_LEVEL_GOOD);
        profile.version++;
        rebuild_netaware_weighted_profile(profile, path_space);
    }
    return profile;
}

void RoceSrc::apply_stor_feedback(const StorFeedbackLevels& feedback,
                                  uint32_t path_space) {
    if (path_space == 0)
        path_space = 1;
    SharedWeightedProfile& profile = shared_stor_profile(path_space);
    StorFeedbackLevels next(path_space, STOR_LEVEL_GOOD);
    for (uint32_t i = 0; i < path_space; i++) {
        if (i < feedback.size())
            next[i] = feedback[i];
    }
    if (profile.levels != next) {
        for (uint32_t path = 0; path < path_space; path++) {
            uint8_t from = path < profile.levels.size() ?
                profile.levels[path] : STOR_LEVEL_GOOD;
            uint8_t to = next[path];
            if (from > STOR_LEVEL_AVOID)
                from = STOR_LEVEL_GOOD;
            if (to > STOR_LEVEL_AVOID)
                to = STOR_LEVEL_GOOD;
            _stor_level_transitions[from][to]++;
            if (from != to)
                _stor_level_changes++;
        }
        if (all_profile_weights_zero(next, _stor_level_weights, path_space))
            _stor_all_zero_profiles++;
        profile.levels.swap(next);
        profile.version++;
        rebuild_shared_weighted_profile(profile, path_space,
                                        _stor_level_weights);
    }
}

void RoceSrc::apply_netaware_snapshot(const NetawareFeedbackLevels& feedback,
                                  uint32_t path_space) {
    if (path_space == 0)
        path_space = 1;
    SharedWeightedProfile& profile = shared_netaware_profile(path_space);
    StorFeedbackLevels next(path_space, STOR_LEVEL_GOOD);
    for (uint32_t i = 0; i < path_space; i++) {
        if (i < feedback.size())
            next[i] = feedback[i];
    }
    if (profile.levels != next) {
        for (uint32_t path = 0; path < path_space; path++) {
            uint8_t from = path < profile.levels.size() ?
                profile.levels[path] : STOR_LEVEL_GOOD;
            uint8_t to = next[path];
            if (from > STOR_LEVEL_AVOID)
                from = STOR_LEVEL_GOOD;
            if (to > STOR_LEVEL_AVOID)
                to = STOR_LEVEL_GOOD;
            _netaware_level_transitions[from][to]++;
            if (from != to)
                _netaware_level_changes++;
        }
        if (all_profile_weights_zero(next, _netaware_level_weights,
                                     path_space))
            _netaware_all_zero_profiles++;
        profile.levels.swap(next);
        profile.version++;
        rebuild_netaware_weighted_profile(profile, path_space);
    }
}

void RoceSrc::apply_stor_feedback_for_test(const StorFeedbackLevels& levels) {
    uint32_t path_space = _path_entropy_size ? _path_entropy_size : 1;
    apply_stor_feedback(levels, path_space);
}

void RoceSrc::apply_netaware_feedback_for_test(const NetawareFeedbackLevels& levels) {
    uint32_t path_space = _path_entropy_size ? _path_entropy_size : 1;
    apply_netaware_snapshot(levels, path_space);
}

std::vector<uint32_t> RoceSrc::netaware_bucket_tickets_for_test(
        Packet::PktPriority priority, uint32_t path_space) {
    (void)priority;
    if (path_space == 0)
        path_space = 1;
    return shared_netaware_profile(path_space).tickets;
}

void RoceSrc::record_netaware_selected_level(uint8_t level) {
    if (level == STOR_LEVEL_GOOD)
        _stor_selected_good++;
    else if (level == STOR_LEVEL_DEGRADED)
        _stor_selected_degraded++;
    else if (level == STOR_LEVEL_BAD)
        _stor_selected_bad++;
    else
        _stor_selected_avoid++;
}

void RoceSrc::record_path_selection(uint32_t selected_ev, uint32_t physical_path) {
    _diag_selected_total++;
    _diag_selected_ev_hist[selected_ev]++;
    uint32_t physical_space = _diag_physical_path_space ?
        _diag_physical_path_space : 1;
    _diag_selected_physical_hist[physical_path % physical_space]++;
    _path_selection_last_event_time = eventlist().now();
    if (_diag_first_selected_evs.size() < 128)
        _diag_first_selected_evs.push_back(selected_ev);
    if (_path_selection_timeline && _diag_selected_total >=
        _path_selection_timeline_next) {
        writePathSelectionTimeline(eventlist().now());
        _path_selection_timeline_next =
            _diag_selected_total + _path_selection_timeline_every;
    }
}

void RoceSrc::sample_reps_buffer_occupancy() {
    _reps_buffer_occupancy_samples++;
    _reps_buffer_occupancy_hist[_reps_valid_count]++;
}

std::array<uint32_t, 5> RoceSrc::mrc_state_counts_for_diag() const {
    std::array<uint32_t, 5> counts = {{0, 0, 0, 0, 0}};
    for (uint32_t i = 0; i < _mrc_evs.size(); i++) {
        uint8_t state = _mrc_evs[i].state;
        if (state < counts.size())
            counts[state]++;
    }
    return counts;
}

std::array<uint32_t, 5> RoceSrc::mrc_state_physical_counts_for_diag() const {
    std::array<std::set<uint32_t>, 5> physicals;
    for (uint32_t i = 0; i < _mrc_evs.size(); i++) {
        uint8_t state = _mrc_evs[i].state;
        if (state < physicals.size())
            physicals[state].insert(_mrc_evs[i].physical_path);
    }

    std::array<uint32_t, 5> counts = {{0, 0, 0, 0, 0}};
    for (uint32_t state = 0; state < counts.size(); state++)
        counts[state] = (uint32_t)physicals[state].size();
    return counts;
}

uint32_t RoceSrc::mrc_backup_remaining_for_diag() const {
    return 0;
}

void RoceSrc::sample_mrc_state_counts() {
    std::array<uint32_t, 5> counts = mrc_state_counts_for_diag();
    std::array<uint32_t, 5> physical_counts = mrc_state_physical_counts_for_diag();
    uint32_t backup = mrc_backup_remaining_for_diag();
    _mrc_state_samples++;
    _mrc_active_count_sum += counts[MRC_PATH_ACTIVE];
    _mrc_backup_count_sum += backup;
    _mrc_cooling_count_sum += counts[MRC_PATH_COOLING];
    _mrc_failed_count_sum += counts[MRC_PATH_FAILED];
    _mrc_active_physical_count_sum += physical_counts[MRC_PATH_ACTIVE];
    _mrc_cooling_physical_count_sum += physical_counts[MRC_PATH_COOLING];
    _mrc_failed_physical_count_sum += physical_counts[MRC_PATH_FAILED];
    _mrc_active_count_max = std::max(_mrc_active_count_max,
                                     counts[MRC_PATH_ACTIVE]);
    _mrc_backup_count_max = std::max(_mrc_backup_count_max, backup);
    _mrc_cooling_count_max = std::max(_mrc_cooling_count_max,
                                      counts[MRC_PATH_COOLING]);
    _mrc_failed_count_max = std::max(_mrc_failed_count_max,
                                     counts[MRC_PATH_FAILED]);
    _mrc_active_physical_count_max = std::max(_mrc_active_physical_count_max,
                                              physical_counts[MRC_PATH_ACTIVE]);
    _mrc_cooling_physical_count_max = std::max(_mrc_cooling_physical_count_max,
                                               physical_counts[MRC_PATH_COOLING]);
    _mrc_failed_physical_count_max = std::max(_mrc_failed_physical_count_max,
                                              physical_counts[MRC_PATH_FAILED]);
}

void RoceSrc::rebuild_shared_weighted_profile(
        SharedWeightedProfile& profile, uint32_t path_space,
        const std::array<uint32_t, 4>& level_weights) {
    if (path_space == 0)
        path_space = 1;
    uint32_t bucket_size = weightedShuffledBucketSize(path_space);
    profile.bucket.clear();
    profile.tickets.assign(path_space, 0);

    std::vector<uint32_t> weights(path_space, 0);
    uint32_t active_gcd = 0;
    uint32_t active_paths = 0;
    for (uint32_t i = 0; i < path_space; i++) {
        uint8_t level = i < profile.levels.size() ?
            profile.levels[i] : STOR_LEVEL_GOOD;
        if (level > STOR_LEVEL_AVOID)
            level = STOR_LEVEL_GOOD;
        uint32_t weight = level_weights[level];
        if (weight == 0)
            continue;
        active_gcd = active_gcd ? selector_gcd(active_gcd, weight) : weight;
        active_paths++;
    }

    if (active_gcd > 0) {
        uint64_t total_weight = 0;
        for (uint32_t i = 0; i < path_space; i++) {
            uint8_t level = i < profile.levels.size() ?
                profile.levels[i] : STOR_LEVEL_GOOD;
            if (level > STOR_LEVEL_AVOID)
                level = STOR_LEVEL_GOOD;
            uint32_t weight = level_weights[level];
            if (weight > 0)
                weight /= active_gcd;
            weights[i] = weight;
            total_weight += weight;
        }

        struct Remainder {
            uint32_t path;
            uint64_t remainder;
            uint32_t weight;
        };
        std::vector<Remainder> remainders;
        uint32_t assigned = 0;
        for (uint32_t i = 0; i < path_space; i++) {
            if (weights[i] == 0)
                continue;
            uint64_t numerator = (uint64_t)bucket_size * weights[i];
            profile.tickets[i] = (uint32_t)(numerator / total_weight);
            assigned += profile.tickets[i];
            Remainder r;
            r.path = i;
            r.remainder = numerator % total_weight;
            r.weight = weights[i];
            remainders.push_back(r);
        }

        if (active_paths <= bucket_size) {
            for (uint32_t i = 0; i < remainders.size(); i++) {
                uint32_t path = remainders[i].path;
                if (profile.tickets[path] == 0) {
                    profile.tickets[path] = 1;
                    assigned++;
                }
            }
        }

        std::sort(remainders.begin(), remainders.end(),
                  [](const Remainder& a, const Remainder& b) {
                      if (a.remainder != b.remainder)
                          return a.remainder > b.remainder;
                      return a.path < b.path;
                  });

        uint32_t remaining = assigned < bucket_size ?
            bucket_size - assigned : 0;
        for (uint32_t i = 0; i < remaining && !remainders.empty(); i++)
            profile.tickets[remainders[i % remainders.size()].path]++;

        while (assigned > bucket_size) {
            uint32_t donor = path_space;
            for (uint32_t i = path_space; i > 0; i--) {
                uint32_t path = i - 1;
                if (profile.tickets[path] > 1) {
                    donor = path;
                    break;
                }
            }
            if (donor == path_space)
                break;
            profile.tickets[donor]--;
            assigned--;
        }

        for (uint32_t i = 0; i < path_space; i++) {
            for (uint32_t n = 0; n < profile.tickets[i]; n++)
                profile.bucket.push_back(i);
        }
    }
}

void RoceSrc::rebuild_netaware_weighted_profile(
        SharedWeightedProfile& profile, uint32_t path_space) {
    if (_netaware_weight_adaptation == NETAWARE_WEIGHT_ADAPTATION_OFF) {
        rebuild_shared_weighted_profile(profile, path_space,
                                        _netaware_level_weights);
        return;
    }

    if (path_space == 0)
        path_space = 1;
    const uint32_t bucket_size = weightedShuffledBucketSize(path_space);
    profile.bucket.clear();
    profile.tickets.assign(path_space, 0);

    std::vector<long double> target(path_space, 0.0L);
    const long double uniform = 1.0L / (long double)path_space;
    long double raw_sum = 0.0L;
    for (uint32_t path = 0; path < path_space; path++) {
        uint8_t level = path < profile.levels.size() ?
            profile.levels[path] : STOR_LEVEL_GOOD;
        if (level > STOR_LEVEL_AVOID)
            level = STOR_LEVEL_GOOD;
        target[path] = (long double)_netaware_level_weights[level];
        raw_sum += target[path];
    }
    if (raw_sum > 0.0L) {
        for (uint32_t path = 0; path < path_space; path++)
            target[path] /= raw_sum;
    } else {
        for (uint32_t path = 0; path < path_space; path++)
            target[path] = uniform;
    }

    long double alpha = 1.0L;
    uint32_t good_paths = 0;
    long double max_good_target = 0.0L;
    for (uint32_t path = 0; path < path_space; path++) {
        uint8_t level = path < profile.levels.size() ?
            profile.levels[path] : STOR_LEVEL_GOOD;
        if (level > STOR_LEVEL_AVOID)
            level = STOR_LEVEL_GOOD;
        if (level == STOR_LEVEL_GOOD) {
            good_paths++;
            max_good_target = std::max(max_good_target, target[path]);
        }
    }
    if (good_paths > 0 && max_good_target > uniform) {
        const long double good_fraction =
            (long double)good_paths / (long double)path_space;
        const long double good_cap =
            (1.0L + good_fraction) / (long double)path_space;
        if (max_good_target > good_cap) {
            alpha = (good_cap - uniform) /
                (max_good_target - uniform);
        }
    }
    alpha = std::max(0.0L, std::min(1.0L, alpha));

    struct Remainder {
        uint32_t path;
        long double fraction;
    };
    std::vector<Remainder> remainders;
    uint32_t assigned = 0;
    for (uint32_t path = 0; path < path_space; path++) {
        long double probability =
            uniform + alpha * (target[path] - uniform);
        long double exact = (long double)bucket_size * probability;
        uint32_t tickets = (uint32_t)floorl(exact);
        profile.tickets[path] = tickets;
        assigned += tickets;
        Remainder remainder;
        remainder.path = path;
        remainder.fraction = exact - (long double)tickets;
        remainders.push_back(remainder);
    }

    std::sort(remainders.begin(), remainders.end(),
              [](const Remainder& a, const Remainder& b) {
                  if (a.fraction != b.fraction)
                      return a.fraction > b.fraction;
                  return a.path < b.path;
              });
    for (uint32_t i = assigned; i < bucket_size; i++)
        profile.tickets[remainders[(i - assigned) % remainders.size()].path]++;

    for (uint32_t path = 0; path < path_space; path++) {
        for (uint32_t ticket = 0; ticket < profile.tickets[path]; ticket++)
            profile.bucket.push_back(path);
    }
}

static uint32_t virtual_feistel(uint32_t value, uint32_t half_bits,
                                uint32_t key) {
    uint32_t mask = ((uint32_t)1 << half_bits) - 1;
    uint32_t left = value >> half_bits;
    uint32_t right = value & mask;
    for (uint32_t round = 0; round < 6; round++) {
        uint32_t mixed = selector_mix(key ^ (round * 0x9e3779b9), right,
                                  0x85ebca6b ^ round) & mask;
        uint32_t next_left = right;
        uint32_t next_right = left ^ mixed;
        left = next_left;
        right = next_right;
    }
    return (left << half_bits) | right;
}

uint32_t RoceSrc::virtual_shuffle_index(uint32_t key, uint64_t epoch,
                                        uint32_t position,
                                        uint32_t domain) {
    if (domain <= 1)
        return 0;
    uint32_t bits = 0;
    uint64_t size = 1;
    while (size < domain) {
        size <<= 1;
        bits++;
    }
    if (bits & 1) {
        size <<= 1;
        bits++;
    }
    uint32_t half_bits = bits / 2;
    uint32_t epoch_key = selector_mix(key, (uint32_t)epoch,
                                  (uint32_t)(epoch >> 32));
    uint32_t value = position;
    do {
        value = virtual_feistel(value, half_bits, epoch_key);
    } while (value >= domain);
    return value;
}

uint32_t RoceSrc::virtualShuffleIndexForTest(uint32_t key, uint64_t epoch,
                                             uint32_t position,
                                             uint32_t domain) {
    return virtual_shuffle_index(key, epoch, position, domain);
}

uint32_t RoceSrc::virtual_selector_key(uint32_t prio,
                                       uint64_t profile_version,
                                       uint64_t epoch,
                                       uint32_t mode_tag) const {
    uint32_t src = _srcaddr == UINT32_MAX ? _node_num : _srcaddr;
    uint32_t dst = _dstaddr == UINT32_MAX ?
        (_node_num ^ 0x5bd1e995) : _dstaddr;
    uint32_t key = selector_mix(src, dst,
                            _flow.flow_id() ^ (prio * 0x9e3779b9));
    key = selector_mix(key, (uint32_t)profile_version,
                   (uint32_t)(profile_version >> 32) ^ mode_tag);
    return selector_mix(key, (uint32_t)epoch,
                    (uint32_t)(epoch >> 32) ^ mode_tag);
}

void RoceSrc::update_stor(const RoceAck& ack) {
    if (_flow_lb_mode != LB_STOR || !ack.has_stor_feedback())
        return;

    uint32_t path_space = _path_entropy_size ? _path_entropy_size : 1;
    apply_stor_feedback(ack.stor_feedback(), path_space);
}

void RoceSrc::update_stor(const RoceNack& nack) {
    if (_flow_lb_mode != LB_STOR || !nack.has_stor_feedback())
        return;

    uint32_t path_space = _path_entropy_size ? _path_entropy_size : 1;
    apply_stor_feedback(nack.stor_feedback(), path_space);
}

uint32_t RoceSrc::choose_netaware_direct_path(uint32_t prio, uint32_t path_space) {
    const StorFeedbackLevels& levels = shared_netaware_profile(path_space).levels;
    uint64_t total_weight = 0;
    for (uint32_t i = 0; i < path_space; i++) {
        uint8_t level = i < levels.size() ? levels[i] : STOR_LEVEL_GOOD;
        if (level > STOR_LEVEL_AVOID)
            level = STOR_LEVEL_GOOD;
        total_weight += _netaware_level_weights[level];
    }

    if (total_weight > 0) {
        uint64_t ticket = _stor_weight_cursor[prio]++ % total_weight;
        for (uint32_t offset = 1; offset <= path_space; offset++) {
            uint32_t candidate = (_selector_cursor[prio] + offset * _selector_stride[prio]) % path_space;
            uint8_t level = candidate < levels.size() ?
                levels[candidate] : STOR_LEVEL_GOOD;
            if (level > STOR_LEVEL_AVOID)
                level = STOR_LEVEL_GOOD;
            uint32_t weight = _netaware_level_weights[level];
            if (ticket < weight) {
                _selector_cursor[prio] = candidate;
                record_netaware_selected_level(level);
                return candidate;
            }
            ticket -= weight;
        }
    }

    uint32_t candidate = (_selector_cursor[prio] + _selector_stride[prio]) % path_space;
    _selector_cursor[prio] = candidate;
    _stor_selected_avoid++;
    return candidate;
}

uint32_t RoceSrc::choose_netaware_bucket_path(uint32_t prio, uint32_t path_space) {
    const StorFeedbackLevels& levels = shared_netaware_profile(path_space).levels;
    uint64_t total_weight = 0;
    uint32_t level_counts[4] = {0, 0, 0, 0};
    for (uint32_t i = 0; i < path_space; i++) {
        uint8_t level = i < levels.size() ? levels[i] : STOR_LEVEL_GOOD;
        if (level > STOR_LEVEL_AVOID)
            level = STOR_LEVEL_GOOD;
        level_counts[level]++;
    }
    for (uint32_t level = STOR_LEVEL_GOOD; level <= STOR_LEVEL_AVOID; level++) {
        if (level_counts[level] > 0)
            total_weight += _netaware_level_weights[level] * (uint64_t)level_counts[level];
    }

    if (total_weight > 0) {
        uint64_t ticket = _stor_weight_cursor[prio]++ % total_weight;
        uint8_t chosen_level = STOR_LEVEL_GOOD;
        for (uint32_t level = STOR_LEVEL_GOOD; level <= STOR_LEVEL_AVOID; level++) {
            if (level_counts[level] == 0)
                continue;
            uint64_t weight = _netaware_level_weights[level] * (uint64_t)level_counts[level];
            if (ticket < weight) {
                chosen_level = level;
                break;
            }
            ticket -= weight;
        }

        for (uint32_t offset = 1; offset <= path_space; offset++) {
            uint32_t candidate = (_selector_cursor[prio] + offset * _selector_stride[prio]) % path_space;
            uint8_t level = candidate < levels.size() ?
                levels[candidate] : STOR_LEVEL_GOOD;
            if (level > STOR_LEVEL_AVOID)
                level = STOR_LEVEL_GOOD;
            if (level == chosen_level) {
                _selector_cursor[prio] = candidate;
                record_netaware_selected_level(level);
                return candidate;
            }
        }
    }

    uint32_t candidate = (_selector_cursor[prio] + _selector_stride[prio]) % path_space;
    _selector_cursor[prio] = candidate;
    _stor_selected_avoid++;
    return candidate;
}

uint32_t RoceSrc::choose_netaware_virtual_bucket_path(uint32_t prio,
                                                  uint32_t path_space) {
    bool stor_mode = _flow_lb_mode == LB_STOR;
    SharedWeightedProfile& profile = stor_mode ?
        shared_stor_profile(path_space) : shared_netaware_profile(path_space);
    const StorFeedbackLevels& levels = profile.levels;
    uint32_t bucket_size = (uint32_t)profile.bucket.size();

    if (bucket_size > 0) {
        uint64_t ordinal = _virtual_selector_counter[prio]++;
        uint64_t epoch = ordinal / bucket_size;
        uint32_t position = (uint32_t)(ordinal % bucket_size);
        uint32_t mode_tag = stor_mode ? 0x53544f52 : 0x4e4d5243;
        uint32_t key = virtual_selector_key(prio, profile.version,
                                            epoch, mode_tag);
        uint32_t index = virtual_shuffle_index(key, epoch, position,
                                               bucket_size);
        uint32_t candidate = profile.bucket[index];
        uint8_t level = candidate < levels.size() ?
            levels[candidate] : STOR_LEVEL_GOOD;
        if (level > STOR_LEVEL_AVOID)
            level = STOR_LEVEL_GOOD;
        _selector_cursor[prio] = candidate;
        record_netaware_selected_level(level);
        return candidate;
    }

    return choose_virtual_binary_path(prio, path_space, levels,
                                      profile.version,
                                      stor_mode ? 0x53544642 : 0x4e4d4642);
}

uint32_t RoceSrc::choose_netaware_topk_path(uint32_t prio,
                                             uint32_t path_space) {
    SharedWeightedProfile& profile = shared_netaware_profile(path_space);
    const StorFeedbackLevels& levels = profile.levels;
    std::vector<std::pair<uint8_t, uint32_t> > ranked;
    ranked.reserve(path_space);
    for (uint32_t ev = 0; ev < path_space; ev++) {
        uint8_t level = ev < levels.size() ? levels[ev] : STOR_LEVEL_GOOD;
        if (level > STOR_LEVEL_AVOID)
            level = STOR_LEVEL_GOOD;
        ranked.push_back(std::make_pair(level, ev));
    }
    std::sort(ranked.begin(), ranked.end());
    uint32_t k = std::min(_netaware_topk, path_space);
    if (k == 0)
        k = 1;
    uint64_t ordinal = _virtual_selector_counter[prio]++;
    uint64_t epoch = ordinal / k;
    uint32_t position = (uint32_t)(ordinal % k);
    uint32_t key = virtual_selector_key(prio, profile.version, epoch,
                                        0x544f504b);
    uint32_t index = virtual_shuffle_index(key, epoch, position, k);
    uint32_t candidate = ranked[index].second;
    _selector_cursor[prio] = candidate;
    record_netaware_selected_level(ranked[index].first);
    return candidate;
}

uint32_t RoceSrc::choose_virtual_binary_path(
        uint32_t prio, uint32_t path_space,
        const StorFeedbackLevels& levels, uint64_t profile_version,
        uint32_t mode_tag) {
    if (path_space == 0)
        path_space = 1;
    uint64_t start = _virtual_selector_counter[prio];
    for (uint32_t attempt = 0; attempt < path_space; attempt++) {
        uint64_t ordinal = start + attempt;
        uint64_t epoch = ordinal / path_space;
        uint32_t position = (uint32_t)(ordinal % path_space);
        uint32_t key = virtual_selector_key(prio, profile_version,
                                            epoch, mode_tag);
        uint32_t candidate = virtual_shuffle_index(key, epoch, position,
                                                   path_space);
        uint8_t level = candidate < levels.size() ?
            levels[candidate] : STOR_LEVEL_GOOD;
        if (level != STOR_LEVEL_AVOID) {
            _virtual_selector_counter[prio] = ordinal + 1;
            record_netaware_selected_level(level);
            return candidate;
        }
    }

    _virtual_selector_counter[prio] = start + path_space;
    uint64_t ordinal = _virtual_selector_counter[prio]++;
    uint64_t epoch = ordinal / path_space;
    uint32_t position = (uint32_t)(ordinal % path_space);
    uint32_t key = virtual_selector_key(prio, profile_version,
                                        epoch, mode_tag ^ 0x46414c4c);
    uint32_t candidate = virtual_shuffle_index(key, epoch, position,
                                               path_space);
    _stor_selected_avoid++;
    return candidate;
}

uint32_t RoceSrc::choose_stor_path(Packet::PktPriority priority, uint32_t path_space) {
    uint32_t prio = selector_priority_index(priority);
    SharedWeightedProfile& profile = shared_stor_profile(path_space);
    const StorFeedbackLevels& levels = profile.levels;
    if (all_profile_weights_zero(levels, _stor_level_weights, path_space))
        _stor_all_zero_selections++;

    if (_stor_binary_selector)
        return choose_virtual_binary_path(prio, path_space, levels,
                                          profile.version, 0x53544249);

    uint32_t avoid_count = 0;
    for (uint32_t i = 0; i < path_space; i++) {
        uint8_t level = i < levels.size() ? levels[i] : STOR_LEVEL_GOOD;
        if (level == STOR_LEVEL_AVOID)
            avoid_count++;
    }

    uint32_t probe_interval = weightedShuffledBucketSize(path_space);
    if (avoid_count > 0 &&
        _stor_selections_since_probe[prio] + 1 >= probe_interval) {
        uint32_t target = _stor_avoid_probe_cursor[prio] % avoid_count;
        uint32_t seen = 0;
        for (uint32_t candidate = 0; candidate < path_space; candidate++) {
            uint8_t level = candidate < levels.size() ?
                levels[candidate] : STOR_LEVEL_GOOD;
            if (level == STOR_LEVEL_AVOID) {
                if (seen++ != target)
                    continue;
                _stor_avoid_probe_cursor[prio]++;
                _stor_selections_since_probe[prio] = 0;
                _selector_cursor[prio] = candidate;
                record_netaware_selected_level(STOR_LEVEL_AVOID);
                return candidate;
            }
        }
    }

    _stor_selections_since_probe[prio]++;
    return choose_netaware_virtual_bucket_path(prio, path_space);
}

uint32_t RoceSrc::choose_netaware_path(Packet::PktPriority priority, uint32_t path_space) {
    uint32_t prio = selector_priority_index(priority);
    SharedWeightedProfile& profile = shared_netaware_profile(path_space);
    if (all_profile_weights_zero(profile.levels, _netaware_level_weights,
                                 path_space))
        _netaware_all_zero_selections++;

    if (_netaware_wrr_mode == NETAWARE_WRR_TOPK)
        return choose_netaware_topk_path(prio, path_space);

    if (_netaware_wrr_mode == NETAWARE_WRR_DIRECT) {
        init_selector_priority(priority, path_space);
        return choose_netaware_direct_path(prio, path_space);
    }
    if (_netaware_wrr_mode == NETAWARE_WRR_BUCKET) {
        init_selector_priority(priority, path_space);
        return choose_netaware_bucket_path(prio, path_space);
    }
    return choose_netaware_virtual_bucket_path(prio, path_space);
}

void RoceSrc::trace_netaware_decision(RocePacket::seq_t seqno, uint32_t path,
                                  Packet::PktPriority priority) {
    if (!_netaware_decision_trace || _flow_lb_mode != LB_NETAWARE)
        return;

    uint32_t path_space = _path_entropy_size ? _path_entropy_size : 1;
    uint32_t prio = selector_priority_index(priority);
    SharedWeightedProfile& profile = shared_netaware_profile(path_space);
    const StorFeedbackLevels& levels = profile.levels;
    uint32_t ev = path % path_space;
    uint8_t level = ev < levels.size() ? levels[ev] : STOR_LEVEL_GOOD;
    if (level > STOR_LEVEL_AVOID)
        level = STOR_LEVEL_GOOD;
    uint32_t weight = _netaware_level_weights[level];
    uint32_t selected_tickets = 0;
    if (_netaware_wrr_mode == NETAWARE_WRR_SHUFFLED_BUCKET &&
        ev < profile.tickets.size()) {
        selected_tickets = profile.tickets[ev];
    }

    (*_netaware_decision_trace)
        << timeAsUs(eventlist().now()) << ","
        << _flow.flow_id() << ","
        << seqno << ","
        << ev << ","
        << path << ","
        << (uint32_t)level << ","
        << weight << ",";
    if (_netaware_wrr_mode == NETAWARE_WRR_SHUFFLED_BUCKET) {
        uint32_t bucket_size = profile.bucket.empty() ? 1 :
            (uint32_t)profile.bucket.size();
        (*_netaware_decision_trace)
            << (_virtual_selector_counter[prio] / bucket_size) << ","
            << profile.version << ","
            << selected_tickets << ",";
        for (uint32_t i = 0; i < profile.tickets.size(); i++) {
            if (i)
                (*_netaware_decision_trace) << "/";
            (*_netaware_decision_trace) << profile.tickets[i];
        }
        (*_netaware_decision_trace) << "\n";
    } else if (_netaware_wrr_mode == NETAWARE_WRR_TOPK) {
        std::vector<std::pair<uint8_t, uint32_t> > ranked;
        for (uint32_t candidate = 0; candidate < path_space; candidate++) {
            uint8_t candidate_level = candidate < levels.size() ?
                levels[candidate] : STOR_LEVEL_GOOD;
            ranked.push_back(std::make_pair(candidate_level, candidate));
        }
        std::sort(ranked.begin(), ranked.end());
        uint32_t k = std::min(_netaware_topk, path_space);
        (*_netaware_decision_trace) << "0," << profile.version << ",1,";
        for (uint32_t candidate = 0; candidate < path_space; candidate++) {
            if (candidate)
                (*_netaware_decision_trace) << "/";
            bool selected = false;
            for (uint32_t rank = 0; rank < k; rank++)
                selected = selected || ranked[rank].second == candidate;
            (*_netaware_decision_trace) << (selected ? 1 : 0);
        }
        (*_netaware_decision_trace) << "\n";
    } else {
        (*_netaware_decision_trace) << ",,,\n";
    }
}

void RoceSrc::reset_nmrc_all_cooling_rr_episode() {
    _nmrc_all_cooling_rr_active = false;
    _nmrc_all_cooling_rr_episode_size = 0;
    _nmrc_all_cooling_rr_progress = 0;
}

void RoceSrc::reset_nmrc_evs() {
    _nmrc_evs.clear();
    _nmrc_cursor = 0;
    _nmrc_path_space = 0;
    _nmrc_evs_ready = false;
    _nmrc_select_ordinal = 0;
    reset_nmrc_all_cooling_rr_episode();
}

void RoceSrc::init_nmrc_evs(uint32_t path_space) {
    if (path_space == 0)
        path_space = 1;
    if (_nmrc_evs_ready && _nmrc_path_space == path_space)
        return;

    reset_nmrc_evs();
    _nmrc_path_space = path_space;
    uint32_t set_size = std::min(path_space, 64U);
    uint32_t src = _srcaddr == UINT32_MAX ? _node_num : _srcaddr;
    uint32_t dst = _dstaddr == UINT32_MAX ?
        (_node_num ^ 0x5bd1e995) : _dstaddr;
    uint32_t qp_key = selector_mix(src, dst, _flow.flow_id());
    uint32_t seed = selector_mix(_nmrc_ev_seed, qp_key,
                                 0x454e4300 ^ set_size ^ path_space);
    uint32_t counter = 0;

    for (uint32_t i = 0; i < set_size; i++)
        _nmrc_evs.push_back(NmrcEv(i, i));
    for (uint32_t i = 0; i < _nmrc_evs.size(); i++) {
        uint32_t remaining = (uint32_t)_nmrc_evs.size() - i;
        uint32_t ix = i + (nmrc_local_next(seed, counter) % remaining);
        std::swap(_nmrc_evs[i], _nmrc_evs[ix]);
    }

    _nmrc_evs_ready = true;
}

uint32_t RoceSrc::nmrc_ev_index(uint32_t ev) const {
    for (uint32_t i = 0; i < _nmrc_evs.size(); i++) {
        if (_nmrc_evs[i].ev == ev)
            return i;
    }
    return UINT32_MAX;
}

uint32_t RoceSrc::nmrc_physical_path(uint32_t ev,
                                     uint32_t path_space) const {
    if (path_space == 0)
        path_space = 1;
    uint32_t index = nmrc_ev_index(ev);
    if (index != UINT32_MAX)
        return _nmrc_evs[index].physical_path % path_space;
    return ev % path_space;
}

RoceSrc::NmrcChoice RoceSrc::choose_nmrc_all_cooling_rr() {
    uint32_t index = _nmrc_cursor % _nmrc_all_cooling_rr_episode_size;
    _nmrc_cursor = (index + 1) % _nmrc_all_cooling_rr_episode_size;
    NmrcChoice choice(_nmrc_evs[index].ev,
                      _nmrc_evs[index].physical_path);
    _nmrc_select_ordinal++;
    _nmrc_all_cooling_rr_progress++;
    _nmrc_all_cooling_rr_selections++;
    if (_nmrc_all_cooling_rr_progress ==
            _nmrc_all_cooling_rr_episode_size) {
        for (uint32_t i = 0; i < _nmrc_evs.size(); i++) {
            _nmrc_evs[i].cooling = false;
            _nmrc_evs[i].cool_until_select_count = 0;
        }
        _nmrc_all_cooling_rr_resets++;
        reset_nmrc_all_cooling_rr_episode();
    }
    return choice;
}

RoceSrc::NmrcChoice RoceSrc::choose_nmrc_ev(uint32_t path_space) {
    if (path_space == 0)
        path_space = 1;
    if (_nmrc_endpoint_policy == NMRC_ENDPOINT_RANDOM_STATELESS) {
        uint32_t src = _srcaddr == UINT32_MAX ? _node_num : _srcaddr;
        uint32_t dst = _dstaddr == UINT32_MAX ?
            (_node_num ^ 0x5bd1e995) : _dstaddr;
        uint32_t qp_key = selector_mix(src, dst, _flow.flow_id());
        uint32_t ordinal_low = (uint32_t)_nmrc_select_ordinal;
        uint32_t ordinal_high = (uint32_t)(_nmrc_select_ordinal >> 32);
        uint32_t key = selector_mix(_nmrc_ev_seed, qp_key, ordinal_low);
        uint32_t ev = selector_mix(
            key, ordinal_high, 0x53544154) % path_space;
        _nmrc_select_ordinal++;
        return NmrcChoice(ev, ev);
    }

    init_nmrc_evs(path_space);
    if (_nmrc_evs.empty())
        return NmrcChoice(UINT32_MAX, 0);

    uint32_t count = (uint32_t)_nmrc_evs.size();
    if (_nmrc_all_cooling_rr_active)
        return choose_nmrc_all_cooling_rr();

    for (uint32_t attempt = 0; attempt < count; attempt++) {
        uint32_t index = _nmrc_cursor % count;
        _nmrc_cursor = (index + 1) % count;
        NmrcEv& candidate = _nmrc_evs[index];
        if (candidate.cooling &&
            _nmrc_select_ordinal >= candidate.cool_until_select_count) {
            candidate.cooling = false;
            candidate.cool_until_select_count = 0;
            _nmrc_cooling_recoveries++;
        }
        if (candidate.cooling) {
            _nmrc_cooling_skips++;
            continue;
        }
        _nmrc_select_ordinal++;
        return NmrcChoice(candidate.ev, candidate.physical_path);
    }

    if (_nmrc_all_cooling_policy == NMRC_ALL_COOLING_RR_RESET) {
        _nmrc_all_cooling_fallbacks++;
        _nmrc_all_cooling_rr_active = true;
        _nmrc_all_cooling_rr_episode_size = count;
        _nmrc_all_cooling_rr_progress = 0;
        _nmrc_all_cooling_rr_episodes++;
        return choose_nmrc_all_cooling_rr();
    }

    uint32_t earliest = 0;
    for (uint32_t i = 1; i < count; i++) {
        if (_nmrc_evs[i].cool_until_select_count <
                _nmrc_evs[earliest].cool_until_select_count ||
            (_nmrc_evs[i].cool_until_select_count ==
                 _nmrc_evs[earliest].cool_until_select_count &&
             _nmrc_evs[i].ev < _nmrc_evs[earliest].ev))
            earliest = i;
    }
    _nmrc_all_cooling_fallbacks++;
    _nmrc_select_ordinal++;
    return NmrcChoice(_nmrc_evs[earliest].ev,
                      _nmrc_evs[earliest].physical_path);
}

bool RoceSrc::notify_nmrc_ev(uint32_t ev) {
    uint32_t index = nmrc_ev_index(ev);
    if (index == UINT32_MAX)
        return false;
    NmrcEv& entry = _nmrc_evs[index];
    if (_nmrc_all_cooling_rr_active && entry.cooling) {
        _nmrc_duplicate_notifications++;
        return false;
    }
    if (entry.cooling &&
        _nmrc_select_ordinal >= entry.cool_until_select_count) {
        entry.cooling = false;
        entry.cool_until_select_count = 0;
        _nmrc_cooling_recoveries++;
    }
    if (entry.cooling) {
        _nmrc_duplicate_notifications++;
        return false;
    }
    entry.cooling = true;
    entry.cool_until_select_count =
        _nmrc_select_ordinal + _nmrc_evs.size();
    _nmrc_cooldown_starts++;
    return true;
}

void RoceSrc::process_nmrc_trim_feedback(const RoceNack& nack,
                                         bool failure_accepted) {
    if (_flow_lb_mode != LB_NMRC || nack.reason() != RoceNack::TRIM)
        return;
    if (!failure_accepted) {
        _nmrc_trim_duplicate_stale_ignored++;
        return;
    }
    if (_nmrc_endpoint_policy == NMRC_ENDPOINT_RANDOM_STATELESS) {
        _nmrc_trim_policy_ignored++;
        return;
    }
    if (!nack.nmrc_detour()) {
        _nmrc_trim_non_detour++;
        if (nack.has_mrc_ev() && notify_nmrc_ev(nack.mrc_ev()))
            _nmrc_trim_nominal_cooldown_starts++;
        return;
    }

    _nmrc_trim_detour++;
    if (!nack.has_nmrc_actual_egress()) {
        _nmrc_trim_actual_unresolved++;
        return;
    }

    uint32_t match = UINT32_MAX;
    for (uint32_t i = 0; i < _nmrc_evs.size(); i++) {
        if (_nmrc_evs[i].physical_path != nack.nmrc_actual_egress())
            continue;
        if (match != UINT32_MAX) {
            _nmrc_trim_actual_unresolved++;
            return;
        }
        match = i;
    }
    if (match == UINT32_MAX) {
        _nmrc_trim_actual_unresolved++;
        return;
    }
    if (notify_nmrc_ev(_nmrc_evs[match].ev))
        _nmrc_trim_actual_cooldown_starts++;
}

void RoceSrc::init_nmrc_evs_for_test(uint32_t path_space) {
    init_nmrc_evs(path_space);
}

std::vector<uint32_t> RoceSrc::nmrc_ev_values_for_test() const {
    std::vector<uint32_t> values;
    for (uint32_t i = 0; i < _nmrc_evs.size(); i++)
        values.push_back(_nmrc_evs[i].ev);
    return values;
}

std::vector<uint32_t> RoceSrc::nmrc_physical_paths_for_test() const {
    std::vector<uint32_t> paths;
    for (uint32_t i = 0; i < _nmrc_evs.size(); i++)
        paths.push_back(_nmrc_evs[i].physical_path);
    return paths;
}

uint32_t RoceSrc::choose_nmrc_ev_for_test(uint32_t path_space) {
    return choose_nmrc_ev(path_space).ev;
}

bool RoceSrc::notify_nmrc_ev_for_test(uint32_t ev) {
    return notify_nmrc_ev(ev);
}

bool RoceSrc::nmrc_ev_cooling_for_test(uint32_t ev) const {
    uint32_t index = nmrc_ev_index(ev);
    return index != UINT32_MAX && _nmrc_evs[index].cooling &&
        _nmrc_select_ordinal < _nmrc_evs[index].cool_until_select_count;
}

bool RoceSrc::nmrc_ev_cooling_flag_for_test(uint32_t ev) const {
    uint32_t index = nmrc_ev_index(ev);
    return index != UINT32_MAX && _nmrc_evs[index].cooling;
}

uint64_t RoceSrc::nmrc_ev_cool_until_for_test(uint32_t ev) const {
    uint32_t index = nmrc_ev_index(ev);
    return index == UINT32_MAX ? 0 :
        _nmrc_evs[index].cool_until_select_count;
}

uint64_t RoceSrc::nmrc_selection_ordinal_for_test() const {
    return _nmrc_select_ordinal;
}

uint64_t RoceSrc::nmrc_all_cooling_fallbacks_for_test() const {
    return _nmrc_all_cooling_fallbacks;
}

bool RoceSrc::nmrc_all_cooling_rr_active_for_test() const {
    return _nmrc_all_cooling_rr_active;
}

uint32_t RoceSrc::nmrc_all_cooling_rr_progress_for_test() const {
    return _nmrc_all_cooling_rr_progress;
}

uint32_t RoceSrc::nmrc_unique_physical_paths_for_diag() const {
    std::set<uint32_t> paths;
    for (uint32_t i = 0; i < _nmrc_evs.size(); i++)
        paths.insert(_nmrc_evs[i].physical_path);
    return paths.size();
}

std::vector<uint32_t> RoceSrc::build_mrc_ev_order(
        uint32_t path_space) const {
    if (path_space == 0)
        path_space = 1;
    std::vector<uint32_t> ids(path_space);
    for (uint32_t i = 0; i < path_space; i++)
        ids[i] = i;

    uint32_t src = _srcaddr == UINT32_MAX ? _node_num : _srcaddr;
    uint32_t dst = _dstaddr == UINT32_MAX ?
        (_node_num ^ 0x5bd1e995) : _dstaddr;
    uint32_t seed =
        selector_mix(src, dst, _flow.flow_id() ^ 0x4d524300);
    for (uint32_t i = 0; i < path_space; i++) {
        uint32_t remaining = path_space - i;
        uint32_t ix = i +
            (selector_mix(seed, i, _flow.flow_id()) % remaining);
        std::swap(ids[i], ids[ix]);
    }
    return ids;
}

void RoceSrc::reset_rr_paths() {
    _rr_evs.clear();
    _rr_cursor = 0;
    _rr_path_space = 0;
    _rr_paths_ready = false;
}

void RoceSrc::init_rr_paths(uint32_t path_space) {
    if (path_space == 0)
        path_space = 1;
    if (_rr_paths_ready && _rr_path_space == path_space)
        return;
    _rr_evs = build_mrc_ev_order(path_space);
    _mrc_flow_metrics.active_evs = (uint32_t)_rr_evs.size();
    _mrc_flow_metrics.initial_active.clear();
    _mrc_flow_metrics.initial_active.insert(
        _rr_evs.begin(), _rr_evs.end());
    _rr_cursor = 0;
    _rr_path_space = path_space;
    _rr_paths_ready = true;
}

uint32_t RoceSrc::choose_rr_path(uint32_t path_space) {
    init_rr_paths(path_space);
    uint32_t selected = _rr_evs[_rr_cursor];
    _rr_cursor = (_rr_cursor + 1) % _rr_evs.size();
    return selected % path_space;
}

void RoceSrc::reset_mrc_paths() {
    _mrc_evs.clear();
    _mrc_active.clear();
    _mrc_path_space = 0;
    _mrc_paths_ready = false;
    _mrc_seq_ev.clear();
    _mrc_seq_sent_at.clear();
    _mrc_select_counter = 0;
    _mrc_rotation = 0;
    _mrc_rotation_slot = 0;
}

uint32_t RoceSrc::mrc_logical_ev_count(uint32_t path_space) const {
    return path_space == MRC_EV_COUNT ? MRC_EV_COUNT : 0;
}

void RoceSrc::init_mrc_paths(uint32_t path_space) {
    if (path_space == 0)
        path_space = 1;
    if (path_space != MRC_EV_COUNT) {
        cerr << "MRC requires exactly 64 paths; got "
             << path_space << endl;
        abort();
    }
    uint32_t logical_count = mrc_logical_ev_count(path_space);
    if (_mrc_paths_ready && _mrc_evs.size() == logical_count &&
        _mrc_path_space == path_space)
        return;

    _mrc_evs.assign(logical_count, MrcEv());
    _mrc_active.clear();
    _mrc_seq_ev.clear();

    vector<uint32_t> ids = build_mrc_ev_order(logical_count);

    for (uint32_t logical = 0; logical < logical_count; logical++) {
        _mrc_evs[logical].logical_ev = logical;
        _mrc_evs[logical].physical_path = logical;
        _mrc_evs[logical].state = MRC_EV_GOOD;
        _mrc_evs[logical].probe_successes = 0;
        _mrc_evs[logical].retry_after = 0;
        _mrc_evs[logical].skip_pending = false;
        _mrc_evs[logical].congestion_epoch = 0;
        _mrc_active.push_back(ids[logical]);
    }

    _mrc_flow_metrics.active_evs = MRC_EV_COUNT;
    _mrc_flow_metrics.initial_active.clear();
    _mrc_flow_metrics.initial_active.insert(
        _mrc_active.begin(), _mrc_active.end());
    _mrc_paths_ready = true;
    _mrc_path_space = path_space;
}

RoceSrc::MrcChoice RoceSrc::mrc_note_selected_choice(const MrcChoice& choice) {
    _mrc_select_counter++;
    return choice;
}

bool RoceSrc::mrc_ev_selectable(uint32_t logical_ev) {
    return logical_ev < _mrc_evs.size() &&
        _mrc_evs[logical_ev].state == MRC_EV_GOOD;
}

void RoceSrc::mrc_activate_ev(uint32_t logical_ev) {
    if (logical_ev >= _mrc_evs.size())
        return;

    _mrc_evs[logical_ev].state = MRC_EV_GOOD;
    _mrc_evs[logical_ev].retry_after = 0;
    _mrc_evs[logical_ev].skip_pending = false;
    _mrc_evs[logical_ev].probe_successes = 0;
}

bool RoceSrc::mrc_mark_congested(uint32_t logical_ev,
                                 mrc_congestion_signal_t signal) {
    if (logical_ev >= _mrc_evs.size())
        return false;
    MrcEv& ev = _mrc_evs[logical_ev];
    if (ev.state != MRC_EV_GOOD) {
        if (ev.state == MRC_EV_SKIP)
            _mrc_duplicate_feedback_ignored++;
        return false;
    }
    ev.state = MRC_EV_SKIP;
    ev.skip_pending = true;
    ev.retry_after = 0;
    ev.probe_successes = 0;
    ev.awaiting_post_cooldown_feedback = false;
    ev.congestion_epoch++;
    _mrc_cycle_cooling_events++;
    sample_mrc_state_counts();
    (void)signal;
    return true;
}

bool RoceSrc::mrc_mark_failed(uint32_t logical_ev) {
    if (!_mrc_failure_recovery_enabled || logical_ev >= _mrc_evs.size())
        return false;

    MrcEv& ev = _mrc_evs[logical_ev];
    if (ev.state == MRC_EV_ASSUMED_BAD)
        return false;
    ev.awaiting_post_cooldown_feedback = false;
    if (ev.state == MRC_PATH_PROBING)
        _mrc_probe_fail_events++;
    ev.state = MRC_EV_ASSUMED_BAD;
    ev.probe_successes = 0;
    ev.retry_after = eventlist().now() + _mrc_failed_retry;
    ev.skip_pending = false;
    sample_mrc_state_counts();
    return true;
}

bool RoceSrc::mrc_note_probe_result(uint32_t logical_ev, bool success) {
    if (!_mrc_failure_recovery_enabled || logical_ev >= _mrc_evs.size())
        return false;
    MrcEv& ev = _mrc_evs[logical_ev];
    if (ev.state != MRC_EV_ASSUMED_BAD && ev.state != MRC_EV_PROBING)
        return false;

    _mrc_probe_events++;
    if (!success) {
        ev.state = MRC_EV_ASSUMED_BAD;
        ev.probe_successes = 0;
        _mrc_probe_fail_events++;
        sample_mrc_state_counts();
        return false;
    }

    ev.state = MRC_EV_PROBING;
    ev.probe_successes++;
    _mrc_probe_success_events++;
    if (ev.probe_successes < _mrc_probe_success_threshold) {
        sample_mrc_state_counts();
        return false;
    }

    mrc_activate_ev(logical_ev);
    sample_mrc_state_counts();
    return true;
}

uint32_t RoceSrc::mrc_choose_probe_ev(uint32_t path_space) {
    (void)path_space;
    if (!_mrc_failure_recovery_enabled)
        return UINT32_MAX;
    if (_mrc_probe_interval_pkts == 0 || _packets_sent == 0 ||
        _packets_sent % _mrc_probe_interval_pkts != 0)
        return UINT32_MAX;

    simtime_picosec now = eventlist().now();
    for (uint32_t i = 0; i < _mrc_evs.size(); i++) {
        if (_mrc_evs[i].state == MRC_PATH_FAILED && now >= _mrc_evs[i].retry_after) {
            _mrc_evs[i].state = MRC_PATH_PROBING;
            _mrc_evs[i].probe_successes = 0;
            _mrc_probe_events++;
            sample_mrc_state_counts();
            return i;
        }
    }

    return UINT32_MAX;
}

uint32_t RoceSrc::choose_mrc_path(uint32_t path_space) {
    return choose_mrc_ev(path_space).physical_path;
}

RoceSrc::MrcChoice RoceSrc::choose_mrc_ev(uint32_t path_space) {
    init_mrc_paths(path_space);
    sample_mrc_state_counts();
    return choose_mrc_skip_ev(path_space);
}

RoceSrc::MrcChoice RoceSrc::choose_mrc_skip_ev(uint32_t path_space) {
    const uint32_t nominal_slots = (uint32_t)_mrc_active.size();
    if (nominal_slots != 64) {
        cerr << "canonical MRC selector expected 64 nominal slots; got "
             << nominal_slots << endl;
        abort();
    }

    bool restored_skip = false;
    const uint32_t max_visits = nominal_slots + 1;
    for (uint32_t visits = 0; visits < max_visits; ++visits) {
        const uint32_t slot = _mrc_rotation_slot;
        const uint32_t logical_ev = _mrc_active[slot];
        MrcEv& ev = _mrc_evs[logical_ev];

        _mrc_rotation_slot++;
        if (_mrc_rotation_slot == nominal_slots) {
            _mrc_rotation_slot = 0;
            _mrc_rotation++;
        }

        if (!restored_skip && ev.state == MRC_EV_SKIP && ev.skip_pending) {
            ev.skip_pending = false;
            ev.state = MRC_EV_GOOD;
            ev.awaiting_post_cooldown_feedback = true;
            _mrc_skip_opportunities_consumed++;
            restored_skip = true;
            continue;
        }

        if (ev.state != MRC_EV_GOOD)
            continue;

        return mrc_note_selected_choice(
            MrcChoice(logical_ev, ev.physical_path % path_space));
    }

    _mrc_data_on_non_good_violations++;
    cerr << "canonical MRC selector found no GOOD EV after "
         << max_visits << " ordinary slot visits" << endl;
    abort();
}

RoceSrc::MrcChoice RoceSrc::choose_mrc_retx_ev(uint32_t path_space,
                                               uint32_t original_logical_ev) {
    (void)original_logical_ev;
    init_mrc_paths(path_space);
    return choose_mrc_skip_ev(path_space);
}

void RoceSrc::note_mrc_packet_ev(RocePacket::seq_t seqno, uint32_t logical_ev) {
    if (!mrc_path_state_enabled())
        return;
    if (logical_ev == UINT32_MAX)
        return;
    _mrc_seq_ev[seqno] = logical_ev;
    _mrc_seq_sent_at[seqno] = eventlist().now();
}

void RoceSrc::clean_mrc_seq_evs() {
    if (!mrc_path_state_enabled())
        return;
    while (!_mrc_seq_ev.empty()) {
        map<RocePacket::seq_t, uint32_t>::iterator it = _mrc_seq_ev.begin();
        if (it->first > _last_acked)
            break;
        _mrc_seq_sent_at.erase(it->first);
        _mrc_seq_ev.erase(it);
    }
}

void RoceSrc::mrc_note_clean_ack(RocePacket::seq_t ackno,
                                 uint32_t explicit_ev) {
    (void)explicit_ev;
    for (map<RocePacket::seq_t, uint32_t>::iterator it = _mrc_seq_ev.begin();
         it != _mrc_seq_ev.end() && it->first <= ackno; it++) {
        uint32_t logical_ev = it->second;
        if (logical_ev >= _mrc_evs.size())
            continue;
        MrcEv& ev = _mrc_evs[logical_ev];
        if (ev.awaiting_post_cooldown_feedback) {
            _mrc_flow_metrics.post_cooldown_first_clean++;
            ev.awaiting_post_cooldown_feedback = false;
        }
        if (ev.state == MRC_PATH_PROBING) {
            ev.probe_successes++;
            _mrc_probe_success_events++;
            mrc_activate_ev(logical_ev);
        }
    }
}

uint32_t RoceSrc::mrc_resolve_encoded_feedback_ev(
        uint32_t explicit_ev, RocePacket::seq_t sequence,
        uint32_t physical_path) {
    uint32_t sequence_ev = UINT32_MAX;
    map<RocePacket::seq_t, uint32_t>::const_iterator it =
        _mrc_seq_ev.upper_bound(sequence);
    if (it != _mrc_seq_ev.begin()) {
        --it;
        if (it->first <= sequence && it->second < _mrc_evs.size())
            sequence_ev = it->second;
    }

    if (explicit_ev < _mrc_evs.size()) {
        _mrc_feedback_exact_ev_events++;
        if (sequence_ev != UINT32_MAX && sequence_ev != explicit_ev)
            _mrc_feedback_cumulative_mismatch_events++;
        return explicit_ev;
    }

    if (sequence_ev != UINT32_MAX) {
        _mrc_feedback_sequence_fallback_events++;
        return sequence_ev;
    }

    uint32_t path_space = _path_entropy_size ? _path_entropy_size : 1;
    uint32_t physical = physical_path % path_space;
    if (physical < _mrc_evs.size() &&
        _mrc_evs[physical].physical_path % path_space == physical) {
        _mrc_feedback_physical_fallback_events++;
        return physical;
    }
    for (uint32_t i = 0; i < _mrc_evs.size(); i++) {
        if (_mrc_evs[i].physical_path % path_space == physical) {
            _mrc_feedback_physical_fallback_events++;
            return i;
        }
    }
    return UINT32_MAX;
}

void RoceSrc::update_mrc_on_ack(const RoceAck& ack) {
    if (!mrc_path_state_enabled())
        return;

    uint32_t path_space = _path_entropy_size ? _path_entropy_size : 1;
    init_mrc_paths(path_space);

    if (ack.flags() & ECN_ECHO) {
        _mrc_ecn_cooldown_events++;
        _mrc_ecn_physical_hist[ack.pathid() % path_space]++;
        uint32_t ev = mrc_resolve_encoded_feedback_ev(
            ack.has_mrc_ev() ? ack.mrc_ev() : UINT32_MAX,
            ack.ackno(), ack.pathid());
        if (ev != UINT32_MAX) {
            mrc_flow_note_quality_feedback();
            bool replacement = false;
            for (uint32_t i = 0; i < _mrc_evs.size(); i++)
                replacement = replacement ||
                    (i != ev &&
                     _mrc_evs[i].state == MRC_PATH_COOLING);
            if (mrc_mark_congested(ev, MRC_CONGESTION_ECN)) {
                mrc_flow_note_effective_update(
                    ev, ack.ackno(), false, replacement);
            }
        }
    } else {
        mrc_note_clean_ack(
            ack.ackno(), ack.has_mrc_ev() ? ack.mrc_ev() : UINT32_MAX);
    }

    clean_mrc_seq_evs();
    sample_mrc_state_counts();
}

void RoceSrc::update_mrc_on_nack(const RoceNack& nack) {
    if (!mrc_path_state_enabled())
        return;

    uint32_t path_space = _path_entropy_size ? _path_entropy_size : 1;
    init_mrc_paths(path_space);
    RocePacket::seq_t first_missing = _last_acked + 1;
    if (nack.reason() == RoceNack::TRIM) {
        _mrc_trim_events++;
        _mrc_trim_physical_hist[nack.pathid() % path_space]++;
        _mrc_nack_physical_hist[nack.pathid() % path_space]++;
        if (nack.trim_is_lh()) {
            sample_mrc_state_counts();
            return;
        }
        _mrc_trim_cooling_events++;
        uint32_t ev = mrc_resolve_encoded_feedback_ev(
            nack.has_mrc_ev() ? nack.mrc_ev() : UINT32_MAX,
            first_missing, nack.pathid());
        if (ev != UINT32_MAX) {
            mrc_flow_note_quality_feedback();
            bool replacement = false;
            for (uint32_t i = 0; i < _mrc_evs.size(); i++)
                replacement = replacement ||
                    (i != ev &&
                     _mrc_evs[i].state == MRC_PATH_COOLING);
            if (mrc_mark_congested(ev, MRC_CONGESTION_TRIM)) {
                mrc_flow_note_effective_update(
                    ev, first_missing, false, replacement);
            }
        }
        sample_mrc_state_counts();
        return;
    }

    if (nack.reason() == RoceNack::OOO) {
        _mrc_nack_physical_hist[nack.pathid() % path_space]++;
        _mrc_ooo_nack_physical_hist[nack.pathid() % path_space]++;
        _mrc_nack_ooo_ignored_for_failure++;
        sample_mrc_state_counts();
        return;
    }

    if (nack.reason() != RoceNack::LOSS) {
        _mrc_nack_unknown_ignored_for_failure++;
        _mrc_nack_physical_hist[nack.pathid() % path_space]++;
        sample_mrc_state_counts();
        return;
    }

    _mrc_nack_physical_hist[nack.pathid() % path_space]++;
    _mrc_loss_nack_physical_hist[nack.pathid() % path_space]++;
    _mrc_nack_loss_fail_events++;
    uint32_t ev = mrc_resolve_encoded_feedback_ev(
        nack.has_mrc_ev() ? nack.mrc_ev() : UINT32_MAX,
        first_missing, nack.pathid());
    if (ev != UINT32_MAX) {
        mrc_flow_note_quality_feedback();
        if (mrc_mark_failed(ev)) {
            mrc_flow_note_effective_update(
                ev, first_missing, true, false);
        }
    }
    sample_mrc_state_counts();
}

void RoceSrc::update_mrc_on_rto() {
    if (!mrc_path_state_enabled())
        return;

    uint32_t path_space = _path_entropy_size ? _path_entropy_size : 1;
    init_mrc_paths(path_space);
    RocePacket::seq_t first_missing = _last_acked + 1;
    if (first_missing <= _highest_sent) {
        map<RocePacket::seq_t, uint32_t>::iterator it =
            _mrc_seq_ev.find(first_missing);
        if (it != _mrc_seq_ev.end() && it->second < _mrc_evs.size()) {
            _mrc_rto_fail_events++;
            mrc_flow_note_quality_feedback();
            if (mrc_mark_failed(it->second)) {
                mrc_flow_note_effective_update(
                    it->second, first_missing, true, false);
            }
        }
        sample_mrc_state_counts();
    }
}

void RoceSrc::init_ndp_paths(uint32_t path_space) {
    if (path_space == 0)
        path_space = 1;
    if (_ndp_paths_ready && _ndp_path_ids.size() == path_space)
        return;

    _ndp_path_ids.resize(path_space);
    for (uint32_t i = 0; i < path_space; i++)
        _ndp_path_ids[i] = i;
    for (uint32_t i = 0; i < path_space; i++) {
        uint32_t ix = i + (random() % (path_space - i));
        uint32_t tmp = _ndp_path_ids[i];
        _ndp_path_ids[i] = _ndp_path_ids[ix];
        _ndp_path_ids[ix] = tmp;
    }
    _ndp_cursor = 0;
    _ndp_paths_ready = true;
}

uint32_t RoceSrc::choose_ndp_path(uint32_t path_space) {
    init_ndp_paths(path_space);
    if (_ndp_cursor >= _ndp_path_ids.size()) {
        _ndp_paths_ready = false;
        init_ndp_paths(path_space);
    }
    return _ndp_path_ids[_ndp_cursor++] % path_space;
}

void RoceSrc::grant_ndp_credit(uint32_t credits) {
    if (UINT32_MAX - _ndp_pull_credit < credits)
        _ndp_pull_credit = UINT32_MAX;
    else
        _ndp_pull_credit += credits;
}

uint32_t RoceSrc::choose_path(Packet::PktPriority priority, bool retransmitted) {
    uint32_t path_space = _path_entropy_size;
    if (path_space == 0)
        path_space = 1;

    if (_flow_lb_mode == LB_REPS) {
        ensure_reps_buffer();
        const uint32_t ev_space = REPS_EV_SPACE;
        if (!retransmitted && _reps_explore_remaining > 0) {
            _reps_explore_remaining--;
            // Algorithm 2 refreshes the exploratory EV once per REPS buffer
            // width; intervening packets retain the current EV.
            if ((_reps_explore_remaining % _reps_buffer.size()) == 0)
                _reps_current_ev = random() % ev_space;
            _reps_selected_ev = _reps_current_ev;
            // This value is only a route placeholder between the endpoint and
            // its ToR. The source ToR hashes the packet's full tuple.
            uint32_t path = _reps_selected_ev % path_space;
            _reps_random_sends++;
            record_path_selection(path, path);
            sample_reps_buffer_occupancy();
            return path;
        }
        if (_reps_valid_count > 0) {
            uint32_t offset = (_reps_head + _reps_buffer.size() - _reps_valid_count) % _reps_buffer.size();
            assert(_reps_buffer[offset].valid);
            _reps_selected_ev = _reps_buffer[offset].cached_ev % ev_space;
            uint32_t path = _reps_selected_ev % path_space;
            _reps_buffer[offset].valid = false;
            _reps_valid_count--;
            path %= path_space;
            _reps_cached_sends++;
            record_path_selection(path, path);
            sample_reps_buffer_occupancy();
            return path;
        }
        if (_reps_freezing && _reps_initialized_count > 0) {
            // Algorithm 2 cycles through cached EVs once valid entries run
            // out. A partially populated ring must skip unwritten slots.
            while (!_reps_buffer[_reps_head].initialized)
                _reps_head = (_reps_head + 1) % _reps_buffer.size();
            uint32_t offset = _reps_head;
            _reps_head = (_reps_head + 1) % _reps_buffer.size();
            _reps_selected_ev = _reps_buffer[offset].cached_ev % ev_space;
            uint32_t path = _reps_selected_ev % path_space;
            _reps_random_sends++;
            record_path_selection(path, path);
            sample_reps_buffer_occupancy();
            return path;
        }
        _reps_selected_ev = random() % ev_space;
        uint32_t path = _reps_selected_ev % path_space;
        _reps_random_sends++;
        record_path_selection(path, path);
        sample_reps_buffer_occupancy();
        return path;
    }

    if (_flow_lb_mode == LB_MPRDMA) {
        uint32_t path = path_space == 1 ? 0 : (_mprdma_last_vp % path_space);
        if (_mprdma_pruned.size() != path_space)
            _mprdma_pruned.assign(path_space, false);
        ++_mprdma_send_count;
        // Probe at most once per measured RTT.  This models the paper's
        // per-RTT 1% probe, rather than applying 1% to every send.
        const simtime_picosec probe_rtt = _rtt ? _rtt : timeFromUs(1.0);
        const bool probe_window = eventlist().now() >=
            _mprdma_last_probe + probe_rtt;
        if (!_mprdma_ack_valid || (probe_window && drand() < 0.01)) {
            path = random() % path_space;
            _mprdma_last_probe = eventlist().now();
            if (!_mprdma_pruned.empty())
                _mprdma_pruned[path] = false;
        } else if (!_mprdma_ack_clock.empty()) {
            path = _mprdma_ack_clock.front() % path_space;
            _mprdma_ack_clock.pop_front();
        } else if (_mprdma_pruned[path]) {
            for (uint32_t i = 0; i < path_space; ++i) {
                uint32_t candidate = (path + i + 1) % path_space;
                if (!_mprdma_pruned[candidate]) {
                    path = candidate;
                    break;
                }
            }
        } else if (_mprdma_ack_clock.empty()) {
            // PaperMpr::Select falls back to a fresh VP when no ACK credit is
            // available. Reusing the last VP creates a non-paper hot path.
            path = random() % path_space;
        }
        record_path_selection(path, path);
        return path;
    }

    if (_flow_lb_mode == LB_CONGA) {
        // CONGA is a source-ToR mechanism. The endpoint only needs a route to
        // its ToR; selecting a flowlet here consumed an extra random choice
        // and was then overwritten by FatTreeSwitch.
        const uint32_t path = _pathid % path_space;
        record_path_selection(path, path);
        return path;
    }

    if (_flow_lb_mode == LB_NETAWARE) {
        uint32_t path = choose_netaware_path(priority, path_space);
        record_path_selection(path, path);
        return path;
    }

    if (_flow_lb_mode == LB_STOR) {
        uint32_t path = choose_stor_path(priority, path_space);
        record_path_selection(path, path);
        return path;
    }

    if (mrc_path_state_enabled()) {
        MrcChoice choice = choose_mrc_ev(path_space);
        record_path_selection(choice.logical_ev, choice.physical_path);
        return choice.physical_path;
    }

    if (_flow_lb_mode == LB_NMRC) {
        NmrcChoice choice = choose_nmrc_ev(path_space);
        record_path_selection(choice.ev, choice.physical_path);
        return choice.ev;
    }

    if (_flow_lb_mode == LB_RR) {
        uint32_t path = choose_rr_path(path_space);
        record_path_selection(path, path);
        return path;
    }

    if (_flow_lb_mode == LB_OPS) {
        uint32_t path = random() % path_space;
        record_path_selection(path, path);
        return path;
    }

    if (_flow_lb_mode == LB_NDP) {
        uint32_t path = choose_ndp_path(path_space);
        record_path_selection(path, path);
        return path;
    }

    uint32_t path = _pathid % path_space;
    record_path_selection(path, path);
    return path;
}

void RoceSrc::update_mprdma(const RoceAck& ack) {
    if (_flow_lb_mode != LB_MPRDMA)
        return;
    uint32_t path_space = _path_entropy_size ? _path_entropy_size : 1;
    uint32_t vp = (ack.routing_entropy() == UINT32_MAX ?
        ack.pathid() : ack.routing_entropy()) % path_space;
    if (_mprdma_pruned.size() != path_space)
        _mprdma_pruned.assign(path_space, false);
    const bool has_ooo_signal = ack.has_delivered_psn();
    const RocePacket::seq_t delivered = has_ooo_signal ?
        ack.delivered_psn() : ack.ackno();
    _mprdma_last_vp = vp;
    _mprdma_ack_valid = true;
    if (has_ooo_signal && delivered > _mprdma_snd_ooh)
        _mprdma_snd_ooh = delivered;
    const RocePacket::seq_t delta =
        (RocePacket::seq_t)_mprdma_ool_delta_pkts * _mss;
    const bool slow = has_ooo_signal && !ack.retransmitted_data() &&
        _mprdma_snd_ooh > delta &&
        delivered + delta < _mprdma_snd_ooh;
    if (slow) {
        _mprdma_pruned[vp] = true;
        return;
    }
    _mprdma_pruned[vp] = false;
    // One ACK clocks at most one normal data packet onto the echoed VP.
    const int64_t available = (int64_t)floor(_cc_cwnd_pkts) -
        (int64_t)_bounded_inflight_pkts - (int64_t)_mprdma_ack_clock.size();
    const uint32_t credits = available > 0 ?
        (uint32_t)std::min<int64_t>(2, available) : 0;
    for (uint32_t i = 0; i < credits; ++i)
        _mprdma_ack_clock.push_back(vp);
}

void RoceSrc::ensure_reps_buffer() {
    uint32_t size = _reps_buffer_size ? _reps_buffer_size : 1;
    if (_reps_buffer.size() == size)
        return;
    _reps_buffer.assign(size, RepsBufferEntry());
    _reps_head = 0;
    _reps_valid_count = 0;
    _reps_initialized_count = 0;
}

void RoceSrc::reset_reps_buffer() {
    uint32_t size = _reps_buffer_size ? _reps_buffer_size : 1;
    _reps_buffer.assign(size, RepsBufferEntry());
    _reps_head = 0;
    _reps_valid_count = 0;
    _reps_initialized_count = 0;
    _reps_explore_remaining = _reps_warmup_pkts;
    _reps_current_ev = random() % REPS_EV_SPACE;
    _reps_selected_ev = _reps_current_ev;
    _reps_freezing = false;
    _reps_freezing_until = 0;
}

void RoceSrc::detect_reps_failure() {
    if (_flow_lb_mode != LB_REPS || _reps_freezing || _reps_explore_remaining)
        return;
    _reps_freezing = true;
    _reps_freezing_until = eventlist().now() + timeFromUs(100.0);
}

void RoceSrc::update_reps(const RoceAck& ack) {
    if (_flow_lb_mode != LB_REPS)
        return;
    if (ack.flags() & ECN_ECHO) {
        _reps_ecn_ack_discarded++;
        sample_reps_buffer_occupancy();
        return;
    }

    ensure_reps_buffer();
    if (!_reps_buffer[_reps_head].valid)
        _reps_valid_count++;
    if (!_reps_buffer[_reps_head].initialized) {
        _reps_buffer[_reps_head].initialized = true;
        _reps_initialized_count++;
    }
    const uint32_t ev = (ack.routing_entropy() == UINT32_MAX ?
        ack.pathid() : ack.routing_entropy()) % REPS_EV_SPACE;
    _reps_buffer[_reps_head].cached_ev = ev;
    _reps_buffer[_reps_head].valid = true;
    _reps_head = (_reps_head + 1) % _reps_buffer.size();
    _reps_clean_ack_cached++;
    // Algorithm 1 exits only on a clean ACK after the deadline.
    if (_reps_freezing && eventlist().now() > _reps_freezing_until) {
        _reps_freezing = false;
        _reps_explore_remaining = (uint32_t)std::max(1.0, floor(_cc_cwnd_pkts));
    }
    sample_reps_buffer_occupancy();
}

void RoceSrc::update_conweave(const RoceAck& ack, simtime_picosec rtt) {
    if (_flow_lb_mode != LB_CONWEAVE)
        return;

    uint32_t path_space = _path_entropy_size ? _path_entropy_size : 1;
    simtime_picosec now = eventlist().now();
    if (rtt <= _conweave_rtt_threshold)
        return;
    if (now - _conweave_last_reroute < _conweave_min_reroute_gap)
        return;

    uint32_t current = ack.pathid() % path_space;
    uint32_t next = random() % path_space;
    if (path_space > 1) {
        while (next == current)
            next = random() % path_space;
    }
    _pathid = next;
    _conweave_last_reroute = now;
}

void RoceSrc::update_netaware(const RoceAck& ack) {
    if (_flow_lb_mode != LB_NETAWARE ||
        !ack.has_netaware_feedback())
        return;

    uint32_t path_space = _path_entropy_size ? _path_entropy_size : 1;
    apply_netaware_snapshot(ack.netaware_feedback(), path_space);
}

static uint64_t fast_cnp_isolation_double_bits(double value) {
    uint64_t bits = 0;
    static_assert(sizeof(bits) == sizeof(value), "double must be 64 bits");
    std::memcpy(&bits, &value, sizeof(bits));
    return bits;
}

static uint64_t fast_cnp_isolation_mix(uint64_t hash, uint64_t value) {
    hash ^= value;
    hash *= 1099511628211ULL;
    return hash;
}

std::array<uint64_t, 64> RoceSrc::fast_cnp_isolation_snapshot() const {
    std::array<uint64_t, 64> snapshot = {{0}};
    size_t i = 0;
#define FASTCNP_SNAPSHOT(value)                    \
    do {                                           \
        assert(i < snapshot.size());               \
        snapshot[i++] = (uint64_t)(value);         \
    } while (0)
    FASTCNP_SNAPSHOT(_bitrate);
    FASTCNP_SNAPSHOT(_highest_sent);
    FASTCNP_SNAPSHOT(_packets_sent);
    FASTCNP_SNAPSHOT(_last_acked);
    FASTCNP_SNAPSHOT(_new_packets_sent);
    FASTCNP_SNAPSHOT(_rtx_packets_sent);
    FASTCNP_SNAPSHOT(_acks_received);
    FASTCNP_SNAPSHOT(_nacks_received);
    FASTCNP_SNAPSHOT(_ooo_nacks_received);
    FASTCNP_SNAPSHOT(_trim_nacks_received);
    FASTCNP_SNAPSHOT(_loss_nacks_received);
    FASTCNP_SNAPSHOT(_ecn_echo_acks_received);
    FASTCNP_SNAPSHOT(_duplicate_acks_received);
    FASTCNP_SNAPSHOT(_duplicate_ack_inflate_suppressed);
    FASTCNP_SNAPSHOT(_bounded_inflight_pkts);
    FASTCNP_SNAPSHOT(_bounded_unique_acks);
    FASTCNP_SNAPSHOT(_bounded_recovery_inflight_bytes);
    FASTCNP_SNAPSHOT(_bounded_recovery_inflight_max_bytes);
    FASTCNP_SNAPSHOT(_bounded_stale_attempt_nacks);
    FASTCNP_SNAPSHOT(_bounded_duplicate_failure_nacks);
    FASTCNP_SNAPSHOT(_bounded_duplicate_confirmations_suppressed);
    FASTCNP_SNAPSHOT(_bounded_acked_revival_rejected);
    FASTCNP_SNAPSHOT(_bounded_attempt_wraps);
    FASTCNP_SNAPSHOT(_bounded_exact_trim_recoveries);
    FASTCNP_SNAPSHOT(_bounded_sack_loss_recoveries);
    FASTCNP_SNAPSHOT(_feedback_acks_received);
    FASTCNP_SNAPSHOT(_feedback_nacks_received);
    FASTCNP_SNAPSHOT(_feedback_zero_bits_received);
    FASTCNP_SNAPSHOT(_acked_packets);
    FASTCNP_SNAPSHOT(_pathid);
    FASTCNP_SNAPSHOT(_state_send);
    FASTCNP_SNAPSHOT(_rtt);
    FASTCNP_SNAPSHOT(_rto);
    FASTCNP_SNAPSHOT(_mdev);
    FASTCNP_SNAPSHOT(_base_rtt);
    FASTCNP_SNAPSHOT(_packet_spacing);
    FASTCNP_SNAPSHOT(_time_last_sent);
    FASTCNP_SNAPSHOT(_send_event_time);
    FASTCNP_SNAPSHOT(_rtx_timeout);
    FASTCNP_SNAPSHOT(_send_event_pending);
    FASTCNP_SNAPSHOT(fast_cnp_isolation_double_bits(_cc_cwnd_pkts));
    FASTCNP_SNAPSHOT(fast_cnp_isolation_double_bits(_cc_inflate_pkts));
    FASTCNP_SNAPSHOT(fast_cnp_isolation_double_bits(_dcqcn_alpha));
    FASTCNP_SNAPSHOT(fast_cnp_isolation_double_bits(_dcqcn_current_rate));
    FASTCNP_SNAPSHOT(fast_cnp_isolation_double_bits(_dcqcn_target_rate));
    FASTCNP_SNAPSHOT(_dcqcn_bytes_since_increase);
    FASTCNP_SNAPSHOT(_dcqcn_recovery_count);
    FASTCNP_SNAPSHOT(_dcqcn_seen_cnp);
    FASTCNP_SNAPSHOT(_dcqcn_marked_since_alpha);
    FASTCNP_SNAPSHOT(_dcqcn_last_cnp);
    FASTCNP_SNAPSHOT(_dcqcn_next_alpha_update);
    FASTCNP_SNAPSHOT(_dcqcn_next_rate_increase);

    uint64_t rtx_hash = 1469598103934665603ULL;
    const std::set<RocePacket::seq_t>& rtx_contents = _rtx_queue.contents();
    for (std::set<RocePacket::seq_t>::const_iterator it = rtx_contents.begin();
         it != rtx_contents.end(); ++it) {
        rtx_hash = fast_cnp_isolation_mix(rtx_hash, *it);
    }
    FASTCNP_SNAPSHOT(rtx_contents.size());
    FASTCNP_SNAPSHOT(rtx_hash);

    uint64_t bounded_hash = 1469598103934665603ULL;
    for (std::map<RocePacket::seq_t, BoundedPacketState>::const_iterator it =
             _bounded_packets.begin(); it != _bounded_packets.end(); ++it) {
        bounded_hash = fast_cnp_isolation_mix(bounded_hash, it->first);
        bounded_hash = fast_cnp_isolation_mix(bounded_hash, it->second.state);
        bounded_hash = fast_cnp_isolation_mix(
            bounded_hash, it->second.attempt_id);
        bounded_hash = fast_cnp_isolation_mix(
            bounded_hash, it->second.counted_inflight);
        bounded_hash = fast_cnp_isolation_mix(
            bounded_hash, it->second.uses_recovery_reserve);
    }
    FASTCNP_SNAPSHOT(_bounded_packets.size());
    FASTCNP_SNAPSHOT(bounded_hash);

    uint64_t mrc_seq_hash = 1469598103934665603ULL;
    for (std::map<RocePacket::seq_t, uint32_t>::const_iterator it =
             _mrc_seq_ev.begin(); it != _mrc_seq_ev.end(); ++it) {
        mrc_seq_hash = fast_cnp_isolation_mix(mrc_seq_hash, it->first);
        mrc_seq_hash = fast_cnp_isolation_mix(mrc_seq_hash, it->second);
    }
    FASTCNP_SNAPSHOT(_mrc_seq_ev.size());
    FASTCNP_SNAPSHOT(mrc_seq_hash);
    FASTCNP_SNAPSHOT(_sack_rxt_psn);
    FASTCNP_SNAPSHOT(_sack_rxt_psn_updated);
    FASTCNP_SNAPSHOT(_sack_rxt_psn_valid);
#undef FASTCNP_SNAPSHOT
    return snapshot;
}

std::array<uint64_t, 64>
RoceSrc::fast_cnp_isolation_snapshot_for_test() const {
    return fast_cnp_isolation_snapshot();
}

void RoceSrc::processFastCnp(const RoceFastCnp& fast_cnp) {
    _nmrc_fastcnp_arrived++;
    _nmrc_fastcnp_bytes += fast_cnp.size();
    if (eventlist().now() >= fast_cnp.trigger_time())
        _nmrc_fastcnp_latency_sum +=
            eventlist().now() - fast_cnp.trigger_time();

    const std::array<uint64_t, 64> isolation_before =
        fast_cnp_isolation_snapshot();
    const auto record_cc_mutation = [this, &isolation_before]() {
        if (fast_cnp_isolation_snapshot() != isolation_before)
            _nmrc_fastcnp_cc_mutations++;
    };

    if (_done) {
        _nmrc_fastcnp_after_done++;
        record_cc_mutation();
        return;
    }

    if (_flow_lb_mode != LB_NMRC || &fast_cnp.flow() != &_flow ||
        fast_cnp.source_host() != _srcaddr) {
        _nmrc_fastcnp_unknown_qp++;
        record_cc_mutation();
        return;
    }

    if (_nmrc_endpoint_policy == NMRC_ENDPOINT_RANDOM_STATELESS) {
        _nmrc_fastcnp_policy_ignored++;
        record_cc_mutation();
        return;
    }

    if (nmrc_ev_index(fast_cnp.ev()) == UINT32_MAX) {
        _nmrc_fastcnp_unknown_ev++;
        record_cc_mutation();
        return;
    }
    if (fast_cnp.need_endpoint_cooldown())
        notify_nmrc_ev(fast_cnp.ev());
    record_cc_mutation();
}

void RoceSrc::processPause(const EthPausePacket& p) {
    if (p.sleepTime()>0){
        //remote end is telling us to shut up.
        //cout << "Source " << str() << " PAUSE " << timeAsUs(eventlist().now()) << endl;
        //assert(_state_send != PAUSED);
        _state_send = PAUSED;
    } else {
        //we are allowed to send!
        //assert(_state_send != READY);
        _state_send = READY;
        //cout << "Source " << str() << " RESUME " << timeAsUs(eventlist().now()) << endl;
        schedule_send_now();
    }
}

void RoceSrc::receivePacket(Packet& pkt) 
{
    if (!_flow_started){
        assert(pkt.type()==ETH_PAUSE);
        return; 
    }

    if (_stop_time && eventlist().now() >= _stop_time) {
        // stop sending new data, but allow us to finish any retransmissions
        _flow_size = _highest_sent+_mss;
        _stop_time = 0;
    }

    if (_done && pkt.type() == ROCEFASTCNP) {
        processFastCnp((const RoceFastCnp&)pkt);
        pkt.free();
        return;
    }

    if (_done)
        return;

    switch (pkt.type()) {
    case ETH_PAUSE:
        processPause((const EthPausePacket&)pkt);
        pkt.free();
        return;
    case ROCENACK: 
        _nacks_received++;
        processNack((const RoceNack&)pkt);
        pkt.free();
        return;
    case ROCEFASTCNP:
        processFastCnp((const RoceFastCnp&)pkt);
        pkt.free();
        return;
    case ROCEACK:
        _acks_received++;
        processAck((const RoceAck&)pkt);
        pkt.free();
        return;
    default:
        abort();
    }
}

// Note: the data sequence number is the number of Byte1 of the packet, not the last byte.
bool RoceSrc::send_packet() {
    RocePacket* p = NULL;
    bool last_packet = false;
    bool retransmitted = false;
    uint8_t bounded_attempt_id = 0;
    bool bounded_used_reserve = false;
    RocePacket::seq_t seqno = 0;
    if (_log_me)
        cout << "Src " << get_id() << " send_packet\n";
    assert(_flow_started);

    clean_retransmit_queue();
    if (_rx_mode == RX_SP_RETX_QUEUE) {
        RocePacket::seq_t candidate = 0;
        if (_rtx_queue.pop_next(_last_acked, _highest_sent, _flow_size, candidate)) {
            seqno = candidate;
            retransmitted = true;
        }
    }

    if (_flow_size && _last_acked >= _flow_size) {
        //flow is finished
        if (_log_me)
            cout << "Src " << get_id() << " flow is finished, not sending\n";
        return false;
    }

    if (retransmitted &&
        _transport_semantics == TRANSPORT_MRC_EXACT_BOUNDED) {
        if (!bounded_begin_retransmission(seqno, bounded_attempt_id,
                                          bounded_used_reserve)) {
            _rtx_queue.insert(seqno);
            return false;
        }
    }

    if (!retransmitted) {
        if (!congestion_window_allows_send())
            return false;
        if (_flow_size && _highest_sent >= _flow_size) {
            if (_log_me)
                cout << "Src " << get_id() << " no new packets to send\n";
            return false;
        }
        seqno = _highest_sent + 1;
    }

    if (_flow_size && seqno + _mss - 1 >= _flow_size) {
        last_packet = true;
        if (_log_me) {
            cout << _name << " " << get_id() << " sending last packet with SEQNO " << seqno << " at " << timeAsUs(eventlist().now()) << endl;
        }
    }

    p = RocePacket::newpkt(_flow, *_route, seqno, _mss, retransmitted, last_packet,_dstaddr);
    
    assert(p);
    p->set_src(_srcaddr);
    if (_transport_semantics == TRANSPORT_MRC_EXACT_BOUNDED) {
        if (!retransmitted) {
            bounded_note_new_send(seqno);
            bounded_attempt_id = _bounded_packets[seqno].attempt_id;
        }
        p->set_attempt_id(bounded_attempt_id);
    }
    uint32_t path = 0;
    uint32_t logical_ev = UINT32_MAX;
    if (mrc_path_state_enabled()) {
        uint32_t path_space = _path_entropy_size ? _path_entropy_size : 1;
        uint32_t original_physical = UINT32_MAX;
        uint32_t original_logical = UINT32_MAX;
        if (retransmitted) {
            map<RocePacket::seq_t, uint32_t>::const_iterator it =
                _mrc_seq_ev.find(seqno);
            if (it != _mrc_seq_ev.end() && it->second < _mrc_evs.size()) {
                original_logical = it->second;
                original_physical = _mrc_evs[it->second].physical_path % path_space;
            }
        }
        MrcChoice choice = retransmitted ?
            choose_mrc_retx_ev(path_space, original_logical) :
            choose_mrc_ev(path_space);
        path = choice.physical_path;
        logical_ev = choice.logical_ev;
        if (retransmitted) {
            if (original_physical == UINT32_MAX)
                _mrc_retx_unknown_original++;
            else if (original_physical == (path % path_space)) {
                _mrc_retx_original_physical++;
                if (original_logical != UINT32_MAX &&
                    logical_ev != UINT32_MAX &&
                    logical_ev != original_logical)
                    _mrc_retx_same_physical_new_ev++;
            } else {
                _mrc_retx_different_physical++;
                if (original_logical != UINT32_MAX &&
                    logical_ev != UINT32_MAX &&
                    logical_ev != original_logical)
                    _mrc_retx_different_physical_new_ev++;
            }
        }
        record_path_selection(logical_ev, path);
    } else if (_flow_lb_mode == LB_NMRC) {
        uint32_t path_space = _path_entropy_size ? _path_entropy_size : 1;
        NmrcChoice choice = choose_nmrc_ev(path_space);
        path = choice.ev;
        logical_ev = choice.ev;
        record_path_selection(choice.ev, choice.physical_path);
    } else if (_flow_lb_mode == LB_MPRDMA && retransmitted) {
        map<RocePacket::seq_t, uint32_t>::const_iterator it =
            _mprdma_seq_vp.find(seqno);
        path = it == _mprdma_seq_vp.end() ?
            choose_path(p->priority(), true) : it->second;
    } else {
        path = choose_path(p->priority(), retransmitted);
        if (_flow_lb_mode == LB_RR)
            logical_ev = path;
    }
    if (!retransmitted &&
        (_flow_lb_mode == LB_RR || mrc_path_state_enabled()))
        mrc_flow_note_new_selection(logical_ev);
    p->set_pathid(path);
    if (_flow_lb_mode == LB_MPRDMA)
        p->set_routing_entropy(path);
    else if (_flow_lb_mode == LB_REPS)
    {
        p->set_routing_entropy(_reps_selected_ev);
        p->set_reps_entropy(true);
    }
    if (_flow_lb_mode == LB_CONGA)
        p->set_lbtag(path % (_conga_uplinks ? _conga_uplinks : 1));
    if (_flow_lb_mode == LB_MPRDMA)
        _mprdma_seq_vp[seqno] = path;
    if (logical_ev != UINT32_MAX)
        p->set_mrc_ev(logical_ev);
    trace_netaware_decision(seqno, path, p->priority());
    note_mrc_packet_ev(seqno, logical_ev);

    p->flow().logTraffic(*p,*this,TrafficLogger::PKT_CREATESEND);
    p->set_ts(eventlist().now());
    
    if (_log_me) {
        cout << "Src " << get_id() << " sent " << seqno << " Flow Size: " << _flow_size
             << (retransmitted ? " retransmit" : "") << endl;
    }
    if (retransmitted) {
        _rtx_packets_sent++;
    } else {
        _highest_sent += _mss;
        _new_packets_sent++;
    }
    _packets_sent++;
    trace_cc_state(retransmitted ? "send_retx" : "send_new", seqno);
    if (_highest_sent > _last_acked && _rtx_timeout == timeInf)
        reset_rtx_timeout();

    //cout << "Sent " << _highest_sent+1 << " Flow Size: " << _flow_size << " Flow " << _name << " time " << timeAsUs(eventlist().now()) << endl;

    p->sendOn();
    return true;
}

void RoceSrc::rtx_timer_hook(simtime_picosec now, simtime_picosec period) {
    if (!_flow_started || _done || _highest_sent == 0)
        return;
    if (_highest_sent <= _last_acked) {
        _rtx_timeout = timeInf;
        return;
    }
    if (_rtx_timeout == timeInf) {
        reset_rtx_timeout();
        return;
    }
    if (now < _rtx_timeout)
        return;

    trace_cc_state("rto_pre", _last_acked + 1);

    if (_log_me) {
        cout << "Src " << get_id() << " RTO at " << timeAsUs(now)
             << " us, rto " << timeAsUs(_rto)
             << " us, ack " << _last_acked
             << " highest " << _highest_sent << endl;
    }

    detect_reps_failure();
    _global_rto_count++;
    bool bounded_failure_accepted = false;
    if (_rx_mode == RX_SP_RETX_QUEUE) {
        RocePacket::seq_t first_missing = _last_acked + 1;
        if (first_missing <= _highest_sent &&
            (!_flow_size || first_missing <= _flow_size)) {
            if (_transport_semantics == TRANSPORT_MRC_EXACT_BOUNDED) {
                map<RocePacket::seq_t, BoundedPacketState>::const_iterator it =
                    _bounded_packets.find(first_missing);
                if (it != _bounded_packets.end())
                    bounded_failure_accepted = bounded_queue_failure(
                        first_missing, it->second.attempt_id);
                if (!bounded_failure_accepted)
                    bounded_failure_accepted =
                        bounded_expire_recovery_reserve();
            } else {
                _rtx_queue.insert(first_missing);
            }
        }
    } else {
        if (_last_acked < _highest_sent)
            _rtx_packets_sent += (_highest_sent - _last_acked + _mss - 1) / _mss;
        _highest_sent = _last_acked;
    }

    update_mrc_on_rto();
    if (_transport_semantics != TRANSPORT_MRC_EXACT_BOUNDED ||
        bounded_failure_accepted) {
        update_congestion_control_on_nack();
    }
    if (!(_rx_mode == RX_SP_RETX_QUEUE && _rto_high)) {
        if (_rto < _min_rto)
            _rto = _min_rto;
        else
            _rto *= 2;
    }
    reset_rtx_timeout();

    if (_flow_lb_mode == LB_NDP)
        grant_ndp_credit();
    if (_state_send == READY &&
        (has_retransmit_work() || congestion_window_allows_send()))
        schedule_send_now();
    trace_cc_state("rto_post", _last_acked + 1);
}

void RoceSrc::doNextEvent() {
    _send_event_pending = false;
    _send_event_time = 0;
    _send_event_handle = EventList::nullHandle();

    if (!_flow_started){
      startflow();
      return;
    }

    assert(_flow_started);
    if (_log_me) 
        cout << "Src " << get_id() << " do next event\n";
        

    if (_state_send==PAUSED) {
        if (_log_me) 
            cout << "Src " << get_id() << " paused\n";
        return;
    }

    if (_flow_lb_mode == LB_NDP && _ndp_pull_credit == 0)
        return;

    clean_retransmit_queue();
    if (!has_retransmit_work() && !congestion_window_allows_send()) {
        trace_cc_state("send_block_awnd");
        return;
    }

    if (_flow_size && _highest_sent >= _flow_size && !has_retransmit_work()) {
        trace_cc_state("send_block_all_sent");
        if (_log_me) 
            cout << "Src " << get_id()  << " stopping send coz highest_sent is " << _highest_sent << endl;
        return;
    }

    if (_time_last_sent==0 || eventlist().now() - _time_last_sent >= _packet_spacing){
        if (_flow_lb_mode == LB_NDP && _ndp_pull_credit > 0)
            _ndp_pull_credit--;
        if (send_packet())
            _time_last_sent = eventlist().now();
    }

    simtime_picosec next_send = _time_last_sent + _packet_spacing;
    if (next_send < eventlist().now())
        next_send = eventlist().now();

    if (has_retransmit_work() ||
        (congestion_window_allows_send() &&
         (!_flow_size || _highest_sent < _flow_size))) {
        schedule_send(next_send);
    }
}

////////////////////////////////////////////////////////////////
//  ROCE SINK
////////////////////////////////////////////////////////////////

uint64_t RoceSrc::ecnNominalCeForDiag() const {
    return _sink ? _sink->ecnNominalCe() : 0;
}

uint64_t RoceSrc::ecnDetourCeForDiag() const {
    return _sink ? _sink->ecnDetourCe() : 0;
}

/* Only use this constructor when there is only one for to this receiver */
RoceSink::RoceSink()
    : DataReceiver("roce_sink"), _cumulative_ack(0) , _total_received(0),
      _ooo_nack_timer(*this)
{
    _src = 0;
    
    _nodename = "rocesink";
    _highest_seqno = 0;
    _log_me = false;
    //if (get_id() == 144214)
    //    _log_me = true;
    _total_received = 0;
    _rx_rcvd_bytes = 0;
    _ecn_nominal_ce = 0;
    _ecn_detour_ce = 0;
    _ooo_first_time = 0;
    _nack_silent_until = 0;
    _ooo_nack_event_pending = false;
    _ooo_nack_event_time = 0;
    _ooo_nack_path_id = 0;
    _ooo_nack_mrc_ev = UINT32_MAX;
}

void RoceSink::log_me() {
    // avoid looping
    if (_log_me == true)
        return;

    _log_me = true;

    if (_src)
        _src->log_me();  
}

/* Connect a src to this sink. */ 
void RoceSink::connect(RoceSrc& src, Route* route)
{
    _src = &src;
    _route = route;
    _cumulative_ack = 0;
    _drops = 0;
    _ooo_packets.clear();
    _highest_seqno = 0;
    _rx_rcvd_bytes = 0;
    _ecn_nominal_ce = 0;
    _ecn_detour_ce = 0;
    _ooo_first_time = 0;
    _nack_silent_until = 0;
    cancel_ooo_nack_timer();
    _ooo_nack_path_id = 0;
    _ooo_nack_mrc_ev = UINT32_MAX;
}

void RoceSink::refresh_ooo_nack_metadata() {
    if (_ooo_packets.empty()) {
        _ooo_nack_path_id = 0;
        _ooo_nack_mrc_ev = UINT32_MAX;
        return;
    }

    const OooPacketInfo& first = _ooo_packets.begin()->second;
    _ooo_nack_path_id = first.path_id;
    _ooo_nack_mrc_ev = first.mrc_ev;
}

void RoceSink::arm_ooo_nack_timer(simtime_picosec when) {
    if (RoceSrc::_rx_mode != RoceSrc::RX_SP_RETX_QUEUE || _ooo_packets.empty())
        return;
    if (when < EventList::now())
        when = EventList::now();

    if (_ooo_nack_event_pending) {
        if (when >= _ooo_nack_event_time)
            return;
        EventList::cancelPendingSourceByTime(_ooo_nack_timer, _ooo_nack_event_time);
    }

    EventList::sourceIsPending(_ooo_nack_timer, when);
    _ooo_nack_event_pending = true;
    _ooo_nack_event_time = when;
}

void RoceSink::cancel_ooo_nack_timer() {
    if (!_ooo_nack_event_pending)
        return;
    EventList::cancelPendingSourceByTime(_ooo_nack_timer, _ooo_nack_event_time);
    _ooo_nack_event_pending = false;
    _ooo_nack_event_time = 0;
}

void RoceSink::ooo_nack_timer_hook() {
    _ooo_nack_event_pending = false;
    _ooo_nack_event_time = 0;

    if (RoceSrc::_rx_mode != RoceSrc::RX_SP_RETX_QUEUE || _ooo_packets.empty())
        return;

    refresh_ooo_nack_metadata();

    simtime_picosec now = EventList::now();
    simtime_picosec ooo_ready = _ooo_first_time + RoceSrc::_ooo_tolerance;
    if (_ooo_first_time != 0 && now < ooo_ready) {
        arm_ooo_nack_timer(ooo_ready);
        return;
    }
    if (now < _nack_silent_until) {
        arm_ooo_nack_timer(_nack_silent_until);
        return;
    }

    uint16_t sack_offset = 0;
    uint16_t sack_valid_length = 0;
    uint64_t bitmap_low = 0;
    uint64_t bitmap_high = 0;
    RocePacket::seq_t sack_start_psn = 0;
    build_sack_bitmap(_cumulative_ack, bitmap_low, bitmap_high,
                      sack_start_psn, sack_valid_length, sack_offset);
    send_nack(now, _cumulative_ack, _ooo_nack_path_id, bitmap_low, sack_offset,
              true, RoceNack::OOO, bitmap_high, sack_start_psn,
              sack_valid_length, _ooo_nack_mrc_ev);
    _nack_silent_until = now + RoceSrc::_nack_interval;

    if (!_ooo_packets.empty())
        arm_ooo_nack_timer(_nack_silent_until);
}


// Receive a packet.
// Note: _cumulative_ack is the last byte we've ACKed.
// seqno is the first byte of the new packet.
void RoceSink::receivePacket(Packet& pkt) {
    /*
      if (random()%10==0){
      pkt.free();
      return;
      }*/

    assert(pkt.dst () == _src->_dstaddr);

    switch (pkt.type()) {
    case ROCE:
        break;
    default:
        abort();
    }

    RocePacket *p = (RocePacket*)(&pkt);
    RocePacket::seq_t seqno = p->seqno();
    if (_log_me) {
        cout << "Sink " << get_id() << " recv'd " << seqno << endl;
    }
    simtime_picosec ts = p->ts();
    //bool last_packet = ((RocePacket*)&pkt)->last_packet();

    if (pkt.header_only()) {
        uint16_t sack_offset = 0;
        uint16_t sack_valid_length = 0;
        bool has_sack = RoceSrc::_rx_mode == RoceSrc::RX_SP_RETX_QUEUE;
        uint64_t bitmap_low = 0;
        uint64_t bitmap_high = 0;
        RocePacket::seq_t sack_start_psn = 0;
        if (has_sack) {
            build_sack_bitmap(_cumulative_ack, bitmap_low, bitmap_high,
                              sack_start_psn, sack_valid_length, sack_offset);
        }
        send_nack(ts, _cumulative_ack, p->path_id(), bitmap_low, sack_offset,
                  has_sack, RoceNack::TRIM, bitmap_high, sack_start_psn,
                  sack_valid_length, p->mrc_ev(), p->seqno(),
                  p->attempt_id(), p->nmrc_detour(),
                  p->nmrc_actual_egress(), p->trim_is_lh());
        pkt.flow().logTraffic(pkt,*this,TrafficLogger::PKT_RCVDESTROY);
        pkt.free();
        return;
    }

    if (pkt.flags() & ECN_CE) {
        if (p->nmrc_detour())
            _ecn_detour_ce++;
        else
            _ecn_nominal_ce++;
    }

    int size = p->size()-RocePacket::ACKSIZE; 
    RocePacket::seq_t packet_end = seqno + size - 1;
    bool duplicate_ack = false;
    bool old_duplicate_ack = false;
    bool first_full_receipt = false;
    if (packet_end > _highest_seqno)
        _highest_seqno = packet_end;

    if (seqno > _cumulative_ack+1) {
        if (RoceSrc::_rx_mode != RoceSrc::RX_SP_RETX_QUEUE) {
            send_nack(ts, _cumulative_ack, p->path_id(), 0, 0, false,
                      RoceNack::LOSS, 0, 0, 0, p->mrc_ev());
            pkt.flow().logTraffic(pkt,*this,TrafficLogger::PKT_RCVDESTROY);
            pkt.free();
            return;
        }

        duplicate_ack = _ooo_packets.find(seqno) != _ooo_packets.end();
        first_full_receipt = !duplicate_ack;
        _ooo_packets[seqno] = OooPacketInfo(size, p->path_id(), p->mrc_ev());
        if (first_full_receipt) {
            _rx_rcvd_bytes += size;
            _total_received = _rx_rcvd_bytes;
        }
        refresh_ooo_nack_metadata();
        if (_ooo_first_time == 0) {
            _ooo_first_time = EventList::now();
            arm_ooo_nack_timer(_ooo_first_time + RoceSrc::_ooo_tolerance);
        }
        if (should_send_sp_nack()) {
            uint16_t sack_offset = 0;
            uint16_t sack_valid_length = 0;
            uint64_t bitmap_low = 0;
            uint64_t bitmap_high = 0;
            RocePacket::seq_t sack_start_psn = 0;
            build_sack_bitmap(_cumulative_ack, bitmap_low, bitmap_high,
                              sack_start_psn, sack_valid_length, sack_offset);
            send_nack(ts, _cumulative_ack, p->path_id(), bitmap_low, sack_offset,
                      true, RoceNack::OOO, bitmap_high, sack_start_psn,
                      sack_valid_length, p->mrc_ev());
            _nack_silent_until = EventList::now() + RoceSrc::_nack_interval;
            arm_ooo_nack_timer(_nack_silent_until);
            pkt.flow().logTraffic(pkt,*this,TrafficLogger::PKT_RCVDESTROY);
            pkt.free();
            return;
        }
    } else if (seqno == _cumulative_ack+1) { // it's the next expected seq no
        first_full_receipt = true;
        _rx_rcvd_bytes += size;
        _total_received = _rx_rcvd_bytes;
        _cumulative_ack = seqno + size - 1;
        while (_ooo_packets.find(_cumulative_ack + 1) != _ooo_packets.end()) {
            RocePacket::seq_t next_seq = _cumulative_ack + 1;
            int next_size = _ooo_packets[next_seq].size;
            _ooo_packets.erase(next_seq);
            _cumulative_ack = next_seq + next_size - 1;
        }
        if (RoceSrc::_rx_mode == RoceSrc::RX_SP_RETX_QUEUE) {
            if (_ooo_packets.empty()) {
                _ooo_first_time = 0;
                cancel_ooo_nack_timer();
            }
            else {
                refresh_ooo_nack_metadata();
                _ooo_first_time = EventList::now();
                arm_ooo_nack_timer(_ooo_first_time + RoceSrc::_ooo_tolerance);
            }
        }
    } else if (seqno < _cumulative_ack+1) {
        //must have been a bad retransmit
        duplicate_ack = true;
        old_duplicate_ack = true;
        if (RoceSrc::_rx_mode == RoceSrc::RX_SP_RETX_QUEUE && should_send_sp_nack()) {
            uint16_t sack_offset = 0;
            uint16_t sack_valid_length = 0;
            uint64_t bitmap_low = 0;
            uint64_t bitmap_high = 0;
            RocePacket::seq_t sack_start_psn = 0;
            build_sack_bitmap(_cumulative_ack, bitmap_low, bitmap_high,
                              sack_start_psn, sack_valid_length, sack_offset);
            send_nack(ts, _cumulative_ack, p->path_id(), bitmap_low, sack_offset,
                      true, RoceNack::OOO, bitmap_high, sack_start_psn,
                      sack_valid_length, p->mrc_ev());
            _nack_silent_until = EventList::now() + RoceSrc::_nack_interval;
            arm_ooo_nack_timer(_nack_silent_until);
            pkt.flow().logTraffic(pkt,*this,TrafficLogger::PKT_RCVDESTROY);
            pkt.free();
            return;
        }
    }
    send_ack(*p, ts, duplicate_ack, old_duplicate_ack);
    // have we seen everything yet?
    pkt.flow().logTraffic(pkt,*this,TrafficLogger::PKT_RCVDESTROY);
    pkt.free();
}

void RoceSink::send_ack(const RocePacket& pkt, simtime_picosec ts,
                        bool duplicate_ack, bool old_duplicate_ack) {
    RoceAck *ack = 0;
    ack = RoceAck::newpkt(_src->_flow, *_route, _cumulative_ack,_srcaddr);
    if (_log_me)
        cout << "Sink " << get_id() << " sending ack " << _cumulative_ack << endl;
    ack->set_pathid(pkt.path_id());
    ack->set_routing_entropy(pkt.routing_entropy());
    ack->set_lbtag(pkt.lbtag());
    // ECN drives the common congestion controller. CONGA feedback carries
    // only its independent Q=3 DRE metric.
    ack->set_conga_metric(pkt.conga_metric());
    ack->copy_sglb_tx_metadata(pkt);
    if (pkt.has_mrc_ev())
        ack->set_mrc_ev(pkt.mrc_ev());
    ack->set_duplicate_ack(duplicate_ack);
    ack->set_old_duplicate_ack(old_duplicate_ack);
    if (_src->_flow_lb_mode == RoceSrc::LB_MPRDMA ||
        (RoceSrc::_transport_semantics ==
            RoceSrc::TRANSPORT_MRC_EXACT_BOUNDED && !duplicate_ack)) {
        ack->set_delivered_psn(pkt.seqno());
    }
    ack->set_retransmitted_data(pkt.retransmitted());
    ack->set_ts(ts);
    ack->set_stor_peer(_src->_dstaddr);
    ack->set_netaware_peer(_src->_dstaddr);
    if (pkt.flags() & ECN_CE)
        ack->set_flags(ack->flags() | ECN_ECHO);
    ack->sendOn();
}

void RoceSink::build_sack_bitmap(RocePacket::seq_t ackno, uint64_t& sack_bitmap_low,
                                 uint64_t& sack_bitmap_high,
                                 RocePacket::seq_t& sack_bitmap_start_psn,
                                 uint16_t& sack_bitmap_valid_length,
                                 uint16_t& sack_offset) const {
    sack_bitmap_low = 0;
    sack_bitmap_high = 0;
    sack_bitmap_valid_length = 0;
    sack_offset = 0;
    RocePacket::seq_t first_traced = ackno + 1;
    sack_bitmap_start_psn = first_traced;
    uint64_t first_ooo_offset = UINT64_MAX;

    for (map<RocePacket::seq_t, OooPacketInfo>::const_iterator it = _ooo_packets.begin();
         it != _ooo_packets.end(); it++) {
        if (it->first < first_traced)
            continue;
        uint64_t offset = (it->first - first_traced) / _src->_mss;
        if (offset < first_ooo_offset)
            first_ooo_offset = offset;
    }

    if (first_ooo_offset == UINT64_MAX)
        return;

    uint64_t block_start = 0;
    uint32_t sack_bitmap_bits = RoceSrc::_sack_bitmap_bits;
    if (first_ooo_offset >= sack_bitmap_bits)
        block_start = first_ooo_offset - (sack_bitmap_bits - 1);
    sack_offset = block_start > UINT16_MAX ? UINT16_MAX : (uint16_t)block_start;
    sack_bitmap_start_psn = first_traced + block_start * _src->_mss;

    for (uint32_t bit = 0; bit < sack_bitmap_bits; bit++) {
        RocePacket::seq_t seq = first_traced + (block_start + bit) * _src->_mss;
        if (_ooo_packets.find(seq) != _ooo_packets.end()) {
            if (bit < 64)
                sack_bitmap_low |= 1ULL << bit;
            else
                sack_bitmap_high |= 1ULL << (bit - 64);
            sack_bitmap_valid_length = bit + 1;
        }
    }
}

bool RoceSink::should_send_sp_nack() const {
    if (_ooo_packets.empty())
        return false;
    if (EventList::now() < _nack_silent_until)
        return false;

    bool ooo_timeout = _ooo_first_time != 0 &&
        EventList::now() >= _ooo_first_time + RoceSrc::_ooo_tolerance;
    uint64_t window_bytes = (uint64_t)RoceSrc::_ooo_window_pkts * _src->_mss;
    bool ooo_window = _highest_seqno > _cumulative_ack &&
        _highest_seqno - _cumulative_ack > window_bytes;

    return ooo_timeout || ooo_window;
}

RoceNack* RoceSink::send_nack(simtime_picosec ts, RocePacket::seq_t ackno, uint32_t path_id,
                              uint64_t sack_bitmap, uint16_t sack_offset, bool has_sack,
                              RoceNack::nack_reason_t reason,
                              uint64_t sack_bitmap_high,
                              RocePacket::seq_t sack_bitmap_start_psn,
                              uint16_t sack_bitmap_valid_length,
                              uint32_t mrc_ev,
                              RocePacket::seq_t missing_psn,
                              uint8_t missing_attempt_id,
                              bool nmrc_detour,
                              uint32_t nmrc_actual_egress,
                              bool trim_is_lh) {
    RoceNack *nack = NULL;
    nack = RoceNack::newpkt(_src->_flow, *_route, ackno,_srcaddr,sack_bitmap,
                            sack_offset, has_sack, sack_bitmap_high,
                            sack_bitmap_start_psn, sack_bitmap_valid_length,
                            RoceSrc::_sack_bitmap_bits);
    if (_log_me)
        cout << "Sink " << get_id() << " sending nack " << ackno
             << " sack_offset " << sack_offset
             << " sack_start " << sack_bitmap_start_psn
             << " sack_valid " << sack_bitmap_valid_length
             << " bitmap_low " << sack_bitmap
             << " bitmap_high " << sack_bitmap_high << endl;

    nack->set_pathid(path_id);
    if (mrc_ev != UINT32_MAX)
        nack->set_mrc_ev(mrc_ev);
    if (missing_psn != 0) {
        nack->set_missing_psn(missing_psn);
        if (RoceSrc::_transport_semantics ==
            RoceSrc::TRANSPORT_MRC_EXACT_BOUNDED) {
            nack->set_attempt_id(missing_attempt_id);
        }
    }
    nack->set_nmrc_detour(nmrc_detour);
    if (nmrc_actual_egress != UINT32_MAX)
        nack->set_nmrc_actual_egress(nmrc_actual_egress);
    nack->set_reason(reason);
    nack->set_trim_is_lh(trim_is_lh);
    nack->set_stor_peer(_src->_dstaddr);
    assert(nack);
    nack->flow().logTraffic(*nack,*this,TrafficLogger::PKT_CREATE);
    nack->set_ts(ts);
    nack->sendOn();
    return nack;
}

////////////////////////////////////////////////////////////////
//  ROCE RETRANSMISSION TIMER
////////////////////////////////////////////////////////////////

RoceRtxTimerScanner::RoceRtxTimerScanner(simtime_picosec scanPeriod, EventList& eventlist)
    : EventSource(eventlist, "RoceRtxScanner"), _scanPeriod(scanPeriod)
{
    eventlist.sourceIsPendingRel(*this, _scanPeriod);
}

void RoceRtxTimerScanner::registerRoce(RoceSrc& src)
{
    _roces.push_back(&src);
}

void RoceRtxTimerScanner::doNextEvent()
{
    simtime_picosec now = eventlist().now();
    roces_t::iterator i;
    for (i = _roces.begin(); i != _roces.end(); i++) {
        (*i)->rtx_timer_hook(now, _scanPeriod);
    }
    eventlist().sourceIsPendingRel(*this, _scanPeriod);
}
