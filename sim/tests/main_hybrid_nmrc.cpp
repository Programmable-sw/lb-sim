// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <set>
#include <vector>

#include "eventlist.h"
#include "ecn.h"
#include "network.h"
#define private public
#include "roce.h"
#undef private

static void expect(bool condition, const char* message);

class DataCaptureSink : public PacketSink {
public:
    DataCaptureSink() : _name("hybrid_nmrc_data_capture") {}

    void receivePacket(Packet& pkt) {
        expect(pkt.type() == ROCE,
               "hybrid n-MRC test route must carry RoCE data");
        RocePacket& data = (RocePacket&)pkt;
        pathids.push_back(data.pathid());
        evs.push_back(data.mrc_ev());
        seqs.push_back(data.seqno());
        pkt.free();
    }

    const string& nodename() { return _name; }

    std::vector<uint32_t> pathids;
    std::vector<uint32_t> evs;
    std::vector<RocePacket::seq_t> seqs;

private:
    string _name;
};

class ControlDropSink : public PacketSink {
public:
    ControlDropSink() : _name("hybrid_nmrc_control_drop") {}

    void receivePacket(Packet& pkt) { pkt.free(); }
    const string& nodename() { return _name; }

private:
    string _name;
};

static void expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << message << std::endl;
        std::exit(1);
    }
}

static EventList& test_eventlist() {
    static EventList eventlist;
    return eventlist;
}

static RoceSrc* make_src(uint32_t flow_id) {
    RoceSrc* src = new RoceSrc(
        NULL, NULL, test_eventlist(), 400000000000ULL);
    src->set_flowid(flow_id);
    src->set_src(17);
    src->set_dst(113);
    return src;
}

static std::vector<uint32_t> values_for(uint32_t paths,
                                        uint32_t flow_id) {
    RoceSrc* src = make_src(flow_id);
    src->init_nmrc_evs_for_test(paths);
    return src->nmrc_ev_values_for_test();
}

static std::vector<uint32_t> mappings_for(uint32_t paths,
                                          uint32_t flow_id) {
    RoceSrc* src = make_src(flow_id);
    src->init_nmrc_evs_for_test(paths);
    return src->nmrc_physical_paths_for_test();
}

static void test_set_sizes_and_uniqueness() {
    std::vector<uint32_t> values = values_for(64, 700);
    std::vector<uint32_t> physical = mappings_for(64, 700);
    expect(values.size() == 64 && physical.size() == 64,
           "canonical n-MRC must initialize all 64 EVs");
    std::set<uint32_t> unique_values(values.begin(), values.end());
    std::set<uint32_t> unique_paths(physical.begin(), physical.end());
    expect(unique_values.size() == 64 && unique_paths.size() == 64,
           "canonical n-MRC EVs and physical paths must be unique");
    for (uint32_t i = 0; i < values.size(); i++)
        expect(values[i] == physical[i],
               "canonical n-MRC EV mapping must be identity");
}

static void test_per_qp_determinism_and_dephasing() {
    RoceSrc::setNmrcEvSeed(0x12345678);

    std::vector<uint32_t> a = values_for(64, 901);
    std::vector<uint32_t> replay = values_for(64, 901);
    std::vector<uint32_t> b = values_for(64, 902);
    expect(a == replay,
           "the same seed and QP must recreate the same 64-EV order");
    expect(a != b, "different QPs must use different 64-EV permutations");
    expect(std::set<uint32_t>(a.begin(), a.end()) ==
               std::set<uint32_t>(b.begin(), b.end()),
           "all QPs must retain all 64 EV members");
}

static void test_ev_initialization_does_not_consume_global_random() {
    RoceSrc* src = make_src(1001);
    RoceSrc::setNmrcEvSeed(0xabcdef01);

    srandom(417);
    long expected_before = random();
    long expected_after = random();

    srandom(417);
    long actual_before = random();
    src->init_nmrc_evs_for_test(64);
    long actual_after = random();

    expect(actual_before == expected_before && actual_after == expected_after,
           "hybrid n-MRC EV initialization must not consume global random state");
}

static void test_one_round_cooling_and_idempotence() {
    RoceSrc::setNmrcEvSeed(0x1234);
    RoceSrc* src = make_src(1101);
    src->init_nmrc_evs_for_test(4);

    uint32_t bad_ev = src->choose_nmrc_ev_for_test(4);
    expect(src->nmrc_selection_ordinal_for_test() == 1,
           "a selected EV must advance the selection ordinal");
    expect(src->notify_nmrc_ev_for_test(bad_ev),
           "the first path notification must start cooldown");
    uint64_t expiry = src->nmrc_ev_cool_until_for_test(bad_ev);
    expect(expiry == 5,
           "cooldown must expire after exactly one four-EV selection round");

    for (uint32_t i = 0; i < 2; i++)
        expect(src->choose_nmrc_ev_for_test(4) != bad_ev,
               "a cooling EV must be skipped");
    expect(!src->notify_nmrc_ev_for_test(bad_ev),
           "an in-flight duplicate notification must be idempotent");
    expect(src->nmrc_ev_cool_until_for_test(bad_ev) == expiry,
           "a duplicate notification must not extend cooldown");
    for (uint32_t i = 2; i < 4; i++)
        expect(src->choose_nmrc_ev_for_test(4) != bad_ev,
               "the EV must stay absent for the full cooldown round");

    expect(!src->nmrc_ev_cooling_for_test(bad_ev),
           "the EV must become eligible after one full round");
    expect(src->notify_nmrc_ev_for_test(bad_ev),
           "a notification after recovery must start a fresh cooldown");
    expect(src->nmrc_ev_cool_until_for_test(bad_ev) == 9,
           "a post-recovery notification must use the current ordinal");
}

static void test_all_cooling_uses_earliest_expiry_fallback() {
    RoceSrc::setNmrcEndpointPolicy(RoceSrc::NMRC_ENDPOINT_RR_COOLDOWN);
    RoceSrc::setNmrcAllCoolingPolicy(RoceSrc::NMRC_ALL_COOLING_EARLIEST);
    RoceSrc* src = make_src(1201);
    src->init_nmrc_evs_for_test(4);
    std::vector<uint32_t> values = src->nmrc_ev_values_for_test();
    for (uint32_t i = 0; i < values.size(); i++)
        expect(src->notify_nmrc_ev_for_test(values[i]),
               "each active EV should enter cooldown once");

    uint32_t selected = src->choose_nmrc_ev_for_test(4);
    expect(selected == *std::min_element(values.begin(), values.end()),
           "legacy all-cooling ties must select the smallest logical EV");
    expect(src->nmrc_all_cooling_fallbacks_for_test() == 1,
           "all-cooling fallback must have a dedicated diagnostic counter");
    for (uint32_t i = 0; i < values.size(); i++) {
        expect(src->nmrc_ev_cooling_flag_for_test(values[i]),
               "legacy earliest fallback must retain raw cooldown flags");
        expect(src->nmrc_ev_cool_until_for_test(values[i]) == values.size(),
               "legacy earliest fallback must retain cooldown deadlines");
    }
}

static void test_nmrc_policy_defaults_and_names() {
    RoceSrc::setNmrcEndpointPolicy(RoceSrc::NMRC_ENDPOINT_RR_COOLDOWN);
    RoceSrc::setNmrcAllCoolingPolicy(RoceSrc::NMRC_ALL_COOLING_EARLIEST);
    expect(RoceSrc::nmrcEndpointPolicy() ==
               RoceSrc::NMRC_ENDPOINT_RR_COOLDOWN &&
               std::string(RoceSrc::nmrcEndpointPolicyName()) ==
                   "rr_cooldown",
           "legacy endpoint policy must have a stable diagnostic name");
    expect(RoceSrc::nmrcAllCoolingPolicy() ==
               RoceSrc::NMRC_ALL_COOLING_EARLIEST &&
               std::string(RoceSrc::nmrcAllCoolingPolicyName()) ==
                   "earliest",
           "legacy all-cooling policy must have a stable diagnostic name");

    RoceSrc::setNmrcEndpointPolicy(RoceSrc::NMRC_ENDPOINT_RANDOM_STATELESS);
    RoceSrc::setNmrcAllCoolingPolicy(RoceSrc::NMRC_ALL_COOLING_RR_RESET);
    expect(std::string(RoceSrc::nmrcEndpointPolicyName()) ==
               "random_stateless" &&
               std::string(RoceSrc::nmrcAllCoolingPolicyName()) == "rr_reset",
           "ablation policies must have stable diagnostic names");
}

static std::vector<uint32_t> run_all_cooling_rr_episode(
        RoceSrc* src, const std::vector<uint32_t>& values) {
    expect(src->choose_nmrc_ev_for_test(8) == values[0],
           "normal RR must move the cursor once before an episode");
    for (uint32_t i = 0; i < values.size(); i++)
        expect(src->notify_nmrc_ev_for_test(values[i]),
               "every EV must enter cooldown before an RR-reset episode");

    std::vector<uint32_t> selected;
    for (uint32_t selection = 1; selection <= values.size(); selection++) {
        selected.push_back(src->choose_nmrc_ev_for_test(8));
        if (selection == 3) {
            uint64_t deadline =
                src->nmrc_ev_cool_until_for_test(values[4]);
            expect(!src->notify_nmrc_ev_for_test(values[4]) &&
                       src->nmrc_ev_cool_until_for_test(values[4]) == deadline,
                   "an episode duplicate must not extend its deadline");
        }
        if (selection < values.size()) {
            expect(src->nmrc_all_cooling_rr_active_for_test(),
                   "RR-reset must remain active for its first P-1 selections");
            expect(src->nmrc_all_cooling_rr_progress_for_test() == selection,
                   "RR-reset progress must count completed selections");
            for (uint32_t i = 0; i < values.size(); i++) {
                expect(src->nmrc_ev_cooling_flag_for_test(values[i]),
                       "RR-reset must retain raw flags before selection P");
                expect(src->nmrc_ev_cool_until_for_test(values[i]) == 9,
                       "RR-reset must retain deadlines before selection P");
            }
        }
    }

    expect(!src->nmrc_all_cooling_rr_active_for_test() &&
               src->nmrc_all_cooling_rr_progress_for_test() == 0,
           "RR-reset must finish exactly after P selections");
    for (uint32_t i = 0; i < values.size(); i++) {
        expect(!src->nmrc_ev_cooling_flag_for_test(values[i]) &&
                   src->nmrc_ev_cool_until_for_test(values[i]) == 0,
               "selection P must clear every raw cooldown state");
    }
    expect(src->_nmrc_all_cooling_rr_episodes == 1 &&
               src->_nmrc_all_cooling_rr_selections == values.size() &&
               src->_nmrc_all_cooling_rr_resets == 1,
           "a completed RR-reset must expose exact episode diagnostics");
    return selected;
}

static void test_all_cooling_rr_reset_episode() {
    RoceSrc::setNmrcEvSeed(0x22334455);
    RoceSrc::setNmrcEndpointPolicy(RoceSrc::NMRC_ENDPOINT_RR_COOLDOWN);
    RoceSrc::setNmrcAllCoolingPolicy(RoceSrc::NMRC_ALL_COOLING_RR_RESET);
    RoceSrc* src = make_src(1202);
    src->init_nmrc_evs_for_test(8);
    std::vector<uint32_t> values = src->nmrc_ev_values_for_test();

    std::vector<uint32_t> selected =
        run_all_cooling_rr_episode(src, values);
    std::vector<uint32_t> expected(values.begin() + 1, values.end());
    expected.push_back(values[0]);
    expect(selected == expected,
           "RR-reset must return the remaining shuffled entries then first");
    expect(src->choose_nmrc_ev_for_test(8) == values[1] &&
               src->nmrc_selection_ordinal_for_test() == 10,
           "selection P+1 must resume normal RR/cooldown selection");
}

static void test_all_cooling_rr_reset_is_per_qp() {
    RoceSrc::setNmrcEvSeed(0x33445566);
    RoceSrc::setNmrcEndpointPolicy(RoceSrc::NMRC_ENDPOINT_RR_COOLDOWN);
    RoceSrc::setNmrcAllCoolingPolicy(RoceSrc::NMRC_ALL_COOLING_RR_RESET);
    RoceSrc* first = make_src(1203);
    RoceSrc* second = make_src(1204);
    first->init_nmrc_evs_for_test(8);
    second->init_nmrc_evs_for_test(8);
    std::vector<uint32_t> first_values = first->nmrc_ev_values_for_test();
    std::vector<uint32_t> second_values = second->nmrc_ev_values_for_test();
    expect(first_values != second_values,
           "two QPs must retain independent shuffled EV orders");

    std::vector<uint32_t> first_selected =
        run_all_cooling_rr_episode(first, first_values);
    std::vector<uint32_t> second_selected =
        run_all_cooling_rr_episode(second, second_values);
    expect(first_selected != second_selected,
           "two QP episodes must follow their independent shuffled orders");
}

static void test_ev_rebuild_abandons_unfinished_rr_reset() {
    RoceSrc::setNmrcEndpointPolicy(RoceSrc::NMRC_ENDPOINT_RR_COOLDOWN);
    RoceSrc::setNmrcAllCoolingPolicy(RoceSrc::NMRC_ALL_COOLING_RR_RESET);
    RoceSrc* src = make_src(1208);
    src->init_nmrc_evs_for_test(8);
    std::vector<uint32_t> values = src->nmrc_ev_values_for_test();
    expect(src->choose_nmrc_ev_for_test(8) == values[0],
           "normal RR must move the cursor before the interrupted episode");
    for (uint32_t i = 0; i < values.size(); i++)
        expect(src->notify_nmrc_ev_for_test(values[i]),
               "all EVs must cool before the interrupted episode");
    src->choose_nmrc_ev_for_test(8);
    expect(src->nmrc_all_cooling_rr_active_for_test(),
           "the interrupted RR-reset must first become active");

    src->init_nmrc_evs_for_test(4);
    expect(!src->nmrc_all_cooling_rr_active_for_test() &&
               src->nmrc_all_cooling_rr_progress_for_test() == 0 &&
               src->_nmrc_all_cooling_rr_episodes == 1 &&
               src->_nmrc_all_cooling_rr_selections == 1 &&
               src->_nmrc_all_cooling_rr_resets == 0,
           "EV rebuild must abandon an unfinished episode without a reset");
}

static std::vector<uint32_t> stateless_sequence(uint32_t flow_id,
                                                 uint32_t selections) {
    RoceSrc* src = make_src(flow_id);
    std::vector<uint32_t> values;
    for (uint32_t i = 0; i < selections; i++)
        values.push_back(src->choose_nmrc_ev_for_test(8));
    expect(src->nmrc_ev_values_for_test().empty(),
           "stateless random must not build a per-QP EV vector");
    expect(src->nmrc_selection_ordinal_for_test() == selections,
           "stateless random must increment its ordinal exactly once");
    return values;
}

static void test_nmrc_stateless_random_selection() {
    RoceSrc::setNmrcEvSeed(0x44556677);
    RoceSrc::setNmrcEndpointPolicy(RoceSrc::NMRC_ENDPOINT_RANDOM_STATELESS);
    RoceSrc::setNmrcAllCoolingPolicy(RoceSrc::NMRC_ALL_COOLING_EARLIEST);
    std::vector<uint32_t> first = stateless_sequence(1205, 32);
    std::vector<uint32_t> replay = stateless_sequence(1205, 32);
    std::vector<uint32_t> other_qp = stateless_sequence(1206, 32);
    expect(first == replay,
           "equal seed/src/dst/QP must reproduce stateless random choices");
    expect(first != other_qp,
           "a different QP must dephase stateless random choices");
    for (uint32_t i = 0; i < first.size(); i++)
        expect(first[i] < 8,
               "stateless random encoded EVs must stay in [0,P)");

    RoceSrc* rng_probe = make_src(1207);
    srandom(519);
    long expected_before = random();
    long expected_after = random();
    srandom(519);
    long actual_before = random();
    for (uint32_t i = 0; i < 32; i++)
        rng_probe->choose_nmrc_ev_for_test(8);
    long actual_after = random();
    expect(actual_before == expected_before && actual_after == expected_after,
           "stateless random selection must not consume global random state");

    RoceSrc::setNmrcEndpointPolicy(RoceSrc::NMRC_ENDPOINT_RR_COOLDOWN);
    RoceSrc* legacy = make_src(1205);
    legacy->init_nmrc_evs_for_test(8);
    std::vector<uint32_t> legacy_sequence;
    for (uint32_t i = 0; i < first.size(); i++)
        legacy_sequence.push_back(legacy->choose_nmrc_ev_for_test(8));
    expect(first != legacy_sequence,
           "stateless random must not reproduce the legacy cursor sequence");
}

static void test_data_packet_carries_exact_ev_and_retransmission_reselects_it() {
    DataCaptureSink capture;
    Route route;
    route.push_back(&capture);

    RoceSrc::setLoadBalancing(RoceSrc::LB_NMRC);
    RoceSrc::setPathEntropySize(8);
    RoceSrc::setCongestionControl(RoceSrc::CC_NONE);
    RoceSrc::setTransportSemantics(RoceSrc::TRANSPORT_LEGACY);
    RoceSrc::setReceiveMode(RoceSrc::RX_SP_RETX_QUEUE);
    RoceSrc* src = make_src(1301);
    src->_route = &route;
    src->_flow_started = true;

    expect(src->send_packet(),
           "hybrid n-MRC source should send a data packet");
    expect(capture.pathids.size() == 1 && capture.evs.size() == 1,
           "hybrid n-MRC packet should reach the capture route");
    expect(capture.pathids[0] == capture.evs[0],
           "packet path ID and MRC metadata must carry the same logical EV");
    expect(capture.evs[0] <= 65535,
           "packet metadata must carry a 16-bit logical EV");

    src->_rtx_queue.insert(capture.seqs[0]);
    expect(src->send_packet(),
           "hybrid n-MRC source should send the queued retransmission");
    expect(capture.seqs.size() == 2 && capture.seqs[1] == capture.seqs[0],
           "retransmission must preserve the packet sequence number");
    expect(capture.evs[1] != capture.evs[0],
           "n-MRC retransmission must select the next eligible EV");
}

static RoceFastCnp* make_fast_cnp(PacketFlow& flow, Route& route,
                                  uint32_t source_host, uint32_t ev,
                                  RocePacket::seq_t psn,
                                  bool need_endpoint_cooldown = true) {
    return RoceFastCnp::newpkt(
        flow, route, source_host, ev, psn,
        7, 3, STOR_LEVEL_BAD, STOR_LEVEL_GOOD,
        41, timeFromUs(9.0), 0,
        std::numeric_limits<double>::quiet_NaN(),
        std::numeric_limits<double>::quiet_NaN(),
        std::numeric_limits<double>::quiet_NaN(), 0,
        need_endpoint_cooldown);
}

static bool bounded_packet_maps_equal(
        const std::map<RocePacket::seq_t, RoceSrc::BoundedPacketState>& a,
        const std::map<RocePacket::seq_t, RoceSrc::BoundedPacketState>& b) {
    if (a.size() != b.size())
        return false;
    std::map<RocePacket::seq_t, RoceSrc::BoundedPacketState>::const_iterator ai =
        a.begin();
    std::map<RocePacket::seq_t, RoceSrc::BoundedPacketState>::const_iterator bi =
        b.begin();
    for (; ai != a.end(); ++ai, ++bi) {
        if (ai->first != bi->first ||
            ai->second.state != bi->second.state ||
            ai->second.attempt_id != bi->second.attempt_id ||
            ai->second.counted_inflight != bi->second.counted_inflight ||
            ai->second.uses_recovery_reserve !=
                bi->second.uses_recovery_reserve) {
            return false;
        }
    }
    return true;
}

static void test_fast_cnp_metadata_priority_and_recycling() {
    DataCaptureSink sink;
    Route route;
    route.push_back(&sink);
    PacketFlow flow(NULL);
    flow.set_flowid(1401);

    uint8_t attempt_id = 3;
    double original_score = 0.75;
    double selected_score = 0.35;
    double selected_gap = 0.40;
    uint64_t action_key = 0x0123456789abcdefULL;
    RoceFastCnp* first = RoceFastCnp::newpkt(
        flow, route, 17, 0xabcd, 9001,
        7, 3, STOR_LEVEL_BAD, STOR_LEVEL_GOOD, 41, timeFromUs(9.0),
        attempt_id, original_score, selected_score, selected_gap, action_key,
        false);
    expect(first->type() == ROCEFASTCNP,
           "path notification needs its own RoCE FastCNP packet type");
    expect(first->size() == RocePacket::ACKSIZE,
           "path FastCNP must reuse the 64-byte RoCE control size");
    expect(first->priority() == Packet::PRIO_HI,
           "path FastCNP must use the high-priority control queue");
    expect(first->source_host() == 17 && first->ev() == 0xabcd &&
               first->psn() == 9001,
           "path FastCNP must identify source host, EV, and PSN");
    expect(first->original_egress() == 7 && first->selected_egress() == 3,
           "path FastCNP must carry original and selected egresses");
    expect(first->original_level() == STOR_LEVEL_BAD &&
               first->selected_level() == STOR_LEVEL_GOOD,
           "path FastCNP must carry the strict grade improvement");
    expect(first->trigger_switch() == 41 &&
               first->trigger_time() == timeFromUs(9.0),
           "path FastCNP must carry trigger switch and timestamp");
    expect(first->attempt_id() == attempt_id &&
               first->original_score() == original_score &&
               first->selected_score() == selected_score &&
               first->selected_gap() == selected_gap &&
               first->action_key() == action_key &&
               !first->need_endpoint_cooldown(),
           "N-MRC4 FastCNP must preserve the complete action tuple");
    first->free();

    RoceFastCnp* recycled = RoceFastCnp::newpkt(
        flow, route, 29, 0x1234, 10003,
        5, 6, STOR_LEVEL_AVOID, STOR_LEVEL_DEGRADED,
        77, timeFromUs(13.0));
    expect(recycled->source_host() == 29 && recycled->ev() == 0x1234 &&
               recycled->psn() == 10003 &&
               recycled->original_egress() == 5 &&
               recycled->selected_egress() == 6 &&
               recycled->original_level() == STOR_LEVEL_AVOID &&
               recycled->selected_level() == STOR_LEVEL_DEGRADED &&
               recycled->trigger_switch() == 77 &&
               recycled->trigger_time() == timeFromUs(13.0) &&
               recycled->attempt_id() == 0 &&
               std::isnan(recycled->original_score()) &&
               std::isnan(recycled->selected_score()) &&
               std::isnan(recycled->selected_gap()) &&
               recycled->action_key() == 0 &&
               recycled->need_endpoint_cooldown(),
           "recycled FastCNP packets must reset every metadata field");
    recycled->free();
}

static void test_fast_cnp_only_cools_ev_without_transport_or_cc_changes() {
    DataCaptureSink sink;
    Route route;
    route.push_back(&sink);

    RoceSrc::setLoadBalancing(RoceSrc::LB_NMRC);
    RoceSrc::setPathEntropySize(8);
    RoceSrc::setCongestionControl(RoceSrc::CC_DCTCP_VARIANT);
    RoceSrc* src = make_src(1501);
    src->_flow_started = true;
    src->init_nmrc_evs_for_test(8);
    std::vector<uint32_t> evs = src->nmrc_ev_values_for_test();
    std::vector<uint32_t> paths = src->nmrc_physical_paths_for_test();
    uint32_t ev = UINT32_MAX;
    uint32_t selected_ev = UINT32_MAX;
    for (uint32_t i = 0; i < evs.size(); i++) {
        if (paths[i] == 7)
            ev = evs[i];
        if (paths[i] == 3)
            selected_ev = evs[i];
    }
    expect(ev != UINT32_MAX && selected_ev != UINT32_MAX && ev != selected_ev,
           "FastCNP isolation test needs distinct original and selected EVs");

    src->_rtx_queue.insert(101);
    src->_rtx_queue.insert(202);
    RoceSrc::BoundedPacketState& bounded = src->_bounded_packets[303];
    bounded.state = RoceSrc::BOUNDED_RTX_PENDING;
    bounded.attempt_id = 5;
    bounded.counted_inflight = true;
    bounded.uses_recovery_reserve = true;
    src->_sack_rxt_psn = 404;
    src->_sack_rxt_psn_updated = timeFromUs(7.0);
    src->_sack_rxt_psn_valid = true;

    std::array<uint64_t, 64> isolation_baseline =
        src->fast_cnp_isolation_snapshot_for_test();
    src->_ooo_nacks_received++;
    expect(src->fast_cnp_isolation_snapshot_for_test() != isolation_baseline,
           "FastCNP isolation snapshot must include specialized NACK counters");
    src->_ooo_nacks_received--;
    src->_bounded_packets[303].attempt_id++;
    expect(src->fast_cnp_isolation_snapshot_for_test() != isolation_baseline,
           "FastCNP isolation snapshot must include bounded packet contents");
    src->_bounded_packets[303].attempt_id--;
    src->_sack_rxt_psn++;
    expect(src->fast_cnp_isolation_snapshot_for_test() != isolation_baseline,
           "FastCNP isolation snapshot must include SACK retransmission cursors");
    src->_sack_rxt_psn--;
    src->_rtx_queue.erase(101);
    src->_rtx_queue.insert(303);
    expect(src->_rtx_queue.size() == 2 &&
               src->fast_cnp_isolation_snapshot_for_test() != isolation_baseline,
           "FastCNP isolation snapshot must detect same-size retransmission content changes");
    src->_rtx_queue.erase(303);
    src->_rtx_queue.insert(101);

    uint32_t acks = src->_acks_received;
    uint32_t nacks = src->_nacks_received;
    linkspeed_bps bitrate = src->bitrate();
    uint64_t packets_sent = src->_packets_sent;
    uint32_t new_packets_sent = src->_new_packets_sent;
    uint32_t rtx_packets_sent = src->_rtx_packets_sent;
    uint32_t acked_packets = src->_acked_packets;
    uint32_t ooo_nacks = src->_ooo_nacks_received;
    uint32_t trim_nacks = src->_trim_nacks_received;
    uint32_t loss_nacks = src->_loss_nacks_received;
    uint32_t ecn_acks = src->_ecn_echo_acks_received;
    uint32_t duplicate_acks = src->_duplicate_acks_received;
    uint32_t duplicate_inflate = src->_duplicate_ack_inflate_suppressed;
    uint64_t bounded_inflight = src->_bounded_inflight_pkts;
    uint64_t bounded_unique = src->_bounded_unique_acks;
    uint32_t bounded_recovery = src->_bounded_recovery_inflight_bytes;
    uint32_t bounded_recovery_max = src->_bounded_recovery_inflight_max_bytes;
    uint64_t bounded_stale = src->_bounded_stale_attempt_nacks;
    uint64_t bounded_duplicate = src->_bounded_duplicate_failure_nacks;
    uint64_t bounded_confirmations =
        src->_bounded_duplicate_confirmations_suppressed;
    uint64_t bounded_revival = src->_bounded_acked_revival_rejected;
    uint64_t bounded_wraps = src->_bounded_attempt_wraps;
    uint64_t bounded_trim = src->_bounded_exact_trim_recoveries;
    uint64_t bounded_sack = src->_bounded_sack_loss_recoveries;
    uint32_t feedback_acks = src->_feedback_acks_received;
    uint32_t feedback_nacks = src->_feedback_nacks_received;
    uint32_t feedback_zero = src->_feedback_zero_bits_received;
    uint64_t last_acked = src->_last_acked;
    uint64_t highest_sent = src->_highest_sent;
    simtime_picosec rtt = src->_rtt;
    simtime_picosec rto = src->_rto;
    simtime_picosec mdev = src->_mdev;
    simtime_picosec base_rtt = src->_base_rtt;
    uint32_t pathid = src->_pathid;
    uint16_t send_state = src->_state_send;
    simtime_picosec time_last_sent = src->_time_last_sent;
    simtime_picosec send_event_time = src->_send_event_time;
    bool send_event_pending = src->_send_event_pending;
    simtime_picosec rtx_timeout = src->_rtx_timeout;
    size_t rtx_size = src->_rtx_queue.size();
    std::set<RocePacket::seq_t> rtx_contents = src->_rtx_queue._seqs;
    std::map<RocePacket::seq_t, RoceSrc::BoundedPacketState> bounded_packets =
        src->_bounded_packets;
    RocePacket::seq_t sack_rxt_psn = src->_sack_rxt_psn;
    simtime_picosec sack_rxt_updated = src->_sack_rxt_psn_updated;
    bool sack_rxt_valid = src->_sack_rxt_psn_valid;
    std::map<RocePacket::seq_t, uint32_t> mrc_seq_evs = src->_mrc_seq_ev;
    double cwnd = src->_cc_cwnd_pkts;
    double inflate = src->_cc_inflate_pkts;
    double alpha = src->_dcqcn_alpha;
    double current_rate = src->_dcqcn_current_rate;
    double target_rate = src->_dcqcn_target_rate;
    mem_b bytes_since_increase = src->_dcqcn_bytes_since_increase;
    uint32_t recovery_count = src->_dcqcn_recovery_count;
    bool seen_cnp = src->_dcqcn_seen_cnp;
    bool marked_since_alpha = src->_dcqcn_marked_since_alpha;
    simtime_picosec last_cnp = src->_dcqcn_last_cnp;
    simtime_picosec next_alpha_update = src->_dcqcn_next_alpha_update;
    simtime_picosec next_rate_increase = src->_dcqcn_next_rate_increase;
    simtime_picosec packet_spacing = src->_packet_spacing;

    RoceFastCnp* fast = make_fast_cnp(
        src->_flow, route, 17, ev, 11001);
    src->receivePacket(*fast);

    expect(src->nmrc_ev_cooling_for_test(ev),
           "a valid path FastCNP must cool its named EV");
    expect(!src->nmrc_ev_cooling_for_test(selected_ev),
           "a valid path FastCNP must not cool the selected egress EV");
    expect(src->_nmrc_fastcnp_arrived == 1 &&
               src->_nmrc_cooldown_starts == 1,
           "a valid path FastCNP must update only path-notification counters");
    expect(src->_acks_received == acks && src->_nacks_received == nacks &&
               src->bitrate() == bitrate &&
               src->_packets_sent == packets_sent &&
               src->_new_packets_sent == new_packets_sent &&
               src->_rtx_packets_sent == rtx_packets_sent &&
               src->_acked_packets == acked_packets &&
               src->_ooo_nacks_received == ooo_nacks &&
               src->_trim_nacks_received == trim_nacks &&
               src->_loss_nacks_received == loss_nacks &&
               src->_ecn_echo_acks_received == ecn_acks &&
               src->_duplicate_acks_received == duplicate_acks &&
               src->_duplicate_ack_inflate_suppressed == duplicate_inflate &&
               src->_bounded_inflight_pkts == bounded_inflight &&
               src->_bounded_unique_acks == bounded_unique &&
               src->_bounded_recovery_inflight_bytes == bounded_recovery &&
               src->_bounded_recovery_inflight_max_bytes == bounded_recovery_max &&
               src->_bounded_stale_attempt_nacks == bounded_stale &&
               src->_bounded_duplicate_failure_nacks == bounded_duplicate &&
               src->_bounded_duplicate_confirmations_suppressed ==
                   bounded_confirmations &&
               src->_bounded_acked_revival_rejected == bounded_revival &&
               src->_bounded_attempt_wraps == bounded_wraps &&
               src->_bounded_exact_trim_recoveries == bounded_trim &&
               src->_bounded_sack_loss_recoveries == bounded_sack &&
               src->_feedback_acks_received == feedback_acks &&
               src->_feedback_nacks_received == feedback_nacks &&
               src->_feedback_zero_bits_received == feedback_zero &&
               src->_last_acked == last_acked &&
               src->_highest_sent == highest_sent &&
               src->_rtt == rtt && src->_rto == rto &&
               src->_mdev == mdev && src->_base_rtt == base_rtt &&
               src->_pathid == pathid && src->_state_send == send_state &&
               src->_time_last_sent == time_last_sent &&
               src->_send_event_time == send_event_time &&
               src->_send_event_pending == send_event_pending &&
               src->_rtx_timeout == rtx_timeout &&
               src->_rtx_queue.size() == rtx_size &&
               src->_rtx_queue._seqs == rtx_contents &&
               bounded_packet_maps_equal(src->_bounded_packets,
                                         bounded_packets) &&
               src->_sack_rxt_psn == sack_rxt_psn &&
               src->_sack_rxt_psn_updated == sack_rxt_updated &&
               src->_sack_rxt_psn_valid == sack_rxt_valid &&
               src->_mrc_seq_ev == mrc_seq_evs,
           "path FastCNP must not acknowledge, time, or retransmit data");
    expect(src->_cc_cwnd_pkts == cwnd && src->_cc_inflate_pkts == inflate &&
               src->_dcqcn_alpha == alpha &&
               src->_dcqcn_current_rate == current_rate &&
               src->_dcqcn_target_rate == target_rate &&
               src->_dcqcn_bytes_since_increase == bytes_since_increase &&
               src->_dcqcn_recovery_count == recovery_count &&
               src->_dcqcn_seen_cnp == seen_cnp &&
               src->_dcqcn_marked_since_alpha == marked_since_alpha &&
               src->_dcqcn_last_cnp == last_cnp &&
               src->_dcqcn_next_alpha_update == next_alpha_update &&
               src->_dcqcn_next_rate_increase == next_rate_increase &&
               src->_packet_spacing == packet_spacing,
           "path FastCNP must not enter DCQCN or alter the send window");
    expect(src->nmrcFastCnpCcMutations() == 0,
           "FastCNP processing must report no transport or CC mutation");

    RoceFastCnp* reroute_only = make_fast_cnp(
        src->_flow, route, 17, selected_ev, 11000, false);
    src->receivePacket(*reroute_only);
    expect(!src->nmrc_ev_cooling_for_test(selected_ev),
           "reroute-only FastCNP must not cool its named EV");
    expect(src->_nmrc_cooldown_starts == 1,
           "reroute-only FastCNP must not start a cooldown");

    uint64_t expiry = src->nmrc_ev_cool_until_for_test(ev);
    RoceFastCnp* duplicate = make_fast_cnp(
        src->_flow, route, 17, ev, 11002);
    src->receivePacket(*duplicate);
    expect(src->nmrc_ev_cool_until_for_test(ev) == expiry &&
               src->_nmrc_duplicate_notifications == 1,
           "an in-flight FastCNP duplicate must not extend cooldown");

    RoceFastCnp* unknown_ev = make_fast_cnp(
        src->_flow, route, 17, 0xffff, 11003);
    src->receivePacket(*unknown_ev);
    expect(src->_nmrc_fastcnp_unknown_ev == 1,
           "an unknown EV FastCNP must be counted and ignored");

    PacketFlow other_flow(NULL);
    other_flow.set_flowid(99991);
    RoceFastCnp* unknown_qp = make_fast_cnp(
        other_flow, route, 17, ev, 11004);
    src->receivePacket(*unknown_qp);
    expect(src->_nmrc_fastcnp_unknown_qp == 1 &&
               src->nmrc_ev_cool_until_for_test(ev) == expiry,
           "an unknown QP FastCNP must not alter this source's EV state");

    uint64_t arrived_before_done = src->_nmrc_fastcnp_arrived;
    uint64_t cooldowns_before_done = src->_nmrc_cooldown_starts;
    src->_done = true;
    RoceFastCnp* after_done = make_fast_cnp(
        src->_flow, route, 17, ev, 11005);
    src->receivePacket(*after_done);
    expect(src->_nmrc_fastcnp_arrived == arrived_before_done + 1 &&
               src->_nmrc_fastcnp_after_done == 1,
           "a physically arrived FastCNP must be counted after QP completion");
    expect(src->_nmrc_cooldown_starts == cooldowns_before_done,
           "a post-completion FastCNP must not restart EV cooling");
}

static void test_receiver_counts_only_natural_ce_on_full_data() {
    ControlDropSink control_drop;
    Route route_out;
    Route route_back;
    route_out.push_back(&control_drop);
    route_back.push_back(&control_drop);

    RoceSrc* src = make_src(1551);
    RoceSink receiver;
    receiver.set_src(17);
    src->connect(&route_out, &route_back, receiver, TRIGGER_START);

    RocePacket::seq_t seq = 1;
    for (uint32_t detour = 0; detour < 2; detour++) {
        for (uint32_t marked = 0; marked < 2; marked++) {
            RocePacket* data = RocePacket::newpkt(
                src->_flow, seq, Packet::data_packet_size(),
                false, false, 113);
            data->set_src(17);
            data->set_mrc_ev(detour);
            data->set_nmrc_detour(detour != 0);
            if (detour)
                data->set_nmrc_actual_egress(3);
            data->set_flags(marked ? ECN_CE : 0);
            receiver.receivePacket(*data);
            seq += Packet::data_packet_size();
        }
    }

    expect(receiver.ecnNominalCe() == 1 && receiver.ecnDetourCe() == 1,
           "receiver must split natural CE between nominal and detour data");

    RocePacket* trimmed = RocePacket::newpkt(
        src->_flow, seq, Packet::data_packet_size(), false, false, 113);
    trimmed->set_src(17);
    trimmed->set_mrc_ev(0);
    trimmed->set_flags(ECN_CE);
    trimmed->strip_payload();
    receiver.receivePacket(*trimmed);
    expect(receiver.ecnNominalCe() == 1 && receiver.ecnDetourCe() == 1,
           "header-only TRIM packets must not count as natural CE");
}

static RoceNack* make_trim_nack(PacketFlow& flow, Route& route,
                                uint32_t ev, bool detour,
                                uint32_t actual_egress) {
    RoceNack* nack = RoceNack::newpkt(flow, route, 0);
    nack->set_reason(RoceNack::TRIM);
    nack->set_missing_psn(1);
    nack->set_attempt_id(0);
    nack->set_mrc_ev(ev);
    nack->set_nmrc_detour(detour);
    nack->set_nmrc_actual_egress(actual_egress);
    return nack;
}

static void test_trim_path_metadata_resets_on_packet_reuse() {
    DataCaptureSink sink;
    Route route;
    route.push_back(&sink);
    PacketFlow flow(NULL);
    flow.set_flowid(1601);

    RocePacket* data = RocePacket::newpkt(flow, route, 1, 4096, false, false);
    data->set_nmrc_detour(true);
    data->set_nmrc_actual_egress(3);
    expect(data->nmrc_detour() && data->nmrc_actual_egress() == 3,
           "data packets must expose the actual n-MRC detour path");
    data->free();

    RocePacket* recycled_data =
        RocePacket::newpkt(flow, route, 4097, 4096, false, false);
    expect(!recycled_data->nmrc_detour() &&
               !recycled_data->has_nmrc_actual_egress(),
           "recycled data packets must clear n-MRC detour metadata");
    recycled_data->free();

    RoceNack* nack = make_trim_nack(flow, route, 2, true, 3);
    expect(nack->nmrc_detour() && nack->nmrc_actual_egress() == 3,
           "TRIM NACKs must carry the data packet's actual detour path");
    nack->free();

    RoceNack* recycled_nack = RoceNack::newpkt(flow, route, 0);
    expect(!recycled_nack->nmrc_detour() &&
               !recycled_nack->has_nmrc_actual_egress(),
           "recycled NACKs must clear n-MRC detour metadata");
    recycled_nack->free();
}

static void test_actual_path_trim_is_default_nmrc_feedback() {
    DataCaptureSink sink;
    Route route;
    route.push_back(&sink);

    RoceSrc::setLoadBalancing(RoceSrc::LB_NMRC);
    RoceSrc* src = make_src(1701);
    src->init_nmrc_evs_for_test(8);
    std::vector<uint32_t> evs = src->nmrc_ev_values_for_test();
    std::vector<uint32_t> paths = src->nmrc_physical_paths_for_test();

    RoceNack* direct =
        make_trim_nack(src->_flow, route, evs[1], false, paths[1]);
    src->process_nmrc_trim_feedback(*direct, true);
    expect(src->nmrc_ev_cooling_for_test(evs[1]) &&
               src->_nmrc_trim_nominal_cooldown_starts == 1,
           "direct TRIM must cool its nominal EV by default");
    direct->free();

    RoceNack* detour =
        make_trim_nack(src->_flow, route, evs[2], true, paths[3]);
    src->process_nmrc_trim_feedback(*detour, true);
    expect(!src->nmrc_ev_cooling_for_test(evs[2]) &&
               src->nmrc_ev_cooling_for_test(evs[3]) &&
               src->_nmrc_trim_actual_cooldown_starts == 1,
           "detour TRIM must cool the EV mapped to actual egress by default");
    detour->free();

    RoceNack* rejected =
        make_trim_nack(src->_flow, route, evs[4], false, paths[4]);
    src->process_nmrc_trim_feedback(*rejected, false);
    expect(!src->nmrc_ev_cooling_for_test(evs[4]) &&
               src->_nmrc_trim_duplicate_stale_ignored == 1,
           "a rejected duplicate or stale TRIM must not start cooldown");
    rejected->free();
}

static void test_stateless_random_ignores_endpoint_feedback() {
    DataCaptureSink sink;
    Route route;
    route.push_back(&sink);

    RoceSrc::setLoadBalancing(RoceSrc::LB_NMRC);
    RoceSrc::setNmrcEndpointPolicy(RoceSrc::NMRC_ENDPOINT_RANDOM_STATELESS);
    RoceSrc::setNmrcAllCoolingPolicy(RoceSrc::NMRC_ALL_COOLING_EARLIEST);
    RoceSrc* src = make_src(1802);
    src->_flow_started = true;

    RoceFastCnp* fast = make_fast_cnp(
        src->_flow, route, 17, 3, 12001);
    src->receivePacket(*fast);
    expect(src->_nmrc_fastcnp_arrived == 1 &&
               src->_nmrc_fastcnp_policy_ignored == 1,
           "stateless random must count arrived and policy-ignored FastCNP");

    RoceNack* trim = make_trim_nack(src->_flow, route, 4, false, 4);
    src->process_nmrc_trim_feedback(*trim, true);
    trim->free();
    expect(src->_nmrc_trim_policy_ignored == 1,
           "stateless random must count accepted TRIM ignored by policy");
    expect(src->_nmrc_cooldown_starts == 0 &&
               src->_nmrc_cooling_skips == 0 &&
               src->_nmrc_cooling_recoveries == 0 &&
               src->_nmrc_all_cooling_fallbacks == 0 &&
               src->_nmrc_trim_nominal_cooldown_starts == 0 &&
               src->_nmrc_trim_actual_cooldown_starts == 0,
           "stateless random feedback must not create endpoint cooldown");
}

int main() {
    test_set_sizes_and_uniqueness();
    test_per_qp_determinism_and_dephasing();
    test_ev_initialization_does_not_consume_global_random();
    test_one_round_cooling_and_idempotence();
    test_all_cooling_uses_earliest_expiry_fallback();
    test_nmrc_policy_defaults_and_names();
    test_all_cooling_rr_reset_episode();
    test_all_cooling_rr_reset_is_per_qp();
    test_ev_rebuild_abandons_unfinished_rr_reset();
    test_nmrc_stateless_random_selection();
    test_data_packet_carries_exact_ev_and_retransmission_reselects_it();
    test_fast_cnp_metadata_priority_and_recycling();
    test_fast_cnp_only_cools_ev_without_transport_or_cc_changes();
    test_receiver_counts_only_natural_ce_on_full_data();
    test_trim_path_metadata_resets_on_packet_reuse();
    test_actual_path_trim_is_default_nmrc_feedback();
    test_stateless_random_ignores_endpoint_feedback();
    std::cout << "Hybrid n-MRC EV-state tests passed" << std::endl;
    return 0;
}
