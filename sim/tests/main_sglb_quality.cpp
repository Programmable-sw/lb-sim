// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <iostream>
#include <limits>
#include <set>
#include "eventlist.h"
#include "network.h"
#include "compositequeue.h"
#include "queue.h"
#include "datacenter/fat_tree_switch.h"
#include "datacenter/fat_tree_topology.h"
#include "ecn.h"
#include "roce.h"
#include "rocepacket.h"
#include "tcppacket.h"

static void expect(bool condition, const char* message);

class DropSink : public PacketSink {
public:
    DropSink() : _name("drop_sink"), _packets(0) {}

    void receivePacket(Packet& pkt) {
        _packets++;
        pkt.free();
    }

    const string& nodename() { return _name; }

    string _name;
    uint32_t _packets;
};

class NoopTimer : public EventSource {
public:
    NoopTimer(EventList& eventlist) : EventSource(eventlist, "noop_timer") {}
    void doNextEvent() {}
};

class FastCnpCapture : public PacketSink {
public:
    FastCnpCapture() : _name("fast_cnp_capture") {}

    void receivePacket(Packet& pkt) {
        expect(pkt.type() == ROCEFASTCNP,
               "source host control route should carry FastCNP");
        RoceFastCnp& fast = (RoceFastCnp&)pkt;
        evs.push_back(fast.ev());
        latencies.push_back(EventList::now() - fast.trigger_time());
        original_egresses.push_back(fast.original_egress());
        selected_egresses.push_back(fast.selected_egress());
        original_levels.push_back(fast.original_level());
        selected_levels.push_back(fast.selected_level());
        psns.push_back(fast.psn());
        attempts.push_back(fast.attempt_id());
        original_scores.push_back(fast.original_score());
        selected_scores.push_back(fast.selected_score());
        selected_gaps.push_back(fast.selected_gap());
        action_keys.push_back(fast.action_key());
        cooldown_requests.push_back(fast.need_endpoint_cooldown());
        pkt.free();
    }

    const string& nodename() { return _name; }

    string _name;
    vector<uint32_t> evs;
    vector<simtime_picosec> latencies;
    vector<uint32_t> original_egresses;
    vector<uint32_t> selected_egresses;
    vector<uint8_t> original_levels;
    vector<uint8_t> selected_levels;
    vector<RocePacket::seq_t> psns;
    vector<uint8_t> attempts;
    vector<double> original_scores;
    vector<double> selected_scores;
    vector<double> selected_gaps;
    vector<uint64_t> action_keys;
    vector<bool> cooldown_requests;
};

class AckCapture : public PacketSink {
public:
    AckCapture() : _name("ack_capture"), ecn_echoes(0) {}

    void receivePacket(Packet& pkt) {
        expect(pkt.type() == ROCEACK,
               "RoCE receiver should return an ACK");
        if (pkt.flags() & ECN_ECHO)
            ecn_echoes++;
        pkt.free();
    }

    const string& nodename() { return _name; }

    string _name;
    uint32_t ecn_echoes;
};

class TestQueue : public BaseQueue {
public:
    TestQueue(linkspeed_bps bitrate, mem_b maxsize, EventList& eventlist)
        : BaseQueue(bitrate, eventlist, NULL), _queuesize(0), _maxsize(maxsize) {}

    void receivePacket(Packet& pkt) {
        _queuesize += pkt.size();
    }

    void doNextEvent() {}

    mem_b queuesize() const {
        return _queuesize;
    }

    mem_b maxsize() const {
        return _maxsize;
    }

    void set_queuesize(mem_b queuesize) {
        _queuesize = queuesize;
    }

    int utilization_history_samples() {
        return _busyend.size();
    }

private:
    mem_b _queuesize;
    mem_b _maxsize;
};

static Route* make_route(BaseQueue* queue) {
    Route* route = new Route();
    route->push_back(queue);
    return route;
}

static void expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << message << std::endl;
        std::exit(1);
    }
}

static void expect_near(double actual, double expected, double tolerance,
                        const char* message) {
    if (std::fabs(actual - expected) > tolerance) {
        std::cerr << message << ": expected " << expected
                  << ", got " << actual << std::endl;
        std::exit(1);
    }
}

static void test_nmrc_graded_selective_cooldown() {
    expect(!FatTreeSwitch::nmrc_graded_requests_cooldown(
               0.401, 0.399, 0.25),
           "a tiny cross-bucket gap must not cool");
    expect(FatTreeSwitch::nmrc_graded_requests_cooldown(
               0.65, 0.40, 0.25),
           "the cooldown threshold must be inclusive");
    expect(FatTreeSwitch::nmrc_graded_requests_cooldown(
               0.80, 0.40, 0.25),
           "a large improvement must cool");
    expect(!FatTreeSwitch::nmrc_graded_requests_cooldown(
               std::numeric_limits<double>::quiet_NaN(), 0.10, 0.25),
           "invalid scores must not cool");
}

static void test_nmrc_graded_delta_selector() {
    std::vector<uint8_t> levels{
        STOR_LEVEL_BAD, STOR_LEVEL_GOOD, STOR_LEVEL_DEGRADED,
        STOR_LEVEL_GOOD, STOR_LEVEL_DEGRADED};
    std::vector<double> scores{0.70, 0.44, 0.45, 0.45, 0.60};
    std::vector<bool> available(5, true);

    FatTreeSwitch::NmrcRerouteDecision d =
        FatTreeSwitch::nmrc_select_graded_delta_path(
            0, levels, scores, available,
            FatTreeSwitch::NMRC_REROUTE_BETTER_GE3, 8, 0, 0.25);
    expect(d.reroute && d.better_count == 3,
           "graded delta must count only strict-better paths meeting delta");
    expect(d.selected_index == 1 || d.selected_index == 2 ||
               d.selected_index == 3,
           "graded delta must select only a strict-better delta candidate");

    scores[3] = 0.51;
    d = FatTreeSwitch::nmrc_select_graded_delta_path(
        0, levels, scores, available,
        FatTreeSwitch::NMRC_REROUTE_BETTER_GE3, 8, 0, 0.25);
    expect(!d.reroute && d.better_count == 2,
           "graded delta must block when fewer than three paths meet delta");
}

static void test_nmrc_graded_cooldown_modes() {
    expect(FatTreeSwitch::nmrc_graded_cooldown_requested(
               FatTreeSwitch::NMRC_GRADED_COOLDOWN_FULL,
               0.401, 0.399, 0.25),
           "full mode must cool every committed reroute");
    expect(!FatTreeSwitch::nmrc_graded_cooldown_requested(
               FatTreeSwitch::NMRC_GRADED_COOLDOWN_NONE,
               0.80, 0.10, 0.25),
           "none mode must never cool a committed reroute");
    expect(!FatTreeSwitch::nmrc_graded_cooldown_requested(
               FatTreeSwitch::NMRC_GRADED_COOLDOWN_SELECTIVE,
               0.401, 0.399, 0.25),
           "selective mode must suppress tiny raw-score gaps");
    expect(FatTreeSwitch::nmrc_graded_cooldown_requested(
               FatTreeSwitch::NMRC_GRADED_COOLDOWN_SELECTIVE,
               0.70, 0.40, 0.25),
           "selective mode must cool sufficiently large raw-score gaps");
}

static void test_nmrc_relative_delta_selector() {
    std::vector<double> scores{0.70, 0.40, 0.20, 0.60};
    std::vector<bool> valid(4, true);
    std::vector<bool> available(4, true);

    FatTreeSwitch::NmrcRelativeDecision d =
        FatTreeSwitch::nmrc_select_fixed_threshold_path(
            0, scores, valid, available, 0.50, 0.30, 1);
    expect(d.reroute && d.candidate_count == 2,
           "delta selector must admit every path at least Delta better");
    expect((d.selected_index == 1 || d.selected_index == 2) &&
               d.selected_index != 0,
           "delta selector must hash inside the eligible set");
    expect(d.original_score == 0.70 && d.selected_gap >= 0.30 - 1e-12,
           "delta selector must expose the committed score gap");

    scores[1] = 0.4000000000005;
    d = FatTreeSwitch::nmrc_select_fixed_threshold_path(
        0, scores, valid, available, 0.50, 0.30, 0);
    expect(d.reroute,
           "the documented epsilon must admit a representational boundary");

    valid[0] = false;
    d = FatTreeSwitch::nmrc_select_fixed_threshold_path(
        0, scores, valid, available, 0.50, 0.30, 0);
    expect(!d.reroute &&
               d.reason == FatTreeSwitch::NMRC_RELATIVE_ORIGINAL_UNKNOWN,
           "an unknown original must not trigger a relative action");

    valid[0] = true;
    available[1] = false;
    available[2] = false;
    scores[3] = 0.45;
    d = FatTreeSwitch::nmrc_select_fixed_threshold_path(
        0, scores, valid, available, 0.50, 0.30, 0);
    expect(!d.reroute &&
               d.reason == FatTreeSwitch::NMRC_RELATIVE_NO_DELTA_CANDIDATE,
           "unavailable and insufficient-gap paths must not qualify");

    scores = {0.95, 0.60};
    valid.assign(2, true);
    available.assign(2, true);
    d = FatTreeSwitch::nmrc_select_fixed_threshold_path(
        0, scores, valid, available, 0.50, 0.30, 0);
    expect(!d.reroute &&
               d.reason == FatTreeSwitch::NMRC_RELATIVE_NO_SAFE_CANDIDATE,
           "absolute gate must reject a replacement that is also congested");

    scores = {0.49, 0.10};
    d = FatTreeSwitch::nmrc_select_fixed_threshold_path(
        0, scores, valid, available, 0.50, 0.30, 0);
    expect(!d.reroute &&
               d.reason ==
                   FatTreeSwitch::NMRC_RELATIVE_ORIGINAL_BELOW_ABSOLUTE,
           "absolute gate must leave a non-congested original path alone");

    scores = {0.70, 0.45};
    d = FatTreeSwitch::nmrc_select_fixed_threshold_path(
        0, scores, valid, available, 0.50, 0.30, 0);
    expect(!d.reroute &&
               d.reason == FatTreeSwitch::NMRC_RELATIVE_NO_DELTA_CANDIDATE,
           "safe replacements must still satisfy the relative delta");

    d = FatTreeSwitch::nmrc_select_fixed_threshold_path(
        0, scores, valid, available,
        std::numeric_limits<double>::quiet_NaN(), 0.30, 0);
    expect(!d.reroute &&
               d.reason == FatTreeSwitch::NMRC_RELATIVE_INVALID_INPUT,
           "a non-finite absolute threshold must fail closed");
}

static void test_nmrc_two_stage_delta_selector() {
    std::vector<double> scores{0.70, 0.55, 0.35};
    std::vector<bool> valid(3, true);
    std::vector<bool> available(3, true);
    FatTreeSwitch::NmrcRelativeDecision d =
        FatTreeSwitch::nmrc_select_two_stage_delta_path(
            0, scores, valid, available, 0.10, 0.30, 1);
    expect(d.reroute && d.request_cooldown,
           "a gap above the cooldown delta must request endpoint cooldown");
    scores = {0.70, 0.55};
    valid.assign(2, true);
    available.assign(2, true);
    d = FatTreeSwitch::nmrc_select_two_stage_delta_path(
        0, scores, valid, available, 0.10, 0.30, 0);
    expect(d.reroute && !d.request_cooldown,
           "a middle-band gap must reroute without endpoint cooldown");
    scores[1] = 0.65;
    d = FatTreeSwitch::nmrc_select_two_stage_delta_path(
        0, scores, valid, available, 0.10, 0.30, 0);
    expect(!d.reroute,
           "a gap below the route delta must keep the original path");
}

static void test_nmrc_absolute_reroute_selector() {
    std::vector<double> scores{0.70, 0.60, 0.35};
    std::vector<bool> valid(3, true);
    std::vector<bool> available(3, true);
    FatTreeSwitch::NmrcRelativeDecision d =
        FatTreeSwitch::nmrc_select_absolute_reroute_path(
            0, scores, valid, available, 0.50, 0.30, 0);
    expect(d.reroute && d.selected_index == 2 && d.request_cooldown,
           "absolute reroute must choose a best path and cool an outlier");
    scores = {0.70, 0.60};
    valid.assign(2, true);
    available.assign(2, true);
    d = FatTreeSwitch::nmrc_select_absolute_reroute_path(
        0, scores, valid, available, 0.50, 0.30, 0);
    expect(d.reroute && !d.request_cooldown,
           "absolute reroute must rescue without cooling for a small gap");
    scores = {0.49, 0.10};
    d = FatTreeSwitch::nmrc_select_absolute_reroute_path(
        0, scores, valid, available, 0.50, 0.30, 0);
    expect(!d.reroute,
           "a non-congested original must not trigger absolute reroute");
}

static void test_nmrc_piecewise_delta_selector() {
    std::vector<bool> valid(3, true);
    std::vector<bool> available(3, true);
    std::vector<double> scores{0.49, 0.23, 0.30};
    FatTreeSwitch::NmrcRelativeDecision d =
        FatTreeSwitch::nmrc_select_piecewise_delta_path(
            0, scores, valid, available, 0.50, 0.25, 0.15, 0);
    expect(d.reroute && d.selected_index == 1,
           "below the breakpoint the selector must use delta 0.25");

    scores = {0.70, 0.54, 0.60};
    d = FatTreeSwitch::nmrc_select_piecewise_delta_path(
        0, scores, valid, available, 0.50, 0.25, 0.15, 0);
    expect(d.reroute && d.selected_index == 1 && d.selected_score > 0.50,
           "above the breakpoint delta 0.15 must allow a relatively better congested path");

    scores = {0.49, 0.25, 0.10};
    available[2] = false;
    d = FatTreeSwitch::nmrc_select_piecewise_delta_path(
        0, scores, valid, available, 0.50, 0.25, 0.15, 0);
    expect(!d.reroute &&
               d.reason == FatTreeSwitch::NMRC_RELATIVE_NO_DELTA_CANDIDATE,
           "piecewise delta must fail closed without an eligible path");
}

static void test_nmrc_relative_action_identity_and_histogram_boundaries() {
    const uint32_t flow_id = 1601;
    const RocePacket::seq_t psn = 0x123456789abcdef0ULL;
    const uint8_t attempt = 3;
    const uint32_t original_ev = 17;
    const uint32_t original_egress = 1;
    const uint32_t selected_egress = 3;
    const uint64_t key = FatTreeSwitch::nmrc_relative_action_key(
        flow_id, psn, attempt, original_ev,
        original_egress, selected_egress);
    expect(key == FatTreeSwitch::nmrc_relative_action_key(
                      flow_id, psn, attempt, original_ev,
                      original_egress, selected_egress),
           "relative action key must be stable for an identical tuple");
    expect(key != FatTreeSwitch::nmrc_relative_action_key(
                      flow_id + 1, psn, attempt, original_ev,
                      original_egress, selected_egress) &&
               key != FatTreeSwitch::nmrc_relative_action_key(
                      flow_id, psn + 1, attempt, original_ev,
                      original_egress, selected_egress) &&
               key != FatTreeSwitch::nmrc_relative_action_key(
                      flow_id, psn + (1ULL << 32), attempt, original_ev,
                      original_egress, selected_egress) &&
               key != FatTreeSwitch::nmrc_relative_action_key(
                      flow_id, psn, attempt + 1, original_ev,
                      original_egress, selected_egress) &&
               key != FatTreeSwitch::nmrc_relative_action_key(
                      flow_id, psn, attempt, original_ev + 1,
                      original_egress, selected_egress) &&
               key != FatTreeSwitch::nmrc_relative_action_key(
                      flow_id, psn, attempt, original_ev,
                      original_egress + 1, selected_egress) &&
               key != FatTreeSwitch::nmrc_relative_action_key(
                      flow_id, psn, attempt, original_ev,
                      original_egress, selected_egress + 1),
           "all six tuple fields must contribute to the relative action key");
    expect(FatTreeSwitch::nmrc_relative_hist_bin(0.30) == 6 &&
               FatTreeSwitch::nmrc_relative_hist_bin(
                   0.30 - FatTreeSwitch::NMRC_RELATIVE_EPSILON / 2.0) == 6,
           "relative histogram boundaries must use the selector epsilon");
}

int main() {
    FatTreeSwitch::configure_sglb_scheme_defaults(false);
    expect(FatTreeSwitch::_sglb_ofat_factor ==
               FatTreeSwitch::SGLB_OFAT_REAL_GCN_RAW_LINEAR,
           "sglb must default to real-GCN raw-linear paper semantics");
    expect(FatTreeSwitch::_sglb_min_choices == 1 &&
           FatTreeSwitch::_sglb_candidate_policy == FatTreeSwitch::SGLB_CANDIDATE_WHOLE_GRADE_MIN,
           "sglb must default to best-grade-only selection");
    expect_near(FatTreeSwitch::_sglb_nmrc_degraded_threshold, 0.05, 1e-12,
                "sglb good threshold must default to 5 percent");
    expect_near(FatTreeSwitch::_sglb_nmrc_bad_threshold, 0.10, 1e-12,
                "sglb degraded threshold must default to 10 percent");
    expect_near(FatTreeSwitch::_sglb_nmrc_avoid_threshold, 0.20, 1e-12,
                "sglb bad threshold must default to 20 percent");

    FatTreeSwitch::configure_sglb_scheme_defaults(true);
    expect(FatTreeSwitch::_sglb_ofat_factor ==
               FatTreeSwitch::SGLB_OFAT_BASELINE,
           "sglb-old must retain the original implementation");
    expect(FatTreeSwitch::_sglb_min_choices == 3,
           "sglb-old must retain min3 selection");
    expect_near(FatTreeSwitch::_sglb_nmrc_degraded_threshold, 0.10, 1e-12,
                "sglb-old must retain the 10 percent first threshold");
    expect_near(FatTreeSwitch::_sglb_nmrc_bad_threshold, 0.40, 1e-12,
                "sglb-old must retain the 40 percent second threshold");
    expect_near(FatTreeSwitch::_sglb_nmrc_avoid_threshold, 0.60, 1e-12,
                "sglb-old must retain the 60 percent third threshold");

    EventList eventlist;

    expect(FatTreeSwitch::_sglb_score_mode ==
               FatTreeSwitch::SGLB_SCORE_NMRC_QUANTIZED_TOPK,
           "four-level quantized top-k should be the SGLB default");
    expect(FatTreeSwitch::_sglb_nmrc_levels == 4,
           "default SGLB quantizer should use four levels");
    expect(!std::strcmp(FatTreeSwitch::sglb_score_mode_name(),
                        "nmrc_quantized_topk"),
           "default SGLB score mode should have a stable diagnostic name");
    expect(FatTreeSwitch::_sglb_gcn_update_interval == timeFromUs(15.0),
           "SGLB downstream GCN export should refresh every 15us by default");
    expect(FatTreeSwitch::_nmrc_reroute_policy ==
               FatTreeSwitch::NMRC_REROUTE_BETTER_GE3,
           "library and CLI hybrid n-MRC defaults must use better-ge3");
    expect(FatTreeSwitch::_nmrc_network_decision_mode ==
               FatTreeSwitch::NMRC_NETWORK_GRADED,
           "legacy graded n-MRC decision mode should remain the default");

    expect(FatTreeSwitch::nmrc_binary_classify(0.499999, true) ==
               FatTreeSwitch::NMRC_BINARY_SAFE,
           "binary n-MRC should classify a finite complete score below 0.5 as SAFE");
    expect(FatTreeSwitch::nmrc_binary_classify(0.5, true) ==
               FatTreeSwitch::NMRC_BINARY_CONGESTED,
           "binary n-MRC should classify the 0.5 boundary as CONGESTED");
    expect(FatTreeSwitch::nmrc_binary_classify(
               std::numeric_limits<double>::infinity(), true) ==
               FatTreeSwitch::NMRC_BINARY_UNKNOWN,
           "binary n-MRC should classify positive infinity as UNKNOWN");
    expect(FatTreeSwitch::nmrc_binary_classify(
               std::numeric_limits<double>::quiet_NaN(), true) ==
               FatTreeSwitch::NMRC_BINARY_UNKNOWN,
           "binary n-MRC should classify NaN as UNKNOWN");
    expect(FatTreeSwitch::nmrc_binary_classify(0.1, false) ==
               FatTreeSwitch::NMRC_BINARY_UNKNOWN,
           "binary n-MRC should classify incomplete two-hop telemetry as UNKNOWN");

    expect_near(FatTreeSwitch::sglb_nmrc_queue_pressure(0.20), 0.0, 1e-12,
                "n-MRC grade pressure should start at the ECN minimum");
    expect_near(FatTreeSwitch::sglb_nmrc_queue_pressure(0.50), 0.5, 1e-12,
                "n-MRC grade pressure should interpolate across the ECN range");
    expect_near(FatTreeSwitch::sglb_nmrc_queue_pressure(0.80), 1.0, 1e-12,
                "n-MRC grade pressure should saturate at the ECN maximum");
    expect_near(FatTreeSwitch::sglb_nmrc_noisy_or(0.25, 0.50), 0.625, 1e-12,
                "n-MRC grade path score should use noisy-or hop coupling");
    expect(FatTreeSwitch::sglb_nmrc_level(0.09) == STOR_LEVEL_GOOD,
           "score below 0.10 should be GOOD");
    expect(FatTreeSwitch::sglb_nmrc_level(0.10) == STOR_LEVEL_DEGRADED,
           "score at 0.10 should be DEGRADED");
    expect(FatTreeSwitch::sglb_nmrc_level(0.40) == STOR_LEVEL_BAD,
           "score at 0.40 should be BAD");
    expect(FatTreeSwitch::sglb_nmrc_level(0.60) == STOR_LEVEL_AVOID,
           "score at 0.60 should be AVOID");
    expect(FatTreeSwitch::sglb_nmrc_quantized_level(0.09, 4) == 0,
           "four-level quantizer should preserve the GOOD boundary");
    expect(FatTreeSwitch::sglb_nmrc_quantized_level(0.10, 4) == 1,
           "four-level quantizer should enter level 1 at 0.10");
    expect(FatTreeSwitch::sglb_nmrc_quantized_level(0.40, 4) == 2,
           "four-level quantizer should enter level 2 at 0.40");
    expect(FatTreeSwitch::sglb_nmrc_quantized_level(0.60, 4) == 3,
           "four-level quantizer should enter level 3 at 0.60");
    expect(FatTreeSwitch::sglb_nmrc_quantized_level(0.049, 8) == 0,
           "eight-level quantizer should keep scores below 0.05 in level 0");
    expect(FatTreeSwitch::sglb_nmrc_quantized_level(0.05, 8) == 1,
           "eight-level quantizer should split the original GOOD level");
    expect(FatTreeSwitch::sglb_nmrc_quantized_level(0.10, 8) == 2,
           "eight-level quantizer should preserve the 0.10 boundary");
    expect(FatTreeSwitch::sglb_nmrc_quantized_level(0.25, 8) == 3,
           "eight-level quantizer should split the original DEGRADED level");
    expect(FatTreeSwitch::sglb_nmrc_quantized_level(0.40, 8) == 4,
           "eight-level quantizer should preserve the 0.40 boundary");
    expect(FatTreeSwitch::sglb_nmrc_quantized_level(0.50, 8) == 5,
           "eight-level quantizer should split the original BAD level");
    expect(FatTreeSwitch::sglb_nmrc_quantized_level(0.60, 8) == 6,
           "eight-level quantizer should preserve the 0.60 boundary");
    expect(FatTreeSwitch::sglb_nmrc_quantized_level(0.80, 8) == 7,
           "eight-level quantizer should split the original AVOID level");

    {
        vector<double> scores;
        scores.push_back(0.499999);
        scores.push_back(0.1);
        vector<bool> complete(2, true);
        vector<bool> available(2, true);
        FatTreeSwitch::NmrcRerouteDecision safe_original =
            FatTreeSwitch::nmrc_select_binary_path(
                0, scores, complete, available, 0);
        expect(!safe_original.reroute,
               "a SAFE binary original must never reroute");

        scores[0] = 0.5;
        scores[1] = 0.499999;
        FatTreeSwitch::NmrcRerouteDecision boundary =
            FatTreeSwitch::nmrc_select_binary_path(
                0, scores, complete, available, 0);
        expect(boundary.reroute && boundary.selected_index == 1 &&
                   boundary.original_level == 1 &&
                   boundary.selected_level == 0,
               "a CONGESTED binary original should select an available SAFE path with transition 1 to 0");

        available[1] = false;
        FatTreeSwitch::NmrcRerouteDecision unavailable_safe =
            FatTreeSwitch::nmrc_select_binary_path(
                0, scores, complete, available, 0);
        expect(!unavailable_safe.reroute,
               "an unavailable SAFE binary candidate must not receive reroute");

        available[1] = true;
        complete[1] = false;
        FatTreeSwitch::NmrcRerouteDecision unknown_candidate =
            FatTreeSwitch::nmrc_select_binary_path(
                0, scores, complete, available, 0);
        expect(!unknown_candidate.reroute,
               "an UNKNOWN binary candidate must not receive reroute");

        complete[0] = false;
        complete[1] = true;
        FatTreeSwitch::NmrcRerouteDecision unknown_original =
            FatTreeSwitch::nmrc_select_binary_path(
                0, scores, complete, available, 0);
        expect(!unknown_original.reroute,
               "an UNKNOWN binary original must not trigger reroute");
    }

    test_nmrc_graded_selective_cooldown();
    test_nmrc_graded_delta_selector();
    test_nmrc_graded_cooldown_modes();
    test_nmrc_relative_delta_selector();
    test_nmrc_two_stage_delta_selector();
    test_nmrc_absolute_reroute_selector();
    test_nmrc_piecewise_delta_selector();
    test_nmrc_relative_action_identity_and_histogram_boundaries();

    {
        for (uint32_t better = 1; better <= 3; better++) {
            vector<uint8_t> levels(4, STOR_LEVEL_BAD);
            vector<bool> available(4, true);
            for (uint32_t i = 1; i <= better; i++)
                levels[i] = i == 1 ? STOR_LEVEL_GOOD :
                    STOR_LEVEL_DEGRADED;
            FatTreeSwitch::NmrcRerouteDecision any =
                FatTreeSwitch::nmrc_select_better_path(
                    0, levels, available,
                    FatTreeSwitch::NMRC_REROUTE_ANY_BETTER,
                    3, 17);
            expect(any.reroute && any.better_count == better,
                   "any-better policy must reroute for one, two, or three strict upgrades");
            expect(any.selected_index != 0 &&
                       any.selected_level < any.original_level,
                   "hybrid selection must choose a different strict-better path");

            FatTreeSwitch::NmrcRerouteDecision ge3 =
                FatTreeSwitch::nmrc_select_better_path(
                    0, levels, available,
                    FatTreeSwitch::NMRC_REROUTE_BETTER_GE3,
                    3, 17);
            expect(ge3.reroute == (better >= 3),
                   "better-ge3 policy must require three strict-better candidates");
        }

        vector<uint8_t> same_level(4, STOR_LEVEL_DEGRADED);
        vector<bool> all_available(4, true);
        FatTreeSwitch::NmrcRerouteDecision same =
            FatTreeSwitch::nmrc_select_better_path(
                0, same_level, all_available,
                FatTreeSwitch::NMRC_REROUTE_ANY_BETTER,
                3, 0);
        expect(!same.reroute && same.better_count == 0,
               "same-level raw score differences must never trigger hybrid rerouting");

        vector<uint8_t> unavailable_levels;
        unavailable_levels.push_back(STOR_LEVEL_BAD);
        unavailable_levels.push_back(STOR_LEVEL_GOOD);
        vector<bool> unavailable;
        unavailable.push_back(true);
        unavailable.push_back(false);
        FatTreeSwitch::NmrcRerouteDecision down =
            FatTreeSwitch::nmrc_select_better_path(
                0, unavailable_levels, unavailable,
                FatTreeSwitch::NMRC_REROUTE_ANY_BETTER,
                3, 0);
        expect(!down.reroute && down.better_count == 0,
               "an unavailable strict-better path must not qualify");

        vector<uint8_t> kmin_levels(2, STOR_LEVEL_GOOD);
        vector<bool> kmin_available(2, true);
        FatTreeSwitch::NmrcRerouteDecision kmin =
            FatTreeSwitch::nmrc_select_better_path(
                0, kmin_levels, kmin_available,
                FatTreeSwitch::NMRC_REROUTE_ANY_BETTER,
                3, 1);
        expect(!kmin.reroute,
               "a path exactly at Kmin is GOOD and must not reroute to another GOOD path");
    }

    {
        TestQueue queue(
            speedFromMbps((uint64_t)10000), 65536, eventlist);
        NoopTimer timer(eventlist);
        eventlist.sourceIsPendingRel(timer, timeFromUs(1.0));
        eventlist.doNextEvent();
        queue.log_packet_send(timeFromUs(1.0));
        expect(queue.utilization_history_samples() == 1,
               "queue should retain the recorded utilization interval");

        eventlist.sourceIsPendingRel(timer, timeFromUs(31.0));
        eventlist.doNextEvent();
        expect(queue.peek_average_utilization() == 0,
               "expired trace utilization should read as zero");
        expect(queue.utilization_history_samples() == 1,
               "trace utilization must not consume live history");
        expect(queue.average_utilization() == 0,
               "live utilization should also exclude expired history");
        expect(queue.utilization_history_samples() == 0,
               "live utilization may prune expired history");
    }

    {
        CompositeQueue queue(speedFromMbps((uint64_t)10000), 65536, eventlist, NULL);
        DropSink sink;
        route_t route;
        route.push_back(&sink);

        PacketFlow flow(NULL);
        TcpPacket* pkt = TcpPacket::newpkt(flow, route, 1, 4096);
        queue.receivePacket(*pkt);
        eventlist.doNextEvent();

        expect(sink._packets == 1, "packet should leave the composite queue");
        expect(queue.average_utilization() > 0,
               "CompositeQueue should record service time for SGLB utilization scoring");
    }

    {
        FatTreeSwitch sw(eventlist, "sglb_nmrc_quantized_topk_sw",
                         FatTreeSwitch::AGG, 3, 0, NULL);
        TestQueue q0(speedFromMbps((uint64_t)10000), 65536, eventlist);
        TestQueue q1(speedFromMbps((uint64_t)10000), 65536, eventlist);
        TestQueue q2(speedFromMbps((uint64_t)10000), 65536, eventlist);
        TestQueue q3(speedFromMbps((uint64_t)10000), 65536, eventlist);
        q0.set_queuesize(0);
        q1.set_queuesize(20972);
        q2.set_queuesize(20972);
        q3.set_queuesize(20972);

        Route* r0 = make_route(&q0);
        Route* r1 = make_route(&q1);
        Route* r2 = make_route(&q2);
        Route* r3 = make_route(&q3);
        FibEntry e0(r0, 1, UP);
        FibEntry e1(r1, 1, UP);
        FibEntry e2(r2, 1, UP);
        FibEntry e3(r3, 1, UP);
        vector<FibEntry*> routes;
        routes.push_back(&e0);
        routes.push_back(&e1);
        routes.push_back(&e2);
        routes.push_back(&e3);

        FatTreeSwitch::_sglb_update_interval = 0;
        FatTreeSwitch::_sglb_score_mode =
            FatTreeSwitch::SGLB_SCORE_NMRC_QUANTIZED_TOPK;
        FatTreeSwitch::_sglb_nmrc_levels = 4;
        FatTreeSwitch::_sglb_min_choices = 3;
        FatTreeSwitch::_sglb_ofat_factor =
            FatTreeSwitch::SGLB_OFAT_REAL_GCN_RAW_LINEAR;
        const auto saved_policy = FatTreeSwitch::_sglb_candidate_policy;
        FatTreeSwitch::_sglb_candidate_policy = FatTreeSwitch::SGLB_CANDIDATE_EXACT_MIN;
        FatTreeSwitch::reset_sglb_route_diag();
        for (uint32_t i = 0; i < 64; i++)
            sw.sglb_route(&routes, 79);
        expect(FatTreeSwitch::_sglb_diag_candidate_choices == 64 * 3,
               "quantized selection should sample the boundary level and "
               "stop exactly at K");

        FatTreeSwitch::_sglb_candidate_policy = saved_policy;
        FatTreeSwitch::_sglb_ofat_factor = FatTreeSwitch::SGLB_OFAT_BASELINE;
        FatTreeSwitch::reset_sglb_route_diag();
        for (uint32_t i = 0; i < 64; i++)
            sw.sglb_route(&routes, 81);
        expect(FatTreeSwitch::_sglb_diag_candidate_choices == 64 * 4,
               "sglb-old should retain whole-level fill and random choice");

        q1.set_queuesize(0);
        q2.set_queuesize(0);
        q3.set_queuesize(0);
        FatTreeSwitch::reset_sglb_route_diag();
        for (uint32_t i = 0; i < 64; i++)
            sw.sglb_route(&routes, 80);
        expect(FatTreeSwitch::_sglb_diag_candidate_choices == 64 * 4,
               "quantized top-k should not truncate a tied best level");

        FatTreeSwitch::_sglb_score_mode = FatTreeSwitch::SGLB_SCORE_LEGACY;
    }

    {
        FatTreeSwitch sw(eventlist, "sglb_cache_sw", FatTreeSwitch::AGG, 1, 0, NULL);
        TestQueue q0(speedFromMbps((uint64_t)10000), 65536, eventlist);
        TestQueue q1(speedFromMbps((uint64_t)10000), 65536, eventlist);
        q1.set_queuesize(4000);

        Route* r0 = make_route(&q0);
        Route* r1 = make_route(&q1);
        FibEntry e0(r0, 1, UP);
        FibEntry e1(r1, 1, UP);
        vector<FibEntry*> routes;
        routes.push_back(&e0);
        routes.push_back(&e1);

        FatTreeSwitch::_sglb_update_interval = timeFromUs(1.0);
        FatTreeSwitch::_sglb_queue_weight = 1.0;
        FatTreeSwitch::_sglb_util_weight = 0.0;
        FatTreeSwitch::_sglb_downstream_weight = 0.0;
        FatTreeSwitch::_sglb_quality_bucket = 1.0;
        FatTreeSwitch::_sglb_quality_levels = 64;
        FatTreeSwitch::_sglb_max_quality = 63;
        FatTreeSwitch::_sglb_min_choices = 1;

        FatTreeSwitch::reset_sglb_route_diag();
        expect(sw.sglb_route(&routes, 42) == 0,
               "SGLB should initially prefer the empty first path");
        expect(FatTreeSwitch::_sglb_diag_route_calls == 1,
               "SGLB diagnostics should count route decisions");
        expect(FatTreeSwitch::_sglb_diag_candidate_choices == 1,
               "SGLB diagnostics should count the selected candidate set");
        expect(FatTreeSwitch::_sglb_diag_all_same_quality_calls == 0,
               "SGLB diagnostics should distinguish unequal path qualities");

        q0.set_queuesize(16000);
        expect(sw.sglb_route(&routes, 42) == 0,
               "SGLB should keep cached path quality within the local update interval");

        NoopTimer timer(eventlist);
        eventlist.sourceIsPendingRel(timer, timeFromUs(2.0));
        eventlist.doNextEvent();

        expect(sw.sglb_route(&routes, 42) == 1,
               "SGLB should refresh local path quality after the local update interval");
    }

    expect(FatTreeSwitch::sglb_quality_from_score(0.0, 10.0, 8) == 0,
           "SGLB quality should keep an empty path in bucket 0");
    expect(FatTreeSwitch::sglb_quality_from_score(75.0, 10.0, 8) == 7,
           "SGLB quality should saturate at the last configured bucket");
    expect(FatTreeSwitch::sglb_downstream_scale_for_score(0.0, 20.0, false) == 1.0,
           "SGLB should use full downstream weight when the local port is empty");
    expect(FatTreeSwitch::sglb_downstream_scale_for_score(20.0, 20.0, false) < 1.0,
           "SGLB should damp downstream weight when local pressure is visible");
    expect(FatTreeSwitch::sglb_downstream_scale_for_score(40.0, 20.0, false) <
           FatTreeSwitch::sglb_downstream_scale_for_score(20.0, 20.0, false),
           "SGLB downstream damping should grow with local pressure");

    FatTreeSwitch::SglbPathState state;
    state.valid = true;
    state.last_update = 1000;
    expect(FatTreeSwitch::sglb_snapshot_usable(state, 1499, 500),
           "SGLB GCN state should be usable before the aging interval");
    expect(!FatTreeSwitch::sglb_snapshot_usable(state, 1501, 500),
           "SGLB GCN state should age out after the aging interval");

    {
        FatTreeTopology::set_tiers(2);
        FatTreeTopology topo(
            8, speedFromMbps((uint64_t)100000), 100000,
            NULL, &eventlist, NULL, COMPOSITE_ECN_LB,
            timeFromUs(1.0), timeFromUs(1.0));
        FatTreeSwitch* source_leaf =
            dynamic_cast<FatTreeSwitch*>(topo.switches_lp[0]);
        expect(source_leaf != NULL,
               "hybrid integration test needs a source leaf");

        RoceSrc source(NULL, NULL, eventlist,
                       speedFromMbps((uint64_t)100000));
        source.set_flowid(1601);
        source.set_src(0);
        source.set_dst(2);
        RoceSink receiver;
        receiver.set_src(0);
        AckCapture ack_capture;
        Route ack_route;
        ack_route.push_back(&ack_capture);
        DropSink source_route_drop;
        Route unused_source_route;
        unused_source_route.push_back(&source_route_drop);
        source.connect(&unused_source_route, &ack_route, receiver,
                       TRIGGER_START);
        FastCnpCapture fast_capture;
        source_leaf->addHostPort(0, source.flow_id(), &fast_capture);
        dynamic_cast<FatTreeSwitch*>(
            topo.switches_lp[topo.HOST_POD_SWITCH(2)])->addHostPort(
                2, source.flow_id(), &receiver);

        FatTreeSwitch::_strategy = FatTreeSwitch::ECMP;
        FatTreeSwitch::_pathid_only_hash = true;
        FatTreeSwitch::_sglb_score_mode =
            FatTreeSwitch::SGLB_SCORE_NMRC_QUANTIZED_TOPK;
        FatTreeSwitch::_sglb_nmrc_levels = 4;
        FatTreeSwitch::_sglb_min_choices = 3;
        FatTreeSwitch::_sglb_update_interval = 0;
        FatTreeSwitch::_nmrc_reroute_policy =
            FatTreeSwitch::NMRC_REROUTE_ANY_BETTER;
        FatTreeSwitch::_nmrc_network_decision_mode =
            FatTreeSwitch::NMRC_NETWORK_GRADED;
        FatTreeSwitch::_nmrc_hybrid_enabled = false;

        RocePacket* probe = RocePacket::newpkt(
            source._flow, 1, Packet::data_packet_size(), false, false, 2);
        probe->set_src(0);
        probe->set_pathid(0);
        probe->set_mrc_ev(0);
        Route* original_route = source_leaf->getNextHop(*probe, NULL);
        expect(original_route != NULL && original_route->size() > 0,
               "encoded EV must resolve to a source-leaf uplink");
        BaseQueue* original_queue =
            dynamic_cast<BaseQueue*>(original_route->at(0));
        expect(original_queue != NULL,
               "source-leaf route must begin with an egress queue");
        probe->free();

        DropSink background_drop;
        Route background_route;
        background_route.push_back(&background_drop);
        PacketFlow background_flow(NULL);
        for (uint32_t i = 0; i < 15; i++) {
            TcpPacket* background = TcpPacket::newpkt(
                background_flow, background_route, i + 1, 4096);
            original_queue->receivePacket(*background);
        }
        expect(original_queue->queuesize() > 40000,
               "test must place the original EV uplink above the BAD threshold");

        PacketFlow missing_reverse_flow(NULL);
        FatTreeSwitch::_nmrc_hybrid_enabled = false;
        RocePacket* missing_reverse = RocePacket::newpkt(
            missing_reverse_flow, 1, Packet::data_packet_size(),
            false, false, 2);
        missing_reverse->set_src(0);
        missing_reverse->set_pathid(0);
        missing_reverse->set_mrc_ev(0);
        Route* missing_original = source_leaf->getNextHop(
            *missing_reverse, NULL);
        missing_reverse->free();
        missing_reverse = RocePacket::newpkt(
            missing_reverse_flow, 1, Packet::data_packet_size(),
            false, false, 2);
        missing_reverse->set_src(0);
        missing_reverse->set_pathid(0);
        missing_reverse->set_mrc_ev(0);
        FatTreeSwitch::reset_nmrc_hybrid_diag();
        FatTreeSwitch::_nmrc_hybrid_enabled = true;
        FatTreeSwitch::_nmrc_fastcnp_enabled = true;
        Route* missing_selected = source_leaf->getNextHop(
            *missing_reverse, NULL);
        expect(missing_selected == missing_original,
               "FastCNP-on must not reroute when the reverse host route is missing");
        expect(FatTreeSwitch::_nmrc_diag_fastcnp_route_missing == 1,
               "missing reverse FastCNP route must be diagnosed once");
        expect(FatTreeSwitch::_nmrc_diag_reroutes == 0,
               "an unsignalled candidate must not count as an actual reroute");
        missing_reverse->free();

        FatTreeSwitch::reset_nmrc_hybrid_diag();
        FatTreeSwitch::_nmrc_hybrid_enabled = true;
        FatTreeSwitch::_nmrc_fastcnp_enabled = true;
        const uint32_t evs[] = {0, 2};
        for (uint32_t i = 0; i < 2; i++) {
            RocePacket* data = RocePacket::newpkt(
                source._flow,
                1 + i * Packet::data_packet_size(),
                Packet::data_packet_size(), false, false, 2);
            data->set_src(0);
            data->set_pathid(evs[i]);
            data->set_mrc_ev(evs[i]);
            data->set_flags(ECN_CE);
            source_leaf->receivePacket(*data);
        }

        FatTreeSwitch::_nmrc_fastcnp_enabled = false;
        RocePacket* no_signal = RocePacket::newpkt(
            source._flow, 1 + 2 * Packet::data_packet_size(),
            Packet::data_packet_size(), false, false, 2);
        no_signal->set_src(0);
        no_signal->set_pathid(4);
        no_signal->set_mrc_ev(4);
        no_signal->set_flags(ECN_CE);
        source_leaf->receivePacket(*no_signal);

        simtime_picosec deadline = EventList::now() + timeFromUs(50.0);
        while ((fast_capture.evs.size() < 2 || ack_capture.ecn_echoes < 3) &&
               EventList::now() < deadline && eventlist.doNextEvent()) {}

        expect(FatTreeSwitch::_nmrc_diag_route_checks == 3,
               "only source-leaf data ingress may run the hybrid route check");
        expect(FatTreeSwitch::_nmrc_diag_reroutes == 3,
               "each BAD-to-GOOD test packet should actually reroute");
        expect(FatTreeSwitch::_nmrc_diag_fastcnp_generated == 2,
               "FastCNP-off must preserve rerouting without generating control traffic");
        expect(fast_capture.evs.size() == 2,
               "two different rerouted EVs must produce two real control packets");
        expect(std::set<uint32_t>(fast_capture.evs.begin(),
                                  fast_capture.evs.end()).size() == 2,
               "physical aliases must still notify each logical EV separately");
        expect(fast_capture.latencies[0] > 0 &&
                   fast_capture.latencies[1] > 0,
               "FastCNP must incur switch, queue, and link delivery latency");
        expect(!std::isfinite(fast_capture.original_scores[0]) &&
                   !fast_capture.cooldown_requests[0],
               "graded FastCNP must suppress cooldown without valid two-hop scores");
        expect(FatTreeSwitch::_nmrc_diag_graded_cooldown_requested == 0 &&
                   FatTreeSwitch::_nmrc_diag_graded_cooldown_suppressed == 2,
               "invalid-score graded reroutes must be reported without cooldown");
        expect(ack_capture.ecn_echoes == 3,
               "hybrid rerouting must not clear CE or suppress receiver ECN echo");

        FatTreeSwitch::_nmrc_network_decision_mode =
            FatTreeSwitch::NMRC_NETWORK_BINARY_SCORE;
        FatTreeSwitch::_nmrc_reroute_policy =
            FatTreeSwitch::NMRC_REROUTE_BETTER_GE3;
        FatTreeSwitch::_sglb_update_interval = 0;
        FatTreeSwitch::_sglb_gcn_aging_interval = timeFromUs(5.0);
        // This hybrid-routing fixture needs an immediately refreshed remote
        // snapshot; it must not inherit the production SGLB 15us cadence.
        FatTreeSwitch::_sglb_gcn_update_interval = 0;
        FatTreeSwitch::_nmrc_hybrid_enabled = false;
        FatTreeSwitch::_nmrc_fastcnp_enabled = true;

        std::set<Route*> source_routes;
        for (uint32_t ev = 0; ev < 16; ev++) {
            RocePacket* warm_probe = RocePacket::newpkt(
                source._flow, 1, Packet::data_packet_size(),
                false, false, 2);
            warm_probe->set_src(0);
            warm_probe->set_pathid(ev);
            warm_probe->set_mrc_ev(ev);
            Route* source_route = source_leaf->getNextHop(*warm_probe, NULL);
            expect(source_route != NULL && source_route->size() > 2,
                   "binary telemetry warmup needs a two-hop source route");
            source_routes.insert(source_route);
            FatTreeSwitch* downstream =
                dynamic_cast<FatTreeSwitch*>(source_route->at(2));
            expect(downstream != NULL,
                   "binary telemetry warmup must reach the downstream switch");
            FatTreeSwitch::_nmrc_hybrid_enabled = true;
            expect(downstream->getNextHop(*warm_probe, NULL) != NULL,
                   "binary telemetry warmup must resolve the downstream egress");
            FatTreeSwitch::_nmrc_hybrid_enabled = false;
            warm_probe->free();
        }
        expect(source_routes.size() > 1,
               "binary paired-action integration needs multiple physical egresses");

        probe = RocePacket::newpkt(
            source._flow, 1, Packet::data_packet_size(), false, false, 2);
        probe->set_src(0);
        probe->set_pathid(0);
        probe->set_mrc_ev(0);
        original_route = source_leaf->getNextHop(*probe, NULL);
        original_queue =
            dynamic_cast<BaseQueue*>(original_route->at(0));
        expect(original_queue != NULL,
               "binary paired-action original route must start with a queue");
        probe->free();

        for (uint32_t i = 0; i < 15; i++) {
            TcpPacket* background = TcpPacket::newpkt(
                background_flow, background_route, 100 + i, 4096);
            original_queue->receivePacket(*background);
        }

        missing_reverse = RocePacket::newpkt(
            missing_reverse_flow, 1, Packet::data_packet_size(),
            false, false, 2);
        missing_reverse->set_src(0);
        missing_reverse->set_pathid(0);
        missing_reverse->set_mrc_ev(0);
        missing_reverse->set_nmrc_detour(true);
        missing_reverse->set_nmrc_actual_egress(31);
        FatTreeSwitch::reset_nmrc_hybrid_diag();
        FatTreeSwitch::_nmrc_hybrid_enabled = true;
        missing_selected = source_leaf->getNextHop(
            *missing_reverse, NULL);
        expect(missing_selected == original_route,
               "binary reroute must not commit without a reverse control route");
        expect(missing_reverse->nmrc_detour() &&
                   missing_reverse->nmrc_actual_egress() == 31,
               "missing reverse control route must leave packet detour metadata unchanged");
        expect(FatTreeSwitch::_nmrc_diag_binary_route_missing == 1 &&
                   FatTreeSwitch::_nmrc_diag_reroutes == 0 &&
                   FatTreeSwitch::_nmrc_diag_binary_paired_actions == 0 &&
                   FatTreeSwitch::_nmrc_diag_fastcnp_generated == 0,
               "missing reverse control route must leave all committed counters unchanged");
        missing_reverse->free();

        RocePacket* binary_data = RocePacket::newpkt(
            source._flow, 1, Packet::data_packet_size(), false, false, 2);
        binary_data->set_src(0);
        binary_data->set_pathid(0);
        binary_data->set_mrc_ev(0);
        FatTreeSwitch::reset_nmrc_hybrid_diag();
        Route* selected_route = source_leaf->getNextHop(*binary_data, NULL);
        FatTreeSwitch::_sglb_update_interval = timeFromUs(20.0);
        expect(selected_route != original_route && binary_data->nmrc_detour() &&
                   binary_data->has_nmrc_actual_egress(),
               "successful binary action must set actual egress and detour metadata");
        uint32_t selected_egress = binary_data->nmrc_actual_egress();
        expect(FatTreeSwitch::_nmrc_diag_reroutes == 1 &&
                   FatTreeSwitch::_nmrc_diag_binary_paired_actions == 1 &&
                   FatTreeSwitch::_nmrc_diag_fastcnp_generated == 1,
               "binary committed reroutes, paired actions, and generated FastCNP must remain equal");
        expect(FatTreeSwitch::_nmrc_diag_level_transitions[1][0] == 1,
               "successful binary action must record transition 1 to 0");
        expect(FatTreeSwitch::_nmrc_diag_binary_actual_egress[selected_egress] == 1,
               "successful binary action must update the committed actual-egress histogram");
        expect(FatTreeSwitch::_nmrc_diag_binary_original_score_sum >= 0.5 &&
                   FatTreeSwitch::_nmrc_diag_binary_original_score_max >= 0.5 &&
                   FatTreeSwitch::_nmrc_diag_binary_selected_score_sum < 0.5 &&
                   FatTreeSwitch::_nmrc_diag_binary_selected_score_max < 0.5,
               "successful binary action must expose original and selected score diagnostics");

        size_t capture_index = fast_capture.evs.size();
        deadline = EventList::now() + timeFromUs(50.0);
        while (fast_capture.evs.size() == capture_index &&
               EventList::now() < deadline &&
               eventlist.doNextEvent()) {}
        expect(fast_capture.evs.size() == capture_index + 1 &&
                   fast_capture.original_egresses[capture_index] == 0 &&
                   fast_capture.selected_egresses[capture_index] == selected_egress &&
                   fast_capture.original_levels[capture_index] == 1 &&
                   fast_capture.selected_levels[capture_index] == 0,
               "paired FastCNP must match original and selected egress with transition 1 to 0");
        binary_data->free();

        FatTreeSwitch::_sglb_update_interval = 0;
        FatTreeSwitch::_nmrc_hybrid_enabled = false;
        for (uint32_t ev = 0; ev < 16; ev++) {
            RocePacket* warm_probe = RocePacket::newpkt(
                source._flow, 20000, Packet::data_packet_size(),
                false, false, 2);
            warm_probe->set_src(0);
            warm_probe->set_pathid(ev);
            warm_probe->set_mrc_ev(ev);
            Route* source_route = source_leaf->getNextHop(*warm_probe, NULL);
            FatTreeSwitch* downstream =
                dynamic_cast<FatTreeSwitch*>(source_route->at(2));
            expect(downstream != NULL,
                   "relative telemetry refresh must reach the downstream switch");
            FatTreeSwitch::_nmrc_hybrid_enabled = true;
            expect(downstream->getNextHop(*warm_probe, NULL) != NULL,
                   "relative telemetry refresh must resolve downstream egress");
            FatTreeSwitch::_nmrc_hybrid_enabled = false;
            warm_probe->free();
        }
        FatTreeSwitch::_nmrc_hybrid_enabled = true;
        for (uint32_t i = 0; i < 5; i++) {
            TcpPacket* background = TcpPacket::newpkt(
                background_flow, background_route, 180 + i, 4096);
            original_queue->receivePacket(*background);
        }

        FatTreeSwitch::_nmrc_network_decision_mode =
            FatTreeSwitch::NMRC_NETWORK_FIXED_THRESHOLD;
        FatTreeSwitch::_nmrc_absolute_threshold = 0.50;
        FatTreeSwitch::_nmrc_relative_delta = 0.30;
        RocePacket* relative_data = RocePacket::newpkt(
            source._flow, 20001, Packet::data_packet_size(),
            false, false, 2);
        relative_data->set_src(0);
        relative_data->set_pathid(0);
        relative_data->set_mrc_ev(0);
        relative_data->set_attempt_id(3);
        relative_data->set_flags(ECN_CE);
        FatTreeSwitch::reset_nmrc_hybrid_diag();
        capture_index = fast_capture.evs.size();
        Route* relative_selected = source_leaf->getNextHop(
            *relative_data, NULL);
        expect(relative_selected != original_route &&
                   relative_data->nmrc_detour() &&
                   relative_data->has_nmrc_actual_egress(),
               "relative delta must reroute to a sufficiently better path");
        uint32_t relative_egress = relative_data->nmrc_actual_egress();
        expect(FatTreeSwitch::_nmrc_diag_relative_paired_actions == 1 &&
                   FatTreeSwitch::_nmrc_diag_relative_reroutes == 1 &&
                   FatTreeSwitch::_nmrc_diag_fastcnp_generated == 1,
               "relative reroute and FastCNP must commit atomically");
        expect(FatTreeSwitch::_nmrc_diag_relative_reroute_key_count == 1 &&
                   FatTreeSwitch::_nmrc_diag_relative_generated_key_count == 1 &&
                   FatTreeSwitch::_nmrc_diag_relative_reroute_key_sum ==
                       FatTreeSwitch::_nmrc_diag_relative_generated_key_sum &&
                   FatTreeSwitch::_nmrc_diag_relative_reroute_key_xor ==
                       FatTreeSwitch::_nmrc_diag_relative_generated_key_xor,
               "paired action tuple checksums must match");
        expect(FatTreeSwitch::_nmrc_diag_relative_decision_ce_set == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_decision_ce_cleared == 0 &&
                   (relative_data->flags() & ECN_CE),
               "relative decision must not mutate CE");

        deadline = EventList::now() + timeFromUs(50.0);
        while (fast_capture.evs.size() == capture_index &&
               EventList::now() < deadline && eventlist.doNextEvent()) {}
        expect(fast_capture.evs.size() == capture_index + 1 &&
                   fast_capture.evs[capture_index] == 0 &&
                   fast_capture.psns[capture_index] == 20001 &&
                   fast_capture.attempts[capture_index] == 3 &&
                   fast_capture.original_egresses[capture_index] == 0 &&
                   fast_capture.selected_egresses[capture_index] == relative_egress &&
                   fast_capture.original_levels[capture_index] == UINT8_MAX &&
                   fast_capture.selected_levels[capture_index] == UINT8_MAX &&
                   fast_capture.original_scores[capture_index] -
                       fast_capture.selected_scores[capture_index] >= 0.30 - 1e-12 &&
                   fast_capture.selected_gaps[capture_index] >= 0.30 - 1e-12 &&
                   fast_capture.action_keys[capture_index] != 0 &&
                   fast_capture.action_keys[capture_index] ==
                       FatTreeSwitch::_nmrc_diag_relative_reroute_key_sum &&
                   fast_capture.action_keys[capture_index] ==
                       FatTreeSwitch::_nmrc_diag_relative_generated_key_sum,
               "relative FastCNP must carry its six-field identity and scores");
        relative_data->free();

        for (uint32_t i = 0; i < 5; i++) {
            TcpPacket* background = TcpPacket::newpkt(
                background_flow, background_route, 190 + i, 4096);
            original_queue->receivePacket(*background);
        }

        RocePacket* relative_blocked = RocePacket::newpkt(
            missing_reverse_flow, 20002, Packet::data_packet_size(),
            false, false, 2);
        relative_blocked->set_src(0);
        relative_blocked->set_pathid(0);
        relative_blocked->set_mrc_ev(0);
        relative_blocked->set_attempt_id(4);
        relative_blocked->set_nmrc_detour(true);
        relative_blocked->set_nmrc_actual_egress(31);
        FatTreeSwitch::reset_nmrc_hybrid_diag();
        size_t captures_before_block = fast_capture.evs.size();
        Route* relative_blocked_selected = source_leaf->getNextHop(
            *relative_blocked, NULL);
        expect(relative_blocked_selected == original_route &&
                   relative_blocked->nmrc_detour() &&
                   relative_blocked->nmrc_actual_egress() == 31,
               "relative action must not commit without a reverse control route");
        expect(FatTreeSwitch::_nmrc_diag_relative_reverse_path_blocked == 1 &&
                   FatTreeSwitch::_nmrc_diag_relative_reroutes == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_paired_actions == 0 &&
                   FatTreeSwitch::_nmrc_diag_fastcnp_generated == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_original_score_count == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_selected_score_count == 0 &&
                   fast_capture.evs.size() == captures_before_block,
               "blocked relative action must commit neither reroute nor FastCNP");
        relative_blocked->free();

        FatTreeSwitch::_nmrc_network_decision_mode =
            FatTreeSwitch::NMRC_NETWORK_BINARY_SCORE;

        NoopTimer downstream_ttl_timer(eventlist);
        simtime_picosec downstream_ttl_deadline =
            EventList::now() + timeFromUs(6.0);
        eventlist.sourceIsPendingRel(downstream_ttl_timer, timeFromUs(6.0));
        while (EventList::now() < downstream_ttl_deadline &&
               eventlist.doNextEvent()) {}
        RocePacket* stale_downstream = RocePacket::newpkt(
            source._flow, 2, Packet::data_packet_size(), false, false, 2);
        stale_downstream->set_src(0);
        stale_downstream->set_pathid(0);
        stale_downstream->set_mrc_ev(0);
        FatTreeSwitch::reset_nmrc_hybrid_diag();
        Route* stale_selected = source_leaf->getNextHop(
            *stale_downstream, NULL);
        expect(stale_selected == original_route &&
                   !stale_downstream->nmrc_detour() &&
                   !stale_downstream->has_nmrc_actual_egress() &&
                   FatTreeSwitch::_nmrc_diag_reroutes == 0 &&
                   FatTreeSwitch::_nmrc_diag_fastcnp_generated == 0,
               "a cached path must become UNKNOWN when downstream telemetry expires inside the local cache interval");
        stale_downstream->free();
        FatTreeSwitch::_sglb_update_interval = 0;
        FatTreeSwitch::_sglb_gcn_aging_interval = timeFromUs(30.0);

        for (uint32_t i = 0; i < 15; i++) {
            TcpPacket* background = TcpPacket::newpkt(
                background_flow, background_route, 150 + i, 4096);
            original_queue->receivePacket(*background);
        }
        FatTreeSwitch::_sglb_score_mode =
            FatTreeSwitch::SGLB_SCORE_NMRC_QUANTIZED_TOPK;
        FatTreeSwitch::_nmrc_fastcnp_enabled = false;
        RocePacket* provenance_prime = RocePacket::newpkt(
            source._flow, 3, Packet::data_packet_size(), false, false, 2);
        provenance_prime->set_src(0);
        provenance_prime->set_pathid(0);
        provenance_prime->set_mrc_ev(0);
        source_leaf->getNextHop(*provenance_prime, NULL);
        provenance_prime->free();

        FatTreeSwitch::_sglb_score_mode = FatTreeSwitch::SGLB_SCORE_LEGACY;
        FatTreeSwitch::_nmrc_fastcnp_enabled = true;
        RocePacket* legacy_provenance = RocePacket::newpkt(
            source._flow, 4, Packet::data_packet_size(), false, false, 2);
        legacy_provenance->set_src(0);
        legacy_provenance->set_pathid(0);
        legacy_provenance->set_mrc_ev(0);
        FatTreeSwitch::reset_nmrc_hybrid_diag();
        Route* legacy_provenance_selected = source_leaf->getNextHop(
            *legacy_provenance, NULL);
        expect(legacy_provenance_selected == original_route &&
                   !legacy_provenance->nmrc_detour() &&
                   !legacy_provenance->has_nmrc_actual_egress() &&
                   FatTreeSwitch::_nmrc_diag_reroutes == 0 &&
                   FatTreeSwitch::_nmrc_diag_fastcnp_generated == 0,
               "a legacy additive recomputation must clear N-MRC completeness provenance");
        legacy_provenance->free();
        FatTreeSwitch::_sglb_score_mode =
            FatTreeSwitch::SGLB_SCORE_NMRC_QUANTIZED_TOPK;

        uint32_t route_number = 0;
        for (std::set<Route*>::iterator route = source_routes.begin();
             route != source_routes.end(); ++route, ++route_number) {
            BaseQueue* queue = dynamic_cast<BaseQueue*>((*route)->at(0));
            expect(queue != NULL,
                   "binary no-safe integration needs queue-backed egresses");
            for (uint32_t i = 0; i < 15; i++) {
                TcpPacket* background = TcpPacket::newpkt(
                    background_flow, background_route,
                    200 + route_number * 20 + i, 4096);
                queue->receivePacket(*background);
            }
        }
        RocePacket* no_safe = RocePacket::newpkt(
            source._flow, 2, Packet::data_packet_size(), false, false, 2);
        no_safe->set_src(0);
        no_safe->set_pathid(0);
        no_safe->set_mrc_ev(0);
        FatTreeSwitch::reset_nmrc_hybrid_diag();
        Route* no_safe_selected = source_leaf->getNextHop(*no_safe, NULL);
        expect(no_safe_selected == original_route &&
                   FatTreeSwitch::_nmrc_diag_binary_no_safe == 1 &&
                   FatTreeSwitch::_nmrc_diag_reroutes == 0 &&
                   FatTreeSwitch::_nmrc_diag_binary_paired_actions == 0 &&
                   FatTreeSwitch::_nmrc_diag_fastcnp_generated == 0,
               "no SAFE binary candidate must commit neither reroute nor FastCNP");
        no_safe->free();
    }

    {
        FatTreeSwitch::_nmrc_diag_binary_original_safe = 9;
        FatTreeSwitch::_nmrc_diag_binary_original_congested = 9;
        FatTreeSwitch::_nmrc_diag_binary_no_safe = 9;
        FatTreeSwitch::_nmrc_diag_binary_route_missing = 9;
        FatTreeSwitch::_nmrc_diag_binary_paired_actions = 9;
        FatTreeSwitch::_nmrc_diag_binary_original_score_sum = 9.0;
        FatTreeSwitch::_nmrc_diag_binary_original_score_max = 9.0;
        FatTreeSwitch::_nmrc_diag_binary_selected_score_sum = 9.0;
        FatTreeSwitch::_nmrc_diag_binary_selected_score_max = 9.0;
        FatTreeSwitch::_nmrc_diag_binary_actual_egress[0] = 9;
        FatTreeSwitch::_nmrc_diag_binary_actual_egress[32] = 9;
        FatTreeSwitch::_nmrc_diag_relative_checks = 9;
        FatTreeSwitch::_nmrc_diag_relative_original_unknown = 9;
        FatTreeSwitch::_nmrc_diag_relative_original_unavailable = 9;
        FatTreeSwitch::_nmrc_diag_relative_original_below_absolute = 9;
        FatTreeSwitch::_nmrc_diag_relative_no_safe_candidate = 9;
        FatTreeSwitch::_nmrc_diag_relative_no_delta_candidate = 9;
        FatTreeSwitch::_nmrc_diag_relative_reverse_path_blocked = 9;
        FatTreeSwitch::_nmrc_diag_relative_paired_actions = 9;
        FatTreeSwitch::_nmrc_diag_relative_reroutes = 9;
        FatTreeSwitch::_nmrc_diag_relative_selected_gap_violations = 9;
        FatTreeSwitch::_nmrc_diag_relative_decision_ce_set = 9;
        FatTreeSwitch::_nmrc_diag_relative_decision_ce_cleared = 9;
        FatTreeSwitch::_nmrc_diag_relative_reroute_key_count = 9;
        FatTreeSwitch::_nmrc_diag_relative_reroute_key_sum = 9;
        FatTreeSwitch::_nmrc_diag_relative_reroute_key_xor = 9;
        FatTreeSwitch::_nmrc_diag_relative_generated_key_count = 9;
        FatTreeSwitch::_nmrc_diag_relative_generated_key_sum = 9;
        FatTreeSwitch::_nmrc_diag_relative_generated_key_xor = 9;
        FatTreeSwitch::_nmrc_diag_relative_original_score_count = 9;
        FatTreeSwitch::_nmrc_diag_relative_original_score_sum = 9.0;
        FatTreeSwitch::_nmrc_diag_relative_original_score_max = 9.0;
        FatTreeSwitch::_nmrc_diag_relative_selected_score_count = 9;
        FatTreeSwitch::_nmrc_diag_relative_selected_score_sum = 9.0;
        FatTreeSwitch::_nmrc_diag_relative_selected_score_max = 9.0;
        FatTreeSwitch::_nmrc_diag_relative_candidate_count[0] = 9;
        FatTreeSwitch::_nmrc_diag_relative_candidate_count[32] = 9;
        FatTreeSwitch::_nmrc_diag_relative_best_gap[0] = 9;
        FatTreeSwitch::_nmrc_diag_relative_best_gap[20] = 9;
        FatTreeSwitch::_nmrc_diag_relative_selected_gap[0] = 9;
        FatTreeSwitch::_nmrc_diag_relative_selected_gap[20] = 9;
        FatTreeSwitch::_nmrc_diag_relative_original_score[0] = 9;
        FatTreeSwitch::_nmrc_diag_relative_original_score[20] = 9;
        FatTreeSwitch::_nmrc_diag_relative_selected_score[0] = 9;
        FatTreeSwitch::_nmrc_diag_relative_selected_score[20] = 9;
        FatTreeSwitch::_nmrc_diag_relative_actual_egress[0] = 9;
        FatTreeSwitch::_nmrc_diag_relative_actual_egress[32] = 9;
        FatTreeSwitch::reset_nmrc_hybrid_diag();
        expect(FatTreeSwitch::_nmrc_diag_binary_original_safe == 0 &&
                   FatTreeSwitch::_nmrc_diag_binary_original_congested == 0 &&
                   FatTreeSwitch::_nmrc_diag_binary_no_safe == 0 &&
                   FatTreeSwitch::_nmrc_diag_binary_route_missing == 0 &&
                   FatTreeSwitch::_nmrc_diag_binary_paired_actions == 0 &&
                   FatTreeSwitch::_nmrc_diag_binary_original_score_sum == 0.0 &&
                   FatTreeSwitch::_nmrc_diag_binary_original_score_max == 0.0 &&
                   FatTreeSwitch::_nmrc_diag_binary_selected_score_sum == 0.0 &&
                   FatTreeSwitch::_nmrc_diag_binary_selected_score_max == 0.0 &&
                   FatTreeSwitch::_nmrc_diag_binary_actual_egress[0] == 0 &&
                   FatTreeSwitch::_nmrc_diag_binary_actual_egress[32] == 0,
               "reset must clear every binary n-MRC diagnostic");
        expect(FatTreeSwitch::_nmrc_diag_relative_checks == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_original_unknown == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_original_unavailable == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_original_below_absolute == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_no_safe_candidate == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_no_delta_candidate == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_reverse_path_blocked == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_paired_actions == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_reroutes == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_selected_gap_violations == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_decision_ce_set == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_decision_ce_cleared == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_reroute_key_count == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_reroute_key_sum == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_reroute_key_xor == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_generated_key_count == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_generated_key_sum == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_generated_key_xor == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_original_score_count == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_original_score_sum == 0.0 &&
                   FatTreeSwitch::_nmrc_diag_relative_original_score_max == 0.0 &&
                   FatTreeSwitch::_nmrc_diag_relative_selected_score_count == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_selected_score_sum == 0.0 &&
                   FatTreeSwitch::_nmrc_diag_relative_selected_score_max == 0.0 &&
                   FatTreeSwitch::_nmrc_diag_relative_candidate_count[0] == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_candidate_count[32] == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_best_gap[0] == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_best_gap[20] == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_selected_gap[0] == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_selected_gap[20] == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_original_score[0] == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_original_score[20] == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_selected_score[0] == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_selected_score[20] == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_actual_egress[0] == 0 &&
                   FatTreeSwitch::_nmrc_diag_relative_actual_egress[32] == 0,
               "reset must clear every relative-delta n-MRC diagnostic");
    }
    return 0;
}
