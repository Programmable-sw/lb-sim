// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#include "config.h"

#include <array>
#include <cstdlib>
#include <iostream>
#include <list>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#define private public
#include "roce.h"
#undef private

#include "eventlist.h"
#include "network.h"
#include "rocepacket.h"

using namespace std;

static const uint32_t kMss = 1000;

static EventList& test_eventlist() {
    static EventList eventlist;
    return eventlist;
}

static void expect(bool condition, const char* message);

class DropSink : public PacketSink {
public:
    DropSink() : _name("drop_sink") {}
    void receivePacket(Packet& pkt) { pkt.free(); }
    const string& nodename() { return _name; }
private:
    string _name;
};

class BoundedControlCapture : public PacketSink {
public:
    BoundedControlCapture() : _name("bounded_control_capture") {}
    void receivePacket(Packet& pkt) {
        if (pkt.type() == ROCEACK) {
            RoceAck& ack = (RoceAck&)pkt;
            ack_delivered.push_back(
                ack.has_delivered_psn() ? ack.delivered_psn() : 0);
        } else if (pkt.type() == ROCENACK) {
            RoceNack& nack = (RoceNack&)pkt;
            nack_psns.push_back(nack.has_missing_psn() ? nack.missing_psn() : 0);
            nack_attempts.push_back(
                nack.has_attempt_id() ? (int)nack.attempt_id() : -1);
        } else {
            expect(false, "unexpected packet in bounded control capture");
        }
        pkt.free();
    }
    const string& nodename() { return _name; }

    string _name;
    vector<uint64_t> ack_delivered;
    vector<uint64_t> nack_psns;
    vector<int> nack_attempts;
};

class BoundedDataCapture : public PacketSink {
public:
    BoundedDataCapture() : _name("bounded_data_capture") {}
    void receivePacket(Packet& pkt) {
        expect(pkt.type() == ROCE, "expected bounded data packet");
        RocePacket& data = (RocePacket&)pkt;
        psns.push_back(data.seqno());
        attempts.push_back(data.attempt_id());
        retransmitted.push_back(data.retransmitted());
        pkt.free();
    }
    const string& nodename() { return _name; }

    string _name;
    vector<uint64_t> psns;
    vector<uint8_t> attempts;
    vector<bool> retransmitted;
};

static void expect(bool condition, const char* message) {
    if (!condition) {
        cerr << message << endl;
        exit(1);
    }
}

static void configure_bounded_transport() {
    static bool packet_size_configured = false;
    if (!packet_size_configured) {
        Packet::set_packet_size(kMss);
        packet_size_configured = true;
    }
    RoceSrc::setReceiveMode(RoceSrc::RX_SP_RETX_QUEUE);
    RoceSrc::setCongestionControl(RoceSrc::CC_DCTCP_VARIANT);
    RoceSrc::setTransportSemantics(
        RoceSrc::TRANSPORT_MRC_EXACT_BOUNDED);
}

static RoceSrc make_bounded_src(EventList& eventlist, uint32_t packet_count,
                                double cwnd = 10.0);

static void test_duplicate_ack_releases_unique_psn_once() {
    EventList& eventlist = test_eventlist();
    configure_bounded_transport();

    DropSink drop;
    Route route;
    route.push_back(&drop);

    RoceSrc src(NULL, NULL, eventlist, speedFromMbps((uint64_t)100000));
    src._flow_started = true;
    src._state_send = RoceSrc::PAUSED;
    src._highest_sent = kMss;
    src._last_acked = 0;
    src._cc_cwnd_pkts = 10.0;
    src.bounded_note_new_send(1);

    RoceAck* first = RoceAck::newpkt(src._flow, route, kMss, 7);
    first->set_delivered_psn(1);
    src.processAck(*first);
    first->free();

    expect(src._bounded_inflight_pkts == 0,
           "first confirmation must release the unique PSN from inflight");
    expect(src._bounded_unique_acks == 1,
           "first confirmation must produce one unique ACK credit");

    RoceAck* duplicate = RoceAck::newpkt(src._flow, route, kMss, 7);
    duplicate->set_delivered_psn(1);
    duplicate->set_old_duplicate_ack();
    src.processAck(*duplicate);
    duplicate->free();

    expect(src._bounded_inflight_pkts == 0,
           "duplicate ACK must not decrement inflight below zero");
    expect(src._bounded_unique_acks == 1,
           "duplicate ACK must not release a second packet-clock credit");
}

static void test_cumulative_ack_clocks_each_unique_psn() {
    EventList& eventlist = test_eventlist();
    configure_bounded_transport();
    DropSink drop;
    Route route;
    route.push_back(&drop);
    RoceSrc src = make_bounded_src(eventlist, 3, 10.0);

    RoceAck* cumulative = RoceAck::newpkt(src._flow, route, 3 * kMss, 7);
    src.processAck(*cumulative);
    cumulative->free();

    expect(src._bounded_inflight_pkts == 0 && src._bounded_unique_acks == 3,
           "cumulative ACK must confirm three unique PSNs");
    expect(src._cc_cwnd_pkts > 10.299 && src._cc_cwnd_pkts < 10.301,
           "clean cumulative ACK must apply one additive step per unique PSN");
}

static RoceSrc make_bounded_src(EventList& eventlist, uint32_t packet_count,
                                double cwnd) {
    RoceSrc src(NULL, NULL, eventlist, speedFromMbps((uint64_t)100000));
    src._flow_started = true;
    src._state_send = RoceSrc::PAUSED;
    src._highest_sent = packet_count * kMss;
    src._last_acked = 0;
    src._flow_size = packet_count * kMss;
    src._cc_cwnd_pkts = cwnd;
    for (uint32_t i = 0; i < packet_count; i++)
        src.bounded_note_new_send(1 + (uint64_t)i * kMss);
    return src;
}

static RoceNack* exact_trim_nack(RoceSrc& src, Route& route,
                                 uint64_t cack, uint64_t psn,
                                 uint8_t attempt_id,
                                 uint64_t sack_bitmap = 0,
                                 uint16_t sack_valid = 0) {
    RoceNack* nack = RoceNack::newpkt(
        src._flow, route, cack, 7, sack_bitmap, 0, sack_valid != 0,
        0, cack + 1, sack_valid, 64);
    nack->set_reason(RoceNack::TRIM);
    nack->set_missing_psn(psn);
    nack->set_attempt_id(attempt_id);
    return nack;
}

static void test_sack_confirmation_precedes_exact_failure() {
    EventList& eventlist = test_eventlist();
    configure_bounded_transport();
    DropSink drop;
    Route route;
    route.push_back(&drop);
    RoceSrc src = make_bounded_src(eventlist, 1);

    RoceNack* nack = exact_trim_nack(src, route, 0, 1, 0, 1, 1);
    src.processNack(*nack);
    nack->free();

    expect(src._bounded_packets[1].state == RoceSrc::BOUNDED_ACKED,
           "SACK confirmation must win over exact failure in the same NACK");
    expect(src._rtx_queue.empty(),
           "a SACK-confirmed PSN must not be revived by exact failure");
    expect(src._bounded_inflight_pkts == 0,
           "positive SACK processing must release inflight exactly once");
}

static void test_duplicate_nack_queues_once() {
    EventList& eventlist = test_eventlist();
    configure_bounded_transport();
    DropSink drop;
    Route route;
    route.push_back(&drop);
    RoceSrc src = make_bounded_src(eventlist, 1);

    RoceNack* first = exact_trim_nack(src, route, 0, 1, 0);
    src.processNack(*first);
    first->free();
    RoceNack* duplicate = exact_trim_nack(src, route, 0, 1, 0);
    src.processNack(*duplicate);
    duplicate->free();

    expect(src._rtx_queue.size() == 1,
           "duplicate exact NACK must leave one retransmission item");
    expect(src._bounded_packets[1].state == RoceSrc::BOUNDED_RTX_PENDING,
           "valid exact Trim must leave the PSN pending once");
    expect(src._bounded_inflight_pkts == 0,
           "duplicate failure must not release inflight twice");
}

static void test_duplicate_ooo_without_progress_does_not_refresh_rto() {
    EventList& eventlist = test_eventlist();
    configure_bounded_transport();
    DropSink drop;
    Route route;
    route.push_back(&drop);
    RoceSrc src = make_bounded_src(eventlist, 1);

    RoceNack* first = RoceNack::newpkt(
        src._flow, route, 0, 7, 0, 0, false, 0, 1, 0, 64);
    first->set_reason(RoceNack::OOO);
    src.processNack(*first);
    first->free();
    expect(src._rtx_queue.size() == 1,
           "first OOO recovery signal must queue the first hole");

    const simtime_picosec sentinel_timeout = timeFromUs((uint32_t)1234);
    src._rtx_timeout = sentinel_timeout;
    RoceNack* duplicate = RoceNack::newpkt(
        src._flow, route, 0, 7, 0, 0, false, 0, 1, 0, 64);
    duplicate->set_reason(RoceNack::OOO);
    src.processNack(*duplicate);
    duplicate->free();

    expect(src._rtx_timeout == sentinel_timeout,
           "duplicate OOO without new ACK/SACK evidence must not postpone RTO");
}

static void test_attempt_matching_and_retrim() {
    EventList& eventlist = test_eventlist();
    configure_bounded_transport();
    DropSink drop;
    Route route;
    route.push_back(&drop);
    RoceSrc src = make_bounded_src(eventlist, 1);

    RoceNack* original_trim = exact_trim_nack(src, route, 0, 1, 0);
    src.processNack(*original_trim);
    original_trim->free();

    uint8_t retry_attempt = 0;
    bool used_reserve = false;
    expect(src.bounded_begin_retransmission(1, retry_attempt, used_reserve),
           "pending exact PSN must be eligible for retransmission");
    expect(retry_attempt == 1,
           "first retransmission must increment the attempt id");

    RoceNack* stale = exact_trim_nack(src, route, 0, 1, 0);
    src.processNack(*stale);
    stale->free();
    expect(src._bounded_packets[1].state == RoceSrc::BOUNDED_RTX_INFLIGHT,
           "old attempt NACK must not fail the current retransmission");
    expect(src._rtx_queue.empty(),
           "old attempt NACK must not requeue the current retransmission");

    RoceNack* current = exact_trim_nack(src, route, 0, 1, 1);
    src.processNack(*current);
    current->free();
    expect(src._bounded_packets[1].state == RoceSrc::BOUNDED_RTX_PENDING,
           "current retransmission Trim must remain recoverable");
    expect(src._rtx_queue.size() == 1,
           "current retransmission Trim must queue exactly one retry");
}

static void test_sack_loss_and_exact_do_not_double_queue() {
    EventList& eventlist = test_eventlist();
    configure_bounded_transport();
    DropSink drop;
    Route route;
    route.push_back(&drop);
    RoceSrc src = make_bounded_src(eventlist, 2);

    RoceNack* ooo = RoceNack::newpkt(
        src._flow, route, 0, 7, 2, 0, true, 0, 1, 2, 64);
    ooo->set_reason(RoceNack::OOO);
    src.processNack(*ooo);
    ooo->free();
    expect(src._rtx_queue.size() == 1 &&
           src._rtx_queue._seqs.count(1) == 1,
           "OOO recovery must queue only the current first hole");

    RoceNack* exact = exact_trim_nack(src, route, 0, 1, 0, 2, 2);
    src.processNack(*exact);
    exact->free();
    expect(src._rtx_queue.size() == 1 &&
           src._rtx_queue._seqs.count(1) == 1,
           "SACK loss and exact Trim must share one PSN queue item");
}

static void test_one_mtu_recovery_reserve_is_bounded() {
    EventList& eventlist = test_eventlist();
    configure_bounded_transport();
    RoceSrc src = make_bounded_src(eventlist, 2, 1.0);
    expect(src.bounded_queue_failure(1, 0),
           "first current attempt failure must queue recovery");

    uint8_t attempt = 0;
    bool used_reserve = false;
    expect(src.bounded_begin_retransmission(1, attempt, used_reserve) && used_reserve,
           "one retry may use the one-MTU deadlock reserve");
    expect(src._bounded_recovery_inflight_bytes == kMss,
           "recovery reserve must account exactly one MTU");
    expect(src.bounded_queue_failure(1001, 0),
           "second current attempt failure must queue recovery");
    expect(!src.bounded_begin_retransmission(1001, attempt, used_reserve),
           "a second retry must not exceed the one-MTU reserve");
    expect(src._bounded_recovery_inflight_bytes <= kMss,
           "recovery reserve must never exceed one MTU");
    expect(!src.has_retransmit_work(),
           "blocked retry must wait for feedback instead of polling");
    expect(!src.congestion_window_allows_send(),
           "the recovery reserve must not grant new-data credit");
    src.bounded_mark_acked(1);
    expect(src.has_retransmit_work(),
           "reserve release must make the pending retry schedulable");
}

static void test_rto_releases_lost_reserve_attempt() {
    EventList& eventlist = test_eventlist();
    configure_bounded_transport();
    RoceSrc src = make_bounded_src(eventlist, 3, 1.0);

    expect(src.bounded_queue_failure(1 + 2 * kMss, 0),
           "higher exact failure must enter recovery");
    uint8_t attempt = 0;
    bool used_reserve = false;
    expect(src.bounded_begin_retransmission(1 + 2 * kMss, attempt,
                                            used_reserve) && used_reserve,
           "higher retry must occupy the one-MTU reserve");
    expect(src.bounded_queue_failure(1, 0),
           "later lower hole must remain pending");

    expect(src.bounded_expire_recovery_reserve(),
           "RTO must expire a lost reserve attempt");
    expect(src._bounded_recovery_inflight_bytes == 0,
           "expired reserve attempt must release its one-MTU credit");
    expect(src._bounded_packets[1 + 2 * kMss].state ==
               RoceSrc::BOUNDED_RTX_PENDING,
           "expired higher attempt must remain pending exactly once");
    expect(src._bounded_packets[1].state == RoceSrc::BOUNDED_RTX_PENDING,
           "lower cumulative hole must remain pending");

    expect(src.bounded_begin_retransmission(1, attempt, used_reserve) &&
               used_reserve,
           "lowest pending PSN must be able to take the released reserve");
    expect(src._bounded_recovery_inflight_bytes == kMss,
           "reserve handoff must still be bounded to one MTU");
}

static void deliver_to_sink(RoceSink& sink, PacketFlow& flow,
                            uint64_t psn, uint8_t attempt_id,
                            bool trimmed = false) {
    RocePacket* pkt = RocePacket::newpkt(flow, psn, kMss, attempt_id != 0,
                                         false, 2);
    pkt->set_src(1);
    pkt->set_pathid(0);
    pkt->set_attempt_id(attempt_id);
    pkt->set_ts(EventList::now());
    if (trimmed)
        pkt->strip_payload();
    sink.receivePacket(*pkt);
}

static void test_receiver_counts_only_first_full_psn_and_echoes_attempt() {
    EventList& eventlist = test_eventlist();
    configure_bounded_transport();
    RoceSrc::setOooTolerance(timeFromUs((uint32_t)100));
    RoceSrc::setOooWindowPkts(64);

    DropSink data_drop;
    Route routeout;
    routeout.push_back(&data_drop);
    BoundedControlCapture control;
    Route routeback;
    routeback.push_back(&control);
    RoceSrc src(NULL, NULL, eventlist, speedFromMbps((uint64_t)100000));
    src.set_src(1);
    src.set_dst(2);
    src._flow_started = true;
    src._state_send = RoceSrc::PAUSED;
    RoceSink sink;
    sink.set_src(1);
    src.connect(&routeout, &routeback, sink, TRIGGER_START);

    PacketFlow flow(NULL);
    deliver_to_sink(sink, flow, 1, 0);
    deliver_to_sink(sink, flow, 2001, 0);
    deliver_to_sink(sink, flow, 2001, 0);
    deliver_to_sink(sink, flow, 1, 0);
    deliver_to_sink(sink, flow, 1001, 7, true);

    expect(sink.rx_rcvd_bytes() == 2 * kMss,
           "receiver bytes must count each complete PSN once and exclude Trim");
    expect(control.ack_delivered.size() == 4,
           "four full arrivals should produce four ACKs");
    expect(control.ack_delivered[0] == 1 &&
           control.ack_delivered[1] == 2001 &&
           control.ack_delivered[2] == 0 &&
           control.ack_delivered[3] == 0,
           "only first full receipt of a PSN may be delivered credit");
    expect(control.nack_psns.size() == 1 && control.nack_psns[0] == 1001,
           "Trim NACK must identify the exact trimmed PSN");
    expect(control.nack_attempts[0] == 7,
           "Trim NACK must echo the failed packet attempt id");
}

static void test_runtime_send_writes_and_increments_attempt() {
    EventList& eventlist = test_eventlist();
    configure_bounded_transport();
    BoundedDataCapture data;
    Route route;
    route.push_back(&data);
    RoceSrc src(NULL, NULL, eventlist, speedFromMbps((uint64_t)100000));
    src._route = &route;
    src._flow_started = true;
    src._state_send = RoceSrc::PAUSED;
    src._highest_sent = 0;
    src._last_acked = 0;
    src._flow_size = kMss;
    src._cc_cwnd_pkts = 1.0;
    src._bounded_packets.clear();
    src._bounded_inflight_pkts = 0;

    expect(src.send_packet(), "bounded mode should send the first new packet");
    expect(data.attempts.size() == 1 && data.attempts[0] == 0 &&
           !data.retransmitted[0],
           "new packet must carry attempt zero");
    expect(src.bounded_queue_failure(1, 0),
           "attempt zero failure must queue the packet");
    expect(src.send_packet(), "bounded mode should send the queued retry");
    expect(data.attempts.size() == 2 && data.attempts[1] == 1 &&
           data.retransmitted[1],
           "first retry must carry incremented attempt one");
    expect(src._bounded_packets[1].state == RoceSrc::BOUNDED_RTX_INFLIGHT,
           "sent retry must be tracked as the sole current attempt");
}

int main() {
    expect(RoceSrc::transportSemantics() ==
               RoceSrc::TRANSPORT_MRC_EXACT_BOUNDED,
           "Exact+Bounded should be the static transport default");
    expect(RoceSrc::trimRecoveryMode() == RoceSrc::TRIM_RECOVERY_EXACT_PSN,
           "exact-PSN Trim recovery should be the static default");
    test_duplicate_ack_releases_unique_psn_once();
    test_cumulative_ack_clocks_each_unique_psn();
    test_sack_confirmation_precedes_exact_failure();
    test_duplicate_nack_queues_once();
    test_duplicate_ooo_without_progress_does_not_refresh_rto();
    test_attempt_matching_and_retrim();
    test_sack_loss_and_exact_do_not_double_queue();
    test_one_mtu_recovery_reserve_is_bounded();
    test_rto_releases_lost_reserve_attempt();
    test_receiver_counts_only_first_full_psn_and_echoes_attempt();
    test_runtime_send_writes_and_increments_attempt();
    cout << "RESULT exact_bounded_recovery pass=1" << endl;
    return 0;
}
