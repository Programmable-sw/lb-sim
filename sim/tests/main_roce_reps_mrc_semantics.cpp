// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <vector>

#define private public
#include "roce.h"
#undef private

#include "ecn.h"
#include "eventlist.h"
#include "network.h"
#include "rocepacket.h"

class DropSink : public PacketSink {
public:
    DropSink() : _name("drop_sink") {}
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

static RoceAck* make_ack(PacketFlow& flow, Route& route,
                         RocePacket::seq_t ackno, uint32_t pathid,
                         uint32_t flags = 0) {
    RoceAck* ack = RoceAck::newpkt(flow, route, ackno, 7);
    ack->set_pathid(pathid);
    ack->set_flags(flags);
    return ack;
}

static void test_reps_clean_ack_recycling() {
    DropSink drop;
    Route route;
    route.push_back(&drop);

    RoceSrc::setLoadBalancing(RoceSrc::LB_REPS);
    RoceSrc::setPathEntropySize(16);
    RoceSrc::setRepsBufferSize(4);
    RoceSrc::setRepsWarmupPkts(0);

    RoceSrc src(NULL, NULL, test_eventlist(),
                speedFromMbps((uint64_t)100000));
    src.set_flowid(11);
    src._flow_started = true;
    src.reset_reps_buffer();

    RoceAck* clean_a = make_ack(src._flow, route, 1000, 3);
    src.update_reps(*clean_a);
    clean_a->free();
    RoceAck* ecn_b = make_ack(src._flow, route, 2000, 5, ECN_ECHO);
    src.update_reps(*ecn_b);
    ecn_b->free();
    RoceAck* clean_c = make_ack(src._flow, route, 3000, 7);
    src.update_reps(*clean_c);
    clean_c->free();

    src.choose_path(Packet::PRIO_LO, false);
    expect(src._reps_selected_ev == 3,
           "REPS should consume the oldest clean cached EV first");
    src.choose_path(Packet::PRIO_LO, false);
    expect(src._reps_selected_ev == 7,
           "REPS should not cache an ECN-marked EV");
    expect(src._reps_cached_sends == 2,
           "REPS cached-send counter should include both clean ACKs");
}

static void test_rr_and_healthy_mrc_share_the_64_path_rotation() {
    RoceSrc::setPathEntropySize(64);
    RoceSrc rr(NULL, NULL, test_eventlist(),
               speedFromMbps((uint64_t)100000));
    RoceSrc mrc(NULL, NULL, test_eventlist(),
                speedFromMbps((uint64_t)100000));
    rr.set_src(7);
    rr.set_dst(199);
    rr.set_flowid(1701);
    rr._flow_started = true;
    mrc.set_src(7);
    mrc.set_dst(199);
    mrc.set_flowid(1701);
    mrc._flow_started = true;

    for (uint32_t i = 0; i < 64; ++i) {
        const uint32_t rr_path = rr.choose_rr_path(64);
        const RoceSrc::MrcChoice mrc_choice = mrc.choose_mrc_ev(64);
        expect(rr_path == mrc_choice.physical_path,
               "healthy MRC and RR should traverse the same shuffled paths");
    }
}

static void test_reps_freezes_only_after_loss_outside_warmup() {
    DropSink drop;
    Route route;
    route.push_back(&drop);
    RoceSrc::setLoadBalancing(RoceSrc::LB_REPS);
    RoceSrc::setPathEntropySize(8);
    RoceSrc::setRepsBufferSize(4);
    RoceSrc::setRepsWarmupPkts(4);
    RoceSrc src(NULL, NULL, test_eventlist(), speedFromMbps((uint64_t)100000));
    src._flow_started = true;
    src.reset_reps_buffer();

    RoceNack* ooo = RoceNack::newpkt(src._flow, route, 1000);
    ooo->set_reason(RoceNack::OOO);
    src.processNack(*ooo);
    ooo->free();
    expect(!src._reps_freezing, "OOO NACK must not enter REPS freezing");

    RoceNack* loss = RoceNack::newpkt(src._flow, route, 2000);
    loss->set_reason(RoceNack::LOSS);
    src.processNack(*loss);
    loss->free();
    expect(!src._reps_freezing, "REPS must not freeze during warmup");

    src._reps_explore_remaining = 0;
    RoceNack* post_warmup_loss = RoceNack::newpkt(src._flow, route, 3000);
    post_warmup_loss->set_reason(RoceNack::LOSS);
    src.processNack(*post_warmup_loss);
    post_warmup_loss->free();
    expect(src._reps_freezing, "loss after warmup should enter REPS freezing");
}

static void test_mprdma_does_not_prune_without_delivered_psn() {
    DropSink drop;
    Route route;
    route.push_back(&drop);
    RoceSrc::setLoadBalancing(RoceSrc::LB_MPRDMA);
    RoceSrc::setPathEntropySize(8);
    RoceSrc src(NULL, NULL, test_eventlist(), speedFromMbps((uint64_t)100000));
    src._flow_started = true;
    src._mss = 1000;
    RoceAck* ack = make_ack(src._flow, route, 100000, 3);
    src.update_mprdma(*ack);
    ack->free();
    expect(!src._mprdma_pruned[3],
           "cumulative ACK without delivered PSN must not prune an MPRDMA VP");
}


static void test_reps_rotation() {
    DropSink drop; Route route; route.push_back(&drop);
    RoceSrc::setLoadBalancing(RoceSrc::LB_REPS);
    RoceSrc::setRepsBufferSize(4); RoceSrc::setRepsWarmupPkts(0);
    RoceSrc src(NULL, NULL, test_eventlist(), speedFromMbps((uint64_t)100000));
    for (uint32_t ev = 1; ev <= 2; ++ev) {
        RoceAck* ack = make_ack(src._flow, route, 1000, ev);
        src.update_reps(*ack); ack->free();
    }
    src._reps_freezing = true;
    expect(src.choose_path(Packet::PRIO_LO, false) == 1, "consume oldest");
    expect(src.choose_path(Packet::PRIO_LO, false) == 2, "consume next");
    for (int i = 0; i < 8; ++i)
        expect(src.choose_path(Packet::PRIO_LO, false) == (uint32_t)(1 + i % 2),
               "freeze must rotate learned EVs, skipping unwritten slots");
    RoceAck* ack = make_ack(src._flow, route, 2000, 3);
    src.update_reps(*ack); ack->free();
    expect(src.choose_path(Packet::PRIO_LO, false) == 3, "new ACK remains FIFO after frozen rotation");
}

static void test_reps_expiry(bool marked) {
    DropSink drop; Route route; route.push_back(&drop);
    RoceSrc::setLoadBalancing(RoceSrc::LB_REPS);
    RoceSrc::setRepsWarmupPkts(16);
    RoceSrc src(NULL, NULL, test_eventlist(), speedFromMbps((uint64_t)100000));
    src._cc_cwnd_pkts = 5.75;
    src._reps_explore_remaining = 0;
    src._reps_freezing = true;
    src._reps_freezing_until = 1;
    // Advance the test clock without scheduling a live transport.
    EventList::_lasteventtime = 2;
    RoceAck* ack = make_ack(src._flow, route, 1000, 3, marked ? ECN_ECHO : 0);
    src.update_reps(*ack); ack->free();
    if (marked) {
        expect(src._reps_freezing, "ECN ACK must not exit freezing");
        expect(src._reps_explore_remaining == 0, "ECN ACK must not restart exploration");
    } else {
        expect(!src._reps_freezing, "clean ACK after deadline exits freeze");
        expect(src._reps_explore_remaining == 5, "freeze exit explores current cwnd, not initial BDP");
    }
}

static void test_reps_no_rearm() {
    DropSink drop; Route route; route.push_back(&drop);
    RoceSrc::setLoadBalancing(RoceSrc::LB_REPS);
    RoceSrc::setRepsWarmupPkts(0);
    RoceSrc src(NULL, NULL, test_eventlist(), speedFromMbps((uint64_t)100000));
    src._reps_freezing = true; src._reps_freezing_until = 123;
    RoceNack* nack = RoceNack::newpkt(src._flow, route, 1000);
    nack->set_reason(RoceNack::LOSS);
    src.processNack(*nack); nack->free();
    expect(src._reps_freezing_until == 123, "repeated failure must not postpone freeze expiry");
}

static void test_mprdma_psn() {
    DropSink drop; Route route; route.push_back(&drop);
    RoceSrc::setLoadBalancing(RoceSrc::LB_MPRDMA);
    RoceSrc::setPathEntropySize(8);
    RoceSrc src(NULL, NULL, test_eventlist(), speedFromMbps((uint64_t)100000));
    src._mss = 1000;
    src._cc_cwnd_pkts = 8;
    RoceAck* ack = make_ack(src._flow, route, 1000000, 3);
    src.update_mprdma(*ack); ack->free();
    expect(src._mprdma_snd_ooh == 0, "cumulative ACK cannot advance highest SACK PSN");
    ack = make_ack(src._flow, route, 0, 3);
    ack->set_delivered_psn(100001); src.update_mprdma(*ack); ack->free();
    ack = make_ack(src._flow, route, 1000000, 4);
    ack->set_delivered_psn(1001); src.update_mprdma(*ack); ack->free();
    expect(src._mprdma_pruned[4], "slow delivered PSN must prune despite high cumulative ACK");
    ack = make_ack(src._flow, route, 0, 5);
    src.update_mprdma(*ack); ack->free();
    expect(!src._mprdma_pruned[5], "stale cumulative ACK without SACK must not prune");
    expect(src._mprdma_ack_clock.size() == 4,
           "clean ACKs should provide at most two budgeted ACK-clock credits each");
}



static void test_reps_rto() {
    RoceSrc::setLoadBalancing(RoceSrc::LB_REPS);
    RoceSrc::setTransportSemantics(RoceSrc::TRANSPORT_LEGACY);
    RoceSrc::setRepsWarmupPkts(0);
    RoceSrc src(NULL, NULL, test_eventlist(), speedFromMbps((uint64_t)100000));
    src._flow_started = true; src._done = false;
    src._highest_sent = 1000; src._last_acked = 0;
    src._rtx_timeout = 1; src._state_send = RoceSrc::PAUSED;
    src.rtx_timer_hook(2, 1);
    expect(src._reps_freezing, "expired RTO must trigger REPS failure detection");
}

class AckCapture : public PacketSink {
public:
    RoceAck* ack = NULL;
    string name = "ack capture";
    void receivePacket(Packet& pkt) { ack = static_cast<RoceAck*>(&pkt); }
    const string& nodename() { return name; }
};

static void test_mprdma_receiver_feedback() {
    RoceSrc::setLoadBalancing(RoceSrc::LB_MPRDMA);
    RoceSrc::setTransportSemantics(RoceSrc::TRANSPORT_LEGACY);
    RoceSrc::setPathEntropySize(8);
    RoceSrc src(NULL, NULL, test_eventlist(), speedFromMbps((uint64_t)100000));
    RoceSink sink;
    AckCapture capture; Route route; route.push_back(&capture);
    sink.connect(src, &route);
    src._mprdma_snd_ooh = 100001;
    for (int retransmit = 0; retransmit < 2; ++retransmit) {
        RocePacket* packet = RocePacket::newpkt(src._flow, 1001, 1000, retransmit, false);
        packet->set_pathid(3);
        sink.send_ack(*packet, 0, false, false);
        packet->free();
        expect(capture.ack && capture.ack->has_delivered_psn(), "legacy MPRDMA ACK must carry triggering packet PSN");
        expect(capture.ack->delivered_psn() == 1001 && capture.ack->pathid() == 3,
               "receiver must pair packet PSN with its VP");
        src.update_mprdma(*capture.ack);
        expect(src._mprdma_pruned[3] == !retransmit,
               "late original packet prunes, but ReTx ACK treats VP as good");
        capture.ack->free(); capture.ack = NULL;
    }
}

int main(int argc, char** argv) {
    Packet::set_packet_size(1000);
    if (argc > 1) {
        string test(argv[1]);
        if (test == "rto") test_reps_rto();
        else if (test == "receiver") test_mprdma_receiver_feedback();
        else if (test == "rotation") test_reps_rotation();
        else if (test == "expiry") test_reps_expiry(false);
        else if (test == "ecn_expiry") test_reps_expiry(true);
        else if (test == "no_rearm") test_reps_no_rearm();
        else if (test == "mpr_psn") test_mprdma_psn();
        else return 2;
        return 0;
    }
    test_reps_clean_ack_recycling();
    test_rr_and_healthy_mrc_share_the_64_path_rotation();
    test_reps_freezes_only_after_loss_outside_warmup();
    test_mprdma_does_not_prune_without_delivered_psn();
    return 0;
}
