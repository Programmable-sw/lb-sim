// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-        
#include "queue_lossless_input.h"
#include <math.h>
#include <iostream>
#include <sstream>
#include "switch.h"
#include "shared_buffer.h"

uint64_t LosslessInputQueue::_high_threshold = 0;
uint64_t LosslessInputQueue::_low_threshold = 0;
uint64_t LosslessInputQueue::_pause_events = 0;
uint64_t LosslessInputQueue::_resume_events = 0;
uint64_t LosslessInputQueue::_peak_queue_bytes = 0;
uint64_t LosslessInputQueue::_minimum_dynamic_threshold = UINT64_MAX;
bool LosslessInputQueue::_pfc_enabled = true;

LosslessInputQueue::LosslessInputQueue(EventList& eventlist)
    : Queue(speedFromGbps(1),Packet::data_packet_size()*2000,eventlist,NULL),
      VirtualQueue(),
      _dynamic_shared(false)
{
    for (unsigned i = 0; i < PFC_CLASSES; ++i) {
        _mmu_paused[i] = _pause_remote[i] = false;
        _pfc_timers[i] = new PfcStateTimer(eventlist, *this, i);
    }
    assert(_high_threshold>0);
    assert(_high_threshold > _low_threshold);

    _wire = NULL;
}

LosslessInputQueue::LosslessInputQueue(EventList& eventlist,BaseQueue* peer)
    : Queue(speedFromGbps(1),Packet::data_packet_size()*2000,eventlist,NULL),
      VirtualQueue(),
      _dynamic_shared(false)
{
    for (unsigned i = 0; i < PFC_CLASSES; ++i) {
        _mmu_paused[i] = _pause_remote[i] = false;
        _pfc_timers[i] = new PfcStateTimer(eventlist, *this, i);
    }
    assert(_high_threshold>0);
    assert(_high_threshold > _low_threshold);

    stringstream ss;
    ss << "VirtualQueue("<< peer->_name<< ")";
    _nodename = ss.str();
    _remoteEndpoint = peer;
    _switch = NULL;
    _wire = NULL;

    peer->setRemoteEndpoint(this);
}

LosslessInputQueue::LosslessInputQueue(EventList& eventlist,BaseQueue* peer, Switch* sw, simtime_picosec wire_latency, bool dynamic_shared)
    : Queue(speedFromGbps(1),Packet::data_packet_size()*2000,eventlist,NULL),
      VirtualQueue(),
      _dynamic_shared(dynamic_shared)
{
    for (unsigned i = 0; i < PFC_CLASSES; ++i) {
        _mmu_paused[i] = _pause_remote[i] = false;
        _pfc_timers[i] = new PfcStateTimer(eventlist, *this, i);
    }
    assert(_high_threshold>0);
    assert(_high_threshold > _low_threshold);

    stringstream ss;
    ss << "VirtualQueue("<< peer->_name<< ")";
    _nodename = ss.str();
    _remoteEndpoint = peer;
    _switch = sw;

    _wire = new CallbackPipe(wire_latency, eventlist, _remoteEndpoint);

    assert(_switch);

    if (_dynamic_shared) {
        SharedBufferSwitch* shared = dynamic_cast<SharedBufferSwitch*>(_switch);
        assert(shared);
        shared->shared_register_ingress(this);
    }

    peer->setRemoteEndpoint(this);
}


void
LosslessInputQueue::receivePacket(Packet& pkt)
{
    /* normal packet, enqueue it */
    _queuesize += pkt.size();
    if ((uint64_t)_queuesize > _peak_queue_bytes)
        _peak_queue_bytes = _queuesize;

    //send PAUSE notifications if that is the case!
    assert(_queuesize > 0);
    if (!_dynamic_shared && (uint64_t)_queuesize > _high_threshold &&
        !_pause_remote[Packet::PRIO_LO]) {
        _pause_remote[Packet::PRIO_LO] = true;
        sendPause(1000, Packet::PRIO_LO);
    }

    //if (_state_recv==PAUSED)
    //cout << timeAsMs(eventlist().now()) << " queue " << _name << " switch (" << _switch->_name << ") "<< " recv when paused pkt " << pkt.type() << " sz " << _queuesize << endl;        

    if (_queuesize > _maxsize){
        cout << " Queue " << _name << " LOSSLESS not working! I should have dropped this packet" << _queuesize / Packet::data_packet_size() << endl;
    }
    
    //tell the output queue we're here!
    if (pkt.nexthop() < pkt.route()->size()){
        //this should not work...
        //assert(0);
        pkt.sendOn2(this);
    }
    else {
        assert(_switch);
        pkt.set_ingress_queue(this);
        _switch->receivePacket(pkt);
    }
}

uint32_t LosslessInputQueue::priorityGroup(const Packet& pkt) {
    if (pkt.priority() == Packet::PRIO_HI || pkt.priority() == Packet::PRIO_NONE)
        return 0;
    if (pkt.priority() == Packet::PRIO_MID)
        return 2;
    return 3;
}

void LosslessInputQueue::admissionComplete(Packet& pkt) {
    if (!_dynamic_shared || !_pfc_enabled) return;
    SharedBufferSwitch* shared = dynamic_cast<SharedBufferSwitch*>(_switch);
    (void)pkt;
    for (uint32_t pg = 1; pg < PFC_CLASSES; ++pg) {
        const uint64_t threshold = shared->shared_pause_threshold(pg);
        if (threshold < _minimum_dynamic_threshold)
            _minimum_dynamic_threshold = threshold;
        if (shared->shared_should_pause(this, pg)) refreshPause(pg);
    }
    for (uint32_t pg = 1; pg < PFC_CLASSES; ++pg) {
        if (_pause_remote[pg] && _mmu_paused[pg] &&
            shared->shared_should_resume(this, pg)) sendResume(pg);
    }
}

void LosslessInputQueue::discardUnadmitted(Packet& pkt) {
    assert(_queuesize >= pkt.size());
    _queuesize -= pkt.size();
}

void LosslessInputQueue::completedService(Packet& pkt){
    _queuesize -= pkt.size();

    //unblock if that is the case
    assert(_queuesize >= 0);
    const uint32_t pg = priorityGroup(pkt);
    if (_dynamic_shared) {
        SharedBufferSwitch* shared = dynamic_cast<SharedBufferSwitch*>(_switch);
        if (pg && _pause_remote[pg] && _mmu_paused[pg] &&
            shared->shared_should_resume(this, pg)) sendResume(pg);
    } else if ((uint64_t)_queuesize < _low_threshold &&
               _pause_remote[Packet::PRIO_LO]) {
        _pause_remote[Packet::PRIO_LO] = false;
        sendPause(0, Packet::PRIO_LO);
    }
}

void LosslessInputQueue::refreshPause(uint32_t pg) {
    _mmu_paused[pg] = true;
    _pause_remote[pg] = true;
    ++_pause_events;
    sendPause(5, pg);
    eventlist().reschedulePendingSource(*_pfc_timers[pg],
        eventlist().now() + timeFromUs((uint32_t)5));
}

void LosslessInputQueue::sendResume(uint32_t pg) {
    eventlist().cancelPendingSource(*_pfc_timers[pg]);
    _mmu_paused[pg] = false;
    _pause_remote[pg] = false;
    ++_resume_events;
    sendPause(0, pg);
}

void LosslessInputQueue::PfcStateTimer::doNextEvent() {
    _queue.pauseTimerExpired(_pg);
}

void LosslessInputQueue::pauseTimerExpired(uint32_t pg) {
    _mmu_paused[pg] = false;
}

void LosslessInputQueue::sendPause(unsigned int wait, uint32_t pg){
    //cout << "Ingress link " << getRemoteEndpoint() << " PAUSE " << wait << endl;    
    uint32_t switchID = 0;
    if (_switch)
        switchID = getSwitch()->getID();

    EthPausePacket* pkt = EthPausePacket::newpkt(wait,switchID);
    pkt->setPausedClass(pg);

    if (_wire)
        _wire->receivePacket(*pkt);
    else
        getRemoteEndpoint()->receivePacket(*pkt);
};
