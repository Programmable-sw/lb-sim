// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-        
#include <math.h>
#include <stdlib.h>
#include <iostream>
#include <sstream>
#include "switch.h"
#include "hpccpacket.h"
#include "queue_lossless_output.h"
#include "queue_lossless_input.h"
#include "shared_buffer.h"

LosslessOutputQueue::LosslessOutputQueue(linkspeed_bps bitrate, mem_b maxsize,
                                         EventList& eventlist, QueueLogger* logger,
                                         int ECN, mem_b Kmin, mem_b Kmax)
    : LosslessOutputQueue(bitrate, maxsize, eventlist, logger, ECN, Kmin, Kmax, false) {}

LosslessOutputQueue::LosslessOutputQueue(linkspeed_bps bitrate, mem_b maxsize,
                                         EventList& eventlist, QueueLogger* logger, int ECN, mem_b Kmin, mem_b Kmax, bool shared_buffer)
    : Queue(bitrate,maxsize,eventlist,logger),
      _servicing_pkt(NULL), _servicing_vq(NULL), _servicing_pg(0),
      _last_data_pg(PFC_CLASSES - 1),
      _overflow_count(0),
      _ecn_marks(0), _shared_buffer(shared_buffer)
{
    //assume worst case: PAUSE frame waits for one MSS packet to be sent to other switch, and there is 
    //an MSS just beginning to be sent when PAUSE frame arrives; this means 2 packets per incoming
    //port, and we must have buffering for all ports except this one (assuming no one hop cycles!)

    _sending = 0;
    for (unsigned i = 0; i < PFC_CLASSES; ++i) {
        _paused_classes[i] = false;
        _pfc_timers[i] = new PfcResumeTimer(eventlist, *this, i);
    }

    _ecn_enabled = ECN;
    _ecn_minthresh = Kmin;
    _ecn_maxthresh = Kmax ? Kmax : Kmin;
    if (_ecn_maxthresh < _ecn_minthresh)
        _ecn_maxthresh = _ecn_minthresh;
    _txbytes = 0;

    stringstream ss;
    ss << "queue lossless output(" << bitrate/1000000 << "Mb/s," << maxsize << "bytes)";
    _nodename = ss.str();
}

void LosslessOutputQueue::setSwitch(Switch* s) {
    BaseQueue::setSwitch(s);
    if (_shared_buffer) {
        SharedBufferSwitch* shared = dynamic_cast<SharedBufferSwitch*>(s);
        assert(shared);
        shared->shared_register_egress(this);
    }
}

bool LosslessOutputQueue::should_mark_ecn() const {
    if (!_ecn_enabled)
        return false;
    if (_queuesize <= _ecn_minthresh)
        return false;
    if (_ecn_maxthresh <= _ecn_minthresh)
        return true;
    if (_queuesize >= _ecn_maxthresh)
        return true;

    uint64_t p = (0x7FFFFFFFULL * (uint64_t)(_queuesize - _ecn_minthresh)) /
                 (uint64_t)(_ecn_maxthresh - _ecn_minthresh);
    return (uint64_t)random() < p;
}


void
LosslessOutputQueue::receivePacket(Packet& pkt){
    if (pkt.type()==ETH_PAUSE)
        receivePacket(pkt,NULL);
    else {
        LosslessInputQueue* q = pkt.get_ingress_queue();
        pkt.clear_ingress_queue();
        receivePacket(pkt,dynamic_cast<VirtualQueue*>(q));
    }
}

void
LosslessOutputQueue::receivePacket(Packet& pkt,VirtualQueue* prev) 
{
    //is this a PAUSE frame? 
    if (pkt.type()==ETH_PAUSE){
        EthPausePacket* p = (EthPausePacket*)&pkt;
        const uint32_t pg = p->getPausedClass() < PFC_CLASSES ?
            p->getPausedClass() : PFC_CLASSES - 1;

        if (p->sleepTime()>0){
            _paused_classes[pg] = true;
            eventlist().reschedulePendingSource(*_pfc_timers[pg],
                eventlist().now() + timeFromUs(p->sleepTime()));
        }
        else {
            eventlist().cancelPendingSource(*_pfc_timers[pg]);
            _paused_classes[pg] = false;
            if (_queuesize > 0 && !_sending)
                beginService();
        }
        
        pkt.free();
        return;
    }

    /* normal packet, enqueue it */

    //remember the virtual queue that has sent us this packet; will notify the vq once the packet has left our buffer.
    assert(prev!=NULL);

    pkt.flow().logTraffic(pkt, *this, TrafficLogger::PKT_ARRIVE);

    bool queueWasEmpty = _queuesize == 0;

    LosslessInputQueue* ingress = dynamic_cast<LosslessInputQueue*>(prev);
    assert(ingress);
    const uint32_t pg = LosslessInputQueue::priorityGroup(pkt);
    if (_shared_buffer && pg != 0) {
        SharedBufferSwitch* ft_switch = dynamic_cast<SharedBufferSwitch*>(_switch);
        // ns-3 accounts the complete packet held by the switch.  Its workload
        // size is the 4096-byte payload; PPP/IP/UDP/SeqTs/INT add 48 bytes.
        const uint64_t mmu_bytes = pkt.size() + 48;
        SharedBufferSwitch::Admission result = ft_switch->shared_admit(
            ingress, pg, this, pg, mmu_bytes);
        if (result != SharedBufferSwitch::ADMIT) {
            ++_overflow_count;
            ingress->discardUnadmitted(pkt);
            pkt.free();
            return;
        }
        ingress->admissionComplete(pkt);
    }

    Packet* pkt_p = &pkt;
    VirtualQueue* vq = prev;
    _pg_packets[pg].push(pkt_p);
    _pg_vqs[pg].push(vq);

    _queuesize += pkt.size();

    if (_queuesize > _maxsize){
        _overflow_count++;
        cout << " Queue " << _name << " LOSSLESS not working! I should have dropped this packet" << _queuesize / Packet::data_packet_size() << endl;
    }

    if (_logger) 
        _logger->logQueue(*this, QueueLogger::PKT_ENQUEUE, pkt);

    if (queueWasEmpty && !_sending && !_paused_classes[pg]) {
        beginService();
    }
}

void LosslessOutputQueue::beginService(){
    assert(!_sending);
    int selected = -1;
    if (!_pg_packets[0].empty()) {
        selected = 0;
    } else {
        for (uint32_t offset = 1; offset < PFC_CLASSES; ++offset) {
            const uint32_t pg = 1 + ((_last_data_pg - 1 + offset) %
                                      (PFC_CLASSES - 1));
            if (!_paused_classes[pg] && !_pg_packets[pg].empty()) {
                selected = (int)pg;
                break;
            }
        }
    }
    if (selected < 0) return;
    _servicing_pg = (uint32_t)selected;
    if (_servicing_pg) _last_data_pg = _servicing_pg;
    _servicing_pkt = _pg_packets[_servicing_pg].next_to_pop();
    _servicing_vq = _pg_vqs[_servicing_pg].next_to_pop();
    assert(_servicing_pkt && _servicing_vq);
    _sending = 1;
    eventlist().sourceIsPendingRel(*this, drainTime(_servicing_pkt));
}

void LosslessOutputQueue::PfcResumeTimer::doNextEvent() {
    _queue.pauseTimerExpired(_pg);
}

void LosslessOutputQueue::pauseTimerExpired(uint32_t pg) {
    if (pg >= PFC_CLASSES || !_paused_classes[pg]) return;
    _paused_classes[pg] = false;
    if (_queuesize > 0 && !_sending) beginService();
}

void LosslessOutputQueue::completeService(){
    assert(_sending && _servicing_pkt && _servicing_vq);
    Packet* pkt = _pg_packets[_servicing_pg].pop();
    VirtualQueue* q = _pg_vqs[_servicing_pg].pop();
    assert(pkt == _servicing_pkt && q == _servicing_vq);

    if (pkt->type()==HPCC){
        //HPPC INT information adding to packet
        HPCCPacket* h = dynamic_cast<HPCCPacket*>(pkt);
        assert(h->_int_hop<5);

        h->_int_info[h->_int_hop]._queuesize = _queuesize;
        h->_int_info[h->_int_hop]._ts = eventlist().now();

        if (_switch){
            h->_int_info[h->_int_hop]._switchID = _switch->getID();
            h->_int_info[h->_int_hop]._type = _switch->getType();
        }

        h->_int_info[h->_int_hop]._txbytes = _txbytes;
        h->_int_info[h->_int_hop]._linkrate = _bitrate;

        h->_int_hop++;
    }   

    _queuesize -= pkt->size();
    const uint32_t pg = LosslessInputQueue::priorityGroup(*pkt);
    LosslessInputQueue* ingress = dynamic_cast<LosslessInputQueue*>(q);
    assert(ingress);
    bool mark_ecn = false;
    if (_shared_buffer && pg != 0) {
        SharedBufferSwitch* ft_switch = dynamic_cast<SharedBufferSwitch*>(_switch);
        ft_switch->shared_release(ingress, pg, this, pg, pkt->size() + 48);
        const double sample = double((uint64_t)random()) / double(0x7fffffffULL);
        mark_ecn = _ecn_enabled && ft_switch->shared_should_mark_ecn(
            this, pg, _ecn_minthresh, _ecn_maxthresh, sample);
    } else {
        mark_ecn = should_mark_ecn();
    }
    if (mark_ecn) {
        pkt->set_flags(pkt->flags() | ECN_CE);
        _ecn_marks++;
    }
    _txbytes += pkt->size();

    pkt->flow().logTraffic(*pkt, *this, TrafficLogger::PKT_DEPART);

    if (_logger) _logger->logQueue(*this, QueueLogger::PKT_SERVICE, *pkt);

    //tell the virtual input queue this packet is done!
    q->completedService(*pkt);

    //this is used for bandwidth utilization tracking. 
    log_packet_send(drainTime(pkt));

    //if (((uint64_t)timeAsUs(eventlist().now()))%5==0)
    //    cout << "Queue bandwidth utilization " << average_utilization() << "%" << endl;

    /* tell the packet to move on to the next pipe */
    pkt->sendOn();

    _sending = 0;
    _servicing_pkt = NULL;
    _servicing_vq = NULL;
    if (_queuesize > 0) beginService();
}
