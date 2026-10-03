// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-        
#ifndef _LOSSLESS_OUTPUT_QUEUE_H
#define _LOSSLESS_OUTPUT_QUEUE_H
/*
 * A FIFO queue that supports PAUSE frames and lossless operation
 */

#include "queue.h"
#include "config.h"
#include "eventlist.h"
#include "network.h"
#include "loggertypes.h"
#include "eth_pause_packet.h"
#include "ecn.h"

class LosslessOutputQueue : public Queue {
public:
    enum { PFC_CLASSES = 8 };
    LosslessOutputQueue(linkspeed_bps bitrate, mem_b maxsize, EventList &eventlist, QueueLogger* logger, int ECN=0, mem_b Kmin=0, mem_b Kmax=0);
    LosslessOutputQueue(linkspeed_bps bitrate, mem_b maxsize, EventList &eventlist, QueueLogger* logger, int ECN, mem_b Kmin, mem_b Kmax, bool shared_buffer);

    void receivePacket(Packet& pkt);
    void receivePacket(Packet& pkt,VirtualQueue* q);

    void beginService();
    void completeService();
    void setSwitch(Switch* s);

    bool is_paused() {
        for (unsigned i = 1; i < PFC_CLASSES; ++i)
            if (_paused_classes[i]) return true;
        return false;
    }
    uint64_t overflow_count() const { return _overflow_count; }
    uint64_t ecn_mark_count() const { return _ecn_marks; }

    enum queue_state {PAUSED,READY,PAUSE_RECEIVED};

private:
    class PfcResumeTimer : public EventSource {
    public:
        PfcResumeTimer(EventList& ev, LosslessOutputQueue& queue, uint32_t pg)
            : EventSource(ev, "pfc resume"), _queue(queue), _pg(pg) {}
        void doNextEvent();
    private:
        LosslessOutputQueue& _queue;
        uint32_t _pg;
    };
    void pauseTimerExpired(uint32_t pg);
    CircularBuffer<Packet*> _pg_packets[PFC_CLASSES];
    CircularBuffer<VirtualQueue*> _pg_vqs[PFC_CLASSES];
    Packet* _servicing_pkt;
    VirtualQueue* _servicing_vq;
    uint32_t _servicing_pg;
    uint32_t _last_data_pg;
    int _sending;
    uint64_t _txbytes;
    uint64_t _overflow_count;
    uint64_t _ecn_marks;

    int _ecn_enabled;
    mem_b _ecn_minthresh;
    mem_b _ecn_maxthresh;
    bool should_mark_ecn() const;
    bool _shared_buffer;
    bool _paused_classes[PFC_CLASSES];
    PfcResumeTimer* _pfc_timers[PFC_CLASSES];
};

#endif
