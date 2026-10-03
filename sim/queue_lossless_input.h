// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-        
#ifndef _LOSSLESS_INPUT_QUEUE_H
#define _LOSSLESS_INPUT_QUEUE_H
#include "queue.h"
/*
 * A FIFO queue that supports PAUSE frames and lossless operation
 */

#include <list>
#include "config.h"
#include "eventlist.h"
#include "network.h"
#include "loggertypes.h"
#include "eth_pause_packet.h"
#include "switch.h"
#include "callback_pipe.h"

class Switch;

class LosslessInputQueue : public Queue, public VirtualQueue {
public:
    enum { PFC_CLASSES = 8 };
    LosslessInputQueue(EventList &eventlist);
    LosslessInputQueue(EventList &eventlist,BaseQueue* peer, Switch* sw, simtime_picosec wire_latency, bool dynamic_shared = false);
    LosslessInputQueue(EventList &eventlist,BaseQueue* peer);

    virtual void receivePacket(Packet& pkt);

    void sendPause(unsigned int wait, uint32_t pg);
    void admissionComplete(Packet& pkt);
    void discardUnadmitted(Packet& pkt);
    virtual void completedService(Packet& pkt);
    static uint32_t priorityGroup(const Packet& pkt);

    virtual void setName(const string& name) {
        Logged::setName(name); 
        _nodename += name;
    }
    virtual string& nodename() { return _nodename; }

    enum {PAUSED,READY,PAUSE_RECEIVED};

    static uint64_t _low_threshold;
    static uint64_t _high_threshold;
    static uint64_t _pause_events;
    static uint64_t _resume_events;
    static uint64_t _peak_queue_bytes;
    static uint64_t _minimum_dynamic_threshold;
    static bool _pfc_enabled;

private:
    class PfcStateTimer : public EventSource {
    public:
        PfcStateTimer(EventList& ev, LosslessInputQueue& queue, uint32_t pg)
            : EventSource(ev, "pfc state"), _queue(queue), _pg(pg) {}
        void doNextEvent();
    private:
        LosslessInputQueue& _queue;
        uint32_t _pg;
    };
    void pauseTimerExpired(uint32_t pg);
    void refreshPause(uint32_t pg);
    void sendResume(uint32_t pg);
    bool _mmu_paused[PFC_CLASSES];
    bool _pause_remote[PFC_CLASSES];
    PfcStateTimer* _pfc_timers[PFC_CLASSES];
    CallbackPipe* _wire;
    bool _dynamic_shared;
};

#endif
