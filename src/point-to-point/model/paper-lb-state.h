#ifndef PAPER_LB_STATE_H
#define PAPER_LB_STATE_H
#include <algorithm>
#include <cstdint>
#include <deque>
namespace ns3 {
// Time arguments are nanoseconds. REPS Algorithms 1 and 2.
struct PaperReps {
    uint16_t ev[8] = {};
    bool initialized[8] = {};
    bool valid[8] = {};
    unsigned head = 0, count = 0, written = 0, explore = 0;
    uint16_t explorationEv = 0;
    bool frozen = false;
    uint64_t deadline = 0;
    void Fail(uint64_t now) {
        if (!frozen && !explore) { frozen = true; deadline = now + 100000; }
    }
    void Ack(uint16_t value, bool ecn, uint64_t now, unsigned cwnd) {
        if (ecn) return;
        if (!valid[head]) ++count;
        if (!initialized[head]) { initialized[head] = true; ++written; }
        ev[head] = value; valid[head] = true; head = (head + 1) % 8;
        if (frozen && now > deadline) { frozen = false; explore = std::max(1u, cwnd); }
    }
    uint16_t Select(uint16_t randomEv, uint64_t now) {
        (void)now;
        if (explore) {
            if ((--explore % 8) == 0) explorationEv = randomEv;
            return explorationEv;
        }
        if (count) {
            unsigned offset = (head + 8 - count) % 8;
            valid[offset] = false; --count; return ev[offset];
        }
        if (frozen && written) {
            while (!initialized[head]) head = (head + 1) % 8;
            unsigned offset = head; head = (head + 1) % 8; return ev[offset];
        }
        return randomEv;
    }
};
// Routing part of MP-RDMA: VP feedback, OOO pruning, at most two credits/ACK.
struct PaperMpr {
    std::deque<uint16_t> credits;
    uint64_t highest = 0, nextProbe = 0;
    unsigned initial = 0;
    bool ackSeen = false;
    void Ack(uint16_t vp, uint64_t psn, bool retransmitted, uint64_t delta, unsigned budget = 2) {
        highest = std::max(highest, psn); ackSeen = true;
        if (!retransmitted && psn + delta < highest) return;
        for (unsigned i = 0; i < std::min(2u, budget); ++i) credits.push_back(vp);
    }
    uint16_t Select(uint16_t randomVp) {
        if (initial) { --initial; return randomVp; }
        if (credits.empty()) return randomVp;
        uint16_t vp = credits.front(); credits.pop_front(); return vp;
    }
};
}
#endif
