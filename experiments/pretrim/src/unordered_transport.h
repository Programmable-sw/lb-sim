#ifndef V7G_UNORDERED_TRANSPORT_H
#define V7G_UNORDERED_TRANSPORT_H
#include "protocols.h"
#include <bitset>

namespace pretrim {
inline bool unordered_retransmission_needed(bool retransmission,bool outstanding,
                                            uint64_t attempt,uint64_t latest) {
    return !retransmission||(outstanding&&attempt==latest);
}
class UnorderedRxWindow {
public:
    static const uint64_t RANGE = 16384;
    explicit UnorderedRxWindow(SharedRxBudget& budget): budget_(budget) {}
    RxResult accept_data(uint64_t seq, RxPayloadCapacity* payload=0,
                         uint64_t bytes=0) {
        if(seq<next_) return RxResult::OLD;
        if(seq-next_>=RANGE) return RxResult::DISTANCE_OVERFLOW;
        if(bits_[seq%RANGE]) return RxResult::DUPLICATE;
        bool ooo=seq!=next_;
        if(ooo&&!budget_.acquire()) return RxResult::RESOURCE_OVERFLOW;
        if(payload&&!payload->acquire(bytes)) {
            if(ooo) budget_.release();
            return RxResult::CAPACITY_OVERFLOW;
        }
        bits_.set(seq%RANGE);
        if(ooo) ++used_;
        else {
            bits_.reset(next_++%RANGE);
            while(bits_[next_%RANGE]) {
                bits_.reset(next_++%RANGE); --used_; budget_.release();
            }
        }
        return ooo?RxResult::OUT_OF_ORDER:RxResult::CONTIGUOUS;
    }
    uint64_t next_expected() const { return next_; }
    uint64_t max_distance() const { return RANGE; }
    uint32_t occupancy() const { return used_; }
    uint64_t sack(uint64_t base) const {
        uint64_t mask=0;
        for(unsigned i=0;i<64;i++) {
            uint64_t seq=base+i;
            if(seq>=next_&&seq-next_<RANGE&&bits_[seq%RANGE]) mask|=uint64_t(1)<<i;
        }
        return mask;
    }
private:
    SharedRxBudget& budget_;
    std::bitset<RANGE> bits_;
    uint64_t next_=0;
    uint32_t used_=0;
};

class TransportRxWindow {
public:
    TransportRxWindow(uint64_t distance,SharedRxBudget& b,bool unordered):
        limited_(distance,b),unordered_(b),enabled_(unordered) {}
    RxResult accept_data(uint64_t n,RxPayloadCapacity* p=0,uint64_t bytes=0) {
        return enabled_?unordered_.accept_data(n,p,bytes):limited_.accept_data(n,p,bytes);
    }
    uint64_t next_expected() const {return enabled_?unordered_.next_expected():limited_.next_expected();}
    uint64_t max_distance() const {return enabled_?unordered_.max_distance():limited_.max_distance();}
    uint32_t occupancy() const {return enabled_?unordered_.occupancy():limited_.occupancy();}
    uint64_t sack(uint64_t base) const {return enabled_?unordered_.sack(base):0;}
private:
    BoundedRxWindow limited_;
    UnorderedRxWindow unordered_;
    bool enabled_;
};
inline bool sack_confirms(uint64_t seq,uint64_t cumulative,uint64_t base,uint64_t mask) {
    return seq<cumulative||(seq>=base&&seq-base<64&&((mask>>(seq-base))&1));
}

class NativeAckEchoLedger {
public:
    bool first_payload_echo(uint64_t psn,bool payload_echo) {
        return payload_echo&&echoed_.insert(psn).second;
    }
private:
    std::set<uint64_t> echoed_;
};

struct QpDeadline {uint64_t psn,attempt,sent,deadline;};
class QpRetransmissionTimer {
public:
    void sent(uint64_t psn,uint64_t attempt,uint64_t now,uint64_t deadline) {
        auto old=records_.find(psn);
        if(old!=records_.end()&&old->second.attempt>attempt)return;
        records_[psn]=QpDeadline{psn,attempt,now,deadline};
    }
    void ack(uint64_t psn) {records_.erase(psn);}
    bool empty() const {return records_.empty();}
    uint64_t deadline() const {
        uint64_t d=std::numeric_limits<uint64_t>::max();
        for(const auto& r:records_) d=std::min(d,r.second.deadline);
        return d;
    }
    std::vector<QpDeadline> expired(uint64_t now) {
        std::vector<QpDeadline> out;
        for(auto it=records_.begin();it!=records_.end();) {
            if(it->second.deadline<=now) {out.push_back(it->second); it=records_.erase(it);}
            else ++it;
        }
        return out;
    }
    size_t size() const {return records_.size();}
private:
    std::map<uint64_t,QpDeadline> records_;
};

// Author UEC DCTCP variant, units packets. Not alpha-smoothed standard DCTCP.
class UecDctcpState {
public:
    UecDctcpState(double initial,double maximum):cwnd_(initial),maximum_(maximum) {}
    void ack(uint64_t packets,bool ce) {
        if(ce) cwnd_=std::max(1.0,cwnd_-packets/3.0);
        else cwnd_=std::min(maximum_,cwnd_+packets/cwnd_);
    }
    void loss() {cwnd_=std::max(1.0,cwnd_-1.0);}
    double cwnd() const {return cwnd_;}
private:
    double cwnd_,maximum_;
};
}
#endif
