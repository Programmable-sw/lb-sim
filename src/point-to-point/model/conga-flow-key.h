#pragma once

#include <stdint.h>

namespace ns3 {

// CONGA flowlets are identified by the complete transport flow identity.
// Keeping the fields separate avoids truncation and overlapping bit fields.
struct CongaFlowKey {
    uint32_t sip;
    uint32_t dip;
    uint16_t sport;
    uint16_t dport;
    uint16_t pg;

    CongaFlowKey(uint32_t sourceIp, uint32_t destinationIp, uint16_t sourcePort,
                 uint16_t destinationPort, uint16_t priorityGroup)
        : sip(sourceIp), dip(destinationIp), sport(sourcePort), dport(destinationPort),
          pg(priorityGroup) {}

    bool operator<(const CongaFlowKey& other) const {
        if (sip != other.sip) return sip < other.sip;
        if (dip != other.dip) return dip < other.dip;
        if (sport != other.sport) return sport < other.sport;
        if (dport != other.dport) return dport < other.dport;
        return pg < other.pg;
    }
};

inline uint32_t CongaSaturateMetric(uint32_t metric, uint32_t bits) {
    const uint32_t maximum = bits >= 32 ? UINT32_MAX : ((1u << bits) - 1u);
    return metric > maximum ? maximum : metric;
}

}  // namespace ns3
