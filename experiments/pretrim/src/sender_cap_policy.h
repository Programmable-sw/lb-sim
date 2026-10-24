#ifndef V7G_SENDER_CAP_POLICY_H
#define V7G_SENDER_CAP_POLICY_H
#include <cstdint>
#include <limits>

namespace pretrim {
inline bool sender_cap_valid(uint64_t cap) {
    return cap == 0 || cap == 64 || cap == 128;
}
inline uint64_t sender_cap_packets(uint64_t cap) {
    return cap ? cap : std::numeric_limits<uint64_t>::max();
}
inline uint64_t sender_cap_bytes(uint64_t cap, uint64_t payload) {
    return cap ? cap * payload : std::numeric_limits<uint64_t>::max();
}
inline bool sender_cap_reopened(uint64_t outstanding_bytes, uint64_t cap,
                                uint64_t payload) {
    return !cap || outstanding_bytes + payload <= cap * payload;
}
}
#endif
