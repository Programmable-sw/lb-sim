#ifndef CONGA_MODEL_H
#define CONGA_MODEL_H

#include <algorithm>
#include <stdint.h>
#include <vector>

inline uint64_t conga_decay_dre(uint64_t value, uint64_t periods) {
    while (periods-- && value)
        value = value * 4 / 5;
    return value;
}

inline uint8_t conga_quantize_dre(uint64_t bytes, uint64_t bitrate_bps,
                                  uint64_t tau_ps, uint32_t bits) {
    if (!bitrate_bps || !tau_ps || !bits)
        return 0;
    const long double capacity_bytes =
        (long double)bitrate_bps * (long double)tau_ps / 8000000000000.0L;
    const uint32_t maximum = bits >= 8 ? 255u : ((1u << bits) - 1u);
    uint64_t metric = (uint64_t)((long double)bytes * (1u << bits) /
                                 capacity_bytes);
    return (uint8_t)std::min<uint64_t>(metric, maximum);
}

inline uint32_t conga_choose_uplink(const std::vector<uint32_t>& scores,
                                    uint32_t preferred,
                                    uint32_t random_value) {
    if (scores.empty())
        return 0;
    const uint32_t minimum = *std::min_element(scores.begin(), scores.end());
    std::vector<uint32_t> candidates;
    for (uint32_t i = 0; i < scores.size(); ++i)
        if (scores[i] == minimum)
            candidates.push_back(i);
    if (preferred < scores.size() && scores[preferred] == minimum)
        return preferred;
    return candidates[random_value % candidates.size()];
}

#endif
