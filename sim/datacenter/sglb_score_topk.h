#ifndef SGLB_SCORE_TOPK_H
#define SGLB_SCORE_TOPK_H
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <vector>

// Rank continuous scores only. Caller supplies fresh random keys for ties.
inline std::vector<uint32_t> sglb_score_topk(
    const std::vector<double>& scores, const std::vector<bool>& available,
    const std::vector<uint64_t>& tie_keys, uint32_t k) {
    std::vector<uint32_t> selected;
    for (uint32_t i = 0; i < scores.size(); ++i)
        if (available.at(i) && std::isfinite(scores[i])) selected.push_back(i);
    std::sort(selected.begin(), selected.end(), [&](uint32_t a, uint32_t b) {
        if (scores[a] != scores[b]) return scores[a] < scores[b];
        if (tie_keys.at(a) != tie_keys.at(b)) return tie_keys[a] < tie_keys[b];
        return a < b;
    });
    if (selected.size() > k) selected.resize(k);
    return selected;
}
#endif
