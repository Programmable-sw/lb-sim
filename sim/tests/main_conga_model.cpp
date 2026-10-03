#include <cassert>
#include <vector>
#include "../conga_model.h"

int main() {
    assert(conga_decay_dre(1000, 1) == 800);
    assert(conga_decay_dre(1000, 2) == 640);
    assert(conga_quantize_dre(0, 400000000000ULL, 160000000ULL, 3) == 0);
    assert(conga_quantize_dre(8000000, 400000000000ULL, 160000000ULL, 3) == 7);
    std::vector<uint32_t> tied(4, 0);
    assert(conga_choose_uplink(tied, UINT32_MAX, 2) == 2);
    assert(conga_choose_uplink(tied, 3, 0) == 3);
    std::vector<uint32_t> scores;
    scores.push_back(4); scores.push_back(1); scores.push_back(3);
    assert(conga_choose_uplink(scores, 0, 99) == 1);
    return 0;
}
