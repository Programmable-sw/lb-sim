#include "../datacenter/sglb_score_topk.h"
#include <cassert>
#include <limits>
int main() {
    std::vector<double> scores{0.049, 0.011, 0.010, 0.3};
    std::vector<bool> available{true, true, true, false};
    std::vector<uint64_t> keys{0, 1, 2, 3};
    assert((sglb_score_topk(scores, available, keys, 2) == std::vector<uint32_t>{2,1}));
    assert(sglb_score_topk(scores, available, keys, 64).size() == 3);
    scores = {0, 0, 0, 0};
    keys = {5, 1, 3, 0};
    assert((sglb_score_topk(scores, available, keys, 2) == std::vector<uint32_t>{1,2}));
    available.assign(4, false);
    assert(sglb_score_topk(scores, available, keys, 2).empty());
    available.assign(4, true);
    scores[1] = std::numeric_limits<double>::quiet_NaN();
    assert(sglb_score_topk(scores, available, keys, 4).size() == 3);
    assert(sglb_score_topk(scores, available, keys, 0).empty());
}
