#include <cassert>
#include <set>

#include "../../src/point-to-point/model/conga-flow-key.h"

int main() {
    std::set<ns3::CongaFlowKey> keys;
    for (uint32_t source = 0; source < 16; ++source) {
        for (uint32_t destination = 0; destination < 16; ++destination) {
            if (source == destination || source / 4 == destination / 4) continue;
            keys.insert(ns3::CongaFlowKey(source, destination, 100, 100, 3));
        }
    }
    assert(keys.size() == 192);
    assert(ns3::CongaSaturateMetric(6, 3) == 6);
    assert(ns3::CongaSaturateMetric(8, 3) == 7);
    assert(ns3::CongaSaturateMetric(1000, 3) == 7);
    return 0;
}
