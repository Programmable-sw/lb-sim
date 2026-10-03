#include "../../src/point-to-point/model/paper-lb-state.h"
#include <cassert>
int main() {
    ns3::PaperReps r;
    assert(r.Select(7, 0) == 7);
    r.Ack(42, false, 1, 8);
    r.Ack(99, true, 1, 8);
    assert(r.Select(7, 2) == 42);
    assert(r.Select(7, 3) == 7);
    r.Fail(4);
    assert(r.Select(7, 5) == 42);
    r.Ack(43, false, 5, 8);
    assert(r.frozen);
    r.Ack(44, false, 100005, 8);
    assert(!r.frozen && r.explore == 8);
    ns3::PaperMpr m;
    m.Ack(12, 100, false, 32);
    assert(m.Select(5) == 12);
    assert(m.Select(5) == 12);
    assert(m.Select(5) == 5);
    m.Ack(17, 1, false, 32);
    assert(m.Select(5) == 5);
    m.Ack(17, 1, true, 32);
    assert(m.Select(5) == 17);
}
