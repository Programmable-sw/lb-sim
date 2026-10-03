#include "ns3/settings.h"
#include <cassert>
int main() {
    ns3::PaperLbTag data;
    data.seq=65536; data.originalPort=10000; data.ev=60001;
    data.lb=14; data.ecn=1; data.retransmitted=1;
    ns3::Ptr<ns3::Packet> packet=ns3::Create<ns3::Packet>(100);
    packet->AddPacketTag(data);
    ns3::PaperLbTag echoed;
    assert(packet->Copy()->PeekPacketTag(echoed));
    assert(echoed.seq==data.seq && echoed.ev==data.ev);
    assert(echoed.originalPort==10000 && echoed.lb==14);
    assert(echoed.ecn==1 && echoed.retransmitted==1);
    assert(packet->GetSize()==100); // Tags are simulation metadata, not wire bytes.
}
