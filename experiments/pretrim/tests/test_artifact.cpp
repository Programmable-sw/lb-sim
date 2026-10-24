#include "artifact_lb.h"
#include "artifact_cc.h"
#include <cassert>
#include <cmath>
#include <iostream>
using namespace pretrim;
int main(){
 ArtifactReps r(8,4,100); // explicit 100-ps freeze for boundary testing only
 r.feedback(101,false,0);r.feedback(202,false,0);
 assert(r.next(1)==101);assert(r.next(2)==202);assert(r.fresh()==0);
 assert(r.timeout(10));assert(r.frozen());
 assert(r.next(111)==101); // time alone must not exit freezing
 r.feedback(303,true,111);assert(!r.frozen());assert(r.explore_left()==4);
 for(int i=0;i<4;++i)assert(r.take_explore());assert(!r.take_explore());
 r.feedback(404,false,112);assert(r.next(113)==404);
 r.feedback(505,false,114);assert(r.timeout(115));
 assert(!r.timeout(120)); // duplicate timeout must not extend freeze
 ArtifactMp m;assert(!m.has_entropy());
 m.ack(11,204800,204800,204800,4096);assert(!m.has_entropy());
 m.ack(12,204800,204881,196608,4096);assert(m.entropy()==12);
 m.nack(99);assert(m.entropy()==99); // upstream processEv sees existing can_send
 m.ack(13,204800,203435,203435,4096);assert(m.entropy()==99);
 ArtifactDctcp d(50,4096);uint64_t w=d.window_bytes();
 d.ack(8192,false);assert(d.window_bytes()==w+8192*4096/w);
 w=d.window_bytes();d.ack(12288,true);assert(d.window_bytes()==w-4096);
 assert(d.nack(7,0,4096,true,true));assert(d.pretrim_decreases()==0);
 assert(!d.nack(7,0,4096,true,true));
 assert(d.nack(7,1,4096,false,true));assert(d.ordinary_decreases()==1);
 ArtifactNscc n(4096,12000000,400,50);
 assert(n.window_bytes()==204800);auto initial=n.window_bytes();
 n.ack(4096,false,5000000,5000000,196608);
 assert(n.window_bytes()>=initial);assert(n.base_rtt_ps()==5000000);
 auto before=n.window_bytes();assert(n.nack(1,0,4096,true,true,6000000,196608));
 assert(n.window_bytes()==before&&n.pretrim_decreases()==0);
 assert(n.nack(2,0,4096,false,true,7000000,196608));
 assert(n.window_bytes()<=before);assert(n.ordinary_decreases()==1);
 std::cout<<"artifact LB/CC invariants passed\n";
}
