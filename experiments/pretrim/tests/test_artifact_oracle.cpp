#include "artifact_cc.h"
#include "artifact_lb.h"
#include "upstream_oracle.h"
#include "upstream_buffer_reps.cpp"
#include <cassert>
using namespace pretrim;
int main(){
 ArtifactDctcp d(50,4096);ReferenceCc ref;
 // Aggregate newly-received bytes, ECN, integer rounding over varied ACK sizes.
 for(int i=0;i<1000;i++){
  uint64_t bytes=(i%5+1)*4096;bool ce=i%7==0;
  ref.updateCwndOnAck_DCTCP(ce,0,bytes);d.ack(bytes,ce);
  assert(d.window_bytes()==uint64_t(ref._cwnd));
 }
 ArtifactNscc n(4096,12000000,400,50);ReferenceCc ns;
 for(int i=0;i<2000;i++){
  uint64_t bytes=(i%3+1)*4096;bool ce=i%4==0;
  uint64_t raw=(i%60<30?5000000:45000000)+(i%9)*200000;
  uint64_t now=(i+1)*300000;int64_t flight=std::max<int64_t>(0,ns._cwnd-4096);
  ns.clock.value=now;ns._in_flight=flight;
  ns._achieved_bytes+=bytes;ns._received_bytes+=bytes;ns._bytes_ignored+=bytes;
  ns._raw_rtt=raw;ns.update_base_rtt(raw,4096);
  uint64_t delay=ns.get_avg_delay();if(raw>=ns._base_rtt){ns.update_delay(raw,true,ce);delay=raw-ns._base_rtt;}
  ns.updateCwndOnAck_NSCC(ce,delay,bytes);n.ack(bytes,ce,raw,now,flight);
  if(n.window_bytes()!=uint64_t(ns._cwnd)){
   std::cerr<<"NSCC divergence i="<<i<<" got="<<n.window_bytes()<<" expected="<<ns._cwnd<<"\n";return 1;
  }
  assert(n.average_delay_ps()==ns._avg_delay);
  if(i%113==0){
   ns.updateCwndOnNack_NSCC(false,4096);n.nack(i,0,4096,false,true,now,flight);
   assert(n.window_bytes()==uint64_t(ns._cwnd));
  }
 }
 // Compare adapter against unchanged author circular buffer in normal/frozen mode.
 CircularBufferREPS<int> b(8);ArtifactReps r(8,146,100);
 for(int i=0;i<25;i++){
  b.add(i*13);r.feedback(i*13,false,i);
  if(i%3==0){assert(b.remove_earliest_fresh()==r.next(i));}
 }
 b.setFrozenMode(true);r.timeout(30);
 for(int i=0;i<24;i++)assert(b.remove_frozen()==r.next(40+i));
 std::cout<<"upstream oracle: 1000 DCTCP ACKs, 2000 NSCC ACKs/NACKs and REPS buffer matched\n";
}
