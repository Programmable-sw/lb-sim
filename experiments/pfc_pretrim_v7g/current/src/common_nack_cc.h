#ifndef V7G_COMMON_NACK_CC_H
#define V7G_COMMON_NACK_CC_H
#include <algorithm>
#include <cstdint>
#include <set>
#include <utility>
namespace pfc_pretrim_v7 {
struct CommonFeedbackFlags {bool pretrim_origin=false,last_hop_trim=false;};
// REPS artifact b7515887, uec.cpp updateCwndOnAck_DCTCP / OnNack_DCTCP.
// Logical MTU is payload bytes in this model, not Ethernet wire length.
class CommonNackCc {
 uint64_t bytes_,mtu_,ordinary_=0,pretrim_=0;
 std::set<std::pair<uint64_t,uint64_t>> feedback_;
public:
 CommonNackCc(uint64_t initial_packets,uint64_t mtu):bytes_(initial_packets*mtu),mtu_(mtu) {}
 void ack(uint64_t payload_bytes,bool ce) {
  if(ce) bytes_=payload_bytes/3>=bytes_?mtu_:std::max(mtu_,bytes_-payload_bytes/3);
  else bytes_+=payload_bytes*mtu_/bytes_;
 }
 bool nack(uint64_t psn,uint64_t attempt,uint64_t payload_bytes,bool pretrim,bool current) {
  if(!current||!feedback_.insert({psn,attempt}).second)return false;
  if(pretrim){++pretrim_;return true;}
  ++ordinary_;
  bytes_=payload_bytes>=bytes_?mtu_:std::max(mtu_,bytes_-payload_bytes);
  return true;
 }
 void acked(uint64_t psn) {
  auto it=feedback_.lower_bound({psn,0});
  while(it!=feedback_.end()&&it->first==psn)it=feedback_.erase(it);
 }
 void loss_timeout(){bytes_=bytes_<=mtu_?mtu_:std::max(mtu_,bytes_-mtu_);}
 uint64_t window_bytes() const{return bytes_;}
 double cwnd() const{return double(bytes_)/mtu_;}
 uint64_t ordinary_decreases()const{return ordinary_;}
 uint64_t pretrim_feedback()const{return pretrim_;}
 uint64_t pretrim_decreases()const{return 0;}
};
}
#endif
