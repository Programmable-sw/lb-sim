#ifndef PRETRIM_ARTIFACT_LB_H
#define PRETRIM_ARTIFACT_LB_H
// Adapted from REPS_EuroSys_Artifact b7515887, uec.cpp and buffer_reps.cpp.
// Copyright (c) 2025 Ultra Ethernet Consortium. BSD-2-Clause, see upstream/LICENSE.
#include <algorithm>
#include <cstdint>
#include <vector>
namespace pretrim {
// REPS FREEZING branch. Values are UDP entropy, never physical Spine indices.
class ArtifactReps {
 struct Slot {int entropy=0;bool valid=false;};
 std::vector<Slot> slots_;size_t head_=0,count_=0,fresh_=0,frozen_head_=0;
 bool frozen_=false;uint64_t exit_at_=0,duration_,bdp_packets_,explore_=0;
public:
 uint64_t reuse=0,random=0,freeze_entries=0,freeze_exits=0;
 ArtifactReps(size_t size,uint64_t bdp_packets,uint64_t freeze_ps=10000000000ULL)
  :slots_(std::max<size_t>(1,size)),duration_(freeze_ps),bdp_packets_(bdp_packets){}
 void feedback(int entropy,bool ce,uint64_t now){
  // Upstream exits in processEv_freezing(), not nextEntropy_freezing().
  if(frozen_&&now>exit_at_){
   frozen_=false;++freeze_exits;head_=count_=fresh_=frozen_head_=0;
   for(auto&s:slots_)s.valid=false;explore_=bdp_packets_;
  }
  if(ce)return;
  auto&s=slots_[head_];if(!s.valid)++fresh_;
  s.entropy=entropy;s.valid=true;head_=(head_+1)%slots_.size();
  count_=std::min(count_+1,slots_.size());fresh_=std::min(fresh_,slots_.size());
 }
 bool take_explore(){if(!explore_)return false;--explore_;return true;}
 int next(uint64_t){
  if(take_explore()){++random;return -1;}
  if(!count_||(!frozen_&&!fresh_)){++random;return -1;}
  size_t at=frozen_?frozen_head_:(head_+slots_.size()-fresh_)%slots_.size();
  auto&s=slots_[at];int entropy=s.entropy;
  if(s.valid)--fresh_;s.valid=false;
  if(frozen_)frozen_head_=(frozen_head_+1)%count_;
  ++reuse;return entropy;
 }
 bool timeout(uint64_t now){
  if(frozen_||explore_)return false;
  frozen_=true;exit_at_=now+duration_;++freeze_entries;return true;
 }
 bool frozen()const{return frozen_;}
 size_t fresh()const{return fresh_;}
 size_t retained()const{return count_;}
 uint64_t explore_left()const{return explore_;}
 void set_bdp_packets(uint64_t packets){bdp_packets_=packets;}
};
// Author artifact MP-RDMA LB: cwnd-qualified ACK echo, no extra FIFO credit.
// This is distinct from the earlier paper-oriented MpRdmaState adapter.
class ArtifactMp {
 int entropy_=-1;bool can_send_=false;
public:
 uint64_t accepted=0,pruned=0;
 void ack(int entropy,int64_t old_cwnd,int64_t new_cwnd,int64_t flight,int64_t mtu){
  can_send_=new_cwnd-old_cwnd>mtu-66||new_cwnd>flight+(mtu-66);
  if(can_send_){entropy_=entropy;++accepted;}else ++pruned;
 }
 void nack(int entropy){if(can_send_)entropy_=entropy;}
 bool has_entropy()const{return entropy_>=0;}
 int entropy()const{return entropy_;}
 int next()const{return can_send_?entropy_:-1;}
};
}
#endif
