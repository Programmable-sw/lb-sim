#ifndef PRETRIM_ARTIFACT_CC_H
#define PRETRIM_ARTIFACT_CC_H
// REPS_EuroSys_Artifact b7515887, uec.cpp DCTCP and NSCC branches.
// Copyright (c) 2025 Ultra Ethernet Consortium. BSD-2-Clause, see upstream/LICENSE.
#include "common_nack_cc.h"
#include <cmath>
namespace pretrim {
// Same byte/integer update as upstream DCTCP; ACK batching is at the caller.
// PRETRIM exemption and attempt deduplication are local transport extensions.
using ArtifactDctcp=CommonNackCc;
class ArtifactNscc {
 using Bytes=int64_t;using Time=uint64_t;
 Bytes cwnd_,mtu_,maxwnd_,bdp_,achieved_=0,received_=0;
 uint32_t fi_count_=0,ignore_=0,ignored_=0,inc_=0;
 Time base_,avg_=0,qa_end_=0,last_eta_=0,last_adjust_=0,last_dec_=0;
 bool trigger_=false,increase_=false;
 double a_,alpha_,fi_,eta_,gbps_;uint32_t adjust_bytes_=8000;
 uint64_t ordinary_=0,pretrim_=0;
 std::set<std::pair<uint64_t,uint64_t>> feedback_;
 static constexpr Time target_=6000000,period_=12000000;
 bool quick_adapt(bool loss,Time now,Bytes flight){
  if(now>qa_end_){
   if(qa_end_&&(trigger_||loss||avg_>4*target_)&&cwnd_>=achieved_){
    cwnd_=std::max(achieved_,mtu_);ignore_=flight;ignored_=0;trigger_=false;return true;
   }
   achieved_=0;qa_end_=now+base_+target_;
  }return false;
 }
 void fast_increase(uint32_t bytes,Time delay){
  if(delay<1000000){fi_count_+=bytes;if(fi_count_>cwnd_||increase_){
   cwnd_=std::min<Bytes>(maxwnd_,cwnd_+bytes*.5);increase_=true;return;
  }}else fi_count_=0;increase_=false;
 }
public:
 ArtifactNscc(uint64_t mtu,Time network_rtt_ps,double gbps,uint64_t initial_packets)
  :cwnd_(initial_packets*mtu),mtu_(mtu),base_(network_rtt_ps),gbps_(gbps){
  bdp_=Bytes(double(base_)*gbps/8000.0);maxwnd_=1.5*bdp_;
  a_=double(bdp_)/150000.;alpha_=4*a_*.5*mtu_/target_;
  fi_=5*a_;eta_=.15*mtu_*a_;if(!initial_packets)cwnd_=maxwnd_;
 }
 uint64_t window_bytes()const{return cwnd_;}
 double cwnd()const{return double(cwnd_)/mtu_;}
 Time base_rtt_ps()const{return base_;}
 Time average_delay_ps()const{return avg_;}
 void ack(uint64_t bytes,bool ce,Time raw_rtt,Time now,Bytes flight){
  // processAck: update counters and RTT/base RTT before updateCwndOnAck_NSCC.
  achieved_+=bytes;received_+=bytes;ignored_+=bytes;
  if(raw_rtt&&raw_rtt<base_){base_=raw_rtt;bdp_=Bytes(double(base_)*gbps_/8000.);maxwnd_=1.5*bdp_;}
  Time delay=raw_rtt>=base_?raw_rtt-base_:avg_;
  if(raw_rtt>=base_){
   if(!ce&&delay>target_)avg_=.0125*base_*.25+.9875*avg_;
   else avg_=.0125*delay+.9875*avg_;
  }
  if(ignored_<ignore_&&ce)return;
  if(quick_adapt(false,now,flight))return;
  if(!ce&&delay>=target_)inc_+=fi_*mtu_*bytes;
  else if(!ce){
   fast_increase(bytes,delay);
   if(!increase_){inc_+=uint32_t(std::round(alpha_*(target_-delay)*double(bytes)));inc_+=fi_*mtu_*bytes;}
  }else if(delay>=target_){
   increase_=false;fi_count_=0;
   if(avg_>target_&&now-last_dec_>base_){cwnd_*=std::max(1-.8*(avg_-target_)/avg_,.5);last_dec_=now;}
  }
  if(received_>adjust_bytes_||now-last_adjust_>period_){
   cwnd_+=std::min<Bytes>(adjust_bytes_,inc_/cwnd_);inc_=0;received_=0;last_adjust_=now;
  }
  if(now-last_eta_>period_){cwnd_+=eta_;last_eta_=now;}
  cwnd_=std::max(mtu_,std::min(maxwnd_,cwnd_));
 }
 bool nack(uint64_t psn,uint64_t attempt,uint64_t bytes,bool pretrim,bool current,Time now,Bytes flight){
  if(!current||!feedback_.insert({psn,attempt}).second)return false;
  if(pretrim){++pretrim_;return true;}
  ++ordinary_;cwnd_=std::max(mtu_,cwnd_-Bytes(bytes));trigger_=true;
  if(ignored_>=ignore_)quick_adapt(true,now,flight);return true;
 }
 void acked(uint64_t psn){auto it=feedback_.lower_bound({psn,0});while(it!=feedback_.end()&&it->first==psn)it=feedback_.erase(it);}
 void loss_timeout(){cwnd_=std::max(mtu_,cwnd_-mtu_);}
 uint64_t ordinary_decreases()const{return ordinary_;}
 uint64_t pretrim_decreases()const{return 0;}
};
}
#endif
