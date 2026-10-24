#include <common_nack_cc.h>
#include <hybrid_stage_b_dispatch.h>
#include <cassert>
#include <cmath>
int main() {
 using pretrim::hybrid_stage_b_dispatch;
 for(auto proto:{"ops","reps","mprdma"}){
  auto current=hybrid_stage_b_dispatch(true,"healthy",proto,"ecmp","dcqcn",true);
  assert(current.cc=="dctcp_variant");
  auto old=hybrid_stage_b_dispatch(true,"healthy",proto,"ecmp","dcqcn",false);
  assert(old.cc==(std::string(proto)=="ops"?"dcqcn":"fixed"));
  auto bg=hybrid_stage_b_dispatch(true,"background",proto,"ecmp","dcqcn",true);
  assert(bg.protocol=="dcqcn"&&bg.lb=="ecmp"&&bg.cc=="dcqcn");
 }
 for(bool pretrim:{false,true})for(bool lh:{false,true}){
  pretrim::CommonFeedbackFlags header;header.pretrim_origin=pretrim;header.last_hop_trim=lh;
  auto feedback=header;
  assert(feedback.pretrim_origin==pretrim&&feedback.last_hop_trim==lh);
 }
 {
  pretrim::CommonNackCc cc(50,4096);
  assert(cc.nack(1,0,4096,false,true));
  assert(cc.cwnd()==49);
  assert(!cc.nack(1,0,4096,false,true));
  assert(cc.cwnd()==49);
  assert(cc.nack(1,1,4096,true,true));
  assert(cc.cwnd()==49);
  assert(cc.nack(1,2,4096,true,true));
  assert(cc.cwnd()==49);
  assert(!cc.nack(2,0,4096,false,false));
  assert(cc.cwnd()==49);
  cc.ack(4096,true);assert(cc.window_bytes()==49*4096-4096/3);
  assert(cc.pretrim_decreases()==0);
  cc.acked(1);
  assert(!cc.nack(1,3,4096,false,false));
  assert(cc.ordinary_decreases()==1);
  assert(cc.pretrim_feedback()==2);
  auto old=cc.window_bytes();cc.ack(4096,false);
  assert(cc.window_bytes()==old+4096ULL*4096/old);
 }
}
