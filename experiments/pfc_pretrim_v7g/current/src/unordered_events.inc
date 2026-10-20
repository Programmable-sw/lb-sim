 // This include is inside Experiment. No network feedback is delivered locally.
 map<int,pfc_pretrim_v7::QpRetransmissionTimer> unordered_timers;
 map<int,uint64_t> unordered_timer_generation,unordered_timer_at;
 map<uint64_t,shared_ptr<SimPacket>> unordered_last_tx;
 map<int,unique_ptr<pfc_pretrim_v7::UecDctcpState>> unordered_reps_cc;
 map<int,set<uint64_t>> unordered_consumed;
 map<int,uint64_t> unordered_app_next;
 pfc_pretrim_v7::NativeAckEchoLedger unordered_native_echoes;
 set<int> unordered_descriptor_waiters;
 uint64_t unordered_descriptor_peak=0,unordered_descriptor_blocks=0;
 const uint64_t unordered_nic_descriptors=16384;
 bool unordered_test_data_seen=false;
 bool unordered_diagnostic_data(shared_ptr<SimPacket>p){
  if(unordered_test_fault=="none"||unordered_test_data_seen||p->seq!=0||p->attempt!=0)return false;
  if(unordered_test_fault!="payload-loss"&&unordered_test_fault!="late-first")return false;
  unordered_test_data_seen=true;
  LIGHT_TRACE("diagnostic_data_impairment","\"psn\":"+to_string(p->psn)+",\"mode\":\""+unordered_test_fault+"\"");
  if(unordered_test_fault=="late-first")schedule(ev.now()+us(100),[this,p](){deliver_host(p);});
  return true;
 }

 uint64_t unordered_nic_outstanding(int nic) const {
  uint64_t n=0;for(const auto&f:flows)if(f.src==nic)n+=f.outstanding_psns.size();return n;
 }
 bool unordered_sender_ready(FlowState*f){
  uint64_t n=unordered_nic_outstanding(f->src);
  unordered_descriptor_peak=max(unordered_descriptor_peak,n);
  if(n>=unordered_nic_descriptors){unordered_descriptor_waiters.insert(f->src);unordered_descriptor_blocks++;LIGHT_TRACE("unordered_descriptor_block","\"flow\":\""+f->name+"\",\"used\":"+to_string(n));return false;}
  return true;
 }
 void unordered_consume(shared_ptr<SimPacket>p){
  if(!unordered_enabled)return;
  auto&done=unordered_consumed[p->flow];done.insert(p->seq);
  auto&next=unordered_app_next[p->flow];uint64_t n=0;
  while(done.erase(next)){++next;++n;}
  application_available+=n*PAYLOAD;
  if(n)LIGHT_TRACE("application_available","\"flow\":\""+flows[p->flow].name+"\",\"through_seq\":"+to_string(next-1)+",\"new_bytes\":"+to_string(n*PAYLOAD)+",\"requires_host_consumed\":true");
 }
 void unordered_schedule_timer(int id){
  auto&t=unordered_timers[id];uint64_t at=t.deadline();
  if(t.empty()){unordered_timer_at.erase(id);++unordered_timer_generation[id];return;}
  if(unordered_timer_at.count(id)&&unordered_timer_at[id]==at)return;
  unordered_timer_at[id]=at;uint64_t gen=++unordered_timer_generation[id];
  schedule(max(ev.now(),(simtime_picosec)at),[this,id,gen](){
   if(unordered_timer_generation[id]!=gen)return;
   unordered_timer_at.erase(id);
   auto due=unordered_timers[id].expired(ev.now());
   for(const auto&d:due){
    auto&f=flows[id];if(!f.outstanding_psns.count(d.psn)||latest_retrans_attempt[d.psn]!=d.attempt)continue;
    auto it=unordered_last_tx.find(d.psn);if(it==unordered_last_tx.end())continue;
    rto_events++;role_rto[flow_role_name(f.role)]++;retrans_inflight.erase(d.psn);
    LIGHT_TRACE("rto","\"psn\":"+to_string(d.psn)+",\"attempt\":"+to_string(d.attempt)+",\"generation\":"+to_string(gen)+",\"flow_role\":\""+flow_role_name(f.role)+"\",\"timer_scope\":\"qp\"");
    if(qp_protocol(id)=="reps"){if(common_cc(id))common_cc_states[id]->loss_timeout();else unordered_reps_cc[id]->loss();reps_states[id]->note_failure_detection((uint64_t)timeAsUs(ev.now()));}
    request_retransmit(it->second,"rto");
   }
   unordered_schedule_timer(id);
  });
 }
 void unordered_arm_rto(shared_ptr<SimPacket>p){
  if(!flows[p->flow].outstanding_psns.count(p->psn)){
   LIGHT_TRACE("unordered_timer_skip_confirmed_tx","\"psn\":"+to_string(p->psn)+",\"attempt\":"+to_string(p->attempt));return;
  }
  auto old=unordered_last_tx.find(p->psn);
  if(old!=unordered_last_tx.end()&&old->second->attempt>p->attempt)return;
  unordered_last_tx[p->psn]=make_shared<SimPacket>(*p);
  unordered_timers[p->flow].sent(p->psn,p->attempt,ev.now(),ev.now()+us(1000));
  LIGHT_TRACE("unordered_rto_arm","\"psn\":"+to_string(p->psn)+",\"attempt\":"+to_string(p->attempt)+",\"deadline_us\":"+to_string(timeAsUs(ev.now()+us(1000))));
  unordered_schedule_timer(p->flow);
 }
 void unordered_refresh_nack(shared_ptr<SimPacket>p){
  uint64_t gen=++nack_epoch[p->psn];
  schedule(ev.now()+us(50),[this,p,gen](){
   if(nack_epoch[p->psn]!=gen)return;
   auto&w=*rx_windows[p->flow];
   bool received=pfc_pretrim_v7::sack_confirms(p->seq,w.next_expected(),(p->seq/64)*64,w.sack((p->seq/64)*64));
   if(!received){LIGHT_TRACE("nack_refresh","\"psn\":"+to_string(p->psn)+",\"receiver_local_only\":true");send_nack(p);unordered_refresh_nack(p);}
  });
 }
 void unordered_overflow(shared_ptr<SimPacket>p,uint64_t missing,pfc_pretrim_v7::RxResult result){
  // Receiver uses only local packet/window state. Translate missing sequence to
  // sender PSN only after the actual NACK arrives at that sender.
  reorder_overflows++;string reason=result==pfc_pretrim_v7::RxResult::DISTANCE_OVERFLOW?"psn_distance_overflow":(result==pfc_pretrim_v7::RxResult::RESOURCE_OVERFLOW?"shared_rx_resource_exhaustion":"nic_rx_capacity_exhaustion");
  string role=flow_role_name(flows[p->flow].role);
  if(result==pfc_pretrim_v7::RxResult::DISTANCE_OVERFLOW)role_distance_overflow[role]++;else if(result==pfc_pretrim_v7::RxResult::RESOURCE_OVERFLOW)role_resource_overflow[role]++;else role_capacity_overflow[role]++;
  auto feedback=make_shared<SimPacket>(*p);feedback->feedback_reason=reason;
  feedback->rejected_psn=p->psn;feedback->rejected_seq=p->seq;feedback->rejected_attempt=p->attempt;
  feedback->missing_seq=missing;feedback->psn=0;feedback->seq=missing;feedback->attempt=0;
  LIGHT_TRACE("rx_window_overflow","\"rejected_psn\":"+to_string(p->psn)+",\"rejected_seq\":"+to_string(p->seq)+",\"missing_seq\":"+to_string(missing)+",\"reason\":\""+(result==pfc_pretrim_v7::RxResult::DISTANCE_OVERFLOW?"distance":(result==pfc_pretrim_v7::RxResult::RESOURCE_OVERFLOW?"resource":"capacity"))+"\",\"lookup_at_sender\":true");
  if(rejected_feedback_deduper.should_send(missing,reason,p->psn,p->attempt))send_nack(feedback);
 }
 bool unordered_translate_nack(shared_ptr<SimPacket>p){
  if(p->psn)return true;
  auto it=original_packets.find(make_pair(p->flow,p->missing_seq));
  if(it==original_packets.end())return false;
  p->psn=it->second->psn;p->seq=p->missing_seq;
  // A gap report does not claim rejection of the missing packet's newer attempt.
  p->attempt=0;return true;
 }
 void unordered_ack_arrive(shared_ptr<SimPacket>p){
  if(p->header){LIGHT_TRACE("unordered_invalid_header_ack","\"psn\":"+to_string(p->psn));return;}
  FlowState&f=flows[p->flow];vector<shared_ptr<SimPacket>> confirmed;
  for(auto psn:f.outstanding_psns){
   auto tx=unordered_last_tx.find(psn);
   // Confirmation can only refer to physically transmitted PSNs, not queued WQEs.
   if(tx==unordered_last_tx.end())continue;
   auto original=tx->second;
   if(psn!=p->psn&&!pfc_pretrim_v7::sack_confirms(original->seq,p->cumulative_ack,p->sack_base,p->sack_mask))continue;
   auto a=make_shared<SimPacket>(*original);a->header=false;
   a->cumulative_ack=p->cumulative_ack;a->rx_unique=true;
   // Supplementary SACK bits carry no native path/CC/ACK-clock credit.
   a->ecn=psn==p->psn?p->ecn:false;
   if(psn==p->psn){a->spine=p->spine;a->retransmit=p->retransmit;}
   confirmed.push_back(a);
  }
  LIGHT_TRACE("unordered_sack_arrive","\"flow\":\""+f.name+"\",\"cumulative_ack\":"+to_string(p->cumulative_ack)+",\"sack_base\":"+to_string(p->sack_base)+",\"sack_mask\":"+to_string(p->sack_mask)+",\"new_confirmations\":"+to_string(confirmed.size()));
  for(auto&a:confirmed){if(common_cc(p->flow)){
 auto&cc=*common_cc_states[p->flow];uint64_t before=cc.window_bytes();cc.ack(PAYLOAD,a->ecn);cc.acked(a->psn);
 LIGHT_TRACE("common_ack_cc","\"flow\":\""+f.name+"\",\"psn\":"+to_string(a->psn)+",\"ce\":"+(a->ecn?"true":"false")+",\"old_cwnd_bytes\":"+to_string(before)+",\"new_cwnd_bytes\":"+to_string(cc.window_bytes()));
 if(qp_protocol(p->flow)=="mprdma")mprdma_states[p->flow]->use_external_cwnd(cc.cwnd());
 }unordered_timers[p->flow].ack(a->psn);ack_arrive_one(a);unordered_last_tx.erase(a->psn);}
  if(unordered_native_echoes.first_payload_echo(p->psn,true)){
   if(qp_protocol(p->flow)=="mprdma"){
    auto&mp=*mprdma_states[p->flow];double old=mp.cwnd();bool was=mp.recovery_active();
    mp.note_data_ack(p->seq,p->ecn,p->spine,p->retransmit,p->cumulative_ack,!common_cc(p->flow));
    LIGHT_TRACE("mprdma_ack_update","\"psn\":"+to_string(p->psn)+",\"old_cwnd_packets\":"+to_string(old)+",\"new_cwnd_packets\":"+to_string(mp.cwnd())+",\"available_window_packets\":"+to_string(mp.available_window())+",\"ack_credit\":"+to_string(mp.ack_credit())+",\"snd_una\":"+to_string(mp.snd_una())+",\"snd_nxt\":"+to_string(mp.snd_nxt())+",\"path\":"+to_string(p->spine)+",\"real_payload_echo_only\":true");
    if(was&&!mp.recovery_active())LIGHT_TRACE("mprdma_recovery_exit","\"flow\":\""+f.name+"\"");
    if(mp.recovery_active()){uint64_t seq;if(mp.next_retransmission(seq)){auto it=original_packets.find(make_pair(p->flow,seq));if(it!=original_packets.end())request_retransmit(it->second,"mprdma_ack_clock_recovery");}}
    if(f.next_packet<f.packets)schedule(ev.now(),[this,&f](){send_next(&f,0);});
   }else if(qp_protocol(p->flow)=="reps"){
    if(common_cc(p->flow))reps_states[p->flow]->note_data_ack(p->spine,p->ecn,(uint64_t)timeAsUs(ev.now()));else unordered_reps_ack(p);
    if(f.next_packet<f.packets)schedule(ev.now(),[this,&f](){send_next(&f,0);});
   }
  }
  if(qp_protocol(p->flow)=="mprdma"){
   auto&mp=*mprdma_states[p->flow];bool was=mp.recovery_active();uint64_t una=mp.snd_una();
   mp.note_cumulative_progress(p->cumulative_ack);
   if(una!=mp.snd_una()||was!=mp.recovery_active()){
    LIGHT_TRACE("unordered_mp_cumulative_progress","\"flow\":\""+f.name+"\",\"snd_una\":"+to_string(mp.snd_una())+",\"recovery_active\":"+(mp.recovery_active()?"true":"false")+",\"extra_credit\":0");
    if(f.next_packet<f.packets)schedule(ev.now(),[this,&f](){send_next(&f,0);});
   }
  }
  finite_rx_ack_arrive(p);
  unordered_schedule_timer(p->flow);
  // A finite shared NIC descriptor release can also unblock a different QP.
  if(unordered_descriptor_waiters.erase(f.src))for(auto&other:flows)if(other.src==f.src&&other.next_packet<other.packets)schedule(ev.now(),[this,&other](){send_next(&other,0);});
 }
 void unordered_reps_ack(shared_ptr<SimPacket>p){
  auto&cc=*unordered_reps_cc[p->flow];double before=cc.cwnd();cc.ack(1,p->ecn);
  reps_states[p->flow]->note_data_ack(p->spine,p->ecn,(uint64_t)timeAsUs(ev.now()));
  LIGHT_TRACE("unordered_reps_cc","\"flow\":\""+flows[p->flow].name+"\",\"old_cwnd\":"+to_string(before)+",\"new_cwnd\":"+to_string(cc.cwnd())+",\"ce\":"+(p->ecn?"true":"false"));
 }
 void emit_unordered_summary(){
  if(!unordered_enabled)return;uint64_t timers=0;
  for(auto&t:unordered_timers)timers+=t.second.size();
  LIGHT_TRACE("unordered_summary","\"receive_range_psn\":16384,\"sack_bits\":64,\"nic_send_descriptors\":16384,\"descriptor_peak\":"+to_string(unordered_descriptor_peak)+",\"descriptor_blocks\":"+to_string(unordered_descriptor_blocks)+",\"timer_records_final\":"+to_string(timers)+",\"last_tx_records_final\":"+to_string(unordered_last_tx.size())+",\"rto_us\":1000,\"application_requires_host_consume\":true");
 }
