// Included inside Experiment. All callbacks below run at actual sender feedback.
bool artifact_qp(int id)const{
 if(!artifact_lb_enabled||!unordered_enabled)return false;
 if(hybrid_qp_enabled)return flows.at(id).role!=ROLE_BACKGROUND;
 return qp_protocol(id)=="mprdma"||qp_protocol(id)=="reps"||(qp_protocol(id)=="dcqcn"&&artifact_cc_name=="nscc");
}
map<int,unique_ptr<pretrim::ArtifactReps>> artifact_reps;
map<int,pretrim::ArtifactMp> artifact_mp;
map<int,unique_ptr<pretrim::ArtifactDctcp>> artifact_dctcp;
map<int,unique_ptr<pretrim::ArtifactNscc>> artifact_nscc;
map<int,uint64_t> artifact_random_state;
map<int,uint32_t> artifact_last_entropy;
map<int,uint64_t> artifact_base_rtt;
pretrim::NativeAckEchoLedger artifact_echoes;
uint64_t artifact_cc_window(int id)const{
 return artifact_cc_name=="nscc"?artifact_nscc.at(id)->window_bytes():artifact_dctcp.at(id)->window_bytes();
}
void artifact_init(int id){
 if(!artifact_qp(id))return;
 flows[id].qp_config.cc=artifact_cc_name;
 artifact_random_state[id]=mix64(uint64_t(seed)^uint64_t(id)<<32^0x4152544946414354ULL);
 artifact_base_rtt[id]=12000000;
 artifact_reps[id].reset(new pretrim::ArtifactReps(8,600000/PAYLOAD,artifact_reps_freeze_ps));
 if(artifact_cc_name=="nscc")artifact_nscc[id].reset(new pretrim::ArtifactNscc(PAYLOAD,12000000,400,50));
 else artifact_dctcp[id].reset(new pretrim::ArtifactDctcp(50,PAYLOAD));
}
uint32_t artifact_random_entropy(int id){
 auto&state=artifact_random_state[id];state+=0x9e3779b97f4a7c15ULL;
 uint64_t z=state;z=(z^(z>>30))*0xbf58476d1ce4e5b9ULL;
 z=(z^(z>>27))*0x94d049bb133111ebULL;return (z^(z>>31))%65535;
}
int artifact_select(FlowState*f,uint64_t seq,bool retransmission,string&source){
 int id=f-&flows[0],entropy=-1;
 if(qp_protocol(id)=="reps")entropy=artifact_reps[id]->next(ev.now());
 else entropy=artifact_mp[id].next();
 bool reused=entropy>=0;if(!reused)entropy=artifact_random_entropy(id);
 artifact_last_entropy[id]=entropy;
 uint32_t salt=pretrim::ops_freebsd_hash(seed,f->src/16,0x4f505331u);
 int path=pretrim::ops_freebsd_hash(id,entropy,salt)%spine_count;
 source=qp_protocol(id)=="reps"?(reused?"artifact_reps_reuse":"artifact_reps_explore"):(reused?"artifact_mp_ack_entropy":"artifact_mp_random");
 LIGHT_TRACE("artifact_entropy_select","\"flow\":\""+f->name+"\",\"seq\":"+to_string(seq)+",\"entropy\":"+to_string(entropy)+",\"path\":"+to_string(path)+",\"reuse\":"+(reused?"true":"false")+",\"frozen\":"+(artifact_reps[id]->frozen()?"true":"false")+",\"fresh\":"+to_string(artifact_reps[id]->fresh())+",\"retransmit\":"+(retransmission?"true":"false"));
 return route_usable(path);
}
void artifact_ack_feedback(shared_ptr<SimPacket>p,const vector<shared_ptr<SimPacket>>&confirmed){
 int id=p->flow;auto&f=flows[id];uint64_t old=artifact_cc_window(id),bytes=confirmed.size()*PAYLOAD;
 uint64_t flight=f.outstanding_bytes>=bytes?f.outstanding_bytes-bytes:0;
 bool echo=artifact_echoes.first_payload_echo(p->psn,true);
 uint64_t raw=ev.now()-p->tx_time_ps;
 if(raw&&raw<artifact_base_rtt[id]){
  artifact_base_rtt[id]=raw;
  artifact_reps[id]->set_bdp_packets(uint64_t(double(raw)*400/8000.)/PAYLOAD);
 }
 // One received ACK updates CC once with all newly confirmed bytes and its CE.
 // Supplementary SACK bits do not fabricate independent path echoes.
 if(bytes||echo){
  if(artifact_cc_name=="nscc")artifact_nscc[id]->ack(bytes,p->ecn,raw,ev.now(),flight);
  else artifact_dctcp[id]->ack(bytes,p->ecn);
 }
 for(auto&a:confirmed){if(artifact_cc_name=="nscc")artifact_nscc[id]->acked(a->psn);else artifact_dctcp[id]->acked(a->psn);}
 if(echo){
  if(qp_protocol(id)=="mprdma")artifact_mp[id].ack(p->entropy,old,artifact_cc_window(id),flight,PAYLOAD);
  if(qp_protocol(id)=="reps")artifact_reps[id]->feedback(p->entropy,p->ecn,ev.now());
 }
 LIGHT_TRACE("artifact_ack_feedback","\"flow\":\""+f.name+"\",\"psn\":"+to_string(p->psn)+",\"entropy\":"+to_string(p->entropy)+",\"newly_acked_bytes\":"+to_string(bytes)+",\"ce\":"+(p->ecn?"true":"false")+",\"old_cwnd_bytes\":"+to_string(old)+",\"new_cwnd_bytes\":"+to_string(artifact_cc_window(id))+",\"flight_after_ack_bytes\":"+to_string(flight)+",\"rtt_us\":"+to_string(timeAsUs(raw))+",\"native_echo\":"+(echo?"true":"false"));
}
void artifact_nack_feedback(shared_ptr<SimPacket>p){
 int id=p->flow;auto&f=flows[id];uint64_t old=artifact_cc_window(id);
 bool current=pretrim::nack_can_retire_attempt(f.outstanding_psns.count(p->psn),p->attempt,latest_retrans_attempt[p->psn]);
 bool accepted=artifact_cc_name=="nscc"?artifact_nscc[id]->nack(p->psn,p->attempt,PAYLOAD,p->pretrim_origin,current,ev.now(),f.outstanding_bytes):artifact_dctcp[id]->nack(p->psn,p->attempt,PAYLOAD,p->pretrim_origin,current);
 if(accepted&&qp_protocol(id)=="mprdma")artifact_mp[id].nack(p->entropy);
 // processNack dispatches PATH_NACK too: it can exit an expired freeze,
 // but cannot insert a good entropy or enter freezing. PRETRIM is not loss CC.
 if(accepted&&qp_protocol(id)=="reps")artifact_reps[id]->feedback(p->entropy,true,ev.now());
 // Official freezing is entered by RTO, not by a trim/missing NACK.
 LIGHT_TRACE("artifact_nack_feedback","\"flow\":\""+f.name+"\",\"psn\":"+to_string(p->psn)+",\"attempt\":"+to_string(p->attempt)+",\"pretrim\":"+(p->pretrim_origin?"true":"false")+",\"accepted\":"+(accepted?"true":"false")+",\"old_cwnd_bytes\":"+to_string(old)+",\"new_cwnd_bytes\":"+to_string(artifact_cc_window(id)));
}
void artifact_timeout(int id){
 if(artifact_cc_name=="nscc")artifact_nscc[id]->loss_timeout();else artifact_dctcp[id]->loss_timeout();
 if(qp_protocol(id)=="reps"){
  bool entered=artifact_reps[id]->timeout(ev.now());
  LIGHT_TRACE("artifact_reps_freezing","\"flow\":\""+flows[id].name+"\",\"entered\":"+(entered?"true":"false")+",\"retained\":"+to_string(artifact_reps[id]->retained()));
 }
}
void artifact_emit_summary(){
 if(!artifact_lb_enabled)return;
 for(size_t id=0;id<flows.size();++id)if(artifact_qp(id)){
  LIGHT_TRACE("artifact_qp_summary","\"flow\":\""+flows[id].name+"\",\"cc\":\""+artifact_cc_name+"\",\"cwnd_final_bytes\":"+to_string(artifact_cc_window(id))+",\"reps_cache_reuse\":"+to_string(artifact_reps[id]->reuse)+",\"reps_exploration\":"+to_string(artifact_reps[id]->random)+",\"reps_freeze_entries\":"+to_string(artifact_reps[id]->freeze_entries)+",\"reps_freeze_exits\":"+to_string(artifact_reps[id]->freeze_exits)+",\"mp_echo_accepted\":"+to_string(artifact_mp[id].accepted)+",\"mp_echo_pruned\":"+to_string(artifact_mp[id].pruned));
 }
}
