// Included inside Experiment. Every member is passive observer state.
struct LightHop {uint64_t starts=0,retries=0,headers=0; double wait=0,pause=0,max_wait=0;};
struct LightPg {uint64_t peak=0,headroom_peak=0;int64_t margin=INT64_MAX;};
map<pair<string,string>,LightHop> light_hops;
map<pair<string,int>,LightPg> light_pgs;
map<string,map<string,uint64_t>> light_qp_counts;
map<string,double> light_last_application_us;
map<string,set<uint64_t>> light_qp_trim_psns,light_qp_repeated_trim_psns;
map<string,map<string,double>> light_qp_first_us;
uint64_t light_dispatched_events=0,light_physical_data_packets=0,light_physical_data_hops=0;
uint64_t light_unresolved_qp_counter_events=0;
map<string,deque<pair<simtime_picosec,uint64_t>>> light_control_fifo;
map<string,uint64_t> light_control_bytes,light_control_peaks;
map<string,double> light_control_wait;
void light_observe_mmu(const string&owner,int ingress,uint64_t occupancy,uint64_t xoff,uint64_t headroom){
 auto&v=light_pgs[make_pair(owner,ingress)];v.peak=max(v.peak,occupancy);v.headroom_peak=max(v.headroom_peak,headroom);v.margin=min(v.margin,(int64_t)xoff-(int64_t)occupancy);
}
void light_observe_hop(shared_ptr<SimPacket>p,const string&hop,simtime_picosec enqueued){
 auto&v=light_hops[make_pair(flows[p->flow].name,hop)];v.starts++;v.retries+=p->retransmit;v.headers+=p->header;
 double t=timeAsUs(ev.now()),begin=timeAsUs(enqueued),wait=timeAsUs(ev.now()-enqueued);v.wait+=wait;v.max_wait=max(v.max_wait,wait);
 auto add=[&](double a,double b){a=max(a,begin);b=min(b,t);if(b>a)v.pause+=b-a;};
 auto closed=paired_pause_closed.find(hop);if(closed!=paired_pause_closed.end())for(auto&i:closed->second)add(i.first,i.second);
 auto active=paired_pause_active.find(hop);if(active!=paired_pause_active.end())add(active->second.first,active->second.second);
 if(!p->header){light_physical_data_hops++;if(hop=="nic"+to_string(p->src)+"-leaf"+to_string(leaf_of_nic(p->src))){light_physical_data_packets++;light_qp_counts[flows[p->flow].name]["physical_source_data_packets"]++;if(p->retransmit){string name=flows[p->flow].name;light_qp_counts[name]["physical_source_retransmit_packets"]++;if(!light_qp_first_us[name].count("physical_source_retransmit"))light_qp_first_us[name]["physical_source_retransmit"]=t;}}}
}
static string light_text(const string&f,const string&key){string k="\""+key+"\":\"";auto a=f.find(k);if(a==string::npos)return "";a+=k.size();return f.substr(a,f.find('"',a)-a);}
static uint64_t light_number(const string&f,const string&key){string k="\""+key+"\":";auto a=f.find(k);return a==string::npos?0:stoull(f.substr(a+k.size()));}
static bool light_emit(const string&e){
 if(e.compare(0,9,"artifact_")==0)return true;
 static const set<string>keep={"ops_config","ops_entropy_select","special_xoff","pretrim_mark","common_cc_config","common_nack_cc","common_ack_cc","common_cwnd_block","config","resolved_config","hybrid_qp_config","traffic_input","host_consume_config","summary","budget_status","switch_budget","pg_budget","pause_summary","role_summary","retransmit_breakdown","jct_summary","round_start","round_complete","job_complete","flow_complete","receiver_audit_summary","nic_receiver_summary","path","PFC_RX","PFC_TX","NIC_PFC_RX","upstream_pfc_expire","leaf_pfc_expire","switch_mmu_xoff","switch_mmu_xon","switch_mmu_refresh","TRIM_FIRST","trim","admission_drop","headroom_violation","control_drop","link_down","link_up","diagnostic_data_impairment"};
 return keep.count(e)||(e.size()>=7&&e.compare(e.size()-7,7,"summary")==0)||e=="hybrid_trim_decision"||e=="switch_admission"||e=="nic_pfc_xoff"||e=="nic_pfc_xon"||e=="header_arrive";
}
static bool light_needs(const string&e){
 if(!light_summary||light_emit(e))return true;
 static const set<string>observe={"packet_path","packet_send","ack_arrive","source_packet_tx","rx_unique_admit","rx_window_overflow","nack_send","nack_arrive","port_enqueue","tx_start","packet_hop_tx_start","header_arrive","rto","retransmit","retransmit_tx","payload_deliver","duplicate_payload_arrive","nic_control_process_enqueue","nic_control_process_complete","nic_control_process_drop","hybrid_trim_decision"};
 return observe.count(e)||e=="unordered_application_available"||e=="application_available";
}
void light_observe(simtime_picosec at,const string&e,const string&f){
 if(e=="unordered_application_available"||e=="application_available")light_last_application_us[light_text(f,"flow")]=timeAsUs(at);
 static const set<string>counts={"packet_send","rx_unique_admit","payload_deliver","header_arrive","nack_send","nack_arrive","retransmit","retransmit_tx","rto","duplicate_payload_arrive","ack_arrive","trim","rx_window_overflow"};
 if(counts.count(e)){
  string flow=light_text(f,"flow");if(flow.empty()){auto it=window_packet_sequences.find(light_number(f,e=="rx_window_overflow"?"rejected_psn":"psn"));if(it==window_packet_sequences.end())it=window_packet_sequences.find(light_number(f,"rejected_psn"));if(it!=window_packet_sequences.end())flow=it->second.first;}
  if(!flow.empty()){
   light_qp_counts[flow][e]++;
   if(e=="trim"){
    uint64_t psn=light_number(f,"psn");
    if(!light_qp_trim_psns[flow].insert(psn).second){light_qp_counts[flow]["repeated_trim_packets"]++;light_qp_repeated_trim_psns[flow].insert(psn);}
    light_qp_counts[flow]["repeated_trim_psns"]=light_qp_repeated_trim_psns[flow].size();
    light_qp_counts[flow]["trim_unique_psns"]=light_qp_trim_psns[flow].size();
   }
   if(e=="rx_window_overflow")light_qp_counts[flow]["rx_window_overflow_reason_"+light_text(f,"reason")]++;
   if(e=="nack_send"||e=="nack_arrive")light_qp_counts[flow][e+"_reason_"+light_text(f,"direct_reason")]++;
   if(e=="trim"||e=="header_arrive"||e=="nack_send"||e=="nack_arrive")if(!light_qp_first_us[flow].count(e))light_qp_first_us[flow][e]=timeAsUs(at);
  }else light_unresolved_qp_counter_events++;
 }
 if(e=="port_enqueue"||e=="tx_start"){
  if(light_text(f,"priority")!="control")return;string port=light_text(f,"port");
  if(e=="port_enqueue"){uint64_t b=light_number(f,"memory_bytes");light_control_fifo[port].push_back(make_pair(at,b));light_control_bytes[port]+=b;light_control_peaks[port]=max(light_control_peaks[port],light_control_bytes[port]);}
  else if(!light_control_fifo[port].empty()){auto v=light_control_fifo[port].front();light_control_fifo[port].pop_front();light_control_bytes[port]-=v.second;light_control_wait[port]=max(light_control_wait[port],timeAsUs(at-v.first));}
 }
}
void emit_light_summary(){
 cout<<fixed<<setprecision(6)<<"{\"event\":\"light_execution_summary\",\"dispatched_events\":"<<light_dispatched_events<<",\"physical_source_data_packets\":"<<light_physical_data_packets<<",\"physical_data_hop_starts\":"<<light_physical_data_hops<<",\"unresolved_qp_counter_events\":"<<light_unresolved_qp_counter_events<<",\"sender_resource_peaks_sampled\":"<<(light_summary?"true":"false")<<",\"sender_resource_sample_interval_us\":10}\n";
 for(auto&x:light_hops){auto&v=x.second;cout<<"{\"event\":\"light_qp_link_summary\",\"flow\":\""<<x.first.first<<"\",\"link\":\""<<x.first.second<<"\",\"physical_hop_starts\":"<<v.starts<<",\"retransmit_hop_starts\":"<<v.retries<<",\"header_hop_starts\":"<<v.headers<<",\"queue_wait_sum_us\":"<<v.wait<<",\"max_packet_queue_wait_us\":"<<v.max_wait<<",\"packet_pause_wait_sum_us\":"<<v.pause<<"}\n";}
 for(auto&x:light_pgs){auto&v=x.second;cout<<"{\"event\":\"light_mmu_ingress_summary\",\"switch\":\""<<x.first.first<<"\",\"ingress\":"<<x.first.second<<",\"peak_bytes\":"<<v.peak<<",\"headroom_peak_bytes\":"<<v.headroom_peak<<",\"min_margin_bytes\":"<<v.margin<<"}\n";}
 for(auto&f:flows){cout<<"{\"event\":\"light_qp_reliable_summary\",\"flow\":\""<<f.name<<"\",\"outstanding_final_packets\":"<<f.outstanding_psns.size()<<",\"last_application_available_us\":"<<(light_last_application_us.count(f.name)?light_last_application_us[f.name]:-1);for(auto&x:light_qp_first_us[f.name])cout<<",\"first_"<<x.first<<"_us\":"<<x.second;for(auto&x:light_qp_counts[f.name])cout<<",\""<<x.first<<"\":"<<x.second;cout<<"}\n";}
 for(auto&x:light_control_peaks)cout<<"{\"event\":\"light_control_link_summary\",\"link\":\""<<x.first<<"\",\"waiting_peak_bytes\":"<<x.second<<",\"waiting_final_bytes\":"<<light_control_bytes[x.first]<<",\"max_queue_wait_us\":"<<light_control_wait[x.first]<<"}\n";
}
string light_trim_snapshot(const string&owner,const string&port){
 string out=",\"shared_free_bytes\":"+to_string(mmu_shared_capacity(owner)-mmu_shared_used(owner))+",\"ingress_occupancies\":[";bool comma=false;
 auto pgs=switch_pgs.find(owner);if(pgs!=switch_pgs.end())for(auto&x:pgs->second){if(comma)out+=",";comma=true;out+="{\"ingress\":"+to_string(x.first)+",\"bytes\":"+to_string(x.second.bytes)+"}";}
 out+="],\"egress_ingress_resident_bytes\":[";map<int,uint64_t> contributions;auto p=ports.find(port);if(p!=ports.end())for(auto&f:p->second.data)if(f.mmu_accounted)contributions[f.ingress]+=f.memory;
 comma=false;for(auto&x:contributions){if(comma)out+=",";comma=true;out+="{\"ingress\":"+to_string(x.first)+",\"bytes\":"+to_string(x.second)+"}";}return out+"]";
}
