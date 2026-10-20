// Passive observations only: no scheduling, no updates to protocol state.
map<string,uint64_t> cap_resource_peaks;
simtime_picosec cap_resource_next_sample = 0;
void observe_sender_cap_resources(simtime_picosec at, const string& event) {
 static const set<string> watched = {"packet_send","flow_progress","retransmit",
  "retransmit_tx","nack_attempt_guard","sender_attempt_rejected",
  "retransmit_cancelled","rto","rto_arm","port_enqueue","tx_start",
  "flow_complete","summary","budget_status"};
 if (!watched.count(event)) return;
 if(light_summary&&at<cap_resource_next_sample&&event!="budget_status")return;
 map<string,uint64_t> current;
 for (const auto& f : flows) {
  current["qp_max_outstanding_packets"] = max<uint64_t>(current["qp_max_outstanding_packets"], f.outstanding_psns.size());
  current["total_outstanding_packets"] += f.outstanding_psns.size();
 }
 current["qp_max_outstanding_payload_bytes"] = current["qp_max_outstanding_packets"] * PAYLOAD;
 current["total_outstanding_payload_bytes"] = current["total_outstanding_packets"] * PAYLOAD;
 current["original_packet_records"] = original_packets.size();
 current["pending_retransmission_records"] = retrans_pending.size();
 current["inflight_retransmission_records"] = retrans_inflight.size();
 current["rto_generation_records"] = rto_epoch.size();
 current["retransmission_generation_records"] = retrans_epoch.size();
 current["latest_retransmission_attempt_records"] = latest_retrans_attempt.size();
 for (const auto& p : ports) {
  if (p.first.find("nic") != 0) continue;
  current["nic_data_queued_frames"] += p.second.data.size();
  current["one_nic_max_data_queued_frames"] = max<uint64_t>(current["one_nic_max_data_queued_frames"], p.second.data.size());
 }
 uint64_t rejected = 0;
 for (const auto& r : finite_rx_recovery) rejected += r.second->pending();
 current["finite_rx_rejected_records"] = rejected;
 for (const auto& item : current) cap_resource_peaks[item.first] = max(cap_resource_peaks[item.first], item.second);
 bool final = event == "budget_status";
 if (at < cap_resource_next_sample && !final) return;
 cap_resource_next_sample = at + timeFromNs(10000);
 cout << fixed << setprecision(6) << "{\"time_us\":" << timeAsUs(at)
      << ",\"event\":\"sender_cap_resource_sample\",\"peaks_sampled\":" << (light_summary?"true":"false") << ",\"final\":" << (final ? "true" : "false")
      << ",\"common_cap_packets\":" << diagnostic_sender_window_psn
      << ",\"common_cap_enabled\":" << (diagnostic_sender_window_psn ? "true" : "false")
      << ",\"simpacket_sizeof_bytes\":" << sizeof(SimPacket)
      << ",\"queued_frame_sizeof_bytes\":" << sizeof(QueuedFrame);
 for (const auto& item : current) cout << ",\"" << item.first << "\":" << item.second;
 for (const auto& item : cap_resource_peaks) cout << ",\"peak_" << item.first << "\":" << item.second;
 cout << "}\n";
}
