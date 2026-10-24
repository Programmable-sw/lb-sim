"""Trace audits shared by current waterline experiments."""
import csv,gzip,json
PAUSE_FIELDS=("fabric_pause_link_us","source_nic_pause_link_us","nic_last_hop_pause_link_us","all_network_pause_link_us")
def pause_fields(e):return {k:e[k] for k in PAUSE_FIELDS}
def audit_trace(d):
 counters=dict(pretrim_feedback=0,pretrim_direct_decreases=0,metadata_rejections=0,metadata_peak=0)
 for line in gzip.open(d/'trace.jsonl.gz','rt'):
  e=json.loads(line);kind=e['event']
  if kind in ('common_nack_cc','artifact_nack_feedback')and e.get('pretrim')and e.get('accepted'):
   counters['pretrim_feedback']+=1
   counters['pretrim_direct_decreases']+=int(e['new_cwnd_bytes']<e['old_cwnd_bytes'])
  if kind=='nic_receiver_summary':counters['metadata_rejections']+=e['resource_rejections']
  if kind=='rx_metadata_sample':counters['metadata_peak']=max(counters['metadata_peak'],e['global_used'])
  if kind=='summary':counters['metadata_peak']=max(counters['metadata_peak'],e['reorder_peak_entries'])
  if kind=='pause_summary':counters.update(pause_fields(e))
 return counters


