// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#include "config.h"
#include <sstream>
#include <fstream>

#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <string.h>
#include <math.h>
#include <algorithm>
#include <unistd.h>
#include "network.h"
#include "randomqueue.h"
#include "queue_lossless_input.h"
#include "shortflows.h"
#include "pipe.h"
#include "eventlist.h"
#include "logfile.h"
#include "loggers.h"
#include "clock.h"
#include "roce.h"
#include "compositequeue.h"
#include "ecnqueue.h"
#include "firstfit.h"
#include "queue_lossless.h"
#include "queue_lossless_output.h"
#include "topology.h"
#include "connection_matrix.h"
#include "tcppacket.h"

#include "fat_tree_topology.h"
#include "fat_tree_switch.h"

#include <list>
#include <set>
#include <functional>
#include <array>

// Simulation params

#define PRINT_PATHS 1

#define PERIODIC 0
#include "main.h"

uint32_t RTT = 1; // retained for legacy logfile metadata
int DEFAULT_NODES = 256;
#define DEFAULT_QUEUE_SIZE 100
#define REPS_LINKSPEED_MBPS 400000
#define REPS_MTU_BYTES 4096
#define REPS_HOP_LATENCY_US 0.5
#define REPS_SWITCH_LATENCY_US 0.5

//#define SWITCH_BUFFER (SERVICE * RTT / 1000)
#define USE_FIRST_FIT 0
#define FIRST_FIT_INTERVAL 100

EventList eventlist;

static bool is_canonical_two_tier_scale(uint32_t nodes) {
    switch (nodes) {
    case 16:  // four hosts per leaf, four leaves, four spines (smoke topology)
    case 256:
    case 512:
    case 1024:
    case 2048:
    case 4096:
    case 8192:
        return true;
    default:
        return false;
    }
}

struct QueueDiag {
    uint64_t lossless_overflows;
    uint64_t lossless_ecn_marks;
    uint64_t lossy_drops;
    uint64_t lossy_ecn_marks;
    uint64_t composite_trims;
    uint64_t composite_drops;
    uint64_t composite_ecn_marks;

    QueueDiag()
        : lossless_overflows(0),
          lossless_ecn_marks(0),
          lossy_drops(0),
          lossy_ecn_marks(0),
          composite_trims(0),
          composite_drops(0),
          composite_ecn_marks(0) {}
};

struct StorDiag {
    uint8_t min_score;
    uint64_t avoid_entries;
    uint64_t avoid_exits;
    uint64_t clean_signals;
    uint64_t ecn_signals;
    uint64_t trim_signals;

    StorDiag()
        : min_score(255), avoid_entries(0), avoid_exits(0),
          clean_signals(0), ecn_signals(0), trim_signals(0) {}
};

struct NetawareDiag {
    uint64_t samples;
    uint64_t sample_zero_bits;
    uint64_t feedbacks;
    uint64_t packet_feedbacks;
    uint64_t time_feedbacks;
    uint64_t feedback_packets_sum;
    uint64_t feedback_zero_bits;
    uint64_t all_good_feedbacks;

    NetawareDiag()
        : samples(0), sample_zero_bits(0), feedbacks(0),
          packet_feedbacks(0), time_feedbacks(0),
          feedback_packets_sum(0), feedback_zero_bits(0),
          all_good_feedbacks(0) {}
};

static string format_u32_vector(const vector<uint32_t>& values,
                                size_t limit = 128) {
    stringstream out;
    size_t n = values.size() < limit ? values.size() : limit;
    for (size_t i = 0; i < n; i++) {
        if (i)
            out << "/";
        out << values[i];
    }
    return out.str();
}

static string format_u32_u64_hist(const map<uint32_t, uint64_t>& hist,
                                  size_t limit = 0) {
    if (hist.empty())
        return "none";

    stringstream out;
    size_t emitted = 0;
    for (map<uint32_t, uint64_t>::const_iterator it = hist.begin();
         it != hist.end(); ++it) {
        if (limit && emitted >= limit)
            break;
        if (emitted)
            out << "/";
        out << it->first << ":" << it->second;
        emitted++;
    }
    return out.str();
}

static string format_nmrc_level_transitions() {
    stringstream out;
    for (uint32_t original = 0; original < 4; original++) {
        if (original)
            out << "/";
        for (uint32_t selected = 0; selected < 4; selected++) {
            if (selected)
                out << ",";
            out << FatTreeSwitch::_nmrc_diag_level_transitions
                    [original][selected];
        }
    }
    return out.str();
}

static string format_profile_level_transitions(bool netaware) {
    stringstream out;
    for (uint32_t from = 0; from < 4; from++) {
        for (uint32_t to = 0; to < 4; to++) {
            if (from != 0 || to != 0)
                out << "/";
            out << (netaware ?
                RoceSrc::netawareLevelTransition(from, to) :
                RoceSrc::storLevelTransition(from, to));
        }
    }
    return out.str();
}

static string format_top_u32_u64_hist(const map<uint32_t, uint64_t>& hist,
                                      size_t limit) {
    if (hist.empty())
        return "none";

    vector<pair<uint64_t, uint32_t> > sorted;
    for (map<uint32_t, uint64_t>::const_iterator it = hist.begin();
         it != hist.end(); ++it) {
        sorted.push_back(make_pair(it->second, it->first));
    }
    sort(sorted.begin(), sorted.end(), greater<pair<uint64_t, uint32_t> >());

    stringstream out;
    size_t n = sorted.size() < limit ? sorted.size() : limit;
    for (size_t i = 0; i < n; i++) {
        if (i)
            out << "/";
        out << sorted[i].second << ":" << sorted[i].first;
    }
    return out.str();
}

static void add_queue_diag(BaseQueue* queue, set<BaseQueue*>& seen, QueueDiag& diag) {
    if (!queue || !seen.insert(queue).second) {
        return;
    }

    LosslessOutputQueue* lossless_output = dynamic_cast<LosslessOutputQueue*>(queue);
    if (lossless_output) {
        diag.lossless_overflows += lossless_output->overflow_count();
        diag.lossless_ecn_marks += lossless_output->ecn_mark_count();
    }

    LosslessQueue* lossless_queue = dynamic_cast<LosslessQueue*>(queue);
    if (lossless_queue) {
        diag.lossless_overflows += lossless_queue->overflow_count();
    }

    ECNQueue* ecn_queue = dynamic_cast<ECNQueue*>(queue);
    if (ecn_queue) {
        diag.lossy_drops += ecn_queue->drop_count();
        diag.lossy_ecn_marks += ecn_queue->ecn_mark_count();
    }

    CompositeQueue* composite_queue = dynamic_cast<CompositeQueue*>(queue);
    if (composite_queue) {
        diag.composite_trims += composite_queue->trim_count();
        diag.composite_drops += composite_queue->drop_count();
        diag.composite_ecn_marks += composite_queue->ecn_mark_count();
    }
}

static void add_queue_diag(const vector< vector< vector<BaseQueue*> > >& queues,
                           set<BaseQueue*>& seen, QueueDiag& diag) {
    for (size_t i = 0; i < queues.size(); i++) {
        for (size_t j = 0; j < queues[i].size(); j++) {
            for (size_t k = 0; k < queues[i][j].size(); k++) {
                add_queue_diag(queues[i][j][k], seen, diag);
            }
        }
    }
}

static QueueDiag collect_queue_diag(FatTreeTopology* top) {
    QueueDiag diag;
    set<BaseQueue*> seen;

    add_queue_diag(top->queues_nc_nup, seen, diag);
    add_queue_diag(top->queues_nup_nlp, seen, diag);
    add_queue_diag(top->queues_nlp_ns, seen, diag);
    add_queue_diag(top->queues_nup_nc, seen, diag);
    add_queue_diag(top->queues_nlp_nup, seen, diag);
    add_queue_diag(top->queues_ns_nlp, seen, diag);

    return diag;
}

static void add_stor_diag(Switch* sw, StorDiag& diag) {
    FatTreeSwitch* fat_switch = dynamic_cast<FatTreeSwitch*>(sw);
    if (!fat_switch)
        return;
    fat_switch->collect_stor_diag(diag.min_score,
                                  diag.avoid_entries,
                                  diag.avoid_exits,
                                  diag.clean_signals,
                                  diag.ecn_signals,
                                  diag.trim_signals);
}

static void add_netaware_diag(Switch* sw, NetawareDiag& diag) {
    FatTreeSwitch* fat_switch = dynamic_cast<FatTreeSwitch*>(sw);
    if (!fat_switch)
        return;
    fat_switch->collect_netaware_diag(diag.samples,
                                  diag.sample_zero_bits,
                                  diag.feedbacks,
                                  diag.packet_feedbacks,
                                  diag.time_feedbacks,
                                  diag.feedback_packets_sum,
                                  diag.feedback_zero_bits,
                                  diag.all_good_feedbacks);
}

static void add_stor_diag(const vector<Switch*>& switches, StorDiag& diag) {
    for (size_t i = 0; i < switches.size(); i++)
        add_stor_diag(switches[i], diag);
}

static void add_netaware_diag(const vector<Switch*>& switches, NetawareDiag& diag) {
    for (size_t i = 0; i < switches.size(); i++)
        add_netaware_diag(switches[i], diag);
}

static StorDiag collect_stor_diag(FatTreeTopology* top) {
    StorDiag diag;
    add_stor_diag(top->switches_lp, diag);
    add_stor_diag(top->switches_up, diag);
    add_stor_diag(top->switches_c, diag);
    return diag;
}

static NetawareDiag collect_netaware_diag(FatTreeTopology* top) {
    NetawareDiag diag;
    add_netaware_diag(top->switches_lp, diag);
    add_netaware_diag(top->switches_up, diag);
    add_netaware_diag(top->switches_c, diag);
    return diag;
}

class SglbBackgroundDropSink : public PacketSink {
public:
    SglbBackgroundDropSink() : _nodename("sglb_background_drop") {}

    void receivePacket(Packet& pkt) {
        pkt.free();
    }

    const string& nodename() {
        return _nodename;
    }

private:
    string _nodename;
};

class SglbBackgroundSource : public EventSource {
public:
    SglbBackgroundSource(EventList& eventlist, const string& name, Route* route,
                         double rate_gbps, uint32_t packet_size,
                         simtime_picosec on_time, simtime_picosec off_time)
        : EventSource(eventlist, name),
          _flow(NULL),
          _route(route),
          _packet_size(packet_size),
          _seq(1),
          _on_time(on_time),
          _off_time(off_time),
          _interval(1),
          _enabled(route && rate_gbps > 0.0 && packet_size > 0 && on_time > 0) {
        if (_enabled) {
            double ps = ceil(((double)packet_size * 8.0 * 1000.0) / rate_gbps);
            if (ps < 1.0)
                ps = 1.0;
            _interval = (simtime_picosec)ps;
        }
    }

    void start() {
        if (_enabled)
            eventlist().sourceIsPending(*this, EventList::now());
    }

    void doNextEvent() {
        if (!_enabled)
            return;

        simtime_picosec now = eventlist().now();
        simtime_picosec cycle = _on_time + _off_time;
        if (cycle > 0 && _off_time > 0) {
            simtime_picosec phase = now % cycle;
            if (phase >= _on_time) {
                eventlist().sourceIsPendingRel(*this, cycle - phase);
                return;
            }
        }

        TcpPacket* pkt = TcpPacket::newpkt(_flow, *_route, _seq, _packet_size);
        _seq += _packet_size;
        pkt->sendOn();

        simtime_picosec delay = _interval;
        if (cycle > 0 && _off_time > 0) {
            simtime_picosec phase = now % cycle;
            simtime_picosec remaining_on = _on_time - phase;
            if (delay > remaining_on)
                delay = remaining_on;
        }
        if (delay == 0)
            delay = 1;
        eventlist().sourceIsPendingRel(*this, delay);
    }

private:
    PacketFlow _flow;
    Route* _route;
    uint32_t _packet_size;
    TcpPacket::seq_t _seq;
    simtime_picosec _on_time;
    simtime_picosec _off_time;
    simtime_picosec _interval;
    bool _enabled;
};

class SglbQueueCvSampler : public EventSource {
public:
    SglbQueueCvSampler(EventList& eventlist, FatTreeTopology* top, simtime_picosec period)
        : EventSource(eventlist, "sglb_queue_cv_sampler"),
          _top(top),
          _period(period),
          _samples(0),
          _peak_bytes(0),
          _observations(0) {
        _queue_fraction_hist.fill(0);
    }

    void start() {
        if (_top && _period > 0)
            eventlist().sourceIsPendingRel(*this, _period);
    }

    void doNextEvent() {
        vector<double> sample;
        collect_spine_queues(sample);
        if (_sums.empty())
            _sums.assign(sample.size(), 0.0);
        if (sample.size() == _sums.size()) {
            for (size_t i = 0; i < sample.size(); i++)
                _sums[i] += sample[i];
            _samples++;
        }
        eventlist().sourceIsPendingRel(*this, _period);
    }

    double average_queue() const {
        if (_samples == 0 || _sums.empty())
            return 0.0;

        double total = 0.0;
        for (size_t i = 0; i < _sums.size(); i++)
            total += _sums[i] / (double)_samples;
        return total / (double)_sums.size();
    }

    double cv() const {
        if (_samples == 0 || _sums.empty())
            return 0.0;

        double mean = average_queue();
        if (mean <= 0.0)
            return 0.0;

        double variance = 0.0;
        for (size_t i = 0; i < _sums.size(); i++) {
            double avg = _sums[i] / (double)_samples;
            double delta = avg - mean;
            variance += delta * delta;
        }
        variance /= (double)_sums.size();
        return sqrt(variance) / mean;
    }

    uint32_t count() const {
        return (uint32_t)_sums.size();
    }

    uint64_t peak_bytes() const {
        return _peak_bytes;
    }

    double queue_fraction_percentile(double quantile) const {
        if (!_observations)
            return 0.0;
        uint64_t target = (uint64_t)ceil(
            quantile * (double)_observations);
        if (!target)
            target = 1;
        uint64_t cumulative = 0;
        for (uint32_t bin = 0; bin < _queue_fraction_hist.size(); bin++) {
            cumulative += _queue_fraction_hist[bin];
            if (cumulative >= target)
                return (double)bin / 100.0;
        }
        return 2.0;
    }

private:
    void collect_spine_queues(vector<double>& sample) {
        const vector< vector< vector<BaseQueue*> > >& queues =
            _top->get_tiers() == 2 ? _top->queues_nup_nlp : _top->queues_nc_nup;
        for (size_t core = 0; core < queues.size(); core++) {
            for (size_t agg = 0; agg < queues[core].size(); agg++) {
                for (size_t b = 0; b < queues[core][agg].size(); b++) {
                    BaseQueue* q = queues[core][agg][b];
                    if (q) {
                        uint64_t bytes = q->queuesize();
                        sample.push_back((double)bytes);
                        _peak_bytes = std::max(_peak_bytes, bytes);
                        double fraction = q->maxsize() ?
                            (double)bytes / (double)q->maxsize() : 0.0;
                        uint32_t bin = (uint32_t)floor(fraction * 100.0);
                        bin = std::min(bin, 200U);
                        _queue_fraction_hist[bin]++;
                        _observations++;
                    }
                }
            }
        }
    }

    FatTreeTopology* _top;
    simtime_picosec _period;
    vector<double> _sums;
    uint64_t _samples;
    uint64_t _peak_bytes;
    uint64_t _observations;
    std::array<uint64_t, 201> _queue_fraction_hist;
};

class NetawarePathStateTracer : public EventSource {
public:
    NetawarePathStateTracer(EventList& eventlist,
                        FatTreeTopology* top,
                        simtime_picosec period,
                        uint32_t path_count,
                        ostream* out)
        : EventSource(eventlist, "netaware_path_state_tracer"),
          _top(top),
          _period(period),
          _path_count(path_count ? path_count : 1),
          _out(out) {}

    void start() {
        if (_top && _out && _period > 0)
            eventlist().sourceIsPendingRel(*this, _period);
    }

    void doNextEvent() {
        write_sample();
        eventlist().sourceIsPendingRel(*this, _period);
    }

private:
    void write_sample() {
        if (!_top || !_out)
            return;

        uint32_t hosts_per_tor = _top->radix_down(TOR_TIER);
        if (!hosts_per_tor)
            hosts_per_tor = 1;

        double now_us = timeAsUs(eventlist().now());
        for (size_t s = 0; s < _top->switches_lp.size(); s++) {
            FatTreeSwitch* src =
                dynamic_cast<FatTreeSwitch*>(_top->switches_lp[s]);
            if (!src)
                continue;

            for (size_t d = 0; d < _top->switches_lp.size(); d++) {
                FatTreeSwitch* dst =
                    dynamic_cast<FatTreeSwitch*>(_top->switches_lp[d]);
                if (!dst || src->getID() == dst->getID())
                    continue;

                uint32_t dst_host = dst->getID() * hosts_per_tor;
                for (uint32_t ev = 0; ev < _path_count; ev++) {
                    FatTreeSwitch::NetawarePathTraceSample sample;
                    src->netaware_trace_path_state(dst_host, ev, _path_count, sample);
                    uint32_t weight = RoceSrc::netawareLevelWeight(sample.path_grade);
                    (*_out)
                        << now_us << ","
                        << src->getID() << ","
                        << dst->getID() << ","
                        << ev << ","
                        << sample.spine_id << ","
                        << sample.q_leaf_to_spine << ","
                        << sample.q_spine_to_dst_leaf << ","
                        << sample.q_leaf_to_spine_max << ","
                        << sample.q_spine_to_dst_leaf_max << ","
                        << sample.util_leaf_to_spine << ","
                        << sample.util_spine_to_dst_leaf << ","
                        << sample.link_rate_leaf_to_spine << ","
                        << sample.link_rate_spine_to_dst_leaf << ","
                        << (sample.link_down ? 1 : 0) << ","
                        << sample.ecn_marks_on_path << ","
                        << sample.trimmed_packets_on_path << ","
                        << sample.dropped_packets_on_path << ","
                        << sample.bytes_sent_on_path << ","
                        << sample.local_q_pressure << ","
                        << sample.remote_q_pressure << ","
                        << sample.local_util_pressure << ","
                        << sample.remote_util_pressure << ","
                        << sample.path_score << ","
                        << sample.netaware_level_reason << ","
                        << (uint32_t)sample.path_grade << ","
                        << weight << "\n";
                }
            }
        }
    }

    FatTreeTopology* _top;
    simtime_picosec _period;
    uint32_t _path_count;
    ostream* _out;
};

static Route* make_sglb_background_route(BaseQueue* queue, Pipe* pipe, PacketSink* drop) {
    Route* route = new Route();
    route->push_back(queue);
    route->push_back(pipe);
    route->push_back(drop);
    return route;
}

static uint32_t add_sglb_background_links(
        vector<SglbBackgroundSource*>& sources,
        EventList& eventlist,
        SglbBackgroundDropSink* drop,
        const string& label,
        const vector< vector< vector<BaseQueue*> > >& queues,
        const vector< vector< vector<Pipe*> > >& pipes,
        uint32_t max_links,
        double rate_gbps,
        uint32_t packet_size,
        simtime_picosec on_time,
        simtime_picosec off_time) {
    uint32_t added = 0;
    set<pair<BaseQueue*, Pipe*> > selected;
    for (size_t i = 0; i < queues.size() && added < max_links; i++) {
        for (size_t j = 0; j < queues[i].size() && added < max_links; j++) {
            for (size_t b = 0;
                 b < queues[i][j].size() && added < max_links; b++) {
                BaseQueue* queue = queues[i][j][b];
                Pipe* pipe = NULL;
                if (i < pipes.size() && j < pipes[i].size() && b < pipes[i][j].size())
                    pipe = pipes[i][j][b];
                if (!queue || !pipe)
                    continue;

                if (!selected.insert(make_pair(queue, pipe)).second)
                    continue;

                stringstream link_label;
                link_label << label;
                if (max_links > 1)
                    link_label << "_" << added;
                Route* route = make_sglb_background_route(queue, pipe, drop);
                SglbBackgroundSource* source =
                    new SglbBackgroundSource(eventlist,
                                             "sglb_bg_" + link_label.str(), route,
                                             rate_gbps, packet_size, on_time, off_time);
                sources.push_back(source);
                source->start();
                cout << "SGLB background " << link_label.str()
                     << " on " << queue->nodename()
                     << " rate " << rate_gbps
                     << "Gbps on " << timeAsUs(on_time)
                     << "us off " << timeAsUs(off_time)
                     << "us packet " << packet_size
                     << " bytes" << endl;
                added++;
            }
        }
    }
    if (!added)
        cout << "SGLB background " << label << " skipped: no live link" << endl;
    return added;
}

static bool add_fixed_link_background(
        vector<SglbBackgroundSource*>& sources,
        EventList& eventlist,
        SglbBackgroundDropSink* drop,
        const string& name,
        BaseQueue* queue,
        Pipe* pipe,
        double rate_gbps,
        uint32_t packet_size,
        simtime_picosec on_time,
        simtime_picosec off_time) {
    if (!queue || !pipe || rate_gbps <= 0.0 || packet_size == 0 || on_time == 0)
        return false;
    Route* route = make_sglb_background_route(queue, pipe, drop);
    SglbBackgroundSource* source =
        new SglbBackgroundSource(eventlist, name, route, rate_gbps,
                                 packet_size, on_time, off_time);
    sources.push_back(source);
    source->start();
    return true;
}

static uint32_t install_path_hotspot_background(
        FatTreeTopology* top,
        EventList& eventlist,
        vector<SglbBackgroundSource*>& sources,
        SglbBackgroundDropSink* drop,
        uint32_t hot_spines,
        double rate_gbps,
        uint32_t packet_size,
        simtime_picosec on_time,
        simtime_picosec off_time) {
    uint32_t added = 0;
    uint32_t selected = std::min(hot_spines, top->getNAGG());

    for (size_t leaf = 0; leaf < top->queues_nlp_nup.size(); leaf++) {
        for (uint32_t spine = 0; spine < selected; spine++) {
            if (spine >= top->queues_nlp_nup[leaf].size())
                continue;
            for (size_t bundle = 0;
                 bundle < top->queues_nlp_nup[leaf][spine].size(); bundle++) {
                BaseQueue* queue = top->queues_nlp_nup[leaf][spine][bundle];
                Pipe* pipe = NULL;
                if (leaf < top->pipes_nlp_nup.size() &&
                    spine < top->pipes_nlp_nup[leaf].size() &&
                    bundle < top->pipes_nlp_nup[leaf][spine].size())
                    pipe = top->pipes_nlp_nup[leaf][spine][bundle];
                if (add_fixed_link_background(
                        sources, eventlist, drop, "path_hotspot_leaf_to_spine",
                        queue, pipe, rate_gbps, packet_size, on_time, off_time))
                    added++;
            }
        }
    }

    for (uint32_t spine = 0;
         spine < selected && spine < top->queues_nup_nlp.size(); spine++) {
        for (size_t leaf = 0; leaf < top->queues_nup_nlp[spine].size(); leaf++) {
            for (size_t bundle = 0;
                 bundle < top->queues_nup_nlp[spine][leaf].size(); bundle++) {
                BaseQueue* queue = top->queues_nup_nlp[spine][leaf][bundle];
                Pipe* pipe = NULL;
                if (spine < top->pipes_nup_nlp.size() &&
                    leaf < top->pipes_nup_nlp[spine].size() &&
                    bundle < top->pipes_nup_nlp[spine][leaf].size())
                    pipe = top->pipes_nup_nlp[spine][leaf][bundle];
                if (add_fixed_link_background(
                        sources, eventlist, drop, "path_hotspot_spine_to_leaf",
                        queue, pipe, rate_gbps, packet_size, on_time, off_time))
                    added++;
            }
        }
    }
    return added;
}

static uint32_t install_sglb_background(FatTreeTopology* top,
                                        EventList& eventlist,
                                        vector<SglbBackgroundSource*>& sources,
                                        SglbBackgroundDropSink* drop,
                                        uint32_t links_per_direction,
                                        double rate_gbps,
                                        uint32_t packet_size,
                                        simtime_picosec on_time,
                                        simtime_picosec off_time) {
    uint32_t added = 0;
    added += add_sglb_background_links(
        sources, eventlist, drop, "tor_to_leaf",
        top->queues_nlp_nup, top->pipes_nlp_nup, links_per_direction,
        rate_gbps, packet_size, on_time, off_time);
    added += add_sglb_background_links(
        sources, eventlist, drop, "leaf_to_tor",
        top->queues_nup_nlp, top->pipes_nup_nlp, links_per_direction,
        rate_gbps, packet_size, on_time, off_time);
    if (top->get_tiers() == 3) {
        added += add_sglb_background_links(
            sources, eventlist, drop, "leaf_to_spine",
            top->queues_nup_nc, top->pipes_nup_nc, links_per_direction,
            rate_gbps, packet_size, on_time, off_time);
        added += add_sglb_background_links(
            sources, eventlist, drop, "spine_to_leaf",
            top->queues_nc_nup, top->pipes_nc_nup, links_per_direction,
            rate_gbps, packet_size, on_time, off_time);
    }
    return added;
}

const char* nmrc_network_decision_name(
    FatTreeSwitch::NmrcNetworkDecisionMode decision) {
    switch (decision) {
    case FatTreeSwitch::NMRC_NETWORK_GRADED:
        return "graded";
    case FatTreeSwitch::NMRC_NETWORK_BINARY_SCORE:
        return "binary_score";
    case FatTreeSwitch::NMRC_NETWORK_FIXED_THRESHOLD:
        return "fixed_threshold";
    case FatTreeSwitch::NMRC_NETWORK_DELTA:
        return "delta";
    case FatTreeSwitch::NMRC_NETWORK_PIECEWISE_DELTA:
        return "piecewise_delta";
    case FatTreeSwitch::NMRC_NETWORK_TWO_STAGE_DELTA:
        return "two_stage_delta";
    case FatTreeSwitch::NMRC_NETWORK_ABSOLUTE_REROUTE:
        return "absolute_reroute";
    }
    return "unknown";
}

void exit_error(char* progr) {
    cout << "Usage " << progr << " [-nodes N] [-conns C] [-q queue_size] [-tm traffic_matrix_file]\\n\\t[-lb ecmp|ecmp_rr|adaptive-routing|conga|sglb|sglb-ecn-filter|sglb-ecn-clear|sglb-old|sglb-paper|drill|reps|avail|grade|mrc|netaware|n-mrc|n-mrc-fixed0.5|n-mrc-delta|rr|ops|conweave|ndp|mprdma]\\n\\t[-cc none|dcqcn|dctcp_variant]" << endl;
    cout << "\t[-roce_sack_bitmap_bits 64|128]" << endl;
    cout << "\t[-roce_transport_semantics legacy|mrc_exact_bounded]" << endl;
    cout << "\t[-roce_trim_recovery cumulative|exact]" << endl;
    cout << "\t[-cc dctcp_variant_nodup_old] [-pfc on|off]" << endl;
    cout << "\t[-dcqcn_nack_reaction cnp|ignore|rate_cut]" << endl;
    cout << "\t[-sglb_local_damping]" << endl;
    cout << "\t[-sglb_score_mode legacy|nmrc_quantized_topk]" << endl;
    cout << "\t[-sglb_nmrc_q_range qmin qmax]" << endl;
    cout << "\t[-sglb_nmrc_level_thresholds degraded bad avoid]" << endl;
    cout << "\t[-sglb_nmrc_levels 4|8|16]" << endl;
    cout << "\t[-sglb_background] [-sglb_bg_links_per_direction N] "
         << "[-sglb_bg_rate_gbps x] [-sglb_bg_on_us x] "
         << "[-sglb_bg_off_us x] [-sglb_bg_packet_size bytes]" << endl;
    cout << "\t[-path_hotspot_spines N] [-path_hotspot_bg_rate_gbps x] "
         << "[-path_hotspot_bg_on_us x] [-path_hotspot_bg_off_us x]" << endl;
    cout << "\t[-stor_feedback_pkts N] [-stor_feedback_min_us x] [-stor_feedback_max_us x]" << endl;
    cout << "\t[-stor_trim_feedback_min_us x]" << endl;
    cout << "\t[-avail_ecn_only]" << endl;
    cout << "\t[-grade_complex_score]" << endl;
    cout << "\t[-stor_score_profile original|balanced|simple|binary]" << endl;
    cout << "\t[-stor_score_params clean ecn_add trim_add ecn_base trim_base]" << endl;
    cout << "\t[-stor_simple_score_params clean penalty good degraded bad]" << endl;
    cout << "\t[-stor_level_thresholds good degraded bad]" << endl;
    cout << "\t[-stor_level_weights good degraded bad avoid]" << endl;
    cout << "\t[-stor_aging packet|time_ewma|hybrid]" << endl;
    cout << "\t[-stor_time_ewma_us ecn_tau trim_tau score_tau]" << endl;
    cout << "\t[-stor_hybrid_hold_us bad_hold avoid_hold]" << endl;
    cout << "\t[-stor_hybrid_probe interval_pkts clean_promote]" << endl;
    cout << "\t[-netaware_queue_threshold x]" << endl;
    cout << "\t[-netaware_queue_level_thresholds degraded bad avoid]" << endl;
    cout << "\t[-netaware_degraded_util_thresholds queue_floor util]" << endl;
    cout << "\t[-netaware_score_mode gated|worst_hop|sglb_quantized]" << endl;
    cout << "\t[-netaware_path_coupling additive|bottleneck|noisy_or]" << endl;
    cout << "\t[-netaware_score_q_range kmin kmax]" << endl;
    cout << "\t[-netaware_score_util_range low high]" << endl;
    cout << "\t[-netaware_score_weights w_lq w_rq w_lu w_ru]" << endl;
    cout << "\t[-netaware_score_level_thresholds degraded bad avoid]" << endl;
    cout << "\t[-netaware_level_weights good degraded bad avoid]" << endl;
    cout << "\t[-netaware_wrr_mode shuffled_bucket|bucket|direct|topk]" << endl;
    cout << "\t[-netaware_topk k]" << endl;
    cout << "\t[-netaware_weight_adaptation off|good_share_cap]" << endl;
    cout << "\t[-netaware_trace_prefix prefix] [-netaware_trace_period_us x]" << endl;
    cout << "\t[-nmrc_reroute_policy any_better|better_ge3]" << endl;
    cout << "\t[-nmrc_fastcnp on|off]" << endl;
    cout << "\t[-nmrc_endpoint_policy rr_cooldown|random_stateless]" << endl;
    cout << "\t[-nmrc_all_cooling_policy earliest|rr_reset]" << endl;
    cout << "\t[-nmrc_network_decision graded|fixed_threshold|delta]" << endl;
    cout << "\t[-nmrc_binary_threshold 0.5]" << endl;
    cout << "\t[-nmrc_absolute_threshold VALUE]" << endl;
    cout << "\t[-nmrc_relative_delta VALUE]" << endl;
    cout << "\t[-nmrc_graded_cooldown selective|full|none]" << endl;
    cout << "\t[-nmrc_graded_reroute_delta VALUE]" << endl;
    cout << "\t[-nmrc_route_delta VALUE] [-nmrc_cooldown_delta VALUE]" << endl;
    cout << "\t[-queue_cv_sample_us x]" << endl;
    cout << "\t[-path_selection_timeline file] "
         << "[-path_selection_timeline_every N]" << endl;
    cout << "\t[-mixed_lb_traffic]" << endl;
    exit(1);
}

int main(int argc, char **argv) {
    Clock c(timeFromSec(5 / 100.), eventlist);
    mem_b queuesize = DEFAULT_QUEUE_SIZE;
    linkspeed_bps linkspeed = speedFromMbps((double)REPS_LINKSPEED_MBPS);
    int packet_size = REPS_MTU_BYTES;
    uint32_t path_entropy_size = 10000000;
    uint32_t no_of_conns = 0, no_of_nodes = DEFAULT_NODES;
    uint32_t tiers = 2; // current experiments use a two-tier leaf-spine fabric
    double logtime = 0.25; // ms;
    stringstream filename(ios_base::out);
    simtime_picosec hop_latency = timeFromUs(REPS_HOP_LATENCY_US);
    simtime_picosec switch_latency = timeFromUs(REPS_SWITCH_LATENCY_US);
    simtime_picosec start_delta = 0;
    queue_type qt = LOSSLESS_INPUT_ECN;
    float ar_sticky_delta = 10;
    uint32_t ar_granularity = FatTreeSwitch::PER_PACKET;
    RoceSrc::lb_mode_t roce_lb_mode = RoceSrc::LB_ECMP;
    RoceSrc::cc_mode_t roce_cc_mode = RoceSrc::CC_DCTCP_VARIANT;
    bool queue_user_set = false;
    bool queue_type_user_set = false;
    bool roce_rx_mode_user_set = false;
    bool roce_cc_mode_user_set = false;
    bool roce_trim_recovery_user_set = false;
    bool ecn_thresh_user_set = false;
    bool path_entropy_user_set = false;
    bool source_pathid_lb = false;
    string lb_scheme_name = "ecmp";

    queue_type snd_type = FAIR_PRIO;

    uint64_t high_pfc = 80, low_pfc = 20;
    bool pfc_user_set = false;
    bool pfc_enabled = true;
    uint64_t lossless_ecn_kmin_bytes = 0, lossless_ecn_kmax_bytes = 0;
    uint64_t shared_buffer_mb = 9;
    double shared_ingress_alpha = 0.0625;
    double shared_egress_alpha = 1.0;
    uint64_t shared_headroom_bytes = 0;
    uint32_t reps_buffer = 8;
    double conweave_rtt_us = 16.0;
    double conweave_min_reroute_us = 4.0;
    uint32_t ndp_cwnd = 256;
    uint32_t cc_iw_pkts = 0;
    uint32_t cc_min_cwnd_pkts = 1;
    uint32_t cc_max_cwnd_pkts = 0;
    RoceSrc::rx_mode_t roce_rx_mode = RoceSrc::RX_GBN;
    RoceSrc::transport_semantics_t roce_transport_semantics =
        RoceSrc::TRANSPORT_MRC_EXACT_BOUNDED;
    uint32_t roce_sack_bitmap_bits = ROCE_SACK_BITMAP_BITS_DEFAULT;
    double roce_ooo_us = 15.0;
    uint32_t roce_ooo_window_pkts = 32;
    double roce_ooo_window_ratio = 0.0;
    uint64_t roce_bdp_bytes = 0;
    double roce_nack_interval_us = 4.0;
    uint32_t roce_rto_us = 70;
    uint32_t roce_rto_high_us = 0;
    bool roce_ooo_user_set = false;
    bool roce_ooo_window_user_set = false;
    bool roce_ooo_ratio_user_set = false;
    bool roce_rto_user_set = false;
    bool roce_rto_high_user_set = false;
    uint32_t slow_core_downlinks = 0;
    uint32_t slow_core_downlink_divisor = 10;
    uint32_t slow_tor_uplinks = 0;
    uint32_t slow_tor_uplink_divisor = 2;
    bool slow_tor_uplink_random_sparse = false;
    bool sglb_background = false;
    bool mixed_lb_traffic = false;
    double sglb_bg_rate_gbps = 350.0;
    double sglb_bg_on_us = 200.0;
    double sglb_bg_off_us = 200.0;
    uint32_t sglb_bg_packet_size = 0;
    uint32_t sglb_bg_links_per_direction = 1;
    uint32_t path_hotspot_spines = 0;
    double path_hotspot_bg_rate_gbps = 300.0;
    double path_hotspot_bg_on_us = 1000.0;
    double path_hotspot_bg_off_us = 0.0;
    double queue_cv_sample_us = 0.0;
    string path_selection_timeline_file;
    uint64_t path_selection_timeline_every = 100000;
    double paper_sglb_gcn_delay_us = -1.0;
    double stor_feedback_min_us = 5.0;
    double stor_feedback_max_us = 5.0;
    double stor_trim_feedback_min_us = 5.0;
    uint32_t stor_feedback_pkts = 0;
    bool stor_feedback_pkts_user_set = false;
    bool stor_score_profile_user_set = false;
    bool grade_complex_score = false;
    bool avail_ecn_only = false;
    FatTreeSwitch::StorAgingProfile stor_aging =
        FatTreeSwitch::STOR_AGING_PACKET;
    double stor_time_ecn_tau_us = 20.0;
    double stor_time_trim_tau_us = 50.0;
    double stor_time_score_tau_us = 50.0;
    double stor_hybrid_bad_hold_us = 10.0;
    double stor_hybrid_avoid_hold_us = 20.0;
    uint32_t stor_hybrid_probe_interval_pkts = 64;
    uint32_t stor_hybrid_probe_clean_promote = 2;
    double netaware_degraded_queue_threshold =
        FatTreeSwitch::_netaware_degraded_queue_fraction;
    double netaware_bad_queue_threshold =
        FatTreeSwitch::_netaware_bad_queue_fraction;
    double netaware_queue_threshold = 0.8;
    double netaware_util_queue_floor_threshold =
        FatTreeSwitch::_netaware_util_queue_floor_fraction;
    double netaware_degraded_utilization_threshold =
        FatTreeSwitch::_netaware_degraded_utilization_fraction;
    FatTreeSwitch::NetawareScoreMode netaware_score_mode =
        FatTreeSwitch::_netaware_score_mode;
    FatTreeSwitch::NetawarePathCoupling netaware_path_coupling =
        FatTreeSwitch::_netaware_path_coupling;
    double netaware_score_q_min = FatTreeSwitch::_netaware_score_q_min;
    double netaware_score_q_max = FatTreeSwitch::_netaware_score_q_max;
    double netaware_score_util_low = FatTreeSwitch::_netaware_score_util_low;
    double netaware_score_util_high = FatTreeSwitch::_netaware_score_util_high;
    double netaware_score_weight_local_q =
        FatTreeSwitch::_netaware_score_weight_local_q;
    double netaware_score_weight_remote_q =
        FatTreeSwitch::_netaware_score_weight_remote_q;
    double netaware_score_weight_local_util =
        FatTreeSwitch::_netaware_score_weight_local_util;
    double netaware_score_weight_remote_util =
        FatTreeSwitch::_netaware_score_weight_remote_util;
    double netaware_score_degraded_threshold =
        FatTreeSwitch::_netaware_score_degraded_threshold;
    double netaware_score_bad_threshold =
        FatTreeSwitch::_netaware_score_bad_threshold;
    double netaware_score_avoid_threshold =
        FatTreeSwitch::_netaware_score_avoid_threshold;
    string netaware_trace_prefix;
    double netaware_trace_period_us = 10.0;
    FatTreeSwitch::NmrcReroutePolicy nmrc_reroute_policy =
        FatTreeSwitch::NMRC_REROUTE_BETTER_GE3;
    RoceSrc::nmrc_endpoint_policy_t nmrc_endpoint_policy =
        RoceSrc::NMRC_ENDPOINT_RR_COOLDOWN;
    RoceSrc::nmrc_all_cooling_policy_t nmrc_all_cooling_policy =
        RoceSrc::NMRC_ALL_COOLING_EARLIEST;
    FatTreeSwitch::NmrcNetworkDecisionMode nmrc_network_decision =
        FatTreeSwitch::NMRC_NETWORK_GRADED;
    FatTreeSwitch::NmrcGradedCooldownMode nmrc_graded_cooldown =
        FatTreeSwitch::NMRC_GRADED_COOLDOWN_SELECTIVE;
    double nmrc_graded_reroute_delta = 0.0;
    double nmrc_binary_threshold = 0.5;
    double nmrc_absolute_threshold = 0.50;
    double nmrc_relative_delta = 0.25;
    double nmrc_route_delta = 0.10;
    double nmrc_cooldown_delta = 0.30;
    bool nmrc_fastcnp = true;
    bool nmrc_option_user_set = false;
    bool nmrc_reroute_policy_user_set = false;
    bool nmrc_fastcnp_user_set = false;
    bool nmrc_endpoint_policy_user_set = false;
    bool nmrc_all_cooling_policy_user_set = false;
    bool nmrc_network_decision_user_set = false;
    bool nmrc_graded_cooldown_user_set = false;
    bool nmrc_graded_reroute_delta_user_set = false;
    bool nmrc_binary_threshold_user_set = false;
    bool nmrc_absolute_threshold_user_set = false;
    bool nmrc_relative_delta_user_set = false;
    bool nmrc_route_delta_user_set = false;
    bool nmrc_cooldown_delta_user_set = false;
    double ecn_thresh = 1.0;
    uint32_t mrc_logical_evs = 0;
    uint32_t mrc_active_paths = 0;
    uint32_t mrc_backup_paths = 0;
    uint32_t mrc_min_active_paths = 1;
    uint32_t mrc_path_bits = 0;

    bool log_sink = false;
    bool log_tor_downqueue = false;
    bool log_tor_upqueue = false;
    bool log_traffic = false;
    bool log_switches = false;
    bool log_queue_usage = false;
    set<flowid_t> debug_flow_ids;
    RouteStrategy route_strategy = NOT_SET;
    int seed = 13;
    int i = 1;
    filename << "logout.dat";
    int end_time = 1000;//in microseconds

    char* tm_file = NULL;
    char* topo_file = NULL;

    while (i<argc) {
        if (!strcmp(argv[i],"-o")) {
            filename.str(std::string());
            filename << argv[i+1];
            i++;
        } else if (!strcmp(argv[i],"-conns")) {
            no_of_conns = atoi(argv[i+1]);
            cout << "no_of_conns "<<no_of_conns << endl;
            i++;
        } else if (!strcmp(argv[i],"-end")) {
            end_time = atoi(argv[i+1]);
            cout << "endtime(us) "<< end_time << endl;
            i++;            
        } else if (!strcmp(argv[i],"-nodes")) {
            no_of_nodes = atoi(argv[i+1]);
            cout << "no_of_nodes "<<no_of_nodes << endl;
            i++;
        } else if (!strcmp(argv[i],"-tiers")) {
            tiers = atoi(argv[i+1]);
            cout << "tiers "<< tiers << endl;
            assert(tiers == 2 || tiers == 3);
            i++;
        } else if (!strcmp(argv[i],"-queue_type")) {
            if (!strcmp(argv[i+1], "composite")) {
                qt = COMPOSITE;
            } 
            else if (!strcmp(argv[i+1], "composite_ecn")) {
                qt = COMPOSITE_ECN;
            }
            else if (!strcmp(argv[i+1], "composite_ecn_lb") ||
                     !strcmp(argv[i+1], "mrc_trim_ecn") ||
                     !strcmp(argv[i+1], "trim_ecn")) {
                qt = COMPOSITE_ECN_LB;
            }
            else if (!strcmp(argv[i+1], "lossless")) {
                qt = LOSSLESS;
            }
            else if (!strcmp(argv[i+1], "lossless_input")) {
                qt = LOSSLESS_INPUT;
            }
            else if (!strcmp(argv[i+1], "lossless_input_ecn")) {
                qt = LOSSLESS_INPUT_ECN;
            }
            else if (!strcmp(argv[i+1], "shared_buffer_ecn")) {
                qt = SHARED_BUFFER_ECN;
            }
            else {
                cout << "Unknown queue type " << argv[i+1] << endl;
                exit_error(argv[0]);
            }
            cout << "queue_type "<< qt << endl;
            queue_type_user_set = true;
            i++;
        } else if (!strcmp(argv[i],"-shared_buffer_mb")) {
            shared_buffer_mb = strtoull(argv[i+1], NULL, 10);
            i++;
        } else if (!strcmp(argv[i],"-shared_ingress_alpha")) {
            shared_ingress_alpha = atof(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-shared_egress_alpha")) {
            shared_egress_alpha = atof(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-shared_headroom_kb")) {
            shared_headroom_bytes = strtoull(argv[i+1], NULL, 10) * 1024;
            i++;
        } else if (!strcmp(argv[i],"-shared_headroom_bytes")) {
            shared_headroom_bytes = strtoull(argv[i+1], NULL, 10);
            i++;
        } else if (!strcmp(argv[i],"-host_queue_type")) {
            if (!strcmp(argv[i+1], "swift")) {
                snd_type = SWIFT_SCHEDULER;
            } 
            else if (!strcmp(argv[i+1], "prio")) {
                snd_type = PRIORITY;
            }
            else if (!strcmp(argv[i+1], "fair_prio")) {
                snd_type = FAIR_PRIO;
            }
            else {
                cout << "Unknown host queue type " << argv[i+1] << " expecting one of swift|prio|fair_prio" << endl;
                exit_error(argv[0]);
            }
            cout << "host queue_type "<< snd_type << endl;
            i++;
        } else if (!strcmp(argv[i],"-mixed_lb_traffic")){
            mixed_lb_traffic = true;
            cout << "Mixed LB traffic enabled" << endl;
        } else if (!strcmp(argv[i],"-log")){
            if (!strcmp(argv[i+1], "sink")) {
                log_sink = true;
            } else if (!strcmp(argv[i+1], "sink")) {
                cout << "logging sinks\n";
                log_sink = true;
            } else if (!strcmp(argv[i+1], "tor_downqueue")) {
                cout << "logging tor downqueues\n";
                log_tor_downqueue = true;
            } else if (!strcmp(argv[i+1], "tor_upqueue")) {
                cout << "logging tor upqueues\n";
                log_tor_upqueue = true;
            } else if (!strcmp(argv[i+1], "switch")) {
                cout << "logging total switch queues\n";
                log_switches = true;
            } else if (!strcmp(argv[i+1], "traffic")) {
                cout << "logging traffic\n";
                log_traffic = true;
            } else if (!strcmp(argv[i+1], "queue_usage")) {
                cout << "logging queue usage\n";
                log_queue_usage = true;
            } else {
                exit_error(argv[0]);
            }
            i++;
        } else if (!strcmp(argv[i],"-roce_debug_flow_id")) {
            if (i + 1 >= argc) {
                cerr << "Missing RoCE debug flow id" << endl;
                exit(1);
            }
            flowid_t flow_id = (flowid_t)strtoul(argv[i+1], NULL, 10);
            debug_flow_ids.insert(flow_id);
            cout << "RoCE debug flow id " << flow_id << endl;
            i++;
        } else if (!strcmp(argv[i],"-tm")){
            tm_file = argv[i+1];
            cout << "traffic matrix input file: "<< tm_file << endl;
            i++;
        } else if (!strcmp(argv[i],"-topo")){
            topo_file = argv[i+1];
            cout << "FatTree topology input file: "<< topo_file << endl;
            i++;
        } else if (!strcmp(argv[i],"-q")){
            queuesize = atoi(argv[i+1]);
            queue_user_set = true;
            i++;
        } else if (!strcmp(argv[i],"-lb")){
            if (!strcmp(argv[i+1], "ecmp")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                roce_lb_mode = RoceSrc::LB_ECMP;
                lb_scheme_name = "ecmp";
            } else if (!strcmp(argv[i+1], "ecmp_rr")) {
                route_strategy = ECMP_FIB;
                path_entropy_size = 1;
                FatTreeSwitch::set_strategy(FatTreeSwitch::RR);
                roce_lb_mode = RoceSrc::LB_ECMP;
                lb_scheme_name = "ecmp_rr";
            } else if (!strcmp(argv[i+1], "sglb")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::configure_sglb_scheme_defaults(false);
                FatTreeSwitch::set_sglb_ecn_mode(
                    FatTreeSwitch::SGLB_ECN_OFF);
                FatTreeSwitch::set_strategy(FatTreeSwitch::SGLB);
                roce_lb_mode = RoceSrc::LB_ECMP;
                lb_scheme_name = "sglb";
            } else if (!strcmp(argv[i+1], "sglb-ecn-filter") ||
                       !strcmp(argv[i+1], "sglb-ecn-clear")) {
                const bool neutral =
                    !strcmp(argv[i+1], "sglb-ecn-filter");
                route_strategy = ECMP_FIB;
                FatTreeSwitch::configure_sglb_scheme_defaults(false);
                FatTreeSwitch::set_sglb_ecn_mode(
                    neutral ? FatTreeSwitch::SGLB_ECN_NEUTRAL :
                              FatTreeSwitch::SGLB_ECN_CLEAR);
                FatTreeSwitch::set_strategy(FatTreeSwitch::SGLB);
                roce_lb_mode = RoceSrc::LB_ECMP;
                lb_scheme_name = argv[i+1];
            } else if (!strcmp(argv[i+1], "sglb-old")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::configure_sglb_scheme_defaults(true);
                FatTreeSwitch::set_strategy(FatTreeSwitch::SGLB);
                roce_lb_mode = RoceSrc::LB_ECMP;
                lb_scheme_name = "sglb-old";
            } else if (!strcmp(argv[i+1], "sglb-paper")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::PAPER_SGLB);
                roce_lb_mode = RoceSrc::LB_ECMP;
                lb_scheme_name = "sglb-paper";
            } else if (!strcmp(argv[i+1], "reps")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                roce_lb_mode = RoceSrc::LB_REPS;
                lb_scheme_name = "reps";
            } else if (!strcmp(argv[i+1], "avail")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                roce_lb_mode = RoceSrc::LB_STOR;
                lb_scheme_name = "avail";
            } else if (!strcmp(argv[i+1], "grade")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                roce_lb_mode = RoceSrc::LB_STOR;
                lb_scheme_name = "grade";
            } else if (!strcmp(argv[i+1], "netaware")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                roce_lb_mode = RoceSrc::LB_NETAWARE;
                lb_scheme_name = "netaware";
            } else if (!strcmp(argv[i+1], "n-mrc")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                roce_lb_mode = RoceSrc::LB_NMRC;
                lb_scheme_name = "n-mrc";
            } else if (!strcmp(argv[i+1], "n-mrc-fixed0.5")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                roce_lb_mode = RoceSrc::LB_NMRC;
                lb_scheme_name = "n-mrc-fixed0.5";
            } else if (!strcmp(argv[i+1], "n-mrc-delta")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                roce_lb_mode = RoceSrc::LB_NMRC;
                lb_scheme_name = "n-mrc-delta";
            } else if (!strcmp(argv[i+1], "mrc")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                roce_lb_mode = RoceSrc::LB_MRC;
                lb_scheme_name = "mrc";
            } else if (!strcmp(argv[i+1], "rr")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                roce_lb_mode = RoceSrc::LB_RR;
                lb_scheme_name = "rr";
            } else if (!strcmp(argv[i+1], "ops")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                roce_lb_mode = RoceSrc::LB_OPS;
                lb_scheme_name = "ops";
            } else if (!strcmp(argv[i+1], "conweave")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                roce_lb_mode = RoceSrc::LB_CONWEAVE;
                lb_scheme_name = "conweave";
            } else if (!strcmp(argv[i+1], "ndp")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                roce_lb_mode = RoceSrc::LB_NDP;
                lb_scheme_name = "ndp";
            } else if (!strcmp(argv[i+1], "mprdma")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                roce_lb_mode = RoceSrc::LB_MPRDMA;
                lb_scheme_name = "mprdma";
            } else if (!strcmp(argv[i+1], "conga")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                roce_lb_mode = RoceSrc::LB_CONGA;
                lb_scheme_name = "conga";
            } else if (!strcmp(argv[i+1], "adaptive-routing")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ADAPTIVE_ROUTING);
                roce_lb_mode = RoceSrc::LB_ECMP;
                lb_scheme_name = "adaptive-routing";
            } else if (!strcmp(argv[i+1], "drill")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::DRILL);
                roce_lb_mode = RoceSrc::LB_ECMP;
                lb_scheme_name = "drill";
            } else {
                cout << "Unknown lb mode " << argv[i+1] << endl;
                exit_error(argv[0]);
            }
            cout << "lb mode " << lb_scheme_name << endl;
            i++;
        } else if (!strcmp(argv[i],"-cc")){
            if (!strcmp(argv[i+1], "none")) {
                roce_cc_mode = RoceSrc::CC_NONE;
            } else if (!strcmp(argv[i+1], "dcqcn")) {
                roce_cc_mode = RoceSrc::CC_DCQCN;
            } else if (!strcmp(argv[i+1], "dctcp_variant_nodup_old") ||
                       !strcmp(argv[i+1], "dcqcn_variant_nodup_old") ||
                       !strcmp(argv[i+1], "dcqcn-variant-nodup-old") ||
                       !strcmp(argv[i+1], "dctcp-variant-nodup-old")) {
                roce_cc_mode = RoceSrc::CC_DCTCP_VARIANT_NODUP_OLD;
            } else if (!strcmp(argv[i+1], "dctcp_variant") ||
                       !strcmp(argv[i+1], "dcqcn_variant") ||
                       !strcmp(argv[i+1], "dcqcn-variant") ||
                       !strcmp(argv[i+1], "dctcp-variant") ||
                       !strcmp(argv[i+1], "dctcp")) {
                roce_cc_mode = RoceSrc::CC_DCTCP_VARIANT;
            } else {
                cout << "Unknown cc mode " << argv[i+1] << endl;
                exit_error(argv[0]);
            }
            cout << "cc mode " << (roce_cc_mode == RoceSrc::CC_DCTCP_VARIANT ?
                "dctcp_variant" : roce_cc_mode == RoceSrc::CC_DCTCP_VARIANT_NODUP_OLD ?
                "dctcp_variant_nodup_old" : argv[i+1]) << endl;
            roce_cc_mode_user_set = true;
            i++;
        } else if (!strcmp(argv[i],"-roce_trim_recovery")){
            if (i + 1 >= argc) {
                cerr << "Missing RoCE TRIM recovery mode" << endl;
                exit(1);
            }
            if (!strcmp(argv[i+1], "cumulative")) {
                RoceSrc::setTrimRecoveryMode(
                    RoceSrc::TRIM_RECOVERY_CUMULATIVE);
            } else if (!strcmp(argv[i+1], "exact")) {
                RoceSrc::setTrimRecoveryMode(
                    RoceSrc::TRIM_RECOVERY_EXACT_PSN);
            } else {
                cerr << "Invalid RoCE TRIM recovery mode " << argv[i+1]
                     << "; expected cumulative or exact" << endl;
                exit(1);
            }
            cout << "RoCE TRIM recovery mode "
                 << RoceSrc::trimRecoveryModeName() << endl;
            roce_trim_recovery_user_set = true;
            i++;
        } else if (!strcmp(argv[i],"-roce_transport_semantics")){
            if (i + 1 >= argc) {
                cerr << "Missing RoCE transport semantics" << endl;
                exit(1);
            }
            if (!strcmp(argv[i+1], "legacy")) {
                roce_transport_semantics = RoceSrc::TRANSPORT_LEGACY;
            } else if (!strcmp(argv[i+1], "mrc_exact_bounded") ||
                       !strcmp(argv[i+1], "exact_bounded")) {
                roce_transport_semantics =
                    RoceSrc::TRANSPORT_MRC_EXACT_BOUNDED;
            } else {
                cerr << "Invalid RoCE transport semantics " << argv[i+1]
                     << "; expected legacy or mrc_exact_bounded" << endl;
                exit(1);
            }
            i++;
        } else if (!strcmp(argv[i],"-logtime")){
            logtime = atof(argv[i+1]);            
            cout << "logtime "<< logtime << " ms" << endl;
            i++;
        } else if (!strcmp(argv[i],"-linkspeed")){
            // linkspeed specified is in Mbps
            linkspeed = speedFromMbps(atof(argv[i+1]));
            i++;
        } else if (!strcmp(argv[i],"-seed")){
            seed = atoi(argv[i+1]);
            cout << "random seed "<< seed << endl;
            i++;
        } else if (!strcmp(argv[i],"-mtu")){
            packet_size = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-paths")){
            path_entropy_size = atoi(argv[i+1]);
            path_entropy_user_set = true;
            cout << "no of paths " << path_entropy_size << endl;
            i++;
        } else if (!strcmp(argv[i],"-hop_latency")){
            hop_latency = timeFromUs(atof(argv[i+1]));
            cout << "Hop latency set to " << timeAsUs(hop_latency) << endl;
            i++;
        } else if (!strcmp(argv[i],"-switch_latency")){
            switch_latency = timeFromUs(atof(argv[i+1]));
            cout << "Switch latency set to " << timeAsUs(switch_latency) << endl;
            i++;
        } else if (!strcmp(argv[i],"-start_delta")){
            start_delta = atof(argv[i+1]);
            cout << "Start connectios with a random delay of upto " << start_delta << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-ar_sticky_delta")){
            ar_sticky_delta = atof(argv[i+1]);
            cout << "Adaptive routing sticky delta " << ar_sticky_delta << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-ar_granularity")){
            if (!strcmp(argv[i+1], "packet")) {
                ar_granularity = FatTreeSwitch::PER_PACKET;
            } else if (!strcmp(argv[i+1], "flowlet")) {
                ar_granularity = FatTreeSwitch::PER_FLOWLET;
            } else {
                cout << "Unknown AR granularity expecting packet or flowlet" << endl;
                exit(1);
            }
            cout << "Adaptive routing granularity " << argv[i+1] << endl;
            i++;
        } else if (!strcmp(argv[i],"-reps_buffer")){
            reps_buffer = atoi(argv[i+1]);
            cout << "reps buffer size " << reps_buffer << endl;
            i++;
        } else if (!strcmp(argv[i],"-conweave_rtt_us")){
            conweave_rtt_us = atof(argv[i+1]);
            cout << "conweave RTT reroute threshold " << conweave_rtt_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-conweave_min_reroute_us")){
            conweave_min_reroute_us = atof(argv[i+1]);
            cout << "conweave minimum reroute gap " << conweave_min_reroute_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-ndp_cwnd")){
            ndp_cwnd = atoi(argv[i+1]);
            if (!ndp_cwnd)
                ndp_cwnd = 1;
            cout << "ndp initial receiver-pull window " << ndp_cwnd << " packets" << endl;
            i++;
        } else if (!strcmp(argv[i],"-ecn_thresh")){
            ecn_thresh = atof(argv[i+1]);
            if (ecn_thresh < 0.0)
                ecn_thresh = 0.0;
            ecn_thresh_user_set = true;
            cout << "Composite RED ECN Kmax fraction " << ecn_thresh << endl;
            i++;
        } else if (!strcmp(argv[i],"-roce_rx_mode")){
            if (!strcmp(argv[i+1], "gbn") || !strcmp(argv[i+1], "default")) {
                roce_rx_mode = RoceSrc::RX_GBN;
            } else if (!strcmp(argv[i+1], "sp") || !strcmp(argv[i+1], "sack") ||
                       !strcmp(argv[i+1], "selective")) {
                roce_rx_mode = RoceSrc::RX_SP_RETX_QUEUE;
            } else {
                cout << "Unknown RoCE receive mode " << argv[i+1] << endl;
                exit_error(argv[0]);
            }
            cout << "RoCE receive mode " << argv[i+1] << endl;
            roce_rx_mode_user_set = true;
            i++;
        } else if (!strcmp(argv[i],"-roce_sack_bitmap_bits")){
            int requested_bits = atoi(argv[i+1]);
            uint32_t normalized_bits = requested_bits > 0 ?
                RoceSrc::normalizeSackBitmapBits((uint32_t)requested_bits) :
                ROCE_SACK_BITMAP_BITS_DEFAULT;
            roce_sack_bitmap_bits = normalized_bits;
            cout << "RoCE SACK bitmap " << roce_sack_bitmap_bits << " bits";
            if ((uint32_t)(requested_bits > 0 ? requested_bits : 0) != roce_sack_bitmap_bits)
                cout << " (supported values are 64 and 128)";
            cout << endl;
            i++;
        } else if (!strcmp(argv[i],"-roce_ooo_us")){
            roce_ooo_us = atof(argv[i+1]);
            if (roce_ooo_us < 0)
                roce_ooo_us = 0;
            roce_ooo_user_set = true;
            cout << "RoCE OOO tolerance " << roce_ooo_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-roce_ooo_window_pkts") ||
                   !strcmp(argv[i],"-roce_loss_trace_window_pkts")){
            roce_ooo_window_pkts = atoi(argv[i+1]);
            if (!roce_ooo_window_pkts)
                roce_ooo_window_pkts = 1;
            roce_ooo_window_user_set = true;
            cout << "RoCE loss trace window " << roce_ooo_window_pkts << " packets" << endl;
            i++;
        } else if (!strcmp(argv[i],"-roce_ooo_window_ratio") ||
                   !strcmp(argv[i],"-roce_loss_trace_window_ratio")){
            roce_ooo_window_ratio = atof(argv[i+1]);
            if (roce_ooo_window_ratio < 0)
                roce_ooo_window_ratio = 0;
            roce_ooo_ratio_user_set = true;
            cout << "RoCE loss trace window ratio " << roce_ooo_window_ratio << " BDP" << endl;
            i++;
        } else if (!strcmp(argv[i],"-roce_bdp_bytes")){
            roce_bdp_bytes = strtoull(argv[i+1], NULL, 10);
            cout << "RoCE BDP override " << roce_bdp_bytes << " bytes" << endl;
            i++;
        } else if (!strcmp(argv[i],"-roce_nack_interval_us")){
            roce_nack_interval_us = atof(argv[i+1]);
            if (roce_nack_interval_us < 0)
                roce_nack_interval_us = 0;
            cout << "RoCE NACK interval " << roce_nack_interval_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-roce_rto_us")){
            roce_rto_us = atoi(argv[i+1]);
            if (!roce_rto_us)
                roce_rto_us = 1;
            roce_rto_user_set = true;
            cout << "RoCE RTO " << roce_rto_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-roce_rto_high_us")){
            roce_rto_high_us = atoi(argv[i+1]);
            roce_rto_high_user_set = true;
            cout << "RoCE high RTO " << roce_rto_high_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-cc_iw_pkts")){
            cc_iw_pkts = atoi(argv[i+1]);
            if (!cc_iw_pkts)
                cc_iw_pkts = 1;
            cout << "CC initial window " << cc_iw_pkts << " packets" << endl;
            i++;
        } else if (!strcmp(argv[i],"-cc_min_cwnd_pkts")){
            cc_min_cwnd_pkts = atoi(argv[i+1]);
            if (!cc_min_cwnd_pkts)
                cc_min_cwnd_pkts = 1;
            cout << "CC minimum window " << cc_min_cwnd_pkts << " packets" << endl;
            i++;
        } else if (!strcmp(argv[i],"-cc_max_cwnd_pkts")){
            cc_max_cwnd_pkts = atoi(argv[i+1]);
            cout << "CC maximum window " << cc_max_cwnd_pkts << " packets" << endl;
            i++;
        } else if (!strcmp(argv[i],"-dcqcn_g")){
            double g = atof(argv[i+1]);
            if (g < 0)
                g = 0;
            RoceSrc::setDcqcnG(g);
            cout << "DCQCN g " << g << endl;
            i++;
        } else if (!strcmp(argv[i],"-dcqcn_initial_alpha")){
            double alpha = atof(argv[i+1]);
            if (alpha < 0)
                alpha = 0;
            if (alpha > 1)
                alpha = 1;
            RoceSrc::setDcqcnInitialAlpha(alpha);
            cout << "DCQCN initial alpha " << alpha << endl;
            i++;
        } else if (!strcmp(argv[i],"-dcqcn_ai_mbps")){
            double ai_mbps = atof(argv[i+1]);
            if (ai_mbps < 0)
                ai_mbps = 0;
            RoceSrc::setDcqcnAiRate(speedFromMbps(ai_mbps));
            cout << "DCQCN additive increase " << ai_mbps << "Mbps" << endl;
            i++;
        } else if (!strcmp(argv[i],"-dcqcn_min_rate_mbps")){
            double min_mbps = atof(argv[i+1]);
            if (min_mbps < 0)
                min_mbps = 0;
            RoceSrc::setDcqcnMinRate(speedFromMbps(min_mbps));
            cout << "DCQCN minimum rate " << min_mbps << "Mbps" << endl;
            i++;
        } else if (!strcmp(argv[i],"-dcqcn_alpha_us")){
            double alpha_us = atof(argv[i+1]);
            if (alpha_us < 0)
                alpha_us = 0;
            RoceSrc::setDcqcnAlphaInterval(timeFromUs(alpha_us));
            cout << "DCQCN alpha interval " << alpha_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-dcqcn_rate_us")){
            double rate_us = atof(argv[i+1]);
            if (rate_us < 0)
                rate_us = 0;
            RoceSrc::setDcqcnRateIncreaseInterval(timeFromUs(rate_us));
            cout << "DCQCN rate increase interval " << rate_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-dcqcn_cnp_us")){
            double cnp_us = atof(argv[i+1]);
            if (cnp_us < 0)
                cnp_us = 0;
            RoceSrc::setDcqcnCnpInterval(timeFromUs(cnp_us));
            cout << "DCQCN CNP interval " << cnp_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-dcqcn_byte_counter")){
            mem_b bytes = atoll(argv[i+1]);
            if (bytes < 0)
                bytes = 0;
            RoceSrc::setDcqcnByteCounter(bytes);
            cout << "DCQCN byte counter " << bytes << " bytes" << endl;
            i++;
        } else if (!strcmp(argv[i],"-dcqcn_fast_recovery_steps")){
            uint32_t steps = atoi(argv[i+1]);
            RoceSrc::setDcqcnFastRecoverySteps(steps);
            cout << "DCQCN fast recovery steps " << steps << endl;
            i++;
        } else if (!strcmp(argv[i],"-dcqcn_nack_reaction")){
            if (!strcmp(argv[i+1], "cnp")) {
                RoceSrc::setDcqcnNackReaction(RoceSrc::DCQCN_NACK_CNP);
            } else if (!strcmp(argv[i+1], "ignore")) {
                RoceSrc::setDcqcnNackReaction(RoceSrc::DCQCN_NACK_IGNORE);
            } else if (!strcmp(argv[i+1], "rate_cut")) {
                RoceSrc::setDcqcnNackReaction(RoceSrc::DCQCN_NACK_RATE_CUT);
            } else {
                cout << "Unknown DCQCN NACK reaction " << argv[i+1] << endl;
                exit_error(argv[0]);
            }
            cout << "DCQCN NACK reaction " << argv[i+1] << endl;
            i++;
        } else if (!strcmp(argv[i],"-slow_core_downlinks")){
            slow_core_downlinks = atoi(argv[i+1]);
            cout << "Slow core-to-agg downlinks " << slow_core_downlinks << endl;
            i++;
        } else if (!strcmp(argv[i],"-slow_core_downlink_divisor")){
            slow_core_downlink_divisor = atoi(argv[i+1]);
            if (!slow_core_downlink_divisor)
                slow_core_downlink_divisor = 1;
            cout << "Slow core-to-agg downlink divisor " << slow_core_downlink_divisor << endl;
            i++;
        } else if (!strcmp(argv[i],"-slow_tor_uplinks")){
            slow_tor_uplinks = atoi(argv[i+1]);
            cout << "Slow ToR-to-agg uplinks " << slow_tor_uplinks << endl;
            i++;
        } else if (!strcmp(argv[i],"-slow_tor_uplink_divisor")){
            slow_tor_uplink_divisor = atoi(argv[i+1]);
            if (!slow_tor_uplink_divisor)
                slow_tor_uplink_divisor = 1;
            cout << "Slow ToR-to-agg uplink divisor " << slow_tor_uplink_divisor << endl;
            i++;
        } else if (!strcmp(argv[i],"-slow_tor_uplink_select")){
            if (!strcmp(argv[i+1], "spaced")) {
                slow_tor_uplink_random_sparse = false;
            } else if (!strcmp(argv[i+1], "random-sparse")) {
                slow_tor_uplink_random_sparse = true;
            } else {
                cout << "Unknown slow ToR uplink selection mode " << argv[i+1] << endl;
                exit_error(argv[0]);
            }
            cout << "Slow ToR-to-agg uplink selection " << argv[i+1] << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_background")){
            sglb_background = true;
            cout << "SGLB fixed-link background enabled" << endl;
        } else if (!strcmp(argv[i],"-sglb_bg_links_per_direction")){
            if (i + 1 >= argc || argv[i+1][0] == '-') {
                cerr << "SGLB background links per direction must be an integer >= 1"
                     << endl;
                exit_error(argv[0]);
            }
            char* end = NULL;
            errno = 0;
            unsigned long requested = strtoul(argv[i+1], &end, 10);
            if (errno == ERANGE || end == argv[i+1] || *end != '\0' ||
                requested < 1 ||
                requested > numeric_limits<uint32_t>::max()) {
                cerr << "SGLB background links per direction must be an integer >= 1"
                     << endl;
                exit_error(argv[0]);
            }
            sglb_bg_links_per_direction = (uint32_t)requested;
            i++;
        } else if (!strcmp(argv[i],"-sglb_bg_rate_gbps")){
            sglb_bg_rate_gbps = atof(argv[i+1]);
            if (sglb_bg_rate_gbps < 0.0)
                sglb_bg_rate_gbps = 0.0;
            cout << "SGLB background rate " << sglb_bg_rate_gbps << "Gbps" << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_bg_on_us")){
            sglb_bg_on_us = atof(argv[i+1]);
            if (sglb_bg_on_us < 0.0)
                sglb_bg_on_us = 0.0;
            cout << "SGLB background ON " << sglb_bg_on_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_bg_off_us")){
            sglb_bg_off_us = atof(argv[i+1]);
            if (sglb_bg_off_us < 0.0)
                sglb_bg_off_us = 0.0;
            cout << "SGLB background OFF " << sglb_bg_off_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_bg_packet_size")){
            sglb_bg_packet_size = atoi(argv[i+1]);
            cout << "SGLB background packet size " << sglb_bg_packet_size << " bytes" << endl;
            i++;
        } else if (!strcmp(argv[i],"-path_hotspot_spines")){
            int requested = atoi(argv[i+1]);
            if (requested < 0) {
                cerr << "Path hotspot spine count must be non-negative" << endl;
                exit(1);
            }
            path_hotspot_spines = (uint32_t)requested;
            cout << "Path hotspot spines " << path_hotspot_spines << endl;
            i++;
        } else if (!strcmp(argv[i],"-path_hotspot_bg_rate_gbps")){
            path_hotspot_bg_rate_gbps = atof(argv[i+1]);
            if (path_hotspot_bg_rate_gbps < 0.0)
                path_hotspot_bg_rate_gbps = 0.0;
            cout << "Path hotspot background rate "
                 << path_hotspot_bg_rate_gbps << "Gbps" << endl;
            i++;
        } else if (!strcmp(argv[i],"-path_hotspot_bg_on_us")){
            path_hotspot_bg_on_us = atof(argv[i+1]);
            if (path_hotspot_bg_on_us < 0.0)
                path_hotspot_bg_on_us = 0.0;
            cout << "Path hotspot background ON "
                 << path_hotspot_bg_on_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-path_hotspot_bg_off_us")){
            path_hotspot_bg_off_us = atof(argv[i+1]);
            if (path_hotspot_bg_off_us < 0.0)
                path_hotspot_bg_off_us = 0.0;
            cout << "Path hotspot background OFF "
                 << path_hotspot_bg_off_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-queue_cv_sample_us")){
            queue_cv_sample_us = atof(argv[i+1]);
            if (queue_cv_sample_us < 0.0)
                queue_cv_sample_us = 0.0;
            cout << "Queue CV sample interval " << queue_cv_sample_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-path_selection_timeline")){
            path_selection_timeline_file = argv[i+1];
            cout << "path selection timeline "
                 << path_selection_timeline_file << endl;
            i++;
        } else if (!strcmp(argv[i],"-path_selection_timeline_every")){
            path_selection_timeline_every = strtoull(argv[i+1], NULL, 10);
            if (path_selection_timeline_every == 0)
                path_selection_timeline_every = 1;
            cout << "path selection timeline every "
                 << path_selection_timeline_every << " selections" << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_update_us")){
            double sglb_update_us = atof(argv[i+1]);
            if (sglb_update_us < 0)
                sglb_update_us = 0;
            FatTreeSwitch::_sglb_update_interval = timeFromUs(sglb_update_us);
            cout << "sglb local quality update interval " << sglb_update_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_gcn_update_us")){
            double sglb_update_us = atof(argv[i+1]);
            if (sglb_update_us < 0)
                sglb_update_us = 0;
            FatTreeSwitch::_sglb_gcn_update_interval = timeFromUs(sglb_update_us);
            cout << "sglb GCN export update interval " << sglb_update_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_gcn_aging_us")){
            double sglb_aging_us = atof(argv[i+1]);
            if (sglb_aging_us < 0)
                sglb_aging_us = 0;
            FatTreeSwitch::_sglb_gcn_aging_interval = timeFromUs(sglb_aging_us);
            cout << "sglb GCN aging interval " << sglb_aging_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_score_mode")){
            if (!strcmp(argv[i+1], "legacy")) {
                FatTreeSwitch::_sglb_score_mode =
                    FatTreeSwitch::SGLB_SCORE_LEGACY;
            } else if (!strcmp(argv[i+1], "nmrc_quantized_topk")) {
                FatTreeSwitch::_sglb_score_mode =
                    FatTreeSwitch::SGLB_SCORE_NMRC_QUANTIZED_TOPK;
            } else {
                cerr << "unknown SGLB score mode " << argv[i+1] << endl;
                exit(1);
            }
            cout << "sglb score mode "
                 << FatTreeSwitch::sglb_score_mode_name() << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_ofat_factor")) {
            const char* value = argv[i+1];
            struct OfatName {
                const char* name;
                FatTreeSwitch::SglbOfatFactor factor;
            } names[] = {
                {"baseline", FatTreeSwitch::SGLB_OFAT_BASELINE},
                {"topk8", FatTreeSwitch::SGLB_OFAT_TOPK8},
                {"linear_score", FatTreeSwitch::SGLB_OFAT_LINEAR_SCORE},
                {"raw_queue", FatTreeSwitch::SGLB_OFAT_RAW_QUEUE},
                {"paper_levels", FatTreeSwitch::SGLB_OFAT_PAPER_LEVELS},
                {"remote_mean", FatTreeSwitch::SGLB_OFAT_REMOTE_MEAN},
                {"change_triggered", FatTreeSwitch::SGLB_OFAT_CHANGE_TRIGGERED},
                {"delayed_message", FatTreeSwitch::SGLB_OFAT_DELAYED_MESSAGE},
                {"eager_init", FatTreeSwitch::SGLB_OFAT_EAGER_INIT},
                {"versioned", FatTreeSwitch::SGLB_OFAT_VERSIONED},
                {"no_aging", FatTreeSwitch::SGLB_OFAT_NO_AGING},
                {"source_leaf_only", FatTreeSwitch::SGLB_OFAT_SOURCE_LEAF_ONLY},
                {"background_sglb", FatTreeSwitch::SGLB_OFAT_BACKGROUND_SGLB},
                {"shadow_gcn", FatTreeSwitch::SGLB_OFAT_SHADOW_GCN},
                {"raw_queue_topk8",
                 FatTreeSwitch::SGLB_OFAT_RAW_QUEUE_TOPK8},
                {"raw_paper_levels_topk8",
                 FatTreeSwitch::SGLB_OFAT_RAW_PAPER_LEVELS_TOPK8},
                {"raw_linear_paper_levels_topk8",
                 FatTreeSwitch::SGLB_OFAT_RAW_LINEAR_PAPER_LEVELS_TOPK8},
                {"real_gcn_profiles",
                 FatTreeSwitch::SGLB_OFAT_REAL_GCN_PROFILES},
                {"real_gcn_raw_linear",
                 FatTreeSwitch::SGLB_OFAT_REAL_GCN_RAW_LINEAR},
            };
            bool matched = false;
            for (size_t n = 0; n < sizeof(names) / sizeof(names[0]); ++n) {
                if (!strcmp(value, names[n].name)) {
                    FatTreeSwitch::_sglb_ofat_factor = names[n].factor;
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                cerr << "unknown SGLB OFAT factor " << value << endl;
                exit(1);
            }
            cout << "sglb OFAT factor "
                 << FatTreeSwitch::sglb_ofat_factor_name() << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_nmrc_q_range")){
            double q_min = atof(argv[i+1]);
            double q_max = atof(argv[i+2]);
            if (q_min < 0.0 || q_max > 1.0 || q_min >= q_max) {
                cerr << "invalid SGLB n-MRC q range " << q_min
                     << " " << q_max << endl;
                exit(1);
            }
            FatTreeSwitch::_sglb_nmrc_q_min = q_min;
            FatTreeSwitch::_sglb_nmrc_q_max = q_max;
            cout << "sglb n-MRC q range " << q_min << " " << q_max << endl;
            i += 2;
        } else if (!strcmp(argv[i],"-sglb_nmrc_level_thresholds")){
            double degraded = atof(argv[i+1]);
            double bad = atof(argv[i+2]);
            double avoid = atof(argv[i+3]);
            if (degraded < 0.0 || degraded >= bad || bad >= avoid ||
                avoid > 1.0) {
                cerr << "invalid SGLB n-MRC level thresholds "
                     << degraded << " " << bad << " " << avoid << endl;
                exit(1);
            }
            FatTreeSwitch::_sglb_nmrc_degraded_threshold = degraded;
            FatTreeSwitch::_sglb_nmrc_bad_threshold = bad;
            FatTreeSwitch::_sglb_nmrc_avoid_threshold = avoid;
            cout << "sglb n-MRC level thresholds " << degraded << " "
                 << bad << " " << avoid << endl;
            i += 3;
        } else if (!strcmp(argv[i],"-sglb_nmrc_levels")) {
            uint32_t levels = atoi(argv[i+1]);
            if (levels != 4 && levels != 8 && levels != 16) {
                cerr << "invalid SGLB n-MRC levels " << levels
                     << "; expected 4, 8, or 16" << endl;
                exit(1);
            }
            FatTreeSwitch::_sglb_nmrc_levels = levels;
            cout << "sglb n-mrc levels " << levels << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_weights")){
            FatTreeSwitch::_sglb_queue_weight = atof(argv[i+1]);
            FatTreeSwitch::_sglb_util_weight = atof(argv[i+2]);
            FatTreeSwitch::_sglb_remote_queue_weight = FatTreeSwitch::_sglb_queue_weight;
            FatTreeSwitch::_sglb_remote_util_weight = FatTreeSwitch::_sglb_util_weight;
            FatTreeSwitch::_sglb_remote_busy_weight = atof(argv[i+3]);
            cout << "sglb weights queue " << FatTreeSwitch::_sglb_queue_weight
                 << " util " << FatTreeSwitch::_sglb_util_weight
                 << " remote_busy " << FatTreeSwitch::_sglb_remote_busy_weight << endl;
            i += 3;
        } else if (!strcmp(argv[i],"-sglb_factors")){
            FatTreeSwitch::_sglb_queue_weight = atof(argv[i+1]);
            FatTreeSwitch::_sglb_util_weight = atof(argv[i+2]);
            FatTreeSwitch::_sglb_remote_queue_weight = atof(argv[i+3]);
            FatTreeSwitch::_sglb_remote_util_weight = atof(argv[i+4]);
            FatTreeSwitch::_sglb_remote_busy_weight = atof(argv[i+5]);
            cout << "sglb factors local_q " << FatTreeSwitch::_sglb_queue_weight
                 << " local_util " << FatTreeSwitch::_sglb_util_weight
                 << " remote_q " << FatTreeSwitch::_sglb_remote_queue_weight
                 << " remote_util " << FatTreeSwitch::_sglb_remote_util_weight
                 << " remote_busy " << FatTreeSwitch::_sglb_remote_busy_weight << endl;
            i += 5;
        } else if (!strcmp(argv[i],"-sglb_normalize")){
            FatTreeSwitch::_sglb_normalize_scores = true;
            cout << "sglb normalized queue/util factors enabled" << endl;
        } else if (!strcmp(argv[i],"-sglb_local_damping")){
            FatTreeSwitch::_sglb_local_damping = true;
            cout << "sglb local-pressure downstream damping enabled" << endl;
        } else if (!strcmp(argv[i],"-sglb_downstream_weight")){
            FatTreeSwitch::_sglb_downstream_weight = atof(argv[i+1]);
            cout << "sglb downstream weight " << FatTreeSwitch::_sglb_downstream_weight << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_quality_bucket")){
            FatTreeSwitch::_sglb_quality_bucket = atof(argv[i+1]);
            if (FatTreeSwitch::_sglb_quality_bucket <= 0.0)
                FatTreeSwitch::_sglb_quality_bucket = 1.0;
            cout << "sglb quality bucket " << FatTreeSwitch::_sglb_quality_bucket << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_quality_levels")){
            FatTreeSwitch::_sglb_quality_levels = atoi(argv[i+1]);
            if (!FatTreeSwitch::_sglb_quality_levels)
                FatTreeSwitch::_sglb_quality_levels = 1;
            FatTreeSwitch::_sglb_max_quality = FatTreeSwitch::_sglb_quality_levels - 1;
            cout << "sglb quality levels " << FatTreeSwitch::_sglb_quality_levels << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_topk")){
            char* end = NULL;
            long k = strtol(argv[i+1], &end, 10);
            if (end == argv[i+1] || *end || k < 1 || k > 65535) {
                cerr << "sglb_topk must be an integer in [1,65535]" << endl;
                exit(1);
            }
            FatTreeSwitch::_sglb_topk = static_cast<uint32_t>(k);
            FatTreeSwitch::_sglb_candidate_policy = FatTreeSwitch::SGLB_CANDIDATE_SCORE_TOPK;
            cout << "sglb continuous score topk " << k << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_min_choices")){
            FatTreeSwitch::_sglb_min_choices = atoi(argv[i+1]);
            if (!FatTreeSwitch::_sglb_min_choices)
                FatTreeSwitch::_sglb_min_choices = 1;
            cout << "sglb minimum sprayed choices " << FatTreeSwitch::_sglb_min_choices << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_candidate_policy")){
            if (!strcmp(argv[i+1], "strict_k"))
                FatTreeSwitch::_sglb_candidate_policy =
                    FatTreeSwitch::SGLB_CANDIDATE_STRICT_K;
            else if (!strcmp(argv[i+1], "whole_grade_min"))
                FatTreeSwitch::_sglb_candidate_policy =
                    FatTreeSwitch::SGLB_CANDIDATE_WHOLE_GRADE_MIN;
            else if (!strcmp(argv[i+1], "exact_min"))
                FatTreeSwitch::_sglb_candidate_policy =
                    FatTreeSwitch::SGLB_CANDIDATE_EXACT_MIN;
            else {
                cerr << "unknown SGLB candidate policy " << argv[i+1] << endl;
                exit(1);
            }
            cout << "sglb candidate policy " << argv[i+1] << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_candidate_dispatch")){
            if (!strcmp(argv[i+1], "random"))
                FatTreeSwitch::_sglb_candidate_dispatch =
                    FatTreeSwitch::SGLB_DISPATCH_RANDOM;
            else if (!strcmp(argv[i+1], "shuffled_rr"))
                FatTreeSwitch::_sglb_candidate_dispatch =
                    FatTreeSwitch::SGLB_DISPATCH_SHUFFLED_RR;
            else {
                cerr << "unknown SGLB candidate dispatch " << argv[i+1] << endl;
                exit(1);
            }
            cout << "sglb candidate dispatch " << argv[i+1] << endl;
            i++;
        } else if (!strcmp(argv[i],"-sglb_gcn_cadence")){
            if (!strcmp(argv[i+1], "independent"))
                FatTreeSwitch::_sglb_gcn_cadence =
                    FatTreeSwitch::SGLB_GCN_INDEPENDENT;
            else if (!strcmp(argv[i+1], "synchronized"))
                FatTreeSwitch::_sglb_gcn_cadence =
                    FatTreeSwitch::SGLB_GCN_SYNCHRONIZED;
            else {
                cerr << "unknown SGLB GCN cadence " << argv[i+1] << endl;
                exit(1);
            }
            cout << "sglb GCN cadence " << argv[i+1] << endl;
            i++;
        } else if (!strcmp(argv[i],"-stor_feedback_pkts")){
            stor_feedback_pkts = atoi(argv[i+1]);
            if (!stor_feedback_pkts)
                stor_feedback_pkts = 1;
            stor_feedback_pkts_user_set = true;
            cout << "stor feedback packet window " << stor_feedback_pkts << endl;
            i++;
        } else if (!strcmp(argv[i],"-stor_feedback_min_us")){
            stor_feedback_min_us = atof(argv[i+1]);
            if (stor_feedback_min_us < 0)
                stor_feedback_min_us = 0;
            cout << "stor minimum feedback interval " << stor_feedback_min_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-stor_feedback_max_us")){
            stor_feedback_max_us = atof(argv[i+1]);
            if (stor_feedback_max_us < 0)
                stor_feedback_max_us = 0;
            cout << "stor maximum feedback interval " << stor_feedback_max_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-stor_trim_feedback_min_us")){
            stor_trim_feedback_min_us = atof(argv[i+1]);
            if (stor_trim_feedback_min_us < 0)
                stor_trim_feedback_min_us = 0;
            cout << "stor trim feedback minimum interval "
                 << stor_trim_feedback_min_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-stor_aging")){
            if (!strcmp(argv[i+1], "packet")) {
                stor_aging = FatTreeSwitch::STOR_AGING_PACKET;
            } else if (!strcmp(argv[i+1], "time_ewma")) {
                stor_aging = FatTreeSwitch::STOR_AGING_TIME_EWMA;
            } else if (!strcmp(argv[i+1], "hybrid")) {
                stor_aging = FatTreeSwitch::STOR_AGING_HYBRID;
            } else {
                cerr << "Unknown STOR aging profile " << argv[i+1]
                     << "; expected packet, time_ewma, or hybrid" << endl;
                exit(1);
            }
            cout << "stor aging profile " << argv[i+1] << endl;
            i++;
        } else if (!strcmp(argv[i],"-stor_time_ewma_us")){
            stor_time_ecn_tau_us = atof(argv[i+1]);
            stor_time_trim_tau_us = atof(argv[i+2]);
            stor_time_score_tau_us = atof(argv[i+3]);
            if (stor_time_ecn_tau_us < 0.1)
                stor_time_ecn_tau_us = 0.1;
            if (stor_time_trim_tau_us < 0.1)
                stor_time_trim_tau_us = 0.1;
            if (stor_time_score_tau_us < 0.1)
                stor_time_score_tau_us = 0.1;
            cout << "stor time EWMA taus "
                 << stor_time_ecn_tau_us << "/"
                 << stor_time_trim_tau_us << "/"
                 << stor_time_score_tau_us << "us" << endl;
            i += 3;
        } else if (!strcmp(argv[i],"-stor_hybrid_hold_us")){
            stor_hybrid_bad_hold_us = atof(argv[i+1]);
            stor_hybrid_avoid_hold_us = atof(argv[i+2]);
            if (stor_hybrid_bad_hold_us < 0.0)
                stor_hybrid_bad_hold_us = 0.0;
            if (stor_hybrid_avoid_hold_us < 0.0)
                stor_hybrid_avoid_hold_us = 0.0;
            cout << "stor hybrid hold-down "
                 << stor_hybrid_bad_hold_us << "/"
                 << stor_hybrid_avoid_hold_us << "us" << endl;
            i += 2;
        } else if (!strcmp(argv[i],"-stor_hybrid_probe")){
            stor_hybrid_probe_interval_pkts = atoi(argv[i+1]);
            stor_hybrid_probe_clean_promote = atoi(argv[i+2]);
            if (!stor_hybrid_probe_interval_pkts)
                stor_hybrid_probe_interval_pkts = 1;
            if (!stor_hybrid_probe_clean_promote)
                stor_hybrid_probe_clean_promote = 1;
            cout << "stor hybrid probe interval/promote "
                 << stor_hybrid_probe_interval_pkts << "/"
                 << stor_hybrid_probe_clean_promote << endl;
            i += 2;
        } else if (!strcmp(argv[i],"-netaware_queue_threshold")){
            netaware_queue_threshold = atof(argv[i+1]);
            if (netaware_queue_threshold < 0.0)
                netaware_queue_threshold = 0.0;
            if (netaware_queue_threshold > 1.0)
                netaware_queue_threshold = 1.0;
            cout << "netaware queue threshold fraction " << netaware_queue_threshold << endl;
            i++;
        } else if (!strcmp(argv[i],"-netaware_queue_level_thresholds")){
            netaware_degraded_queue_threshold = atof(argv[i+1]);
            netaware_bad_queue_threshold = atof(argv[i+2]);
            netaware_queue_threshold = atof(argv[i+3]);
            if (netaware_degraded_queue_threshold < 0.0)
                netaware_degraded_queue_threshold = 0.0;
            if (netaware_degraded_queue_threshold > 1.0)
                netaware_degraded_queue_threshold = 1.0;
            if (netaware_bad_queue_threshold < 0.0)
                netaware_bad_queue_threshold = 0.0;
            if (netaware_bad_queue_threshold > 1.0)
                netaware_bad_queue_threshold = 1.0;
            if (netaware_queue_threshold < 0.0)
                netaware_queue_threshold = 0.0;
            if (netaware_queue_threshold > 1.0)
                netaware_queue_threshold = 1.0;
            if (netaware_bad_queue_threshold < netaware_degraded_queue_threshold)
                netaware_bad_queue_threshold = netaware_degraded_queue_threshold;
            if (netaware_queue_threshold < netaware_bad_queue_threshold)
                netaware_queue_threshold = netaware_bad_queue_threshold;
            cout << "netaware queue level thresholds "
                 << netaware_degraded_queue_threshold << "/"
                 << netaware_bad_queue_threshold << "/"
                 << netaware_queue_threshold << endl;
            i += 3;
        } else if (!strcmp(argv[i],"-netaware_degraded_util_thresholds")){
            netaware_util_queue_floor_threshold = atof(argv[i+1]);
            netaware_degraded_utilization_threshold = atof(argv[i+2]);
            if (netaware_util_queue_floor_threshold < 0.0)
                netaware_util_queue_floor_threshold = 0.0;
            if (netaware_util_queue_floor_threshold > 1.0)
                netaware_util_queue_floor_threshold = 1.0;
            if (netaware_degraded_utilization_threshold < 0.0)
                netaware_degraded_utilization_threshold = 0.0;
            if (netaware_degraded_utilization_threshold > 1.0)
                netaware_degraded_utilization_threshold = 1.0;
            cout << "netaware degraded utilization thresholds "
                 << netaware_util_queue_floor_threshold << "/"
                 << netaware_degraded_utilization_threshold << endl;
            i += 2;
        } else if (!strcmp(argv[i],"-netaware_score_mode")){
            if (!strcmp(argv[i+1], "gated")) {
                netaware_score_mode = FatTreeSwitch::NETAWARE_SCORE_GATED;
            } else if (!strcmp(argv[i+1], "worst_hop")) {
                netaware_score_mode = FatTreeSwitch::NETAWARE_SCORE_WORST_HOP;
            } else if (!strcmp(argv[i+1], "sglb_quantized")) {
                netaware_score_mode = FatTreeSwitch::NETAWARE_SCORE_SGLB_QUANTIZED;
            } else {
                cerr << "Invalid netaware score mode " << argv[i+1]
                     << "; expected gated, worst_hop, or sglb_quantized" << endl;
                exit(1);
            }
            cout << "netaware score mode " << argv[i+1] << endl;
            i++;
        } else if (!strcmp(argv[i],"-netaware_score_q_range")){
            netaware_score_q_min = atof(argv[i+1]);
            netaware_score_q_max = atof(argv[i+2]);
            if (netaware_score_q_min < 0.0)
                netaware_score_q_min = 0.0;
            if (netaware_score_q_min > 1.0)
                netaware_score_q_min = 1.0;
            if (netaware_score_q_max < 0.0)
                netaware_score_q_max = 0.0;
            if (netaware_score_q_max > 1.0)
                netaware_score_q_max = 1.0;
            if (netaware_score_q_max < netaware_score_q_min)
                netaware_score_q_max = netaware_score_q_min;
            cout << "netaware score queue range "
                 << netaware_score_q_min << "/" << netaware_score_q_max << endl;
            i += 2;
        } else if (!strcmp(argv[i],"-netaware_path_coupling")){
            if (!strcmp(argv[i+1], "additive")) {
                netaware_path_coupling =
                    FatTreeSwitch::NETAWARE_PATH_COUPLING_ADDITIVE;
            } else if (!strcmp(argv[i+1], "bottleneck")) {
                netaware_path_coupling =
                    FatTreeSwitch::NETAWARE_PATH_COUPLING_BOTTLENECK;
            } else if (!strcmp(argv[i+1], "noisy_or")) {
                netaware_path_coupling =
                    FatTreeSwitch::NETAWARE_PATH_COUPLING_NOISY_OR;
            } else {
                cerr << "Invalid netaware path coupling " << argv[i+1]
                     << "; expected additive, bottleneck, or noisy_or" << endl;
                exit(1);
            }
            cout << "netaware path coupling " << argv[i+1] << endl;
            i++;
        } else if (!strcmp(argv[i],"-netaware_score_util_range")){
            netaware_score_util_low = atof(argv[i+1]);
            netaware_score_util_high = atof(argv[i+2]);
            if (netaware_score_util_low < 0.0)
                netaware_score_util_low = 0.0;
            if (netaware_score_util_low > 1.0)
                netaware_score_util_low = 1.0;
            if (netaware_score_util_high < 0.0)
                netaware_score_util_high = 0.0;
            if (netaware_score_util_high > 1.0)
                netaware_score_util_high = 1.0;
            if (netaware_score_util_high < netaware_score_util_low)
                netaware_score_util_high = netaware_score_util_low;
            cout << "netaware score utilization range "
                 << netaware_score_util_low << "/" << netaware_score_util_high << endl;
            i += 2;
        } else if (!strcmp(argv[i],"-netaware_score_weights")){
            netaware_score_weight_local_q = atof(argv[i+1]);
            netaware_score_weight_remote_q = atof(argv[i+2]);
            netaware_score_weight_local_util = atof(argv[i+3]);
            netaware_score_weight_remote_util = atof(argv[i+4]);
            if (netaware_score_weight_local_q < 0.0)
                netaware_score_weight_local_q = 0.0;
            if (netaware_score_weight_remote_q < 0.0)
                netaware_score_weight_remote_q = 0.0;
            if (netaware_score_weight_local_util < 0.0)
                netaware_score_weight_local_util = 0.0;
            if (netaware_score_weight_remote_util < 0.0)
                netaware_score_weight_remote_util = 0.0;
            cout << "netaware score weights "
                 << netaware_score_weight_local_q << "/"
                 << netaware_score_weight_remote_q << "/"
                 << netaware_score_weight_local_util << "/"
                 << netaware_score_weight_remote_util << endl;
            i += 4;
        } else if (!strcmp(argv[i],"-netaware_score_level_thresholds")){
            netaware_score_degraded_threshold = atof(argv[i+1]);
            netaware_score_bad_threshold = atof(argv[i+2]);
            netaware_score_avoid_threshold = atof(argv[i+3]);
            if (netaware_score_degraded_threshold < 0.0)
                netaware_score_degraded_threshold = 0.0;
            if (netaware_score_bad_threshold < 0.0)
                netaware_score_bad_threshold = 0.0;
            if (netaware_score_avoid_threshold < 0.0)
                netaware_score_avoid_threshold = 0.0;
            if (netaware_score_bad_threshold < netaware_score_degraded_threshold)
                netaware_score_bad_threshold = netaware_score_degraded_threshold;
            if (netaware_score_avoid_threshold < netaware_score_bad_threshold)
                netaware_score_avoid_threshold = netaware_score_bad_threshold;
            cout << "netaware score level thresholds "
                 << netaware_score_degraded_threshold << "/"
                 << netaware_score_bad_threshold << "/"
                 << netaware_score_avoid_threshold << endl;
            i += 3;
        } else if (!strcmp(argv[i],"-netaware_level_weights")){
            uint32_t good = atoi(argv[i+1]);
            uint32_t degraded = atoi(argv[i+2]);
            uint32_t bad = atoi(argv[i+3]);
            uint32_t avoid = atoi(argv[i+4]);
            RoceSrc::setNetawareLevelWeights(good, degraded, bad, avoid);
            cout << "netaware level weights good " << good
                 << " degraded " << degraded
                 << " bad " << bad
                 << " avoid " << avoid << endl;
            i += 4;
        } else if (!strcmp(argv[i],"-netaware_wrr_mode")){
            if (!strcmp(argv[i+1], "shuffled_bucket")) {
                RoceSrc::setNetawareWrrMode(RoceSrc::NETAWARE_WRR_SHUFFLED_BUCKET);
                cout << "netaware WRR mode shuffled_bucket" << endl;
            } else if (!strcmp(argv[i+1], "bucket")) {
                RoceSrc::setNetawareWrrMode(RoceSrc::NETAWARE_WRR_BUCKET);
                cout << "netaware WRR mode bucket" << endl;
            } else if (!strcmp(argv[i+1], "direct")) {
                RoceSrc::setNetawareWrrMode(RoceSrc::NETAWARE_WRR_DIRECT);
                cout << "netaware WRR mode direct" << endl;
            } else if (!strcmp(argv[i+1], "topk")) {
                RoceSrc::setNetawareWrrMode(RoceSrc::NETAWARE_WRR_TOPK);
                cout << "netaware WRR mode topk" << endl;
            } else {
                cerr << "Invalid netaware WRR mode " << argv[i+1]
                     << "; expected shuffled_bucket, bucket, direct, or topk" << endl;
                exit(1);
            }
            i++;
        } else if (!strcmp(argv[i],"-netaware_topk")){
            uint32_t k = atoi(argv[i+1]);
            RoceSrc::setNetawareTopK(k);
            cout << "netaware top-k size " << RoceSrc::netawareTopK() << endl;
            i++;
        } else if (!strcmp(argv[i],"-netaware_weight_adaptation")){
            if (i + 1 >= argc) {
                cerr << "Missing netaware weight adaptation mode" << endl;
                exit(1);
            }
            if (!strcmp(argv[i+1], "off")) {
                RoceSrc::setNetawareWeightAdaptation(
                    RoceSrc::NETAWARE_WEIGHT_ADAPTATION_OFF);
            } else if (!strcmp(argv[i+1], "good_share_cap")) {
                RoceSrc::setNetawareWeightAdaptation(
                    RoceSrc::NETAWARE_WEIGHT_ADAPTATION_GOOD_SHARE_CAP);
            } else {
                cerr << "Invalid netaware weight adaptation " << argv[i+1]
                     << "; expected off or good_share_cap" << endl;
                exit(1);
            }
            cout << "netaware weight adaptation "
                 << RoceSrc::netawareWeightAdaptationName() << endl;
            i++;
        } else if (!strcmp(argv[i],"-netaware_trace_prefix")){
            netaware_trace_prefix = argv[i+1];
            cout << "netaware trace prefix " << netaware_trace_prefix << endl;
            i++;
        } else if (!strcmp(argv[i],"-netaware_trace_period_us")){
            netaware_trace_period_us = atof(argv[i+1]);
            if (netaware_trace_period_us <= 0.0)
                netaware_trace_period_us = 0.1;
            cout << "netaware path trace period " << netaware_trace_period_us << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-nmrc_reroute_policy")) {
            if (i + 1 >= argc) {
                cerr << "missing value for -nmrc_reroute_policy" << endl;
                exit(1);
            }
            if (!strcmp(argv[i+1], "any_better"))
                nmrc_reroute_policy = FatTreeSwitch::NMRC_REROUTE_ANY_BETTER;
            else if (!strcmp(argv[i+1], "better_ge3"))
                nmrc_reroute_policy = FatTreeSwitch::NMRC_REROUTE_BETTER_GE3;
            else {
                cerr << "invalid n-MRC reroute policy " << argv[i+1] << endl;
                exit(1);
            }
            nmrc_option_user_set = true;
            nmrc_reroute_policy_user_set = true;
            i++;
        } else if (!strcmp(argv[i],"-nmrc_fastcnp")) {
            if (i + 1 >= argc) {
                cerr << "missing value for -nmrc_fastcnp" << endl;
                exit(1);
            }
            if (!strcmp(argv[i+1], "on"))
                nmrc_fastcnp = true;
            else if (!strcmp(argv[i+1], "off"))
                nmrc_fastcnp = false;
            else {
                cerr << "invalid n-MRC FastCNP mode " << argv[i+1] << endl;
                exit(1);
            }
            nmrc_option_user_set = true;
            nmrc_fastcnp_user_set = true;
            i++;
        } else if (!strcmp(argv[i],"-nmrc_endpoint_policy")) {
            if (i + 1 >= argc) {
                cerr << "missing value for -nmrc_endpoint_policy" << endl;
                exit(1);
            }
            if (!strcmp(argv[i+1], "rr_cooldown"))
                nmrc_endpoint_policy = RoceSrc::NMRC_ENDPOINT_RR_COOLDOWN;
            else if (!strcmp(argv[i+1], "random_stateless"))
                nmrc_endpoint_policy = RoceSrc::NMRC_ENDPOINT_RANDOM_STATELESS;
            else {
                cerr << "invalid n-MRC endpoint policy " << argv[i+1]
                     << "; expected rr_cooldown or random_stateless" << endl;
                exit(1);
            }
            nmrc_option_user_set = true;
            nmrc_endpoint_policy_user_set = true;
            i++;
        } else if (!strcmp(argv[i],"-nmrc_all_cooling_policy")) {
            if (i + 1 >= argc) {
                cerr << "missing value for -nmrc_all_cooling_policy" << endl;
                exit(1);
            }
            if (!strcmp(argv[i+1], "earliest"))
                nmrc_all_cooling_policy =
                    RoceSrc::NMRC_ALL_COOLING_EARLIEST;
            else if (!strcmp(argv[i+1], "rr_reset"))
                nmrc_all_cooling_policy =
                    RoceSrc::NMRC_ALL_COOLING_RR_RESET;
            else {
                cerr << "invalid n-MRC all-cooling policy " << argv[i+1]
                     << "; expected earliest or rr_reset" << endl;
                exit(1);
            }
            nmrc_option_user_set = true;
            nmrc_all_cooling_policy_user_set = true;
            i++;
        } else if (!strcmp(argv[i],"-nmrc_network_decision")) {
            if (i + 1 >= argc) {
                cerr << "missing value for -nmrc_network_decision" << endl;
                exit(1);
            }
            if (!strcmp(argv[i+1], "graded"))
                nmrc_network_decision = FatTreeSwitch::NMRC_NETWORK_GRADED;
            else if (!strcmp(argv[i+1], "fixed_threshold"))
                nmrc_network_decision =
                    FatTreeSwitch::NMRC_NETWORK_FIXED_THRESHOLD;
            else if (!strcmp(argv[i+1], "delta"))
                nmrc_network_decision = FatTreeSwitch::NMRC_NETWORK_DELTA;
            else {
                cerr << "invalid n-MRC network decision " << argv[i+1]
                     << "; expected graded, fixed_threshold, or delta"
                     << endl;
                exit(1);
            }
            nmrc_option_user_set = true;
            nmrc_network_decision_user_set = true;
            i++;
        } else if (!strcmp(argv[i],"-nmrc_binary_threshold")) {
            if (i + 1 >= argc) {
                cerr << "missing value for -nmrc_binary_threshold" << endl;
                exit(1);
            }
            char* end = NULL;
            errno = 0;
            nmrc_binary_threshold = strtod(argv[i+1], &end);
            if (errno || end == argv[i+1] || *end != '\0' ||
                !std::isfinite(nmrc_binary_threshold) ||
                nmrc_binary_threshold != 0.5) {
                cerr << "invalid n-MRC binary threshold " << argv[i+1]
                     << "; expected 0.5" << endl;
                exit(1);
            }
            nmrc_option_user_set = true;
            nmrc_binary_threshold_user_set = true;
            i++;
        } else if (!strcmp(argv[i],"-nmrc_absolute_threshold")) {
            if (i + 1 >= argc) {
                cerr << "missing value for -nmrc_absolute_threshold" << endl;
                exit(1);
            }
            if (nmrc_absolute_threshold_user_set) {
                cerr << "-nmrc_absolute_threshold may only be specified once"
                     << endl;
                exit(1);
            }
            char* end = NULL;
            errno = 0;
            double parsed_absolute_threshold = strtod(argv[i+1], &end);
            if (errno || end == argv[i+1] || *end != '\0' ||
                !std::isfinite(parsed_absolute_threshold) ||
                parsed_absolute_threshold <= 0.0 ||
                parsed_absolute_threshold > 1.0) {
                cerr << "invalid n-MRC absolute threshold " << argv[i+1]
                     << "; expected 0 < value <= 1" << endl;
                exit(1);
            }
            nmrc_absolute_threshold = parsed_absolute_threshold;
            nmrc_option_user_set = true;
            nmrc_absolute_threshold_user_set = true;
            i++;
        } else if (!strcmp(argv[i],"-nmrc_graded_cooldown")) {
            if (i + 1 >= argc) {
                cerr << "missing value for -nmrc_graded_cooldown" << endl;
                exit(1);
            }
            if (nmrc_graded_cooldown_user_set) {
                cerr << "-nmrc_graded_cooldown may only be specified once"
                     << endl;
                exit(1);
            }
            if (!strcmp(argv[i+1], "selective"))
                nmrc_graded_cooldown =
                    FatTreeSwitch::NMRC_GRADED_COOLDOWN_SELECTIVE;
            else if (!strcmp(argv[i+1], "full"))
                nmrc_graded_cooldown =
                    FatTreeSwitch::NMRC_GRADED_COOLDOWN_FULL;
            else if (!strcmp(argv[i+1], "none"))
                nmrc_graded_cooldown =
                    FatTreeSwitch::NMRC_GRADED_COOLDOWN_NONE;
            else {
                cerr << "invalid -nmrc_graded_cooldown " << argv[i+1]
                     << "; expected selective, full, or none" << endl;
                exit(1);
            }
            nmrc_option_user_set = true;
            nmrc_graded_cooldown_user_set = true;
            i++;
        } else if (!strcmp(argv[i],"-nmrc_graded_reroute_delta")) {
            if (i + 1 >= argc) {
                cerr << "missing value for -nmrc_graded_reroute_delta"
                     << endl;
                exit(1);
            }
            if (nmrc_graded_reroute_delta_user_set) {
                cerr << "-nmrc_graded_reroute_delta may only be specified once"
                     << endl;
                exit(1);
            }
            char* end = NULL;
            errno = 0;
            double value = strtod(argv[i+1], &end);
            if (errno || end == argv[i+1] || *end != '\0' ||
                !std::isfinite(value) || value < 0.0 || value > 1.0) {
                cerr << "invalid -nmrc_graded_reroute_delta " << argv[i+1]
                     << "; expected 0 <= value <= 1" << endl;
                exit(1);
            }
            nmrc_graded_reroute_delta = value;
            nmrc_option_user_set = true;
            nmrc_graded_reroute_delta_user_set = true;
            i++;
        } else if (!strcmp(argv[i],"-nmrc_relative_delta")) {
            if (i + 1 >= argc) {
                cerr << "missing value for -nmrc_relative_delta" << endl;
                exit(1);
            }
            if (nmrc_relative_delta_user_set) {
                cerr << "-nmrc_relative_delta may only be specified once"
                     << endl;
                exit(1);
            }
            char* end = NULL;
            errno = 0;
            double parsed_relative_delta = strtod(argv[i+1], &end);
            if (errno || end == argv[i+1] || *end != '\0' ||
                !std::isfinite(parsed_relative_delta) ||
                parsed_relative_delta <= 0.0 || parsed_relative_delta > 1.0) {
                cerr << "invalid n-MRC relative delta " << argv[i+1]
                     << "; expected 0 < value <= 1" << endl;
                exit(1);
            }
            nmrc_relative_delta = parsed_relative_delta;
            nmrc_option_user_set = true;
            nmrc_relative_delta_user_set = true;
            i++;
        } else if (!strcmp(argv[i],"-nmrc_route_delta") ||
                   !strcmp(argv[i],"-nmrc_cooldown_delta")) {
            const bool route_option =
                !strcmp(argv[i], "-nmrc_route_delta");
            if (i + 1 >= argc) {
                cerr << "missing value for " << argv[i] << endl;
                exit(1);
            }
            bool& already_set = route_option ? nmrc_route_delta_user_set
                                             : nmrc_cooldown_delta_user_set;
            if (already_set) {
                cerr << argv[i] << " may only be specified once" << endl;
                exit(1);
            }
            char* end = NULL;
            errno = 0;
            double value = strtod(argv[i+1], &end);
            if (errno || end == argv[i+1] || *end != '\0' ||
                !std::isfinite(value) || value <= 0.0 || value > 1.0) {
                cerr << "invalid " << argv[i] << " " << argv[i+1]
                     << "; expected 0 < value <= 1" << endl;
                exit(1);
            }
            if (route_option)
                nmrc_route_delta = value;
            else
                nmrc_cooldown_delta = value;
            already_set = true;
            nmrc_option_user_set = true;
            i++;
        } else if (!strcmp(argv[i],"-avail_ecn_only")) {
            avail_ecn_only = true;
            cout << "avail ECN-only override enabled" << endl;
        } else if (!strcmp(argv[i],"-grade_complex_score")) {
            grade_complex_score = true;
            cout << "grade complex score enabled" << endl;
        } else if (!strcmp(argv[i],"-stor_score_profile")){
            stor_score_profile_user_set = true;
            if (!strcmp(argv[i+1], "original") ||
                !strcmp(argv[i+1], "image") ||
                !strcmp(argv[i+1], "legacy")) {
                FatTreeSwitch::set_stor_score_profile(
                    FatTreeSwitch::STOR_SCORE_PROFILE_ORIGINAL);
            } else if (!strcmp(argv[i+1], "balanced") ||
                       !strcmp(argv[i+1], "current") ||
                       !strcmp(argv[i+1], "default")) {
                FatTreeSwitch::set_stor_score_profile(
                    FatTreeSwitch::STOR_SCORE_PROFILE_BALANCED);
            } else if (!strcmp(argv[i+1], "simple")) {
                FatTreeSwitch::set_stor_score_profile(
                    FatTreeSwitch::STOR_SCORE_PROFILE_SIMPLE);
            } else if (!strcmp(argv[i+1], "binary")) {
                FatTreeSwitch::set_stor_score_profile(
                    FatTreeSwitch::STOR_SCORE_PROFILE_BINARY);
            } else {
                cerr << "Unknown STOR score profile " << argv[i+1]
                     << "; expected original, balanced, simple, or binary" << endl;
                exit(1);
            }
            RoceSrc::setStorBinarySelector(
                FatTreeSwitch::_stor_score_profile ==
                    FatTreeSwitch::STOR_SCORE_PROFILE_BINARY);
            cout << "stor score profile "
                 << FatTreeSwitch::stor_score_profile_name() << endl;
            i++;
        } else if (!strcmp(argv[i],"-stor_score_params")){
            stor_score_profile_user_set = true;
            int clean = atoi(argv[i+1]);
            int ecn_add = atoi(argv[i+2]);
            int trim_add = atoi(argv[i+3]);
            int ecn_base = atoi(argv[i+4]);
            int trim_base = atoi(argv[i+5]);
            if (clean < 0) clean = 0;
            if (ecn_add < 0) ecn_add = 0;
            if (trim_add < 0) trim_add = 0;
            if (ecn_base < 0) ecn_base = 0;
            if (trim_base < 0) trim_base = 0;
            if (clean > 255) clean = 255;
            if (ecn_add > 255) ecn_add = 255;
            if (trim_add > 255) trim_add = 255;
            if (ecn_base > 255) ecn_base = 255;
            if (trim_base > 255) trim_base = 255;
            FatTreeSwitch::_stor_clean_gain = (uint8_t)clean;
            FatTreeSwitch::_stor_ecn_acc_add = (uint8_t)ecn_add;
            FatTreeSwitch::_stor_trim_acc_add = (uint8_t)trim_add;
            FatTreeSwitch::_stor_ecn_base_penalty = (uint8_t)ecn_base;
            FatTreeSwitch::_stor_trim_base_penalty = (uint8_t)trim_base;
            FatTreeSwitch::set_stor_score_profile(
                FatTreeSwitch::STOR_SCORE_PROFILE_CUSTOM);
            RoceSrc::setStorBinarySelector(false);
            cout << "stor score params clean " << clean
                 << " ecn_add " << ecn_add
                 << " trim_add " << trim_add
                 << " ecn_base " << ecn_base
                 << " trim_base " << trim_base << endl;
            i += 5;
        } else if (!strcmp(argv[i],"-stor_simple_score_params")) {
            stor_score_profile_user_set = true;
            int clean = atoi(argv[i+1]);
            int penalty = atoi(argv[i+2]);
            int good = atoi(argv[i+3]);
            int degraded = atoi(argv[i+4]);
            int bad = atoi(argv[i+5]);
            if (clean < 0) clean = 0;
            if (penalty < 0) penalty = 0;
            if (good < 0) good = 0;
            if (degraded < 0) degraded = 0;
            if (bad < 0) bad = 0;
            if (clean > 15) clean = 15;
            if (penalty > 15) penalty = 15;
            if (good > 15) good = 15;
            if (degraded > good) degraded = good;
            if (bad > degraded) bad = degraded;
            FatTreeSwitch::set_stor_score_profile(
                FatTreeSwitch::STOR_SCORE_PROFILE_SIMPLE);
            RoceSrc::setStorBinarySelector(false);
            FatTreeSwitch::_stor_simple_clean_gain = (uint8_t)clean;
            FatTreeSwitch::_stor_simple_congestion_penalty = (uint8_t)penalty;
            FatTreeSwitch::_stor_good_threshold = (uint8_t)good;
            FatTreeSwitch::_stor_degraded_threshold = (uint8_t)degraded;
            FatTreeSwitch::_stor_bad_threshold = (uint8_t)bad;
            cout << "stor simple score params clean " << clean
                 << " penalty " << penalty
                 << " thresholds " << good << "/" << degraded
                 << "/" << bad << endl;
            i += 5;
        } else if (!strcmp(argv[i],"-stor_level_thresholds")){
            stor_score_profile_user_set = true;
            int good = atoi(argv[i+1]);
            int degraded = atoi(argv[i+2]);
            int bad = atoi(argv[i+3]);
            if (good < 0) good = 0;
            if (degraded < 0) degraded = 0;
            if (bad < 0) bad = 0;
            if (good > 255) good = 255;
            if (degraded > 255) degraded = 255;
            if (bad > 255) bad = 255;
            if (degraded > good)
                degraded = good;
            if (bad > degraded)
                bad = degraded;
            FatTreeSwitch::_stor_good_threshold = (uint8_t)good;
            FatTreeSwitch::_stor_degraded_threshold = (uint8_t)degraded;
            FatTreeSwitch::_stor_bad_threshold = (uint8_t)bad;
            FatTreeSwitch::set_stor_score_profile(
                FatTreeSwitch::STOR_SCORE_PROFILE_CUSTOM);
            RoceSrc::setStorBinarySelector(false);
            cout << "stor level thresholds good " << good
                 << " degraded " << degraded
                 << " bad " << bad << endl;
            i += 3;
        } else if (!strcmp(argv[i],"-stor_level_weights")){
            uint32_t good = atoi(argv[i+1]);
            uint32_t degraded = atoi(argv[i+2]);
            uint32_t bad = atoi(argv[i+3]);
            uint32_t avoid = atoi(argv[i+4]);
            RoceSrc::setStorLevelWeights(good, degraded, bad, avoid);
            cout << "stor level weights good " << good
                 << " degraded " << degraded
                 << " bad " << bad
                 << " avoid " << avoid << endl;
            i += 4;
        }
         else if (!strcmp(argv[i],"-lossless_ecn_kb")){
            lossless_ecn_kmin_bytes = strtoull(argv[i+1], NULL, 10) * 1024;
            lossless_ecn_kmax_bytes = strtoull(argv[i+2], NULL, 10) * 1024;
            cout << "Lossless ECN thresholds " << lossless_ecn_kmin_bytes
                 << " bytes " << lossless_ecn_kmax_bytes << " bytes" << endl;
            i += 2;
         } else if (!strcmp(argv[i],"-lossless_ecn_bytes")){
            lossless_ecn_kmin_bytes = strtoull(argv[i+1], NULL, 10);
            lossless_ecn_kmax_bytes = strtoull(argv[i+2], NULL, 10);
            cout << "Lossless ECN thresholds " << lossless_ecn_kmin_bytes
                 << " bytes " << lossless_ecn_kmax_bytes << " bytes" << endl;
            i += 2;
         } else if (!strcmp(argv[i],"-pfc")){
            if (!strcmp(argv[i+1], "off"))
                pfc_enabled = false;
            else if (!strcmp(argv[i+1], "on"))
                pfc_enabled = true;
            else {
                cout << "Unknown PFC mode " << argv[i+1] << endl;
                exit_error(argv[0]);
            }
            cout << "PFC " << (pfc_enabled ? "enabled" : "disabled") << endl;
            i++;
         } else if (!strcmp(argv[i],"-pfc_thresholds")){
            low_pfc = atoi(argv[i+1]);
            high_pfc = atoi(argv[i+2]);
            pfc_user_set = true;
            cout << "PFC thresholds high " << high_pfc << " low " << low_pfc << endl;
            i+=2;
        } else if (!strcmp(argv[i],"-ar_method")){
            if (!strcmp(argv[i+1],"pause")){
                cout << "Adaptive routing based on pause state " << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_pause;
            }
            else if (!strcmp(argv[i+1],"queue")){
                cout << "Adaptive routing based on queue size " << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_queuesize;
            }
            else if (!strcmp(argv[i+1],"bandwidth")){
                cout << "Adaptive routing based on bandwidth utilization " << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_bandwidth;
            }
            else if (!strcmp(argv[i+1],"flowcount")){
                cout << "Adaptive routing based on bandwidth utilization " << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_flow_count;
            }
            else if (!strcmp(argv[i+1],"pqb")){
                cout << "Adaptive routing based on pause, queuesize and bandwidth utilization " << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_pqb;
            }
            else if (!strcmp(argv[i+1],"pq")){
                cout << "Adaptive routing based on pause, queuesize" << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_pq;
            }
            else if (!strcmp(argv[i+1],"pb")){
                cout << "Adaptive routing based on pause, bandwidth utilization" << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_pb;
            }
            else if (!strcmp(argv[i+1],"qb")){
                cout << "Adaptive routing based on queuesize, bandwidth utilization" << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_qb; 
            }
            else {
                cout << "Unknown AR method expecting one of pause, queue, bandwidth, pqb, pq, pb, qb" << endl;
                exit(1);
            }
            i++;
        }  else if (!strcmp(argv[i],"-strat")){
            if (!strcmp(argv[i+1], "perm")) {
                route_strategy = SCATTER_PERMUTE;
            } else if (!strcmp(argv[i+1], "rand")) {
                route_strategy = SCATTER_RANDOM;
            } else if (!strcmp(argv[i+1], "ecmp")) {
                route_strategy = SCATTER_ECMP;
            } else if (!strcmp(argv[i+1], "pull")) {
                route_strategy = PULL_BASED;
            } else if (!strcmp(argv[i+1], "single")) {
                route_strategy = SINGLE_PATH;
            } else if (!strcmp(argv[i+1], "ecmp_host")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
            } else if (!strcmp(argv[i+1], "ecmp_ar")) {
                route_strategy = ECMP_FIB;
                path_entropy_size = 1;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ADAPTIVE_ROUTING);
            } else if (!strcmp(argv[i+1], "ecmp_host_ar")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP_ADAPTIVE);
                FatTreeSwitch::set_ar_fraction(atoi(argv[i+2]));
                cout << "AR fraction: " << atoi(argv[i+2]) << endl;
                i++;
            } else if (!strcmp(argv[i+1], "ecmp_rr")) {
                route_strategy = ECMP_FIB;
                path_entropy_size = 1;
                FatTreeSwitch::set_strategy(FatTreeSwitch::RR);
            }
            i++;
        } else {
            exit_error(argv[0]);
        }
                
        i++;
    }

    srand(seed);
    srandom(seed);
    RoceSrc::setNmrcEvSeed(seed);

    if (grade_complex_score && lb_scheme_name != "grade") {
        cerr << "grade complex override requires -lb grade" << endl;
        exit(1);
    }
    if (avail_ecn_only && lb_scheme_name != "avail") {
        cerr << "avail ECN-only override requires -lb avail" << endl;
        exit(1);
    }
    if (nmrc_relative_delta_user_set &&
        lb_scheme_name != "n-mrc-fixed0.5" &&
        lb_scheme_name != "n-mrc-delta") {
        cerr << "-nmrc_relative_delta requires -lb n-mrc-fixed0.5 or n-mrc-delta" << endl;
        exit(1);
    }
    if (nmrc_absolute_threshold_user_set &&
        lb_scheme_name != "n-mrc-fixed0.5") {
        cerr << "-nmrc_absolute_threshold requires -lb n-mrc-fixed0.5" << endl;
        exit(1);
    }
    if ((nmrc_graded_cooldown_user_set ||
         nmrc_graded_reroute_delta_user_set) &&
        lb_scheme_name != "n-mrc") {
        cerr << "graded n-MRC controls require -lb n-mrc" << endl;
        exit(1);
    }
    if (nmrc_route_delta_user_set || nmrc_cooldown_delta_user_set) {
        cerr << "-nmrc_route_delta and -nmrc_cooldown_delta are reserved for future work" << endl;
        exit(1);
    }
    if (nmrc_option_user_set && roce_lb_mode != RoceSrc::LB_NMRC) {
        cerr << "n-MRC options require an N-MRC load-balancing preset" << endl;
        exit(1);
    }
    if (roce_lb_mode == RoceSrc::LB_NMRC && lb_scheme_name != "n-mrc") {
        RoceSrc::nmrc_endpoint_policy_t required_endpoint =
            RoceSrc::NMRC_ENDPOINT_RR_COOLDOWN;
        RoceSrc::nmrc_all_cooling_policy_t required_all_cooling =
            RoceSrc::NMRC_ALL_COOLING_EARLIEST;
        FatTreeSwitch::NmrcNetworkDecisionMode required_network =
            FatTreeSwitch::NMRC_NETWORK_GRADED;
        bool required_fastcnp = true;

        if (lb_scheme_name == "n-mrc-fixed0.5")
            required_network = FatTreeSwitch::NMRC_NETWORK_FIXED_THRESHOLD;
        else if (lb_scheme_name == "n-mrc-delta")
            required_network = FatTreeSwitch::NMRC_NETWORK_DELTA;

        const char* reroute_policy_name =
            nmrc_reroute_policy == FatTreeSwitch::NMRC_REROUTE_ANY_BETTER ?
                "any_better" : "better_ge3";
        const char* endpoint_policy_name =
            nmrc_endpoint_policy == RoceSrc::NMRC_ENDPOINT_RR_COOLDOWN ?
                "rr_cooldown" : "random_stateless";
        const char* all_cooling_policy_name =
            nmrc_all_cooling_policy == RoceSrc::NMRC_ALL_COOLING_EARLIEST ?
                "earliest" : "rr_reset";
        const char* network_decision_name =
            nmrc_network_decision_name(nmrc_network_decision);
        const char* required_endpoint_name =
            required_endpoint == RoceSrc::NMRC_ENDPOINT_RR_COOLDOWN ?
                "rr_cooldown" : "random_stateless";
        const char* required_all_cooling_name =
            required_all_cooling == RoceSrc::NMRC_ALL_COOLING_EARLIEST ?
                "earliest" : "rr_reset";
        const char* required_network_name =
            nmrc_network_decision_name(required_network);

        if ((lb_scheme_name == "n-mrc-fixed0.5" ||
             lb_scheme_name == "n-mrc-delta") &&
            nmrc_reroute_policy_user_set) {
            cerr << "n-MRC preset " << lb_scheme_name
                 << " does not allow -nmrc_reroute_policy" << endl;
            exit(1);
        }
        if (nmrc_reroute_policy_user_set &&
            nmrc_reroute_policy != FatTreeSwitch::NMRC_REROUTE_BETTER_GE3) {
            cerr << "n-MRC preset " << lb_scheme_name
                 << " conflicts with -nmrc_reroute_policy "
                 << reroute_policy_name << "; requires better_ge3" << endl;
            exit(1);
        }
        if (nmrc_fastcnp_user_set && nmrc_fastcnp != required_fastcnp) {
            cerr << "n-MRC preset " << lb_scheme_name
                 << " conflicts with -nmrc_fastcnp "
                 << (nmrc_fastcnp ? "on" : "off") << "; requires "
                 << (required_fastcnp ? "on" : "off") << endl;
            exit(1);
        }
        if (nmrc_endpoint_policy_user_set &&
            nmrc_endpoint_policy != required_endpoint) {
            cerr << "n-MRC preset " << lb_scheme_name
                 << " conflicts with -nmrc_endpoint_policy "
                 << endpoint_policy_name << "; requires "
                 << required_endpoint_name << endl;
            exit(1);
        }
        if (nmrc_all_cooling_policy_user_set &&
            nmrc_all_cooling_policy != required_all_cooling) {
            cerr << "n-MRC preset " << lb_scheme_name
                 << " conflicts with -nmrc_all_cooling_policy "
                 << all_cooling_policy_name << "; requires "
                 << required_all_cooling_name << endl;
            exit(1);
        }
        if (nmrc_network_decision_user_set &&
            nmrc_network_decision != required_network) {
            cerr << "n-MRC preset " << lb_scheme_name
                 << " conflicts with -nmrc_network_decision "
                 << network_decision_name << "; requires "
                 << required_network_name << endl;
            exit(1);
        }
        if ((lb_scheme_name == "n-mrc-fixed0.5" ||
             lb_scheme_name == "n-mrc-delta") &&
            nmrc_binary_threshold_user_set) {
            cerr << "n-MRC preset " << lb_scheme_name
                 << " does not allow -nmrc_binary_threshold" << endl;
            exit(1);
        }
        if (nmrc_binary_threshold_user_set && nmrc_binary_threshold != 0.5) {
            cerr << "n-MRC preset " << lb_scheme_name
                 << " conflicts with -nmrc_binary_threshold "
                 << nmrc_binary_threshold << "; requires 0.5" << endl;
            exit(1);
        }

        nmrc_reroute_policy = FatTreeSwitch::NMRC_REROUTE_BETTER_GE3;
        nmrc_fastcnp = required_fastcnp;
        nmrc_endpoint_policy = required_endpoint;
        nmrc_all_cooling_policy = required_all_cooling;
        nmrc_network_decision = required_network;
        nmrc_binary_threshold = 0.5;
    }
    if (roce_lb_mode == RoceSrc::LB_NMRC &&
        nmrc_network_decision == FatTreeSwitch::NMRC_NETWORK_BINARY_SCORE &&
        !nmrc_fastcnp) {
        cerr << "n-MRC binary_score network decision requires "
             << "-nmrc_fastcnp on" << endl;
        exit(1);
    }
    RoceSrc::setNmrcEndpointPolicy(nmrc_endpoint_policy);
    RoceSrc::setNmrcAllCoolingPolicy(nmrc_all_cooling_policy);
    FatTreeSwitch::_stor_binary_trim_bad = !avail_ecn_only;
    if (lb_scheme_name == "avail") {
        if (stor_score_profile_user_set &&
            FatTreeSwitch::_stor_score_profile !=
                FatTreeSwitch::STOR_SCORE_PROFILE_BINARY) {
            cerr << "avail requires the binary availability profile" << endl;
            exit(1);
        }
        FatTreeSwitch::set_stor_score_profile(
            FatTreeSwitch::STOR_SCORE_PROFILE_BINARY);
        RoceSrc::setStorBinarySelector(true);
    } else if (lb_scheme_name == "grade") {
        if (stor_score_profile_user_set &&
            FatTreeSwitch::_stor_score_profile ==
                FatTreeSwitch::STOR_SCORE_PROFILE_BINARY) {
            cerr << "grade requires a graded score profile" << endl;
            exit(1);
        }
        if (grade_complex_score && stor_score_profile_user_set) {
            cerr << "grade complex override cannot be combined with an explicit score profile" << endl;
            exit(1);
        }
        if (!stor_score_profile_user_set)
            FatTreeSwitch::set_stor_score_profile(
                grade_complex_score ?
                FatTreeSwitch::STOR_SCORE_PROFILE_BALANCED :
                FatTreeSwitch::STOR_SCORE_PROFILE_SIMPLE);
        RoceSrc::setStorBinarySelector(false);
    }

    cout << "Parsed args\n";
    if (sglb_background)
        cout << "SGLB background links per direction "
             << sglb_bg_links_per_direction << endl;
    if (!sglb_bg_packet_size)
        sglb_bg_packet_size = packet_size;
    Packet::set_packet_size(packet_size);

    if (roce_lb_mode == RoceSrc::LB_MRC) {
        if (!queue_type_user_set) {
            qt = COMPOSITE_ECN_LB;
            cout << "MRC default queue_type composite_ecn_lb (trim + priority headers + ECN)" << endl;
        }
        if (!roce_rx_mode_user_set) {
            roce_rx_mode = RoceSrc::RX_SP_RETX_QUEUE;
            cout << "MRC default receive mode sp (SACK/selective retransmission)" << endl;
        }
        if (!roce_cc_mode_user_set) {
            roce_cc_mode = RoceSrc::CC_DCTCP_VARIANT;
            cout << "MRC default cc dctcp_variant" << endl;
        }
        if (!ecn_thresh_user_set) {
            ecn_thresh = 0.8;
            cout << "MRC default composite RED ECN Kmax fraction " << ecn_thresh << endl;
        }
    }

    if (roce_lb_mode == RoceSrc::LB_STOR) {
        if (!queue_type_user_set) {
            qt = COMPOSITE_ECN_LB;
            cout << lb_scheme_name << " default queue_type composite_ecn_lb (trim + priority headers + ECN)" << endl;
        }
        if (!roce_rx_mode_user_set) {
            roce_rx_mode = RoceSrc::RX_SP_RETX_QUEUE;
            cout << lb_scheme_name << " default receive mode sp (SACK/selective retransmission)" << endl;
        }
        if (!roce_cc_mode_user_set) {
            roce_cc_mode = RoceSrc::CC_DCTCP_VARIANT;
            cout << lb_scheme_name << " default cc dctcp_variant" << endl;
        }
        if (!ecn_thresh_user_set) {
            ecn_thresh = 0.8;
            cout << lb_scheme_name << " default composite RED ECN Kmax fraction " << ecn_thresh << endl;
        }
    }

    if (roce_lb_mode == RoceSrc::LB_NETAWARE) {
        if (!queue_type_user_set) {
            qt = COMPOSITE_ECN_LB;
            cout << "NetAware default queue_type composite_ecn_lb (trim + priority headers + ECN)" << endl;
        }
        if (!roce_rx_mode_user_set) {
            roce_rx_mode = RoceSrc::RX_SP_RETX_QUEUE;
            cout << "NetAware default receive mode sp (SACK/selective retransmission)" << endl;
        }
        if (!roce_cc_mode_user_set) {
            roce_cc_mode = RoceSrc::CC_DCTCP_VARIANT;
            cout << "NetAware default cc dctcp_variant" << endl;
        }
        if (!ecn_thresh_user_set) {
            ecn_thresh = 0.8;
            cout << "NetAware default composite RED ECN Kmax fraction " << ecn_thresh << endl;
        }
    }

    if (roce_lb_mode == RoceSrc::LB_NMRC) {
        if (!queue_type_user_set) {
            qt = COMPOSITE_ECN_LB;
            cout << "n-MRC default queue_type composite_ecn_lb (trim + priority headers + ECN)" << endl;
        }
        if (!roce_rx_mode_user_set) {
            roce_rx_mode = RoceSrc::RX_SP_RETX_QUEUE;
            cout << "n-MRC default receive mode sp (SACK/selective retransmission)" << endl;
        }
        if (!roce_cc_mode_user_set) {
            roce_cc_mode = RoceSrc::CC_DCTCP_VARIANT;
            cout << "n-MRC default cc dctcp_variant" << endl;
        }
        if (!ecn_thresh_user_set) {
            ecn_thresh = 0.8;
            cout << "n-MRC default composite RED ECN Kmax fraction " << ecn_thresh << endl;
        }
    }

    if (roce_transport_semantics ==
            RoceSrc::TRANSPORT_MRC_EXACT_BOUNDED &&
        roce_rx_mode != RoceSrc::RX_SP_RETX_QUEUE) {
        if (roce_rx_mode_user_set) {
            cerr << "mrc_exact_bounded transport requires -roce_rx_mode sp"
                 << endl;
            exit(1);
        }
        roce_rx_mode = RoceSrc::RX_SP_RETX_QUEUE;
        cout << "mrc_exact_bounded default receive mode sp "
             << "(SACK/selective retransmission)" << endl;
    }
    if (roce_transport_semantics ==
            RoceSrc::TRANSPORT_MRC_EXACT_BOUNDED &&
        roce_trim_recovery_user_set &&
        RoceSrc::trimRecoveryMode() != RoceSrc::TRIM_RECOVERY_EXACT_PSN) {
        cerr << "mrc_exact_bounded transport requires exact Trim recovery; "
             << "use legacy for cumulative recovery" << endl;
        exit(1);
    }

    source_pathid_lb = (roce_lb_mode == RoceSrc::LB_STOR ||
                        roce_lb_mode == RoceSrc::LB_NETAWARE ||
                        roce_lb_mode == RoceSrc::LB_NMRC ||
                        roce_lb_mode == RoceSrc::LB_MRC ||
                        roce_lb_mode == RoceSrc::LB_RR ||
                        roce_lb_mode == RoceSrc::LB_CONWEAVE ||
                        roce_lb_mode == RoceSrc::LB_NDP ||
                        roce_lb_mode == RoceSrc::LB_REPS ||
                        roce_lb_mode == RoceSrc::LB_MPRDMA ||
                        roce_lb_mode == RoceSrc::LB_CONGA);
    if (source_pathid_lb && !path_entropy_user_set && path_entropy_size > 10000) {
        path_entropy_size = 1;
        cout << "source-controlled LB will auto-calibrate path count from topology" << endl;
    }

    uint32_t one_way_links = tiers == 3 ? 6 : 4;
    uint32_t one_way_switches = tiers == 3 ? 5 : 3;
    double estimated_rtt_us = 2.0 * (one_way_links * timeAsUs(hop_latency) +
                                     one_way_switches * timeAsUs(switch_latency));
    uint64_t estimated_bdp_bytes = roce_bdp_bytes;
    if (!estimated_bdp_bytes) {
        estimated_bdp_bytes = (uint64_t)(((double)linkspeed * estimated_rtt_us * 1e-6) / 8.0);
    }
    uint32_t estimated_bdp_pkts = (uint32_t)ceil(
        (double)estimated_bdp_bytes / Packet::data_packet_size());
    if (!estimated_bdp_pkts)
        estimated_bdp_pkts = 1;
    if (!queue_user_set) {
        queuesize = estimated_bdp_pkts;
        cout << "reps default queue size 1BDP = " << queuesize
             << " packets (" << estimated_bdp_bytes << " bytes, estimated RTT "
             << estimated_rtt_us << "us)" << endl;
    }

    if (!pfc_user_set) {
        low_pfc = (uint64_t)ceil((double)queuesize * 0.2);
        high_pfc = (uint64_t)ceil((double)queuesize * 0.8);
        if (!low_pfc)
            low_pfc = 1;
        if (high_pfc <= low_pfc)
            high_pfc = low_pfc + 1;
        cout << "Default PFC thresholds scaled with queue: high "
             << high_pfc << " low " << low_pfc << " packets" << endl;
    }

    if (stor_feedback_max_us < stor_feedback_min_us)
        stor_feedback_max_us = stor_feedback_min_us;

    if (roce_rx_mode == RoceSrc::RX_SP_RETX_QUEUE) {
        if (!roce_rto_user_set) {
            roce_rto_us = 70;
        }
        if (!roce_rto_high_user_set)
            roce_rto_high_us = 0;
        if (!roce_ooo_user_set) {
            double kmax_drain_us = ((double)estimated_bdp_bytes * 0.8 * 8.0) /
                                   ((double)linkspeed * 1e-6);
            roce_ooo_us = estimated_rtt_us + kmax_drain_us;
        }
        if (!roce_ooo_window_user_set && !roce_ooo_ratio_user_set) {
            roce_ooo_window_ratio = 1.0;
        }
        cout << "RoCE SP effective RTO " << roce_rto_us << "us";
        if (roce_rto_high_us)
            cout << " (high " << roce_rto_high_us << "us)";
        cout << ", OOO tolerance " << roce_ooo_us
             << "us, NACK interval " << roce_nack_interval_us << "us" << endl;
        cout << "RoCE SP SACK feedback uses aPSN + bitmap_start_psn + valid_len + "
             << roce_sack_bitmap_bits << "-bit data bitmap" << endl;
    }

    if (roce_ooo_window_ratio > 0.0) {
        uint64_t bdp_bytes = roce_bdp_bytes;
        if (!bdp_bytes) {
            bdp_bytes = estimated_bdp_bytes;
            cout << "RoCE estimated RTT " << estimated_rtt_us
                 << "us, BDP " << bdp_bytes << " bytes" << endl;
        }
        roce_ooo_window_pkts = (uint32_t)ceil((roce_ooo_window_ratio * bdp_bytes) /
                                              Packet::data_packet_size());
        if (!roce_ooo_window_pkts)
            roce_ooo_window_pkts = 1;
        cout << "RoCE loss trace window from ratio " << roce_ooo_window_ratio
             << " = " << roce_ooo_window_pkts << " packets" << endl;
        if (roce_ooo_window_pkts > roce_sack_bitmap_bits) {
            cout << "RoCE loss trace window exceeds one SACK bitmap; "
                 << "offsetted SACK blocks report " << roce_sack_bitmap_bits
                 << " packets at a time" << endl;
        }
    } else if (roce_ooo_window_user_set && roce_ooo_window_pkts > roce_sack_bitmap_bits) {
        cout << "RoCE loss trace window " << roce_ooo_window_pkts
             << " packets exceeds one SACK bitmap; offsetted SACK blocks report "
             << roce_sack_bitmap_bits << " packets at a time" << endl;
    }

    FatTreeSwitch::_ar_sticky = ar_granularity;
    FatTreeSwitch::_sticky_delta = timeFromUs(ar_sticky_delta);
    FatTreeSwitch::_ecn_threshold_fraction = ecn_thresh;
    FatTreeSwitch::_lossless_ecn_kmin_bytes = lossless_ecn_kmin_bytes;
    FatTreeSwitch::_lossless_ecn_kmax_bytes = lossless_ecn_kmax_bytes;
    FatTreeSwitch::_shared_buffer_bytes = shared_buffer_mb * 1024 * 1024;
    FatTreeSwitch::_shared_ingress_alpha = shared_ingress_alpha;
    FatTreeSwitch::_shared_egress_alpha = shared_egress_alpha;
    if (!shared_headroom_bytes) {
        shared_headroom_bytes = (uint64_t)(
            2.0 * (double)linkspeed * timeAsSec(hop_latency) / 8.0) +
            2ULL * (uint64_t)(packet_size + 48);
        cout << "Shared-buffer per-port headroom " << shared_headroom_bytes
             << " bytes (2*link-flight + 2*(payload+48))" << endl;
    }
    FatTreeSwitch::_shared_headroom_bytes = shared_headroom_bytes;
    FatTreeSwitch::_stor_feedback_min_interval = timeFromUs(stor_feedback_min_us);
    FatTreeSwitch::_stor_feedback_max_interval = timeFromUs(stor_feedback_max_us);
    FatTreeSwitch::_stor_trim_feedback_min_interval =
        timeFromUs(stor_trim_feedback_min_us);
    FatTreeSwitch::set_stor_aging_profile(stor_aging);
    FatTreeSwitch::_stor_time_ecn_tau = timeFromUs(stor_time_ecn_tau_us);
    FatTreeSwitch::_stor_time_trim_tau = timeFromUs(stor_time_trim_tau_us);
    FatTreeSwitch::_stor_time_score_tau = timeFromUs(stor_time_score_tau_us);
    FatTreeSwitch::_stor_hybrid_bad_hold =
        timeFromUs(stor_hybrid_bad_hold_us);
    FatTreeSwitch::_stor_hybrid_avoid_hold =
        timeFromUs(stor_hybrid_avoid_hold_us);
    FatTreeSwitch::_stor_hybrid_probe_interval_pkts =
        stor_hybrid_probe_interval_pkts;
    FatTreeSwitch::_stor_hybrid_probe_clean_promote =
        stor_hybrid_probe_clean_promote;
    FatTreeSwitch::_netaware_degraded_queue_fraction = netaware_degraded_queue_threshold;
    FatTreeSwitch::_netaware_bad_queue_fraction = netaware_bad_queue_threshold;
    FatTreeSwitch::_netaware_queue_threshold_fraction = netaware_queue_threshold;
    FatTreeSwitch::_netaware_util_queue_floor_fraction = netaware_util_queue_floor_threshold;
    FatTreeSwitch::_netaware_degraded_utilization_fraction = netaware_degraded_utilization_threshold;
    FatTreeSwitch::_netaware_score_mode = netaware_score_mode;
    FatTreeSwitch::_netaware_path_coupling = netaware_path_coupling;
    FatTreeSwitch::_netaware_score_q_min = netaware_score_q_min;
    FatTreeSwitch::_netaware_score_q_max = netaware_score_q_max;
    FatTreeSwitch::_netaware_score_util_low = netaware_score_util_low;
    FatTreeSwitch::_netaware_score_util_high = netaware_score_util_high;
    FatTreeSwitch::_netaware_score_weight_local_q = netaware_score_weight_local_q;
    FatTreeSwitch::_netaware_score_weight_remote_q = netaware_score_weight_remote_q;
    FatTreeSwitch::_netaware_score_weight_local_util = netaware_score_weight_local_util;
    FatTreeSwitch::_netaware_score_weight_remote_util = netaware_score_weight_remote_util;
    FatTreeSwitch::_netaware_score_degraded_threshold = netaware_score_degraded_threshold;
    FatTreeSwitch::_netaware_score_bad_threshold = netaware_score_bad_threshold;
    FatTreeSwitch::_netaware_score_avoid_threshold = netaware_score_avoid_threshold;
    FatTreeSwitch::_pathid_only_hash = source_pathid_lb;
    FatTreeSwitch::_nmrc_hybrid_enabled =
        roce_lb_mode == RoceSrc::LB_NMRC;
    FatTreeSwitch::_nmrc_fastcnp_enabled = nmrc_fastcnp;
    FatTreeSwitch::_nmrc_reroute_policy = nmrc_reroute_policy;
    FatTreeSwitch::_nmrc_network_decision_mode = nmrc_network_decision;
    FatTreeSwitch::_nmrc_graded_cooldown_mode = nmrc_graded_cooldown;
    FatTreeSwitch::_nmrc_graded_reroute_delta =
        nmrc_graded_reroute_delta;
    FatTreeSwitch::_nmrc_absolute_threshold = nmrc_absolute_threshold;
    FatTreeSwitch::_nmrc_relative_delta = nmrc_relative_delta;
    FatTreeSwitch::_nmrc_route_delta = nmrc_route_delta;
    FatTreeSwitch::_nmrc_cooldown_delta = nmrc_cooldown_delta;
    if (roce_lb_mode == RoceSrc::LB_NMRC) {
        FatTreeSwitch::_sglb_score_mode =
            FatTreeSwitch::SGLB_SCORE_NMRC_QUANTIZED_TOPK;
        FatTreeSwitch::_sglb_nmrc_levels = 4;
    }
    if (FatTreeSwitch::_strategy == FatTreeSwitch::SGLB) {
        cout << "SGLB effective config: score mode "
             << FatTreeSwitch::sglb_score_mode_name()
             << ", OFAT factor " << FatTreeSwitch::sglb_ofat_factor_name()
             << ", local quality update "
             << timeAsUs(FatTreeSwitch::_sglb_update_interval)
             << "us, GCN update " << timeAsUs(FatTreeSwitch::_sglb_gcn_update_interval)
             << "us, aging " << timeAsUs(FatTreeSwitch::_sglb_gcn_aging_interval)
             << "us, quality levels " << FatTreeSwitch::_sglb_quality_levels
             << ", bucket " << FatTreeSwitch::_sglb_quality_bucket
             << ", min choices " << FatTreeSwitch::_sglb_min_choices
             << ", continuous topk " << FatTreeSwitch::_sglb_topk
             << ", candidate policy "
             << (FatTreeSwitch::_sglb_candidate_policy ==
                         FatTreeSwitch::SGLB_CANDIDATE_SCORE_TOPK ? "score_topk" :
                (FatTreeSwitch::_sglb_candidate_policy ==
                         FatTreeSwitch::SGLB_CANDIDATE_STRICT_K ? "strict_k" :
                 FatTreeSwitch::_sglb_candidate_policy ==
                         FatTreeSwitch::SGLB_CANDIDATE_WHOLE_GRADE_MIN ?
                         "whole_grade_min" : "exact_min"))
             << ", candidate dispatch "
             << (FatTreeSwitch::_sglb_candidate_dispatch ==
                         FatTreeSwitch::SGLB_DISPATCH_RANDOM ?
                         "random" : "shuffled_rr")
             << ", GCN cadence "
             << (FatTreeSwitch::_sglb_gcn_cadence ==
                         FatTreeSwitch::SGLB_GCN_SYNCHRONIZED ?
                         "synchronized" : "independent")
             << ", n-mrc q range " << FatTreeSwitch::_sglb_nmrc_q_min
             << "/" << FatTreeSwitch::_sglb_nmrc_q_max
             << ", netaware thresholds "
             << FatTreeSwitch::_sglb_nmrc_degraded_threshold << "/"
             << FatTreeSwitch::_sglb_nmrc_bad_threshold << "/"
             << FatTreeSwitch::_sglb_nmrc_avoid_threshold
             << ", nmrc_levels " << FatTreeSwitch::_sglb_nmrc_levels
             << ", downstream weight " << FatTreeSwitch::_sglb_downstream_weight
             << ", local damping " << (FatTreeSwitch::_sglb_local_damping ? "on" : "off")
             << endl;
    }
    if (FatTreeSwitch::_strategy == FatTreeSwitch::PAPER_SGLB) {
        if (tiers != 2) {
            cerr << "sglb-paper requires -tiers 2" << endl;
            exit(1);
        }
        cout << "Paper SGLB effective config: five-factor weights "
             << FatTreeSwitch::_paper_sglb_weight_local_queue << "/"
             << FatTreeSwitch::_paper_sglb_weight_local_util << "/"
             << FatTreeSwitch::_paper_sglb_weight_remote_queue << "/"
             << FatTreeSwitch::_paper_sglb_weight_remote_util << "/"
             << FatTreeSwitch::_paper_sglb_weight_remote_busy
             << ", selector "
             << (FatTreeSwitch::_paper_sglb_selection_mode ==
                     FatTreeSwitch::PAPER_SGLB_TOPK ? "topk" : "best-level")
             << ", remote " << FatTreeSwitch::paper_sglb_remote_mode_name()
             << ", ablation " << FatTreeSwitch::paper_sglb_ablation_name()
             << ", source-leaf-only, 4 AR levels, top-K "
             << FatTreeSwitch::_paper_sglb_k << ", local "
             << timeAsUs(FatTreeSwitch::_paper_sglb_sample_interval)
             << "us, GCN "
             << timeAsUs(FatTreeSwitch::_paper_sglb_gcn_interval)
             << "us, GCN packet 256B high-priority" << endl;
    }

    RoceSrc::setLoadBalancing(roce_lb_mode);
    RoceSrc::setPathEntropySize(path_entropy_size);
    RoceSrc::setSackBitmapBits(roce_sack_bitmap_bits);
    RoceSrc::setReceiveMode(roce_rx_mode);
    RoceSrc::setTransportSemantics(roce_transport_semantics);
    if (roce_transport_semantics ==
            RoceSrc::TRANSPORT_MRC_EXACT_BOUNDED) {
        RoceSrc::setTrimRecoveryMode(RoceSrc::TRIM_RECOVERY_EXACT_PSN);
    }
    RoceSrc::setOooTolerance(timeFromUs(roce_ooo_us));
    RoceSrc::setOooWindowPkts(roce_ooo_window_pkts);
    RoceSrc::setNackInterval(timeFromUs(roce_nack_interval_us));
    RoceSrc::setMinRTO(roce_rto_us);
    RoceSrc::setHighRTO(roce_rto_high_us);
    RoceSrc::setRepsBufferSize(reps_buffer);
    // REPS Section 3.1: explore random entropies during the first BDP worth
    // of packets of a new or idle connection.
    RoceSrc::setRepsWarmupPkts(estimated_bdp_pkts);
    cout << "REPS warmup exploration " << estimated_bdp_pkts
         << " packets (1BDP)" << endl;
    RoceSrc::setConweaveRttThreshold(timeFromUs(conweave_rtt_us));
    RoceSrc::setConweaveMinRerouteGap(timeFromUs(conweave_min_reroute_us));
    RoceSrc::setNdpInitialWindow(ndp_cwnd);
    cout << "FinalCcMrcConfig dctcp_variant_inflate="
         << (roce_transport_semantics ==
                 RoceSrc::TRANSPORT_MRC_EXACT_BOUNDED ?
             "disabled" :
             (roce_cc_mode == RoceSrc::CC_DCTCP_VARIANT_NODUP_OLD ?
                 "natural_nodup_old" : "natural")) << " "
         << "mrc_ecn_trim_penalty=mode_uniform "
         << "roce_trim_recovery=" << RoceSrc::trimRecoveryModeName()
         << endl;
    cout << "RoceTransportConfig semantics="
         << RoceSrc::transportSemanticsName()
         << " awnd="
         << (roce_transport_semantics ==
                 RoceSrc::TRANSPORT_MRC_EXACT_BOUNDED ?
             "cwnd_minus_inflight" : "legacy")
         << " exact_trim_attempt_id="
         << (roce_transport_semantics ==
                 RoceSrc::TRANSPORT_MRC_EXACT_BOUNDED ? "on" : "off")
         << " recovery_reserve_bytes="
         << (roce_transport_semantics ==
                 RoceSrc::TRANSPORT_MRC_EXACT_BOUNDED ? packet_size : 0)
         << endl;
    RoceSrc::setCongestionControl(roce_cc_mode);
    if (roce_cc_mode == RoceSrc::CC_DCQCN)
        RoceSrc::printDcqcnConfiguration(cout);
    if (!cc_iw_pkts) {
        if (roce_cc_mode == RoceSrc::CC_DCQCN ||
            roce_cc_mode == RoceSrc::CC_DCTCP_VARIANT ||
            roce_cc_mode == RoceSrc::CC_DCTCP_VARIANT_NODUP_OLD) {
            cc_iw_pkts = estimated_bdp_pkts;
            if (!cc_iw_pkts)
                cc_iw_pkts = 1;
            cout << "Default RoCE CC flight cap 1BDP = "
                 << cc_iw_pkts << " packets" << endl;
        } else {
            cc_iw_pkts = queuesize ? queuesize : 1;
        }
    }
    RoceSrc::setCcInitialWindow(cc_iw_pkts);
    RoceSrc::setCcMinWindow(cc_min_cwnd_pkts);
    RoceSrc::setCcMaxWindow(cc_max_cwnd_pkts);

    double roce_rto_scan_us = roce_rto_us / 10.0;
    if (roce_rto_scan_us < 1.0)
        roce_rto_scan_us = 1.0;
    RoceRtxTimerScanner roceRtxScanner(timeFromUs(roce_rto_scan_us), eventlist);

    if (!pfc_enabled) {
        // Keep lossless buffering and ECN, but suppress pause generation.
        LosslessInputQueue::_high_threshold = UINT64_MAX;
        LosslessInputQueue::_low_threshold = UINT64_MAX - 1;
    } else {
        LosslessInputQueue::_high_threshold = Packet::data_packet_size()*high_pfc;
        LosslessInputQueue::_low_threshold = Packet::data_packet_size()*low_pfc;
    }
    LosslessInputQueue::_pfc_enabled = pfc_enabled;

    eventlist.setEndtime(timeFromUs((uint32_t)end_time));
    queuesize = memFromPkt(queuesize);
    switch (route_strategy) {
    case ECMP_FIB:
    case SCATTER_ECMP:
        if (path_entropy_size > 10000) {
            fprintf(stderr, "Route Strategy is ecmp.  Must specify path count using -paths\n");
            exit(1);
        }
        break;
    case SINGLE_PATH:
        if (path_entropy_size < 10000 && path_entropy_size > 1) {
            fprintf(stderr, "Route Strategy is SINGLE_PATH, but multiple paths are specifiec using -paths\n");
            exit(1);
        }
        break;
    case NOT_SET:
        fprintf(stderr, "Route Strategy not set.  Use the -strat param.  \nValid values are perm, rand, pull, rg and single\n");
        exit(1);
    default:
        break;
    }

    // prepare the loggers

    cout << "Logging to " << filename.str() << endl;
    //Logfile 
    Logfile logfile(filename.str(), eventlist);

    logfile.setStartTime(timeFromSec(0));

    RoceSinkLoggerSampling sinkLogger = RoceSinkLoggerSampling(timeFromMs(logtime), eventlist);
    if (log_sink) {
        logfile.addLogger(sinkLogger);
    }
    RoceTrafficLogger traffic_logger = RoceTrafficLogger();
    if (log_traffic) {
        logfile.addLogger(traffic_logger);
    }

    RoceSrc* roceSrc;
    RoceSink* roceSnk;

    Route* routeout, *routein;

    QueueLoggerFactory *qlf = 0;
    if (log_tor_downqueue || log_tor_upqueue) {
        qlf = new QueueLoggerFactory(&logfile, QueueLoggerFactory::LOGGER_SAMPLING, eventlist);
        qlf->set_sample_period(timeFromUs(10.0));
    } else if (log_queue_usage) {
        qlf = new QueueLoggerFactory(&logfile, QueueLoggerFactory::LOGGER_EMPTY, eventlist);
        qlf->set_sample_period(timeFromUs(10.0));
    }
#ifdef FAT_TREE
    FatTreeTopology* top;
    if (topo_file) {
        if (slow_core_downlinks) {
            cerr << "-slow_core_downlinks is only supported with generated fat-tree topologies\n";
            exit(1);
        }
        if (slow_tor_uplinks) {
            cerr << "-slow_tor_uplinks is only supported with generated fat-tree topologies\n";
            exit(1);
        }
        top = FatTreeTopology::load(topo_file, qlf, eventlist, queuesize, qt, snd_type);
    } else {
        FatTreeTopology::set_tiers(tiers);
        if (tiers == 2) {
            if (!is_canonical_two_tier_scale(no_of_nodes)) {
                cerr << "Generated 2-tier leaf-spine supports node counts: "
                     << "16, 256, 512, 1024, 2048, 4096, 8192; got "
                     << no_of_nodes << endl;
                exit(1);
            }
            FatTreeTopology::set_two_tier_leaf_spine_radix(
                no_of_nodes == 16 ? 4 : 64);
        }
        FatTreeTopology::set_slow_link_divisor(slow_core_downlink_divisor);
        FatTreeTopology::set_slow_tor_uplinks(slow_tor_uplinks);
        FatTreeTopology::set_slow_tor_uplink_divisor(slow_tor_uplink_divisor);
        FatTreeTopology::set_slow_tor_uplink_random_sparse(slow_tor_uplink_random_sparse);
        FatTreeTopology::set_slow_tor_uplink_seed(seed);
        top = new FatTreeTopology(no_of_nodes, linkspeed, queuesize, qlf, 
                                               &eventlist,NULL,qt,hop_latency,switch_latency,snd_type,slow_core_downlinks);
    }
    if (FatTreeSwitch::_strategy == FatTreeSwitch::PAPER_SGLB ||
        (FatTreeSwitch::_strategy == FatTreeSwitch::SGLB &&
         FatTreeSwitch::_sglb_ofat_factor ==
             FatTreeSwitch::SGLB_OFAT_SHADOW_GCN)) {
        const simtime_picosec paper_delay = paper_sglb_gcn_delay_us >= 0.0 ?
            timeFromUs(paper_sglb_gcn_delay_us) : hop_latency;
        FatTreeSwitch::initialize_paper_sglb(top, paper_delay);
    }
    FatTreeSwitch::initialize_sglb_ofat(top);
    FatTreeSwitch::initialize_sglb_real_gcn_profiles(top);
    if (top->get_tiers() == 2 && top->no_of_nodes() >= 256) {
        cout << "Canonical64TopologyDiag spines=" << top->getNAGG()
             << " leaf_uplinks=" << top->radix_up(TOR_TIER)
             << " leaf_downlinks=" << top->radix_down(TOR_TIER)
             << " hosts_per_leaf=" << top->radix_down(TOR_TIER)
             << " leaves="
             << top->no_of_nodes() / top->radix_down(TOR_TIER)
             << " paths=" << top->radix_up(TOR_TIER)
             << endl;
    }
#endif

#ifdef OV_FAT_TREE
    OversubscribedFatTreeTopology* top = new OversubscribedFatTreeTopology(lf, &eventlist,ff);
#endif

#ifdef MH_FAT_TREE
    MultihomedFatTreeTopology* top = new MultihomedFatTreeTopology(lf, &eventlist,ff);
#endif

#ifdef STAR
    StarTopology* top = new StarTopology(lf, &eventlist,ff);
#endif

#ifdef BCUBE
    BCubeTopology* top = new BCubeTopology(lf, &eventlist,ff);
    cout << "BCUBE " << K << endl;
#endif

#ifdef VL2
    VL2Topology* top = new VL2Topology(lf, &eventlist,ff);
#endif

    if (log_switches) {
        top->add_switch_loggers(logfile, timeFromUs(20.0));
    }

    SglbBackgroundDropSink* sglb_bg_drop = NULL;
    vector<SglbBackgroundSource*> sglb_bg_sources;
    if (sglb_background) {
        sglb_bg_drop = new SglbBackgroundDropSink();
        uint32_t added = install_sglb_background(
            top, eventlist, sglb_bg_sources, sglb_bg_drop,
            sglb_bg_links_per_direction,
            sglb_bg_rate_gbps, sglb_bg_packet_size,
            timeFromUs(sglb_bg_on_us), timeFromUs(sglb_bg_off_us));
        cout << "SGLB background installed " << added << " fixed-link sources" << endl;
    }
    if (path_hotspot_spines) {
        if (top->get_tiers() != 2) {
            cerr << "Path hotspot fixed-link background only supports 2-tier topologies"
                 << endl;
            exit(1);
        }
        if (path_hotspot_bg_rate_gbps <= 0.0 || path_hotspot_bg_on_us <= 0.0) {
            cerr << "Path hotspot background requires positive rate and ON time"
                 << endl;
            exit(1);
        }
        if (!sglb_bg_drop)
            sglb_bg_drop = new SglbBackgroundDropSink();
        uint32_t packet_size = sglb_bg_packet_size ?
            sglb_bg_packet_size : Packet::data_packet_size();
        uint32_t hot_spines = std::min(path_hotspot_spines, top->getNAGG());
        uint32_t added = install_path_hotspot_background(
            top, eventlist, sglb_bg_sources, sglb_bg_drop,
            hot_spines, path_hotspot_bg_rate_gbps, packet_size,
            timeFromUs(path_hotspot_bg_on_us),
            timeFromUs(path_hotspot_bg_off_us));
        cout << "Path hotspot background installed " << added
             << " fixed-link sources hot_spines " << hot_spines
             << " rate " << path_hotspot_bg_rate_gbps
             << "Gbps on " << path_hotspot_bg_on_us
             << "us off " << path_hotspot_bg_off_us << "us" << endl;
    }

    SglbQueueCvSampler* queue_cv_sampler = NULL;
    if (queue_cv_sample_us > 0.0) {
        queue_cv_sampler = new SglbQueueCvSampler(eventlist, top, timeFromUs(queue_cv_sample_us));
        queue_cv_sampler->start();
        cout << "Queue CV sampler enabled every " << queue_cv_sample_us << "us" << endl;
    }

    uint32_t topology_path_combo = top->radix_up(TOR_TIER);
    if (top->get_tiers() == 3) {
        topology_path_combo *= top->radix_up(AGG_TIER);
        topology_path_combo *= top->bundlesize(CORE_TIER);
        topology_path_combo *= top->bundlesize(AGG_TIER);
    } else {
        topology_path_combo *= top->bundlesize(AGG_TIER);
    }
    if (!topology_path_combo)
        topology_path_combo = 1;

    if (source_pathid_lb) {
        if (topology_path_combo != path_entropy_size) {
            cout << "source-controlled tier path count adjusted from " << path_entropy_size
                 << " to " << topology_path_combo
                 << " to match topology path combinations" << endl;
            path_entropy_size = topology_path_combo;
            RoceSrc::setPathEntropySize(path_entropy_size);
        }
    }
    RoceSrc::setDiagPhysicalPathSpace(topology_path_combo);
    RoceSrc::resetPathSelectionDiag();
    RoceSrc::resetStorProfileDiag();
    RoceSrc::resetCongaSharedState();
    FatTreeSwitch::reset_sglb_route_diag();
    FatTreeSwitch::reset_nmrc_hybrid_diag();
    RoceSrc::setHostsPerTor(top->radix_down(TOR_TIER));
    RoceSrc::setCongaUplinks(top->radix_up(TOR_TIER));

    uint32_t path_space = path_entropy_size ? path_entropy_size : 1;
    if ((roce_lb_mode == RoceSrc::LB_MRC ||
         roce_lb_mode == RoceSrc::LB_NMRC) && path_space != 64) {
        cerr << (roce_lb_mode == RoceSrc::LB_NMRC ? "N-MRC" : "MRC")
             << " requires exactly 64 physical paths; topology provides "
             << path_space << endl;
        exit(1);
    }
    FatTreeSwitch::_netaware_path_count = path_space;
    FatTreeSwitch::_netaware_enabled =
        roce_lb_mode == RoceSrc::LB_NETAWARE;
    FatTreeSwitch::_stor_path_count = path_space;
    FatTreeSwitch::_stor_enabled = roce_lb_mode == RoceSrc::LB_STOR;
    uint32_t auto_stor_feedback_pkts = lb_scheme_name == "avail" ?
        FatTreeSwitch::avail_default_feedback_pkts(path_space) :
        FatTreeSwitch::grade_default_feedback_pkts(path_space);
    const double netaware_feedback_min_us =
        FatTreeSwitch::netaware_default_feedback_min_us(path_space);
    const double netaware_feedback_max_us =
        FatTreeSwitch::netaware_default_feedback_max_us(path_space);
    FatTreeSwitch::_netaware_feedback_min_interval = timeFromUs(netaware_feedback_min_us);
    FatTreeSwitch::_netaware_feedback_max_interval = timeFromUs(netaware_feedback_max_us);
    FatTreeSwitch::_netaware_feedback_pkts =
        FatTreeSwitch::netaware_default_feedback_pkts(path_space);
    FatTreeSwitch::_stor_feedback_pkts =
        stor_feedback_pkts_user_set ? stor_feedback_pkts : auto_stor_feedback_pkts;
    if (roce_lb_mode == RoceSrc::LB_NETAWARE) {
        uint32_t netaware_bucket_size =
            RoceSrc::netawareShuffledBucketSize(path_space);
        cout << "netaware canonical: paths " << path_space
             << ", feedback_pkts " << FatTreeSwitch::_netaware_feedback_pkts
             << ", min_interval_us " << netaware_feedback_min_us
             << ", max_interval_us " << netaware_feedback_max_us
             << ", local_update_us "
             << timeAsUs(FatTreeSwitch::_netaware_state_update_interval)
             << ", remote_update_us "
             << timeAsUs(FatTreeSwitch::_netaware_remote_update_interval)
             << ", queue_threshold " << FatTreeSwitch::_netaware_queue_threshold_fraction
             << ", grade_queue_thresholds "
             << FatTreeSwitch::_netaware_degraded_queue_fraction << "/"
             << FatTreeSwitch::_netaware_bad_queue_fraction << "/"
             << FatTreeSwitch::_netaware_queue_threshold_fraction
             << ", util_degrade "
             << FatTreeSwitch::_netaware_util_queue_floor_fraction << "/"
             << FatTreeSwitch::_netaware_degraded_utilization_fraction
             << ", score_mode " << FatTreeSwitch::netaware_score_mode_name()
             << ", path_coupling "
             << FatTreeSwitch::netaware_path_coupling_name()
             << ", score_q_range "
             << FatTreeSwitch::_netaware_score_q_min << "/"
             << FatTreeSwitch::_netaware_score_q_max
             << ", score_util_range "
             << FatTreeSwitch::_netaware_score_util_low << "/"
             << FatTreeSwitch::_netaware_score_util_high
             << ", score_weights "
             << FatTreeSwitch::_netaware_score_weight_local_q << "/"
             << FatTreeSwitch::_netaware_score_weight_remote_q << "/"
             << FatTreeSwitch::_netaware_score_weight_local_util << "/"
             << FatTreeSwitch::_netaware_score_weight_remote_util
             << ", score_level_thresholds "
             << FatTreeSwitch::_netaware_score_degraded_threshold << "/"
             << FatTreeSwitch::_netaware_score_bad_threshold << "/"
             << FatTreeSwitch::_netaware_score_avoid_threshold
             << ", weights "
             << RoceSrc::netawareLevelWeight(STOR_LEVEL_GOOD) << "/"
             << RoceSrc::netawareLevelWeight(STOR_LEVEL_DEGRADED) << "/"
             << RoceSrc::netawareLevelWeight(STOR_LEVEL_BAD) << "/"
             << RoceSrc::netawareLevelWeight(STOR_LEVEL_AVOID)
             << ", wrr_mode "
             << RoceSrc::netawareWrrModeName()
             << ", weight_adaptation "
             << RoceSrc::netawareWeightAdaptationName()
             << ", shuffled_bucket_size "
             << netaware_bucket_size
             << ", selector_impl shared_virtual"
             << ", selector_state per_qp_counter"
             << ", shared_profile tor_pair"
             << endl;
    }
    if (roce_lb_mode == RoceSrc::LB_STOR) {
        cout << lb_scheme_name << " canonical: paths " << path_space
             << ", feedback_pkts " << FatTreeSwitch::_stor_feedback_pkts
             << ", min_interval_us " << stor_feedback_min_us
             << ", max_interval_us " << stor_feedback_max_us
             << ", trim_min_interval_us " << stor_trim_feedback_min_us
             << ", aging " << FatTreeSwitch::stor_aging_profile_name()
             << ", ewma_tau_us "
             << stor_time_ecn_tau_us << "/"
             << stor_time_trim_tau_us << "/"
             << stor_time_score_tau_us
             << ", hybrid_hold_us "
             << stor_hybrid_bad_hold_us << "/"
             << stor_hybrid_avoid_hold_us
             << ", hybrid_probe "
             << stor_hybrid_probe_interval_pkts << "/"
             << stor_hybrid_probe_clean_promote
             << ", score_profile " << FatTreeSwitch::stor_score_profile_name()
             << (lb_scheme_name == "grade" ?
                    (FatTreeSwitch::_stor_score_profile ==
                            FatTreeSwitch::STOR_SCORE_PROFILE_SIMPLE ?
                        ", grade_score_mode simple" :
                        ", grade_score_mode complex") : "")
             << ", score clean " << (uint32_t)FatTreeSwitch::_stor_clean_gain
             << " ecn_add " << (uint32_t)FatTreeSwitch::_stor_ecn_acc_add
             << " trim_add " << (uint32_t)FatTreeSwitch::_stor_trim_acc_add
             << " ecn_base " << (uint32_t)FatTreeSwitch::_stor_ecn_base_penalty
             << " trim_base " << (uint32_t)FatTreeSwitch::_stor_trim_base_penalty
             << ", simple max/clean/penalty "
             << (uint32_t)FatTreeSwitch::_stor_simple_max_score << "/"
             << (uint32_t)FatTreeSwitch::_stor_simple_clean_gain << "/"
             << (uint32_t)FatTreeSwitch::_stor_simple_congestion_penalty
             << ", thresholds "
             << (uint32_t)FatTreeSwitch::_stor_good_threshold << "/"
             << (uint32_t)FatTreeSwitch::_stor_degraded_threshold << "/"
             << (uint32_t)FatTreeSwitch::_stor_bad_threshold
             << ", weights "
             << RoceSrc::storLevelWeight(STOR_LEVEL_GOOD) << "/"
             << RoceSrc::storLevelWeight(STOR_LEVEL_DEGRADED) << "/"
             << RoceSrc::storLevelWeight(STOR_LEVEL_BAD) << "/"
             << RoceSrc::storLevelWeight(STOR_LEVEL_AVOID)
             << ", wrr_mode "
             << (RoceSrc::storBinarySelector() ?
                    "bitmap" : "shuffled_bucket")
             << ", shuffled_bucket_size "
             << RoceSrc::weightedShuffledBucketSize(path_space)
             << ", avoid_probe_interval "
             << RoceSrc::weightedShuffledBucketSize(path_space)
             << ", selector_impl shared_virtual"
             << ", selector_state per_qp_counter"
             << ", shared_profile tor_pair"
             << (RoceSrc::storBinarySelector() ?
                    (FatTreeSwitch::_stor_binary_trim_bad ?
                        ", binary_bad_signal ecn_trim" :
                        ", binary_bad_signal ecn_only") : "")
             << ", endpoint preselected EV set, "
             << (RoceSrc::storBinarySelector() ?
                    (FatTreeSwitch::_stor_binary_trim_bad ?
                        "source-ToR ACK-ECN/TRIM availability feedback" :
                        "source-ToR ACK-ECN availability feedback") :
                    "source-ToR ACK/TRIM graded feedback")
             << endl;
    }
    if (roce_lb_mode == RoceSrc::LB_NMRC) {
        const char* reroute_policy_name =
            nmrc_reroute_policy == FatTreeSwitch::NMRC_REROUTE_ANY_BETTER ?
                "any_better" : "better_ge3";
        uint32_t ev_set_size = 64;
        const char* network_decision_name =
            nmrc_network_decision_name(nmrc_network_decision);
        const char* graded_cooldown_name =
            nmrc_graded_cooldown ==
                    FatTreeSwitch::NMRC_GRADED_COOLDOWN_FULL ?
                "full" :
            (nmrc_graded_cooldown ==
                    FatTreeSwitch::NMRC_GRADED_COOLDOWN_NONE ?
                "none" : "selective");
        if (lb_scheme_name == "n-mrc-fixed0.5" ||
            lb_scheme_name == "n-mrc-delta") {
            cout << "HybridNmrcConfig preset=" << lb_scheme_name
                 << " endpoint_policy=" << RoceSrc::nmrcEndpointPolicyName()
                 << " all_cooling_policy="
                 << RoceSrc::nmrcAllCoolingPolicyName()
                 << " network_decision=" << network_decision_name
                 << " absolute_threshold=" << nmrc_absolute_threshold
                 << " relative_delta=" << nmrc_relative_delta
                 << " fastcnp=" << (nmrc_fastcnp ? "on" : "off")
                 << " reroute_policy=n/a"
                 << " trim_cooldown=actual_path"
                 << " paths=" << path_space
                 << " ev_set_size=" << ev_set_size
                 << " ev_mapping=identity"
                 << endl;
        } else {
            cout << "HybridNmrcConfig reroute_policy=" << reroute_policy_name
                 << " fastcnp=" << (nmrc_fastcnp ? "on" : "off")
                 << " trim_cooldown=actual_path"
                 << " paths=" << path_space
                 << " ev_set_size=" << ev_set_size
                 << " ev_mapping=identity"
                 << " preset=" << lb_scheme_name
                 << " endpoint_policy=" << RoceSrc::nmrcEndpointPolicyName()
                 << " all_cooling_policy="
                 << RoceSrc::nmrcAllCoolingPolicyName()
                 << " network_decision=" << network_decision_name
                 << " graded_cooldown=" << graded_cooldown_name
                 << " graded_reroute_delta="
                 << nmrc_graded_reroute_delta
                 << " binary_threshold=" << nmrc_binary_threshold
                 << endl;
        }
    }
    if (roce_lb_mode == RoceSrc::LB_MRC) {
        mrc_logical_evs = 64;
        mrc_active_paths = 64;
        mrc_backup_paths = 0;
        mrc_min_active_paths = 1;
        uint32_t encoded_universe = 64;
        while (((uint64_t)1 << mrc_path_bits) < encoded_universe &&
               mrc_path_bits < 31)
            mrc_path_bits++;
        cout << "MRC: paths " << path_space
             << ", mrc_ev_model encoded"
             << ", physical_path_space " << path_space
             << ", effective_physical_paths " << path_space
             << ", logical_evs " << mrc_logical_evs
             << ", active_evs " << mrc_active_paths
             << ", backup_evs " << mrc_backup_paths
             << ", path_bits " << mrc_path_bits
             << ", unique_active_physical_paths " << mrc_active_paths
             << ", min_active_paths " << mrc_min_active_paths
             << ", congestion_reaction skip_once"
             << ", failure_recovery disabled"
             << ", source_pathid_mode true"
             << ", ev_path_mapping encoded_identity"
             << ", alias_ratio 1"
             << ", mrc_alias_ratio 1"
             << ", duplicate_physical_aliases false"
             << ", composite_ecn_kmax_fraction " << ecn_thresh << endl;
        cout << "MrcEvModelDiag "
             << "mrc_ev_model=encoded"
             << " mrc_active_evs=" << mrc_active_paths
             << " mrc_backup_evs=" << mrc_backup_paths
             << " mrc_logical_evs=" << mrc_logical_evs
             << " mrc_effective_physical_paths=" << path_space
             << " mrc_path_bits=" << mrc_path_bits
             << " mrc_alias_ratio=1"
             << " mrc_ev_path_mapping=encoded_identity"
             << endl;
        cout << "MrcPolicyDiag policy=skip_once"
             << " all_skip_resolution=ordinary_rotation" << endl;
        cout << "MrcFailureRecoveryDiag enabled=0"
             << " assumed_bad=0 probe_packets=0" << endl;
    }
    if (roce_lb_mode == RoceSrc::LB_RR) {
        cout << "RR: stateless_mrc true"
             << ", physical_path_space " << path_space
             << ", active_evs "
             << path_space
             << ", ev_path_mapping encoded_identity"
             << endl;
    }
    RoceSrc::setMrcFailureRecoveryEnabled(false);
    RoceSrc::setPathEntropySize(path_entropy_size);

    ofstream path_selection_timeline;
    if (!path_selection_timeline_file.empty()) {
        path_selection_timeline.open(path_selection_timeline_file.c_str());
        if (!path_selection_timeline.is_open()) {
            cerr << "Could not open path selection timeline "
                 << path_selection_timeline_file << endl;
            exit(1);
        }
        RoceSrc::configurePathSelectionTimeline(
            &path_selection_timeline, path_selection_timeline_every);
    }

    ofstream netaware_path_trace;
    ofstream netaware_decision_trace;
    NetawarePathStateTracer* netaware_path_tracer = NULL;
    if (!netaware_trace_prefix.empty()) {
        string path_trace_file = netaware_trace_prefix + "_path_state.csv";
        string decision_trace_file = netaware_trace_prefix + "_nic_decisions.csv";
        netaware_path_trace.open(path_trace_file.c_str());
        if (!netaware_path_trace.is_open()) {
            cerr << "Could not open netaware path trace " << path_trace_file << endl;
            exit(1);
        }
        netaware_decision_trace.open(decision_trace_file.c_str());
        if (!netaware_decision_trace.is_open()) {
            cerr << "Could not open netaware decision trace " << decision_trace_file << endl;
            exit(1);
        }

        netaware_path_trace
            << "time_us,src_leaf,dst_leaf,path_id,spine_id,"
            << "q_leaf_to_spine,q_spine_to_dst_leaf,"
            << "q_leaf_to_spine_max,q_spine_to_dst_leaf_max,"
            << "util_leaf_to_spine,util_spine_to_dst_leaf,"
            << "link_rate_leaf_to_spine,link_rate_spine_to_dst_leaf,"
            << "link_down,ecn_marks_on_path,trimmed_packets_on_path,"
            << "dropped_packets_on_path,bytes_sent_on_path,"
            << "local_q_pressure,remote_q_pressure,local_util_pressure,remote_util_pressure,path_score,netaware_level_reason,"
            << "path_grade,path_weight\n";
        netaware_decision_trace
            << "time_us,qp_id,packet_id,selected_ev,selected_path_id,"
            << "selected_grade,selected_weight,bucket_epoch,profile_version,"
            << "selected_bucket_tickets,bucket_allocation\n";
        netaware_path_tracer = new NetawarePathStateTracer(
            eventlist, top, timeFromUs(netaware_trace_period_us),
            path_space, &netaware_path_trace);
        netaware_path_tracer->start();
        RoceSrc::setNetawareDecisionTrace(&netaware_decision_trace);
        cout << "netaware traces enabled: " << path_trace_file
             << " and " << decision_trace_file
             << " every " << netaware_trace_period_us << "us" << endl;
    }

    vector<const Route*>*** net_paths;
    net_paths = new vector<const Route*>**[no_of_nodes];

    int **path_refcounts;
    path_refcounts = new int*[no_of_nodes];

    int* is_dest = new int[no_of_nodes];
    
    for (size_t s = 0; s < no_of_nodes; s++) {
        is_dest[s] = 0;
        net_paths[s] = new vector<const Route*>*[no_of_nodes];
        path_refcounts[s] = new int[no_of_nodes];
        for (size_t d = 0; d < no_of_nodes; d++) {
            net_paths[s][d] = NULL;
            path_refcounts[s][d] = 0;
        }
    }
    
    ConnectionMatrix* conns = new ConnectionMatrix(no_of_nodes);

    if (tm_file){
        cout << "Loading connection matrix from  " << tm_file << endl;

        if (!conns->load(tm_file))
            exit(-1);
    }
    else {
        cout << "Loading connection matrix from  standard input" << endl;        
        conns->load(cin);
    }

    if (conns->N != no_of_nodes){
        cout << "Connection matrix number of nodes is " << conns->N << " while I am using " << no_of_nodes << endl;
        exit(-1);
    }

    // handle link failures specified in the connection matrix.
    for (size_t c = 0; c < conns->failures.size(); c++){
        failure* crt = conns->failures.at(c);

        cout << "Adding link failure switch type" << crt->switch_type << " Switch ID " << crt->switch_id << " link ID "  << crt->link_id << endl;
        top->add_failed_link(crt->switch_type,crt->switch_id,crt->link_id);
    }
    
    vector<connection*>* all_conns;
    
    // used just to print out stats data at the end
    //list <const Route*> routes;

    all_conns = conns->getAllConnections();
    vector <RoceSrc*> roce_srcs;

    for (size_t c = 0; c < all_conns->size(); c++){
        connection* crt = all_conns->at(c);
        int src = crt->src;
        int dest = crt->dst;
        path_refcounts[src][dest]++;
        path_refcounts[dest][src]++;
                        
        if (!net_paths[src][dest]&&route_strategy!=ECMP_FIB) {
            vector<const Route*>* paths = top->get_bidir_paths(src,dest,false);
            net_paths[src][dest] = paths;
            /*
              for (unsigned int i = 0; i < paths->size(); i++) {
              routes.push_back((*paths)[i]);
              }
            */
        }
        if (!net_paths[dest][src]&&route_strategy!=ECMP_FIB) {
            vector<const Route*>* paths = top->get_bidir_paths(dest,src,false);
            net_paths[dest][src] = paths;
        }
    }

    map <flowid_t, TriggerTarget*> flowmap;
    size_t explicit_ecmp_background_flows = 0;

    for (size_t c = 0; c < all_conns->size(); c++){
        connection* crt = all_conns->at(c);
        int src = crt->src;
        int dest = crt->dst;
        cout << "Connection " << crt->src << "->" <<crt->dst << " starting at " << timeAsUs(crt->start)
             << " size " << crt->size;
        if (crt->rate_mbps)
            cout << " rate_mbps " << crt->rate_mbps;
        cout << endl;

        linkspeed_bps src_rate = crt->rate_mbps ? speedFromMbps((uint64_t)crt->rate_mbps) : linkspeed;
        roceSrc = new RoceSrc(NULL, NULL, eventlist, src_rate);
        if (log_traffic)
            roceSrc->set_traffic_logger(&traffic_logger);
        roceRtxScanner.registerRoce(*roceSrc);

        roce_srcs.push_back(roceSrc);
        roceSrc->set_src(src);
        roceSrc->set_dst(dest);
                        
        if (crt->size>0){
            roceSrc->set_flowsize(crt->size);
        }

        if (crt->flowid) {
            roceSrc->set_flowid(crt->flowid);
            assert(flowmap.find(crt->flowid) == flowmap.end()); // don't have dups
            flowmap[crt->flowid] = roceSrc;
        }

        if (crt->ecmp_override) {
            roceSrc->set_flow_ecmp_override(true);
            explicit_ecmp_background_flows++;
        } else if (mixed_lb_traffic) {
            roceSrc->set_flow_ecmp_override(c % 10 == 0);
        }

        if (crt->trigger) {
            Trigger* trig = conns->getTrigger(crt->trigger, eventlist);
            trig->add_target(*roceSrc);
        }
        if (crt->send_done_trigger) {
            Trigger* trig = conns->getTrigger(crt->send_done_trigger, eventlist);
            roceSrc->set_end_trigger(*trig);
        }

        roceSnk = new RoceSink();
                        
        roceSrc->setName("Roce_" + ntoa(src) + "_" + ntoa(dest));

        logfile.writeName(*roceSrc);

        roceSnk->set_src(src);
                        
        roceSnk->setName("Roce_sink_" + ntoa(src) + "_" + ntoa(dest));
        logfile.writeName(*roceSnk);
                        
        ((HostQueue*)top->queues_ns_nlp[src][top->HOST_POD_SWITCH(src)][0])->addHostSender(roceSrc);

        if (route_strategy!=SINGLE_PATH && route_strategy!=ECMP_FIB){
            abort();
        } else if (route_strategy==ECMP_FIB) {
            Route* srctotor = new Route();
            
            srctotor->push_back(top->queues_ns_nlp[src][top->HOST_POD_SWITCH(src)][0]);
            srctotor->push_back(top->pipes_ns_nlp[src][top->HOST_POD_SWITCH(src)][0]);
            srctotor->push_back(top->queues_ns_nlp[src][top->HOST_POD_SWITCH(src)][0]->getRemoteEndpoint());

            Route* dsttotor = new Route();
            dsttotor->push_back(top->queues_ns_nlp[dest][top->HOST_POD_SWITCH(dest)][0]);
            dsttotor->push_back(top->pipes_ns_nlp[dest][top->HOST_POD_SWITCH(dest)][0]);
            dsttotor->push_back(top->queues_ns_nlp[dest][top->HOST_POD_SWITCH(dest)][0]->getRemoteEndpoint());


            if (crt->start != TRIGGER_START && start_delta > 0){
                crt->start += timeFromUs(drand48()*start_delta);
                cout << "Start is " << timeAsUs(crt->start) << endl;
            }
            roceSrc->connect(srctotor, dsttotor, *roceSnk, crt->start);

            //register src and snk to receive packets from their respective TORs. 
            assert(top->switches_lp[top->HOST_POD_SWITCH(src)]);
            assert(top->switches_lp[top->HOST_POD_SWITCH(src)]);
            top->switches_lp[top->HOST_POD_SWITCH(src)]->addHostPort(src,roceSrc->flow_id(),roceSrc);
            top->switches_lp[top->HOST_POD_SWITCH(dest)]->addHostPort(dest,roceSrc->flow_id(),roceSnk);
            if (debug_flow_ids.find(roceSrc->flow_id()) != debug_flow_ids.end())
                roceSrc->log_me();
        } else {
            int choice = rand()%net_paths[src][dest]->size();
            routeout = new Route(*(net_paths[src][dest]->at(choice)));
            routeout->add_endpoints(roceSrc, roceSnk);
            routeout->push_back(roceSnk);
                                
            routein = new Route(*top->get_bidir_paths(dest,src,false)->at(choice));
            routein->add_endpoints(roceSnk, roceSrc);
            routein->push_back(roceSrc);
            roceSrc->connect(routeout, routein, *roceSnk, timeFromUs((uint32_t)rand()%20));
        }

        path_refcounts[src][dest]--;
        path_refcounts[dest][src]--;

        // free up the routes if no other connection needs them 
        if (path_refcounts[src][dest] == 0 && net_paths[src][dest]) {
            vector<const Route*>::iterator i;
            for (i = net_paths[src][dest]->begin(); i != net_paths[src][dest]->end(); i++) {
                if ((*i)->reverse())
                    delete (*i)->reverse();
                delete *i;
            }
            delete net_paths[src][dest];
        }
        if (path_refcounts[dest][src] == 0 && net_paths[dest][src]) {
            vector<const Route*>::iterator i;
            for (i = net_paths[dest][src]->begin(); i != net_paths[dest][src]->end(); i++) {
                if ((*i)->reverse())
                    delete (*i)->reverse();
                delete *i;
            }
            delete net_paths[dest][src];
        }

        if (log_sink) {
            sinkLogger.monitorSink(roceSnk);
        }
    }

    if (mixed_lb_traffic) {
        size_t background_flows = (all_conns->size() + 9) / 10;
        cout << "MixedLbDiag enabled=on total_flows=" << all_conns->size()
             << " main_flows=" << all_conns->size() - background_flows
             << " ecmp_background_flows=" << background_flows << endl;
    }
    if (explicit_ecmp_background_flows) {
        cout << "ExplicitLbDiag ecmp_background_flows="
             << explicit_ecmp_background_flows << endl;
    }

    for (size_t ix = 0; ix < no_of_nodes; ix++) {
        delete path_refcounts[ix];
    }

    Logged::dump_idmap();
    // Record the setup
    int pktsize = Packet::data_packet_size();
    logfile.write("# pktsize=" + ntoa(pktsize) + " bytes");
    logfile.write("# hostnicrate = " + ntoa(linkspeed/1000000) + " Mbps");
    //logfile.write("# corelinkrate = " + ntoa(HOST_NIC*CORE_TO_HOST) + " pkt/sec");
    //logfile.write("# buffer = " + ntoa((double) (queues_na_ni[0][1]->_maxsize) / ((double) pktsize)) + " pkt");
    double rtt = timeAsSec(timeFromUs(RTT));
    logfile.write("# rtt =" + ntoa(rtt));
    
    // GO!
    cout << "Starting simulation" << endl;
    while (eventlist.doNextEvent()) {
    }

    cout << "Done" << endl;
    int new_pkts = 0, rtx_pkts = 0;
    uint64_t ack_pkts = 0, nack_pkts = 0;
    uint64_t ooo_nacks = 0, trim_nacks = 0, trim_lh_nacks = 0;
    uint64_t loss_nacks = 0;
    uint64_t ecn_echo_acks = 0, feedback_acks = 0, feedback_nacks = 0;
    uint64_t duplicate_acks = 0, duplicate_inflate_suppressed = 0;
    uint64_t bounded_inflight_final = 0, bounded_unique_acks = 0;
    uint64_t bounded_recovery_inflight_final_bytes = 0;
    uint32_t bounded_recovery_inflight_max_bytes = 0;
    uint64_t bounded_stale_attempt_nacks = 0;
    uint64_t bounded_duplicate_failure_nacks = 0;
    uint64_t bounded_duplicate_confirmations_suppressed = 0;
    uint64_t bounded_acked_revival_rejected = 0;
    uint64_t bounded_attempt_wraps = 0;
    uint64_t bounded_exact_trim_recoveries = 0;
    uint64_t bounded_sack_loss_recoveries = 0;
    uint64_t feedback_zero_bits = 0;
    uint64_t netaware_ev_skips = 0, netaware_bitmap_fallbacks = 0;
    uint64_t stor_selected_good = 0, stor_selected_degraded = 0;
    uint64_t stor_selected_bad = 0, stor_selected_avoid = 0;
    uint64_t reps_random_sends = 0, reps_cached_sends = 0;
    uint64_t reps_clean_ack_cached = 0, reps_ecn_ack_discarded = 0;
    uint64_t reps_buffer_occupancy_samples = 0;
    map<uint32_t, uint64_t> reps_buffer_occupancy_hist;
    uint64_t mrc_ecn_cooldown_events = 0;
    uint64_t mrc_trim_events = 0, mrc_rto_fail_events = 0;
    uint64_t mrc_trim_cooling_events = 0;
    uint64_t mrc_nack_ooo_ignored_for_failure = 0;
    uint64_t mrc_nack_loss_fail_events = 0;
    uint64_t mrc_nack_unknown_ignored_for_failure = 0;
    uint64_t mrc_failure_backup_promotions = 0;
    uint64_t mrc_forced_cooling_use = 0;
    uint64_t mrc_forced_cooling_earliest_use = 0;
    uint64_t mrc_forced_cooling_round_robin_use = 0;
    uint64_t mrc_feedback_exact_ev_events = 0;
    uint64_t mrc_feedback_sequence_fallback_events = 0;
    uint64_t mrc_feedback_physical_fallback_events = 0;
    uint64_t mrc_feedback_cumulative_mismatch_events = 0;
    uint64_t mrc_cycle_cooling_events = 0;
    uint64_t mrc_cycle_cooling_expiries = 0;
    uint64_t mrc_cooling_skip_selection_sum = 0;
    uint64_t mrc_cooling_skip_selection_events = 0;
    uint64_t mrc_duplicate_feedback_ignored = 0;
    uint64_t mrc_skip_opportunities_consumed = 0;
    uint64_t mrc_data_on_non_good_violations = 0;
    uint64_t mrc_probe_events = 0, mrc_backup_replacement_events = 0;
    uint64_t mrc_probe_success_events = 0, mrc_probe_fail_events = 0;
    uint64_t mrc_backup_replacement_diff_physical = 0;
    uint64_t mrc_retx_original_physical = 0;
    uint64_t mrc_retx_different_physical = 0;
    uint64_t mrc_retx_same_physical_new_ev = 0;
    uint64_t mrc_retx_different_physical_new_ev = 0;
    uint64_t mrc_retx_unknown_original = 0;
    uint64_t mrc_retx_fallback_events = 0;
    uint64_t nmrc_fastcnp_arrived = 0;
    uint64_t nmrc_fastcnp_after_done = 0;
    uint64_t nmrc_fastcnp_arrived_bytes = 0;
    uint64_t nmrc_fastcnp_latency_sum = 0;
    uint64_t nmrc_fastcnp_unknown_qp = 0;
    uint64_t nmrc_fastcnp_unknown_ev = 0;
    uint64_t nmrc_fastcnp_cc_mutations = 0;
    uint64_t nmrc_ecn_nominal_ce = 0;
    uint64_t nmrc_ecn_detour_ce = 0;
    uint64_t nmrc_trim_non_detour = 0;
    uint64_t nmrc_trim_detour = 0;
    uint64_t nmrc_trim_nominal_cooldown_starts = 0;
    uint64_t nmrc_trim_actual_cooldown_starts = 0;
    uint64_t nmrc_trim_duplicate_stale_ignored = 0;
    uint64_t nmrc_trim_actual_unresolved = 0;
    uint64_t nmrc_cooldown_starts = 0;
    uint64_t nmrc_cooling_skips = 0;
    uint64_t nmrc_cooling_recoveries = 0;
    uint64_t nmrc_duplicate_notifications = 0;
    uint64_t nmrc_all_cooling_fallbacks = 0;
    uint64_t nmrc_all_cooling_rr_episodes = 0;
    uint64_t nmrc_all_cooling_rr_selections = 0;
    uint64_t nmrc_all_cooling_rr_resets = 0;
    uint64_t nmrc_fastcnp_policy_ignored = 0;
    uint64_t nmrc_trim_policy_ignored = 0;
    uint64_t mrc_state_samples = 0, mrc_active_count_sum = 0;
    uint64_t mrc_backup_count_sum = 0, mrc_cooling_count_sum = 0;
    uint64_t mrc_failed_count_sum = 0;
    uint64_t mrc_active_physical_count_sum = 0;
    uint64_t mrc_cooling_physical_count_sum = 0;
    uint64_t mrc_failed_physical_count_sum = 0;
    uint32_t mrc_active_count_max = 0, mrc_backup_count_max = 0;
    uint32_t mrc_cooling_count_max = 0, mrc_failed_count_max = 0;
    uint32_t mrc_active_physical_count_max = 0;
    uint32_t mrc_cooling_physical_count_max = 0;
    uint32_t mrc_failed_physical_count_max = 0;
    std::array<uint64_t, 5> mrc_final_state_counts = {{0, 0, 0, 0, 0}};
    std::array<uint64_t, 5> mrc_final_physical_state_counts = {{0, 0, 0, 0, 0}};
    uint64_t mrc_backup_remaining_final = 0;
    map<uint32_t, uint64_t> mrc_ecn_physical_hist;
    map<uint32_t, uint64_t> mrc_trim_physical_hist;
    map<uint32_t, uint64_t> mrc_nack_physical_hist;
    map<uint32_t, uint64_t> mrc_ooo_nack_physical_hist;
    map<uint32_t, uint64_t> mrc_loss_nack_physical_hist;
    for (size_t ix = 0; ix < roce_srcs.size(); ix++) {
        new_pkts += roce_srcs[ix]->_new_packets_sent;
        rtx_pkts += roce_srcs[ix]->_rtx_packets_sent;
        ack_pkts += roce_srcs[ix]->_acks_received;
        nack_pkts += roce_srcs[ix]->_nacks_received;
        ooo_nacks += roce_srcs[ix]->_ooo_nacks_received;
        trim_nacks += roce_srcs[ix]->_trim_nacks_received;
        trim_lh_nacks += roce_srcs[ix]->_trim_lh_nacks_received;
        loss_nacks += roce_srcs[ix]->_loss_nacks_received;
        ecn_echo_acks += roce_srcs[ix]->_ecn_echo_acks_received;
        duplicate_acks += roce_srcs[ix]->_duplicate_acks_received;
        duplicate_inflate_suppressed +=
            roce_srcs[ix]->_duplicate_ack_inflate_suppressed;
        bounded_inflight_final += roce_srcs[ix]->_bounded_inflight_pkts;
        bounded_unique_acks += roce_srcs[ix]->_bounded_unique_acks;
        bounded_recovery_inflight_final_bytes +=
            roce_srcs[ix]->_bounded_recovery_inflight_bytes;
        bounded_recovery_inflight_max_bytes = max(
            bounded_recovery_inflight_max_bytes,
            roce_srcs[ix]->_bounded_recovery_inflight_max_bytes);
        bounded_stale_attempt_nacks +=
            roce_srcs[ix]->_bounded_stale_attempt_nacks;
        bounded_duplicate_failure_nacks +=
            roce_srcs[ix]->_bounded_duplicate_failure_nacks;
        bounded_duplicate_confirmations_suppressed +=
            roce_srcs[ix]->_bounded_duplicate_confirmations_suppressed;
        bounded_acked_revival_rejected +=
            roce_srcs[ix]->_bounded_acked_revival_rejected;
        bounded_attempt_wraps += roce_srcs[ix]->_bounded_attempt_wraps;
        bounded_exact_trim_recoveries +=
            roce_srcs[ix]->_bounded_exact_trim_recoveries;
        bounded_sack_loss_recoveries +=
            roce_srcs[ix]->_bounded_sack_loss_recoveries;
        feedback_acks += roce_srcs[ix]->_feedback_acks_received;
        feedback_nacks += roce_srcs[ix]->_feedback_nacks_received;
        feedback_zero_bits += roce_srcs[ix]->_feedback_zero_bits_received;
        netaware_ev_skips += roce_srcs[ix]->_netaware_ev_skips;
        netaware_bitmap_fallbacks += roce_srcs[ix]->_netaware_bitmap_fallbacks;
        stor_selected_good += roce_srcs[ix]->_stor_selected_good;
        stor_selected_degraded += roce_srcs[ix]->_stor_selected_degraded;
        stor_selected_bad += roce_srcs[ix]->_stor_selected_bad;
        stor_selected_avoid += roce_srcs[ix]->_stor_selected_avoid;
        reps_random_sends += roce_srcs[ix]->_reps_random_sends;
        reps_cached_sends += roce_srcs[ix]->_reps_cached_sends;
        reps_clean_ack_cached += roce_srcs[ix]->_reps_clean_ack_cached;
        reps_ecn_ack_discarded += roce_srcs[ix]->_reps_ecn_ack_discarded;
        reps_buffer_occupancy_samples += roce_srcs[ix]->_reps_buffer_occupancy_samples;
        for (map<uint32_t, uint64_t>::const_iterator it =
                 roce_srcs[ix]->_reps_buffer_occupancy_hist.begin();
             it != roce_srcs[ix]->_reps_buffer_occupancy_hist.end(); ++it) {
            reps_buffer_occupancy_hist[it->first] += it->second;
        }
        mrc_ecn_cooldown_events += roce_srcs[ix]->_mrc_ecn_cooldown_events;
        mrc_trim_events += roce_srcs[ix]->_mrc_trim_events;
        mrc_rto_fail_events += roce_srcs[ix]->_mrc_rto_fail_events;
        mrc_trim_cooling_events += roce_srcs[ix]->_mrc_trim_cooling_events;
        mrc_nack_ooo_ignored_for_failure +=
            roce_srcs[ix]->_mrc_nack_ooo_ignored_for_failure;
        mrc_nack_loss_fail_events +=
            roce_srcs[ix]->_mrc_nack_loss_fail_events;
        mrc_nack_unknown_ignored_for_failure +=
            roce_srcs[ix]->_mrc_nack_unknown_ignored_for_failure;
        mrc_failure_backup_promotions +=
            roce_srcs[ix]->_mrc_failure_backup_promotions;
        mrc_forced_cooling_use += roce_srcs[ix]->_mrc_forced_cooling_use;
        mrc_forced_cooling_earliest_use +=
            roce_srcs[ix]->_mrc_forced_cooling_earliest_use;
        mrc_forced_cooling_round_robin_use +=
            roce_srcs[ix]->_mrc_forced_cooling_round_robin_use;
        mrc_feedback_exact_ev_events +=
            roce_srcs[ix]->_mrc_feedback_exact_ev_events;
        mrc_feedback_sequence_fallback_events +=
            roce_srcs[ix]->_mrc_feedback_sequence_fallback_events;
        mrc_feedback_physical_fallback_events +=
            roce_srcs[ix]->_mrc_feedback_physical_fallback_events;
        mrc_feedback_cumulative_mismatch_events +=
            roce_srcs[ix]->_mrc_feedback_cumulative_mismatch_events;
        mrc_cycle_cooling_events +=
            roce_srcs[ix]->_mrc_cycle_cooling_events;
        mrc_cycle_cooling_expiries +=
            roce_srcs[ix]->_mrc_cycle_cooling_expiries;
        mrc_cooling_skip_selection_sum +=
            roce_srcs[ix]->_mrc_cooling_skip_selection_sum;
        mrc_cooling_skip_selection_events +=
            roce_srcs[ix]->_mrc_cooling_skip_selection_events;
        mrc_duplicate_feedback_ignored +=
            roce_srcs[ix]->_mrc_duplicate_feedback_ignored;
        mrc_skip_opportunities_consumed +=
            roce_srcs[ix]->_mrc_skip_opportunities_consumed;
        mrc_data_on_non_good_violations +=
            roce_srcs[ix]->_mrc_data_on_non_good_violations;
        mrc_probe_events += roce_srcs[ix]->_mrc_probe_events;
        mrc_probe_success_events += roce_srcs[ix]->_mrc_probe_success_events;
        mrc_probe_fail_events += roce_srcs[ix]->_mrc_probe_fail_events;
        mrc_backup_replacement_events += roce_srcs[ix]->_mrc_backup_replacement_events;
        mrc_backup_replacement_diff_physical +=
            roce_srcs[ix]->_mrc_backup_replacement_diff_physical;
        mrc_retx_original_physical += roce_srcs[ix]->_mrc_retx_original_physical;
        mrc_retx_different_physical += roce_srcs[ix]->_mrc_retx_different_physical;
        mrc_retx_same_physical_new_ev +=
            roce_srcs[ix]->_mrc_retx_same_physical_new_ev;
        mrc_retx_different_physical_new_ev +=
            roce_srcs[ix]->_mrc_retx_different_physical_new_ev;
        mrc_retx_unknown_original += roce_srcs[ix]->_mrc_retx_unknown_original;
        mrc_retx_fallback_events += roce_srcs[ix]->_mrc_retx_fallback_events;
        nmrc_fastcnp_arrived += roce_srcs[ix]->_nmrc_fastcnp_arrived;
        nmrc_fastcnp_after_done +=
            roce_srcs[ix]->_nmrc_fastcnp_after_done;
        nmrc_fastcnp_arrived_bytes += roce_srcs[ix]->_nmrc_fastcnp_bytes;
        nmrc_fastcnp_latency_sum +=
            roce_srcs[ix]->_nmrc_fastcnp_latency_sum;
        nmrc_fastcnp_unknown_qp +=
            roce_srcs[ix]->_nmrc_fastcnp_unknown_qp;
        nmrc_fastcnp_unknown_ev +=
            roce_srcs[ix]->_nmrc_fastcnp_unknown_ev;
        nmrc_fastcnp_cc_mutations +=
            roce_srcs[ix]->nmrcFastCnpCcMutations();
        nmrc_ecn_nominal_ce += roce_srcs[ix]->ecnNominalCeForDiag();
        nmrc_ecn_detour_ce += roce_srcs[ix]->ecnDetourCeForDiag();
        nmrc_trim_non_detour += roce_srcs[ix]->_nmrc_trim_non_detour;
        nmrc_trim_detour += roce_srcs[ix]->_nmrc_trim_detour;
        nmrc_trim_nominal_cooldown_starts +=
            roce_srcs[ix]->_nmrc_trim_nominal_cooldown_starts;
        nmrc_trim_actual_cooldown_starts +=
            roce_srcs[ix]->_nmrc_trim_actual_cooldown_starts;
        nmrc_trim_duplicate_stale_ignored +=
            roce_srcs[ix]->_nmrc_trim_duplicate_stale_ignored;
        nmrc_trim_actual_unresolved +=
            roce_srcs[ix]->_nmrc_trim_actual_unresolved;
        nmrc_cooldown_starts +=
            roce_srcs[ix]->nmrc_cooldown_starts_for_diag();
        nmrc_cooling_skips +=
            roce_srcs[ix]->nmrc_cooling_skips_for_diag();
        nmrc_cooling_recoveries +=
            roce_srcs[ix]->nmrc_cooling_recoveries_for_diag();
        nmrc_duplicate_notifications +=
            roce_srcs[ix]->nmrc_duplicate_notifications_for_diag();
        nmrc_all_cooling_fallbacks +=
            roce_srcs[ix]->nmrc_all_cooling_fallbacks_for_diag();
        nmrc_all_cooling_rr_episodes +=
            roce_srcs[ix]->nmrc_all_cooling_rr_episodes_for_diag();
        nmrc_all_cooling_rr_selections +=
            roce_srcs[ix]->nmrc_all_cooling_rr_selections_for_diag();
        nmrc_all_cooling_rr_resets +=
            roce_srcs[ix]->nmrc_all_cooling_rr_resets_for_diag();
        nmrc_fastcnp_policy_ignored +=
            roce_srcs[ix]->nmrc_fastcnp_policy_ignored_for_diag();
        nmrc_trim_policy_ignored +=
            roce_srcs[ix]->nmrc_trim_policy_ignored_for_diag();
        mrc_state_samples += roce_srcs[ix]->_mrc_state_samples;
        mrc_active_count_sum += roce_srcs[ix]->_mrc_active_count_sum;
        mrc_backup_count_sum += roce_srcs[ix]->_mrc_backup_count_sum;
        mrc_cooling_count_sum += roce_srcs[ix]->_mrc_cooling_count_sum;
        mrc_failed_count_sum += roce_srcs[ix]->_mrc_failed_count_sum;
        mrc_active_physical_count_sum +=
            roce_srcs[ix]->_mrc_active_physical_count_sum;
        mrc_cooling_physical_count_sum +=
            roce_srcs[ix]->_mrc_cooling_physical_count_sum;
        mrc_failed_physical_count_sum +=
            roce_srcs[ix]->_mrc_failed_physical_count_sum;
        mrc_active_count_max = max(mrc_active_count_max,
                                   roce_srcs[ix]->_mrc_active_count_max);
        mrc_backup_count_max = max(mrc_backup_count_max,
                                   roce_srcs[ix]->_mrc_backup_count_max);
        mrc_cooling_count_max = max(mrc_cooling_count_max,
                                    roce_srcs[ix]->_mrc_cooling_count_max);
        mrc_failed_count_max = max(mrc_failed_count_max,
                                   roce_srcs[ix]->_mrc_failed_count_max);
        mrc_active_physical_count_max = max(mrc_active_physical_count_max,
            roce_srcs[ix]->_mrc_active_physical_count_max);
        mrc_cooling_physical_count_max = max(mrc_cooling_physical_count_max,
            roce_srcs[ix]->_mrc_cooling_physical_count_max);
        mrc_failed_physical_count_max = max(mrc_failed_physical_count_max,
            roce_srcs[ix]->_mrc_failed_physical_count_max);
        std::array<uint32_t, 5> states = roce_srcs[ix]->mrc_state_counts_for_diag();
        for (size_t s = 0; s < states.size(); s++)
            mrc_final_state_counts[s] += states[s];
        std::array<uint32_t, 5> physical_states =
            roce_srcs[ix]->mrc_state_physical_counts_for_diag();
        for (size_t s = 0; s < physical_states.size(); s++)
            mrc_final_physical_state_counts[s] += physical_states[s];
        mrc_backup_remaining_final += roce_srcs[ix]->mrc_backup_remaining_for_diag();
        for (map<uint32_t, uint64_t>::const_iterator it =
                 roce_srcs[ix]->_mrc_ecn_physical_hist.begin();
             it != roce_srcs[ix]->_mrc_ecn_physical_hist.end(); ++it)
            mrc_ecn_physical_hist[it->first] += it->second;
        for (map<uint32_t, uint64_t>::const_iterator it =
                 roce_srcs[ix]->_mrc_trim_physical_hist.begin();
             it != roce_srcs[ix]->_mrc_trim_physical_hist.end(); ++it)
            mrc_trim_physical_hist[it->first] += it->second;
        for (map<uint32_t, uint64_t>::const_iterator it =
                 roce_srcs[ix]->_mrc_nack_physical_hist.begin();
             it != roce_srcs[ix]->_mrc_nack_physical_hist.end(); ++it)
            mrc_nack_physical_hist[it->first] += it->second;
        for (map<uint32_t, uint64_t>::const_iterator it =
                 roce_srcs[ix]->_mrc_ooo_nack_physical_hist.begin();
             it != roce_srcs[ix]->_mrc_ooo_nack_physical_hist.end(); ++it)
            mrc_ooo_nack_physical_hist[it->first] += it->second;
        for (map<uint32_t, uint64_t>::const_iterator it =
                 roce_srcs[ix]->_mrc_loss_nack_physical_hist.begin();
             it != roce_srcs[ix]->_mrc_loss_nack_physical_hist.end(); ++it)
            mrc_loss_nack_physical_hist[it->first] += it->second;
    }
    cout << "New: " << new_pkts << " Rtx: " << rtx_pkts << endl;
    cout << "RoceDiag "
         << "acks=" << ack_pkts
         << " nacks=" << nack_pkts
         << " nacks_ooo=" << ooo_nacks
         << " nacks_trim=" << trim_nacks
         << " nacks_trim_lh=" << trim_lh_nacks
         << " nacks_trim_non_lh=" << (trim_nacks - trim_lh_nacks)
         << " nacks_loss=" << loss_nacks
         << " rtos=" << RoceSrc::_global_rto_count
         << " ecn_echo_acks=" << ecn_echo_acks
         << " duplicate_acks=" << duplicate_acks
         << " duplicate_inflate_suppressed=" << duplicate_inflate_suppressed
         << " feedback_acks=" << feedback_acks
         << " feedback_nacks=" << feedback_nacks
         << " feedback_messages_total=" << feedback_acks + feedback_nacks
         << " feedback_zero_bits=" << feedback_zero_bits
         << " netaware_ev_skips=" << netaware_ev_skips
         << " netaware_bitmap_fallbacks=" << netaware_bitmap_fallbacks
         << " stor_selected_good=" << stor_selected_good
         << " stor_selected_degraded=" << stor_selected_degraded
         << " stor_selected_bad=" << stor_selected_bad
         << " stor_selected_avoid=" << stor_selected_avoid
         << endl;
    cout << "BoundedRecoveryDiag "
         << "semantics=" << RoceSrc::transportSemanticsName()
         << " inflight_final=" << bounded_inflight_final
         << " unique_acks=" << bounded_unique_acks
         << " recovery_inflight_final_bytes="
         << bounded_recovery_inflight_final_bytes
         << " recovery_inflight_max_bytes="
         << bounded_recovery_inflight_max_bytes
         << " stale_attempt_nacks=" << bounded_stale_attempt_nacks
         << " duplicate_failure_nacks=" << bounded_duplicate_failure_nacks
         << " duplicate_confirmations_suppressed="
         << bounded_duplicate_confirmations_suppressed
         << " acked_revival_rejected=" << bounded_acked_revival_rejected
         << " attempt_wraps=" << bounded_attempt_wraps
         << " exact_trim_recoveries=" << bounded_exact_trim_recoveries
         << " sack_loss_recoveries=" << bounded_sack_loss_recoveries
         << endl;
    uint64_t reps_total_sends = reps_random_sends + reps_cached_sends;
    cout << "RepsLikeDiag "
         << "random_sends=" << reps_random_sends
         << " cached_sends=" << reps_cached_sends
         << " cache_hit_ratio=" << (reps_total_sends ?
                                    (double)reps_cached_sends / (double)reps_total_sends : 0.0)
         << " clean_ack_cached=" << reps_clean_ack_cached
         << " ecn_ack_discarded=" << reps_ecn_ack_discarded
         << " buffer_occupancy_samples=" << reps_buffer_occupancy_samples
         << " buffer_occupancy_hist=" << format_u32_u64_hist(reps_buffer_occupancy_hist)
         << endl;
    cout << "MrcDiag "
         << "ecn_cooldown_events=" << mrc_ecn_cooldown_events
         << " ecn_cooling_events=" << mrc_ecn_cooldown_events
         << " trim_events=" << mrc_trim_events
         << " rto_fail_events=" << mrc_rto_fail_events
         << " trim_cooling_events=" << mrc_trim_cooling_events
         << " nack_ooo_ignored_for_failure="
         << mrc_nack_ooo_ignored_for_failure
         << " nack_ooo_ignored_for_state="
         << mrc_nack_ooo_ignored_for_failure
         << " nack_loss_fail_events=" << mrc_nack_loss_fail_events
         << " loss_fail_events=" << mrc_nack_loss_fail_events
         << " nack_unknown_ignored_for_failure="
         << mrc_nack_unknown_ignored_for_failure
         << " feedback_exact_ev_events="
         << mrc_feedback_exact_ev_events
         << " feedback_sequence_fallback_events="
         << mrc_feedback_sequence_fallback_events
         << " feedback_physical_fallback_events="
         << mrc_feedback_physical_fallback_events
         << " feedback_cumulative_mismatch_events="
         << mrc_feedback_cumulative_mismatch_events
         << " failure_backup_promotions="
         << mrc_failure_backup_promotions
         << " forced_cooling_use=" << mrc_forced_cooling_use
         << " forced_cooling_earliest_use="
         << mrc_forced_cooling_earliest_use
         << " forced_cooling_round_robin_use="
         << mrc_forced_cooling_round_robin_use
         << " cycle_cooling_events=" << mrc_cycle_cooling_events
         << " cycle_cooling_expiries=" << mrc_cycle_cooling_expiries
         << " cooling_skip_selection_sum="
         << mrc_cooling_skip_selection_sum
         << " cooling_skip_selection_events="
         << mrc_cooling_skip_selection_events
         << " duplicate_feedback_ignored="
         << mrc_duplicate_feedback_ignored
         << " skip_opportunities_consumed="
         << mrc_skip_opportunities_consumed
         << " data_on_non_good_violations="
         << mrc_data_on_non_good_violations
         << " cooling_skip_selection_avg="
         << (mrc_cooling_skip_selection_events ?
             (double)mrc_cooling_skip_selection_sum /
             (double)mrc_cooling_skip_selection_events : 0.0)
         << " probe_events=" << mrc_probe_events
         << " probe_success_events=" << mrc_probe_success_events
         << " probe_fail_events=" << mrc_probe_fail_events
         << " backup_replacement_events=" << mrc_backup_replacement_events
         << " backup_replacement_diff_physical="
         << mrc_backup_replacement_diff_physical
         << " retx_original_physical=" << mrc_retx_original_physical
         << " retx_different_physical=" << mrc_retx_different_physical
         << " retx_same_physical_new_ev=" << mrc_retx_same_physical_new_ev
         << " retx_different_physical_new_ev="
         << mrc_retx_different_physical_new_ev
         << " retx_unknown_original=" << mrc_retx_unknown_original
         << " retx_fallback_events=" << mrc_retx_fallback_events
         << " state_samples=" << mrc_state_samples
         << " active_avg=" << (mrc_state_samples ?
                               (double)mrc_active_count_sum / (double)mrc_state_samples : 0.0)
         << " backup_avg=" << (mrc_state_samples ?
                               (double)mrc_backup_count_sum / (double)mrc_state_samples : 0.0)
         << " cooling_avg=" << (mrc_state_samples ?
                                (double)mrc_cooling_count_sum / (double)mrc_state_samples : 0.0)
         << " failed_avg=" << (mrc_state_samples ?
                               (double)mrc_failed_count_sum / (double)mrc_state_samples : 0.0)
         << " active_physical_avg=" << (mrc_state_samples ?
             (double)mrc_active_physical_count_sum / (double)mrc_state_samples : 0.0)
         << " cooling_physical_avg=" << (mrc_state_samples ?
             (double)mrc_cooling_physical_count_sum / (double)mrc_state_samples : 0.0)
         << " failed_physical_avg=" << (mrc_state_samples ?
             (double)mrc_failed_physical_count_sum / (double)mrc_state_samples : 0.0)
         << " active_max=" << mrc_active_count_max
         << " backup_max=" << mrc_backup_count_max
         << " cooling_max=" << mrc_cooling_count_max
         << " failed_max=" << mrc_failed_count_max
         << " active_physical_max=" << mrc_active_physical_count_max
         << " cooling_physical_max=" << mrc_cooling_physical_count_max
         << " failed_physical_max=" << mrc_failed_physical_count_max
         << " unused_final=" << mrc_final_state_counts[0]
         << " active_final=" << mrc_final_state_counts[1]
         << " cooling_final=" << mrc_final_state_counts[2]
         << " failed_final=" << mrc_final_state_counts[3]
         << " probing_final=" << mrc_final_state_counts[4]
         << " unused_physical_final=" << mrc_final_physical_state_counts[0]
         << " active_physical_final=" << mrc_final_physical_state_counts[1]
         << " cooling_physical_final=" << mrc_final_physical_state_counts[2]
         << " failed_physical_final=" << mrc_final_physical_state_counts[3]
         << " probing_physical_final=" << mrc_final_physical_state_counts[4]
         << " backup_remaining_final=" << mrc_backup_remaining_final
         << " ecn_physical_hist=" << format_u32_u64_hist(mrc_ecn_physical_hist)
         << " trim_physical_hist=" << format_u32_u64_hist(mrc_trim_physical_hist)
         << " nack_physical_hist=" << format_u32_u64_hist(mrc_nack_physical_hist)
         << " ooo_nack_physical_hist=" << format_u32_u64_hist(mrc_ooo_nack_physical_hist)
         << " loss_nack_physical_hist=" << format_u32_u64_hist(mrc_loss_nack_physical_hist)
         << endl;
    const map<uint32_t, uint64_t>& selected_ev_hist = RoceSrc::diagSelectedEvHist();
    const map<uint32_t, uint64_t>& selected_physical_hist = RoceSrc::diagSelectedPhysicalHist();
    RoceSrc::flushPathSelectionTimeline();
    cout << "PathSelectDiag "
         << "selected_total=" << RoceSrc::diagSelectedTotal()
         << " unique_evs=" << selected_ev_hist.size()
         << " unique_physical_mods=" << selected_physical_hist.size()
         << " topology_path_combo=" << topology_path_combo
         << " first128=" << format_u32_vector(RoceSrc::diagFirstSelectedEvs())
         << " ev_hist_top=" << format_top_u32_u64_hist(selected_ev_hist, 32)
         << " physical_mod_hist=" << format_u32_u64_hist(selected_physical_hist)
         << endl;
    QueueDiag queue_diag = collect_queue_diag(top);
    cout << "QueueDiag "
         << "lossless_overflows=" << queue_diag.lossless_overflows
         << " lossless_ecn_marks=" << queue_diag.lossless_ecn_marks
         << " lossy_drops=" << queue_diag.lossy_drops
         << " lossy_ecn_marks=" << queue_diag.lossy_ecn_marks
         << " composite_trims=" << queue_diag.composite_trims
         << " composite_drops=" << queue_diag.composite_drops
         << " composite_ecn_marks=" << queue_diag.composite_ecn_marks
         << endl;
    cout << "SharedBufferDiag total_bytes=" << FatTreeSwitch::_shared_buffer_bytes
         << " peak_bytes=" << FatTreeSwitch::_shared_buffer_peak_bytes
         << " headroom_bytes=" << FatTreeSwitch::_shared_headroom_bytes
         << " overflows=" << FatTreeSwitch::_shared_buffer_overflows_total
         << " ingress_alpha=" << FatTreeSwitch::_shared_ingress_alpha
         << " egress_alpha=" << FatTreeSwitch::_shared_egress_alpha
         << " pfc_pause=" << LosslessInputQueue::_pause_events
         << " pfc_resume=" << LosslessInputQueue::_resume_events
         << " ingress_peak_bytes=" << LosslessInputQueue::_peak_queue_bytes
         << " min_pause_threshold_bytes="
         << (LosslessInputQueue::_minimum_dynamic_threshold == UINT64_MAX ? 0 :
             LosslessInputQueue::_minimum_dynamic_threshold)
         << endl;
    StorDiag stor_diag = collect_stor_diag(top);
    cout << "StorDiag "
         << "stor_min_score=" << (uint32_t)stor_diag.min_score
         << " stor_avoid_entries=" << stor_diag.avoid_entries
         << " stor_avoid_exits=" << stor_diag.avoid_exits
         << " stor_clean_signals=" << stor_diag.clean_signals
         << " stor_ecn_signals=" << stor_diag.ecn_signals
         << " stor_trim_signals=" << stor_diag.trim_signals
         << endl;
    cout << "StorProfileDiag "
         << "stor_level_transitions="
         << format_profile_level_transitions(false)
         << " stor_level_changes=" << RoceSrc::storLevelChanges()
         << " stor_all_zero_profiles=" << RoceSrc::storAllZeroProfiles()
         << " stor_all_zero_selections=" << RoceSrc::storAllZeroSelections()
         << " netaware_level_transitions="
         << format_profile_level_transitions(true)
         << " netaware_level_changes=" << RoceSrc::netawareLevelChanges()
         << " netaware_all_zero_profiles="
         << RoceSrc::netawareAllZeroProfiles()
         << " netaware_all_zero_selections="
         << RoceSrc::netawareAllZeroSelections()
         << endl;
    NetawareDiag netaware_diag = collect_netaware_diag(top);
    cout << "NetawareDiag "
         << "samples=" << netaware_diag.samples
         << " sample_zero_bits=" << netaware_diag.sample_zero_bits
         << " feedbacks=" << netaware_diag.feedbacks
         << " packet_feedbacks=" << netaware_diag.packet_feedbacks
         << " time_feedbacks=" << netaware_diag.time_feedbacks
         << " feedback_packets_sum=" << netaware_diag.feedback_packets_sum
         << " feedback_zero_bits=" << netaware_diag.feedback_zero_bits
         << " all_good_feedbacks=" << netaware_diag.all_good_feedbacks
         << endl;
    if (roce_lb_mode == RoceSrc::LB_NMRC) {
        map<uint32_t, uint64_t> better_count_hist;
        map<uint32_t, uint64_t> binary_actual_egress_hist;
        map<uint32_t, uint64_t> relative_candidate_count_hist;
        map<uint32_t, uint64_t> relative_best_gap_hist;
        map<uint32_t, uint64_t> relative_selected_gap_hist;
        map<uint32_t, uint64_t> relative_original_score_hist;
        map<uint32_t, uint64_t> relative_selected_score_hist;
        map<uint32_t, uint64_t> relative_actual_egress_hist;
        for (uint32_t count = 0; count <= 32; count++) {
            if (FatTreeSwitch::_nmrc_diag_better_count[count])
                better_count_hist[count] =
                    FatTreeSwitch::_nmrc_diag_better_count[count];
            if (FatTreeSwitch::_nmrc_diag_binary_actual_egress[count])
                binary_actual_egress_hist[count] =
                    FatTreeSwitch::_nmrc_diag_binary_actual_egress[count];
            if (FatTreeSwitch::_nmrc_diag_relative_candidate_count[count])
                relative_candidate_count_hist[count] =
                    FatTreeSwitch::_nmrc_diag_relative_candidate_count[count];
            if (FatTreeSwitch::_nmrc_diag_relative_actual_egress[count])
                relative_actual_egress_hist[count] =
                    FatTreeSwitch::_nmrc_diag_relative_actual_egress[count];
        }
        for (uint32_t bin = 0; bin <= 20; bin++) {
            if (FatTreeSwitch::_nmrc_diag_relative_best_gap[bin])
                relative_best_gap_hist[bin] =
                    FatTreeSwitch::_nmrc_diag_relative_best_gap[bin];
            if (FatTreeSwitch::_nmrc_diag_relative_selected_gap[bin])
                relative_selected_gap_hist[bin] =
                    FatTreeSwitch::_nmrc_diag_relative_selected_gap[bin];
            if (FatTreeSwitch::_nmrc_diag_relative_original_score[bin])
                relative_original_score_hist[bin] =
                    FatTreeSwitch::_nmrc_diag_relative_original_score[bin];
            if (FatTreeSwitch::_nmrc_diag_relative_selected_score[bin])
                relative_selected_score_hist[bin] =
                    FatTreeSwitch::_nmrc_diag_relative_selected_score[bin];
        }
        uint64_t observed_evs =
            FatTreeSwitch::nmrc_diag_observed_evs();
        uint64_t observed_paths =
            FatTreeSwitch::nmrc_diag_observed_paths();
        double observed_alias_ratio = observed_evs ?
            1.0 - (double)observed_paths / (double)observed_evs : 0.0;
        cout << "HybridNmrcDiag"
             << " route_checks=" << FatTreeSwitch::_nmrc_diag_route_checks
             << " reroutes=" << FatTreeSwitch::_nmrc_diag_reroutes
             << " threshold_blocked="
             << FatTreeSwitch::_nmrc_diag_threshold_blocked
             << " better_count_hist="
             << format_u32_u64_hist(better_count_hist)
             << " level_transitions="
             << format_nmrc_level_transitions()
             << " fastcnp_generated="
             << FatTreeSwitch::_nmrc_diag_fastcnp_generated
             << " fastcnp_arrived=" << nmrc_fastcnp_arrived
             << " fastcnp_after_done=" << nmrc_fastcnp_after_done
             << " fastcnp_bytes="
             << FatTreeSwitch::_nmrc_diag_fastcnp_generated *
                    RocePacket::ACKSIZE
             << " fastcnp_arrived_bytes=" << nmrc_fastcnp_arrived_bytes
             << " fastcnp_latency_avg_us="
             << (nmrc_fastcnp_arrived ?
                    timeAsUs(nmrc_fastcnp_latency_sum) /
                        (double)nmrc_fastcnp_arrived : 0.0)
             << " fastcnp_route_missing="
             << FatTreeSwitch::_nmrc_diag_fastcnp_route_missing
             << " fastcnp_unknown_qp=" << nmrc_fastcnp_unknown_qp
             << " fastcnp_unknown_ev=" << nmrc_fastcnp_unknown_ev
             << " cooldown_starts=" << nmrc_cooldown_starts
             << " cooling_skips=" << nmrc_cooling_skips
             << " cooling_recoveries=" << nmrc_cooling_recoveries
             << " duplicate_notifications="
             << nmrc_duplicate_notifications
             << " all_cooling_fallbacks="
             << nmrc_all_cooling_fallbacks
             << " trim_non_detour=" << nmrc_trim_non_detour
             << " trim_detour=" << nmrc_trim_detour
             << " trim_nominal_cooldown_starts="
             << nmrc_trim_nominal_cooldown_starts
             << " trim_actual_cooldown_starts="
             << nmrc_trim_actual_cooldown_starts
             << " trim_duplicate_stale_ignored="
             << nmrc_trim_duplicate_stale_ignored
             << " trim_actual_unresolved="
             << nmrc_trim_actual_unresolved
             << " observed_evs=" << observed_evs
             << " observed_first_hop_choices=" << observed_paths
             << " observed_first_hop_alias_ratio=" << observed_alias_ratio
             << " all_cooling_rr_episodes="
             << nmrc_all_cooling_rr_episodes
             << " all_cooling_rr_selections="
             << nmrc_all_cooling_rr_selections
             << " all_cooling_rr_resets=" << nmrc_all_cooling_rr_resets
             << " fastcnp_policy_ignored=" << nmrc_fastcnp_policy_ignored
             << " trim_policy_ignored=" << nmrc_trim_policy_ignored
             << " graded_cooldown_requested="
             << FatTreeSwitch::_nmrc_diag_graded_cooldown_requested
             << " graded_cooldown_suppressed="
             << FatTreeSwitch::_nmrc_diag_graded_cooldown_suppressed
             << " binary_original_safe="
             << FatTreeSwitch::_nmrc_diag_binary_original_safe
             << " binary_original_congested="
             << FatTreeSwitch::_nmrc_diag_binary_original_congested
             << " binary_no_safe="
             << FatTreeSwitch::_nmrc_diag_binary_no_safe
             << " binary_route_missing="
             << FatTreeSwitch::_nmrc_diag_binary_route_missing
             << " binary_paired_actions="
             << FatTreeSwitch::_nmrc_diag_binary_paired_actions
             << " binary_original_score_count="
             << FatTreeSwitch::_nmrc_diag_binary_paired_actions
             << " binary_original_score_sum="
             << FatTreeSwitch::_nmrc_diag_binary_original_score_sum
             << " binary_original_score_max="
             << FatTreeSwitch::_nmrc_diag_binary_original_score_max
             << " binary_selected_score_count="
             << FatTreeSwitch::_nmrc_diag_binary_paired_actions
             << " binary_selected_score_sum="
             << FatTreeSwitch::_nmrc_diag_binary_selected_score_sum
             << " binary_selected_score_max="
             << FatTreeSwitch::_nmrc_diag_binary_selected_score_max
             << " binary_actual_egress_hist="
             << format_u32_u64_hist(binary_actual_egress_hist)
             << " relative_checks="
             << FatTreeSwitch::_nmrc_diag_relative_checks
             << " relative_original_unknown="
             << FatTreeSwitch::_nmrc_diag_relative_original_unknown
             << " relative_original_unavailable="
             << FatTreeSwitch::_nmrc_diag_relative_original_unavailable
             << " relative_original_below_absolute="
             << FatTreeSwitch::_nmrc_diag_relative_original_below_absolute
             << " relative_no_safe_candidate="
             << FatTreeSwitch::_nmrc_diag_relative_no_safe_candidate
             << " relative_no_delta_candidate="
             << FatTreeSwitch::_nmrc_diag_relative_no_delta_candidate
             << " relative_reverse_path_blocked="
             << FatTreeSwitch::_nmrc_diag_relative_reverse_path_blocked
             << " relative_paired_actions="
             << FatTreeSwitch::_nmrc_diag_relative_paired_actions
             << " relative_reroutes="
             << FatTreeSwitch::_nmrc_diag_relative_reroutes
             << " two_stage_reroute_only="
             << FatTreeSwitch::_nmrc_diag_two_stage_reroute_only
             << " two_stage_cooldown_requested="
             << FatTreeSwitch::_nmrc_diag_two_stage_cooldown_requested
             << " relative_reroute_key_count="
             << FatTreeSwitch::_nmrc_diag_relative_reroute_key_count
             << " relative_reroute_key_sum="
             << FatTreeSwitch::_nmrc_diag_relative_reroute_key_sum
             << " relative_reroute_key_xor="
             << FatTreeSwitch::_nmrc_diag_relative_reroute_key_xor
             << " relative_generated_key_count="
             << FatTreeSwitch::_nmrc_diag_relative_generated_key_count
             << " relative_generated_key_sum="
             << FatTreeSwitch::_nmrc_diag_relative_generated_key_sum
             << " relative_generated_key_xor="
             << FatTreeSwitch::_nmrc_diag_relative_generated_key_xor
             << " relative_candidate_count_hist="
             << format_u32_u64_hist(relative_candidate_count_hist)
             << " relative_best_gap_hist="
             << format_u32_u64_hist(relative_best_gap_hist)
             << " relative_selected_gap_hist="
             << format_u32_u64_hist(relative_selected_gap_hist)
             << " relative_original_score_count="
             << FatTreeSwitch::_nmrc_diag_relative_original_score_count
             << " relative_original_score_sum="
             << FatTreeSwitch::_nmrc_diag_relative_original_score_sum
             << " relative_original_score_max="
             << FatTreeSwitch::_nmrc_diag_relative_original_score_max
             << " relative_original_score_hist="
             << format_u32_u64_hist(relative_original_score_hist)
             << " relative_selected_score_count="
             << FatTreeSwitch::_nmrc_diag_relative_selected_score_count
             << " relative_selected_score_sum="
             << FatTreeSwitch::_nmrc_diag_relative_selected_score_sum
             << " relative_selected_score_max="
             << FatTreeSwitch::_nmrc_diag_relative_selected_score_max
             << " relative_selected_score_hist="
             << format_u32_u64_hist(relative_selected_score_hist)
             << " relative_actual_egress_hist="
             << format_u32_u64_hist(relative_actual_egress_hist)
             << " relative_selected_gap_violations="
             << FatTreeSwitch::_nmrc_diag_relative_selected_gap_violations
             << " relative_decision_ce_set="
             << FatTreeSwitch::_nmrc_diag_relative_decision_ce_set
             << " relative_decision_ce_cleared="
             << FatTreeSwitch::_nmrc_diag_relative_decision_ce_cleared
             << " ecn_nominal_ce=" << nmrc_ecn_nominal_ce
             << " ecn_detour_ce=" << nmrc_ecn_detour_ce
             << " fastcnp_cc_mutations=" << nmrc_fastcnp_cc_mutations
             << endl;
    }
    if (queue_cv_sampler) {
        cout << "QueueCvDiag "
             << "spine_queue_cv=" << queue_cv_sampler->cv()
             << " spine_queue_avg=" << queue_cv_sampler->average_queue()
             << " spine_queue_count=" << queue_cv_sampler->count()
             << " spine_queue_peak_bytes=" << queue_cv_sampler->peak_bytes()
             << " spine_queue_p95_fraction="
             << queue_cv_sampler->queue_fraction_percentile(0.95)
             << " spine_queue_p99_fraction="
             << queue_cv_sampler->queue_fraction_percentile(0.99)
             << endl;
    }
    double sglb_route_calls =
        (double)FatTreeSwitch::_sglb_diag_route_calls;
    cout << "SglbRouteDiag "
         << "route_calls=" << FatTreeSwitch::_sglb_diag_route_calls
         << " avg_available_choices="
         << (sglb_route_calls ?
             FatTreeSwitch::_sglb_diag_available_choices / sglb_route_calls : 0.0)
         << " avg_candidate_choices="
         << (sglb_route_calls ?
             FatTreeSwitch::_sglb_diag_candidate_choices / sglb_route_calls : 0.0)
         << " avg_best_quality_choices="
         << (sglb_route_calls ?
             FatTreeSwitch::_sglb_diag_best_quality_choices / sglb_route_calls : 0.0)
         << " avg_distinct_qualities="
         << (sglb_route_calls ?
             FatTreeSwitch::_sglb_diag_distinct_qualities / sglb_route_calls : 0.0)
         << " all_same_quality_calls="
         << FatTreeSwitch::_sglb_diag_all_same_quality_calls
         << " all_zero_quality_calls="
         << FatTreeSwitch::_sglb_diag_all_zero_quality_calls
         << " selected_nonbest_quality="
         << FatTreeSwitch::_sglb_diag_selected_nonbest_quality
         << " avg_score_spread="
         << (sglb_route_calls ?
             FatTreeSwitch::_sglb_diag_score_spread_sum / sglb_route_calls : 0.0)
         << " observed_good="
         << FatTreeSwitch::_sglb_diag_observed_levels[STOR_LEVEL_GOOD]
         << " observed_degraded="
         << FatTreeSwitch::_sglb_diag_observed_levels[STOR_LEVEL_DEGRADED]
         << " observed_bad="
         << FatTreeSwitch::_sglb_diag_observed_levels[STOR_LEVEL_BAD]
         << " observed_avoid="
         << FatTreeSwitch::_sglb_diag_observed_levels[STOR_LEVEL_AVOID]
         << " selected_good="
         << FatTreeSwitch::_sglb_diag_selected_levels[STOR_LEVEL_GOOD]
         << " selected_degraded="
         << FatTreeSwitch::_sglb_diag_selected_levels[STOR_LEVEL_DEGRADED]
         << " selected_bad="
         << FatTreeSwitch::_sglb_diag_selected_levels[STOR_LEVEL_BAD]
         << " selected_avoid="
         << FatTreeSwitch::_sglb_diag_selected_levels[STOR_LEVEL_AVOID]
         << " remote_snapshot_used="
         << FatTreeSwitch::_sglb_diag_remote_snapshot_used
         << " remote_snapshot_missing="
         << FatTreeSwitch::_sglb_diag_remote_snapshot_missing
         << endl;
    const double paper_calls =
        static_cast<double>(FatTreeSwitch::_paper_sglb_diag_route_calls);
    cout << "SglbEcnFilterSummary mode="
         << (FatTreeSwitch::_sglb_ecn_mode == FatTreeSwitch::SGLB_ECN_NEUTRAL ?
             "neutral" :
             FatTreeSwitch::_sglb_ecn_mode == FatTreeSwitch::SGLB_ECN_CLEAR ?
             "clear" : "off")
         << " total_ecn=" << FatTreeSwitch::_sglb_ecn_total
         << " stale_ecn=" << FatTreeSwitch::_sglb_ecn_stale
         << " neutralized=" << FatTreeSwitch::_sglb_ecn_neutralized
         << " cleared=" << FatTreeSwitch::_sglb_ecn_cleared
         << " missing_metadata=" << FatTreeSwitch::_sglb_ecn_missing_metadata
         << endl;
    cout << "PaperSglbDiag "
         << "route_calls=" << FatTreeSwitch::_paper_sglb_diag_route_calls
         << " avg_candidates=" << (paper_calls ?
             FatTreeSwitch::_paper_sglb_diag_candidate_sum / paper_calls : 0.0)
         << " remote_missing="
         << FatTreeSwitch::_paper_sglb_diag_remote_missing
         << " gcn_updates=" << FatTreeSwitch::_paper_sglb_diag_gcn_updates
         << " gcn_packets=" << FatTreeSwitch::_paper_sglb_diag_gcn_packets
         << " gcn_bytes=" << FatTreeSwitch::_paper_sglb_diag_gcn_bytes
         << " gcn_deliveries="
         << FatTreeSwitch::_paper_sglb_diag_gcn_deliveries
         << " gcn_profile_updates="
         << FatTreeSwitch::_paper_sglb_diag_gcn_profile_updates
         << " gcn_stale=" << FatTreeSwitch::_paper_sglb_diag_gcn_stale
         << endl;

    /*list <const Route*>::iterator rt_i;
      int counts[10]; int hop;
      for (int i = 0; i < 10; i++)
      counts[i] = 0;
      for (rt_i = routes.begin(); rt_i != routes.end(); rt_i++) {
      const Route* r = (*rt_i);
      //print_route(*r);
      #ifdef PRINTPATHS
      cout << "Path:" << endl;
      #endif
      hop = 0;
      for (int i = 0; i < r->size(); i++) {
      PacketSink *ps = r->at(i); 
      CompositeQueue *q = dynamic_cast<CompositeQueue*>(ps);
      if (q == 0) {
      #ifdef PRINTPATHS
      cout << ps->nodename() << endl;
      #endif
      } else {
      #ifdef PRINTPATHS
      cout << q->nodename() << " id=" << q->id << " " << q->num_packets() << "pkts " 
                     << q->num_headers() << "hdrs " << q->num_acks() << "acks " << q->num_nacks() << "nacks " << q->num_stripped() << "stripped"
                     << endl;
#endif
                counts[hop] += q->num_stripped();
                hop++;
            }
        } 
#ifdef PRINTPATHS
        cout << endl;
#endif
    }
    for (int i = 0; i < 10; i++)
    cout << "Hop " << i << " Count " << counts[i] << endl;*/
        
}
