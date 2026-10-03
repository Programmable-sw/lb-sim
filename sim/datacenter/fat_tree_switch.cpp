// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#include "fat_tree_switch.h"
#include "sglb_score_topk.h"
#include "routetable.h"
#include "fat_tree_topology.h"
#include "callback_pipe.h"
#include "queue_lossless.h"
#include "queue_lossless_output.h"
#include "compositequeue.h"
#include "ecnqueue.h"
#include "rocepacket.h"
#include "roce.h"
#include "sglbpacket.h"
#include "ecn.h"
#include "../conga_model.h"
#include <algorithm>
#include <cmath>
#include <limits>

unordered_map<BaseQueue*,uint32_t> FatTreeSwitch::_port_flow_counts;
uint64_t FatTreeSwitch::_shared_buffer_bytes = 9ULL * 1024 * 1024;
double FatTreeSwitch::_shared_ingress_alpha = 0.0625;
double FatTreeSwitch::_shared_egress_alpha = 1.0;
uint64_t FatTreeSwitch::_shared_headroom_bytes = 256 * 1024;
uint64_t FatTreeSwitch::_shared_buffer_overflows_total = 0;
uint64_t FatTreeSwitch::_shared_buffer_peak_bytes = 0;

// Match the ns-3.19 experiment's MurmurHash3-style five-tuple ECMP family.
// htsim does not carry UDP ports separately, so the stable flow id occupies
// the transport tuple word.
static uint32_t paper_ecmp_hash(uint32_t src, uint32_t dst, uint32_t flow,
                                uint32_t seed) {
    const uint32_t words[3] = {src, dst, flow};
    uint32_t h = seed;
    for (uint32_t i = 0; i < 3; ++i) {
        uint32_t k = words[i];
        k *= 0xcc9e2d51u;
        k = (k << 15) | (k >> 17);
        k *= 0x1b873593u;
        h ^= k;
        h = (h << 13) | (h >> 19);
        h += (h << 2) + 0xe6546b64u;
    }
    h ^= 12u;
    h ^= h >> 16;
    h *= 0x85ebca6bu;
    h ^= h >> 13;
    h *= 0xc2b2ae35u;
    h ^= h >> 16;
    return h;
}

static uint64_t paper_sglb_remote_key(uint32_t spine, uint32_t destination_tor) {
    return (static_cast<uint64_t>(spine) << 32) | destination_tor;
}

static FatTreeSwitch::PaperSglbFactors paper_sglb_remote_factors(
    FatTreeTopology* top, uint32_t spine, uint32_t destination_tor) {
    FatTreeSwitch::PaperSglbFactors factors;
    if (!top || spine >= top->queues_nup_nlp.size() ||
        destination_tor >= top->queues_nup_nlp[spine].size()) {
        factors.remote_queue = 1.0;
        factors.remote_utilization = 1.0;
        factors.remote_busyness = 1.0;
        return factors;
    }

    const vector<BaseQueue*>& lag = top->queues_nup_nlp[spine][destination_tor];
    double queue_sum = 0.0;
    double utilization_sum = 0.0;
    uint32_t lag_ports = 0;
    for (size_t i = 0; i < lag.size(); ++i) {
        BaseQueue* queue = lag[i];
        if (!queue || queue->maxsize() == 0)
            continue;
        queue_sum += static_cast<double>(queue->queuesize()) / queue->maxsize();
        utilization_sum +=
            static_cast<double>(queue->peek_average_utilization()) / 100.0;
        ++lag_ports;
    }
    if (lag_ports == 0) {
        factors.remote_queue = 1.0;
        factors.remote_utilization = 1.0;
    } else {
        factors.remote_queue = queue_sum / lag_ports;
        factors.remote_utilization = utilization_sum / lag_ports;
    }

    double busy_sum = 0.0;
    uint32_t busy_ports = 0;
    for (size_t destination = 0;
         destination < top->queues_nup_nlp[spine].size(); ++destination) {
        const vector<BaseQueue*>& output = top->queues_nup_nlp[spine][destination];
        for (size_t bundle = 0; bundle < output.size(); ++bundle) {
            BaseQueue* queue = output[bundle];
            if (!queue || queue->maxsize() == 0)
                continue;
            busy_sum += static_cast<double>(queue->queuesize()) / queue->maxsize();
            ++busy_ports;
        }
    }
    factors.remote_busyness = busy_ports ? busy_sum / busy_ports : 1.0;
    return factors;
}

static bool paper_sglb_remote_factors_equal(
    const FatTreeSwitch::PaperSglbFactors& left,
    const FatTreeSwitch::PaperSglbFactors& right) {
    return left.remote_queue == right.remote_queue &&
        left.remote_utilization == right.remote_utilization &&
        left.remote_busyness == right.remote_busyness;
}

class SglbGcnTimer : public EventSource {
public:
    SglbGcnTimer(EventList& eventlist, FatTreeSwitch* sw)
        : EventSource(eventlist, "sglb_gcn_timer"), _sw(sw) {}

    void doNextEvent() {
        if (!_sw)
            return;
        _sw->_sglb_gcn_timer_pending = false;
        _sw->sglb_periodic_refresh_exports();
    }

private:
    FatTreeSwitch* _sw;
};

class SglbRealGcnTimer : public EventSource {
public:
    SglbRealGcnTimer(EventList& eventlist, FatTreeSwitch* sw,
                     uint32_t destination)
        : EventSource(eventlist, "sglb_real_gcn_timer"), _sw(sw),
          _destination(destination) {}

    void doNextEvent() {
        if (_sw)
            _sw->sglb_real_gcn_timer_fired(_destination);
    }

private:
    FatTreeSwitch* _sw;
    uint32_t _destination;
};

class NetawareExportTimer : public EventSource {
public:
    NetawareExportTimer(EventList& eventlist, FatTreeSwitch* sw)
        : EventSource(eventlist, "netaware_export_timer"), _sw(sw) {}

    void doNextEvent() {
        if (!_sw)
            return;
        _sw->_netaware_export_timer_pending = false;
        _sw->netaware_periodic_refresh_exports();
    }

private:
    FatTreeSwitch* _sw;
};

FatTreeSwitch::FatTreeSwitch(EventList& eventlist, string s, switch_type t, uint32_t id,simtime_picosec delay, FatTreeTopology* ft): Switch(eventlist, s) {
    _id = id;
    _type = t;
    // The reference SwitchMmu uses a 1048-byte cell/guarantee quantum even
    // when the experiment payload is configured larger.
    _shared_mmu = new BroadcomMmu(_shared_buffer_bytes,
        1048, _shared_ingress_alpha,
        _shared_egress_alpha, _shared_headroom_bytes);
    _shared_buffer_overflows = 0;
    _pipe = new CallbackPipe(delay,eventlist, this);
    _uproutes = NULL;
    _ft = ft;
    _crt_route = 0;
    _hash_salt = random();
    _last_choice = eventlist.now();
    _sglb_gcn_timer = NULL;
    _sglb_gcn_timer_pending = false;
    _netaware_export_timer = NULL;
    _netaware_export_timer_pending = false;
    _paper_sglb_gcn_flow = NULL;
    _paper_sglb_gcn_last_sent = 0;
    _paper_sglb_gcn_version = 0;
    _paper_sglb_gcn_dirty = false;
    _paper_sglb_gcn_has_sent = false;
    _fib = new RouteTable();
}

void FatTreeSwitch::shared_register_ingress(const void* queue) {
    if (_shared_ingress_ports.count(queue)) return;
    uint32_t port = _shared_ingress_ports.size() + 1;
    _shared_ingress_ports[queue] = port;
    _shared_mmu->registerIngressPort(port);
}

void FatTreeSwitch::shared_register_egress(const void* queue) {
    if (_shared_egress_ports.count(queue)) return;
    uint32_t port = _shared_egress_ports.size() + 1;
    _shared_egress_ports[queue] = port;
    _shared_mmu->registerEgressPort(port);
}

SharedBufferSwitch::Admission FatTreeSwitch::shared_admit(
        const void* ingress, uint32_t pg, const void* egress,
        uint32_t queue, uint64_t bytes) {
    shared_register_ingress(ingress);
    shared_register_egress(egress);
    BroadcomMmu::Admission result = _shared_mmu->admit(
        _shared_ingress_ports[ingress], pg, _shared_egress_ports[egress],
        queue, bytes);
    if (result != BroadcomMmu::ADMIT) {
        ++_shared_buffer_overflows;
        ++_shared_buffer_overflows_total;
    } else if (_shared_mmu->totalIngressBytes() > _shared_buffer_peak_bytes) {
        _shared_buffer_peak_bytes = _shared_mmu->totalIngressBytes();
    }
    return result == BroadcomMmu::ADMIT ? ADMIT :
        result == BroadcomMmu::DROP_EGRESS ? DROP_EGRESS : DROP_INGRESS;
}

void FatTreeSwitch::shared_release(const void* ingress, uint32_t pg,
        const void* egress, uint32_t queue, uint64_t bytes) {
    _shared_mmu->release(_shared_ingress_ports.at(ingress), pg,
        _shared_egress_ports.at(egress), queue, bytes);
}

bool FatTreeSwitch::shared_should_pause(const void* ingress, uint32_t pg) const {
    return _shared_mmu->shouldPause(_shared_ingress_ports.at(ingress), pg);
}

bool FatTreeSwitch::shared_should_resume(const void* ingress, uint32_t pg) const {
    return _shared_mmu->shouldResume(_shared_ingress_ports.at(ingress), pg);
}

uint64_t FatTreeSwitch::shared_pause_threshold(uint32_t pg) const {
    return _shared_mmu->pauseThresholdBytes(pg);
}

bool FatTreeSwitch::shared_should_mark_ecn(const void* egress, uint32_t queue,
        uint64_t kmin, uint64_t kmax, double sample01) const {
    return _shared_mmu->shouldMarkEcn(_shared_egress_ports.at(egress), queue,
        kmin, kmax, sample01);
}

uint64_t FatTreeSwitch::shared_buffer_used() const {
    return _shared_mmu->totalIngressBytes();
}

void FatTreeSwitch::receivePacket(Packet& pkt){
    if (pkt.type() == SGLB_GCN) {
        SglbGcnPacket* gcn = dynamic_cast<SglbGcnPacket*>(&pkt);
        assert(gcn);
        if (_strategy == SGLB && sglb_ofat_uses_real_gcn_profiles())
            receive_sglb_real_gcn(*gcn);
        else
            receive_paper_sglb_gcn(*gcn);
        gcn->free();
        return;
    }
    if (pkt.type()==ETH_PAUSE){
        EthPausePacket* p = (EthPausePacket*)&pkt;
        //I must be in lossless mode!
        //find the egress queue that should process this, and pass it over for processing. 
        for (size_t i = 0;i < _ports.size();i++){
            LosslessQueue* q = (LosslessQueue*)_ports.at(i);
            if (q->getRemoteEndpoint() && ((Switch*)q->getRemoteEndpoint())->getID() == p->senderID()){
                q->receivePacket(pkt);
                break;
            }
        }
        
        return;
    }

    if (_packets.find(&pkt)==_packets.end()){
        //ingress pipeline processing.

        _packets[&pkt] = true;

        const Route * nh = getNextHop(pkt,NULL);
        if (!nh) {
            _packets.erase(&pkt);
            pkt.free();
            return;
        }
        // CONGA DRE is updated by every data packet on an inter-switch link,
        // including ECMP short flows. Tagged CONGA packets carry the maximum
        // Q=3 metric observed along their path.
        if (_strategy == ECMP) {
            BaseQueue* egress = nh->size() ? dynamic_cast<BaseQueue*>(nh->at(0)) : NULL;
            if (egress && !egress->is_last_hop()) {
                const simtime_picosec period = timeFromUs(32.0);
                const simtime_picosec tau = timeFromUs(160.0);
                CongaDreState& state = _conga_dre[egress];
                const simtime_picosec now = eventlist().now();
                if (!state.last_decay)
                    state.last_decay = now;
                if (now > state.last_decay) {
                    const uint64_t periods = (now - state.last_decay) / period;
                    if (periods) {
                        state.bytes = conga_decay_dre(state.bytes, periods);
                        state.last_decay += periods * period;
                    }
                }
                state.bytes += pkt.size() + 48;
                const uint32_t metric = conga_quantize_dre(
                    state.bytes, egress->bitrate(), tau, 3);
                if (pkt.type() == ROCE &&
                    ((RocePacket&)pkt).lbtag() != UINT32_MAX) {
                    RocePacket& data = (RocePacket&)pkt;
                    data.set_conga_metric(metric);
                    if (_type == TOR)
                        RoceSrc::updateCongaLocalMetric(
                            _id, data.lbtag(), metric);
                }
            }
        }
        //set next hop which is peer switch.
        pkt.set_route(*nh);

        //emulate the switching latency between ingress and packet arriving at the egress queue.
        _pipe->receivePacket(pkt); 
    }
    else {
        _packets.erase(&pkt);
        
        //egress queue processing.
        //cout << "Switch type " << _type <<  " id " << _id << " pkt dst " << pkt.dst() << " dir " << pkt.get_direction() << endl;
        pkt.sendOn();
    }
};

void FatTreeSwitch::addHostPort(int addr, int flowid, PacketSink* transport){
    Route* rt = new Route();
    rt->push_back(_ft->queues_nlp_ns[_ft->HOST_POD_SWITCH(addr)][addr][0]);
    rt->push_back(_ft->pipes_nlp_ns[_ft->HOST_POD_SWITCH(addr)][addr][0]);
    rt->push_back(transport);
    _fib->addHostRoute(addr,rt,flowid);
}

uint32_t mhash(uint32_t x) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}

uint32_t FatTreeSwitch::adaptive_route_p2c(vector<FibEntry*>* ecmp_set, int8_t (*cmp)(FibEntry*,FibEntry*)){
    uint32_t choice = 0, min = UINT32_MAX;
    uint32_t start, i = 0;
    static const uint16_t nr_choices = 2;
    
    do {
        start = random()%ecmp_set->size();

        Route * r= (*ecmp_set)[start]->getEgressPort();
        assert(r && r->size()>1);
        BaseQueue* q = (BaseQueue*)(r->at(0));
        assert(q);
        if (q->queuesize()<min){
            choice = start;
            min = q->queuesize();
        }
        i++;
    } while (i<nr_choices);
    return choice;
}

uint32_t FatTreeSwitch::adaptive_route(vector<FibEntry*>* ecmp_set, int8_t (*cmp)(FibEntry*,FibEntry*)){
    //cout << "adaptive_route" << endl;
    uint32_t choice = 0;

    uint32_t best_choices[256];
    uint32_t best_choices_count = 0;
  
    FibEntry* min = (*ecmp_set)[choice];
    best_choices[best_choices_count++] = choice;

    for (uint32_t i = 1; i< ecmp_set->size(); i++){
        int8_t c = cmp(min,(*ecmp_set)[i]);

        if (c < 0){
            choice = i;
            min = (*ecmp_set)[choice];
            best_choices_count = 0;
            best_choices[best_choices_count++] = choice;
        }
        else if (c==0){
            assert(best_choices_count<255);
            best_choices[best_choices_count++] = i;
        }        
    }

    assert (best_choices_count>=1);
    uint32_t choiceindex = random()%best_choices_count;
    choice = best_choices[choiceindex];
    //cout << "ECMP set choices " << ecmp_set->size() << " Choice count " << best_choices_count << " chosen entry " << choiceindex << " chosen path " << choice << " ";

    if (cmp==compare_flow_count){
        //for (uint32_t i = 0; i<best_choices_count;i++)
          //  cout << "pathcnt " << best_choices[i] << "="<< _port_flow_counts[(BaseQueue*)( (*ecmp_set)[best_choices[i]]->getEgressPort()->at(0))]<< " ";
        
        _port_flow_counts[(BaseQueue*)((*ecmp_set)[choice]->getEgressPort()->at(0))]++;
    }

    return choice;
}

uint32_t FatTreeSwitch::replace_worst_choice(vector<FibEntry*>* ecmp_set, int8_t (*cmp)(FibEntry*,FibEntry*),uint32_t my_choice){
    uint32_t best_choice = 0;
    uint32_t worst_choice = 0;

    uint32_t best_choices[256];
    uint32_t best_choices_count = 0;

    FibEntry* min = (*ecmp_set)[best_choice];
    FibEntry* max = (*ecmp_set)[worst_choice];
    best_choices[best_choices_count++] = best_choice;

    for (uint32_t i = 1; i< ecmp_set->size(); i++){
        int8_t c = cmp(min,(*ecmp_set)[i]);

        if (c < 0){
            best_choice = i;
            min = (*ecmp_set)[best_choice];
            best_choices_count = 0;
            best_choices[best_choices_count++] = best_choice;
        }
        else if (c==0){
            assert(best_choices_count<256);
            best_choices[best_choices_count++] = i;
        }        

        if (cmp(max,(*ecmp_set)[i])>0){
            worst_choice = i;
            max = (*ecmp_set)[worst_choice];
        }
    }

    //might need to play with different alternatives here, compare to worst rather than just to worst index.
    int8_t r = cmp((*ecmp_set)[my_choice],(*ecmp_set)[worst_choice]);
    assert(r>=0);

    if (r==0){
        assert (best_choices_count>=1);
        return best_choices[random()%best_choices_count];
    }
    else return my_choice;
}


int8_t FatTreeSwitch::compare_pause(FibEntry* left, FibEntry* right){
    Route * r1= left->getEgressPort();
    assert(r1 && r1->size()>1);
    LosslessOutputQueue* q1 = dynamic_cast<LosslessOutputQueue*>(r1->at(0));
    Route * r2= right->getEgressPort();
    assert(r2 && r2->size()>1);
    LosslessOutputQueue* q2 = dynamic_cast<LosslessOutputQueue*>(r2->at(0));

    if (!q1->is_paused()&&q2->is_paused())
        return 1;
    else if (q1->is_paused()&&!q2->is_paused())
        return -1;
    else 
        return 0;
}

int8_t FatTreeSwitch::compare_flow_count(FibEntry* left, FibEntry* right){
    Route * r1= left->getEgressPort();
    assert(r1 && r1->size()>1);
    BaseQueue* q1 = (BaseQueue*)(r1->at(0));
    Route * r2= right->getEgressPort();
    assert(r2 && r2->size()>1);
    BaseQueue* q2 = (BaseQueue*)(r2->at(0));

    if (_port_flow_counts.find(q1)==_port_flow_counts.end())
        _port_flow_counts[q1] = 0;

    if (_port_flow_counts.find(q2)==_port_flow_counts.end())
        _port_flow_counts[q2] = 0;

    //cout << "CMP q1 " << q1 << "=" << _port_flow_counts[q1] << " q2 " << q2 << "=" << _port_flow_counts[q2] << endl; 

    if (_port_flow_counts[q1] < _port_flow_counts[q2])
        return 1;
    else if (_port_flow_counts[q1] > _port_flow_counts[q2] )
        return -1;
    else 
        return 0;
}

int8_t FatTreeSwitch::compare_queuesize(FibEntry* left, FibEntry* right){
    Route * r1= left->getEgressPort();
    assert(r1 && r1->size()>1);
    BaseQueue* q1 = dynamic_cast<BaseQueue*>(r1->at(0));
    Route * r2= right->getEgressPort();
    assert(r2 && r2->size()>1);
    BaseQueue* q2 = dynamic_cast<BaseQueue*>(r2->at(0));

    if (q1->quantized_queuesize() < q2->quantized_queuesize())
        return 1;
    else if (q1->quantized_queuesize() > q2->quantized_queuesize())
        return -1;
    else 
        return 0;
}

int8_t FatTreeSwitch::compare_bandwidth(FibEntry* left, FibEntry* right){
    Route * r1= left->getEgressPort();
    assert(r1 && r1->size()>1);
    BaseQueue* q1 = dynamic_cast<BaseQueue*>(r1->at(0));
    Route * r2= right->getEgressPort();
    assert(r2 && r2->size()>1);
    BaseQueue* q2 = dynamic_cast<BaseQueue*>(r2->at(0));

    if (q1->quantized_utilization() < q2->quantized_utilization())
        return 1;
    else if (q1->quantized_utilization() > q2->quantized_utilization())
        return -1;
    else 
        return 0;

    /*if (q1->average_utilization() < q2->average_utilization())
        return 1;
    else if (q1->average_utilization() > q2->average_utilization())
        return -1;
    else 
        return 0;        */
}

int8_t FatTreeSwitch::compare_pqb(FibEntry* left, FibEntry* right){
    //compare pause, queuesize, bandwidth.
    int8_t p = compare_pause(left, right);

    if (p!=0)
        return p;
    
    p = compare_queuesize(left,right);

    if (p!=0)
        return p;

    return compare_bandwidth(left,right);
}

int8_t FatTreeSwitch::compare_pq(FibEntry* left, FibEntry* right){
    //compare pause, queuesize, bandwidth.
    int8_t p = compare_pause(left, right);

    if (p!=0)
        return p;
    
    return compare_queuesize(left,right);
}

int8_t FatTreeSwitch::compare_qb(FibEntry* left, FibEntry* right){
    //compare pause, queuesize, bandwidth.
    int8_t p = compare_queuesize(left, right);

    if (p!=0)
        return p;
    
    return compare_bandwidth(left,right);
}

int8_t FatTreeSwitch::compare_pb(FibEntry* left, FibEntry* right){
    //compare pause, queuesize, bandwidth.
    int8_t p = compare_pause(left, right);

    if (p!=0)
        return p;
    
    return compare_bandwidth(left,right);
}

void FatTreeSwitch::permute_paths(vector<FibEntry *>* uproutes) {
    if (!uproutes)
        return;
    int len = uproutes->size();
    for (int i = 0; i < len; i++) {
        int ix = random() % (len - i);
        FibEntry* tmppath = (*uproutes)[ix];
        (*uproutes)[ix] = (*uproutes)[len-1-i];
        (*uproutes)[len-1-i] = tmppath;
    }
}

FatTreeSwitch::routing_strategy FatTreeSwitch::_strategy = FatTreeSwitch::NIX;

static double paper_sglb_clamp01(double value) {
    return std::max(0.0, std::min(1.0, value));
}

double FatTreeSwitch::paper_sglb_value(const PaperSglbFactors& factors) {
    if (_paper_sglb_ablation == PAPER_SGLB_ABLATION_LEGACY_NOISY_OR)
        return paper_sglb_noisy_or(factors.local_queue,
                                   factors.remote_queue);
    if (_paper_sglb_ablation == PAPER_SGLB_ABLATION_LEGACY_QUEUE_PRESSURE)
        return paper_sglb_clamp01(
            0.5 * paper_sglb_queue_pressure(factors.local_queue) +
            0.5 * paper_sglb_queue_pressure(factors.remote_queue));
    const double value =
        _paper_sglb_weight_local_queue * paper_sglb_clamp01(factors.local_queue) +
        _paper_sglb_weight_local_util * paper_sglb_clamp01(factors.local_utilization) +
        _paper_sglb_weight_remote_queue * paper_sglb_clamp01(factors.remote_queue) +
        _paper_sglb_weight_remote_util * paper_sglb_clamp01(factors.remote_utilization) +
        _paper_sglb_weight_remote_busy * paper_sglb_clamp01(factors.remote_busyness);
    return paper_sglb_clamp01(value);
}

uint8_t FatTreeSwitch::paper_sglb_ar_level(double value) {
    value = paper_sglb_clamp01(value);
    if (_paper_sglb_ablation == PAPER_SGLB_ABLATION_LEGACY_LEVELS) {
        if (value < 0.10) return 0;
        if (value < 0.40) return 1;
        if (value < 0.60) return 2;
        return 3;
    }
    if (value < 0.05)
        return 0;
    if (value < 0.10)
        return 1;
    if (value < 0.20)
        return 2;
    return 3;
}

vector<uint32_t> FatTreeSwitch::paper_sglb_best_level(
    const vector<uint8_t>& levels, const vector<bool>& available,
    uint32_t min_choices) {
    vector<uint32_t> choices;
    const size_t count = std::min(levels.size(), available.size());
    uint8_t best = 255;
    for (size_t i = 0; i < count; ++i) {
        if (available[i] && levels[i] < best)
            best = levels[i];
    }
    const uint32_t required = min_choices ? min_choices : 1;
    for (uint32_t level = best;
         level <= 255 && choices.size() < required; ++level) {
        for (uint32_t i = 0; i < count; ++i) {
            if (available[i] && levels[i] == level)
                choices.push_back(i);
        }
        if (level == 255)
            break;
    }
    return choices;
}

vector<uint32_t> FatTreeSwitch::paper_sglb_exact_min_by_level(
    const vector<uint8_t>& levels, const vector<bool>& available,
    const vector<uint64_t>& tie_keys, uint32_t min_choices) {
    vector<uint32_t> choices;
    const size_t count = std::min(
        levels.size(), std::min(available.size(), tie_keys.size()));
    uint8_t best = 255;
    for (size_t i = 0; i < count; ++i) {
        if (available[i] && levels[i] < best)
            best = levels[i];
    }
    for (uint32_t i = 0; i < count; ++i) {
        if (available[i])
            choices.push_back(i);
    }
    std::sort(choices.begin(), choices.end(), [&](uint32_t a, uint32_t b) {
        if (levels[a] != levels[b])
            return levels[a] < levels[b];
        if (tie_keys[a] != tie_keys[b])
            return tie_keys[a] < tie_keys[b];
        return a < b;
    });

    const uint32_t required = min_choices ? min_choices : 1;
    size_t best_count = 0;
    while (best_count < choices.size() && levels[choices[best_count]] == best)
        ++best_count;
    const size_t keep = std::max(best_count,
                                 std::min<size_t>(required, choices.size()));
    choices.resize(keep);
    return choices;
}

vector<uint32_t> FatTreeSwitch::paper_sglb_strict_k_by_level(
    const vector<uint8_t>& levels, const vector<bool>& available,
    const vector<uint64_t>& tie_keys, uint32_t requested) {
    vector<uint32_t> choices;
    const size_t count = std::min(
        levels.size(), std::min(available.size(), tie_keys.size()));
    for (uint32_t i = 0; i < count; ++i) {
        if (available[i])
            choices.push_back(i);
    }
    std::sort(choices.begin(), choices.end(), [&](uint32_t a, uint32_t b) {
        if (levels[a] != levels[b])
            return levels[a] < levels[b];
        if (tie_keys[a] != tie_keys[b])
            return tie_keys[a] < tie_keys[b];
        return a < b;
    });
    const size_t keep = std::min<size_t>(requested ? requested : 1,
                                         choices.size());
    choices.resize(keep);
    return choices;
}

uint32_t FatTreeSwitch::sglb_shuffled_rr_select(
    const vector<uint32_t>& candidates, uint32_t switch_id,
    uint32_t dst_tor, uint64_t quality_signature,
    SglbShuffledRrState& state) {
    if (candidates.empty())
        return UINT32_MAX;

    vector<uint32_t> members = candidates;
    std::sort(members.begin(), members.end());
    const bool changed = !state.valid || state.members != members ||
        state.quality_signature != quality_signature;
    if (changed) {
        state.members = members;
        state.quality_signature = quality_signature;
        state.cursor = 0;
        state.generation++;
        state.valid = true;
    } else if (state.cursor >= state.order.size()) {
        state.cursor = 0;
        state.generation++;
    }

    if (changed || state.order.empty() || state.cursor == 0) {
        state.order = state.members;
        const uint32_t generation_low =
            static_cast<uint32_t>(state.generation);
        const uint32_t generation_high =
            static_cast<uint32_t>(state.generation >> 32);
        std::sort(state.order.begin(), state.order.end(),
                  [&](uint32_t a, uint32_t b) {
            uint32_t ah = freeBSDHash(
                switch_id ^ generation_high, dst_tor ^ generation_low, a);
            uint32_t bh = freeBSDHash(
                switch_id ^ generation_high, dst_tor ^ generation_low, b);
            if (ah != bh)
                return ah < bh;
            return a < b;
        });
    }
    return state.order[state.cursor++];
}

vector<uint32_t> FatTreeSwitch::paper_sglb_topk_by_level(
    const vector<uint8_t>& levels, const vector<bool>& available,
    const vector<uint64_t>& tie_keys, uint32_t k) {
    vector<uint32_t> choices;
    const size_t count = std::min(
        levels.size(), std::min(available.size(), tie_keys.size()));
    for (uint32_t i = 0; i < count; ++i) {
        if (available[i])
            choices.push_back(i);
    }
    std::sort(choices.begin(), choices.end(), [&](uint32_t a, uint32_t b) {
        if (levels[a] != levels[b])
            return levels[a] < levels[b];
        if (tie_keys[a] != tie_keys[b])
            return tie_keys[a] < tie_keys[b];
        return a < b;
    });
    if (choices.size() > k)
        choices.resize(k);
    return choices;
}

bool FatTreeSwitch::paper_sglb_snapshot_refresh_due(
    bool valid, simtime_picosec last_update, simtime_picosec now,
    simtime_picosec interval) {
    return !valid || interval == 0 || now < last_update ||
        now - last_update >= interval;
}

bool FatTreeSwitch::paper_sglb_emit_due(
    bool has_sent, simtime_picosec last_sent, simtime_picosec now,
    simtime_picosec interval) {
    return !has_sent || interval == 0 || now < last_sent ||
        now - last_sent >= interval;
}

double FatTreeSwitch::paper_sglb_noisy_or(double local, double remote) {
    local = std::max(0.0, std::min(1.0, local));
    remote = std::max(0.0, std::min(1.0, remote));
    return 1.0 - (1.0 - local) * (1.0 - remote);
}

double FatTreeSwitch::paper_sglb_queue_pressure(double queue_fraction) {
    if (_paper_sglb_q_high <= _paper_sglb_q_low)
        return queue_fraction >= _paper_sglb_q_high ? 1.0 : 0.0;
    return std::max(0.0, std::min(
        1.0, (queue_fraction - _paper_sglb_q_low) /
                 (_paper_sglb_q_high - _paper_sglb_q_low)));
}

uint8_t FatTreeSwitch::paper_sglb_quantized_level(double score,
                                                   uint32_t levels) {
    if (levels == 0)
        return 0;
    score = std::max(0.0, std::min(1.0, score));
    uint32_t level = static_cast<uint32_t>(score * levels);
    if (level >= levels)
        level = levels - 1;
    return static_cast<uint8_t>(level);
}

vector<uint32_t> FatTreeSwitch::paper_sglb_strict_topk(
    const vector<uint8_t>& levels, const vector<double>& scores,
    const vector<bool>& available, const vector<uint64_t>& stable_keys,
    uint32_t k) {
    vector<uint32_t> choices;
    const size_t n = std::min(
        std::min(levels.size(), scores.size()),
        std::min(available.size(), stable_keys.size()));
    for (uint32_t i = 0; i < n; ++i) {
        if (available[i])
            choices.push_back(i);
    }
    std::sort(choices.begin(), choices.end(),
              [&](uint32_t a, uint32_t b) {
                  if (levels[a] != levels[b]) return levels[a] < levels[b];
                  if (scores[a] != scores[b]) return scores[a] < scores[b];
                  if (stable_keys[a] != stable_keys[b])
                      return stable_keys[a] < stable_keys[b];
                  return a < b;
              });
    if (choices.size() > k)
        choices.resize(k);
    return choices;
}

bool FatTreeSwitch::paper_sglb_accept_gcn(
    PaperSglbRemoteState& state, bool link_up, double remote_queue,
    double remote_utilization, double remote_busyness, uint8_t level,
    uint64_t version, simtime_picosec received_at) {
    if (_paper_sglb_ablation != PAPER_SGLB_ABLATION_NO_VERSION &&
        state.valid && version <= state.version)
        return false;
    state.level = level;
    state.version = version;
    state.link_up = link_up;
    state.remote_queue = paper_sglb_clamp01(remote_queue);
    state.remote_utilization = paper_sglb_clamp01(remote_utilization);
    state.remote_busyness = paper_sglb_clamp01(remote_busyness);
    state.received_at = received_at;
    state.valid = true;
    return true;
}

void FatTreeSwitch::initialize_paper_sglb(
    FatTreeTopology* topology, simtime_picosec propagation_delay) {
    const bool shadow_gcn = _strategy == SGLB &&
        _sglb_ofat_factor == SGLB_OFAT_SHADOW_GCN;
    if (_strategy != PAPER_SGLB && !shadow_gcn)
        return;
    if (!topology || FatTreeTopology::get_tiers() != 2) {
        cerr << "sglb-paper requires a two-tier Clos topology" << endl;
        abort();
    }
    if (_paper_sglb_k == 0) {
        cerr << "invalid sglb-paper top-K configuration" << endl;
        abort();
    }
    _paper_sglb_gcn_delay = propagation_delay;
    const simtime_picosec now = topology->_eventlist->now();
    for (size_t source = 0; source < topology->switches_lp.size(); ++source) {
        FatTreeSwitch* tor = dynamic_cast<FatTreeSwitch*>(
            topology->switches_lp[source]);
        if (!tor)
            continue;
        for (uint32_t spine = 0; spine < topology->getNAGG(); ++spine) {
            const vector<BaseQueue*>& lag =
                topology->queues_nlp_nup[source][spine];
            for (size_t bundle = 0; bundle < lag.size(); ++bundle) {
                BaseQueue* queue = lag[bundle];
                PaperSglbLocalState state;
                state.queue = queue && queue->maxsize() ?
                    static_cast<double>(queue->queuesize()) /
                        queue->maxsize() : 1.0;
                state.utilization = queue ?
                    static_cast<double>(queue->peek_average_utilization()) /
                        100.0 : 1.0;
                state.last_update = now;
                state.valid = true;
                tor->_paper_sglb_local[queue] = state;
            }
        }
    }
    for (uint32_t spine = 0; spine < topology->getNAGG(); ++spine) {
        FatTreeSwitch* producer_switch = dynamic_cast<FatTreeSwitch*>(
            topology->switches_up[spine]);
        assert(producer_switch);
        if (!producer_switch->_paper_sglb_gcn_flow)
            producer_switch->_paper_sglb_gcn_flow = new PacketFlow(NULL);

        for (uint32_t receiver = 0;
             receiver < topology->switches_lp.size(); ++receiver) {
            vector<Route*>& routes =
                producer_switch->_paper_sglb_gcn_routes[receiver];
            const vector<BaseQueue*>& queues =
                topology->queues_nup_nlp[spine][receiver];
            const vector<Pipe*>& pipes =
                topology->pipes_nup_nlp[spine][receiver];
            for (size_t bundle = 0;
                 bundle < queues.size() && bundle < pipes.size(); ++bundle) {
                if (!queues[bundle] || !pipes[bundle])
                    continue;
                Route* route = new Route();
                route->push_back(queues[bundle]);
                route->push_back(pipes[bundle]);
                route->push_back(queues[bundle]->getRemoteEndpoint());
                routes.push_back(route);
            }
        }

        for (uint32_t dst_tor = 0;
             dst_tor < topology->switches_lp.size(); ++dst_tor) {
            const PaperSglbFactors factors = paper_sglb_remote_factors(
                topology, spine, dst_tor);
            PaperSglbFactors remote_only = factors;
            const uint8_t level = paper_sglb_ar_level(
                paper_sglb_value(remote_only));
            for (size_t source = 0;
                 source < topology->switches_lp.size(); ++source) {
                FatTreeSwitch* tor = dynamic_cast<FatTreeSwitch*>(
                    topology->switches_lp[source]);
                if (tor && _paper_sglb_ablation !=
                               PAPER_SGLB_ABLATION_LAZY_INIT)
                    paper_sglb_accept_gcn(
                        tor->_paper_sglb_remote[
                            paper_sglb_remote_key(spine, dst_tor)],
                        true, factors.remote_queue,
                        factors.remote_utilization,
                        factors.remote_busyness, level, 0,
                        tor->eventlist().now());
            }
            PaperSglbProducerState& producer =
                producer_switch->_paper_sglb_producers[dst_tor];
            producer.current = factors;
            producer.advertised = factors;
            producer.last_sample = now;
            producer.valid = true;
        }
        producer_switch->_paper_sglb_gcn_dirty = true;
        producer_switch->_paper_sglb_gcn_has_sent = false;
        producer_switch->_paper_sglb_gcn_version = 0;
        producer_switch->_paper_sglb_gcn_last_sent = 0;
    }
}

void FatTreeSwitch::paper_sglb_observe_remote_on_lookup(
    uint32_t destination_tor) {
    const bool shadow_gcn = _strategy == SGLB &&
        _sglb_ofat_factor == SGLB_OFAT_SHADOW_GCN;
    if ((_strategy != PAPER_SGLB && !shadow_gcn) || _type != AGG || !_ft)
        return;
    PaperSglbProducerState& producer = _paper_sglb_producers[destination_tor];
    const simtime_picosec now = eventlist().now();
    if (paper_sglb_snapshot_refresh_due(
            producer.valid, producer.last_sample, now,
            _paper_sglb_sample_interval)) {
        const PaperSglbFactors observed = paper_sglb_remote_factors(
            _ft, _id, destination_tor);
        producer.current = observed;
        producer.last_sample = now;
        producer.valid = true;
        if (!paper_sglb_remote_factors_equal(
                producer.current, producer.advertised))
            _paper_sglb_gcn_dirty = true;
    }
    paper_sglb_emit_if_due();
}

void FatTreeSwitch::paper_sglb_emit_if_due() {
    const simtime_picosec now = eventlist().now();
    if (!_paper_sglb_gcn_dirty ||
        !paper_sglb_emit_due(_paper_sglb_gcn_has_sent,
                             _paper_sglb_gcn_last_sent, now,
                             _paper_sglb_gcn_interval))
        return;

    ++_paper_sglb_gcn_version;
    vector<SglbGcnRecord> records;
    records.reserve(_paper_sglb_producers.size());
    for (unordered_map<uint32_t,PaperSglbProducerState>::const_iterator it =
             _paper_sglb_producers.begin();
         it != _paper_sglb_producers.end(); ++it) {
        if (!it->second.valid)
            continue;
        const PaperSglbFactors& factors = it->second.current;
        const uint8_t port_quality = paper_sglb_ar_level(
            paper_sglb_value(factors));
        records.push_back(SglbGcnRecord(
            it->first, it->first, true, factors.remote_queue,
            factors.remote_utilization, factors.remote_busyness,
            port_quality));
    }
    std::sort(records.begin(), records.end(),
              [](const SglbGcnRecord& left, const SglbGcnRecord& right) {
                  return left.destination_switch_id <
                      right.destination_switch_id;
              });
    for (uint32_t receiver = 0;
         receiver < _ft->switches_lp.size(); ++receiver) {
        vector<Route*>& routes = _paper_sglb_gcn_routes[receiver];
        if (routes.empty())
            continue;
        Route* route = routes[
            (_paper_sglb_gcn_version + receiver) % routes.size()];
        SglbGcnPacket* packet = SglbGcnPacket::newpkt(
            *_paper_sglb_gcn_flow, *route, _id, records,
            _paper_sglb_gcn_version, now,
            _paper_sglb_ablation ==
                    PAPER_SGLB_ABLATION_NO_CONTROL_BANDWIDTH ?
                0 : SglbGcnPacket::PACKET_SIZE);
        ++_paper_sglb_diag_gcn_packets;
        _paper_sglb_diag_gcn_bytes += packet->size();
        packet->sendOn();
    }
    for (unordered_map<uint32_t,PaperSglbProducerState>::iterator it =
             _paper_sglb_producers.begin();
         it != _paper_sglb_producers.end(); ++it)
        it->second.advertised = it->second.current;
    _paper_sglb_gcn_last_sent = now;
    _paper_sglb_gcn_has_sent = true;
    _paper_sglb_gcn_dirty = false;
    ++_paper_sglb_diag_gcn_updates;
}

void FatTreeSwitch::receive_paper_sglb_gcn(SglbGcnPacket& packet) {
    uint64_t accepted = 0;
    for (size_t i = 0; i < packet.record_count(); ++i) {
        const SglbGcnRecord& record = packet.record(i);
        if (paper_sglb_accept_gcn(
                _paper_sglb_remote[paper_sglb_remote_key(
                    packet.sender_switch_id(),
                    record.destination_switch_id)],
                record.link_up, record.remote_queue,
                record.remote_utilization, record.remote_busyness,
                record.port_quality, packet.version(), eventlist().now()))
            ++accepted;
    }
    if (accepted) {
        ++_paper_sglb_diag_gcn_deliveries;
        _paper_sglb_diag_gcn_profile_updates += accepted;
    } else {
        ++_paper_sglb_diag_gcn_stale;
    }
}

const char* FatTreeSwitch::paper_sglb_remote_mode_name() {
    return _paper_sglb_remote_mode == PAPER_SGLB_REMOTE_DIRECT ?
        "direct" : "gcn-profile";
}

const char* FatTreeSwitch::paper_sglb_ablation_name() {
    static const char* names[] = {
        "none", "legacy_candidates", "legacy_noisy_or",
        "legacy_queue_pressure", "legacy_levels",
        "legacy_remote_semantics", "legacy_transport", "lazy_init",
        "no_version", "all_switch_decisions", "legacy_background",
        "no_control_bandwidth"
    };
    const uint32_t value = static_cast<uint32_t>(_paper_sglb_ablation);
    return value < sizeof(names) / sizeof(names[0]) ? names[value] : "invalid";
}

uint32_t FatTreeSwitch::paper_sglb_route(vector<FibEntry*>* ecmp_set,
                                         Packet& pkt) {
    ++_paper_sglb_diag_route_calls;
    const uint32_t dst_tor = _ft->HOST_POD_SWITCH(pkt.dst());
    vector<uint8_t> levels(ecmp_set->size(), 255);
    vector<bool> available(ecmp_set->size(), false);
    vector<uint64_t> tie_keys(ecmp_set->size(), 0);
    const simtime_picosec now = eventlist().now();
    for (uint32_t i = 0; i < ecmp_set->size(); ++i) {
        FibEntry* entry = (*ecmp_set)[i];
        Route* route = entry ? entry->getEgressPort() : NULL;
        BaseQueue* local = route && route->size() > 0 ?
            dynamic_cast<BaseQueue*>(route->at(0)) : NULL;
        const uint32_t spine = sglb_next_hop_id(entry);
        available[i] = local && spine != UINT32_MAX &&
            sglb_entry_available(entry);
        if (!available[i])
            continue;

        PaperSglbLocalState& local_sample = _paper_sglb_local[local];
        if (paper_sglb_snapshot_refresh_due(
                local_sample.valid, local_sample.last_update, now,
                _paper_sglb_sample_interval)) {
            local_sample.queue = local->maxsize() ?
                static_cast<double>(local->queuesize()) / local->maxsize() : 1.0;
            local_sample.utilization =
                static_cast<double>(local->peek_average_utilization()) / 100.0;
            local_sample.last_update = now;
            local_sample.valid = true;
        }

        unordered_map<uint64_t,PaperSglbRemoteState>::const_iterator remote =
            _paper_sglb_remote.find(
                paper_sglb_remote_key(spine, dst_tor));
        PaperSglbFactors factors;
        factors.local_queue = local_sample.queue;
        factors.local_utilization = local_sample.utilization;
        if (_paper_sglb_ablation ==
                PAPER_SGLB_ABLATION_LEGACY_REMOTE_SEMANTICS) {
            const SglbPathState* legacy = sglb_neighbor_snapshot(
                entry, pkt.dst());
            if (legacy) {
                factors.remote_queue = legacy->queue_fraction;
                factors.remote_utilization = 0.0;
                factors.remote_busyness = legacy->avg_busy;
            } else {
                factors.remote_queue = 0.0;
                factors.remote_utilization = 0.0;
                factors.remote_busyness = 0.0;
            }
        } else if (_paper_sglb_remote_mode == PAPER_SGLB_REMOTE_DIRECT ||
                   _paper_sglb_ablation ==
                       PAPER_SGLB_ABLATION_LEGACY_TRANSPORT) {
            const PaperSglbFactors observed = paper_sglb_remote_factors(
                _ft, spine, dst_tor);
            factors.remote_queue = observed.remote_queue;
            factors.remote_utilization = observed.remote_utilization;
            factors.remote_busyness = observed.remote_busyness;
        } else if (remote == _paper_sglb_remote.end() ||
                   !remote->second.valid) {
            factors.remote_queue = 1.0;
            factors.remote_utilization = 1.0;
            factors.remote_busyness = 1.0;
            ++_paper_sglb_diag_remote_missing;
        } else {
            factors.remote_queue = remote->second.remote_queue;
            factors.remote_utilization = remote->second.remote_utilization;
            factors.remote_busyness = remote->second.remote_busyness;
            available[i] = available[i] && remote->second.link_up;
        }
        levels[i] = paper_sglb_ar_level(paper_sglb_value(factors));
        tie_keys[i] = (static_cast<uint64_t>(random()) << 32) ^ random();
    }
    vector<uint32_t> candidates =
        _paper_sglb_ablation == PAPER_SGLB_ABLATION_LEGACY_CANDIDATES ?
        paper_sglb_best_level(levels, available, 3) :
        _paper_sglb_selection_mode == PAPER_SGLB_TOPK ?
        paper_sglb_topk_by_level(
            levels, available, tie_keys, _paper_sglb_k) :
        paper_sglb_best_level(levels, available, _paper_sglb_k);
    _paper_sglb_diag_candidate_sum += candidates.size();
    if (candidates.empty())
        return pathid_ecmp_choice(
            pkt, ecmp_set->size(), (*ecmp_set)[0]->getDirection());
    return candidates[random() % candidates.size()];
}
uint16_t FatTreeSwitch::_ar_fraction = 0;
uint16_t FatTreeSwitch::_ar_sticky = FatTreeSwitch::PER_PACKET;
simtime_picosec FatTreeSwitch::_sticky_delta = timeFromUs((uint32_t)10);
double FatTreeSwitch::_ecn_threshold_fraction = 1.0;
uint64_t FatTreeSwitch::_lossless_ecn_kmin_bytes = 0;
uint64_t FatTreeSwitch::_lossless_ecn_kmax_bytes = 0;
double FatTreeSwitch::_speculative_threshold_fraction = 0.2;
double FatTreeSwitch::_sglb_downstream_weight = 0.25;
double FatTreeSwitch::_sglb_queue_weight = 0.5;
double FatTreeSwitch::_sglb_util_weight = 0.05;
double FatTreeSwitch::_sglb_remote_queue_weight = 0.5;
double FatTreeSwitch::_sglb_remote_util_weight = 0.05;
double FatTreeSwitch::_sglb_remote_busy_weight = 0.2;
double FatTreeSwitch::_sglb_quality_bucket = 20.0;
uint32_t FatTreeSwitch::_sglb_max_quality = 7;
simtime_picosec FatTreeSwitch::_sglb_update_interval = timeFromUs(1.0);
uint32_t FatTreeSwitch::_sglb_quality_levels = 8;
uint32_t FatTreeSwitch::_sglb_min_choices = 1;
uint32_t FatTreeSwitch::_sglb_topk = 20;
FatTreeSwitch::SglbCandidatePolicy FatTreeSwitch::_sglb_candidate_policy =
    FatTreeSwitch::SGLB_CANDIDATE_WHOLE_GRADE_MIN;
FatTreeSwitch::SglbCandidateDispatch FatTreeSwitch::_sglb_candidate_dispatch =
    FatTreeSwitch::SGLB_DISPATCH_RANDOM;
FatTreeSwitch::SglbGcnCadence FatTreeSwitch::_sglb_gcn_cadence =
    FatTreeSwitch::SGLB_GCN_INDEPENDENT;
simtime_picosec FatTreeSwitch::_sglb_gcn_update_interval = timeFromUs(15.0);
simtime_picosec FatTreeSwitch::_sglb_gcn_aging_interval = timeFromUs(30.0);
simtime_picosec FatTreeSwitch::_paper_sglb_sample_interval = timeFromUs(1.0);
simtime_picosec FatTreeSwitch::_paper_sglb_gcn_interval = timeFromUs(15.0);
simtime_picosec FatTreeSwitch::_paper_sglb_gcn_delay = 0;
double FatTreeSwitch::_paper_sglb_weight_local_queue = 0.5;
double FatTreeSwitch::_paper_sglb_weight_local_util = 0.0;
double FatTreeSwitch::_paper_sglb_weight_remote_queue = 0.5;
double FatTreeSwitch::_paper_sglb_weight_remote_util = 0.0;
double FatTreeSwitch::_paper_sglb_weight_remote_busy = 0.0;
FatTreeSwitch::PaperSglbSelectionMode FatTreeSwitch::_paper_sglb_selection_mode =
    FatTreeSwitch::PAPER_SGLB_BEST_LEVEL;
FatTreeSwitch::PaperSglbRemoteMode FatTreeSwitch::_paper_sglb_remote_mode =
    FatTreeSwitch::PAPER_SGLB_REMOTE_GCN_PROFILE;
FatTreeSwitch::PaperSglbAblation FatTreeSwitch::_paper_sglb_ablation =
    FatTreeSwitch::PAPER_SGLB_ABLATION_NONE;
uint32_t FatTreeSwitch::_paper_sglb_levels = 4;
uint32_t FatTreeSwitch::_paper_sglb_k = 8;
double FatTreeSwitch::_paper_sglb_q_low = 0.20;
double FatTreeSwitch::_paper_sglb_q_high = 0.80;
uint64_t FatTreeSwitch::_paper_sglb_diag_route_calls = 0;
uint64_t FatTreeSwitch::_paper_sglb_diag_candidate_sum = 0;
uint64_t FatTreeSwitch::_paper_sglb_diag_remote_missing = 0;
uint64_t FatTreeSwitch::_paper_sglb_diag_gcn_updates = 0;
uint64_t FatTreeSwitch::_paper_sglb_diag_gcn_deliveries = 0;
uint64_t FatTreeSwitch::_paper_sglb_diag_gcn_packets = 0;
uint64_t FatTreeSwitch::_paper_sglb_diag_gcn_bytes = 0;
uint64_t FatTreeSwitch::_paper_sglb_diag_gcn_stale = 0;
uint64_t FatTreeSwitch::_paper_sglb_diag_gcn_profile_updates = 0;
bool FatTreeSwitch::_sglb_normalize_scores = false;
bool FatTreeSwitch::_sglb_local_damping = false;
FatTreeSwitch::SglbScoreMode FatTreeSwitch::_sglb_score_mode =
    FatTreeSwitch::SGLB_SCORE_NMRC_QUANTIZED_TOPK;
FatTreeSwitch::SglbOfatFactor FatTreeSwitch::_sglb_ofat_factor =
    FatTreeSwitch::SGLB_OFAT_BASELINE;
simtime_picosec FatTreeSwitch::_sglb_ofat_message_delay = timeFromUs(0.5);
double FatTreeSwitch::_sglb_nmrc_q_min = 0.20;
double FatTreeSwitch::_sglb_nmrc_q_max = 0.80;
double FatTreeSwitch::_sglb_nmrc_degraded_threshold = 0.10;
double FatTreeSwitch::_sglb_nmrc_bad_threshold = 0.40;
double FatTreeSwitch::_sglb_nmrc_avoid_threshold = 0.60;
uint32_t FatTreeSwitch::_sglb_nmrc_levels = 4;
uint64_t FatTreeSwitch::_sglb_diag_route_calls = 0;
uint64_t FatTreeSwitch::_sglb_diag_available_choices = 0;
uint64_t FatTreeSwitch::_sglb_diag_candidate_choices = 0;
uint64_t FatTreeSwitch::_sglb_diag_best_quality_choices = 0;
uint64_t FatTreeSwitch::_sglb_diag_distinct_qualities = 0;
uint64_t FatTreeSwitch::_sglb_diag_all_same_quality_calls = 0;
uint64_t FatTreeSwitch::_sglb_diag_all_zero_quality_calls = 0;
uint64_t FatTreeSwitch::_sglb_diag_selected_nonbest_quality = 0;
uint64_t FatTreeSwitch::_sglb_diag_remote_snapshot_used = 0;
uint64_t FatTreeSwitch::_sglb_diag_remote_snapshot_missing = 0;
uint64_t FatTreeSwitch::_sglb_diag_observed_levels[4] = {0, 0, 0, 0};
uint64_t FatTreeSwitch::_sglb_diag_selected_levels[4] = {0, 0, 0, 0};
double FatTreeSwitch::_sglb_diag_score_spread_sum = 0.0;
bool FatTreeSwitch::_nmrc_hybrid_enabled = false;
bool FatTreeSwitch::_nmrc_fastcnp_enabled = true;
FatTreeSwitch::NmrcReroutePolicy FatTreeSwitch::_nmrc_reroute_policy =
    FatTreeSwitch::NMRC_REROUTE_BETTER_GE3;
FatTreeSwitch::NmrcNetworkDecisionMode
    FatTreeSwitch::_nmrc_network_decision_mode =
        FatTreeSwitch::NMRC_NETWORK_GRADED;
FatTreeSwitch::NmrcGradedCooldownMode
    FatTreeSwitch::_nmrc_graded_cooldown_mode =
        FatTreeSwitch::NMRC_GRADED_COOLDOWN_SELECTIVE;
double FatTreeSwitch::_nmrc_graded_reroute_delta = 0.0;
double FatTreeSwitch::_nmrc_graded_cooldown_delta = 0.25;
double FatTreeSwitch::_nmrc_absolute_threshold = 0.50;
double FatTreeSwitch::_nmrc_relative_delta = 0.25;
double FatTreeSwitch::_nmrc_piecewise_delta_below = 0.25;
double FatTreeSwitch::_nmrc_piecewise_delta_above = 0.15;
double FatTreeSwitch::_nmrc_route_delta = 0.10;
double FatTreeSwitch::_nmrc_cooldown_delta = 0.30;
const double FatTreeSwitch::NMRC_RELATIVE_EPSILON = 1e-12;
uint64_t FatTreeSwitch::_nmrc_diag_route_checks = 0;
uint64_t FatTreeSwitch::_nmrc_diag_reroutes = 0;
uint64_t FatTreeSwitch::_nmrc_diag_threshold_blocked = 0;
uint64_t FatTreeSwitch::_nmrc_diag_fastcnp_generated = 0;
uint64_t FatTreeSwitch::_nmrc_diag_fastcnp_route_missing = 0;
uint64_t FatTreeSwitch::_nmrc_diag_graded_cooldown_requested = 0;
uint64_t FatTreeSwitch::_nmrc_diag_graded_cooldown_suppressed = 0;
uint64_t FatTreeSwitch::_nmrc_diag_better_count[33] = {0};
uint64_t FatTreeSwitch::_nmrc_diag_level_transitions[4][4] = {{0}};
uint64_t FatTreeSwitch::_nmrc_diag_binary_original_safe = 0;
uint64_t FatTreeSwitch::_nmrc_diag_binary_original_congested = 0;
uint64_t FatTreeSwitch::_nmrc_diag_binary_no_safe = 0;
uint64_t FatTreeSwitch::_nmrc_diag_binary_route_missing = 0;
uint64_t FatTreeSwitch::_nmrc_diag_binary_paired_actions = 0;
double FatTreeSwitch::_nmrc_diag_binary_original_score_sum = 0.0;
double FatTreeSwitch::_nmrc_diag_binary_original_score_max = 0.0;
double FatTreeSwitch::_nmrc_diag_binary_selected_score_sum = 0.0;
double FatTreeSwitch::_nmrc_diag_binary_selected_score_max = 0.0;
uint64_t FatTreeSwitch::_nmrc_diag_binary_actual_egress[33] = {0};
uint64_t FatTreeSwitch::_nmrc_diag_relative_checks = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_original_unknown = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_original_unavailable = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_original_below_absolute = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_no_safe_candidate = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_no_delta_candidate = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_reverse_path_blocked = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_paired_actions = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_reroutes = 0;
uint64_t FatTreeSwitch::_nmrc_diag_two_stage_reroute_only = 0;
uint64_t FatTreeSwitch::_nmrc_diag_two_stage_cooldown_requested = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_selected_gap_violations = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_decision_ce_set = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_decision_ce_cleared = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_reroute_key_count = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_reroute_key_sum = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_reroute_key_xor = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_generated_key_count = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_generated_key_sum = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_generated_key_xor = 0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_original_score_count = 0;
double FatTreeSwitch::_nmrc_diag_relative_original_score_sum = 0.0;
double FatTreeSwitch::_nmrc_diag_relative_original_score_max = 0.0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_selected_score_count = 0;
double FatTreeSwitch::_nmrc_diag_relative_selected_score_sum = 0.0;
double FatTreeSwitch::_nmrc_diag_relative_selected_score_max = 0.0;
uint64_t FatTreeSwitch::_nmrc_diag_relative_candidate_count[33] = {0};
uint64_t FatTreeSwitch::_nmrc_diag_relative_best_gap[21] = {0};
uint64_t FatTreeSwitch::_nmrc_diag_relative_selected_gap[21] = {0};
uint64_t FatTreeSwitch::_nmrc_diag_relative_original_score[21] = {0};
uint64_t FatTreeSwitch::_nmrc_diag_relative_selected_score[21] = {0};
uint64_t FatTreeSwitch::_nmrc_diag_relative_actual_egress[33] = {0};
std::set<uint64_t> FatTreeSwitch::_nmrc_diag_observed_flow_evs;
std::set<uint64_t> FatTreeSwitch::_nmrc_diag_observed_flow_paths;
bool FatTreeSwitch::_netaware_enabled = false;
uint32_t FatTreeSwitch::_netaware_path_count = 1;
uint32_t FatTreeSwitch::_netaware_feedback_pkts = 1;
simtime_picosec FatTreeSwitch::_netaware_feedback_min_interval = timeFromUs(5.0);
simtime_picosec FatTreeSwitch::_netaware_feedback_max_interval = timeFromUs(5.0);
double FatTreeSwitch::_netaware_queue_threshold_fraction = 0.8;
double FatTreeSwitch::_netaware_degraded_queue_fraction = 0.30;
double FatTreeSwitch::_netaware_bad_queue_fraction = 0.60;
double FatTreeSwitch::_netaware_degraded_utilization_fraction = 0.90;
double FatTreeSwitch::_netaware_util_queue_floor_fraction = 0.10;
double FatTreeSwitch::_netaware_slow_link_fraction = 0.90;
simtime_picosec FatTreeSwitch::_netaware_state_update_interval = timeFromUs(1.0);
simtime_picosec FatTreeSwitch::_netaware_remote_update_interval = timeFromUs(5.0);
FatTreeSwitch::NetawareScoreMode FatTreeSwitch::_netaware_score_mode =
    FatTreeSwitch::NETAWARE_SCORE_SGLB_QUANTIZED;
FatTreeSwitch::NetawarePathCoupling FatTreeSwitch::_netaware_path_coupling =
    FatTreeSwitch::NETAWARE_PATH_COUPLING_NOISY_OR;
double FatTreeSwitch::_netaware_score_q_min = 0.20;
double FatTreeSwitch::_netaware_score_q_max = 0.80;
double FatTreeSwitch::_netaware_score_util_low = 0.90;
double FatTreeSwitch::_netaware_score_util_high = 1.00;
double FatTreeSwitch::_netaware_score_weight_local_q = 0.50;
double FatTreeSwitch::_netaware_score_weight_remote_q = 0.50;
double FatTreeSwitch::_netaware_score_weight_local_util = 0.00;
double FatTreeSwitch::_netaware_score_weight_remote_util = 0.00;
double FatTreeSwitch::_netaware_score_degraded_threshold = 0.10;
double FatTreeSwitch::_netaware_score_bad_threshold = 0.40;
double FatTreeSwitch::_netaware_score_avoid_threshold = 0.60;
bool FatTreeSwitch::_stor_enabled = false;
uint32_t FatTreeSwitch::_stor_path_count = 1;
uint32_t FatTreeSwitch::_stor_feedback_pkts = 1;
simtime_picosec FatTreeSwitch::_stor_feedback_min_interval = timeFromUs(5.0);
simtime_picosec FatTreeSwitch::_stor_feedback_max_interval = timeFromUs(5.0);
simtime_picosec FatTreeSwitch::_stor_trim_feedback_min_interval = timeFromUs(5.0);
bool FatTreeSwitch::_stor_feedback_on_trim = true;
bool FatTreeSwitch::_stor_binary_trim_bad = true;
uint8_t FatTreeSwitch::_stor_clean_gain = 4;
uint8_t FatTreeSwitch::_stor_ecn_acc_add = 24;
uint8_t FatTreeSwitch::_stor_trim_acc_add = 48;
uint8_t FatTreeSwitch::_stor_ecn_base_penalty = 16;
uint8_t FatTreeSwitch::_stor_trim_base_penalty = 48;
uint8_t FatTreeSwitch::_stor_ecn_decay_shift = 3;
uint8_t FatTreeSwitch::_stor_trim_decay_shift = 2;
uint8_t FatTreeSwitch::_stor_ecn_penalty_shift = 3;
uint8_t FatTreeSwitch::_stor_trim_penalty_shift = 2;
uint8_t FatTreeSwitch::_stor_good_threshold = 240;
uint8_t FatTreeSwitch::_stor_degraded_threshold = 160;
uint8_t FatTreeSwitch::_stor_bad_threshold = 80;
uint8_t FatTreeSwitch::_stor_simple_max_score = 15;
uint8_t FatTreeSwitch::_stor_simple_clean_gain = 1;
uint8_t FatTreeSwitch::_stor_simple_congestion_penalty = 4;
FatTreeSwitch::StorScoreProfile FatTreeSwitch::_stor_score_profile =
    FatTreeSwitch::STOR_SCORE_PROFILE_BALANCED;
FatTreeSwitch::StorAgingProfile FatTreeSwitch::_stor_aging_profile =
    FatTreeSwitch::STOR_AGING_PACKET;
simtime_picosec FatTreeSwitch::_stor_time_ecn_tau = timeFromUs(20.0);
simtime_picosec FatTreeSwitch::_stor_time_trim_tau = timeFromUs(50.0);
simtime_picosec FatTreeSwitch::_stor_time_score_tau = timeFromUs(50.0);
simtime_picosec FatTreeSwitch::_stor_hybrid_bad_hold = timeFromUs(10.0);
simtime_picosec FatTreeSwitch::_stor_hybrid_avoid_hold = timeFromUs(20.0);
uint32_t FatTreeSwitch::_stor_hybrid_probe_interval_pkts = 64;
uint32_t FatTreeSwitch::_stor_hybrid_probe_clean_promote = 2;
bool FatTreeSwitch::_pathid_only_hash = false;
FatTreeSwitch::SglbEcnMode FatTreeSwitch::_sglb_ecn_mode =
    FatTreeSwitch::SGLB_ECN_OFF;
uint64_t FatTreeSwitch::_sglb_ecn_total = 0;
uint64_t FatTreeSwitch::_sglb_ecn_stale = 0;
uint64_t FatTreeSwitch::_sglb_ecn_neutralized = 0;
uint64_t FatTreeSwitch::_sglb_ecn_cleared = 0;
uint64_t FatTreeSwitch::_sglb_ecn_missing_metadata = 0;

void FatTreeSwitch::set_stor_score_profile(StorScoreProfile profile) {
    _stor_score_profile = profile;

    if (profile == STOR_SCORE_PROFILE_CUSTOM)
        return;

    if (profile == STOR_SCORE_PROFILE_BINARY) {
        _stor_good_threshold = 1;
        _stor_degraded_threshold = 1;
        _stor_bad_threshold = 1;
        return;
    }

    if (profile == STOR_SCORE_PROFILE_SIMPLE) {
        _stor_simple_max_score = 15;
        _stor_simple_clean_gain = 1;
        _stor_simple_congestion_penalty = 4;
        _stor_good_threshold = 12;
        _stor_degraded_threshold = 7;
        _stor_bad_threshold = 3;
        return;
    }

    _stor_clean_gain = 4;
    _stor_ecn_decay_shift = 3;
    _stor_trim_decay_shift = 2;
    _stor_ecn_penalty_shift = 3;
    _stor_trim_penalty_shift = 2;

    if (profile == STOR_SCORE_PROFILE_ORIGINAL) {
        _stor_ecn_acc_add = 16;
        _stor_trim_acc_add = 32;
        _stor_ecn_base_penalty = 8;
        _stor_trim_base_penalty = 32;
        _stor_good_threshold = 200;
        _stor_degraded_threshold = 120;
        _stor_bad_threshold = 50;
        return;
    }

    _stor_ecn_acc_add = 24;
    _stor_trim_acc_add = 48;
    _stor_ecn_base_penalty = 16;
    _stor_trim_base_penalty = 48;
    _stor_good_threshold = 240;
    _stor_degraded_threshold = 160;
    _stor_bad_threshold = 80;
}

const char* FatTreeSwitch::stor_score_profile_name() {
    switch (_stor_score_profile) {
    case STOR_SCORE_PROFILE_ORIGINAL:
        return "original";
    case STOR_SCORE_PROFILE_BALANCED:
        return "balanced";
    case STOR_SCORE_PROFILE_SIMPLE:
        return "simple";
    case STOR_SCORE_PROFILE_BINARY:
        return "binary";
    default:
        return "custom";
    }
}

void FatTreeSwitch::set_stor_aging_profile(StorAgingProfile profile) {
    _stor_aging_profile = profile;
}

const char* FatTreeSwitch::stor_aging_profile_name() {
    switch (_stor_aging_profile) {
    case STOR_AGING_PACKET:
        return "packet";
    case STOR_AGING_TIME_EWMA:
        return "time_ewma";
    case STOR_AGING_HYBRID:
        return "hybrid";
    }
    return "packet";
}

uint8_t FatTreeSwitch::stor_level_from_score(uint8_t score) {
    if (_stor_score_profile == STOR_SCORE_PROFILE_BINARY)
        return score ? STOR_LEVEL_GOOD : STOR_LEVEL_AVOID;
    if (score >= _stor_good_threshold)
        return STOR_LEVEL_GOOD;
    if (score >= _stor_degraded_threshold)
        return STOR_LEVEL_DEGRADED;
    if (score >= _stor_bad_threshold)
        return STOR_LEVEL_BAD;
    return STOR_LEVEL_AVOID;
}

static uint8_t stor_decay(uint8_t value, uint8_t shift) {
    return value - (value >> shift);
}

static uint8_t stor_add_sat(uint8_t value, uint32_t delta) {
    uint32_t sum = (uint32_t)value + delta;
    return sum > 255 ? 255 : (uint8_t)sum;
}

static uint8_t stor_sub_sat(uint8_t value, uint32_t delta) {
    return value < delta ? 0 : (uint8_t)(value - delta);
}

void FatTreeSwitch::stor_apply_signal(StorEvState& state, StorSignal signal) {
    if (_stor_score_profile == STOR_SCORE_PROFILE_BINARY) {
        state.ecn_acc = 0;
        state.trim_acc = 0;
        if (signal == STOR_SIGNAL_ECN ||
            (signal == STOR_SIGNAL_TRIM && _stor_binary_trim_bad))
            state.score = 0;
        return;
    }
    if (_stor_score_profile == STOR_SCORE_PROFILE_SIMPLE) {
        if (state.score > _stor_simple_max_score)
            state.score = _stor_simple_max_score;
        state.ecn_acc = 0;
        state.trim_acc = 0;
        if (signal == STOR_SIGNAL_CLEAN) {
            uint32_t recovered = state.score + _stor_simple_clean_gain;
            state.score = recovered > _stor_simple_max_score ?
                _stor_simple_max_score : (uint8_t)recovered;
        } else {
            state.score = stor_sub_sat(
                state.score, _stor_simple_congestion_penalty);
        }
        return;
    }

    state.ecn_acc = stor_decay(state.ecn_acc, _stor_ecn_decay_shift);
    state.trim_acc = stor_decay(state.trim_acc, _stor_trim_decay_shift);

    if (signal == STOR_SIGNAL_CLEAN) {
        state.score = stor_add_sat(state.score, _stor_clean_gain);
        return;
    }

    if (signal == STOR_SIGNAL_ECN) {
        state.ecn_acc = stor_add_sat(state.ecn_acc, _stor_ecn_acc_add);
        uint32_t penalty = _stor_ecn_base_penalty +
                           (state.ecn_acc >> _stor_ecn_penalty_shift);
        state.score = stor_sub_sat(state.score, penalty);
        return;
    }

    state.trim_acc = stor_add_sat(state.trim_acc, _stor_trim_acc_add);
    uint32_t penalty = _stor_trim_base_penalty +
                       (state.trim_acc >> _stor_trim_penalty_shift);
    state.score = stor_sub_sat(state.score, penalty);
}

static uint8_t stor_time_decay_value(uint8_t value, simtime_picosec dt,
                                     simtime_picosec tau) {
    if (!value || dt == 0 || tau == 0)
        return value;
    uint64_t steps = dt / tau;
    uint32_t out = value;
    for (uint64_t i = 0; i < steps && out > 0; i++)
        out -= (out + 1) / 2;
    return (uint8_t)out;
}

static uint8_t stor_time_recover_score(uint8_t score, simtime_picosec dt,
                                       simtime_picosec tau,
                                       uint8_t max_score) {
    if (dt == 0 || tau == 0 || score >= max_score)
        return score;
    uint64_t steps = dt / tau;
    uint32_t out = score;
    for (uint64_t i = 0; i < steps && out < max_score; i++)
        out += (max_score - out + 1) / 2;
    return out > max_score ? max_score : (uint8_t)out;
}

void FatTreeSwitch::stor_note_level_transition(StorEvState& ev,
                                               uint8_t old_level) {
    uint8_t new_level = stor_level_from_score(ev.score);
    if (old_level != STOR_LEVEL_AVOID && new_level == STOR_LEVEL_AVOID)
        ev.avoid_entries++;
    if (old_level == STOR_LEVEL_AVOID && new_level != STOR_LEVEL_AVOID)
        ev.avoid_exits++;
    ev.last_level = new_level;
}

void FatTreeSwitch::stor_apply_time_aging(StorState& state,
                                          simtime_picosec now) {
    for (uint32_t i = 0; i < state.evs.size(); i++) {
        StorEvState& ev = state.evs[i];
        simtime_picosec last = ev.last_update ? ev.last_update : state.last_feedback;
        simtime_picosec dt = now > last ? now - last : 0;
        if (dt == 0)
            continue;

        uint8_t old_level = stor_level_from_score(ev.score);
        if (_stor_aging_profile == STOR_AGING_TIME_EWMA) {
            ev.ecn_acc = stor_time_decay_value(ev.ecn_acc, dt, _stor_time_ecn_tau);
            ev.trim_acc = stor_time_decay_value(ev.trim_acc, dt, _stor_time_trim_tau);
            uint8_t max_score = _stor_score_profile == STOR_SCORE_PROFILE_SIMPLE ?
                _stor_simple_max_score : 255;
            ev.score = stor_time_recover_score(
                ev.score, dt, _stor_time_score_tau, max_score);
        } else if (_stor_aging_profile == STOR_AGING_HYBRID) {
            simtime_picosec hold = old_level == STOR_LEVEL_AVOID ?
                _stor_hybrid_avoid_hold : _stor_hybrid_bad_hold;
            uint32_t probe_interval = _stor_hybrid_probe_interval_pkts ?
                _stor_hybrid_probe_interval_pkts : 1;
            bool probe_due = probe_interval <= 1 ||
                state.signals_seen - ev.last_probe_packet >= probe_interval;
            if ((old_level == STOR_LEVEL_AVOID || old_level == STOR_LEVEL_BAD) &&
                ev.has_bad_time && now >= ev.last_bad_time &&
                now - ev.last_bad_time >= hold && probe_due) {
                if (old_level == STOR_LEVEL_AVOID && ev.score < _stor_bad_threshold)
                    ev.score = _stor_bad_threshold;
                else if (old_level == STOR_LEVEL_BAD &&
                         ev.score < _stor_degraded_threshold)
                    ev.score = _stor_degraded_threshold;
                ev.last_probe_time = now;
                ev.last_probe_packet = state.signals_seen;
            }
        }

        if (ev.score < ev.min_score_seen)
            ev.min_score_seen = ev.score;
        ev.last_update = now;
        stor_note_level_transition(ev, old_level);
    }
}

int8_t (*FatTreeSwitch::fn)(FibEntry*,FibEntry*)= &FatTreeSwitch::compare_queuesize;

uint32_t FatTreeSwitch::sglb_queue_kbytes(BaseQueue* q) {
    if (!q)
        return 0;

    uint64_t q_kbytes = q->queuesize() / 1000;
    if (q_kbytes > 65535)
        q_kbytes = 65535;

    return (uint32_t)q_kbytes;
}

double FatTreeSwitch::sglb_queue_fraction(BaseQueue* q) {
    if (!q)
        return 0.0;

    mem_b maxsize = q->maxsize();
    if (maxsize <= 0)
        return 0.0;

    double fraction = (double)q->queuesize() / (double)maxsize;
    if (fraction < 0.0)
        fraction = 0.0;
    if (fraction > 1.0)
        fraction = 1.0;
    return fraction;
}

uint32_t FatTreeSwitch::sglb_utilization_percent(BaseQueue* q) {
    if (!q)
        return 0;

    uint32_t utilization = q->average_utilization();
    if (utilization > 100)
        utilization = 100;

    LosslessOutputQueue* lq = dynamic_cast<LosslessOutputQueue*>(q);
    if (lq && lq->is_paused())
        utilization = 100;

    return utilization;
}

const char* FatTreeSwitch::netaware_score_mode_name() {
    switch (_netaware_score_mode) {
    case NETAWARE_SCORE_GATED:
        return "gated";
    case NETAWARE_SCORE_WORST_HOP:
        return "worst_hop";
    case NETAWARE_SCORE_SGLB_QUANTIZED:
    default:
        return "sglb_quantized";
    }
}

const char* FatTreeSwitch::netaware_path_coupling_name() {
    switch (_netaware_path_coupling) {
    case NETAWARE_PATH_COUPLING_BOTTLENECK:
        return "bottleneck";
    case NETAWARE_PATH_COUPLING_NOISY_OR:
        return "noisy_or";
    case NETAWARE_PATH_COUPLING_ADDITIVE:
    default:
        return "additive";
    }
}

uint32_t FatTreeSwitch::netaware_default_feedback_pkts(uint32_t path_count) {
    (void)path_count;
    return 1;
}

double FatTreeSwitch::netaware_default_feedback_min_us(uint32_t path_count) {
    (void)path_count;
    return 5.0;
}

double FatTreeSwitch::netaware_default_feedback_max_us(uint32_t path_count) {
    (void)path_count;
    return 5.0;
}

uint32_t FatTreeSwitch::avail_default_feedback_pkts(uint32_t path_count) {
    (void)path_count;
    return 1;
}

uint32_t FatTreeSwitch::grade_default_feedback_pkts(uint32_t path_count) {
    (void)path_count;
    return 1;
}

double FatTreeSwitch::netaware_pressure_from_range(double value, double low,
                                               double high) {
    if (value <= low)
        return 0.0;
    if (high <= low)
        return value > low ? 1.0 : 0.0;
    if (value >= high)
        return 1.0;
    return (value - low) / (high - low);
}

double FatTreeSwitch::netaware_composite_score(double local_q_pressure,
                                           double remote_q_pressure,
                                           double local_util_pressure,
                                           double remote_util_pressure) {
    return _netaware_score_weight_local_q * local_q_pressure +
           _netaware_score_weight_remote_q * remote_q_pressure +
           _netaware_score_weight_local_util * local_util_pressure +
           _netaware_score_weight_remote_util * remote_util_pressure;
}

double FatTreeSwitch::netaware_couple_hop_scores(double local_q_pressure,
                                             double remote_q_pressure,
                                             double local_util_pressure,
                                             double remote_util_pressure) {
    if (_netaware_path_coupling == NETAWARE_PATH_COUPLING_ADDITIVE) {
        return netaware_composite_score(local_q_pressure, remote_q_pressure,
                                    local_util_pressure, remote_util_pressure);
    }

    double local_weight =
        _netaware_score_weight_local_q + _netaware_score_weight_local_util;
    double remote_weight =
        _netaware_score_weight_remote_q + _netaware_score_weight_remote_util;
    double local_score = local_weight > 0.0
        ? (_netaware_score_weight_local_q * local_q_pressure +
           _netaware_score_weight_local_util * local_util_pressure) / local_weight
        : 0.0;
    double remote_score = remote_weight > 0.0
        ? (_netaware_score_weight_remote_q * remote_q_pressure +
           _netaware_score_weight_remote_util * remote_util_pressure) / remote_weight
        : 0.0;

    if (_netaware_path_coupling == NETAWARE_PATH_COUPLING_BOTTLENECK)
        return local_score > remote_score ? local_score : remote_score;
    return 1.0 - (1.0 - local_score) * (1.0 - remote_score);
}

uint8_t FatTreeSwitch::netaware_level_from_score(double score) {
    if (score < _netaware_score_degraded_threshold)
        return STOR_LEVEL_GOOD;
    if (score < _netaware_score_bad_threshold)
        return STOR_LEVEL_DEGRADED;
    if (score < _netaware_score_avoid_threshold)
        return STOR_LEVEL_BAD;
    return STOR_LEVEL_AVOID;
}

uint8_t FatTreeSwitch::netaware_gated_level_from_inputs(bool paused,
                                                    double rate_ratio,
                                                    double queue_fraction,
                                                    double util_fraction,
                                                    bool grade_queues) {
    if (paused)
        return STOR_LEVEL_AVOID;

    bool slow_port = rate_ratio < _netaware_slow_link_fraction;
    if (slow_port)
        return STOR_LEVEL_BAD;

    if (!grade_queues)
        return STOR_LEVEL_GOOD;

    double avoid = _netaware_queue_threshold_fraction;
    if (avoid < 0.0)
        avoid = 0.0;
    if (avoid > 1.0)
        avoid = 1.0;

    if (queue_fraction >= avoid)
        return STOR_LEVEL_AVOID;
    if (queue_fraction >= _netaware_bad_queue_fraction)
        return STOR_LEVEL_BAD;
    if (queue_fraction >= _netaware_degraded_queue_fraction)
        return STOR_LEVEL_DEGRADED;
    if (queue_fraction >= _netaware_util_queue_floor_fraction &&
        util_fraction >= _netaware_degraded_utilization_fraction)
        return STOR_LEVEL_DEGRADED;

    return STOR_LEVEL_GOOD;
}

void FatTreeSwitch::reset_sglb_route_diag() {
    _sglb_diag_route_calls = 0;
    _sglb_diag_available_choices = 0;
    _sglb_diag_candidate_choices = 0;
    _sglb_diag_best_quality_choices = 0;
    _sglb_diag_distinct_qualities = 0;
    _sglb_diag_all_same_quality_calls = 0;
    _sglb_diag_all_zero_quality_calls = 0;
    _sglb_diag_selected_nonbest_quality = 0;
    _sglb_diag_remote_snapshot_used = 0;
    _sglb_diag_remote_snapshot_missing = 0;
    for (uint32_t level = 0; level < 4; level++) {
        _sglb_diag_observed_levels[level] = 0;
        _sglb_diag_selected_levels[level] = 0;
    }
    _sglb_diag_score_spread_sum = 0.0;
    _sglb_ecn_total = 0;
    _sglb_ecn_stale = 0;
    _sglb_ecn_neutralized = 0;
    _sglb_ecn_cleared = 0;
    _sglb_ecn_missing_metadata = 0;
}

void FatTreeSwitch::reset_nmrc_hybrid_diag() {
    _nmrc_diag_route_checks = 0;
    _nmrc_diag_reroutes = 0;
    _nmrc_diag_threshold_blocked = 0;
    _nmrc_diag_fastcnp_generated = 0;
    _nmrc_diag_fastcnp_route_missing = 0;
    _nmrc_diag_graded_cooldown_requested = 0;
    _nmrc_diag_graded_cooldown_suppressed = 0;
    _nmrc_diag_binary_original_safe = 0;
    _nmrc_diag_binary_original_congested = 0;
    _nmrc_diag_binary_no_safe = 0;
    _nmrc_diag_binary_route_missing = 0;
    _nmrc_diag_binary_paired_actions = 0;
    _nmrc_diag_binary_original_score_sum = 0.0;
    _nmrc_diag_binary_original_score_max = 0.0;
    _nmrc_diag_binary_selected_score_sum = 0.0;
    _nmrc_diag_binary_selected_score_max = 0.0;
    _nmrc_diag_relative_checks = 0;
    _nmrc_diag_relative_original_unknown = 0;
    _nmrc_diag_relative_original_unavailable = 0;
    _nmrc_diag_relative_original_below_absolute = 0;
    _nmrc_diag_relative_no_safe_candidate = 0;
    _nmrc_diag_relative_no_delta_candidate = 0;
    _nmrc_diag_relative_reverse_path_blocked = 0;
    _nmrc_diag_relative_paired_actions = 0;
    _nmrc_diag_relative_reroutes = 0;
    _nmrc_diag_two_stage_reroute_only = 0;
    _nmrc_diag_two_stage_cooldown_requested = 0;
    _nmrc_diag_relative_selected_gap_violations = 0;
    _nmrc_diag_relative_decision_ce_set = 0;
    _nmrc_diag_relative_decision_ce_cleared = 0;
    _nmrc_diag_relative_reroute_key_count = 0;
    _nmrc_diag_relative_reroute_key_sum = 0;
    _nmrc_diag_relative_reroute_key_xor = 0;
    _nmrc_diag_relative_generated_key_count = 0;
    _nmrc_diag_relative_generated_key_sum = 0;
    _nmrc_diag_relative_generated_key_xor = 0;
    _nmrc_diag_relative_original_score_count = 0;
    _nmrc_diag_relative_original_score_sum = 0.0;
    _nmrc_diag_relative_original_score_max = 0.0;
    _nmrc_diag_relative_selected_score_count = 0;
    _nmrc_diag_relative_selected_score_sum = 0.0;
    _nmrc_diag_relative_selected_score_max = 0.0;
    _nmrc_diag_observed_flow_evs.clear();
    _nmrc_diag_observed_flow_paths.clear();
    for (uint32_t count = 0; count <= 32; count++) {
        _nmrc_diag_better_count[count] = 0;
        _nmrc_diag_binary_actual_egress[count] = 0;
        _nmrc_diag_relative_candidate_count[count] = 0;
        _nmrc_diag_relative_actual_egress[count] = 0;
    }
    for (uint32_t bin = 0; bin <= 20; bin++) {
        _nmrc_diag_relative_best_gap[bin] = 0;
        _nmrc_diag_relative_selected_gap[bin] = 0;
        _nmrc_diag_relative_original_score[bin] = 0;
        _nmrc_diag_relative_selected_score[bin] = 0;
    }
    for (uint32_t original = 0; original < 4; original++) {
        for (uint32_t selected = 0; selected < 4; selected++)
            _nmrc_diag_level_transitions[original][selected] = 0;
    }
}

double FatTreeSwitch::sglb_port_score(BaseQueue* q, double queue_weight, double util_weight) {
    if (!q)
        return 0.0;

    double queue = _sglb_normalize_scores ? sglb_queue_fraction(q) : (double)sglb_queue_kbytes(q);
    double utilization = _sglb_normalize_scores ?
        (double)sglb_utilization_percent(q) / 100.0 :
        (double)sglb_utilization_percent(q);

    return queue_weight * queue + util_weight * utilization;
}

double FatTreeSwitch::sglb_nmrc_queue_pressure(double queue_fraction) {
    return netaware_pressure_from_range(queue_fraction, _sglb_nmrc_q_min,
                                    _sglb_nmrc_q_max);
}

bool FatTreeSwitch::sglb_gcn_export_changed(
    const SglbPathState& advertised,
    const SglbPathState& observed) {
    return advertised.valid != observed.valid ||
        advertised.link_available != observed.link_available ||
        advertised.queue_fraction != observed.queue_fraction ||
        advertised.avg_busy != observed.avg_busy ||
        advertised.quality != observed.quality;
}

bool FatTreeSwitch::sglb_gcn_observe_export(
    SglbGcnProducerState& producer,
    const SglbPathState& observed,
    simtime_picosec now,
    simtime_picosec sample_interval) {
    if (producer.valid && sample_interval > 0 &&
        now >= producer.last_sample &&
        now - producer.last_sample < sample_interval)
        return false;

    producer.current = observed;
    producer.last_sample = now;
    producer.valid = true;
    producer.dirty = sglb_gcn_export_changed(
        producer.advertised, producer.current);
    return true;
}

void FatTreeSwitch::sglb_gcn_mark_advertised(
    SglbGcnProducerState& producer,
    simtime_picosec now) {
    ++producer.version;
    producer.current.version = producer.version;
    producer.advertised = producer.current;
    producer.last_sent = now;
    producer.has_sent = true;
    producer.dirty = false;
}

double FatTreeSwitch::sglb_nmrc_noisy_or(double local, double remote) {
    return 1.0 - (1.0 - local) * (1.0 - remote);
}

double FatTreeSwitch::sglb_nmrc_queue_signal(double queue_fraction) {
    if (sglb_ofat_uses_raw_queue())
        return std::max(0.0, std::min(1.0, queue_fraction));
    return sglb_nmrc_queue_pressure(queue_fraction);
}

double FatTreeSwitch::sglb_nmrc_couple(double local, double remote) {
    if (sglb_ofat_uses_linear_score())
        return 0.5 * local + 0.5 * remote;
    return sglb_nmrc_noisy_or(local, remote);
}

uint8_t FatTreeSwitch::sglb_nmrc_level(double score) {
    if (sglb_ofat_uses_paper_levels()) {
        if (score < 0.05)
            return STOR_LEVEL_GOOD;
        if (score < 0.10)
            return STOR_LEVEL_DEGRADED;
        if (score < 0.20)
            return STOR_LEVEL_BAD;
        return STOR_LEVEL_AVOID;
    }
    if (score < _sglb_nmrc_degraded_threshold)
        return STOR_LEVEL_GOOD;
    if (score < _sglb_nmrc_bad_threshold)
        return STOR_LEVEL_DEGRADED;
    if (score < _sglb_nmrc_avoid_threshold)
        return STOR_LEVEL_BAD;
    return STOR_LEVEL_AVOID;
}

uint8_t FatTreeSwitch::sglb_nmrc_quantized_level(double score,
                                                  uint32_t levels) {
    if (levels == 16) {
        score = std::max(0.0, std::min(1.0, score));
        const uint32_t level = static_cast<uint32_t>(score * 16.0);
        return static_cast<uint8_t>(level >= 16 ? 15 : level);
    }
    if (levels == 8) {
        static const double thresholds[7] = {
            0.125, 0.250, 0.375, 0.500, 0.625, 0.750, 0.875
        };
        for (uint8_t level = 0; level < 7; level++) {
            if (score < thresholds[level])
                return level;
        }
        return 7;
    }
    return sglb_nmrc_level(score);
}

const char* FatTreeSwitch::sglb_score_mode_name() {
    if (_sglb_score_mode == SGLB_SCORE_NMRC_QUANTIZED_TOPK)
        return "nmrc_quantized_topk";
    return "legacy";
}

const char* FatTreeSwitch::sglb_ofat_factor_name() {
    static const char* names[] = {
        "baseline", "topk8", "linear_score", "raw_queue",
        "paper_levels", "remote_mean", "change_triggered",
        "delayed_message", "eager_init", "versioned", "no_aging",
        "source_leaf_only", "background_sglb", "shadow_gcn",
        "raw_queue_topk8", "raw_paper_levels_topk8",
        "raw_linear_paper_levels_topk8", "real_gcn_profiles",
        "real_gcn_raw_linear"
    };
    const uint32_t value = static_cast<uint32_t>(_sglb_ofat_factor);
    return value < sizeof(names) / sizeof(names[0]) ? names[value] : "invalid";
}

void FatTreeSwitch::configure_sglb_scheme_defaults(bool legacy) {
    _sglb_score_mode = SGLB_SCORE_NMRC_QUANTIZED_TOPK;
    _sglb_nmrc_levels = 4;
    _sglb_gcn_update_interval = timeFromUs(15.0);
    _sglb_gcn_aging_interval = timeFromUs(30.0);

    if (legacy) {
        _sglb_ofat_factor = SGLB_OFAT_BASELINE;
        _sglb_min_choices = 3;
        _sglb_candidate_dispatch = SGLB_DISPATCH_RANDOM;
        _sglb_gcn_cadence = SGLB_GCN_INDEPENDENT;
        _sglb_nmrc_degraded_threshold = 0.10;
        _sglb_nmrc_bad_threshold = 0.40;
        _sglb_nmrc_avoid_threshold = 0.60;
        return;
    }

    _sglb_ofat_factor = SGLB_OFAT_REAL_GCN_RAW_LINEAR;
    _sglb_min_choices = 1;
    _sglb_candidate_policy = SGLB_CANDIDATE_WHOLE_GRADE_MIN;
    _sglb_candidate_dispatch = SGLB_DISPATCH_RANDOM;
    _sglb_gcn_cadence = SGLB_GCN_INDEPENDENT;
    _sglb_nmrc_degraded_threshold = 0.05;
    _sglb_nmrc_bad_threshold = 0.10;
    _sglb_nmrc_avoid_threshold = 0.20;
}

bool FatTreeSwitch::sglb_ofat_uses_topk8() {
    return _sglb_ofat_factor == SGLB_OFAT_TOPK8 ||
           _sglb_ofat_factor == SGLB_OFAT_RAW_QUEUE_TOPK8 ||
           _sglb_ofat_factor == SGLB_OFAT_RAW_PAPER_LEVELS_TOPK8 ||
           _sglb_ofat_factor == SGLB_OFAT_RAW_LINEAR_PAPER_LEVELS_TOPK8;
}

bool FatTreeSwitch::sglb_ofat_uses_raw_queue() {
    return _sglb_ofat_factor == SGLB_OFAT_RAW_QUEUE ||
           _sglb_ofat_factor == SGLB_OFAT_RAW_QUEUE_TOPK8 ||
           _sglb_ofat_factor == SGLB_OFAT_RAW_PAPER_LEVELS_TOPK8 ||
           _sglb_ofat_factor == SGLB_OFAT_RAW_LINEAR_PAPER_LEVELS_TOPK8 ||
           _sglb_ofat_factor == SGLB_OFAT_REAL_GCN_RAW_LINEAR;
}

bool FatTreeSwitch::sglb_ofat_uses_linear_score() {
    return _sglb_ofat_factor == SGLB_OFAT_LINEAR_SCORE ||
           _sglb_ofat_factor == SGLB_OFAT_RAW_LINEAR_PAPER_LEVELS_TOPK8 ||
           _sglb_ofat_factor == SGLB_OFAT_REAL_GCN_RAW_LINEAR;
}

bool FatTreeSwitch::sglb_ofat_uses_paper_levels() {
    return _sglb_ofat_factor == SGLB_OFAT_PAPER_LEVELS ||
           _sglb_ofat_factor == SGLB_OFAT_RAW_PAPER_LEVELS_TOPK8 ||
           _sglb_ofat_factor == SGLB_OFAT_RAW_LINEAR_PAPER_LEVELS_TOPK8;
}

bool FatTreeSwitch::sglb_ofat_uses_real_gcn_profiles() {
    return _sglb_ofat_factor == SGLB_OFAT_REAL_GCN_PROFILES ||
           _sglb_ofat_factor == SGLB_OFAT_REAL_GCN_RAW_LINEAR;
}

bool FatTreeSwitch::sglb_uses_leaf_profiles() const {
    return _ft && _strategy == SGLB &&
           sglb_ofat_uses_real_gcn_profiles() &&
           FatTreeTopology::get_tiers() == 2;
}

uint32_t FatTreeSwitch::sglb_profile_destination(uint32_t dst) const {
    return sglb_uses_leaf_profiles() ? _ft->HOST_POD_SWITCH(dst) : dst;
}

uint32_t FatTreeSwitch::sglb_profile_observation_destination(
    uint32_t profile) const {
    return sglb_uses_leaf_profiles() ?
        profile * _ft->radix_down(TOR_TIER) : profile;
}

bool FatTreeSwitch::sglb_accept_real_gcn_profile(
    SglbPathState& state, const SglbGcnRecord& record,
    uint64_t version, simtime_picosec received_at) {
    if (state.valid && version <= state.version)
        return false;
    state.queue_fraction = paper_sglb_clamp01(record.remote_queue);
    state.queue_pressure = sglb_nmrc_queue_pressure(state.queue_fraction);
    state.avg_busy = std::max(0.0, record.remote_busyness);
    state.best_score = state.queue_pressure;
    state.score = state.queue_pressure;
    state.quality = record.port_quality;
    state.version = version;
    state.last_update = received_at;
    state.valid = true;
    state.link_available = record.link_up;
    return true;
}

void FatTreeSwitch::sglb_candidate_queues(uint32_t dst, vector<BaseQueue*>& queues) {
    if (_type == TOR) {
        if (_ft->HOST_POD_SWITCH(dst) == _id) {
            for (uint32_t b = 0; b < _ft->bundlesize(TOR_TIER); b++) {
                BaseQueue* q = _ft->queues_nlp_ns[_id][dst][b];
                if (q)
                    queues.push_back(q);
            }
            return;
        }

        uint32_t agg_min, agg_max;
        if (_ft->get_tiers() == 3) {
            uint32_t podid = _id / _ft->tor_switches_per_pod();
            agg_min = _ft->MIN_POD_AGG_SWITCH(podid);
            agg_max = _ft->MAX_POD_AGG_SWITCH(podid);
        } else {
            agg_min = 0;
            agg_max = _ft->getNAGG() - 1;
        }

        for (uint32_t agg = agg_min; agg <= agg_max; agg++) {
            for (uint32_t b = 0; b < _ft->bundlesize(AGG_TIER); b++) {
                BaseQueue* q = _ft->queues_nlp_nup[_id][agg][b];
                if (q)
                    queues.push_back(q);
            }
        }
        return;
    }

    if (_type == AGG) {
        if (_ft->get_tiers() == 2 || _ft->HOST_POD(dst) == _ft->AGG_SWITCH_POD_ID(_id)) {
            uint32_t target_tor = _ft->HOST_POD_SWITCH(dst);
            for (uint32_t b = 0; b < _ft->bundlesize(AGG_TIER); b++) {
                BaseQueue* q = _ft->queues_nup_nlp[_id][target_tor][b];
                if (q)
                    queues.push_back(q);
            }
            return;
        }

        uint32_t podpos = _id % _ft->agg_switches_per_pod();
        uint32_t uplink_bundles = _ft->radix_up(AGG_TIER) / _ft->bundlesize(CORE_TIER);
        for (uint32_t l = 0; l < uplink_bundles; l++) {
            uint32_t core = l * _ft->agg_switches_per_pod() + podpos;
            for (uint32_t b = 0; b < _ft->bundlesize(CORE_TIER); b++) {
                BaseQueue* q = _ft->queues_nup_nc[_id][core][b];
                if (q)
                    queues.push_back(q);
            }
        }
        return;
    }

    if (_type == CORE) {
        uint32_t target_agg = _ft->MIN_POD_AGG_SWITCH(_ft->HOST_POD(dst)) +
                              (_id % _ft->agg_switches_per_pod());
        for (uint32_t b = 0; b < _ft->bundlesize(CORE_TIER); b++) {
            BaseQueue* q = _ft->queues_nc_nup[_id][target_agg][b];
            if (q)
                queues.push_back(q);
        }
    }
}

FatTreeSwitch::SglbPathState FatTreeSwitch::sglb_compute_export_state(uint32_t dst) {
    SglbPathState state;
    vector<BaseQueue*> queues;
    sglb_candidate_queues(dst, queues);
    if (queues.empty())
        return state;

    double best = std::numeric_limits<double>::max();
    double best_queue_fraction = std::numeric_limits<double>::max();
    double queue_fraction_sum = 0.0;
    double busy = 0.0;
    for (uint32_t i = 0; i < queues.size(); i++) {
        double queue_fraction = sglb_queue_fraction(queues[i]);
        double score = sglb_port_score(queues[i], _sglb_remote_queue_weight, _sglb_remote_util_weight);
        if (score < best)
            best = score;
        if (queue_fraction < best_queue_fraction)
            best_queue_fraction = queue_fraction;
        queue_fraction_sum += queue_fraction;
        busy += _sglb_normalize_scores ? sglb_queue_fraction(queues[i]) : (double)sglb_queue_kbytes(queues[i]);
    }

    double avg_busy = busy / queues.size();
    if (best_queue_fraction == std::numeric_limits<double>::max())
        best_queue_fraction = 0.0;
    if (_sglb_ofat_factor == SGLB_OFAT_REMOTE_MEAN)
        best_queue_fraction = queue_fraction_sum / queues.size();
    state.queue_fraction = best_queue_fraction;
    state.queue_pressure = sglb_nmrc_queue_signal(best_queue_fraction);
    state.best_score = best == std::numeric_limits<double>::max() ? 0.0 : best;
    state.avg_busy = avg_busy;
    if (_sglb_score_mode == SGLB_SCORE_LEGACY) {
        state.score = state.best_score + _sglb_remote_busy_weight * state.avg_busy;
        state.quality = sglb_quality(state.score);
    } else {
        state.best_score = state.queue_pressure;
        state.score = state.queue_pressure;
        state.quality = sglb_nmrc_level(state.score);
    }
    state.candidate_count = queues.size();
    state.link_available = true;
    state.valid = true;
    return state;
}

void FatTreeSwitch::sglb_maybe_refresh_export(uint32_t dst) {
    dst = sglb_profile_destination(dst);
    if (sglb_ofat_uses_real_gcn_profiles()) {
        sglb_observe_real_gcn_export(dst);
        return;
    }
    simtime_picosec now = eventlist().now();
    SglbPathState& cached = _sglb_exported_state[dst];
    if (cached.valid && _sglb_gcn_update_interval > 0 &&
        now >= cached.last_update &&
        now - cached.last_update < _sglb_gcn_update_interval) {
        if (_sglb_ofat_factor != SGLB_OFAT_CHANGE_TRIGGERED)
            sglb_schedule_periodic_gcn();
        return;
    }

    SglbPathState state = sglb_compute_export_state(
        sglb_profile_observation_destination(dst));
    if (_sglb_ofat_factor == SGLB_OFAT_CHANGE_TRIGGERED && cached.valid &&
        state.valid == cached.valid && state.score == cached.score &&
        state.queue_fraction == cached.queue_fraction &&
        state.link_available == cached.link_available) {
        return;
    }
    if (cached.valid)
        _sglb_previous_exported_state[dst] = cached;
    state.version = (_sglb_ofat_factor == SGLB_OFAT_VERSIONED ||
                     sglb_ofat_uses_real_gcn_profiles()) ?
        cached.version + 1 : 0;
    state.last_update = now;
    cached = state;
    if (_sglb_ofat_factor != SGLB_OFAT_CHANGE_TRIGGERED)
        sglb_schedule_periodic_gcn();
}

void FatTreeSwitch::sglb_schedule_periodic_gcn() {
    if (_strategy != SGLB || _sglb_gcn_update_interval == 0 ||
        _sglb_ofat_factor == SGLB_OFAT_CHANGE_TRIGGERED ||
        _sglb_gcn_timer_pending || _sglb_exported_state.empty()) {
        return;
    }
    if (sglb_ofat_uses_real_gcn_profiles() && _type != AGG)
        return;

    if (!_sglb_gcn_timer)
        _sglb_gcn_timer = new SglbGcnTimer(eventlist(), this);
    eventlist().sourceIsPendingRel(*_sglb_gcn_timer, _sglb_gcn_update_interval);
    _sglb_gcn_timer_pending = true;
}

void FatTreeSwitch::sglb_periodic_refresh_exports() {
    if (_strategy != SGLB || _sglb_gcn_update_interval == 0 ||
        _sglb_exported_state.empty()) {
        return;
    }

    simtime_picosec now = eventlist().now();
    vector<uint32_t> dsts;
    dsts.reserve(_sglb_exported_state.size());
    for (unordered_map<uint32_t,SglbPathState>::const_iterator it = _sglb_exported_state.begin();
         it != _sglb_exported_state.end(); ++it) {
        dsts.push_back(it->first);
    }

    for (size_t i = 0; i < dsts.size(); i++) {
        SglbPathState state = sglb_compute_export_state(
            sglb_profile_observation_destination(dsts[i]));
        SglbPathState& cached = _sglb_exported_state[dsts[i]];
        if (cached.valid)
            _sglb_previous_exported_state[dsts[i]] = cached;
        state.version = (_sglb_ofat_factor == SGLB_OFAT_VERSIONED ||
                         sglb_ofat_uses_real_gcn_profiles()) ?
            cached.version + 1 : 0;
        state.last_update = now;
        cached = state;
    }
    sglb_schedule_periodic_gcn();
}

void FatTreeSwitch::sglb_observe_real_gcn_export(uint32_t destination) {
    if (_type != AGG || !_ft)
        return;

    const simtime_picosec now = eventlist().now();
    SglbGcnProducerState& producer = _sglb_gcn_producers[destination];
    SglbPathState observed = sglb_compute_export_state(
        sglb_profile_observation_destination(destination));
    observed.version = producer.version;
    observed.last_update = now;
    if (!sglb_gcn_observe_export(
            producer, observed, now, _sglb_update_interval) ||
        !producer.dirty)
        return;

    if (_sglb_gcn_cadence == SGLB_GCN_SYNCHRONIZED &&
        _sglb_gcn_update_interval > 0) {
        const simtime_picosec phase = now % _sglb_gcn_update_interval;
        const simtime_picosec delay = phase == 0 ?
            _sglb_gcn_update_interval : _sglb_gcn_update_interval - phase;
        sglb_schedule_real_gcn(destination, delay);
        return;
    }

    if (!producer.has_sent || _sglb_gcn_update_interval == 0 ||
        now < producer.last_sent ||
        now - producer.last_sent >= _sglb_gcn_update_interval) {
        sglb_emit_real_gcn(destination);
        return;
    }

    sglb_schedule_real_gcn(
        destination,
        _sglb_gcn_update_interval - (now - producer.last_sent));
}

void FatTreeSwitch::sglb_schedule_real_gcn(
    uint32_t destination, simtime_picosec delay) {
    SglbGcnProducerState& producer = _sglb_gcn_producers[destination];
    if (producer.timer_pending)
        return;
    SglbRealGcnTimer*& timer = _sglb_real_gcn_timers[destination];
    if (!timer)
        timer = new SglbRealGcnTimer(eventlist(), this, destination);
    eventlist().sourceIsPendingRel(*timer, delay);
    producer.timer_pending = true;
}

void FatTreeSwitch::sglb_real_gcn_timer_fired(uint32_t destination) {
    SglbGcnProducerState& producer = _sglb_gcn_producers[destination];
    producer.timer_pending = false;
    if (!producer.dirty)
        return;

    const simtime_picosec now = eventlist().now();
    if (producer.has_sent && _sglb_gcn_update_interval > 0 &&
        now >= producer.last_sent &&
        now - producer.last_sent < _sglb_gcn_update_interval) {
        sglb_schedule_real_gcn(
            destination,
            _sglb_gcn_update_interval - (now - producer.last_sent));
        return;
    }
    sglb_emit_real_gcn(destination);
}

void FatTreeSwitch::sglb_emit_real_gcn(uint32_t destination) {
    if (!sglb_ofat_uses_real_gcn_profiles() || _type != AGG || !_ft ||
        !_paper_sglb_gcn_flow)
        return;

    unordered_map<uint32_t,SglbGcnProducerState>::iterator found =
        _sglb_gcn_producers.find(destination);
    if (found == _sglb_gcn_producers.end() || !found->second.valid ||
        !found->second.dirty)
        return;
    SglbGcnProducerState& producer = found->second;
    const uint64_t next_version = producer.version + 1;

    vector<SglbGcnRecord> records;
    records.push_back(SglbGcnRecord(
        destination, destination, producer.current.link_available,
        producer.current.queue_fraction, 0.0, producer.current.avg_busy,
        producer.current.quality));

    for (uint32_t receiver = 0; receiver < _ft->switches_lp.size(); ++receiver) {
        vector<Route*>& routes = _paper_sglb_gcn_routes[receiver];
        if (routes.empty())
            continue;
        Route* route = routes[
            (next_version + receiver + destination) % routes.size()];
        SglbGcnPacket* packet = SglbGcnPacket::newpkt(
            *_paper_sglb_gcn_flow, *route, _id, records,
            next_version, eventlist().now());
        ++_paper_sglb_diag_gcn_packets;
        _paper_sglb_diag_gcn_bytes += packet->size();
        packet->sendOn();
    }
    sglb_gcn_mark_advertised(producer, eventlist().now());
    producer.advertised.last_update = eventlist().now();
    _sglb_exported_state[destination] = producer.advertised;
    ++_paper_sglb_diag_gcn_updates;
}

void FatTreeSwitch::receive_sglb_real_gcn(SglbGcnPacket& packet) {
    if (!_ft)
        return;
    uint64_t accepted = 0;
    for (size_t i = 0; i < packet.record_count(); ++i) {
        const SglbGcnRecord& record = packet.record(i);
        if (sglb_accept_real_gcn_profile(
                _sglb_received_profiles[paper_sglb_remote_key(
                    packet.sender_switch_id(),
                    record.destination_switch_id)],
                record, packet.version(), eventlist().now()))
            ++accepted;
    }
    if (accepted) {
        ++_paper_sglb_diag_gcn_deliveries;
        _paper_sglb_diag_gcn_profile_updates += accepted;
    } else {
        ++_paper_sglb_diag_gcn_stale;
    }
}

void FatTreeSwitch::initialize_sglb_ofat(FatTreeTopology* topology) {
    if (!topology || _strategy != SGLB ||
        _sglb_ofat_factor != SGLB_OFAT_EAGER_INIT)
        return;
    vector<FatTreeSwitch*> switches;
    for (size_t i = 0; i < topology->switches_lp.size(); ++i) {
        FatTreeSwitch* sw = dynamic_cast<FatTreeSwitch*>(topology->switches_lp[i]);
        if (sw)
            switches.push_back(sw);
    }
    for (size_t i = 0; i < topology->switches_up.size(); ++i) {
        FatTreeSwitch* sw = dynamic_cast<FatTreeSwitch*>(topology->switches_up[i]);
        if (sw)
            switches.push_back(sw);
    }
    for (size_t i = 0; i < switches.size(); ++i) {
        for (uint32_t dst = 0; dst < topology->no_of_nodes(); ++dst)
            switches[i]->sglb_maybe_refresh_export(dst);
    }
}

void FatTreeSwitch::initialize_sglb_real_gcn_profiles(
    FatTreeTopology* topology) {
    if (!topology || _strategy != SGLB ||
        !sglb_ofat_uses_real_gcn_profiles())
        return;
    if (FatTreeTopology::get_tiers() != 2) {
        cerr << "real-GCN SGLB profiles require a two-tier Clos topology"
             << endl;
        abort();
    }

    const simtime_picosec now = topology->_eventlist->now();
    vector<FatTreeSwitch*> switches;
    for (size_t i = 0; i < topology->switches_lp.size(); ++i) {
        FatTreeSwitch* sw = dynamic_cast<FatTreeSwitch*>(
            topology->switches_lp[i]);
        if (sw)
            switches.push_back(sw);
    }
    for (size_t i = 0; i < topology->switches_up.size(); ++i) {
        FatTreeSwitch* sw = dynamic_cast<FatTreeSwitch*>(
            topology->switches_up[i]);
        if (sw)
            switches.push_back(sw);
    }

    // Phase 1: install one export per destination leaf before profiles read it.
    for (size_t i = 0; i < switches.size(); ++i) {
        for (uint32_t dst_leaf = 0;
             dst_leaf < topology->switches_lp.size(); ++dst_leaf) {
            const uint32_t representative =
                dst_leaf * topology->radix_down(TOR_TIER);
            SglbPathState state =
                switches[i]->sglb_compute_export_state(representative);
            state.version = 0;
            state.last_update = now;
            switches[i]->_sglb_exported_state[dst_leaf] = state;
        }
    }

    // Build real spine-to-leaf control paths and preinstall received profiles.
    for (uint32_t spine = 0; spine < topology->getNAGG(); ++spine) {
        FatTreeSwitch* producer = dynamic_cast<FatTreeSwitch*>(
            topology->switches_up[spine]);
        assert(producer);
        if (!producer->_paper_sglb_gcn_flow)
            producer->_paper_sglb_gcn_flow = new PacketFlow(NULL);
        producer->_paper_sglb_gcn_version = 0;

        for (uint32_t dst_leaf = 0;
             dst_leaf < topology->switches_lp.size(); ++dst_leaf) {
            SglbGcnProducerState& state =
                producer->_sglb_gcn_producers[dst_leaf];
            state.current = producer->_sglb_exported_state[dst_leaf];
            state.advertised = state.current;
            state.last_sample = now;
            state.last_sent = 0;
            state.version = 0;
            state.valid = true;
            state.dirty = false;
            state.has_sent = false;
            state.timer_pending = false;
        }

        for (uint32_t receiver = 0;
             receiver < topology->switches_lp.size(); ++receiver) {
            vector<Route*>& routes =
                producer->_paper_sglb_gcn_routes[receiver];
            const vector<BaseQueue*>& queues =
                topology->queues_nup_nlp[spine][receiver];
            const vector<Pipe*>& pipes =
                topology->pipes_nup_nlp[spine][receiver];
            for (size_t bundle = 0;
                 bundle < queues.size() && bundle < pipes.size(); ++bundle) {
                if (!queues[bundle] || !pipes[bundle])
                    continue;
                Route* route = new Route();
                route->push_back(queues[bundle]);
                route->push_back(pipes[bundle]);
                route->push_back(queues[bundle]->getRemoteEndpoint());
                routes.push_back(route);
            }
        }

        for (size_t source = 0; source < topology->switches_lp.size(); ++source) {
            FatTreeSwitch* leaf = dynamic_cast<FatTreeSwitch*>(
                topology->switches_lp[source]);
            assert(leaf);
            for (uint32_t dst_leaf = 0;
                 dst_leaf < topology->switches_lp.size(); ++dst_leaf) {
                SglbPathState initial =
                    producer->_sglb_exported_state[dst_leaf];
                initial.version = 0;
                initial.last_update = now;
                leaf->_sglb_received_profiles[
                    paper_sglb_remote_key(spine, dst_leaf)] = initial;
            }
        }
    }

    // Phase 2: materialize every source-leaf candidate quality profile.
    for (size_t source = 0; source < topology->switches_lp.size(); ++source) {
        FatTreeSwitch* leaf = dynamic_cast<FatTreeSwitch*>(
            topology->switches_lp[source]);
        assert(leaf);
        for (uint32_t dst_leaf = 0;
             dst_leaf < topology->switches_lp.size(); ++dst_leaf) {
            vector<FibEntry*>* routes =
                leaf->_fib->getLeafRoutes(dst_leaf);
            if (!routes)
                continue;
            const uint32_t dst =
                dst_leaf * topology->radix_down(TOR_TIER);
            for (size_t candidate = 0; candidate < routes->size(); ++candidate)
                leaf->sglb_quality_snapshot(routes->at(candidate), dst, 1);
        }
    }
}

uint32_t FatTreeSwitch::sglb_next_hop_id(FibEntry* entry) const {
    if (!entry)
        return UINT32_MAX;

    Route *r = entry->getEgressPort();
    if (!r || r->size() <= 2)
        return UINT32_MAX;

    FatTreeSwitch* next = dynamic_cast<FatTreeSwitch*>(r->at(2));
    if (!next)
        return UINT32_MAX;

    return next->getID();
}

bool FatTreeSwitch::sglb_entry_available(FibEntry* entry) const {
    uint32_t next_id = sglb_next_hop_id(entry);
    if (next_id == UINT32_MAX)
        return true;

    unordered_map<uint32_t,bool>::const_iterator it = _sglb_neighbor_available.find(next_id);
    return it == _sglb_neighbor_available.end() || it->second;
}

const FatTreeSwitch::SglbPathState* FatTreeSwitch::sglb_neighbor_snapshot(FibEntry* entry,
                                                                        uint32_t dst) const {
    if (!sglb_entry_available(entry))
        return NULL;

    Route *r = entry->getEgressPort();
    if (!r || r->size() <= 2)
        return NULL;

    FatTreeSwitch* next = dynamic_cast<FatTreeSwitch*>(r->at(2));
    if (!next)
        return NULL;

    if (sglb_ofat_uses_real_gcn_profiles()) {
        const uint32_t profile = sglb_profile_destination(dst);
        unordered_map<uint64_t,SglbPathState>::const_iterator received =
            _sglb_received_profiles.find(
                paper_sglb_remote_key(next->getID(), profile));
        if (received == _sglb_received_profiles.end())
            return NULL;
        const simtime_picosec now = eventlist().now();
        if (!sglb_snapshot_usable(
                received->second, now, _sglb_gcn_aging_interval))
            return NULL;
        return &received->second;
    }

    const uint32_t profile = sglb_profile_destination(dst);
    unordered_map<uint32_t,SglbPathState>::const_iterator it =
        next->_sglb_exported_state.find(profile);
    if (it == next->_sglb_exported_state.end())
        return NULL;

    simtime_picosec now = eventlist().now();
    const simtime_picosec aging =
        _sglb_ofat_factor == SGLB_OFAT_NO_AGING ? 0 :
        _sglb_gcn_aging_interval;
    if (_sglb_ofat_factor == SGLB_OFAT_DELAYED_MESSAGE &&
        now >= it->second.last_update &&
        now - it->second.last_update < _sglb_ofat_message_delay) {
        unordered_map<uint32_t,SglbPathState>::const_iterator previous =
            next->_sglb_previous_exported_state.find(profile);
        if (previous == next->_sglb_previous_exported_state.end() ||
            !sglb_snapshot_usable(previous->second, now, aging))
            return NULL;
        return &previous->second;
    }
    if (!sglb_snapshot_usable(it->second, now, aging))
        return NULL;

    return &it->second;
}

void FatTreeSwitch::sglb_mark_neighbor_link(uint32_t neighbor_id, bool available) {
    _sglb_neighbor_available[neighbor_id] = available;
}

double FatTreeSwitch::sglb_compute_score(FibEntry* entry, uint32_t dst, uint32_t depth) {
    if (!sglb_entry_available(entry))
        return std::numeric_limits<double>::max();

    Route *r = entry->getEgressPort();
    assert(r && r->size() > 0);

    BaseQueue* q = dynamic_cast<BaseQueue*>(r->at(0));
    if (!q)
        return std::numeric_limits<double>::max();

    double score = sglb_port_score(q, _sglb_queue_weight, _sglb_util_weight);
    double local_score = score;

    if (depth > 0) {
        const SglbPathState* remote = sglb_neighbor_snapshot(entry, dst);
        if (remote) {
            _sglb_diag_remote_snapshot_used++;
            double downstream_scale = _sglb_local_damping ?
                sglb_downstream_scale_for_score(local_score,
                                               _sglb_quality_bucket,
                                               _sglb_normalize_scores) :
                1.0;
            score += _sglb_downstream_weight * downstream_scale * remote->score;
        } else {
            _sglb_diag_remote_snapshot_missing++;
        }
    }
    return score;
}

double FatTreeSwitch::sglb_compute_nmrc_score(FibEntry* entry, uint32_t dst,
                                              uint32_t depth,
                                              bool* local_valid,
                                              bool* downstream_valid,
                                              simtime_picosec* downstream_last_update) {
    if (local_valid)
        *local_valid = false;
    if (downstream_valid)
        *downstream_valid = depth == 0;
    if (downstream_last_update)
        *downstream_last_update = 0;

    if (!sglb_entry_available(entry))
        return std::numeric_limits<double>::max();

    Route *r = entry->getEgressPort();
    assert(r && r->size() > 0);
    BaseQueue* q = dynamic_cast<BaseQueue*>(r->at(0));
    if (!q)
        return std::numeric_limits<double>::max();

    if (local_valid)
        *local_valid = true;

    double local_fraction = sglb_queue_fraction(q);
    double local_pressure = sglb_nmrc_queue_signal(local_fraction);
    double remote_pressure = 0.0;
    if (depth > 0) {
        const SglbPathState* remote = sglb_neighbor_snapshot(entry, dst);
        if (remote) {
            _sglb_diag_remote_snapshot_used++;
            remote_pressure = sglb_ofat_uses_raw_queue() ?
                remote->queue_fraction : remote->queue_pressure;
            if (downstream_valid)
                *downstream_valid = true;
            if (downstream_last_update)
                *downstream_last_update = remote->last_update;
        } else {
            _sglb_diag_remote_snapshot_missing++;
        }
    }
    return sglb_nmrc_couple(local_pressure, remote_pressure);
}

const FatTreeSwitch::SglbQualitySnapshot&
FatTreeSwitch::sglb_quality_snapshot(FibEntry* entry, uint32_t dst, uint32_t depth) {
    simtime_picosec now = eventlist().now();
    SglbQualitySnapshot& cached =
        _sglb_quality_table[sglb_profile_destination(dst)][entry];

    if (cached.valid && _sglb_update_interval > 0 &&
        now >= cached.last_update &&
        now - cached.last_update < _sglb_update_interval) {
        if (depth > 0 && cached.downstream_valid &&
            _sglb_ofat_factor != SGLB_OFAT_NO_AGING &&
            _sglb_gcn_aging_interval > 0 &&
            (now < cached.downstream_last_update ||
             now - cached.downstream_last_update >
                 _sglb_gcn_aging_interval)) {
            cached.downstream_valid = false;
        }
        return cached;
    }

    cached.local_valid = false;
    cached.downstream_valid = false;
    cached.downstream_last_update = 0;
    if (_sglb_score_mode == SGLB_SCORE_LEGACY) {
        cached.score = sglb_compute_score(entry, dst, depth);
        cached.quality = sglb_quality(cached.score);
    } else {
        cached.score = sglb_compute_nmrc_score(
            entry, dst, depth, &cached.local_valid,
            &cached.downstream_valid,
            &cached.downstream_last_update);
        cached.quality = sglb_nmrc_quantized_level(
            cached.score, _sglb_nmrc_levels);
    }
    cached.last_update = now;
    cached.valid = true;
    return cached;
}

uint8_t FatTreeSwitch::sglb_quality(double score) {
    uint32_t levels = _sglb_quality_levels ? _sglb_quality_levels : (_sglb_max_quality + 1);
    return sglb_quality_from_score(score, _sglb_quality_bucket, levels);
}

FatTreeSwitch::NmrcRerouteDecision
FatTreeSwitch::nmrc_select_better_path(
        uint32_t original_index,
        const vector<uint8_t>& levels,
        const vector<bool>& available,
        NmrcReroutePolicy policy,
        uint32_t min_choices,
        uint32_t selection_value) {
    NmrcRerouteDecision decision;
    if (original_index >= levels.size() || levels.size() != available.size())
        return decision;

    decision.original_level = levels[original_index];
    for (uint32_t i = 0; i < levels.size(); i++) {
        if (i != original_index && available[i] &&
            levels[i] < decision.original_level)
            decision.better_count++;
    }

    uint32_t required = policy == NMRC_REROUTE_BETTER_GE3 ? 3 : 1;
    if (decision.better_count < required)
        return decision;

    if (min_choices == 0)
        min_choices = 1;
    vector<uint32_t> candidates;
    for (uint32_t level = STOR_LEVEL_GOOD;
         level < decision.original_level &&
             candidates.size() < min_choices;
         level++) {
        for (uint32_t i = 0; i < levels.size(); i++) {
            if (i != original_index && available[i] &&
                levels[i] == level)
                candidates.push_back(i);
        }
    }
    decision.candidate_count = candidates.size();
    if (candidates.empty())
        return decision;

    decision.selected_index =
        candidates[selection_value % candidates.size()];
    decision.selected_level = levels[decision.selected_index];
    decision.reroute = decision.selected_index != original_index &&
        decision.selected_level < decision.original_level;
    return decision;
}

FatTreeSwitch::NmrcRerouteDecision
FatTreeSwitch::nmrc_select_graded_delta_path(
        uint32_t original_index,
        const vector<uint8_t>& levels,
        const vector<double>& scores,
        const vector<bool>& available,
        NmrcReroutePolicy policy,
        uint32_t min_choices,
        uint32_t selection_value,
        double delta) {
    NmrcRerouteDecision decision;
    if (original_index >= levels.size() ||
        levels.size() != scores.size() ||
        levels.size() != available.size() ||
        !std::isfinite(delta) || delta < 0.0)
        return decision;

    if (delta <= NMRC_RELATIVE_EPSILON)
        return nmrc_select_better_path(
            original_index, levels, available, policy,
            min_choices, selection_value);

    vector<bool> eligible(available.size(), false);
    eligible[original_index] = available[original_index];
    if (std::isfinite(scores[original_index])) {
        for (uint32_t i = 0; i < levels.size(); i++) {
            if (i == original_index || !available[i] ||
                levels[i] >= levels[original_index] ||
                !std::isfinite(scores[i]))
                continue;
            eligible[i] =
                scores[original_index] - scores[i] >=
                delta - NMRC_RELATIVE_EPSILON;
        }
    }
    return nmrc_select_better_path(
        original_index, levels, eligible, policy,
        min_choices, selection_value);
}

bool FatTreeSwitch::nmrc_graded_requests_cooldown(
        double original_score, double selected_score, double threshold) {
    return std::isfinite(original_score) &&
        std::isfinite(selected_score) &&
        std::isfinite(threshold) &&
        threshold > 0.0 &&
        original_score - selected_score >=
            threshold - NMRC_RELATIVE_EPSILON;
}

bool FatTreeSwitch::nmrc_graded_cooldown_requested(
        NmrcGradedCooldownMode mode,
        double original_score, double selected_score, double threshold) {
    if (mode == NMRC_GRADED_COOLDOWN_FULL)
        return true;
    if (mode == NMRC_GRADED_COOLDOWN_NONE)
        return false;
    return nmrc_graded_requests_cooldown(
        original_score, selected_score, threshold);
}

bool FatTreeSwitch::nmrc_binary_safe(double score, bool two_hop_valid) {
    return two_hop_valid && std::isfinite(score) && score < 0.5;
}

FatTreeSwitch::NmrcBinaryClass
FatTreeSwitch::nmrc_binary_classify(double score, bool two_hop_valid) {
    if (!two_hop_valid || !std::isfinite(score))
        return NMRC_BINARY_UNKNOWN;
    return nmrc_binary_safe(score, two_hop_valid) ?
        NMRC_BINARY_SAFE : NMRC_BINARY_CONGESTED;
}

FatTreeSwitch::NmrcRerouteDecision
FatTreeSwitch::nmrc_select_binary_path(
        uint32_t original_index,
        const vector<double>& scores,
        const vector<bool>& two_hop_valid,
        const vector<bool>& available,
        uint32_t selection_value) {
    NmrcRerouteDecision decision;
    if (original_index >= scores.size() ||
        scores.size() != two_hop_valid.size() ||
        scores.size() != available.size()) {
        return decision;
    }

    if (nmrc_binary_classify(scores[original_index],
                             two_hop_valid[original_index]) !=
        NMRC_BINARY_CONGESTED) {
        return decision;
    }

    decision.original_level = 1;
    vector<uint32_t> candidates;
    for (uint32_t i = 0; i < scores.size(); i++) {
        if (i != original_index && available[i] &&
            nmrc_binary_safe(scores[i], two_hop_valid[i])) {
            candidates.push_back(i);
        }
    }

    decision.better_count = candidates.size();
    decision.candidate_count = candidates.size();
    if (candidates.empty())
        return decision;

    decision.selected_index =
        candidates[selection_value % candidates.size()];
    decision.selected_level = 0;
    decision.reroute = true;
    return decision;
}

FatTreeSwitch::NmrcRelativeDecision
FatTreeSwitch::nmrc_select_fixed_threshold_path(
        uint32_t original_index,
        const vector<double>& scores,
        const vector<bool>& two_hop_valid,
        const vector<bool>& available,
        double absolute_threshold,
        double delta,
        uint32_t selection_value) {
    NmrcRelativeDecision decision;
    if (original_index >= scores.size() ||
        scores.size() != two_hop_valid.size() ||
        scores.size() != available.size() ||
        !std::isfinite(absolute_threshold) ||
        absolute_threshold <= 0.0 || absolute_threshold > 1.0 ||
        !std::isfinite(delta) || delta <= 0.0 || delta > 1.0) {
        return decision;
    }
    if (!available[original_index]) {
        decision.reason = NMRC_RELATIVE_ORIGINAL_UNAVAILABLE;
        return decision;
    }
    if (!two_hop_valid[original_index] ||
        !std::isfinite(scores[original_index])) {
        decision.reason = NMRC_RELATIVE_ORIGINAL_UNKNOWN;
        return decision;
    }

    decision.original_score = scores[original_index];
    if (decision.original_score <
            absolute_threshold - NMRC_RELATIVE_EPSILON) {
        decision.reason = NMRC_RELATIVE_ORIGINAL_BELOW_ABSOLUTE;
        return decision;
    }
    vector<uint32_t> candidates;
    uint32_t safe_candidates = 0;
    for (uint32_t i = 0; i < scores.size(); i++) {
        if (i == original_index || !available[i] ||
            !two_hop_valid[i] || !std::isfinite(scores[i])) {
            continue;
        }
        if (scores[i] >= absolute_threshold - NMRC_RELATIVE_EPSILON)
            continue;
        safe_candidates++;
        double gap = scores[original_index] - scores[i];
        decision.best_gap = std::max(decision.best_gap, gap);
        if (gap >= delta - NMRC_RELATIVE_EPSILON)
            candidates.push_back(i);
    }
    decision.candidate_count = candidates.size();
    if (candidates.empty()) {
        decision.reason = safe_candidates == 0
            ? NMRC_RELATIVE_NO_SAFE_CANDIDATE
            : NMRC_RELATIVE_NO_DELTA_CANDIDATE;
        return decision;
    }

    decision.selected_index =
        candidates[selection_value % candidates.size()];
    decision.selected_score = scores[decision.selected_index];
    decision.selected_gap =
        scores[original_index] - decision.selected_score;
    decision.reroute = true;
    decision.reason = NMRC_RELATIVE_SELECTED;
    return decision;
}

FatTreeSwitch::NmrcRelativeDecision
FatTreeSwitch::nmrc_select_delta_path(
        uint32_t original_index,
        const vector<double>& scores,
        const vector<bool>& two_hop_valid,
        const vector<bool>& available,
        double delta,
        uint32_t selection_value) {
    NmrcRelativeDecision decision;
    if (original_index >= scores.size() ||
        scores.size() != two_hop_valid.size() ||
        scores.size() != available.size() ||
        !std::isfinite(delta) || delta <= 0.0 || delta > 1.0)
        return decision;
    if (!available[original_index]) {
        decision.reason = NMRC_RELATIVE_ORIGINAL_UNAVAILABLE;
        return decision;
    }
    if (!two_hop_valid[original_index] ||
        !std::isfinite(scores[original_index])) {
        decision.reason = NMRC_RELATIVE_ORIGINAL_UNKNOWN;
        return decision;
    }

    decision.original_score = scores[original_index];
    vector<uint32_t> candidates;
    for (uint32_t i = 0; i < scores.size(); i++) {
        if (i == original_index || !available[i] || !two_hop_valid[i] ||
            !std::isfinite(scores[i]))
            continue;
        double gap = scores[original_index] - scores[i];
        decision.best_gap = std::max(decision.best_gap, gap);
        if (gap >= delta - NMRC_RELATIVE_EPSILON)
            candidates.push_back(i);
    }
    decision.candidate_count = candidates.size();
    if (candidates.empty()) {
        decision.reason = NMRC_RELATIVE_NO_DELTA_CANDIDATE;
        return decision;
    }
    decision.selected_index = candidates[selection_value % candidates.size()];
    decision.selected_score = scores[decision.selected_index];
    decision.selected_gap = decision.original_score - decision.selected_score;
    decision.reroute = true;
    decision.reason = NMRC_RELATIVE_SELECTED;
    return decision;
}

FatTreeSwitch::NmrcRelativeDecision
FatTreeSwitch::nmrc_select_piecewise_delta_path(
        uint32_t original_index,
        const vector<double>& scores,
        const vector<bool>& two_hop_valid,
        const vector<bool>& available,
        double breakpoint,
        double delta_below,
        double delta_above,
        uint32_t selection_value) {
    NmrcRelativeDecision decision;
    if (original_index >= scores.size() ||
        scores.size() != two_hop_valid.size() ||
        scores.size() != available.size() ||
        !std::isfinite(breakpoint) || breakpoint <= 0.0 || breakpoint > 1.0 ||
        !std::isfinite(delta_below) || delta_below <= 0.0 || delta_below > 1.0 ||
        !std::isfinite(delta_above) || delta_above <= 0.0 || delta_above > 1.0)
        return decision;
    if (!available[original_index]) {
        decision.reason = NMRC_RELATIVE_ORIGINAL_UNAVAILABLE;
        return decision;
    }
    if (!two_hop_valid[original_index] ||
        !std::isfinite(scores[original_index])) {
        decision.reason = NMRC_RELATIVE_ORIGINAL_UNKNOWN;
        return decision;
    }
    decision.original_score = scores[original_index];
    const double delta = decision.original_score < breakpoint
        ? delta_below : delta_above;
    vector<uint32_t> candidates;
    for (uint32_t i = 0; i < scores.size(); i++) {
        if (i == original_index || !available[i] ||
            !two_hop_valid[i] || !std::isfinite(scores[i]))
            continue;
        const double gap = decision.original_score - scores[i];
        decision.best_gap = std::max(decision.best_gap, gap);
        if (gap >= delta - NMRC_RELATIVE_EPSILON)
            candidates.push_back(i);
    }
    decision.candidate_count = candidates.size();
    if (candidates.empty()) {
        decision.reason = NMRC_RELATIVE_NO_DELTA_CANDIDATE;
        return decision;
    }
    decision.selected_index = candidates[selection_value % candidates.size()];
    decision.selected_score = scores[decision.selected_index];
    decision.selected_gap = decision.original_score - decision.selected_score;
    decision.reroute = true;
    decision.reason = NMRC_RELATIVE_SELECTED;
    return decision;
}

FatTreeSwitch::NmrcRelativeDecision
FatTreeSwitch::nmrc_select_two_stage_delta_path(
        uint32_t original_index,
        const vector<double>& scores,
        const vector<bool>& two_hop_valid,
        const vector<bool>& available,
        double route_delta,
        double cooldown_delta,
        uint32_t selection_value) {
    NmrcRelativeDecision decision;
    if (original_index >= scores.size() ||
        scores.size() != two_hop_valid.size() ||
        scores.size() != available.size() ||
        !std::isfinite(route_delta) || route_delta <= 0.0 ||
        !std::isfinite(cooldown_delta) || cooldown_delta > 1.0 ||
        route_delta > cooldown_delta)
        return decision;
    if (!available[original_index]) {
        decision.reason = NMRC_RELATIVE_ORIGINAL_UNAVAILABLE;
        return decision;
    }
    if (!two_hop_valid[original_index] ||
        !std::isfinite(scores[original_index])) {
        decision.reason = NMRC_RELATIVE_ORIGINAL_UNKNOWN;
        return decision;
    }
    decision.original_score = scores[original_index];
    vector<uint32_t> candidates;
    for (uint32_t i = 0; i < scores.size(); i++) {
        if (i == original_index || !available[i] ||
            !two_hop_valid[i] || !std::isfinite(scores[i]))
            continue;
        const double gap = decision.original_score - scores[i];
        decision.best_gap = std::max(decision.best_gap, gap);
        if (gap >= route_delta - NMRC_RELATIVE_EPSILON)
            candidates.push_back(i);
    }
    decision.candidate_count = candidates.size();
    if (candidates.empty()) {
        decision.reason = NMRC_RELATIVE_NO_DELTA_CANDIDATE;
        return decision;
    }
    decision.selected_index = candidates[selection_value % candidates.size()];
    decision.selected_score = scores[decision.selected_index];
    decision.selected_gap = decision.original_score - decision.selected_score;
    decision.request_cooldown =
        decision.selected_gap >= cooldown_delta - NMRC_RELATIVE_EPSILON;
    decision.reroute = true;
    decision.reason = NMRC_RELATIVE_SELECTED;
    return decision;
}

FatTreeSwitch::NmrcRelativeDecision
FatTreeSwitch::nmrc_select_absolute_reroute_path(
        uint32_t original_index,
        const vector<double>& scores,
        const vector<bool>& two_hop_valid,
        const vector<bool>& available,
        double absolute_threshold,
        double cooldown_delta,
        uint32_t selection_value) {
    NmrcRelativeDecision decision;
    if (original_index >= scores.size() ||
        scores.size() != two_hop_valid.size() ||
        scores.size() != available.size() ||
        !std::isfinite(absolute_threshold) || absolute_threshold <= 0.0 ||
        absolute_threshold > 1.0 || !std::isfinite(cooldown_delta) ||
        cooldown_delta <= 0.0 || cooldown_delta > 1.0)
        return decision;
    if (!available[original_index]) {
        decision.reason = NMRC_RELATIVE_ORIGINAL_UNAVAILABLE;
        return decision;
    }
    if (!two_hop_valid[original_index] ||
        !std::isfinite(scores[original_index])) {
        decision.reason = NMRC_RELATIVE_ORIGINAL_UNKNOWN;
        return decision;
    }
    decision.original_score = scores[original_index];
    if (decision.original_score < absolute_threshold - NMRC_RELATIVE_EPSILON) {
        decision.reason = NMRC_RELATIVE_ORIGINAL_BELOW_ABSOLUTE;
        return decision;
    }

    double best_score = decision.original_score;
    vector<uint32_t> candidates;
    for (uint32_t i = 0; i < scores.size(); i++) {
        if (i == original_index || !available[i] || !two_hop_valid[i] ||
            !std::isfinite(scores[i]) ||
            scores[i] >= decision.original_score - NMRC_RELATIVE_EPSILON)
            continue;
        if (scores[i] < best_score - NMRC_RELATIVE_EPSILON) {
            best_score = scores[i];
            candidates.clear();
            candidates.push_back(i);
        } else if (std::fabs(scores[i] - best_score) <= NMRC_RELATIVE_EPSILON) {
            candidates.push_back(i);
        }
    }
    decision.candidate_count = candidates.size();
    if (candidates.empty()) {
        decision.reason = NMRC_RELATIVE_NO_DELTA_CANDIDATE;
        return decision;
    }
    decision.best_gap = decision.original_score - best_score;
    decision.selected_index = candidates[selection_value % candidates.size()];
    decision.selected_score = scores[decision.selected_index];
    decision.selected_gap = decision.original_score - decision.selected_score;
    decision.request_cooldown =
        decision.best_gap >= cooldown_delta - NMRC_RELATIVE_EPSILON;
    decision.reroute = true;
    decision.reason = NMRC_RELATIVE_SELECTED;
    return decision;
}

bool FatTreeSwitch::nmrc_is_source_leaf_data(
        Packet& pkt, vector<FibEntry*>* available_hops) const {
    if (!_nmrc_hybrid_enabled || _type != TOR || !_ft ||
        pkt.type() != ROCE || !available_hops || available_hops->empty())
        return false;
    if (pkt.get_direction() != ::NONE ||
        (*available_hops)[0]->getDirection() != UP)
        return false;
    RocePacket* data = dynamic_cast<RocePacket*>(&pkt);
    if (!data || !data->has_mrc_ev() || data->src() == UINT32_MAX)
        return false;
    return _ft->HOST_POD_SWITCH(data->src()) == _id;
}

bool FatTreeSwitch::nmrc_inject_fastcnp(
        const RocePacket& data,
        uint32_t original_egress,
        uint32_t selected_egress,
        uint8_t original_level,
        uint8_t selected_level,
        double original_score,
        double selected_score,
        bool need_endpoint_cooldown) {
    HostFibEntry* host = _fib->getHostRoute(data.src(), data.flow_id());
    if (!host || !host->getEgressPort()) {
        _nmrc_diag_fastcnp_route_missing++;
        return false;
    }

    Route* route = host->getEgressPort();
    RoceFastCnp* fast_cnp = RoceFastCnp::newpkt(
        data.flow(), *route, data.src(), data.mrc_ev(), data.seqno(),
        original_egress, selected_egress,
        original_level, selected_level, _id, eventlist().now(),
        data.attempt_id(), original_score, selected_score,
        original_score - selected_score, 0, need_endpoint_cooldown);
    _nmrc_diag_fastcnp_generated++;
    receivePacket(*fast_cnp);
    return true;
}

uint64_t FatTreeSwitch::nmrc_relative_action_key(
        uint32_t flow_id, RocePacket::seq_t psn, uint8_t attempt,
        uint32_t original_ev, uint32_t original_egress,
        uint32_t selected_egress) {
    uint32_t psn_low = (uint32_t)psn;
    uint32_t psn_high = (uint32_t)(psn >> 32);
    uint32_t low = freeBSDHash(
        flow_id, psn_low ^ psn_high,
        original_ev ^ ((uint32_t)attempt << 24));
    uint32_t high = freeBSDHash(
        original_egress, selected_egress, low ^ psn_high);
    return ((uint64_t)high << 32) | low;
}

bool FatTreeSwitch::nmrc_inject_relative_fastcnp(
        RocePacket& data, uint32_t original_egress,
        uint32_t selected_egress, double original_score,
        double selected_score, uint64_t action_key,
        bool need_endpoint_cooldown) {
    HostFibEntry* host = _fib->getHostRoute(data.src(), data.flow_id());
    if (!host || !host->getEgressPort())
        return false;

    RoceFastCnp* fast_cnp = RoceFastCnp::newpkt(
        data.flow(), *host->getEgressPort(), data.src(), data.mrc_ev(),
        data.seqno(), original_egress, selected_egress,
        UINT8_MAX, UINT8_MAX, _id, eventlist().now(), data.attempt_id(),
        original_score, selected_score, original_score - selected_score,
        action_key, need_endpoint_cooldown);
    fast_cnp->sendOn();
    return true;
}

uint32_t FatTreeSwitch::nmrc_relative_hist_bin(double value) {
    if (!std::isfinite(value) || value <= 0.0)
        return 0;
    return std::min(
        (uint32_t)std::floor(
            (value + NMRC_RELATIVE_EPSILON) / 0.05),
        20U);
}

uint32_t FatTreeSwitch::nmrc_maybe_reroute(
        Packet& pkt, vector<FibEntry*>* available_hops,
        uint32_t original_choice) {
    if (!nmrc_is_source_leaf_data(pkt, available_hops) ||
        original_choice >= available_hops->size())
        return original_choice;

    _nmrc_diag_route_checks++;
    uint64_t flow_key = ((uint64_t)pkt.flow_id()) << 32;
    RocePacket& data = (RocePacket&)pkt;
    if (_nmrc_network_decision_mode == NMRC_NETWORK_GRADED) {
        data.set_nmrc_detour(false);
        data.set_nmrc_actual_egress(original_choice);
    }
    _nmrc_diag_observed_flow_evs.insert(flow_key | data.mrc_ev());
    _nmrc_diag_observed_flow_paths.insert(flow_key | original_choice);

    if (_nmrc_network_decision_mode == NMRC_NETWORK_FIXED_THRESHOLD ||
        _nmrc_network_decision_mode == NMRC_NETWORK_DELTA ||
        _nmrc_network_decision_mode == NMRC_NETWORK_PIECEWISE_DELTA ||
        _nmrc_network_decision_mode == NMRC_NETWORK_TWO_STAGE_DELTA ||
        _nmrc_network_decision_mode == NMRC_NETWORK_ABSOLUTE_REROUTE) {
        _nmrc_diag_relative_checks++;
        const bool ce_before = (data.flags() & ECN_CE) != 0;
        const auto finish_relative = [&data, ce_before](uint32_t choice) {
            const bool ce_after = (data.flags() & ECN_CE) != 0;
            if (!ce_before && ce_after)
                FatTreeSwitch::_nmrc_diag_relative_decision_ce_set++;
            if (ce_before && !ce_after)
                FatTreeSwitch::_nmrc_diag_relative_decision_ce_cleared++;
            return choice;
        };

        vector<double> scores(available_hops->size(),
                              std::numeric_limits<double>::infinity());
        vector<bool> two_hop_valid(available_hops->size(), false);
        vector<bool> available(available_hops->size(), false);
        for (uint32_t i = 0; i < available_hops->size(); i++) {
            available[i] = sglb_entry_available((*available_hops)[i]);
            const SglbQualitySnapshot& snapshot =
                sglb_quality_snapshot((*available_hops)[i], pkt.dst(), 1);
            scores[i] = snapshot.score;
            two_hop_valid[i] = snapshot.two_hop_valid();
        }

        uint32_t selection_value = freeBSDHash(
            pkt.flow_id(), data.mrc_ev(),
            (uint32_t)data.seqno() ^ _hash_salt);
        NmrcRelativeDecision decision;
        if (_nmrc_network_decision_mode == NMRC_NETWORK_DELTA)
            decision = nmrc_select_delta_path(
                original_choice, scores, two_hop_valid, available,
                _nmrc_relative_delta, selection_value);
        else if (_nmrc_network_decision_mode == NMRC_NETWORK_ABSOLUTE_REROUTE)
            decision = nmrc_select_absolute_reroute_path(
                original_choice, scores, two_hop_valid, available,
                _nmrc_absolute_threshold, _nmrc_cooldown_delta,
                selection_value);
        else if (_nmrc_network_decision_mode == NMRC_NETWORK_TWO_STAGE_DELTA)
            decision = nmrc_select_two_stage_delta_path(
                original_choice, scores, two_hop_valid, available,
                _nmrc_route_delta, _nmrc_cooldown_delta, selection_value);
        else if (_nmrc_network_decision_mode == NMRC_NETWORK_PIECEWISE_DELTA)
            decision = nmrc_select_piecewise_delta_path(
                original_choice, scores, two_hop_valid, available,
                _nmrc_absolute_threshold, _nmrc_piecewise_delta_below,
                _nmrc_piecewise_delta_above, selection_value);
        else
            decision = nmrc_select_fixed_threshold_path(
                original_choice, scores, two_hop_valid, available,
                _nmrc_absolute_threshold, _nmrc_relative_delta,
                selection_value);
        _nmrc_diag_relative_candidate_count[
            std::min(decision.candidate_count, 32U)]++;

        if (decision.reason == NMRC_RELATIVE_ORIGINAL_UNAVAILABLE) {
            _nmrc_diag_relative_original_unavailable++;
            return finish_relative(original_choice);
        }
        if (decision.reason == NMRC_RELATIVE_ORIGINAL_UNKNOWN) {
            _nmrc_diag_relative_original_unknown++;
            return finish_relative(original_choice);
        }

        if (std::isfinite(decision.original_score)) {
            _nmrc_diag_relative_best_gap[
                nmrc_relative_hist_bin(decision.best_gap)]++;
        }

        if (!decision.reroute) {
            if (decision.reason ==
                    NMRC_RELATIVE_ORIGINAL_BELOW_ABSOLUTE)
                _nmrc_diag_relative_original_below_absolute++;
            else if (decision.reason == NMRC_RELATIVE_NO_SAFE_CANDIDATE)
                _nmrc_diag_relative_no_safe_candidate++;
            else if (decision.reason == NMRC_RELATIVE_NO_DELTA_CANDIDATE)
                _nmrc_diag_relative_no_delta_candidate++;
            return finish_relative(original_choice);
        }

        const double required_delta =
            _nmrc_network_decision_mode == NMRC_NETWORK_TWO_STAGE_DELTA
            ? _nmrc_route_delta
            : _nmrc_network_decision_mode == NMRC_NETWORK_PIECEWISE_DELTA
            ? (decision.original_score < _nmrc_absolute_threshold
                ? _nmrc_piecewise_delta_below
                : _nmrc_piecewise_delta_above)
            : _nmrc_relative_delta;
        if ((_nmrc_network_decision_mode == NMRC_NETWORK_ABSOLUTE_REROUTE &&
             decision.selected_gap <= NMRC_RELATIVE_EPSILON) ||
            (_nmrc_network_decision_mode != NMRC_NETWORK_ABSOLUTE_REROUTE &&
             decision.selected_gap < required_delta - NMRC_RELATIVE_EPSILON)) {
            _nmrc_diag_relative_selected_gap_violations++;
            return finish_relative(original_choice);
        }
        if (!_nmrc_fastcnp_enabled)
            return finish_relative(original_choice);

        uint64_t action_key = nmrc_relative_action_key(
            data.flow_id(), data.seqno(), data.attempt_id(), data.mrc_ev(),
            original_choice, decision.selected_index);
        if (!nmrc_inject_relative_fastcnp(
                data, original_choice, decision.selected_index,
                decision.original_score, decision.selected_score,
                action_key, decision.request_cooldown)) {
            _nmrc_diag_relative_reverse_path_blocked++;
            _nmrc_diag_fastcnp_route_missing++;
            return finish_relative(original_choice);
        }

        data.set_nmrc_detour(true);
        data.set_nmrc_actual_egress(decision.selected_index);
        _nmrc_diag_reroutes++;
        _nmrc_diag_fastcnp_generated++;
        _nmrc_diag_relative_paired_actions++;
        _nmrc_diag_relative_reroutes++;
        if (_nmrc_network_decision_mode == NMRC_NETWORK_TWO_STAGE_DELTA ||
            _nmrc_network_decision_mode == NMRC_NETWORK_ABSOLUTE_REROUTE) {
            if (decision.request_cooldown)
                _nmrc_diag_two_stage_cooldown_requested++;
            else
                _nmrc_diag_two_stage_reroute_only++;
        }
        _nmrc_diag_relative_reroute_key_count++;
        _nmrc_diag_relative_reroute_key_sum += action_key;
        _nmrc_diag_relative_reroute_key_xor ^= action_key;
        _nmrc_diag_relative_generated_key_count++;
        _nmrc_diag_relative_generated_key_sum += action_key;
        _nmrc_diag_relative_generated_key_xor ^= action_key;
        _nmrc_diag_relative_original_score_count++;
        _nmrc_diag_relative_original_score_sum += decision.original_score;
        _nmrc_diag_relative_original_score_max = std::max(
            _nmrc_diag_relative_original_score_max,
            decision.original_score);
        _nmrc_diag_relative_original_score[
            nmrc_relative_hist_bin(decision.original_score)]++;
        _nmrc_diag_relative_selected_score_count++;
        _nmrc_diag_relative_selected_score_sum += decision.selected_score;
        _nmrc_diag_relative_selected_score_max = std::max(
            _nmrc_diag_relative_selected_score_max,
            decision.selected_score);
        _nmrc_diag_relative_selected_score[
            nmrc_relative_hist_bin(decision.selected_score)]++;
        _nmrc_diag_relative_selected_gap[
            nmrc_relative_hist_bin(decision.selected_gap)]++;
        _nmrc_diag_relative_actual_egress[
            std::min(decision.selected_index, 32U)]++;
        return finish_relative(decision.selected_index);
    }

    if (_nmrc_network_decision_mode == NMRC_NETWORK_BINARY_SCORE) {
        vector<double> scores(available_hops->size(),
                              std::numeric_limits<double>::infinity());
        vector<bool> two_hop_valid(available_hops->size(), false);
        vector<bool> available(available_hops->size(), false);
        for (uint32_t i = 0; i < available_hops->size(); i++) {
            available[i] = sglb_entry_available((*available_hops)[i]);
            const SglbQualitySnapshot& snapshot =
                sglb_quality_snapshot((*available_hops)[i], pkt.dst(), 1);
            scores[i] = snapshot.score;
            two_hop_valid[i] = snapshot.two_hop_valid();
        }

        NmrcBinaryClass original_class = nmrc_binary_classify(
            scores[original_choice], two_hop_valid[original_choice]);
        if (original_class == NMRC_BINARY_SAFE) {
            _nmrc_diag_binary_original_safe++;
            return original_choice;
        }
        if (original_class == NMRC_BINARY_UNKNOWN)
            return original_choice;

        _nmrc_diag_binary_original_congested++;
        uint32_t selection_value = freeBSDHash(
            pkt.flow_id(), data.mrc_ev(),
            (uint32_t)data.seqno() ^ _hash_salt);
        NmrcRerouteDecision decision = nmrc_select_binary_path(
            original_choice, scores, two_hop_valid, available,
            selection_value);
        uint32_t better_bucket = std::min(decision.better_count, 32U);
        _nmrc_diag_better_count[better_bucket]++;
        if (!decision.reroute) {
            _nmrc_diag_binary_no_safe++;
            return original_choice;
        }

        if (!_nmrc_fastcnp_enabled)
            return original_choice;

        HostFibEntry* host = _fib->getHostRoute(data.src(), data.flow_id());
        if (!host || !host->getEgressPort()) {
            _nmrc_diag_binary_route_missing++;
            _nmrc_diag_fastcnp_route_missing++;
            return original_choice;
        }

        if (!nmrc_inject_fastcnp(
                data, original_choice, decision.selected_index, 1, 0,
                scores[original_choice], scores[decision.selected_index],
                true)) {
            return original_choice;
        }

        data.set_nmrc_detour(true);
        data.set_nmrc_actual_egress(decision.selected_index);
        _nmrc_diag_reroutes++;
        _nmrc_diag_binary_paired_actions++;
        _nmrc_diag_level_transitions[1][0]++;
        _nmrc_diag_binary_original_score_sum += scores[original_choice];
        _nmrc_diag_binary_original_score_max = std::max(
            _nmrc_diag_binary_original_score_max,
            scores[original_choice]);
        _nmrc_diag_binary_selected_score_sum +=
            scores[decision.selected_index];
        _nmrc_diag_binary_selected_score_max = std::max(
            _nmrc_diag_binary_selected_score_max,
            scores[decision.selected_index]);
        _nmrc_diag_binary_actual_egress[
            std::min(decision.selected_index, 32U)]++;
        return decision.selected_index;
    }

    vector<uint8_t> levels(available_hops->size(), STOR_LEVEL_AVOID);
    vector<double> scores(
        available_hops->size(),
        std::numeric_limits<double>::quiet_NaN());
    vector<bool> available(available_hops->size(), false);
    for (uint32_t i = 0; i < available_hops->size(); i++) {
        available[i] = sglb_entry_available((*available_hops)[i]);
        const SglbQualitySnapshot& snapshot =
            sglb_quality_snapshot((*available_hops)[i], pkt.dst(), 1);
        if (snapshot.two_hop_valid())
            scores[i] = snapshot.score;
        levels[i] = snapshot.quality;
        if (_sglb_score_mode == SGLB_SCORE_NMRC_QUANTIZED_TOPK &&
            _sglb_nmrc_levels == 8)
            levels[i] /= 2;
        if (levels[i] > STOR_LEVEL_AVOID)
            levels[i] = STOR_LEVEL_AVOID;
    }

    uint32_t selection_value = freeBSDHash(
        pkt.flow_id(), data.mrc_ev(),
        (uint32_t)data.seqno() ^ _hash_salt);
    NmrcRerouteDecision decision = nmrc_select_graded_delta_path(
        original_choice, levels, scores, available, _nmrc_reroute_policy,
        _sglb_min_choices, selection_value, _nmrc_graded_reroute_delta);
    uint32_t better_bucket = std::min(decision.better_count, 32U);
    _nmrc_diag_better_count[better_bucket]++;
    if (!decision.reroute) {
        if (decision.better_count > 0)
            _nmrc_diag_threshold_blocked++;
        return original_choice;
    }

    const bool request_cooldown = nmrc_graded_cooldown_requested(
        _nmrc_graded_cooldown_mode,
        scores[original_choice], scores[decision.selected_index],
        _nmrc_graded_cooldown_delta);
    if (_nmrc_fastcnp_enabled) {
        if (!nmrc_inject_fastcnp(
                data, original_choice, decision.selected_index,
                decision.original_level, decision.selected_level,
                scores[original_choice], scores[decision.selected_index],
                request_cooldown)) {
            // PATH_REROUTE FastCNP is the endpoint's only indication that its
            // EV was overridden. With signalling enabled, do not create an
            // unobservable reroute when the reverse host route is unavailable.
            return original_choice;
        }
        if (request_cooldown)
            _nmrc_diag_graded_cooldown_requested++;
        else
            _nmrc_diag_graded_cooldown_suppressed++;
    }

    _nmrc_diag_reroutes++;
    if (decision.original_level <= STOR_LEVEL_AVOID &&
        decision.selected_level <= STOR_LEVEL_AVOID) {
        _nmrc_diag_level_transitions[decision.original_level]
                                    [decision.selected_level]++;
    }
    data.set_nmrc_detour(true);
    data.set_nmrc_actual_egress(decision.selected_index);
    return decision.selected_index;
}

uint32_t FatTreeSwitch::sglb_best_score(uint32_t dst, uint32_t depth) {
    vector<FibEntry*> *available_hops = sglb_uses_leaf_profiles() ?
        _fib->getLeafRoutes(_ft->HOST_POD_SWITCH(dst)) :
        _fib->getRoutes(dst);
    if (!available_hops || available_hops->empty())
        return 0;

    double best = std::numeric_limits<double>::max();
    for (uint32_t i = 0; i < available_hops->size(); i++) {
        const SglbQualitySnapshot& snapshot =
            sglb_quality_snapshot((*available_hops)[i], dst, depth);
        if (snapshot.score < best)
            best = snapshot.score;
    }
    return best == std::numeric_limits<double>::max() ? 0 : (uint32_t)best;
}

bool FatTreeSwitch::sglb_update_candidate_state(
        SglbCandidateState& state, const vector<uint32_t>& candidates,
        simtime_picosec now) {
    vector<uint32_t> members = candidates;
    std::sort(members.begin(), members.end());
    members.erase(std::unique(members.begin(), members.end()), members.end());
    if (!state.valid) {
        state.valid = true;
        state.members = members;
        return false;
    }
    if (state.members == members)
        return false;
    for (size_t i = 0; i < state.members.size(); ++i) {
        if (!std::binary_search(members.begin(), members.end(), state.members[i]))
            state.excluded_at[state.members[i]] = now;
    }
    state.members = members;
    state.epoch++;
    return true;
}

bool FatTreeSwitch::sglb_stale_excluded(
        const SglbCandidateState& state, uint64_t tx_epoch,
        uint32_t selected_path) {
    return state.valid && tx_epoch < state.epoch &&
        !std::binary_search(
            state.members.begin(), state.members.end(), selected_path);
}

uint32_t FatTreeSwitch::sglb_route(vector<FibEntry*>* ecmp_set, uint32_t dst,
                                   vector<uint32_t>* candidates_out) {
    vector<uint8_t> qualities(ecmp_set->size(), 255);
    vector<double> scores(ecmp_set->size(), 0.0);
    vector<bool> available(ecmp_set->size(), false);
    uint8_t best_quality = 255;
    double min_score = std::numeric_limits<double>::max();
    double max_score = 0.0;
    uint32_t available_count = 0;

    for (uint32_t i = 0; i < ecmp_set->size(); i++) {
        available[i] = sglb_entry_available((*ecmp_set)[i]);
        if (!available[i])
            continue;
        const SglbQualitySnapshot& snapshot =
            sglb_quality_snapshot((*ecmp_set)[i], dst, 1);
        qualities[i] = snapshot.quality;
        scores[i] = snapshot.score;
        uint8_t diagnostic_level = snapshot.quality;
        if (_sglb_score_mode == SGLB_SCORE_NMRC_QUANTIZED_TOPK &&
            _sglb_nmrc_levels == 8) {
            diagnostic_level /= 2;
        }
        if (diagnostic_level <= STOR_LEVEL_AVOID)
            _sglb_diag_observed_levels[diagnostic_level]++;
        available_count++;
        if (snapshot.score < min_score)
            min_score = snapshot.score;
        if (snapshot.score > max_score)
            max_score = snapshot.score;
        if (qualities[i] < best_quality)
            best_quality = qualities[i];
    }

    uint32_t distinct_quality_count = 0;
    uint32_t best_quality_count = 0;
    bool seen_quality[256] = {false};
    for (uint32_t i = 0; i < ecmp_set->size(); i++) {
        if (!available[i])
            continue;
        if (!seen_quality[qualities[i]]) {
            seen_quality[qualities[i]] = true;
            distinct_quality_count++;
        }
        if (qualities[i] == best_quality)
            best_quality_count++;
    }

    vector<uint32_t> best_choices;
    bool use_shuffled_rr = false;
    uint32_t rr_dst_tor = dst;
    uint64_t rr_quality_signature = 0;
    if (_sglb_candidate_policy == SGLB_CANDIDATE_SCORE_TOPK) {
        vector<uint64_t> tie_keys(ecmp_set->size(), 0);
        for (uint32_t i = 0; i < ecmp_set->size(); ++i)
            tie_keys[i] = (static_cast<uint64_t>(random()) << 32) ^ random();
        best_choices = sglb_score_topk(scores, available, tie_keys, _sglb_topk);
    } else if (sglb_ofat_uses_topk8()) {
        vector<uint64_t> tie_keys(ecmp_set->size(), 0);
        for (uint32_t i = 0; i < ecmp_set->size(); ++i)
            tie_keys[i] = (static_cast<uint64_t>(random()) << 32) ^ random();
        best_choices = paper_sglb_topk_by_level(
            qualities, available, tie_keys, 8);
    } else {
        uint32_t min_choices = _sglb_min_choices ? _sglb_min_choices : 1;
        if (_sglb_score_mode == SGLB_SCORE_NMRC_QUANTIZED_TOPK &&
            _sglb_ofat_factor == SGLB_OFAT_REAL_GCN_RAW_LINEAR) {
            use_shuffled_rr =
                _sglb_candidate_dispatch == SGLB_DISPATCH_SHUFFLED_RR;
            rr_dst_tor = sglb_profile_destination(dst);
            vector<uint64_t> tie_keys(ecmp_set->size(), 0);
            for (uint32_t i = 0; i < ecmp_set->size(); ++i) {
                uint64_t path_state =
                    (static_cast<uint64_t>(i) << 16) |
                    (static_cast<uint64_t>(available[i] ? 1 : 0) << 8) |
                    qualities[i];
                rr_quality_signature ^= path_state;
                rr_quality_signature *= 1099511628211ULL;
            }
            for (uint32_t i = 0; i < ecmp_set->size(); ++i) {
                tie_keys[i] = (static_cast<uint64_t>(random()) << 32) ^
                    static_cast<uint64_t>(random());
            }
            if (_sglb_candidate_policy == SGLB_CANDIDATE_STRICT_K) {
                best_choices = paper_sglb_strict_k_by_level(
                    qualities, available, tie_keys, min_choices);
            } else if (_sglb_candidate_policy ==
                       SGLB_CANDIDATE_WHOLE_GRADE_MIN) {
                best_choices = paper_sglb_best_level(
                    qualities, available, min_choices);
            } else {
                best_choices = paper_sglb_exact_min_by_level(
                    qualities, available, tie_keys, min_choices);
            }
        } else {
            uint32_t levels = _sglb_quality_levels ? _sglb_quality_levels :
                (_sglb_max_quality + 1);
            for (uint32_t q = best_quality;
                 q < levels && best_choices.size() < min_choices; q++) {
                for (uint32_t i = 0; i < ecmp_set->size(); i++) {
                    if (available[i] && qualities[i] == q)
                        best_choices.push_back(i);
                }
            }
        }
    }

    if (best_choices.empty()) {
        if (candidates_out) {
            candidates_out->resize(ecmp_set->size());
            for (uint32_t i = 0; i < ecmp_set->size(); ++i)
                (*candidates_out)[i] = i;
        }
        return random() % ecmp_set->size();
    }
    if (candidates_out)
        *candidates_out = best_choices;
    uint32_t selected = use_shuffled_rr ?
        sglb_shuffled_rr_select(
            best_choices, _id, rr_dst_tor, rr_quality_signature,
            _sglb_shuffled_rr_states[rr_dst_tor]) :
        best_choices[random() % best_choices.size()];
    _sglb_diag_route_calls++;
    _sglb_diag_available_choices += available_count;
    _sglb_diag_candidate_choices += best_choices.size();
    _sglb_diag_best_quality_choices += best_quality_count;
    _sglb_diag_distinct_qualities += distinct_quality_count;
    if (distinct_quality_count == 1)
        _sglb_diag_all_same_quality_calls++;
    if (distinct_quality_count == 1 && best_quality == 0)
        _sglb_diag_all_zero_quality_calls++;
    if (qualities[selected] > best_quality)
        _sglb_diag_selected_nonbest_quality++;
    uint8_t selected_diagnostic_level = qualities[selected];
    if (_sglb_score_mode == SGLB_SCORE_NMRC_QUANTIZED_TOPK &&
        _sglb_nmrc_levels == 8) {
        selected_diagnostic_level /= 2;
    }
    if (selected_diagnostic_level <= STOR_LEVEL_AVOID)
        _sglb_diag_selected_levels[selected_diagnostic_level]++;
    if (min_score != std::numeric_limits<double>::max())
        _sglb_diag_score_spread_sum += max_score - min_score;
    return selected;
}

uint32_t FatTreeSwitch::drill_route(vector<FibEntry*>* ecmp_set, uint32_t dst) {
    uint32_t candidates[3];
    uint32_t candidate_count = 0;
    uint32_t hop_count = ecmp_set->size();

    candidates[candidate_count++] = random() % hop_count;
    candidates[candidate_count++] = random() % hop_count;

    if (_drill_memory.find(dst) != _drill_memory.end())
        candidates[candidate_count++] = _drill_memory[dst] % hop_count;

    uint32_t best = candidates[0];
    for (uint32_t i = 1; i < candidate_count; i++) {
        int8_t c = fn((*ecmp_set)[best], (*ecmp_set)[candidates[i]]);
        if (c < 0)
            best = candidates[i];
    }

    _drill_memory[dst] = best;
    return best;
}

uint32_t FatTreeSwitch::pathid_ecmp_choice(Packet& pkt, uint32_t hop_count, packet_direction direction) {
    uint32_t choice = UINT32_MAX;
    // REPS changes the UDP source port to a 16-bit entropy value. Model that
    // operation at the source ToR instead of mapping EV->path at the endpoint.
    if (_type == TOR && direction == UP && pkt.type() == ROCE) {
        RocePacket& data = (RocePacket&)pkt;
        if (data.reps_entropy() && data.routing_entropy() != UINT32_MAX) {
            // REPS replaces UDP source port with the 16-bit EV. htsim has no
            // UDP header, so use the stable flow id as the destination-port
            // identity and pack the two 16-bit ports exactly as ECMP does.
            const uint32_t ports =
                (data.routing_entropy() & 0xffffu) |
                ((pkt.flow_id() & 0xffffu) << 16);
            choice = paper_ecmp_hash(data.src(), data.dst(),
                                     ports, _id) % hop_count;
            if (getenv("HTSIM_ECMP_DIAG")) {
                cerr << "EcmpDecision time_ps=" << eventlist().now()
                     << " leaf=" << _id
                     << " flow=" << pkt.flow_id()
                     << " dst_leaf=" << (_ft ? _ft->HOST_POD_SWITCH(pkt.dst()) : 0)
                     << " selected=" << choice
                     << " pathid=" << pkt.pathid() << endl;
            }
            return choice;
        }
    }
    if (!_pathid_only_hash) {
        choice = paper_ecmp_hash(pkt.flow_id(), pkt.dst(),
                                 pkt.flow_id() ^ pkt.dst(), _id) % hop_count;
        if (_type == TOR && direction == UP && pkt.type() == ROCE &&
            getenv("HTSIM_ECMP_DIAG")) {
            cerr << "EcmpDecision time_ps=" << eventlist().now()
                 << " leaf=" << _id
                 << " flow=" << pkt.flow_id()
                 << " dst_leaf=" << (_ft ? _ft->HOST_POD_SWITCH(pkt.dst()) : 0)
                 << " selected=" << choice
                 << " pathid=" << pkt.pathid() << endl;
        }
        return choice;
    }

    uint32_t pathid = pkt.pathid();
    if (_type == TOR) {
        choice = pathid % hop_count;
        if (direction == UP && pkt.type() == ROCE && getenv("HTSIM_ECMP_DIAG")) {
            cerr << "EcmpDecision time_ps=" << eventlist().now()
                 << " leaf=" << _id
                 << " flow=" << pkt.flow_id()
                 << " dst_leaf=" << (_ft ? _ft->HOST_POD_SWITCH(pkt.dst()) : 0)
                 << " selected=" << choice
                 << " pathid=" << pkt.pathid() << endl;
        }
        return choice;
    }

    uint32_t tor_choices = _ft->radix_up(TOR_TIER);
    if (tor_choices == 0)
        tor_choices = 1;

    if (_type == AGG) {
        if (direction == UP)
            return (pathid / tor_choices) % hop_count;

        uint32_t divisor = tor_choices;
        if (_ft->get_tiers() == 3) {
            uint32_t agg_up_choices = _ft->radix_up(AGG_TIER);
            if (agg_up_choices == 0)
                agg_up_choices = 1;
            uint32_t core_down_choices = _ft->bundlesize(CORE_TIER);
            if (core_down_choices == 0)
                core_down_choices = 1;
            divisor *= agg_up_choices;
            divisor *= core_down_choices;
        }
        return (pathid / divisor) % hop_count;
    }
    if (_type == CORE) {
        uint32_t agg_up_choices = _ft->radix_up(AGG_TIER);
        if (agg_up_choices == 0)
            agg_up_choices = 1;
        return (pathid / (tor_choices * agg_up_choices)) % hop_count;
    }
    return pathid % hop_count;
}

BaseQueue* FatTreeSwitch::netaware_local_queue_for_ev(uint32_t dst, uint32_t ev) {
    vector<FibEntry*>* hops = _fib->getRoutes(dst);
    if (!hops || hops->empty())
        return NULL;

    FibEntry* entry = hops->at(ev % hops->size());
    if (!entry)
        return NULL;

    Route* route = entry->getEgressPort();
    if (!route || route->size() == 0)
        return NULL;

    return dynamic_cast<BaseQueue*>(route->at(0));
}

BaseQueue* FatTreeSwitch::netaware_trace_spine_queue_for_ev(uint32_t dst,
                                                        uint32_t ev) {
    vector<FibEntry*>* hops = _fib->getRoutes(dst);
    if (!hops || hops->empty())
        return NULL;

    FibEntry* entry = hops->at(ev % hops->size());
    if (!entry)
        return NULL;

    Route* route = entry->getEgressPort();
    if (!route || route->size() <= 2)
        return NULL;

    FatTreeSwitch* spine = dynamic_cast<FatTreeSwitch*>(route->at(2));
    if (!spine)
        return NULL;

    vector<FibEntry*>* spine_hops = spine->_fib->getRoutes(dst);
    if (!spine_hops || spine_hops->empty())
        return NULL;

    uint32_t tor_choices = _ft->radix_up(TOR_TIER);
    if (tor_choices == 0)
        tor_choices = 1;
    uint32_t spine_choice = (ev / tor_choices) % spine_hops->size();
    FibEntry* spine_entry = spine_hops->at(spine_choice);
    if (!spine_entry)
        return NULL;

    Route* spine_route = spine_entry->getEgressPort();
    if (!spine_route || spine_route->size() == 0)
        return NULL;

    return dynamic_cast<BaseQueue*>(spine_route->at(0));
}

FatTreeSwitch::NetawarePortSnapshot
FatTreeSwitch::netaware_read_port_snapshot(BaseQueue* q) {
    NetawarePortSnapshot snapshot;
    if (!q)
        return snapshot;

    snapshot.queue_fraction = sglb_queue_fraction(q);
    snapshot.utilization_fraction =
        (double)sglb_utilization_percent(q) / 100.0;
    snapshot.bitrate = q->bitrate();
    LosslessOutputQueue* lq = dynamic_cast<LosslessOutputQueue*>(q);
    snapshot.paused = lq && lq->is_paused();
    snapshot.bytes_sent = q->bytes_sent_count();
    snapshot.last_update = eventlist().now();
    snapshot.valid = true;
    return snapshot;
}

FatTreeSwitch::NetawarePortSnapshot
FatTreeSwitch::netaware_local_snapshot(BaseQueue* q) {
    if (!q)
        return NetawarePortSnapshot();

    NetawarePortSnapshot& cached = _netaware_local_state[q];
    simtime_picosec now = eventlist().now();
    if (netaware_snapshot_refresh_due(cached, now,
                                  _netaware_state_update_interval)) {
        NetawarePortSnapshot sample = netaware_read_port_snapshot(q);
        if (cached.valid && sample.last_update > cached.last_update &&
            sample.bitrate > 0 && sample.bytes_sent >= cached.bytes_sent) {
            double dt = timeAsSec(sample.last_update - cached.last_update);
            double rate = dt > 0.0 ?
                ((double)(sample.bytes_sent - cached.bytes_sent) * 8.0) /
                (dt * (double)sample.bitrate) : 0.0;
            if (rate > 1.0)
                rate = 1.0;
            // DRE EWMA: fast response to bursts, gradual aging after idle.
            const double alpha = 0.25;
            sample.dre_pressure = (1.0 - alpha) * cached.dre_pressure +
                                  alpha * rate;
            sample.utilization_fraction = std::max(
                sample.utilization_fraction, sample.dre_pressure);
        }
        cached = sample;
    }
    return cached;
}

FatTreeSwitch::NetawareExportState
FatTreeSwitch::netaware_compute_export_state(uint32_t dst) {
    NetawareExportState state;
    vector<FibEntry*>* hops = _fib->getRoutes(dst);
    if (!hops || hops->empty())
        return state;

    state.ports.reserve(hops->size());
    for (size_t i = 0; i < hops->size(); i++) {
        BaseQueue* q = NULL;
        FibEntry* entry = hops->at(i);
        if (entry) {
            Route* route = entry->getEgressPort();
            if (route && route->size() > 0)
                q = dynamic_cast<BaseQueue*>(route->at(0));
        }
        state.ports.push_back(netaware_local_snapshot(q));
    }
    state.last_update = eventlist().now();
    state.valid = true;
    return state;
}

void FatTreeSwitch::netaware_maybe_refresh_export(uint32_t dst) {
    simtime_picosec now = eventlist().now();
    NetawareExportState& cached = _netaware_exported_state[dst];
    bool due = !cached.valid || _netaware_remote_update_interval == 0 ||
               now < cached.last_update ||
               now - cached.last_update >= _netaware_remote_update_interval;
    if (due)
        cached = netaware_compute_export_state(dst);
    netaware_schedule_periodic_export();
}

void FatTreeSwitch::netaware_schedule_periodic_export() {
    if (!_netaware_enabled || _netaware_remote_update_interval == 0 ||
        _netaware_export_timer_pending || _netaware_exported_state.empty()) {
        return;
    }

    if (!_netaware_export_timer)
        _netaware_export_timer = new NetawareExportTimer(eventlist(), this);
    eventlist().sourceIsPendingRel(*_netaware_export_timer,
                                  _netaware_remote_update_interval);
    _netaware_export_timer_pending = true;
}

void FatTreeSwitch::netaware_periodic_refresh_exports() {
    if (!_netaware_enabled || _netaware_remote_update_interval == 0 ||
        _netaware_exported_state.empty()) {
        return;
    }

    vector<uint32_t> destinations;
    destinations.reserve(_netaware_exported_state.size());
    for (unordered_map<uint32_t,NetawareExportState>::const_iterator it =
             _netaware_exported_state.begin(); it != _netaware_exported_state.end();
         ++it) {
        destinations.push_back(it->first);
    }
    for (size_t i = 0; i < destinations.size(); i++)
        _netaware_exported_state[destinations[i]] =
            netaware_compute_export_state(destinations[i]);
    netaware_schedule_periodic_export();
}

const FatTreeSwitch::NetawarePortSnapshot*
FatTreeSwitch::netaware_neighbor_snapshot(uint32_t dst, uint32_t ev) {
    vector<FibEntry*>* hops = _fib->getRoutes(dst);
    if (!hops || hops->empty())
        return NULL;

    FibEntry* entry = hops->at(ev % hops->size());
    if (!entry)
        return NULL;
    Route* route = entry->getEgressPort();
    if (!route || route->size() <= 2)
        return NULL;
    FatTreeSwitch* spine = dynamic_cast<FatTreeSwitch*>(route->at(2));
    if (!spine)
        return NULL;

    spine->netaware_maybe_refresh_export(dst);
    unordered_map<uint32_t,NetawareExportState>::const_iterator it =
        spine->_netaware_exported_state.find(dst);
    if (it == spine->_netaware_exported_state.end() || !it->second.valid ||
        it->second.ports.empty()) {
        return NULL;
    }

    uint32_t tor_choices = _ft ? _ft->radix_up(TOR_TIER) : 1;
    if (tor_choices == 0)
        tor_choices = 1;
    uint32_t spine_choice =
        (ev / tor_choices) % (uint32_t)it->second.ports.size();
    const NetawarePortSnapshot& snapshot = it->second.ports[spine_choice];
    return snapshot.valid ? &snapshot : NULL;
}

uint8_t FatTreeSwitch::netaware_port_level(const NetawarePortSnapshot& snapshot,
                                       linkspeed_bps normal_bitrate,
                                       bool grade_queues) {
    if (!snapshot.valid)
        return STOR_LEVEL_GOOD;
    (void)normal_bitrate;

    if (snapshot.paused)
        return STOR_LEVEL_AVOID;

    double queue = snapshot.queue_fraction;

    if (!grade_queues)
        return STOR_LEVEL_GOOD;

    double avoid = _netaware_queue_threshold_fraction;
    if (avoid < 0.0)
        avoid = 0.0;
    if (avoid > 1.0)
        avoid = 1.0;

    if (queue >= avoid)
        return STOR_LEVEL_AVOID;

    if (queue >= _netaware_bad_queue_fraction)
        return STOR_LEVEL_BAD;
    if (queue >= _netaware_degraded_queue_fraction)
        return STOR_LEVEL_DEGRADED;

    double util = snapshot.utilization_fraction;
    if (queue >= _netaware_util_queue_floor_fraction &&
        util >= _netaware_degraded_utilization_fraction)
        return STOR_LEVEL_DEGRADED;

    return STOR_LEVEL_GOOD;
}

uint8_t FatTreeSwitch::netaware_gated_port_level(const NetawarePortSnapshot& snapshot,
                                             linkspeed_bps normal_bitrate,
                                             bool grade_queues) {
    if (!snapshot.valid)
        return STOR_LEVEL_GOOD;

    double rate_ratio = 1.0;
    if (normal_bitrate > 0)
        rate_ratio = (double)snapshot.bitrate / (double)normal_bitrate;

    return netaware_gated_level_from_inputs(
        snapshot.paused, rate_ratio, snapshot.queue_fraction,
        snapshot.utilization_fraction, grade_queues);
}

static const char* netaware_dominant_reason(double local_q_contribution,
                                        double remote_q_contribution,
                                        double local_util_contribution,
                                        double remote_util_contribution) {
    const char* reason = "local_q";
    double best = local_q_contribution;
    if (remote_q_contribution > best) {
        best = remote_q_contribution;
        reason = "remote_q";
    }
    if (local_util_contribution > best) {
        best = local_util_contribution;
        reason = "local_util";
    }
    if (remote_util_contribution > best) {
        reason = "remote_util";
    }
    return reason;
}

FatTreeSwitch::NetawarePathScore FatTreeSwitch::netaware_path_score(
        const NetawarePortSnapshot& local,
        const NetawarePortSnapshot& remote) {
    NetawarePathScore score;
    if (!local.valid || !remote.valid) {
        score.reason = "missing_queue";
        return score;
    }

    score.local_q_pressure =
        netaware_pressure_from_range(local.queue_fraction,
                                 _netaware_score_q_min,
                                 _netaware_score_q_max);
    score.remote_q_pressure =
        netaware_pressure_from_range(remote.queue_fraction,
                                 _netaware_score_q_min,
                                 _netaware_score_q_max);
    score.local_util_pressure =
        netaware_pressure_from_range(local.utilization_fraction,
                                 _netaware_score_util_low,
                                 _netaware_score_util_high);
    score.remote_util_pressure =
        netaware_pressure_from_range(remote.utilization_fraction,
                                 _netaware_score_util_low,
                                 _netaware_score_util_high);
    score.path_score =
        netaware_couple_hop_scores(score.local_q_pressure,
                               score.remote_q_pressure,
                               score.local_util_pressure,
                               score.remote_util_pressure);
    score.level = netaware_level_from_score(score.path_score);

    if (score.level == STOR_LEVEL_GOOD) {
        score.reason = "good";
    } else {
        double local_q_contribution =
            _netaware_score_weight_local_q * score.local_q_pressure;
        double remote_q_contribution =
            _netaware_score_weight_remote_q * score.remote_q_pressure;
        double local_util_contribution =
            _netaware_score_weight_local_util * score.local_util_pressure;
        double remote_util_contribution =
            _netaware_score_weight_remote_util * score.remote_util_pressure;
        score.reason = netaware_dominant_reason(local_q_contribution,
                                            remote_q_contribution,
                                            local_util_contribution,
                                            remote_util_contribution);
    }

    return score;
}

StorFeedbackLevels FatTreeSwitch::netaware_compute_levels(uint32_t dst,
                                                       uint32_t path_count) {
    if (path_count == 0)
        path_count = 1;

    vector<NetawarePortSnapshot> local(path_count);
    vector<NetawarePortSnapshot> remote(path_count);
    for (uint32_t ev = 0; ev < path_count; ev++) {
        local[ev] = netaware_local_snapshot(netaware_local_queue_for_ev(dst, ev));
        const NetawarePortSnapshot* neighbor = netaware_neighbor_snapshot(dst, ev);
        if (neighbor)
            remote[ev] = *neighbor;
    }

    if (_netaware_score_mode == NETAWARE_SCORE_SGLB_QUANTIZED) {
        StorFeedbackLevels levels(path_count, STOR_LEVEL_GOOD);
        for (uint32_t ev = 0; ev < path_count; ev++) {
            NetawarePathScore score = netaware_path_score(local[ev], remote[ev]);
            levels[ev] = score.level;
        }
        return levels;
    }

    linkspeed_bps normal_bitrate = 0;
    for (uint32_t ev = 0; ev < path_count; ev++) {
        if (local[ev].valid && local[ev].bitrate > normal_bitrate)
            normal_bitrate = local[ev].bitrate;
        if (remote[ev].valid && remote[ev].bitrate > normal_bitrate)
            normal_bitrate = remote[ev].bitrate;
    }

    bool grade_queues = false;
    if (normal_bitrate > 0) {
        for (uint32_t ev = 0; ev < path_count; ev++) {
            if ((local[ev].valid &&
                 (double)local[ev].bitrate / (double)normal_bitrate <
                     _netaware_slow_link_fraction) ||
                (remote[ev].valid &&
                 (double)remote[ev].bitrate / (double)normal_bitrate <
                     _netaware_slow_link_fraction)) {
                grade_queues = true;
                break;
            }
        }
    }

    StorFeedbackLevels levels(path_count, STOR_LEVEL_GOOD);
    for (uint32_t ev = 0; ev < path_count; ev++) {
        uint8_t local_level = STOR_LEVEL_GOOD;
        uint8_t spine_level = STOR_LEVEL_GOOD;
        if (_netaware_score_mode == NETAWARE_SCORE_GATED) {
            local_level = netaware_gated_port_level(local[ev], normal_bitrate,
                                                grade_queues);
            spine_level = netaware_gated_port_level(remote[ev], normal_bitrate,
                                                grade_queues);
        } else {
            local_level = netaware_port_level(local[ev], normal_bitrate,
                                          grade_queues);
            spine_level = netaware_port_level(remote[ev], normal_bitrate,
                                          grade_queues);
        }
        levels[ev] = local_level > spine_level ? local_level : spine_level;
    }
    return levels;
}

void FatTreeSwitch::netaware_refresh_levels(NetawareState& state, uint32_t dst,
                                         uint32_t path_count,
                                         simtime_picosec now) {
    if (path_count == 0)
        path_count = 1;

    bool size_changed = state.levels.size() != path_count;
    bool stale = !state.levels_valid ||
                 now - state.last_level_update >= _netaware_state_update_interval;
    if (!size_changed && !stale)
        return;

    state.levels = netaware_compute_levels(dst, path_count);
    state.levels_valid = true;
    state.last_level_update = now;
}

static uint32_t netaware_zero_bits(const StorFeedbackLevels& levels) {
    uint32_t zero_bits = 0;
    for (size_t i = 0; i < levels.size(); i++) {
        if (levels[i] >= STOR_LEVEL_BAD)
            zero_bits++;
    }
    return zero_bits;
}

static bool netaware_all_good(const StorFeedbackLevels& levels) {
    for (size_t i = 0; i < levels.size(); i++) {
        if (levels[i] != STOR_LEVEL_GOOD)
            return false;
    }
    return true;
}

static uint64_t netaware_trace_queue_ecn(BaseQueue* q) {
    if (!q)
        return 0;

    LosslessOutputQueue* lossless_output = dynamic_cast<LosslessOutputQueue*>(q);
    if (lossless_output)
        return lossless_output->ecn_mark_count();

    CompositeQueue* composite_queue = dynamic_cast<CompositeQueue*>(q);
    if (composite_queue)
        return composite_queue->ecn_mark_count();

    ECNQueue* ecn_queue = dynamic_cast<ECNQueue*>(q);
    if (ecn_queue)
        return ecn_queue->ecn_mark_count();

    return 0;
}

static uint64_t netaware_trace_queue_trims(BaseQueue* q) {
    if (!q)
        return 0;

    CompositeQueue* composite_queue = dynamic_cast<CompositeQueue*>(q);
    if (composite_queue)
        return composite_queue->trim_count();

    return 0;
}

static uint64_t netaware_trace_queue_drops(BaseQueue* q) {
    if (!q)
        return 0;

    CompositeQueue* composite_queue = dynamic_cast<CompositeQueue*>(q);
    if (composite_queue)
        return composite_queue->drop_count();

    ECNQueue* ecn_queue = dynamic_cast<ECNQueue*>(q);
    if (ecn_queue)
        return ecn_queue->drop_count();

    Queue* queue = dynamic_cast<Queue*>(q);
    if (queue)
        return queue->num_drops();

    return 0;
}

bool FatTreeSwitch::netaware_trace_path_state(uint32_t dst,
                                          uint32_t ev,
                                          uint32_t path_count,
                                          NetawarePathTraceSample& sample) {
    sample = NetawarePathTraceSample();
    if (!_ft)
        return false;

    BaseQueue* local_q = netaware_local_queue_for_ev(dst, ev);
    BaseQueue* spine_q = netaware_trace_spine_queue_for_ev(dst, ev);
    if (!local_q || !spine_q) {
        sample.link_down = true;
        return false;
    }

    vector<FibEntry*>* hops = _fib->getRoutes(dst);
    if (hops && !hops->empty()) {
        FibEntry* entry = hops->at(ev % hops->size());
        if (entry) {
            Route* route = entry->getEgressPort();
            if (route && route->size() > 2) {
                FatTreeSwitch* spine = dynamic_cast<FatTreeSwitch*>(route->at(2));
                if (spine)
                    sample.spine_id = spine->getID();
            }
        }
    }

    sample.q_leaf_to_spine = local_q->queuesize();
    sample.q_spine_to_dst_leaf = spine_q->queuesize();
    sample.q_leaf_to_spine_max = local_q->maxsize();
    sample.q_spine_to_dst_leaf_max = spine_q->maxsize();
    sample.util_leaf_to_spine =
        (double)local_q->peek_average_utilization() / 100.0;
    sample.util_spine_to_dst_leaf =
        (double)spine_q->peek_average_utilization() / 100.0;
    sample.link_rate_leaf_to_spine = local_q->bitrate();
    sample.link_rate_spine_to_dst_leaf = spine_q->bitrate();
    sample.link_down = false;
    sample.ecn_marks_on_path =
        netaware_trace_queue_ecn(local_q) + netaware_trace_queue_ecn(spine_q);
    sample.trimmed_packets_on_path =
        netaware_trace_queue_trims(local_q) + netaware_trace_queue_trims(spine_q);
    sample.dropped_packets_on_path =
        netaware_trace_queue_drops(local_q) + netaware_trace_queue_drops(spine_q);
    sample.bytes_sent_on_path =
        local_q->bytes_sent_count() + spine_q->bytes_sent_count();

    // Build trace-only snapshots without refreshing NetAware's live local or
    // remote caches. A periodic diagnostic must not change feedback cadence.
    const auto trace_snapshot = [this](BaseQueue* q) {
        NetawarePortSnapshot snapshot;
        if (!q)
            return snapshot;
        snapshot.queue_fraction = sglb_queue_fraction(q);
        snapshot.utilization_fraction =
            (double)q->peek_average_utilization() / 100.0;
        snapshot.bitrate = q->bitrate();
        LosslessOutputQueue* lossless =
            dynamic_cast<LosslessOutputQueue*>(q);
        snapshot.paused = lossless && lossless->is_paused();
        snapshot.last_update = eventlist().now();
        snapshot.valid = true;
        return snapshot;
    };

    NetawarePortSnapshot local = trace_snapshot(local_q);
    NetawarePortSnapshot remote = trace_snapshot(spine_q);
    NetawarePathScore score = netaware_path_score(local, remote);
    sample.local_q_pressure = score.local_q_pressure;
    sample.remote_q_pressure = score.remote_q_pressure;
    sample.local_util_pressure = score.local_util_pressure;
    sample.remote_util_pressure = score.remote_util_pressure;
    sample.path_score = score.path_score;
    sample.netaware_level_reason = score.reason;

    if (_netaware_score_mode == NETAWARE_SCORE_SGLB_QUANTIZED) {
        sample.path_grade = score.level;
    } else {
        linkspeed_bps normal_bitrate = 0;
        vector<NetawarePortSnapshot> trace_local(path_count);
        vector<NetawarePortSnapshot> trace_remote(path_count);
        for (uint32_t path = 0; path < path_count; path++) {
            trace_local[path] = trace_snapshot(
                netaware_local_queue_for_ev(dst, path));
            trace_remote[path] = trace_snapshot(
                netaware_trace_spine_queue_for_ev(dst, path));
            if (trace_local[path].valid &&
                trace_local[path].bitrate > normal_bitrate)
                normal_bitrate = trace_local[path].bitrate;
            if (trace_remote[path].valid &&
                trace_remote[path].bitrate > normal_bitrate)
                normal_bitrate = trace_remote[path].bitrate;
        }

        bool grade_queues = false;
        if (normal_bitrate > 0) {
            for (uint32_t path = 0; path < path_count; path++) {
                if ((trace_local[path].valid &&
                     (double)trace_local[path].bitrate /
                         (double)normal_bitrate <
                             _netaware_slow_link_fraction) ||
                    (trace_remote[path].valid &&
                     (double)trace_remote[path].bitrate /
                         (double)normal_bitrate <
                             _netaware_slow_link_fraction)) {
                    grade_queues = true;
                    break;
                }
            }
        }

        uint8_t local_level = STOR_LEVEL_GOOD;
        uint8_t remote_level = STOR_LEVEL_GOOD;
        if (_netaware_score_mode == NETAWARE_SCORE_GATED) {
            local_level = netaware_gated_port_level(
                trace_local[ev], normal_bitrate, grade_queues);
            remote_level = netaware_gated_port_level(
                trace_remote[ev], normal_bitrate, grade_queues);
        } else {
            local_level = netaware_port_level(
                trace_local[ev], normal_bitrate, grade_queues);
            remote_level = netaware_port_level(
                trace_remote[ev], normal_bitrate, grade_queues);
        }
        sample.path_grade = std::max(local_level, remote_level);
        sample.netaware_level_reason = "worst_hop";
    }

    return true;
}

void FatTreeSwitch::maybe_update_netaware_feedback(Packet& pkt) {
    if (!_netaware_enabled || _type != TOR || !_ft || pkt.type() != ROCEACK)
        return;
    if (pkt.dst() == UINT32_MAX || _ft->HOST_POD_SWITCH(pkt.dst()) != _id)
        return;

    RoceAck* ack = dynamic_cast<RoceAck*>(&pkt);
    if (!ack || ack->netaware_peer() == UINT32_MAX)
        return;

    uint32_t path_count = _netaware_path_count ? _netaware_path_count : 1;
    uint32_t peer_key = _ft->HOST_POD_SWITCH(ack->netaware_peer());
    NetawareState& state = _netaware_states[peer_key];
    if (state.levels.size() != path_count) {
        state.levels.assign(path_count, STOR_LEVEL_GOOD);
        state.levels_valid = false;
        state.packets = 0;
        state.last_feedback = 0;
    }

    simtime_picosec now = eventlist().now();
    netaware_refresh_levels(state, ack->netaware_peer(), path_count, now);
    uint32_t zero_bits = netaware_zero_bits(state.levels);
    state.samples++;
    state.sample_zero_bits += zero_bits;
    state.packets++;

    uint32_t feedback_pkts = _netaware_feedback_pkts ? _netaware_feedback_pkts : 1;
    bool packet_trigger = state.packets >= feedback_pkts;
    bool time_trigger = state.packets > 0 &&
                        now - state.last_feedback >= _netaware_feedback_max_interval;
    bool min_elapsed = now - state.last_feedback >= _netaware_feedback_min_interval;

    bool packet_ready = packet_trigger && min_elapsed;
    bool time_ready = time_trigger && !packet_ready;
    if (packet_ready || time_ready) {
        ack->set_netaware_feedback(state.levels);
        state.feedbacks++;
        if (packet_ready)
            state.packet_feedbacks++;
        else
            state.time_feedbacks++;
        state.feedback_packets_sum += state.packets;
        state.feedback_zero_bits += zero_bits;
        if (netaware_all_good(state.levels))
            state.all_good_feedbacks++;
        state.packets = 0;
        state.last_feedback = now;
    }
}

StorFeedbackLevels FatTreeSwitch::stor_feedback_after_signal(uint32_t src_host,
                                                             uint32_t peer_host,
                                                             uint32_t pathid,
                                                             uint32_t path_count,
                                                             StorSignal signal) {
    (void)src_host;
    if (path_count == 0)
        path_count = 1;

    uint32_t peer_key = peer_host;
    if (_ft && peer_host != UINT32_MAX)
        peer_key = _ft->HOST_POD_SWITCH(peer_host);

    StorState& state = _stor_states[peer_key];
    if (state.evs.size() != path_count) {
        state.evs.assign(path_count, StorEvState());
        if (_stor_score_profile == STOR_SCORE_PROFILE_SIMPLE) {
            for (uint32_t i = 0; i < state.evs.size(); i++) {
                state.evs[i].score = _stor_simple_max_score;
                state.evs[i].min_score_seen = _stor_simple_max_score;
            }
        }
        state.packets = 0;
        state.last_feedback = 0;
        state.signals_seen = 0;
        state.dirty = false;
    }

    simtime_picosec now = eventlist().now();
    if (_stor_score_profile != STOR_SCORE_PROFILE_BINARY &&
        _stor_aging_profile != STOR_AGING_PACKET)
        stor_apply_time_aging(state, now);

    uint32_t ev = pathid % path_count;
    StorEvState& ev_state = state.evs[ev];
    uint8_t old_level = stor_level_from_score(ev_state.score);
    stor_apply_signal(ev_state, signal);
    ev_state.last_update = now;
    if (signal == STOR_SIGNAL_CLEAN) {
        ev_state.clean_signals++;
        if (_stor_aging_profile == STOR_AGING_HYBRID &&
            (old_level == STOR_LEVEL_AVOID || old_level == STOR_LEVEL_BAD)) {
            ev_state.probe_clean_streak++;
            if (ev_state.probe_clean_streak >= _stor_hybrid_probe_clean_promote) {
                if (old_level == STOR_LEVEL_AVOID && ev_state.score < _stor_bad_threshold)
                    ev_state.score = _stor_bad_threshold;
                else if (old_level == STOR_LEVEL_BAD &&
                         ev_state.score < _stor_degraded_threshold)
                    ev_state.score = _stor_degraded_threshold;
                ev_state.last_probe_packet = state.signals_seen;
                ev_state.last_probe_time = now;
                ev_state.probe_clean_streak = 0;
            }
        }
    } else if (signal == STOR_SIGNAL_ECN) {
        ev_state.ecn_signals++;
        ev_state.last_bad_time = now;
        ev_state.has_bad_time = true;
        ev_state.probe_clean_streak = 0;
    } else {
        ev_state.trim_signals++;
        ev_state.last_bad_time = now;
        ev_state.has_bad_time = true;
        ev_state.probe_clean_streak = 0;
    }
    if (ev_state.score < ev_state.min_score_seen)
        ev_state.min_score_seen = ev_state.score;
    stor_note_level_transition(ev_state, old_level);
    state.signals_seen++;
    state.packets++;
    state.dirty = true;

    uint32_t feedback_pkts = _stor_feedback_pkts ? _stor_feedback_pkts : 1;
    bool packet_trigger = state.packets >= feedback_pkts;
    bool trim_trigger = signal == STOR_SIGNAL_TRIM && _stor_feedback_on_trim &&
                        _stor_score_profile != STOR_SCORE_PROFILE_BINARY;
    bool time_trigger = state.dirty &&
                        now - state.last_feedback >= _stor_feedback_max_interval;
    bool min_elapsed = now - state.last_feedback >= _stor_feedback_min_interval;
    bool trim_min_elapsed =
        now - state.last_feedback >= _stor_trim_feedback_min_interval;

    if (!((packet_trigger && min_elapsed) ||
          (trim_trigger && trim_min_elapsed) ||
          time_trigger))
        return StorFeedbackLevels();

    StorFeedbackLevels levels;
    levels.reserve(path_count);
    for (uint32_t i = 0; i < path_count; i++)
        levels.push_back(stor_level_from_score(state.evs[i].score));
    if (_stor_score_profile == STOR_SCORE_PROFILE_BINARY) {
        for (uint32_t i = 0; i < path_count; i++) {
            state.evs[i].score = 255;
            state.evs[i].ecn_acc = 0;
            state.evs[i].trim_acc = 0;
        }
    }
    state.packets = 0;
    state.last_feedback = now;
    state.dirty = false;
    return levels;
}

void FatTreeSwitch::collect_stor_diag(uint8_t& min_score,
                                      uint64_t& avoid_entries,
                                      uint64_t& avoid_exits,
                                      uint64_t& clean_signals,
                                      uint64_t& ecn_signals,
                                      uint64_t& trim_signals) const {
    for (auto const& peer : _stor_states) {
        for (auto const& ev : peer.second.evs) {
            if (ev.min_score_seen < min_score)
                min_score = ev.min_score_seen;
            avoid_entries += ev.avoid_entries;
            avoid_exits += ev.avoid_exits;
            clean_signals += ev.clean_signals;
            ecn_signals += ev.ecn_signals;
            trim_signals += ev.trim_signals;
        }
    }
}

void FatTreeSwitch::collect_netaware_diag(uint64_t& samples,
                                      uint64_t& sample_zero_bits,
                                      uint64_t& feedbacks,
                                      uint64_t& packet_feedbacks,
                                      uint64_t& time_feedbacks,
                                      uint64_t& feedback_packets_sum,
                                      uint64_t& feedback_zero_bits,
                                      uint64_t& all_good_feedbacks) const {
    for (auto const& peer : _netaware_states) {
        samples += peer.second.samples;
        sample_zero_bits += peer.second.sample_zero_bits;
        feedbacks += peer.second.feedbacks;
        packet_feedbacks += peer.second.packet_feedbacks;
        time_feedbacks += peer.second.time_feedbacks;
        feedback_packets_sum += peer.second.feedback_packets_sum;
        feedback_zero_bits += peer.second.feedback_zero_bits;
        all_good_feedbacks += peer.second.all_good_feedbacks;
    }
}

const FatTreeSwitch::StorEvState* FatTreeSwitch::stor_state_for_test(
        uint32_t peer_host, uint32_t pathid) const {
    uint32_t peer_key = peer_host;
    if (_ft && peer_host != UINT32_MAX)
        peer_key = _ft->HOST_POD_SWITCH(peer_host);
    auto it = _stor_states.find(peer_key);
    if (it == _stor_states.end() || it->second.evs.empty())
        return NULL;
    return &it->second.evs[pathid % it->second.evs.size()];
}

void FatTreeSwitch::stor_apply_time_aging_for_test(uint32_t peer_host,
                                                   simtime_picosec now) {
    uint32_t peer_key = peer_host;
    if (_ft && peer_host != UINT32_MAX)
        peer_key = _ft->HOST_POD_SWITCH(peer_host);
    auto it = _stor_states.find(peer_key);
    if (it == _stor_states.end())
        return;
    stor_apply_time_aging(it->second, now);
}

void FatTreeSwitch::maybe_update_stor_feedback(Packet& pkt) {
    if (!_stor_enabled || _type != TOR || !_ft)
        return;
    if (pkt.dst() == UINT32_MAX || _ft->HOST_POD_SWITCH(pkt.dst()) != _id)
        return;

    uint32_t path_count = _stor_path_count ? _stor_path_count : 1;

    if (pkt.type() == ROCEACK) {
        RoceAck* ack = dynamic_cast<RoceAck*>(&pkt);
        if (!ack || ack->stor_peer() == UINT32_MAX)
            return;
        StorSignal signal = (ack->flags() & ECN_ECHO) ?
            STOR_SIGNAL_ECN : STOR_SIGNAL_CLEAN;
        StorFeedbackLevels levels =
            stor_feedback_after_signal(pkt.dst(), ack->stor_peer(),
                                       pkt.pathid(), path_count, signal);
        if (!levels.empty())
            ack->set_stor_feedback(levels);
        return;
    }

    if (pkt.type() == ROCENACK) {
        RoceNack* nack = dynamic_cast<RoceNack*>(&pkt);
        if (!nack || nack->stor_peer() == UINT32_MAX ||
            nack->reason() != RoceNack::TRIM) {
            return;
        }
        StorFeedbackLevels levels =
            stor_feedback_after_signal(pkt.dst(), nack->stor_peer(),
                                       pkt.pathid(), path_count,
                                       STOR_SIGNAL_TRIM);
        if (!levels.empty())
            nack->set_stor_feedback(levels);
    }
}

Route* FatTreeSwitch::getNextHop(Packet& pkt, BaseQueue* ingress_port){
    if (_strategy == SGLB ||
        (_nmrc_hybrid_enabled && pkt.type() == ROCE) ||
        (_strategy == PAPER_SGLB &&
         _paper_sglb_ablation ==
             PAPER_SGLB_ABLATION_LEGACY_REMOTE_SEMANTICS))
        sglb_maybe_refresh_export(pkt.dst());

    const bool leaf_routing =
        _ft && FatTreeTopology::get_tiers() == 2;
    const uint32_t destination_leaf =
        leaf_routing ? _ft->HOST_POD_SWITCH(pkt.dst()) : 0;
    const bool directly_connected =
        leaf_routing && _type == TOR && destination_leaf == _id;
    // Receiver-side CONGA processing.  The forward path estimate is retained
    // in FromLeaf; feedback piggybacked by the peer updates ToLeaf.  ACKs do
    // not carry CONGA feedback in the paper/ns-3 state machine.
    if (directly_connected && pkt.type() == ROCE) {
        RocePacket& data = (RocePacket&)pkt;
        if (data.lbtag() != UINT32_MAX && data.src() != UINT32_MAX) {
            const uint32_t source_leaf = _ft->HOST_POD_SWITCH(data.src());
            CongaPathInfo& learned =
                _conga_from_leaf[source_leaf][data.lbtag()];
            const bool changed = learned.metric != data.conga_metric();
            learned.metric = data.conga_metric();
            learned.updated = eventlist().now();
            learned.dirty = learned.dirty || changed;
            if (data.has_conga_feedback())
                _conga_to_leaf[source_leaf][data.conga_feedback_path()] =
                    CongaPathInfo(data.conga_feedback_metric(), eventlist().now());
        }
    }
    if (_sglb_ecn_mode != SGLB_ECN_OFF && directly_connected &&
        pkt.type() == ROCEACK) {
        RoceAck& ack = (RoceAck&)pkt;
        if (ack.flags() & ECN_ECHO) {
            if (!ack.has_sglb_tx_metadata() ||
                ack.sglb_source_leaf() != _id) {
                _sglb_ecn_missing_metadata++;
            } else {
                _sglb_ecn_total++;
                std::map<uint32_t, SglbCandidateState>::iterator state_it =
                    _sglb_candidate_states.find(
                        ack.sglb_destination_leaf());
                if (state_it != _sglb_candidate_states.end() &&
                    sglb_stale_excluded(
                        state_it->second,
                        ack.sglb_tx_candidate_epoch(),
                        ack.sglb_selected_path())) {
                    _sglb_ecn_stale++;
                    if (_sglb_ecn_mode == SGLB_ECN_NEUTRAL) {
                        ack.set_neutral_ecn(true);
                        _sglb_ecn_neutralized++;
                    } else if (_sglb_ecn_mode == SGLB_ECN_CLEAR) {
                        ack.set_flags(ack.flags() & ~ECN_ECHO);
                        _sglb_ecn_cleared++;
                    }
                }
            }
        }
    }
    vector<FibEntry*> * available_hops =
        leaf_routing && !directly_connected ?
            _fib->getLeafRoutes(destination_leaf) :
            _fib->getRoutes(pkt.dst());

    if (available_hops){
        if ((_strategy == PAPER_SGLB ||
             (_strategy == SGLB &&
              _sglb_ofat_factor == SGLB_OFAT_SHADOW_GCN)) &&
            _type == AGG &&
            !available_hops->empty() &&
            (*available_hops)[0]->getDirection() == DOWN)
            paper_sglb_observe_remote_on_lookup(
                _ft->HOST_POD_SWITCH(pkt.dst()));
        //implement a form of ECMP hashing; might need to revisit based on measured performance.
        uint32_t ecmp_choice = 0;
        vector<uint32_t> sglb_candidates;
        bool used_sglb_candidates = false;
        if (available_hops->size()>1)
            switch(_strategy){
            case NIX:
                abort();
            case ECMP:
                // CONGA is a source-leaf mechanism.  Decide its flowlet here,
                // after host-link serialization, so the decision observes the
                // local DRE state accumulated by earlier arrivals at this leaf.
                if (_type == TOR &&
                    (*available_hops)[0]->getDirection() == UP &&
                    pkt.type() == ROCE &&
                    ((RocePacket&)pkt).lbtag() != UINT32_MAX) {
                    RocePacket& data = (RocePacket&)pkt;
                    const simtime_picosec now = eventlist().now();
                    const simtime_picosec flowlet_gap = timeFromUs(500.0);
                    FlowletInfo* flowlet = NULL;
                    unordered_map<uint32_t, FlowletInfo*>::iterator flowlet_it =
                        _conga_flowlet_maps.find(pkt.flow_id());
                    if (flowlet_it != _conga_flowlet_maps.end())
                        flowlet = flowlet_it->second;
                    if (flowlet && now - flowlet->_last < flowlet_gap) {
                        ecmp_choice = flowlet->_egress % available_hops->size();
                        flowlet->_last = now;
                    } else {
                        std::vector<uint32_t> scores(available_hops->size(), 0);
                        const simtime_picosec period = timeFromUs(32.0);
                        const simtime_picosec tau = timeFromUs(160.0);
                        for (uint32_t i = 0; i < available_hops->size(); ++i) {
                            Route* route = (*available_hops)[i]->getEgressPort();
                            BaseQueue* queue = route && route->size() ?
                                dynamic_cast<BaseQueue*>(route->at(0)) : NULL;
                            uint32_t local = 0;
                            if (queue) {
                                CongaDreState& state = _conga_dre[queue];
                                if (!state.last_decay)
                                    state.last_decay = now;
                                if (now > state.last_decay) {
                                    const uint64_t periods = (now - state.last_decay) / period;
                                    if (periods) {
                                        state.bytes = conga_decay_dre(state.bytes, periods);
                                        state.last_decay += periods * period;
                                    }
                                }
                                local = conga_quantize_dre(
                                    state.bytes, queue->bitrate(), tau, 3);
                            }
                            uint32_t remote = 0;
                            unordered_map<uint32_t,
                                unordered_map<uint32_t,CongaPathInfo> >::iterator
                                leaf_it = _conga_to_leaf.find(destination_leaf);
                            if (leaf_it != _conga_to_leaf.end()) {
                                unordered_map<uint32_t,CongaPathInfo>::iterator
                                    path_it = leaf_it->second.find(i);
                                if (path_it != leaf_it->second.end()) {
                                    const simtime_picosec age =
                                        eventlist().now() - path_it->second.updated;
                                    remote = age > timeFromMs(10.0) ?
                                        0 : path_it->second.metric;
                                }
                            }
                            scores[i] = std::max(local, remote);
                        }
                        const uint32_t preferred = flowlet ? flowlet->_egress : UINT32_MAX;
                        uint32_t tie = (uint32_t)random();
                        ecmp_choice = conga_choose_uplink(scores, preferred, tie);
                        if (getenv("HTSIM_CONGA_DIAG")) {
                            cerr << "CongaDecision time_ps=" << now
                                 << " leaf=" << _id
                                 << " flow=" << pkt.flow_id()
                                 << " dst_leaf=" << destination_leaf
                                 << " scores=";
                            for (uint32_t i = 0; i < scores.size(); ++i) {
                                if (i) cerr << '/';
                                cerr << scores[i];
                            }
                            cerr << " selected=" << ecmp_choice
                                 << " preferred=" << preferred << endl;
                        }
                        if (flowlet) {
                            flowlet->_egress = ecmp_choice;
                            flowlet->_last = now;
                        } else {
                            _conga_flowlet_maps[pkt.flow_id()] =
                                new FlowletInfo(ecmp_choice, now);
                        }
                    }
                    // The feedback label must identify the source-leaf uplink
                    // that was actually selected, not the endpoint's stale
                    // preliminary choice.
                    pkt.set_pathid(ecmp_choice);
                    data.set_lbtag(ecmp_choice);
                    // Uniformly select one learned reverse-path estimate for
                    // piggyback, as ns-3 CongaRouting::RouteInput does.
                    unordered_map<uint32_t,
                        unordered_map<uint32_t,CongaPathInfo> >::iterator fb_leaf =
                        _conga_from_leaf.find(destination_leaf);
                    if (fb_leaf != _conga_from_leaf.end() &&
                        !fb_leaf->second.empty()) {
                        for (unordered_map<uint32_t,CongaPathInfo>::iterator it =
                                 fb_leaf->second.begin(); it != fb_leaf->second.end();) {
                            if (eventlist().now() - it->second.updated > timeFromMs(10.0))
                                it = fb_leaf->second.erase(it);
                            else
                                ++it;
                        }
                    }
                    if (fb_leaf != _conga_from_leaf.end() &&
                        !fb_leaf->second.empty()) {
                        uint32_t& cursor =
                            _conga_feedback_cursor[destination_leaf];
                        for (uint32_t tries = 0;
                             tries < fb_leaf->second.size(); ++tries) {
                            unordered_map<uint32_t,CongaPathInfo>::iterator fb =
                                fb_leaf->second.begin();
                            std::advance(fb, cursor % fb_leaf->second.size());
                            cursor++;
                            if (fb->second.dirty) {
                                data.set_conga_feedback(fb->first,
                                    fb->second.metric);
                                fb->second.dirty = false;
                                break;
                            }
                        }
                    }
                } else {
                    ecmp_choice = pathid_ecmp_choice(
                        pkt, available_hops->size(),
                        (*available_hops)[0]->getDirection());
                }
                ecmp_choice = nmrc_maybe_reroute(
                    pkt, available_hops, ecmp_choice);
                break;
            case ADAPTIVE_ROUTING:
                if (_ar_sticky==FatTreeSwitch::PER_PACKET){
                    ecmp_choice = adaptive_route(available_hops,fn); 
                } 
                else if (_ar_sticky==FatTreeSwitch::PER_FLOWLET){     
                    if (_flowlet_maps.find(pkt.flow_id())!=_flowlet_maps.end()){
                        FlowletInfo* f = _flowlet_maps[pkt.flow_id()];
                        
                        // only reroute an existing flow if its inter packet time is larger than _sticky_delta and
                        // and
                        // 50% chance happens. 
                        // and (commented out) if the switch has not taken any other placement decision that we've not seen the effects of.
                        if (eventlist().now() - f->_last > _sticky_delta && /*eventlist().now() - _last_choice > _pipe->delay() + BaseQueue::_update_period  &&*/ random()%2==0){ 
                            //cout << "AR 1 " << timeAsUs(eventlist().now()) << endl;
                            uint32_t new_route = adaptive_route(available_hops,fn); 
                            if (fn(available_hops->at(f->_egress),available_hops->at(new_route)) < 0){
                                f->_egress = new_route;
                                _last_choice = eventlist().now();
                                //cout << "Switch " << _type << ":" << _id << " choosing new path "<<  f->_egress << " for " << pkt.flow_id() << " at " << timeAsUs(eventlist().now()) << " last is " << timeAsUs(f->_last) << endl;
                            }
                        }
                        ecmp_choice = f->_egress;

                        f->_last = eventlist().now();
                    }
                    else {
                        //cout << "AR 2 " << timeAsUs(eventlist().now()) << endl;
                        ecmp_choice = adaptive_route(available_hops,fn); 
                        _last_choice = eventlist().now();

                        _flowlet_maps[pkt.flow_id()] = new FlowletInfo(ecmp_choice,eventlist().now());
                    }
                }

                break;
            case ECMP_ADAPTIVE:
                ecmp_choice = freeBSDHash(pkt.flow_id(),pkt.pathid(),_hash_salt) % available_hops->size();
                if (random()%100 < 50)
                    ecmp_choice = replace_worst_choice(available_hops,fn, ecmp_choice);
                break;
            case RR:
                if (_crt_route>=5 * available_hops->size()){
                    _crt_route = 0;
                    permute_paths(available_hops);
                }
                ecmp_choice = _crt_route % available_hops->size();
                _crt_route ++;
                break;
            case RR_ECMP:
                if (_type == TOR){
                    if (_crt_route>=5 * available_hops->size()){
                        _crt_route = 0;
                        permute_paths(available_hops);
                    }
                    ecmp_choice = _crt_route % available_hops->size();
                    _crt_route ++;
                }
                else ecmp_choice = freeBSDHash(pkt.flow_id(),pkt.pathid(),_hash_salt) % available_hops->size();
                
                break;
            case SGLB:
                if (pkt.flow().background_traffic() &&
                    _sglb_ofat_factor != SGLB_OFAT_BACKGROUND_SGLB)
                    ecmp_choice = freeBSDHash(
                        pkt.flow_id(), pkt.pathid(), _hash_salt
                    ) % available_hops->size();
                else if (_sglb_ofat_factor == SGLB_OFAT_SOURCE_LEAF_ONLY &&
                         !(_type == TOR &&
                           (*available_hops)[0]->getDirection() == UP))
                    ecmp_choice = pathid_ecmp_choice(
                        pkt, available_hops->size(),
                        (*available_hops)[0]->getDirection());
                else {
                    ecmp_choice = sglb_route(
                        available_hops, pkt.dst(), &sglb_candidates);
                    used_sglb_candidates = true;
                }
                break;
            case PAPER_SGLB:
                if (pkt.flow().background_traffic() &&
                    _paper_sglb_ablation ==
                        PAPER_SGLB_ABLATION_LEGACY_BACKGROUND) {
                    ecmp_choice = freeBSDHash(
                        pkt.flow_id(), pkt.pathid(), _hash_salt) %
                        available_hops->size();
                } else if (_type == TOR &&
                    (*available_hops)[0]->getDirection() == UP) {
                    ecmp_choice = paper_sglb_route(available_hops, pkt);
                } else if (_paper_sglb_ablation ==
                               PAPER_SGLB_ABLATION_ALL_SWITCH_DECISIONS) {
                    ecmp_choice = paper_sglb_route(available_hops, pkt);
                } else {
                    ecmp_choice = pathid_ecmp_choice(
                        pkt, available_hops->size(),
                        (*available_hops)[0]->getDirection());
                }
                break;
            case DRILL:
                ecmp_choice = drill_route(available_hops, pkt.dst());
                break;
            }
        
        FibEntry* e = (*available_hops)[ecmp_choice];
        pkt.set_direction(e->getDirection());
        if (used_sglb_candidates && _type == TOR &&
            e->getDirection() == UP && pkt.type() == ROCE) {
            SglbCandidateState& state =
                _sglb_candidate_states[destination_leaf];
            sglb_update_candidate_state(
                state, sglb_candidates, eventlist().now());
            ((RocePacket&)pkt).set_sglb_tx_metadata(
                ecmp_choice, state.epoch, _id, destination_leaf);
        }
        
        return e->getEgressPort();
    }

    //no route table entries for this destination. Add them to FIB or fail. 
    if (_type == TOR){
        if ( _ft->HOST_POD_SWITCH(pkt.dst()) == _id) { 
            //this host is directly connected!
            HostFibEntry* fe = _fib->getHostRoute(pkt.dst(),pkt.flow_id());
            assert(fe);
            pkt.set_direction(DOWN);
            maybe_update_netaware_feedback(pkt);
            maybe_update_stor_feedback(pkt);
            return fe->getEgressPort();
        } else {
            //route packet up!
            if (_uproutes)
                if (leaf_routing)
                    _fib->setLeafRoutes(destination_leaf, _uproutes);
                else
                    _fib->setRoutes(pkt.dst(),_uproutes);
            else {
                uint32_t podid,agg_min,agg_max;

                if (_ft->get_tiers()==3) {
                    podid = _id / _ft->tor_switches_per_pod();
                    agg_min = _ft->MIN_POD_AGG_SWITCH(podid);
                    agg_max = _ft->MAX_POD_AGG_SWITCH(podid);
                }
                else {
                    agg_min = 0;
                    agg_max = _ft->getNAGG()-1;
                }

                for (uint32_t k=agg_min; k<=agg_max;k++){
                    for (uint32_t b = 0; b < _ft->bundlesize(AGG_TIER); b++) {
                        Route * r = new Route();
                        r->push_back(_ft->queues_nlp_nup[_id][k][b]);
                        assert(((BaseQueue*)r->at(0))->getSwitch() == this);

                        r->push_back(_ft->pipes_nlp_nup[_id][k][b]);
                        r->push_back(_ft->queues_nlp_nup[_id][k][b]->getRemoteEndpoint());
                        if (leaf_routing)
                            _fib->addLeafRoute(destination_leaf, r, 1, UP);
                        else
                            _fib->addRoute(pkt.dst(),r,1,UP);
                    }

                    /*
                      FatTreeSwitch* next = (FatTreeSwitch*)_ft->queues_nlp_nup[_id][k]->getRemoteEndpoint();
                      assert (next->getType()==AGG && next->getID() == k);
                    */
                }
                _uproutes = leaf_routing ?
                    _fib->getLeafRoutes(destination_leaf) :
                    _fib->getRoutes(pkt.dst());
                permute_paths(_uproutes);
            }
        }
    } else if (_type == AGG) {
        if ( _ft->get_tiers()==2 || _ft->HOST_POD(pkt.dst()) == _ft->AGG_SWITCH_POD_ID(_id)) {
            //must go down!
            //target NLP id is 2 * pkt.dst()/K
            uint32_t target_tor = _ft->HOST_POD_SWITCH(pkt.dst());
            for (uint32_t b = 0; b < _ft->bundlesize(AGG_TIER); b++) {
                Route * r = new Route();
                r->push_back(_ft->queues_nup_nlp[_id][target_tor][b]);
                assert(((BaseQueue*)r->at(0))->getSwitch() == this);

                r->push_back(_ft->pipes_nup_nlp[_id][target_tor][b]);          
                r->push_back(_ft->queues_nup_nlp[_id][target_tor][b]->getRemoteEndpoint());

                if (leaf_routing)
                    _fib->addLeafRoute(destination_leaf, r, 1, DOWN);
                else
                    _fib->addRoute(pkt.dst(),r,1, DOWN);
            }
        } else {
            //go up!
            if (_uproutes)
                _fib->setRoutes(pkt.dst(),_uproutes);
            else {
                uint32_t podpos = _id % _ft->agg_switches_per_pod();
                uint32_t uplink_bundles = _ft->radix_up(AGG_TIER) / _ft->bundlesize(CORE_TIER);
                for (uint32_t l = 0; l <  uplink_bundles ; l++) {
                    uint32_t core = l * _ft->agg_switches_per_pod() + podpos;
                    for (uint32_t b = 0; b < _ft->bundlesize(CORE_TIER); b++) {
                        BaseQueue* q = _ft->queues_nup_nc[_id][core][b];
                        Pipe* pipe = _ft->pipes_nup_nc[_id][core][b];
                        if (!q || !pipe)
                            continue;
                        Route *r = new Route();
                        r->push_back(q);
                        assert(((BaseQueue*)r->at(0))->getSwitch() == this);

                        r->push_back(pipe);
                        r->push_back(q->getRemoteEndpoint());

                        /*
                          FatTreeSwitch* next = (FatTreeSwitch*)_ft->queues_nup_nc[_id][k]->getRemoteEndpoint();
                          assert (next->getType()==CORE && next->getID() == k);
                        */
                    
                        _fib->addRoute(pkt.dst(),r,1,UP);

                        //cout << "AGG switch " << _id << " adding route to " << pkt.dst() << " via CORE " << k << " bundle_id " << b << endl;
                    }
                }
                //_uproutes = _fib->getRoutes(pkt.dst());
                permute_paths(_fib->getRoutes(pkt.dst()));
            }
        }
    } else if (_type == CORE) {
        uint32_t nup = _ft->MIN_POD_AGG_SWITCH(_ft->HOST_POD(pkt.dst())) + (_id % _ft->agg_switches_per_pod());
        for (uint32_t b = 0; b < _ft->bundlesize(CORE_TIER); b++) {
            BaseQueue* q = _ft->queues_nc_nup[_id][nup][b];
            Pipe* pipe = _ft->pipes_nc_nup[_id][nup][b];
            if (!q || !pipe)
                continue;
            Route *r = new Route();
            //cout << "CORE switch " << _id << " adding route to " << pkt.dst() << " via AGG " << nup << endl;

            r->push_back(q);
            assert(((BaseQueue*)r->at(0))->getSwitch() == this);

            r->push_back(pipe);

            r->push_back(q->getRemoteEndpoint());
            _fib->addRoute(pkt.dst(),r,1,DOWN);
        }
    }
    else {
        cerr << "Route lookup on switch with no proper type: " << _type << endl;
        abort();
    }
    if (leaf_routing && !directly_connected) {
        if (!_fib->getLeafRoutes(destination_leaf))
            return NULL;
    } else if (!_fib->getRoutes(pkt.dst()))
        return NULL;

    //FIB has been filled in; return choice. 
    return getNextHop(pkt, ingress_port);
};
