// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#ifndef ROCEPACKET_H
#define ROCEPACKET_H

#include <list>
#include <limits>
#include <vector>
#include "network.h"

typedef std::vector<uint8_t> StorFeedbackLevels;
typedef std::vector<uint8_t> NetawareFeedbackLevels;

enum StorLevel {
    STOR_LEVEL_GOOD = 0,
    STOR_LEVEL_DEGRADED = 1,
    STOR_LEVEL_BAD = 2,
    STOR_LEVEL_AVOID = 3
};

// NdpPacket and NdpAck are subclasses of Packet.
// They incorporate a packet database, to reuse packet objects that are no longer needed.
// Note: you never construct a new NdpPacket or NdpAck directly; 
// rather you use the static method newpkt() which knows to reuse old packets from the database.

#define VALUE_NOT_SET -1
//#define PULL_MAXPATHS 256 // maximum path ID that can be pulled

class RocePacket : public Packet {
 public:
    typedef uint64_t seq_t;

    // pseudo-constructor for a routeless packet - routing information
    // must be filled in later
    inline static RocePacket* newpkt(PacketFlow &flow, 
                                    seq_t seqno, int size, 
                                    bool retransmitted, 
                                    bool last_packet,
                                    uint32_t destination = UINT32_MAX) {
                RocePacket* p = _packetdb.allocPacket();
                p->set_attrs(flow, size+ACKSIZE, seqno+size-1); // The NDP sequence number is the first byte of the packet; I will ID the packet by its last byte.
                p->_type = ROCE;
                p->_is_header = false;
                p->_seqno = seqno;
                p->_retransmitted = retransmitted;
                p->_lbtag = UINT32_MAX;
                p->_conga_metric = 0;
                p->_conga_feedback_path = UINT32_MAX;
                p->_conga_feedback_metric = 0;
                p->_routing_entropy = UINT32_MAX;
                p->_reps_entropy = false;
                p->_last_packet = last_packet;
                p->_path_len = 0;
                p->_direction = NONE;
                p->_srcaddr = UINT32_MAX;
                p->_has_stor_feedback = false;
                p->_stor_feedback.clear();
                p->_stor_peer = UINT32_MAX;
                p->_mrc_ev = UINT32_MAX;
                p->_attempt_id = 0;
                p->_nmrc_detour = false;
                p->_nmrc_actual_egress = UINT32_MAX;
                p->_has_sglb_tx_metadata = false;
                p->_sglb_selected_path = UINT32_MAX;
                p->_sglb_tx_candidate_epoch = 0;
                p->_sglb_source_leaf = UINT32_MAX;
                p->_sglb_destination_leaf = UINT32_MAX;
                p->set_dst(destination);
                return p;
    }
  
    inline static RocePacket* newpkt(PacketFlow &flow, const Route &route, 
                                    seq_t seqno, int size, 
                                    bool retransmitted,
                                    bool last_packet,
                                    uint32_t destination = UINT32_MAX) {
                RocePacket* p = _packetdb.allocPacket();
                p->set_route(flow,route,size+ACKSIZE,seqno+size-1); // The NDP sequence number is the first byte of the packet; I will ID the packet by its last byte.
                p->_type = ROCE;
                p->_seqno = seqno;
                p->_is_header = false;
                p->_direction = NONE;        
                p->_retransmitted = retransmitted;
                p->_lbtag = UINT32_MAX;
                p->_conga_metric = 0;
                p->_conga_feedback_path = UINT32_MAX;
                p->_conga_feedback_metric = 0;
                p->_last_packet = last_packet;
                p->_routing_entropy = UINT32_MAX;
                p->_reps_entropy = false;
                p->_path_len = route.size();
                p->_srcaddr = UINT32_MAX;
                p->_has_stor_feedback = false;
                p->_stor_feedback.clear();
                p->_stor_peer = UINT32_MAX;
                p->_mrc_ev = UINT32_MAX;
                p->_attempt_id = 0;
                p->_nmrc_detour = false;
                p->_nmrc_actual_egress = UINT32_MAX;
                p->_has_sglb_tx_metadata = false;
                p->_sglb_selected_path = UINT32_MAX;
                p->_sglb_tx_candidate_epoch = 0;
                p->_sglb_source_leaf = UINT32_MAX;
                p->_sglb_destination_leaf = UINT32_MAX;
                p->set_dst(destination);
                return p;
    }
  
    void free() {_packetdb.freePacket(this);}
    virtual ~RocePacket(){}
    virtual inline void strip_payload() {
        Packet::strip_payload();
        _size = ACKSIZE;
    }
    
        inline seq_t seqno() const {return _seqno;}
    inline bool retransmitted() const {return _retransmitted;}
    inline void set_lbtag(uint32_t tag) {_lbtag = tag;}
    inline uint32_t lbtag() const {return _lbtag;}
    inline void set_conga_metric(uint8_t metric) {
        if (metric > _conga_metric) _conga_metric = metric;
    }
    inline uint8_t conga_metric() const {return _conga_metric;}
    inline void set_conga_feedback(uint32_t path, uint8_t metric) {
        _conga_feedback_path = path;
        _conga_feedback_metric = metric;
    }
    inline bool has_conga_feedback() const {
        return _conga_feedback_path != UINT32_MAX;
    }
    inline uint32_t conga_feedback_path() const {
        return _conga_feedback_path;
    }
    inline uint8_t conga_feedback_metric() const {
        return _conga_feedback_metric;
    }
    inline void set_routing_entropy(uint32_t value) {_routing_entropy = value;}
    inline uint32_t routing_entropy() const {return _routing_entropy;}
    inline void set_reps_entropy(bool value) {_reps_entropy = value;}
    inline bool reps_entropy() const {return _reps_entropy;}
    inline bool last_packet() const {return _last_packet;}
    inline simtime_picosec ts() const {return _ts;}
    inline void set_ts(simtime_picosec ts) {_ts = ts;}
    inline uint32_t src() const {return _srcaddr;}
    inline void set_src(uint32_t src) {_srcaddr = src;}
    inline uint32_t path_id() const {if (_pathid!=UINT32_MAX) return _pathid; else return _route->path_id();}
    inline void set_stor_feedback(const StorFeedbackLevels& levels) {
        _stor_feedback = levels;
        _has_stor_feedback = true;
    }
    inline bool has_stor_feedback() const {return _has_stor_feedback;}
    inline const StorFeedbackLevels& stor_feedback() const {return _stor_feedback;}
    inline void set_stor_peer(uint32_t peer) {_stor_peer = peer;}
    inline uint32_t stor_peer() const {return _stor_peer;}
    inline void set_mrc_ev(uint32_t ev) {_mrc_ev = ev;}
    inline uint32_t mrc_ev() const {return _mrc_ev;}
    inline bool has_mrc_ev() const {return _mrc_ev != UINT32_MAX;}
    inline void set_attempt_id(uint8_t attempt_id) {_attempt_id = attempt_id;}
    inline uint8_t attempt_id() const {return _attempt_id;}
    inline void set_nmrc_detour(bool detour) {_nmrc_detour = detour;}
    inline bool nmrc_detour() const {return _nmrc_detour;}
    inline void set_nmrc_actual_egress(uint32_t egress) {
        _nmrc_actual_egress = egress;
    }
    inline bool has_nmrc_actual_egress() const {
        return _nmrc_actual_egress != UINT32_MAX;
    }
    inline uint32_t nmrc_actual_egress() const {
        return _nmrc_actual_egress;
    }
    inline void set_sglb_tx_metadata(uint32_t selected_path, uint64_t epoch,
                                     uint32_t source_leaf,
                                     uint32_t destination_leaf) {
        _has_sglb_tx_metadata = true;
        _sglb_selected_path = selected_path;
        _sglb_tx_candidate_epoch = epoch;
        _sglb_source_leaf = source_leaf;
        _sglb_destination_leaf = destination_leaf;
    }
    inline bool has_sglb_tx_metadata() const {return _has_sglb_tx_metadata;}
    inline uint32_t sglb_selected_path() const {return _sglb_selected_path;}
    inline uint64_t sglb_tx_candidate_epoch() const {
        return _sglb_tx_candidate_epoch;
    }
    inline uint32_t sglb_source_leaf() const {return _sglb_source_leaf;}
    inline uint32_t sglb_destination_leaf() const {
        return _sglb_destination_leaf;
    }
    virtual PktPriority priority() const {return Packet::PRIO_LO;}
    const static int ACKSIZE=64;
 protected:
    seq_t _seqno;
    simtime_picosec _ts;
    bool _retransmitted;
    uint32_t _lbtag;
    uint8_t _conga_metric;
    uint32_t _conga_feedback_path;
    uint8_t _conga_feedback_metric;
    uint32_t _routing_entropy;
    bool _reps_entropy;
    bool _last_packet;  // set to true in the last packet in a flow.
    uint32_t _srcaddr;
    bool _has_stor_feedback;
    StorFeedbackLevels _stor_feedback;
    uint32_t _stor_peer;
    uint32_t _mrc_ev;
    uint8_t _attempt_id;
    bool _nmrc_detour;
    uint32_t _nmrc_actual_egress;
    bool _has_sglb_tx_metadata;
    uint32_t _sglb_selected_path;
    uint64_t _sglb_tx_candidate_epoch;
    uint32_t _sglb_source_leaf;
    uint32_t _sglb_destination_leaf;
    static PacketDB<RocePacket> _packetdb;
};

class RoceAck : public Packet {
 public:
    typedef RocePacket::seq_t seq_t;
  
    inline static RoceAck* newpkt(PacketFlow &flow, const Route &route, 
                                 seq_t ackno,
                                 uint32_t destination = UINT32_MAX) {
                RoceAck* p = _packetdb.allocPacket();
                p->set_route(flow,route,RocePacket::ACKSIZE,ackno);
                p->_type = ROCEACK;
                p->_is_header = true;
                p->_ackno = ackno;
                p->_path_len = 0;
                p->_direction = NONE;
                p->_has_stor_feedback = false;
                p->_stor_feedback.clear();
                p->_stor_peer = UINT32_MAX;
                p->_has_netaware_feedback = false;
                p->_netaware_feedback.clear();
                p->_netaware_peer = UINT32_MAX;
                p->_mrc_ev = UINT32_MAX;
                p->_duplicate_ack = false;
                p->_old_duplicate_ack = false;
                p->_has_delivered_psn = false;
                p->_lbtag = UINT32_MAX;
                p->_conga_metric = 0;
                p->_routing_entropy = UINT32_MAX;
                p->_delivered_psn = 0;
                p->_retransmitted_data = false;
                p->_has_sglb_tx_metadata = false;
                p->_sglb_selected_path = UINT32_MAX;
                p->_sglb_tx_candidate_epoch = 0;
                p->_sglb_source_leaf = UINT32_MAX;
                p->_sglb_destination_leaf = UINT32_MAX;
                p->_neutral_ecn = false;
                p->set_dst(destination);
                return p;
    }
  
    void free() {_packetdb.freePacket(this);}
    inline seq_t ackno() const {return _ackno;}
    inline simtime_picosec ts() const {return _ts;}
    inline void set_ts(simtime_picosec ts) {_ts = ts;}
    inline void set_stor_feedback(const StorFeedbackLevels& levels) {
        _stor_feedback = levels;
        _has_stor_feedback = true;
    }
    inline bool has_stor_feedback() const {return _has_stor_feedback;}
    inline const StorFeedbackLevels& stor_feedback() const {return _stor_feedback;}
    inline void set_stor_peer(uint32_t peer) {_stor_peer = peer;}
    inline uint32_t stor_peer() const {return _stor_peer;}
    inline void set_netaware_feedback(const NetawareFeedbackLevels& levels) {
        _netaware_feedback = levels;
        _has_netaware_feedback = true;
    }
    inline bool has_netaware_feedback() const {return _has_netaware_feedback;}
    inline const NetawareFeedbackLevels& netaware_feedback() const {return _netaware_feedback;}
    inline void set_netaware_peer(uint32_t peer) {_netaware_peer = peer;}
    inline uint32_t netaware_peer() const {return _netaware_peer;}
    inline void set_mrc_ev(uint32_t ev) {_mrc_ev = ev;}
    inline uint32_t mrc_ev() const {return _mrc_ev;}
    inline bool has_mrc_ev() const {return _mrc_ev != UINT32_MAX;}
    inline void set_duplicate_ack(bool duplicate = true) {
        _duplicate_ack = duplicate;
    }
    inline bool is_duplicate_ack() const {return _duplicate_ack;}
    inline void set_old_duplicate_ack(bool old_duplicate = true) {
        _old_duplicate_ack = old_duplicate;
        if (old_duplicate)
            _duplicate_ack = true;
    }
    inline bool is_old_duplicate_ack() const {return _old_duplicate_ack;}
    inline void set_delivered_psn(seq_t psn) {
        _delivered_psn = psn;
        _has_delivered_psn = true;
    }
    inline void set_retransmitted_data(bool value) {_retransmitted_data = value;}
    inline bool retransmitted_data() const {return _retransmitted_data;}
    inline bool has_delivered_psn() const {return _has_delivered_psn;}
    inline void set_lbtag(uint32_t tag) {_lbtag = tag;}
    inline uint32_t lbtag() const {return _lbtag;}
    inline void set_conga_metric(uint8_t metric) {_conga_metric = metric;}
    inline uint8_t conga_metric() const {return _conga_metric;}
    inline void set_routing_entropy(uint32_t value) {_routing_entropy = value;}
    inline uint32_t routing_entropy() const {return _routing_entropy;}
    inline seq_t delivered_psn() const {return _delivered_psn;}
    inline void copy_sglb_tx_metadata(const RocePacket& packet) {
        if (!packet.has_sglb_tx_metadata())
            return;
        _has_sglb_tx_metadata = true;
        _sglb_selected_path = packet.sglb_selected_path();
        _sglb_tx_candidate_epoch = packet.sglb_tx_candidate_epoch();
        _sglb_source_leaf = packet.sglb_source_leaf();
        _sglb_destination_leaf = packet.sglb_destination_leaf();
    }
    inline bool has_sglb_tx_metadata() const {return _has_sglb_tx_metadata;}
    inline uint32_t sglb_selected_path() const {return _sglb_selected_path;}
    inline uint64_t sglb_tx_candidate_epoch() const {
        return _sglb_tx_candidate_epoch;
    }
    inline uint32_t sglb_source_leaf() const {return _sglb_source_leaf;}
    inline uint32_t sglb_destination_leaf() const {
        return _sglb_destination_leaf;
    }
    inline void set_neutral_ecn(bool neutral) {_neutral_ecn = neutral;}
    inline bool neutral_ecn() const {return _neutral_ecn;}
    virtual PktPriority priority() const {return Packet::PRIO_HI;}

    virtual ~RoceAck(){}

 protected:
    seq_t _ackno;
    simtime_picosec _ts;
    bool _has_stor_feedback;
    StorFeedbackLevels _stor_feedback;
    uint32_t _stor_peer;
    bool _has_netaware_feedback;
    NetawareFeedbackLevels _netaware_feedback;
    uint32_t _netaware_peer;
    uint32_t _mrc_ev;
    bool _duplicate_ack;
    bool _old_duplicate_ack;
    bool _retransmitted_data;
    bool _has_delivered_psn;
    uint32_t _lbtag;
    uint8_t _conga_metric;
    uint32_t _routing_entropy;
    seq_t _delivered_psn;
    bool _has_sglb_tx_metadata;
    uint32_t _sglb_selected_path;
    uint64_t _sglb_tx_candidate_epoch;
    uint32_t _sglb_source_leaf;
    uint32_t _sglb_destination_leaf;
    bool _neutral_ecn;
    static PacketDB<RoceAck> _packetdb;
};


class RoceNack : public Packet {
 public:
    typedef RocePacket::seq_t seq_t;
    typedef enum {LOSS = 0, TRIM = 1, OOO = 2} nack_reason_t;

    inline static RoceNack* newpkt(PacketFlow &flow, const Route &route,
                                  seq_t ackno,
                                  uint32_t destination = UINT32_MAX,
                                  uint64_t sack_bitmap = 0,
                                  uint16_t sack_offset = 0,
                                  bool has_sack = false,
                                  uint64_t sack_bitmap_high = 0,
                                  seq_t sack_bitmap_start_psn = 0,
                                  uint16_t sack_bitmap_valid_length = 0,
                                  uint16_t sack_bitmap_encoded_bits = 64) {
                RoceNack* p = _packetdb.allocPacket();
                uint32_t packet_size = RocePacket::ACKSIZE;
                if (has_sack && sack_bitmap_encoded_bits > 64)
                    packet_size += sizeof(uint64_t);
                p->set_route(flow,route,packet_size,ackno);
                p->_type = ROCENACK;
                p->_is_header = true;
                p->_ackno = ackno;
                p->_sack_bitmap = sack_bitmap;
                p->_sack_bitmap_high = sack_bitmap_high;
                p->_sack_offset = sack_offset;
                p->_sack_bitmap_start_psn = sack_bitmap_start_psn ?
                    sack_bitmap_start_psn : ackno + 1;
                p->_sack_bitmap_valid_length = has_sack ?
                    sack_bitmap_valid_length : 0;
                p->_has_sack = has_sack;
                p->_reason = LOSS;
                p->_direction = NONE;
                p->_has_stor_feedback = false;
                p->_stor_feedback.clear();
                p->_stor_peer = UINT32_MAX;
                p->_mrc_ev = UINT32_MAX;
                p->_missing_psn = 0;
                p->_has_attempt_id = false;
                p->_attempt_id = 0;
                p->_nmrc_detour = false;
                p->_nmrc_actual_egress = UINT32_MAX;
                p->set_dst(destination);
                return p;
    }

    void free() {_packetdb.freePacket(this);}
    inline seq_t ackno() const {return _ackno;}
    inline seq_t apsn() const {return _ackno;}
    inline uint64_t sack_bitmap() const {return _sack_bitmap;}
    inline uint64_t sack_bitmap_low() const {return _sack_bitmap;}
    inline uint64_t sack_bitmap_high() const {return _sack_bitmap_high;}
    inline uint16_t sack_offset() const {return _sack_offset;}
    inline seq_t sack_bitmap_start_psn() const {return _sack_bitmap_start_psn;}
    inline uint16_t sack_bitmap_valid_length() const {return _sack_bitmap_valid_length;}
    inline bool has_sack() const {return _has_sack;}
    inline void set_sack_bitmap(uint64_t bitmap) {_sack_bitmap = bitmap;}
    inline void set_sack_bitmap_high(uint64_t bitmap) {_sack_bitmap_high = bitmap;}
    inline void set_sack_offset(uint16_t offset) {_sack_offset = offset;}
    inline void set_sack_bitmap_start_psn(seq_t psn) {_sack_bitmap_start_psn = psn;}
    inline void set_sack_bitmap_valid_length(uint16_t length) {_sack_bitmap_valid_length = length;}
    inline void set_has_sack(bool has_sack) {_has_sack = has_sack;}
    inline bool sack_bit(uint32_t bit) const {
        if (bit >= 128)
            return false;
        return bit < 64 ? (_sack_bitmap & (1ULL << bit)) :
            (_sack_bitmap_high & (1ULL << (bit - 64)));
    }
    inline nack_reason_t reason() const {return _reason;}
    inline void set_reason(nack_reason_t reason) {_reason = reason;}
    inline simtime_picosec ts() const {return _ts;}
    inline void set_ts(simtime_picosec ts) {_ts = ts;}
    inline void set_stor_feedback(const StorFeedbackLevels& levels) {
        _stor_feedback = levels;
        _has_stor_feedback = true;
    }
    inline bool has_stor_feedback() const {return _has_stor_feedback;}
    inline const StorFeedbackLevels& stor_feedback() const {return _stor_feedback;}
    inline void set_stor_peer(uint32_t peer) {_stor_peer = peer;}
    inline uint32_t stor_peer() const {return _stor_peer;}
    inline void set_mrc_ev(uint32_t ev) {_mrc_ev = ev;}
    inline uint32_t mrc_ev() const {return _mrc_ev;}
    inline bool has_mrc_ev() const {return _mrc_ev != UINT32_MAX;}
    inline void set_missing_psn(seq_t psn) {_missing_psn = psn;}
    inline seq_t missing_psn() const {return _missing_psn;}
    inline bool has_missing_psn() const {return _missing_psn != 0;}
    inline void set_attempt_id(uint8_t attempt_id) {
        _attempt_id = attempt_id;
        _has_attempt_id = true;
    }
    inline bool has_attempt_id() const {return _has_attempt_id;}
    inline uint8_t attempt_id() const {return _attempt_id;}
    inline void set_nmrc_detour(bool detour) {_nmrc_detour = detour;}
    inline bool nmrc_detour() const {return _nmrc_detour;}
    inline void set_nmrc_actual_egress(uint32_t egress) {
        _nmrc_actual_egress = egress;
    }
    inline bool has_nmrc_actual_egress() const {
        return _nmrc_actual_egress != UINT32_MAX;
    }
    inline uint32_t nmrc_actual_egress() const {
        return _nmrc_actual_egress;
    }
    virtual PktPriority priority() const {return Packet::PRIO_HI;}
  
    virtual ~RoceNack(){}

protected:
    seq_t _ackno;
    uint64_t _sack_bitmap;
    uint64_t _sack_bitmap_high;
    uint16_t _sack_offset;
    seq_t _sack_bitmap_start_psn;
    uint16_t _sack_bitmap_valid_length;
    bool _has_sack;
    nack_reason_t _reason;
    simtime_picosec _ts;
    bool _has_stor_feedback;
    StorFeedbackLevels _stor_feedback;
    uint32_t _stor_peer;
    uint32_t _mrc_ev;
    seq_t _missing_psn;
    bool _has_attempt_id;
    uint8_t _attempt_id;
    bool _nmrc_detour;
    uint32_t _nmrc_actual_egress;
    static PacketDB<RoceNack> _packetdb;
};

class RoceFastCnp : public Packet {
public:
    typedef RocePacket::seq_t seq_t;

    inline static RoceFastCnp* newpkt(
            PacketFlow& flow, const Route& route,
            uint32_t source_host, uint32_t ev, seq_t psn,
            uint32_t original_egress, uint32_t selected_egress,
            uint8_t original_level, uint8_t selected_level,
            uint32_t trigger_switch, simtime_picosec trigger_time,
            uint8_t attempt_id = 0,
            double original_score = std::numeric_limits<double>::quiet_NaN(),
            double selected_score = std::numeric_limits<double>::quiet_NaN(),
            double selected_gap = std::numeric_limits<double>::quiet_NaN(),
            uint64_t action_key = 0,
            bool need_endpoint_cooldown = true) {
        assert(ev <= 0xffffU);
        RoceFastCnp* p = _packetdb.allocPacket();
        p->set_route(flow, route, RocePacket::ACKSIZE, (packetid_t)psn);
        p->_type = ROCEFASTCNP;
        p->_is_header = true;
        p->_direction = NONE;
        p->_path_len = route.size();
        p->_source_host = source_host;
        p->_ev = (uint16_t)ev;
        p->_psn = psn;
        p->_original_egress = original_egress;
        p->_selected_egress = selected_egress;
        p->_original_level = original_level;
        p->_selected_level = selected_level;
        p->_trigger_switch = trigger_switch;
        p->_trigger_time = trigger_time;
        p->_attempt_id = attempt_id;
        p->_original_score = original_score;
        p->_selected_score = selected_score;
        p->_selected_gap = selected_gap;
        p->_action_key = action_key;
        p->_need_endpoint_cooldown = need_endpoint_cooldown;
        p->set_dst(source_host);
        p->set_pathid(UINT32_MAX);
        p->set_flags(0);
        return p;
    }

    void free() {_packetdb.freePacket(this);}
    inline uint32_t source_host() const {return _source_host;}
    inline uint32_t ev() const {return _ev;}
    inline seq_t psn() const {return _psn;}
    inline uint32_t original_egress() const {return _original_egress;}
    inline uint32_t selected_egress() const {return _selected_egress;}
    inline uint8_t original_level() const {return _original_level;}
    inline uint8_t selected_level() const {return _selected_level;}
    inline uint32_t trigger_switch() const {return _trigger_switch;}
    inline simtime_picosec trigger_time() const {return _trigger_time;}
    inline uint8_t attempt_id() const {return _attempt_id;}
    inline double original_score() const {return _original_score;}
    inline double selected_score() const {return _selected_score;}
    inline double selected_gap() const {return _selected_gap;}
    inline uint64_t action_key() const {return _action_key;}
    inline bool need_endpoint_cooldown() const {
        return _need_endpoint_cooldown;
    }
    virtual PktPriority priority() const {return Packet::PRIO_HI;}
    virtual ~RoceFastCnp() {}

protected:
    uint32_t _source_host;
    uint16_t _ev;
    seq_t _psn;
    uint32_t _original_egress;
    uint32_t _selected_egress;
    uint8_t _original_level;
    uint8_t _selected_level;
    uint32_t _trigger_switch;
    simtime_picosec _trigger_time;
    uint8_t _attempt_id;
    double _original_score;
    double _selected_score;
    double _selected_gap;
    uint64_t _action_key;
    bool _need_endpoint_cooldown;
    static PacketDB<RoceFastCnp> _packetdb;
};

class RoceEvProbe : public Packet {
public:
    inline static RoceEvProbe* newpkt(
            PacketFlow& flow, const Route& route, uint64_t request_id,
            uint32_t target_ev, bool response, bool success) {
        assert(target_ev <= 0xffffU);
        RoceEvProbe* p = _packetdb.allocPacket();
        p->set_route(flow, route, RocePacket::ACKSIZE,
                     (packetid_t)(request_id & 0xffffffffU));
        p->_type = ROCEEVPROBE;
        p->_is_header = true;
        p->_direction = NONE;
        p->_path_len = route.size();
        p->_request_id = request_id;
        p->_target_ev = (uint16_t)target_ev;
        p->_response = response;
        p->_success = success;
        p->set_pathid(UINT32_MAX);
        p->set_flags(0);
        return p;
    }

    void free() {_packetdb.freePacket(this);}
    inline uint64_t request_id() const {return _request_id;}
    inline uint32_t target_ev() const {return _target_ev;}
    inline bool response() const {return _response;}
    inline bool success() const {return _success;}
    virtual PktPriority priority() const {return Packet::PRIO_HI;}
    virtual ~RoceEvProbe() {}

protected:
    uint64_t _request_id;
    uint16_t _target_ev;
    bool _response;
    bool _success;
    static PacketDB<RoceEvProbe> _packetdb;
};


#endif
