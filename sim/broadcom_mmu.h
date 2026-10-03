#ifndef HTSIM_BROADCOM_MMU_H
#define HTSIM_BROADCOM_MMU_H

#include <algorithm>
#include <assert.h>
#include <stdint.h>
#include <unordered_map>
#include <unordered_set>

// Byte-accurate model of the Broadcom-style SwitchMmu used by the ns-3.19
// reference tree.  Port identifiers are assigned by the topology; PG/queue
// indices use the packet priority class.
class BroadcomMmu {
public:
    enum Admission { ADMIT, DROP_EGRESS, DROP_INGRESS };

    BroadcomMmu(uint64_t buffer_bytes, uint32_t mtu, double ingress_alpha,
                 double egress_alpha, uint64_t headroom_bytes)
        : _buffer_bytes(buffer_bytes), _mtu(mtu),
          _ingress_alpha(ingress_alpha), _egress_alpha(egress_alpha),
          _headroom_limit(headroom_bytes), _total_ingress(0) {
        for (unsigned i = 0; i < 4; ++i)
            _ingress_sp[i] = _egress_sp[i] = 0;
        recomputeLimits();
    }

    void registerIngressPort(uint32_t port) {
        _ingress_ports.insert(port);
        recomputeLimits();
    }
    void registerEgressPort(uint32_t port) {
        _egress_ports.insert(port);
        recomputeLimits();
    }

    Admission admit(uint32_t in_port, uint32_t pg, uint32_t out_port,
                    uint32_t queue, uint32_t bytes) {
        registerIngressPort(in_port);
        registerEgressPort(out_port);
        if (!checkEgress(out_port, queue, bytes)) return DROP_EGRESS;
        if (!checkIngress(in_port, pg, bytes)) return DROP_INGRESS;
        updateIngress(in_port, pg, bytes);
        updateEgress(out_port, queue, bytes);
        return ADMIT;
    }

    void release(uint32_t in_port, uint32_t pg, uint32_t out_port,
                 uint32_t queue, uint32_t bytes) {
        removeIngress(in_port, pg, bytes);
        removeEgress(out_port, queue, bytes);
    }

    bool shouldPause(uint32_t port, uint32_t pg) const {
        const uint64_t used = get(_ingress_pg, key(port, pg));
        if (used <= 2ULL * _mtu) return false;
        const uint64_t sp = _ingress_sp[ingressSp(pg)];
        const double remaining = _ingress_sp_limit > sp ?
            double(_ingress_sp_limit - sp) : 0.0;
        return double(used - 2ULL * _mtu) > _ingress_alpha * remaining ||
               get(_ingress_headroom, key(port, pg)) != 0;
    }

    bool shouldResume(uint32_t port, uint32_t pg) const {
        if (get(_ingress_headroom, key(port, pg)) != 0) return false;
        const uint64_t used = get(_ingress_pg, key(port, pg));
        const uint64_t sp = _ingress_sp[ingressSp(pg)];
        const uint64_t remaining = _ingress_sp_limit > sp + 16 ?
            _ingress_sp_limit - sp - 16 : 0;
        const int64_t shared = int64_t(used) - int64_t(2ULL * _mtu);
        return double(shared) < _ingress_alpha * double(remaining);
    }

    bool shouldMarkEcn(uint32_t port, uint32_t queue, uint64_t kmin,
                       uint64_t kmax, double sample01) const {
        if (queue == 0) return false;
        const uint64_t used = egressQueueSharedBytes(port, queue);
        if (used > kmax) return true;
        if (used <= kmin || kmin == kmax) return false;
        const double probability = double(used - kmin) / double(kmax - kmin);
        return sample01 < probability;
    }

    uint64_t totalIngressBytes() const { return _total_ingress; }
    uint64_t ingressPgBytes(uint32_t p, uint32_t q) const {
        return get(_ingress_pg, key(p, q));
    }
    uint64_t ingressHeadroomBytes(uint32_t p, uint32_t q) const {
        return get(_ingress_headroom, key(p, q));
    }
    uint64_t ingressServicePoolBytes(uint32_t sp) const { return _ingress_sp[sp]; }
    uint64_t pauseThresholdBytes(uint32_t pg) const {
        const uint64_t sp = _ingress_sp[ingressSp(pg)];
        const double remaining = _ingress_sp_limit > sp ?
            double(_ingress_sp_limit - sp) : 0.0;
        return 2ULL * _mtu + (uint64_t)(_ingress_alpha * remaining);
    }
    uint64_t egressServicePoolBytes(uint32_t sp) const { return _egress_sp[sp]; }
    uint64_t egressQueueMinBytes(uint32_t p, uint32_t q) const {
        return get(_egress_min, key(p, q));
    }
    uint64_t egressQueueSharedBytes(uint32_t p, uint32_t q) const {
        return get(_egress_shared, key(p, q));
    }

private:
    static uint64_t key(uint32_t port, uint32_t pg) {
        return (uint64_t(port) << 32) | pg;
    }
    template <typename K>
    static uint64_t get(const std::unordered_map<K, uint64_t>& map, K k) {
        typename std::unordered_map<K, uint64_t>::const_iterator it = map.find(k);
        return it == map.end() ? 0 : it->second;
    }
    static uint32_t ingressSp(uint32_t pg) { return pg == 1 ? 1 : 0; }
    static uint32_t egressSp(uint32_t q) { return q == 0 ? 0 : 1; }

    void recomputeLimits() {
        const uint64_t ports = std::max(_ingress_ports.size(), _egress_ports.size());
        const uint64_t reserved = ports * std::max<uint64_t>(8ULL * _mtu, _mtu);
        const uint64_t headroom = ports * _headroom_limit;
        _ingress_sp_limit = _buffer_bytes > reserved + headroom ?
            _buffer_bytes - reserved - headroom : 0;
        _egress_sp_limit = _buffer_bytes > reserved ? _buffer_bytes - reserved : 0;
    }

    bool checkIngress(uint32_t p, uint32_t pg, uint32_t bytes) const {
        if (_total_ingress + bytes > _buffer_bytes) return false;
        if (ingressPgBytes(p, pg) + bytes > _mtu &&
            get(_ingress_port, p) + bytes > _mtu &&
            _ingress_sp[ingressSp(pg)] > _ingress_sp_limit &&
            ingressHeadroomBytes(p, pg) + bytes > _headroom_limit)
            return false;
        return true;
    }

    bool checkEgress(uint32_t p, uint32_t q, uint32_t bytes) const {
        const uint32_t sp = egressSp(q);
        if (_egress_sp[sp] + bytes > _egress_sp_limit) return false;
        if (get(_egress_port, p) + bytes > _buffer_bytes) return false;
        if (egressQueueSharedBytes(p, q) + bytes > _buffer_bytes) return false;
        const double remaining = _egress_sp_limit > _egress_sp[sp] ?
            double(_egress_sp_limit - _egress_sp[sp]) : 0.0;
        return double(egressQueueSharedBytes(p, q) + bytes) <=
               _egress_alpha * remaining;
    }

    void updateIngress(uint32_t p, uint32_t pg, uint32_t bytes) {
        _total_ingress += bytes;
        _ingress_port[p] += bytes;
        _ingress_pg[key(p, pg)] += bytes;
        _ingress_sp[ingressSp(pg)] += bytes;
        if (_ingress_sp[ingressSp(pg)] > _ingress_sp_limit)
            _ingress_headroom[key(p, pg)] += bytes;
    }

    void updateEgress(uint32_t p, uint32_t q, uint32_t bytes) {
        const uint64_t k = key(p, q);
        const uint64_t qmin = 1ULL + _mtu;
        if (_egress_min[k] + bytes < qmin) {
            _egress_min[k] += bytes;
            _egress_port[p] += bytes;
            return;
        }
        if (_egress_min[k] != qmin) {
            const uint64_t shared = bytes + _egress_min[k] - qmin;
            _egress_shared[k] += shared;
            _egress_sp[egressSp(q)] += shared;
            _egress_min[k] = qmin;
        } else {
            _egress_shared[k] += bytes;
            _egress_sp[egressSp(q)] += bytes;
        }
        _egress_port[p] += bytes;
    }

    void removeIngress(uint32_t p, uint32_t pg, uint32_t bytes) {
        const uint64_t k = key(p, pg);
        assert(_total_ingress >= bytes && _ingress_port[p] >= bytes &&
               _ingress_pg[k] >= bytes && _ingress_sp[ingressSp(pg)] >= bytes);
        _total_ingress -= bytes;
        _ingress_port[p] -= bytes;
        _ingress_pg[k] -= bytes;
        _ingress_sp[ingressSp(pg)] -= bytes;
        uint64_t& hdr = _ingress_headroom[k];
        hdr = hdr > bytes ? hdr - bytes : 0;
    }

    void removeEgress(uint32_t p, uint32_t q, uint32_t bytes) {
        const uint64_t k = key(p, q);
        const uint64_t qmin = 1ULL + _mtu;
        assert(_egress_port[p] >= bytes);
        if (_egress_min[k] < qmin) {
            assert(_egress_min[k] >= bytes);
            _egress_min[k] -= bytes;
        } else if (_egress_shared[k] < bytes) {
            const uint64_t from_shared = _egress_shared[k];
            assert(_egress_min[k] + from_shared >= bytes);
            _egress_min[k] = _egress_min[k] + from_shared - bytes;
            _egress_sp[egressSp(q)] -= from_shared;
            _egress_shared[k] = 0;
        } else {
            _egress_shared[k] -= bytes;
            _egress_sp[egressSp(q)] -= bytes;
        }
        _egress_port[p] -= bytes;
    }

    uint64_t _buffer_bytes;
    uint32_t _mtu;
    double _ingress_alpha;
    double _egress_alpha;
    uint64_t _headroom_limit;
    uint64_t _ingress_sp_limit;
    uint64_t _egress_sp_limit;
    uint64_t _total_ingress;
    uint64_t _ingress_sp[4];
    uint64_t _egress_sp[4];
    std::unordered_set<uint32_t> _ingress_ports;
    std::unordered_set<uint32_t> _egress_ports;
    std::unordered_map<uint32_t, uint64_t> _ingress_port;
    std::unordered_map<uint64_t, uint64_t> _ingress_pg;
    std::unordered_map<uint64_t, uint64_t> _ingress_headroom;
    std::unordered_map<uint32_t, uint64_t> _egress_port;
    std::unordered_map<uint64_t, uint64_t> _egress_min;
    std::unordered_map<uint64_t, uint64_t> _egress_shared;
};

#endif
