#ifndef PFC_PRETRIM_V7_PROTOCOLS_H
#define PFC_PRETRIM_V7_PROTOCOLS_H

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <deque>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <vector>

namespace pretrim {

inline bool trim_induced_ce_allowed(bool enabled, bool payload_trimmed,
                                    bool effective_pause_gate, bool ecn_capable) {
    return enabled && payload_trimmed && effective_pause_gate && ecn_capable;
}

inline uint64_t live_pacing_eligible_ps(uint64_t last_tx_ps, uint64_t now_ps,
                                       double rate_gbps, uint32_t wire_bytes) {
    if (!last_tx_ps) return now_ps;
    const uint64_t interval = (uint64_t)std::ceil(wire_bytes * 8000.0 / rate_gbps);
    return std::max(now_ps, last_tx_ps + interval);
}

inline bool nack_can_retire_attempt(bool outstanding, uint64_t feedback_attempt,
                                    uint64_t latest_attempt) {
    return outstanding && feedback_attempt == latest_attempt;
}

class RejectedFeedbackDeduper {
public:
    bool should_send(uint64_t missing_psn, const std::string& reason,
                     uint64_t rejected_psn, uint32_t rejected_attempt) {
        return sent_.insert(std::make_tuple(missing_psn, reason, rejected_psn,
                                            rejected_attempt)).second;
    }
private:
    std::set<std::tuple<uint64_t, std::string, uint64_t, uint32_t> > sent_;
};

class SharedRxBudget {
public:
    explicit SharedRxBudget(uint32_t entries) : capacity_(entries), used_(0) {}
    bool acquire() {
        if (used_ == capacity_) return false;
        ++used_;
        return true;
    }
    void release() { if (used_) --used_; }
    uint32_t used() const { return used_; }
    uint32_t capacity() const { return capacity_; }
private:
    uint32_t capacity_;
    uint32_t used_;
};

class RxPayloadCapacity {
public:
    explicit RxPayloadCapacity(uint64_t bytes)
        : capacity_(bytes), used_(0), peak_(0), rejects_(0) {}
    bool acquire(uint64_t bytes) {
        if (bytes > capacity_ - used_) { ++rejects_; return false; }
        used_ += bytes;
        peak_ = std::max(peak_, used_);
        return true;
    }
    void release(uint64_t bytes) {
        used_ = bytes >= used_ ? 0 : used_ - bytes;
    }
    uint64_t capacity() const { return capacity_; }
    uint64_t used() const { return used_; }
    uint64_t peak() const { return peak_; }
    uint64_t rejects() const { return rejects_; }
private:
    uint64_t capacity_, used_, peak_, rejects_;
};

struct ControlServiceTicket {
    bool accepted;
    uint64_t enqueue_ns;
    uint64_t start_ns;
    uint64_t finish_ns;
};

class ControlServerSchedule {
public:
    ControlServerSchedule(uint32_t servers, uint64_t service_ns,
                          uint32_t waiting_capacity)
        : service_ns_(service_ns), waiting_capacity_(waiting_capacity),
          accepted_(0), dropped_(0), peak_in_system_(0) {
        next_free_.assign(std::max<uint32_t>(1, servers), 0);
    }
    ControlServiceTicket enqueue(uint64_t now_ns) {
        while (!finish_times_.empty() && finish_times_.front() <= now_ns)
            finish_times_.pop_front();
        if (finish_times_.size() >= next_free_.size() + waiting_capacity_) {
            ++dropped_;
            return ControlServiceTicket{false, now_ns, 0, 0};
        }
        size_t server = 0;
        for (size_t i = 1; i < next_free_.size(); ++i)
            if (next_free_[i] < next_free_[server]) server = i;
        const uint64_t start = std::max(now_ns, next_free_[server]);
        const uint64_t finish = start + service_ns_;
        next_free_[server] = finish;
        std::deque<uint64_t>::iterator pos = std::upper_bound(
            finish_times_.begin(), finish_times_.end(), finish);
        finish_times_.insert(pos, finish);
        ++accepted_;
        peak_in_system_ = std::max<uint64_t>(peak_in_system_, finish_times_.size());
        return ControlServiceTicket{true, now_ns, start, finish};
    }
    uint64_t accepted() const { return accepted_; }
    uint64_t dropped() const { return dropped_; }
    uint64_t peak_in_system() const { return peak_in_system_; }
    uint32_t servers() const { return static_cast<uint32_t>(next_free_.size()); }
    uint64_t service_ns() const { return service_ns_; }
    uint32_t waiting_capacity() const { return waiting_capacity_; }
private:
    uint64_t service_ns_;
    uint32_t waiting_capacity_;
    std::vector<uint64_t> next_free_;
    std::deque<uint64_t> finish_times_;
    uint64_t accepted_, dropped_, peak_in_system_;
};

enum class RxResult {
    CONTIGUOUS,
    OUT_OF_ORDER,
    DUPLICATE,
    OLD,
    DISTANCE_OVERFLOW,
    RESOURCE_OVERFLOW,
    CAPACITY_OVERFLOW
};

class BoundedRxWindow {
public:
    BoundedRxWindow(uint64_t distance, SharedRxBudget& budget)
        : distance_(distance), budget_(budget), next_(0) {}

    RxResult accept_data(uint64_t psn, RxPayloadCapacity* payload = 0,
                         uint64_t payload_bytes = 0) {
        if (psn < next_) return RxResult::OLD;
        if (psn == next_) {
            if (payload && !payload->acquire(payload_bytes))
                return RxResult::CAPACITY_OVERFLOW;
            ++next_;
            while (ooo_.erase(next_)) {
                budget_.release();
                ++next_;
            }
            return RxResult::CONTIGUOUS;
        }
        if (ooo_.count(psn)) return RxResult::DUPLICATE;
        if (psn - next_ >= distance_) return RxResult::DISTANCE_OVERFLOW;
        if (!budget_.acquire()) return RxResult::RESOURCE_OVERFLOW;
        if (payload && !payload->acquire(payload_bytes)) {
            budget_.release();
            return RxResult::CAPACITY_OVERFLOW;
        }
        ooo_.insert(psn);
        return RxResult::OUT_OF_ORDER;
    }

    uint64_t next_expected() const { return next_; }
    uint64_t max_distance() const { return distance_; }
    uint32_t occupancy() const { return static_cast<uint32_t>(ooo_.size()); }

private:
    uint64_t distance_;
    SharedRxBudget& budget_;
    uint64_t next_;
    std::set<uint64_t> ooo_;
};

struct RejectedPacket {
    uint64_t psn;
    uint64_t sequence;
    uint64_t attempt;
};

// Sender-side state for the explicitly named finite-RX-window recovery
// extension used by GLB/REPS.  Callers may populate it only when a real
// reverse-path rejection feedback packet arrives.
class FiniteRxRecovery {
public:
    explicit FiniteRxRecovery(uint64_t distance)
        : distance_(distance ? distance : 1), cumulative_ack_(0) {}

    void note_rejected_feedback(uint64_t psn, uint64_t sequence,
                                uint64_t attempt) {
        std::map<uint64_t, RejectedPacket>::iterator it = rejected_.find(psn);
        if (it == rejected_.end() || attempt >= it->second.attempt)
            rejected_[psn] = RejectedPacket{psn, sequence, attempt};
    }

    std::vector<RejectedPacket> advance_cumulative_ack(uint64_t cumulative) {
        cumulative_ack_ = std::max(cumulative_ack_, cumulative);
        std::vector<RejectedPacket> ready;
        for (std::map<uint64_t, RejectedPacket>::iterator it = rejected_.begin();
             it != rejected_.end();) {
            const RejectedPacket item = it->second;
            if (item.sequence < cumulative_ack_ ||
                item.sequence - cumulative_ack_ < distance_) {
                ready.push_back(item);
                it = rejected_.erase(it);
            } else {
                ++it;
            }
        }
        return ready;
    }

    void note_acked(uint64_t psn) { rejected_.erase(psn); }
    uint64_t cumulative_ack() const { return cumulative_ack_; }
    size_t pending() const { return rejected_.size(); }

private:
    uint64_t distance_;
    uint64_t cumulative_ack_;
    std::map<uint64_t, RejectedPacket> rejected_;
};

class MpRdmaState {
public:
    MpRdmaState(double initial_cwnd, uint32_t paths, uint64_t ooo_delta)
        : cwnd_(initial_cwnd), paths_(paths ? paths : 1), delta_(ooo_delta),
          highest_delivered_(0), ack_credit_(0), snd_nxt_(0), snd_una_(0),
          inflate_(0), recovery_active_(false), snd_retx_(0),
          recovery_boundary_(0), pruned_(paths_, false) {}

    void note_trim_header(uint64_t, bool, uint32_t) {
        // A trim header is congestion/missing/path feedback, never delivery.
    }

    void note_trim_congestion(bool ce) {
        if (ce) cwnd_ = std::max(1.0, cwnd_ - 0.5);
    }

    void note_sent(uint64_t sequence) {
        snd_nxt_ = std::max(snd_nxt_, sequence + 1);
    }

    void note_data_ack(uint64_t delivered_psn, bool ce, uint32_t path,
                       bool retransmitted,
                       uint64_t cumulative_ack = std::numeric_limits<uint64_t>::max(), bool update_cc = true) {
        path %= paths_;
        const bool slow_path = !retransmitted && highest_delivered_ > delta_ &&
                               delivered_psn + delta_ < highest_delivered_;
        acked_.insert(delivered_psn);
        if (delivered_psn >= snd_una_) ++inflate_;
        uint64_t new_una = snd_una_;
        if (cumulative_ack != std::numeric_limits<uint64_t>::max()) {
            new_una = std::max(new_una, cumulative_ack);
        } else {
            while (acked_.count(new_una)) ++new_una;
        }
        if (new_una > snd_una_) {
            const uint64_t advanced = new_una - snd_una_;
            inflate_ = advanced >= inflate_ ? 0 : inflate_ - advanced;
            snd_una_ = new_una;
            for (std::set<uint64_t>::iterator it = acked_.begin();
                 it != acked_.end() && *it < snd_una_;)
                it = acked_.erase(it);
        }
        if (recovery_active_ && snd_una_ >= recovery_boundary_)
            recovery_active_ = false;

        if (slow_path) {
            pruned_[path] = true;
            if(update_cc)cwnd_ = std::max(1.0, cwnd_ - 1.0);
            return;
        }
        highest_delivered_ = std::max(highest_delivered_, delivered_psn);
        pruned_[path] = false;
        if(update_cc){if (ce) cwnd_ = std::max(1.0, cwnd_ - 0.5);
        else cwnd_ += 1.0 / cwnd_;}
        ++ack_credit_;
        ack_clock_.push_back(path);
    }

    void enter_recovery(uint64_t missing_sequence) {
        if (!recovery_active_) {
            recovery_active_ = true;
            recovery_boundary_ = snd_nxt_;
            snd_retx_ = missing_sequence;
        } else {
            snd_retx_ = std::min(snd_retx_, missing_sequence);
        }
    }

    // Unordered adapter: cumulative feedback is idempotent window progress,
    // not an extra successful-payload echo, congestion update or path credit.
    void note_cumulative_progress(uint64_t cumulative) {
        uint64_t new_una = std::min(snd_nxt_, std::max(snd_una_, cumulative));
        if (new_una > snd_una_) {
            uint64_t advanced = new_una - snd_una_;
            inflate_ = advanced >= inflate_ ? 0 : inflate_ - advanced;
            snd_una_ = new_una;
            for (auto it = acked_.begin(); it != acked_.end() && *it < snd_una_;)
                it = acked_.erase(it);
        }
        if (recovery_active_ && snd_una_ >= recovery_boundary_)
            recovery_active_ = false;
    }

    bool next_retransmission(uint64_t& sequence) {
        if (!recovery_active_) return false;
        while (snd_retx_ < recovery_boundary_ &&
               (snd_retx_ < snd_una_ || acked_.count(snd_retx_)))
            ++snd_retx_;
        if (snd_retx_ >= recovery_boundary_) return false;
        sequence = snd_retx_++;
        return true;
    }

    void use_external_cwnd(double value){cwnd_=value;}
    double available_window() const {
        const double flight = static_cast<double>(snd_nxt_ - snd_una_);
        return std::max(0.0, cwnd_ + static_cast<double>(inflate_) - flight);
    }

    bool new_data_allowed(uint64_t outstanding_packets, uint64_t count_cap) const {
        return outstanding_packets < count_cap && !recovery_active_ &&
               available_window() >= 1.0 && (snd_nxt_ < 128 || has_credit());
    }

    bool consume_credit(uint32_t& path) {
        if (ack_clock_.empty()) return false;
        path = ack_clock_.front();
        ack_clock_.pop_front();
        --ack_credit_;
        return true;
    }
    bool has_credit() const { return !ack_clock_.empty(); }
    bool path_pruned(uint32_t path) const { return pruned_[path % paths_]; }
    double cwnd() const { return cwnd_; }
    uint64_t ack_credit() const { return ack_credit_; }
    uint64_t snd_nxt() const { return snd_nxt_; }
    uint64_t snd_una() const { return snd_una_; }
    uint64_t inflate() const { return inflate_; }
    bool recovery_active() const { return recovery_active_; }
    uint64_t recovery_boundary() const { return recovery_boundary_; }

private:
    double cwnd_;
    uint32_t paths_;
    uint64_t delta_;
    uint64_t highest_delivered_;
    uint64_t ack_credit_;
    uint64_t snd_nxt_;
    uint64_t snd_una_;
    uint64_t inflate_;
    bool recovery_active_;
    uint64_t snd_retx_;
    uint64_t recovery_boundary_;
    std::set<uint64_t> acked_;
    std::vector<bool> pruned_;
    std::deque<uint32_t> ack_clock_;
};

class RepsState {
public:
    RepsState(uint32_t capacity, uint32_t warmup, uint64_t seed,
              bool trim_freezing_extension = true)
        : capacity_(capacity ? capacity : 1), warmup_(warmup), state_(seed),
          freezing_(false), freeze_until_(0),
          trim_freezing_extension_(trim_freezing_extension),
          cache_reuse_selections_(0), random_exploration_selections_(0),
          timeout_freeze_entries_(0), trim_freeze_entries_(0) {}

    uint32_t select_path(uint32_t paths, bool allow_exploration = false) {
        paths = paths ? paths : 1;
        if (freezing_ && !history_.empty()) {
            uint32_t path = history_[frozen_cursor_ % history_.size()] % paths;
            frozen_cursor_ = (frozen_cursor_ + 1) % history_.size();
            ++cache_reuse_selections_;
            ++frozen_cache_reuse_selections_;
            return path;
        }
        if (!cache_.empty() && !allow_exploration) {
            const uint32_t path = cache_.front() % paths;
            cache_.pop_front();
            ++cache_reuse_selections_;
            return path;
        }
        ++random_exploration_selections_;
        state_ += 0x9e3779b97f4a7c15ULL;
        uint64_t z = state_;
        z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
        z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
        return static_cast<uint32_t>((z ^ (z >> 31)) % paths);
    }

    uint32_t select_path_at(uint32_t paths, uint64_t now_us, bool allow_exploration = false) {
        advance_time(now_us);
        return select_path(paths, allow_exploration);
    }
    void advance_time(uint64_t now_us) {
        if (freezing_ && now_us > freeze_until_) {
            freezing_ = false; ++freeze_exits_;
            cache_.clear(); history_.clear(); history_head_ = frozen_cursor_ = 0;
        }
    }
    void note_data_ack(uint32_t path, bool ce, uint64_t now_us) {
        advance_time(now_us);
        if (ce) return;
        if (cache_.size() == capacity_) cache_.pop_front();
        cache_.push_back(path);
        // Keep the bounded circular history after an EV loses fresh validity.
        // Freezing must reuse even consumed EVs, not drain into random paths.
        if (history_.size() < capacity_) history_.push_back(path);
        else { history_[history_head_] = path; history_head_ = (history_head_ + 1) % capacity_; }
    }
    void note_trim_header(uint32_t, bool, uint64_t) {
        // The REPS extension reports congestion/missing/path state only.
    }
    void note_trim_missing(uint64_t now_us) {
        if (!trim_freezing_extension_) return;
        ++trim_freeze_entries_;
        enter_freezing(now_us);
    }
    void note_failure_detection(uint64_t now_us) {
        ++timeout_freeze_entries_;
        enter_freezing(now_us);
    }
    void note_missing_feedback(uint64_t now_us) { note_trim_missing(now_us); }
    void enter_freezing(uint64_t now_us) {
        freezing_ = true;
        freeze_until_ = now_us + 100;
    }
    bool freezing() const { return freezing_; }
    uint64_t frozen_cache_reuse_selections() const { return frozen_cache_reuse_selections_; }
    uint64_t freeze_exits() const { return freeze_exits_; }
    uint64_t retained_entropies() const { return history_.size(); }
    uint32_t cached() const { return static_cast<uint32_t>(cache_.size()); }
    uint64_t cache_reuse_selections() const { return cache_reuse_selections_; }
    uint64_t random_exploration_selections() const {
        return random_exploration_selections_;
    }
    uint64_t timeout_freeze_entries() const { return timeout_freeze_entries_; }
    uint64_t trim_freeze_entries() const { return trim_freeze_entries_; }

private:
    uint32_t capacity_;
    uint32_t warmup_;
    uint64_t state_;
    bool freezing_;
    uint64_t freeze_until_;
    bool trim_freezing_extension_;
    uint64_t cache_reuse_selections_;
    uint64_t random_exploration_selections_;
    uint64_t timeout_freeze_entries_;
    uint64_t trim_freeze_entries_;
    std::deque<uint32_t> cache_;
    std::vector<uint32_t> history_;
    uint64_t history_head_ = 0, frozen_cursor_ = 0;
    uint64_t frozen_cache_reuse_selections_ = 0, freeze_exits_ = 0;
};

class GlbApproxState {
public:
    GlbApproxState(uint32_t paths, uint64_t feedback_delay_us)
        : paths_(paths ? paths : 1), delay_(feedback_delay_us),
          visible_(paths_, 0), rr_(0) {}

    void observe(uint32_t path, uint8_t quality, uint64_t now_us) {
        pending_.push_back(Update{path % paths_, quality, now_us + delay_});
    }
    uint32_t select(uint64_t now_us) {
        while (!pending_.empty() && pending_.front().visible_at <= now_us) {
            visible_[pending_.front().path] = pending_.front().quality;
            pending_.pop_front();
        }
        uint8_t best = std::numeric_limits<uint8_t>::max();
        for (uint32_t i = 0; i < paths_; ++i) best = std::min(best, visible_[i]);
        for (uint32_t n = 0; n < paths_; ++n) {
            uint32_t candidate = (rr_ + n) % paths_;
            if (visible_[candidate] == best) {
                rr_ = (candidate + 1) % paths_;
                return candidate;
            }
        }
        return 0;
    }

private:
    struct Update { uint32_t path; uint8_t quality; uint64_t visible_at; };
    uint32_t paths_;
    uint64_t delay_;
    std::vector<uint8_t> visible_;
    uint32_t rr_;
    std::deque<Update> pending_;
};

enum class DcqcnRecovery { FAST, ACTIVE, HYPER };

class DcqcnState {
public:
    DcqcnState(double line_rate, double g, double rai, double rhai,
               uint32_t fast_cycles, uint64_t byte_threshold)
        : line_rate_(line_rate), g_(g), rai_(rai), rhai_(rhai),
          fast_cycles_(fast_cycles), byte_threshold_(byte_threshold),
          current_(line_rate), target_(line_rate), alpha_(0.0),
          recovery_stage_(0), acked_bytes_(0) {}

    void on_cnp() {
        alpha_ = (1.0 - g_) * alpha_ + g_;
        target_ = current_;
        current_ = std::max(1.0, current_ * (1.0 - alpha_ / 2.0));
        recovery_stage_ = 0;
        acked_bytes_ = 0;
    }

    void alpha_timer(bool saw_cnp) {
        alpha_ = (1.0 - g_) * alpha_ + (saw_cnp ? g_ : 0.0);
    }

    DcqcnRecovery recover() {
        ++recovery_stage_;
        DcqcnRecovery phase;
        if (recovery_stage_ <= fast_cycles_) {
            phase = DcqcnRecovery::FAST;
        } else if (recovery_stage_ <= 2 * fast_cycles_) {
            target_ = std::min(line_rate_, target_ + rai_);
            phase = DcqcnRecovery::ACTIVE;
        } else {
            target_ = std::min(line_rate_, target_ + rhai_);
            phase = DcqcnRecovery::HYPER;
        }
        current_ = std::min(line_rate_, (current_ + target_) / 2.0);
        acked_bytes_ = 0;
        return phase;
    }

    bool note_acked_bytes(uint64_t bytes) {
        acked_bytes_ += bytes;
        return byte_threshold_ && acked_bytes_ >= byte_threshold_;
    }

    double current_rate() const { return current_; }
    double target_rate() const { return target_; }
    double alpha() const { return alpha_; }
    uint32_t recovery_stage() const { return recovery_stage_; }

private:
    double line_rate_, g_, rai_, rhai_;
    uint32_t fast_cycles_;
    uint64_t byte_threshold_;
    double current_, target_, alpha_;
    uint32_t recovery_stage_;
    uint64_t acked_bytes_;
};

} // namespace pretrim

#endif
