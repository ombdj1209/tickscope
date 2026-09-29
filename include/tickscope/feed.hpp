// Synthetic feed generator, lossy transport, and a feed handler with gap detection and recovery.
#pragma once
#include <algorithm>
#include <cstdint>
#include <map>
#include <random>
#include <vector>

#include "book.hpp"
#include "protocol.hpp"

namespace tickscope {

struct GenConfig {
    uint32_t instruments = 64;
    uint64_t messages = 1'000'000;
    uint64_t seed = 1;
};

// Valid order lifecycles: every execute, cancel or delete refers to a live order with enough quantity.
// Bids rest below and asks above a per-instrument reference price, so books never cross (as on a real feed).
inline std::vector<Msg> generate(const GenConfig& cfg) {
    std::mt19937_64 r(cfg.seed);
    std::uniform_real_distribution<double> u(0, 1);
    std::vector<std::vector<uint64_t>> live(cfg.instruments);
    std::vector<Order> info;          // indexed by order id
    info.reserve(cfg.messages + 1);
    info.push_back({});
    std::vector<int64_t> mid(cfg.instruments, 1'000'000);
    std::vector<Msg> out;
    out.reserve(cfg.messages);
    uint64_t ts = 0, next_id = 1;
    while (out.size() < cfg.messages) {
        uint32_t ins = static_cast<uint32_t>(r() % cfg.instruments);
        ts += 50 + r() % 400;
        Msg m{};
        m.seq = out.size() + 1;
        m.ts_ns = ts;
        auto& lv = live[ins];
        double x = u(r);
        if (lv.size() < 8 || x < 0.46) {
            Side s = u(r) < 0.5 ? Side::Buy : Side::Sell;
            int64_t off = 1 + static_cast<int64_t>(r() % 20);
            m.type = MsgType::Add;
            m.order_id = next_id++;
            m.instrument = ins;
            m.side = s;
            m.qty = 1 + static_cast<uint32_t>(r() % 500);
            m.price = (s == Side::Buy ? mid[ins] - off : mid[ins] + off) * 100;
            info.push_back({ins, s, m.qty, m.price});
            lv.push_back(m.order_id);
        } else {
            std::size_t k = r() % lv.size();
            uint64_t id = lv[k];
            Order& o = info[id];
            m.order_id = id;
            if (x < 0.72) { m.type = MsgType::Execute; m.qty = 1 + static_cast<uint32_t>(r() % o.qty); }
            else if (x < 0.86) { m.type = MsgType::Cancel; m.qty = 1 + static_cast<uint32_t>(r() % o.qty); }
            else { m.type = MsgType::Delete; m.qty = o.qty; }
            o.qty -= m.qty;
            if (o.qty == 0) { lv[k] = lv.back(); lv.pop_back(); }
            if (m.type == MsgType::Delete) m.qty = 0;
        }
        out.push_back(m);
    }
    return out;
}

// Groups messages into packets of 1 to 24 messages (bursty), encoded back to back.
inline std::vector<Packet> packetize(const std::vector<Msg>& msgs, uint64_t seed) {
    std::mt19937_64 r(seed);
    std::vector<Packet> out;
    std::byte tmp[64];
    for (std::size_t i = 0; i < msgs.size();) {
        std::size_t n = std::min<std::size_t>(msgs.size() - i, (r() % 10 == 0) ? 24 : 1 + r() % 6);
        Packet p{msgs[i].seq, static_cast<uint32_t>(n), {}};
        p.bytes.reserve(n * 48);
        for (std::size_t k = 0; k < n; ++k) {
            std::size_t len = encode(msgs[i + k], tmp);
            p.bytes.insert(p.bytes.end(), tmp, tmp + len);
        }
        out.push_back(std::move(p));
        i += n;
    }
    return out;
}

struct Faults {
    double loss = 0, duplicate = 0;
    int reorder_window = 0;   // swap a packet with one up to this many positions later
    uint64_t seed = 7;
};

inline std::vector<const Packet*> transmit(const std::vector<Packet>& ps, const Faults& f) {
    std::mt19937_64 r(f.seed);
    std::uniform_real_distribution<double> u(0, 1);
    std::vector<const Packet*> out;
    out.reserve(ps.size());
    for (auto& p : ps) {
        if (u(r) < f.loss) continue;
        out.push_back(&p);
        if (u(r) < f.duplicate) out.push_back(&p);
    }
    if (f.reorder_window > 0)
        for (std::size_t i = 0; i + 1 < out.size(); ++i)
            if (u(r) < 0.05) std::swap(out[i], out[std::min(out.size() - 1, i + 1 + r() % f.reorder_window)]);
    return out;
}

// Retransmission service: serves any sequence range from the exchange's log.
struct Recovery {
    const std::vector<Msg>* log;
    uint64_t requests = 0, messages = 0;
    template <class F>
    void fetch(uint64_t from, uint64_t to, F&& apply) {
        ++requests;
        for (uint64_t s = from; s <= to; ++s) { apply((*log)[s - 1]); ++messages; }
    }
};

template <class Table>
class FeedHandler {
public:
    FeedHandler(Book<Table>& book, Recovery& rec, int reorder_tolerance) : book_(book), rec_(rec), tol_(reorder_tolerance) {}

    void on_packet(const Packet& p) {
        uint64_t last = p.first_seq + p.count - 1;
        if (last < expected_) { ++duplicates_; return; }
        if (p.first_seq > expected_) {
            pending_.emplace(p.first_seq, &p);
            if (++waited_ > tol_) recover(pending_.begin()->first - 1);
            return;
        }
        apply_packet(p);
        drain();
    }

    // End of session: anything still missing is fetched explicitly.
    void flush(uint64_t final_seq) {
        drain();
        while (!pending_.empty()) { recover(pending_.begin()->first - 1); drain(); }
        if (expected_ <= final_seq) recover(final_seq);
    }

    uint64_t expected() const { return expected_; }
    uint64_t duplicates() const { return duplicates_; }
    uint64_t rejected() const { return rejected_; }
    uint64_t gaps_detected() const { return gaps_; }

private:
    void apply_msg(const Msg& m) { if (m.seq == expected_) { if (!book_.apply(m)) ++rejected_; ++expected_; } }

    void apply_packet(const Packet& p) {
        std::span<const std::byte> buf(p.bytes);
        Msg m;
        while (!buf.empty()) {
            std::size_t n = parse(buf, m);
            if (n == 0) break;
            apply_msg(m);            // skips messages already applied when a packet overlaps a retransmit
            buf = buf.subspan(n);
        }
    }

    void drain() {
        while (!pending_.empty()) {
            auto it = pending_.begin();
            const Packet* p = it->second;
            if (p->first_seq > expected_) break;
            pending_.erase(it);
            if (p->first_seq + p->count - 1 >= expected_) apply_packet(*p);
        }
        if (pending_.empty()) waited_ = 0;
    }

    void recover(uint64_t to) {
        if (to < expected_) return;
        ++gaps_;
        rec_.fetch(expected_, to, [&](const Msg& m) { apply_msg(m); });
        waited_ = 0;
        drain();
    }

    Book<Table>& book_;
    Recovery& rec_;
    int tol_;
    uint64_t expected_ = 1, duplicates_ = 0, rejected_ = 0, gaps_ = 0;
    int waited_ = 0;
    std::map<uint64_t, const Packet*> pending_;
};

}  // namespace tickscope
