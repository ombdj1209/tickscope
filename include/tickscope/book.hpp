// Price-level order book, generic over the order-id table so allocation strategies can be benchmarked.
#pragma once
#include <cstdint>
#include <functional>
#include <map>
#include <unordered_map>
#include <vector>

#include "protocol.hpp"

namespace tickscope {

struct Order {
    uint32_t instrument{};
    Side side{};
    uint32_t qty{};
    int64_t price{};
};

// Open addressing, linear probing, preallocated. Key 0 is reserved for "empty"; order ids start at 1.
// Deletion uses backward shifting instead of tombstones, so probe chains never grow with churn.
class OpenTable {
public:
    explicit OpenTable(std::size_t capacity_pow2 = 1 << 20) : keys_(capacity_pow2, 0), vals_(capacity_pow2), mask_(capacity_pow2 - 1) {}

    Order* find(uint64_t k) {
        for (std::size_t i = hash(k) & mask_;; i = (i + 1) & mask_) {
            if (keys_[i] == k) return &vals_[i];
            if (keys_[i] == 0) return nullptr;
        }
    }
    bool insert(uint64_t k, const Order& o) {
        if ((size_ + 1) * 10 > keys_.size() * 7) return false;   // keep load factor under 0.7
        for (std::size_t i = hash(k) & mask_;; i = (i + 1) & mask_) {
            if (keys_[i] == k) return false;
            if (keys_[i] == 0) {
                keys_[i] = k;
                vals_[i] = o;
                ++size_;
                return true;
            }
        }
    }
    void erase(uint64_t k) {
        std::size_t i = hash(k) & mask_;
        while (keys_[i] != k) {
            if (keys_[i] == 0) return;
            i = (i + 1) & mask_;
        }
        for (std::size_t j = i;;) {                       // shift later entries of the chain back into the hole
            j = (j + 1) & mask_;
            if (keys_[j] == 0) break;
            std::size_t h = hash(keys_[j]) & mask_;
            bool movable = (i <= j) ? (h <= i || h > j) : (h <= i && h > j);
            if (movable) {
                keys_[i] = keys_[j];
                vals_[i] = vals_[j];
                i = j;
            }
        }
        keys_[i] = 0;
        --size_;
    }
    std::size_t size() const { return size_; }

private:
    static std::size_t hash(uint64_t k) { k ^= k >> 33; k *= 0xff51afd7ed558ccdULL; k ^= k >> 33; return static_cast<std::size_t>(k); }
    std::vector<uint64_t> keys_;
    std::vector<Order> vals_;
    std::size_t mask_, size_ = 0;
};

class StdTable {
public:
    explicit StdTable(std::size_t reserve = 0) { if (reserve) m_.reserve(reserve); }
    Order* find(uint64_t k) { auto it = m_.find(k); return it == m_.end() ? nullptr : &it->second; }
    bool insert(uint64_t k, const Order& o) { return m_.emplace(k, o).second; }
    void erase(uint64_t k) { m_.erase(k); }
    std::size_t size() const { return m_.size(); }

private:
    std::unordered_map<uint64_t, Order> m_;
};

template <class Table>
class Book {
public:
    explicit Book(uint32_t instruments, Table table = Table()) : bids_(instruments), asks_(instruments), orders_(std::move(table)) {}

    // Applies one message. Returns false for messages that reference unknown orders or are malformed.
    bool apply(const Msg& m) {
        switch (m.type) {
            case MsgType::Add: {
                if (m.instrument >= bids_.size() || m.qty == 0) return false;
                if (!orders_.insert(m.order_id, {m.instrument, m.side, m.qty, m.price})) return false;
                with_level(m.instrument, m.side, [&](auto& lv) { lv[m.price] += m.qty; });
                return true;
            }
            case MsgType::Execute:
            case MsgType::Cancel: {
                Order* o = orders_.find(m.order_id);
                if (!o || m.qty == 0 || m.qty > o->qty) return false;
                reduce(*o, m.qty);
                o->qty -= m.qty;
                if (o->qty == 0) orders_.erase(m.order_id);
                return true;
            }
            case MsgType::Delete: {
                Order* o = orders_.find(m.order_id);
                if (!o) return false;
                reduce(*o, o->qty);
                orders_.erase(m.order_id);
                return true;
            }
        }
        return false;
    }

    std::optional<int64_t> best_bid(uint32_t i) const { return bids_[i].empty() ? std::nullopt : std::optional(bids_[i].begin()->first); }
    std::optional<int64_t> best_ask(uint32_t i) const { return asks_[i].empty() ? std::nullopt : std::optional(asks_[i].begin()->first); }

    // FNV-1a over every (instrument, side, price, qty) level: identical books give identical hashes.
    uint64_t state_hash() const {
        uint64_t h = 1469598103934665603ULL;
        auto mix = [&](uint64_t v) { for (int i = 0; i < 8; ++i) { h ^= (v >> (8 * i)) & 0xff; h *= 1099511628211ULL; } };
        for (uint32_t i = 0; i < bids_.size(); ++i) {
            for (auto& [p, q] : bids_[i]) { mix(i); mix('B'); mix(static_cast<uint64_t>(p)); mix(q); }
            for (auto& [p, q] : asks_[i]) { mix(i); mix('S'); mix(static_cast<uint64_t>(p)); mix(q); }
        }
        return h;
    }

    uint64_t resting_qty() const {
        uint64_t s = 0;
        for (auto& lv : bids_) for (auto& [p, q] : lv) s += q;
        for (auto& lv : asks_) for (auto& [p, q] : lv) s += q;
        return s;
    }
    std::size_t orders() const { return orders_.size(); }

private:
    using Bids = std::map<int64_t, uint64_t, std::greater<>>;
    using Asks = std::map<int64_t, uint64_t>;

    template <class F>
    void with_level(uint32_t i, Side s, F&& f) { if (s == Side::Buy) f(bids_[i]); else f(asks_[i]); }

    void reduce(const Order& o, uint32_t q) {
        with_level(o.instrument, o.side, [&](auto& lv) {
            auto it = lv.find(o.price);
            it->second -= q;
            if (it->second == 0) lv.erase(it);
        });
    }

    std::vector<Bids> bids_;
    std::vector<Asks> asks_;
    Table orders_;
};

}  // namespace tickscope
