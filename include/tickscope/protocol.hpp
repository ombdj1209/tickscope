// ITCH-inspired binary market-data protocol (not NASDAQ ITCH 5.0; field layout is our own).
// Little-endian, fixed-size messages, one sequence number per message.
#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <vector>

namespace tickscope {

enum class MsgType : uint8_t { Add = 'A', Execute = 'E', Cancel = 'X', Delete = 'D' };
enum class Side : uint8_t { Buy = 'B', Sell = 'S' };

// Header: seq(8) type(1) ts_ns(8) = 17 bytes, then the body.
constexpr std::size_t kHeader = 17;
constexpr std::size_t kAddBody = 8 + 4 + 1 + 4 + 8;   // order id, instrument, side, qty, price
constexpr std::size_t kExecBody = 8 + 4;               // order id, qty
constexpr std::size_t kCancelBody = 8 + 4;             // order id, qty
constexpr std::size_t kDeleteBody = 8;                 // order id

constexpr std::size_t body_size(MsgType t) {
    switch (t) {
        case MsgType::Add: return kAddBody;
        case MsgType::Execute: return kExecBody;
        case MsgType::Cancel: return kCancelBody;
        case MsgType::Delete: return kDeleteBody;
    }
    return 0;
}

// Decoded view. Scalars are copied out with memcpy (alignment-safe); nothing is heap allocated.
struct Msg {
    uint64_t seq{};
    MsgType type{};
    uint64_t ts_ns{};
    uint64_t order_id{};
    uint32_t instrument{};
    Side side{};
    uint32_t qty{};
    int64_t price{};   // price in 1e-4 units
};

template <class T>
inline T load(const std::byte* p) {
    T v;
    std::memcpy(&v, p, sizeof(T));
    return v;
}

template <class T>
inline void store(std::byte* p, T v) {
    std::memcpy(p, &v, sizeof(T));
}

inline std::size_t encode(const Msg& m, std::byte* out) {
    store(out, m.seq);
    store(out + 8, static_cast<uint8_t>(m.type));
    store(out + 9, m.ts_ns);
    std::byte* b = out + kHeader;
    switch (m.type) {
        case MsgType::Add:
            store(b, m.order_id); store(b + 8, m.instrument); store(b + 12, static_cast<uint8_t>(m.side));
            store(b + 13, m.qty); store(b + 17, m.price);
            break;
        case MsgType::Execute:
        case MsgType::Cancel:
            store(b, m.order_id); store(b + 8, m.qty);
            break;
        case MsgType::Delete:
            store(b, m.order_id);
            break;
    }
    return kHeader + body_size(m.type);
}

// Parses one message at the front of `buf`. Returns bytes consumed, or 0 if truncated or invalid.
inline std::size_t parse(std::span<const std::byte> buf, Msg& m) {
    if (buf.size() < kHeader) return 0;
    const std::byte* p = buf.data();
    auto t = load<uint8_t>(p + 8);
    if (t != 'A' && t != 'E' && t != 'X' && t != 'D') return 0;
    m.type = static_cast<MsgType>(t);
    std::size_t need = kHeader + body_size(m.type);
    if (buf.size() < need) return 0;
    m.seq = load<uint64_t>(p);
    m.ts_ns = load<uint64_t>(p + 9);
    const std::byte* b = p + kHeader;
    m.order_id = load<uint64_t>(b);
    switch (m.type) {
        case MsgType::Add: {
            m.instrument = load<uint32_t>(b + 8);
            auto s = load<uint8_t>(b + 12);
            if (s != 'B' && s != 'S') return 0;
            m.side = static_cast<Side>(s);
            m.qty = load<uint32_t>(b + 13);
            m.price = load<int64_t>(b + 17);
            break;
        }
        case MsgType::Execute:
        case MsgType::Cancel:
            m.qty = load<uint32_t>(b + 8);
            break;
        case MsgType::Delete:
            break;
    }
    return need;
}

// A packet is a contiguous run of messages with consecutive sequence numbers.
struct Packet {
    uint64_t first_seq{};
    uint32_t count{};
    std::vector<std::byte> bytes;
};

}  // namespace tickscope
