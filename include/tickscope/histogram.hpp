// Log-linear latency histogram (HDR-style): 64 linear sub-buckets per power of two, ~1.5% precision.
#pragma once
#include <array>
#include <bit>
#include <cstdint>

namespace tickscope {

class Histogram {
public:
    void record(uint64_t v) {
        ++counts_[index(v)];
        ++n_;
        if (v > max_) max_ = v;
    }
    uint64_t percentile(double q) const {
        uint64_t target = static_cast<uint64_t>(q * static_cast<double>(n_ - 1)) + 1, seen = 0;
        for (std::size_t i = 0; i < counts_.size(); ++i) {
            seen += counts_[i];
            if (seen >= target) return value(i);
        }
        return max_;
    }
    uint64_t count() const { return n_; }
    uint64_t max() const { return max_; }

private:
    static constexpr int kSub = 6;
    static std::size_t index(uint64_t v) {
        if (v < (1u << kSub)) return static_cast<std::size_t>(v);
        int e = std::bit_width(v) - 1;
        return static_cast<std::size_t>(((e - kSub + 1) << kSub) + ((v >> (e - kSub)) & ((1u << kSub) - 1)));
    }
    static uint64_t value(std::size_t i) {
        if (i < (1u << kSub)) return i;
        int e = static_cast<int>(i >> kSub) + kSub - 1;
        return (1ull << e) | ((i & ((1u << kSub) - 1)) << (e - kSub));
    }
    std::array<uint64_t, (64 - kSub + 1) << kSub> counts_{};
    uint64_t n_ = 0, max_ = 0;
};

}  // namespace tickscope
