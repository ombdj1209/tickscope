// Single-producer single-consumer ring buffer (lock-free) and a mutex queue to compare it with.
#pragma once
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <new>
#include <optional>
#include <vector>

namespace tickscope {

inline constexpr std::size_t kCacheLine = 64;

template <class T>
class Spsc {
public:
    explicit Spsc(std::size_t capacity_pow2) : buf_(capacity_pow2), mask_(capacity_pow2 - 1) {}

    bool push(const T& v) {
        const auto h = head_.load(std::memory_order_relaxed);
        if (h - tail_cache_ == buf_.size()) {                       // looks full: refresh the consumer's position
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if (h - tail_cache_ == buf_.size()) return false;
        }
        buf_[h & mask_] = v;
        head_.store(h + 1, std::memory_order_release);
        return true;
    }

    bool pop(T& out) {
        const auto t = tail_.load(std::memory_order_relaxed);
        if (t == head_cache_) {
            head_cache_ = head_.load(std::memory_order_acquire);
            if (t == head_cache_) return false;
        }
        out = buf_[t & mask_];
        tail_.store(t + 1, std::memory_order_release);
        return true;
    }

private:
    std::vector<T> buf_;
    const std::size_t mask_;
    alignas(kCacheLine) std::atomic<std::size_t> head_{0};
    alignas(kCacheLine) std::size_t tail_cache_{0};              // producer-local copy of tail
    alignas(kCacheLine) std::atomic<std::size_t> tail_{0};
    alignas(kCacheLine) std::size_t head_cache_{0};              // consumer-local copy of head
};

template <class T>
class MutexQueue {
public:
    bool push(const T& v) { { std::lock_guard g(m_); q_.push_back(v); } cv_.notify_one(); return true; }
    bool pop(T& out) {
        std::lock_guard g(m_);
        if (q_.empty()) return false;
        out = q_.front();
        q_.pop_front();
        return true;
    }

private:
    std::mutex m_;
    std::condition_variable cv_;
    std::deque<T> q_;
};

}  // namespace tickscope
