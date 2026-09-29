// Benchmark matrix. Every number in the README comes from this binary. `make bench`
#include <chrono>
#include <cstdio>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>

#include "tickscope/book.hpp"
#include "tickscope/feed.hpp"
#include "tickscope/histogram.hpp"
#include "tickscope/spsc.hpp"

using namespace tickscope;
using Clock = std::chrono::steady_clock;

static inline uint64_t now_ns() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count());
}

template <class T> static inline void keep(T&& v) { asm volatile("" : : "g"(v) : "memory"); }

static std::string cpu_model() {
    std::ifstream f("/proc/cpuinfo");
    std::string line;
    while (std::getline(f, line))
        if (line.rfind("model name", 0) == 0) return line.substr(line.find(':') + 2);
    return "unknown";
}

static std::ostringstream js;
static bool first_section = true;
static void section(const char* name) { js << (first_section ? "" : ",") << "\"" << name << "\":{"; first_section = false; }

static uint64_t clock_overhead() {
    Histogram h;
    for (int i = 0; i < 200000; ++i) { uint64_t a = now_ns(); uint64_t b = now_ns(); h.record(b - a); }
    return h.percentile(0.5);
}

// 1. Parsing: zero-copy scalar decode versus a heap allocation per message
static void bench_parse(const std::vector<Packet>& ps, uint64_t n_msgs) {
    auto t0 = Clock::now();
    uint64_t sum = 0;
    for (int rep = 0; rep < 3; ++rep)
        for (auto& p : ps) {
            std::span<const std::byte> b(p.bytes);
            Msg m;
            while (!b.empty()) { std::size_t k = parse(b, m); sum += m.order_id; b = b.subspan(k); }
        }
    double zc = std::chrono::duration<double>(Clock::now() - t0).count() / 3;
    keep(sum);
    t0 = Clock::now();
    for (int rep = 0; rep < 3; ++rep) {
        std::vector<std::unique_ptr<Msg>> out;
        for (auto& p : ps) {
            std::span<const std::byte> b(p.bytes);
            while (!b.empty()) { auto m = std::make_unique<Msg>(); std::size_t k = parse(b, *m); b = b.subspan(k); out.push_back(std::move(m)); }
        }
        keep(out.size());
    }
    double heap = std::chrono::duration<double>(Clock::now() - t0).count() / 3;
    std::printf("parse   zero-copy %.1f M msg/s (%.1f ns/msg) | heap-per-msg %.1f M msg/s (%.1f ns/msg)\n",
                n_msgs / zc / 1e6, zc * 1e9 / n_msgs, n_msgs / heap / 1e6, heap * 1e9 / n_msgs);
    section("parse");
    js << "\"zero_copy_ns_per_msg\":" << zc * 1e9 / n_msgs << ",\"heap_per_msg_ns_per_msg\":" << heap * 1e9 / n_msgs
       << ",\"zero_copy_mmsg_per_s\":" << n_msgs / zc / 1e6 << ",\"heap_mmsg_per_s\":" << n_msgs / heap / 1e6 << "}";
}

// 2. Book update: order-table allocation strategy
template <class Table, class Make>
static void book_run(const char* name, const std::vector<Msg>& msgs, uint32_t ins, Make make, uint64_t ovh, bool comma) {
    {   // throughput pass, then a separately timed per-message pass on a fresh book
        Book<Table> b(ins, make());
        auto t0 = Clock::now();
        for (auto& m : msgs) b.apply(m);
        double s = std::chrono::duration<double>(Clock::now() - t0).count();
        Book<Table> b2(ins, make());
        Histogram h;
        for (auto& m : msgs) { uint64_t a = now_ns(); b2.apply(m); uint64_t e = now_ns(); h.record(e - a > ovh ? e - a - ovh : 0); }
        std::printf("book    %-26s %6.1f ns/msg mean | p50 %4lu p99 %5lu p99.9 %6lu max %8lu ns (timer overhead removed)\n", name,
                    s * 1e9 / msgs.size(), (unsigned long)h.percentile(0.5), (unsigned long)h.percentile(0.99),
                    (unsigned long)h.percentile(0.999), (unsigned long)h.max());
        js << (comma ? "," : "") << "\"" << name << "\":{\"mean_ns\":" << s * 1e9 / msgs.size() << ",\"p50_ns\":" << h.percentile(0.5)
           << ",\"p99_ns\":" << h.percentile(0.99) << ",\"p999_ns\":" << h.percentile(0.999) << ",\"max_ns\":" << h.max()
           << ",\"hash\":\"" << std::hex << b.state_hash() << std::dec << "\"}";
    }
}

// 3. Thread handoff: lock-free SPSC versus mutex queue
template <class Q>
static void handoff(const char* name, Q& q, uint64_t n, bool comma) {
    Histogram h;
    auto t0 = Clock::now();
    std::thread prod([&] { for (uint64_t i = 0; i < n; ++i) { uint64_t ts = now_ns(); while (!q.push(ts)) std::this_thread::yield(); } });
    uint64_t got = 0, v;
    while (got < n) { if (q.pop(v)) { h.record(now_ns() - v); ++got; } else std::this_thread::yield(); }
    prod.join();
    double s = std::chrono::duration<double>(Clock::now() - t0).count();
    std::printf("handoff %-12s %6.1f M items/s | queueing delay p50 %lu p99 %lu ns\n", name, n / s / 1e6,
                (unsigned long)h.percentile(0.5), (unsigned long)h.percentile(0.99));
    js << (comma ? "," : "") << "\"" << name << "\":{\"mitems_per_s\":" << n / s / 1e6 << ",\"delay_p50_ns\":" << h.percentile(0.5)
       << ",\"delay_p99_ns\":" << h.percentile(0.99) << "}";
}

int main() {
    const uint32_t INS = 64;
    const uint64_t N = 3'000'000;
    long cpus = sysconf(_SC_NPROCESSORS_ONLN);
    std::printf("cpu: %s, %ld online core(s)\n", cpu_model().c_str(), cpus);
    auto t_gen = Clock::now();
    auto msgs = generate({INS, N, 42});
    auto packets = packetize(msgs, 42);
    std::printf("generated %lu messages in %lu packets (%.2f s)\n", (unsigned long)msgs.size(), (unsigned long)packets.size(),
                std::chrono::duration<double>(Clock::now() - t_gen).count());
    uint64_t ovh = clock_overhead();
    js << "{\"environment\":{\"cpu\":\"" << cpu_model() << "\",\"cores\":" << cpus << ",\"compiler\":\"" << __VERSION__
       << "\",\"flags\":\"-O2 -DNDEBUG\",\"messages\":" << N << ",\"packets\":" << packets.size() << ",\"instruments\":" << INS
       << ",\"timer_overhead_ns\":" << ovh << "},";
    first_section = true;

    bench_parse(packets, N);

    js << ",";
    first_section = true;
    section("book");
    book_run<OpenTable>("open addressing (prealloc)", msgs, INS, [] { return OpenTable(1 << 22); }, ovh, false);
    book_run<StdTable>("std::unordered_map", msgs, INS, [] { return StdTable(); }, ovh, true);
    book_run<StdTable>("std::unordered_map reserved", msgs, INS, [] { return StdTable(1 << 21); }, ovh, true);
    js << "}";

    // 4. End to end on one thread: packet in, parsed, applied; per-packet latency
    {
        Book<OpenTable> b(INS, OpenTable(1 << 22));
        Recovery rec{&msgs};
        FeedHandler<OpenTable> fh(b, rec, 4);
        Histogram per_pkt;
        auto t0 = Clock::now();
        for (auto& p : packets) { uint64_t a = now_ns(); fh.on_packet(p); per_pkt.record(now_ns() - a); }
        double s = std::chrono::duration<double>(Clock::now() - t0).count();
        std::printf("e2e     %.1f M msg/s | per packet p50 %lu p99 %lu p99.9 %lu ns (avg %.1f msgs/packet)\n", N / s / 1e6,
                    (unsigned long)per_pkt.percentile(0.5), (unsigned long)per_pkt.percentile(0.99),
                    (unsigned long)per_pkt.percentile(0.999), double(N) / packets.size());
        js << ",\"end_to_end\":{\"mmsg_per_s\":" << N / s / 1e6 << ",\"packet_p50_ns\":" << per_pkt.percentile(0.5)
           << ",\"packet_p99_ns\":" << per_pkt.percentile(0.99) << ",\"packet_p999_ns\":" << per_pkt.percentile(0.999)
           << ",\"msgs_per_packet\":" << double(N) / packets.size() << "}";
    }

    // 5. Loss, duplication and reordering: recovery cost and correctness
    uint64_t truth;
    { Book<OpenTable> b(INS, OpenTable(1 << 22)); for (auto& m : msgs) b.apply(m); truth = b.state_hash(); }
    js << ",\"recovery\":[";
    bool c = false;
    for (double loss : {0.0, 0.001, 0.01, 0.05})
        for (int tol : {0, 4}) {
            Book<OpenTable> b(INS, OpenTable(1 << 22));
            Recovery rec{&msgs};
            FeedHandler<OpenTable> fh(b, rec, tol);
            Faults f{loss, 0.01, 3, 99};
            auto arrive = transmit(packets, f);
            auto t0 = Clock::now();
            for (auto* p : arrive) fh.on_packet(*p);
            fh.flush(msgs.size());
            double s = std::chrono::duration<double>(Clock::now() - t0).count();
            bool ok = b.state_hash() == truth;
            std::printf("recover loss %5.1f%% tol %d: %6lu retransmit requests, %8lu msgs re-sent, %6lu dup packets ignored, %.2f s, book %s\n",
                        loss * 100, tol, (unsigned long)rec.requests, (unsigned long)rec.messages, (unsigned long)fh.duplicates(), s,
                        ok ? "IDENTICAL" : "DIFFERENT");
            js << (c ? "," : "") << "{\"loss\":" << loss << ",\"reorder_tolerance\":" << tol << ",\"requests\":" << rec.requests
               << ",\"messages_resent\":" << rec.messages << ",\"duplicates_ignored\":" << fh.duplicates() << ",\"seconds\":" << s
               << ",\"book_identical\":" << (ok ? "true" : "false") << "}";
            c = true;
        }
    js << "]";

    // 6. Handoff between threads
    js << ",";
    first_section = true;
    section("handoff");
    { Spsc<uint64_t> q(1 << 12); handoff("spsc lock-free", q, 2'000'000, false); }
    { MutexQueue<uint64_t> q; handoff("mutex queue", q, 2'000'000, true); }
    js << "}}";

    std::ofstream("results/results.json") << js.str() << "\n";
    std::printf("wrote results/results.json\n");
}
