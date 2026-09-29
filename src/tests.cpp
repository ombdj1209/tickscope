// Dependency-free tests. `make test` (optimised) and `make asan` (AddressSanitizer + UBSan).
#include <cstdio>
#include <thread>

#include "tickscope/book.hpp"
#include "tickscope/feed.hpp"
#include "tickscope/histogram.hpp"
#include "tickscope/spsc.hpp"

using namespace tickscope;

static int passed = 0, failed = 0;
#define CHECK(c) do { if (!(c)) { std::printf("  FAIL  %s:%d  %s\n", __FILE__, __LINE__, #c); ok = false; } } while (0)
#define TEST(name) static void name(bool& ok)
#define RUN(name) do { bool ok = true; name(ok); (ok ? passed : failed)++; std::printf("  %s  %s\n", ok ? "ok  " : "FAIL", #name); } while (0)

template <class Table>
static uint64_t replay(const std::vector<Msg>& msgs, uint32_t instruments, Table t) {
    Book<Table> b(instruments, std::move(t));
    for (auto& m : msgs) b.apply(m);
    return b.state_hash();
}

TEST(roundtrip_every_message_type) {
    std::byte buf[64];
    for (auto t : {MsgType::Add, MsgType::Execute, MsgType::Cancel, MsgType::Delete}) {
        Msg m{42, t, 123456789, 77, 5, Side::Sell, 300, -12345};
        if (t != MsgType::Add) { m.instrument = 0; m.side = Side{}; m.price = 0; }
        if (t == MsgType::Delete) m.qty = 0;
        std::size_t n = encode(m, buf);
        Msg d{};
        CHECK(parse({buf, n}, d) == n);
        CHECK(d.seq == 42 && d.type == t && d.ts_ns == 123456789 && d.order_id == 77);
        if (t == MsgType::Add) CHECK(d.instrument == 5 && d.side == Side::Sell && d.qty == 300 && d.price == -12345);
        if (t == MsgType::Execute || t == MsgType::Cancel) CHECK(d.qty == 300);
    }
}

TEST(parser_rejects_truncated_and_invalid_input) {
    std::byte buf[64];
    Msg m{1, MsgType::Add, 0, 1, 0, Side::Buy, 10, 100};
    std::size_t n = encode(m, buf);
    Msg d{};
    for (std::size_t k = 0; k < n; ++k) CHECK(parse({buf, k}, d) == 0);
    buf[8] = std::byte{'Z'};
    CHECK(parse({buf, n}, d) == 0);
    encode(m, buf);
    buf[kHeader + 12] = std::byte{'Q'};
    CHECK(parse({buf, n}, d) == 0);
}

TEST(book_quantities_are_conserved) {
    auto msgs = generate({8, 200'000, 3});
    Book<OpenTable> b(8, OpenTable(1 << 18));
    uint64_t added = 0, removed = 0;
    for (auto& m : msgs) {
        CHECK(b.apply(m));
        if (m.type == MsgType::Add) added += m.qty;
        else if (m.type != MsgType::Delete) removed += m.qty;
    }
    // resting quantity = everything added minus executions and cancels minus deleted remainders
    CHECK(b.resting_qty() <= added - removed);
    for (uint32_t i = 0; i < 8; ++i) if (auto bb = b.best_bid(i), ba = b.best_ask(i); bb && ba) CHECK(*bb < *ba);
}

TEST(invalid_references_are_rejected_without_side_effects) {
    Book<OpenTable> b(2, OpenTable(1 << 10));
    uint64_t h0 = b.state_hash();
    CHECK(!b.apply(Msg{1, MsgType::Execute, 0, 999, 0, {}, 5, 0}));
    CHECK(!b.apply(Msg{2, MsgType::Delete, 0, 999, 0, {}, 0, 0}));
    CHECK(b.apply(Msg{3, MsgType::Add, 0, 1, 0, Side::Buy, 10, 100}));
    CHECK(!b.apply(Msg{4, MsgType::Add, 0, 1, 0, Side::Buy, 10, 100}));    // duplicate id
    CHECK(!b.apply(Msg{5, MsgType::Cancel, 0, 1, 0, {}, 11, 0}));          // more than resting
    CHECK(b.state_hash() != h0 && b.resting_qty() == 10);
}

TEST(order_tables_produce_identical_books) {
    auto msgs = generate({16, 300'000, 5});
    uint64_t a = replay(msgs, 16, OpenTable(1 << 19));
    CHECK(a == replay(msgs, 16, StdTable()));
    CHECK(a == replay(msgs, 16, StdTable(1 << 18)));
    CHECK(a == replay(generate({16, 300'000, 5}), 16, OpenTable(1 << 19)));   // generator is deterministic
}

TEST(gap_recovery_rebuilds_the_exact_book) {
    auto msgs = generate({16, 200'000, 9});
    auto packets = packetize(msgs, 9);
    uint64_t truth = replay(msgs, 16, OpenTable(1 << 19));
    for (double loss : {0.0, 0.01, 0.05})
        for (int tol : {0, 4}) {
            Book<OpenTable> b(16, OpenTable(1 << 19));
            Recovery rec{&msgs};
            FeedHandler<OpenTable> fh(b, rec, tol);
            for (auto* p : transmit(packets, {loss, 0.02, 3, 11})) fh.on_packet(*p);
            fh.flush(msgs.size());
            CHECK(b.state_hash() == truth);
            CHECK(fh.expected() == msgs.size() + 1 && fh.rejected() == 0);
            if (loss == 0.0) CHECK(fh.duplicates() > 0);
        }
}

TEST(spsc_preserves_order_across_threads) {
    Spsc<uint64_t> q(1 << 10);
    constexpr uint64_t N = 2'000'000;
    std::thread prod([&] { for (uint64_t i = 1; i <= N; ++i) while (!q.push(i)) std::this_thread::yield(); });
    uint64_t expect = 1, v;
    bool ok_order = true;
    while (expect <= N) { if (q.pop(v)) { ok_order &= (v == expect); ++expect; } else std::this_thread::yield(); }
    prod.join();
    CHECK(ok_order);
    CHECK(!q.pop(v));
}

TEST(histogram_percentiles_are_close) {
    Histogram h;
    for (uint64_t v = 1; v <= 100'000; ++v) h.record(v);
    auto near = [](uint64_t got, double want) { return got >= want * 0.97 && got <= want * 1.03; };
    CHECK(near(h.percentile(0.5), 50'000));
    CHECK(near(h.percentile(0.99), 99'000));
    CHECK(h.max() == 100'000 && h.count() == 100'000);
}

int main() {
    RUN(roundtrip_every_message_type);
    RUN(parser_rejects_truncated_and_invalid_input);
    RUN(book_quantities_are_conserved);
    RUN(invalid_references_are_rejected_without_side_effects);
    RUN(order_tables_produce_identical_books);
    RUN(gap_recovery_rebuilds_the_exact_book);
    RUN(spsc_preserves_order_across_threads);
    RUN(histogram_percentiles_are_close);
    std::printf("%d passed, %d failed\n", passed, failed);
    return failed ? 1 : 0;
}
