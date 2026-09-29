# Tickscope

**A market-data feed handler you can measure.** A synthetic ITCH-inspired binary feed with packet loss, duplication and reordering. The C++20 pipeline has a zero-copy parser, a price-level order book with a preallocated open-addressing order table, gap detection with retransmission recovery, a lock-free SPSC queue and a log-linear latency histogram. A benchmark matrix explains where the nanoseconds go and what each design choice buys.

The protocol is ITCH-*inspired*: fixed-size little-endian messages (add, execute, cancel, delete) with per-message sequence numbers. It is not NASDAQ ITCH 5.0. The feed is synthetic, and nothing here trades.

## Why this exists

IMC's Amsterdam C++ execution teams build software in the latency-critical loop: exchange connectivity, real-time processing of market data, and hardware integration where software alone is not fast enough, while balancing latency, throughput, simplicity and testability. Tickscope is a small, honest version of the first stage of that loop, built to show the engineering habits that matter there:

- measure before optimising;
- prove correctness under failure;
- report the tail, not the mean;
- say exactly what hardware the numbers came from.

## Architecture

```
generator ──► packetizer ──► lossy transport ──► FeedHandler ──► parse (zero-copy) ──► Book<OrderTable>
 valid order    1-24 msgs     loss, duplicates,     expected seq,                        price levels per
 lifecycles     per packet    reorder window        reorder tolerance,                   instrument and side,
                                                    retransmit from Recovery             state hash for replay
```

| Piece | File |
|---|---|
| Wire format, encoder, bounds-checked zero-copy parser | `include/tickscope/protocol.hpp` |
| Order book, open-addressing table with backward-shift deletion, `std::unordered_map` table | `include/tickscope/book.hpp` |
| Feed generator, packetizer, fault injection, recovery service, feed handler | `include/tickscope/feed.hpp` |
| Lock-free SPSC ring (cache-line separated indices, cached peer index) and mutex queue | `include/tickscope/spsc.hpp` |
| HDR-style latency histogram | `include/tickscope/histogram.hpp` |
| Benchmark matrix, tests | `src/bench.cpp`, `src/tests.cpp` |

## Results

`make bench`. Hardware: Intel Xeon @ 2.10 GHz (cloud VM, **1 online core**), g++ 13.3, `-O2 -DNDEBUG`. The feed is 3,000,000 messages in 540,183 packets over 64 instruments. Per-message timings subtract the measured 26 ns timer overhead. Single-core, shared-VM numbers are noisy between runs; treat them as relative comparisons.

### Parsing

| Decoder | ns per message | Messages per second |
|---|---|---|
| **Zero-copy (scalars via `memcpy` from the packet buffer)** | **14.9** | **67.1 M** |
| One heap allocation per message | 72.2 | 13.9 M |

A per-message allocation costs almost 5 times the parse itself.

### Order book: choice of order-id table

| Order table | Mean | p50 | p99 | p99.9 |
|---|---|---|---|---|
| **Open addressing, preallocated, backward-shift delete** | **97.9 ns** | 180 ns | **464 ns** | **800 ns** |
| `std::unordered_map` | 180.2 ns | 196 ns | 848 ns | 1,312 ns |
| `std::unordered_map`, reserved | 154.2 ns | 172 ns | 848 ns | 1,248 ns |

All three produce the identical book (same state hash). **A finding along the way:** the first version of the open-addressing table used tombstones for deletes. Its mean was fine (104 ns) but its p99.9 was 2,144 ns, *worse* than `std::unordered_map`, because tombstones from millions of deleted orders lengthened probe chains. Switching to backward-shift deletion took p99.9 to 800 ns. The mean alone would never have shown it.

### End to end, one thread

Packets in, parsed, sequence-checked and applied: **7.1 M messages/s**. Per packet (5.6 messages on average): p50 512 ns, p99 3.1 µs, p99.9 14.0 µs.

### Loss, duplication and reordering

Every scenario adds 1% duplicate packets and reordering within a 3-packet window. "Tolerance" is how many out-of-order packets the handler buffers before it requests a retransmit.

| Packet loss | Tolerance | Retransmit requests | Messages re-sent | Book matches lossless replay |
|---|---|---|---|---|
| 0% | 0 | 25,770 | 280,221 | yes |
| 0% | **4** | **465** | **2,676** | yes |
| 0.1% | 0 | 26,239 | 281,853 | yes |
| 0.1% | 4 | 1,014 | 5,560 | yes |
| 1% | 0 | 30,133 | 308,853 | yes |
| 1% | 4 | 5,757 | 31,964 | yes |
| 5% | 0 | 46,490 | 417,311 | yes |
| 5% | 4 | 25,912 | 152,250 | yes |

**Treating every gap as loss is expensive.** With no loss at all, mild reordering alone triggers 25,770 retransmit requests; buffering four packets first removes 98% of them. Every run, at every loss rate, rebuilds a book identical to the lossless replay (same state hash).

### Thread handoff (read with care)

| Queue | Throughput | Queueing delay p50 | p99 |
|---|---|---|---|
| Lock-free SPSC | 16.4 M items/s | 123 µs | 154 µs |
| Mutex queue | 10.9 M items/s | 5.3 ms | 10.1 ms |

**On one core these are scheduler measurements, not cache-coherence measurements.** Producer and consumer time-slice on the same CPU, so the delay is dominated by the scheduling quantum. On a machine with isolated, pinned cores the SPSC handoff should drop to tens to hundreds of nanoseconds. That run is the most important missing number in this repo.

## Correctness

```bash
make test    # 8 tests, optimised build
make asan    # the same tests under AddressSanitizer + UndefinedBehaviorSanitizer
make tsan    # the same tests under ThreadSanitizer (SPSC across two threads)
```

All three pass. The tests cover:

- round-trip of every message type;
- rejection of truncated input at every byte boundary, and of invalid type and side;
- conservation of quantity and books that never cross;
- rejection of unknown, duplicate and over-quantity references with no side effects;
- identical books across all three order tables, and a deterministic generator;
- exact book reconstruction under 0% to 5% loss with duplicates and reordering;
- SPSC ordering across 2 million items;
- histogram precision.

## Run it

```bash
make test && make bench      # needs g++ 11+ or clang 14+ with C++20; no third-party libraries
```

## Design notes

- **No allocation on the hot path.** The order table is sized up front; the parser decodes into a stack `Msg`. Price levels use `std::map`, which does allocate on new levels; that is the next thing to replace (see below).
- **`memcpy` for field loads** keeps the parser free of alignment and strict-aliasing undefined behaviour; compilers turn it into single loads.
- **The handler applies by sequence number**, so an overlapping retransmit and a late original packet cannot double-apply a message.
- **The state hash covers every price level**, which turns "the book is correct after recovery" into one comparison.
- **Tails are recorded per message** in a log-linear histogram, with the timer overhead measured and subtracted.

## Limitations

- One core, a shared VM, no CPU pinning or isolation, no huge pages, no hardware counters (`perf` is unavailable in this environment). The handoff numbers especially need a real multi-core machine.
- Transport is in-memory. There is no socket, kernel bypass, multicast or pcap capture, so no network-stage latency is measured.
- Price levels use `std::map` (node allocation on new levels).
- The book is a passive feed book. There is no matching, auction or snapshot-message handling.

## Next steps

1. Run the matrix on an isolated multi-core machine with pinned threads and `perf stat` (cycles, cache misses, branch misses) per stage.
2. Replace `std::map` price levels with a tick-indexed array around the touch plus an overflow map, and measure the p99.9.
3. Capture a synthetic multicast stream to pcap and replay it through a UDP receive path.
4. Fuzz the parser with libFuzzer (the bounds checks are tested, not fuzzed).
