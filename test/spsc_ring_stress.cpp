// spsc_ring_stress.cpp - Push numbered payloads through the ring from one
// producer thread to one consumer thread, asserting exact FIFO order.

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <thread>
#include <type_traits>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

#include "orderbook/spsc_ring.hpp"

namespace {

struct Payload {
    uint64_t sequence;
};

static_assert(std::is_trivially_copyable_v<Payload>);

constexpr uint64_t kDefaultMessages = 100'000'000;
constexpr std::size_t kRingCapacity = 65'536;

using StressRing = ob::SpscRing<Payload, kRingCapacity>;

// Static storage keeps this large fixed buffer off the thread stacks.
StressRing ring;

// Busy-wait politely while preserving the "push as fast as accepted" stress
// shape. On x86, pause reduces pipeline pressure inside spin loops.
void pause_spin() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#else
    std::this_thread::yield();
#endif
}

bool parse_message_count(const char* text, uint64_t& count) {
    errno = 0;
    char* end = nullptr;
    const unsigned long long parsed = std::strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || parsed == 0) {
        return false;
    }

    count = static_cast<uint64_t>(parsed);
    return true;
}

// Start both threads from the same gate so the producer cannot fill the ring
// before the consumer begins.
void wait_for_start(const std::atomic<bool>& start) noexcept {
    while (!start.load(std::memory_order_acquire)) {
        pause_spin();
    }
}

void produce(uint64_t message_count, const std::atomic<bool>& start,
             const std::atomic<bool>& failed) {
    wait_for_start(start);

    // Keep trying until each sequence number has been accepted by the ring.
    for (uint64_t sequence = 0;
         sequence < message_count && !failed.load(std::memory_order_acquire);) {
        if (ring.push(Payload{.sequence = sequence})) {
            ++sequence;
        } else {
            pause_spin();
        }
    }
}

void consume(uint64_t message_count, const std::atomic<bool>& start, std::atomic<bool>& failed) {
    wait_for_start(start);

    // Every popped payload must be the exact next sequence number.
    for (uint64_t expected = 0; expected < message_count;) {
        Payload payload{};
        if (!ring.pop(payload)) {
            pause_spin();
            continue;
        }

        if (payload.sequence != expected) {
            // Any duplicate, gap, or reordering appears as the first sequence
            // mismatch seen by the consumer.
            std::fprintf(stderr, "sequence mismatch: expected %llu, got %llu\n",
                         static_cast<unsigned long long>(expected),
                         static_cast<unsigned long long>(payload.sequence));
            failed.store(true, std::memory_order_release);
            return;
        }

        ++expected;
    }
}

}  // namespace

int main(int argc, char** argv) {
    uint64_t message_count = kDefaultMessages;
    if (argc > 2 || (argc == 2 && !parse_message_count(argv[1], message_count))) {
        std::fprintf(stderr, "usage: %s [message_count]\n", argv[0]);
        return 2;
    }

    std::atomic<bool> start{false};
    std::atomic<bool> failed{false};

    const auto begin = std::chrono::steady_clock::now();

    std::thread producer{produce, message_count, std::cref(start), std::cref(failed)};
    std::thread consumer{consume, message_count, std::cref(start), std::ref(failed)};

    start.store(true, std::memory_order_release);

    producer.join();
    consumer.join();

    const auto end = std::chrono::steady_clock::now();
    const std::chrono::duration<double> elapsed = end - begin;

    if (failed.load(std::memory_order_acquire)) {
        return 1;
    }

    const double messages_per_second = static_cast<double>(message_count) / elapsed.count();
    std::printf("spsc_ring_stress OK: %llu messages in %.3f s (%.0f msg/s)\n",
                static_cast<unsigned long long>(message_count), elapsed.count(),
                messages_per_second);
    return 0;
}
