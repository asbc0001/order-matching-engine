// spsc_ring_bench.cpp - Ring handoff throughput benchmark and ablations.

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <mutex>
#include <string_view>
#include <thread>
#include <type_traits>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

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
constexpr uint64_t kWarmupMessages = 1'000'000;
constexpr std::size_t kRingCapacity = 65'536;

using PaddedRing = ob::SpscRing<Payload, kRingCapacity>;

// Benchmark-only copy of the SPSC algorithm with cache-line padding removed.
// This isolates the cost of false sharing without changing the production ring.
template <typename T, std::size_t N>
class UnpaddedSpscRing {
    static_assert(N > 0);
    static_assert((N & (N - 1)) == 0);
    static_assert(std::is_trivially_copyable_v<T>);

  public:
    [[nodiscard]] bool push(const T& value) noexcept {
        const uint64_t head = head_.load(std::memory_order_relaxed);

        if (head - cached_tail_ == N) {
            cached_tail_ = tail_.load(std::memory_order_acquire);
            if (head - cached_tail_ == N) {
                return false;
            }
        }

        slots_[slot_index(head)] = value;
        head_.store(head + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool pop(T& out) noexcept {
        const uint64_t tail = tail_.load(std::memory_order_relaxed);

        if (cached_head_ == tail) {
            cached_head_ = head_.load(std::memory_order_acquire);
            if (cached_head_ == tail) {
                return false;
            }
        }

        out = slots_[slot_index(tail)];
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

  private:
    static constexpr std::size_t slot_index(uint64_t index) noexcept {
        return static_cast<std::size_t>(index & (N - 1));
    }

    std::atomic<uint64_t> head_{0};
    std::atomic<uint64_t> tail_{0};
    uint64_t cached_tail_{0};
    uint64_t cached_head_{0};
    std::array<T, N> slots_{};
};

// Same fixed-size array shape as the SPSC ring, but guarded by a mutex. This
// isolates synchronization cost from allocation/container-growth effects.
template <typename T, std::size_t N>
class MutexPreallocatedRing {
    static_assert(N > 0);
    static_assert((N & (N - 1)) == 0);

  public:
    [[nodiscard]] bool push(const T& value) {
        std::lock_guard<std::mutex> lock{mutex_};
        if (head_ - tail_ == N) {
            return false;
        }

        slots_[slot_index(head_)] = value;
        ++head_;
        return true;
    }

    [[nodiscard]] bool pop(T& out) {
        std::lock_guard<std::mutex> lock{mutex_};
        if (head_ == tail_) {
            return false;
        }

        out = slots_[slot_index(tail_)];
        ++tail_;
        return true;
    }

  private:
    static constexpr std::size_t slot_index(uint64_t index) noexcept {
        return static_cast<std::size_t>(index & (N - 1));
    }

    std::mutex mutex_;
    uint64_t head_{0};
    uint64_t tail_{0};
    std::array<T, N> slots_{};
};

// Conventional locking with library-owned blocks. This is useful as a familiar
// baseline, but it includes both mutex cost and deque storage behavior.
template <typename T, std::size_t N>
class MutexDequeRing {
  public:
    [[nodiscard]] bool push(const T& value) {
        std::lock_guard<std::mutex> lock{mutex_};
        if (queue_.size() == N) {
            return false;
        }

        queue_.push_back(value);
        return true;
    }

    [[nodiscard]] bool pop(T& out) {
        std::lock_guard<std::mutex> lock{mutex_};
        if (queue_.empty()) {
            return false;
        }

        out = queue_.front();
        queue_.pop_front();
        return true;
    }

  private:
    std::mutex mutex_;
    std::deque<T> queue_;
};

struct Result {
    double seconds;
    double messages_per_second;
};

struct ThreadPlacement {
    int producer_cpu;
    int consumer_cpu;
};

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

bool parse_cpu(const char* text, int& cpu) {
    errno = 0;
    char* end = nullptr;
    const long parsed = std::strtol(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || parsed < 0) {
        return false;
    }

    cpu = static_cast<int>(parsed);
    return true;
}

bool should_run(std::string_view selected, std::string_view name) noexcept {
    return selected == "all" || selected == name;
}

bool pin_current_thread(int cpu, const char* name) {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);

    const int rc = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    if (rc != 0) {
        std::fprintf(stderr, "failed to pin %s to CPU %d (pthread error %d)\n", name, cpu, rc);
        return false;
    }
    return true;
#else
    (void)cpu;
    (void)name;
    std::fprintf(stderr, "thread pinning is not supported by this benchmark on this platform\n");
    return false;
#endif
}

// All variants use the same producer/consumer loop. The sequence check stays in
// the timed run so a faster broken queue cannot produce a benchmark row.
template <typename Queue>
Result run_queue(Queue& queue, uint64_t message_count, ThreadPlacement placement, bool& ok) {
    std::atomic<bool> start{false};
    std::atomic<bool> failed{false};
    std::atomic<int> ready{0};

    auto producer = [&]() {
        if (!pin_current_thread(placement.producer_cpu, "producer")) {
            failed.store(true, std::memory_order_release);
            ready.fetch_add(1, std::memory_order_release);
            return;
        }
        ready.fetch_add(1, std::memory_order_release);

        while (!start.load(std::memory_order_acquire)) {
            pause_spin();
        }

        for (uint64_t sequence = 0;
             sequence < message_count && !failed.load(std::memory_order_acquire);) {
            if (queue.push(Payload{.sequence = sequence})) {
                ++sequence;
            } else {
                pause_spin();
            }
        }
    };

    auto consumer = [&]() {
        if (!pin_current_thread(placement.consumer_cpu, "consumer")) {
            failed.store(true, std::memory_order_release);
            ready.fetch_add(1, std::memory_order_release);
            return;
        }
        ready.fetch_add(1, std::memory_order_release);

        while (!start.load(std::memory_order_acquire)) {
            pause_spin();
        }

        for (uint64_t expected = 0; expected < message_count;) {
            Payload payload{};
            if (!queue.pop(payload)) {
                pause_spin();
                continue;
            }

            if (payload.sequence != expected) {
                std::fprintf(stderr, "sequence mismatch: expected %llu, got %llu\n",
                             static_cast<unsigned long long>(expected),
                             static_cast<unsigned long long>(payload.sequence));
                failed.store(true, std::memory_order_release);
                return;
            }

            ++expected;
        }
    };

    std::thread producer_thread{producer};
    std::thread consumer_thread{consumer};

    while (ready.load(std::memory_order_acquire) != 2) {
        pause_spin();
    }

    const auto begin = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);

    producer_thread.join();
    consumer_thread.join();
    const auto end = std::chrono::steady_clock::now();

    const std::chrono::duration<double> elapsed = end - begin;
    ok = !failed.load(std::memory_order_acquire);
    return Result{
        .seconds = elapsed.count(),
        .messages_per_second = static_cast<double>(message_count) / elapsed.count(),
    };
}

template <typename Queue>
bool run_and_print(const char* name, Queue& queue, uint64_t message_count,
                   ThreadPlacement placement) {
    bool warmup_ok = false;
    (void)run_queue(queue, kWarmupMessages, placement, warmup_ok);
    if (!warmup_ok) {
        return false;
    }

    bool ok = false;
    const Result result = run_queue(queue, message_count, placement, ok);
    if (!ok) {
        return false;
    }

    std::printf("%s,%llu,%.6f,%.0f\n", name, static_cast<unsigned long long>(message_count),
                result.seconds, result.messages_per_second);
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    uint64_t message_count = kDefaultMessages;
    ThreadPlacement placement{
        .producer_cpu = 0,
        .consumer_cpu = 1,
    };
    std::string_view selected_variant = "all";

    if (argc > 5 || (argc >= 2 && !parse_message_count(argv[1], message_count)) ||
        (argc >= 3 && !parse_cpu(argv[2], placement.producer_cpu)) ||
        (argc >= 4 && !parse_cpu(argv[3], placement.consumer_cpu))) {
        std::fprintf(stderr, "usage: %s [message_count [producer_cpu consumer_cpu [variant]]]\n",
                     argv[0]);
        return 2;
    }
    if (argc == 5) {
        selected_variant = argv[4];
    }

    std::printf("variant,messages,seconds,msg_per_second\n");

    // Static storage keeps the large fixed-size variants off the stack, matching
    // how production-sized rings are expected to be owned.
    static PaddedRing padded_ring;
    static UnpaddedSpscRing<Payload, kRingCapacity> unpadded_ring;
    static MutexPreallocatedRing<Payload, kRingCapacity> mutex_preallocated_ring;
    static MutexDequeRing<Payload, kRingCapacity> mutex_deque_ring;

    bool ran_any = false;
    bool ok = true;
    if (should_run(selected_variant, "padded_spsc_ring")) {
        ran_any = true;
        ok = ok && run_and_print("padded_spsc_ring", padded_ring, message_count, placement);
    }
    if (should_run(selected_variant, "unpadded_spsc_ring")) {
        ran_any = true;
        ok = ok && run_and_print("unpadded_spsc_ring", unpadded_ring, message_count, placement);
    }
    if (should_run(selected_variant, "mutex_preallocated_ring")) {
        ran_any = true;
        ok = ok && run_and_print("mutex_preallocated_ring", mutex_preallocated_ring, message_count,
                                 placement);
    }
    if (should_run(selected_variant, "mutex_deque")) {
        ran_any = true;
        ok = ok && run_and_print("mutex_deque", mutex_deque_ring, message_count, placement);
    }

    if (!ran_any) {
        std::fprintf(stderr, "unknown variant: %.*s\n", static_cast<int>(selected_variant.size()),
                     selected_variant.data());
        return 2;
    }
    if (!ok) {
        return 1;
    }

    return 0;
}
