// spsc_ring.hpp - Fixed-capacity single-producer/single-consumer ring buffer.
//
// Provides a bounded handoff queue for one producer thread and one consumer
// thread, using fixed storage and no heap allocation. Production-sized rings
// should live in static storage or startup heap storage, not on the stack.

#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace ob {

inline constexpr std::size_t CACHE_LINE = 64;  // x86-64 cache-line assumption

template <typename T, std::size_t N>
class SpscRing {
    static_assert(N > 0, "SpscRing capacity must be nonzero");
    static_assert((N & (N - 1)) == 0, "SpscRing capacity must be a power of two");
    static_assert(std::is_trivially_copyable_v<T>, "SpscRing payloads must be trivially copyable");

  public:
    SpscRing() noexcept = default;

    SpscRing(const SpscRing&) = delete;
    SpscRing& operator=(const SpscRing&) = delete;

    static constexpr std::size_t capacity() noexcept {
        return N;
    }

    // Try to enqueue one item. A false return means the ring is full right now;
    // the caller keeps ownership and may retry without losing the value.
    [[nodiscard]] bool push(const T& value) noexcept(std::is_nothrow_copy_assignable_v<T>) {
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

    // Try to dequeue one item. A false return means the ring is empty and `out`
    // is left untouched.
    [[nodiscard]] bool pop(T& out) noexcept(std::is_nothrow_copy_assignable_v<T>) {
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

    // The producer is the only writer of head_, and the consumer is the only
    // writer of tail_. Release stores publish slot writes and freed capacity;
    // acquire loads observe those publications on the opposite thread.
    alignas(CACHE_LINE) std::atomic<uint64_t> head_{0};
    alignas(CACHE_LINE) std::atomic<uint64_t> tail_{0};
    alignas(CACHE_LINE) uint64_t cached_tail_{0};
    alignas(CACHE_LINE) uint64_t cached_head_{0};
    alignas(CACHE_LINE) std::array<T, N> slots_{};
};

}  // namespace ob
