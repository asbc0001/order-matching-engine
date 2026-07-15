// spsc_ring_test.cpp - Single-thread checks for the SPSC ring buffer.

#include <array>
#include <cstdint>
#include <cstdio>
#include <type_traits>

#include "orderbook/spsc_ring.hpp"

namespace {

// Keep the payload minimal so failures point at ring ordering/capacity, not
// payload behavior.
struct Payload {
    uint64_t sequence;
};

static_assert(std::is_trivially_copyable_v<Payload>);

bool fail(const char* message) {
    std::fprintf(stderr, "%s\n", message);
    return false;
}

bool check_fill_and_drain_exact_capacity() {
    constexpr std::size_t kCapacity = 8;
    ob::SpscRing<Payload, kCapacity> ring;

    for (uint64_t sequence = 0; sequence < kCapacity; ++sequence) {
        if (!ring.push(Payload{.sequence = sequence})) {
            return fail("Ring rejected before exact capacity");
        }
    }
    if (ring.push(Payload{.sequence = kCapacity})) {
        return fail("Ring accepted beyond exact capacity");
    }

    for (uint64_t expected = 0; expected < kCapacity; ++expected) {
        Payload payload{};
        if (!ring.pop(payload) || payload.sequence != expected) {
            return fail("Ring drain order mismatch");
        }
    }

    Payload payload{};
    return !ring.pop(payload) ? true : fail("Ring pop succeeded after drain");
}

// Interleave pushes and pops instead of filling/draining in one block. This
// moves the indices around the small ring many times while preserving FIFO
// order.
bool check_interleaved_wraparound() {
    constexpr std::size_t kCapacity = 8;
    constexpr std::size_t kCycles = 512;
    ob::SpscRing<Payload, kCapacity> ring;

    uint64_t next_push = 0;
    uint64_t next_pop = 0;

    auto push_one = [&]() {
        if (!ring.push(Payload{.sequence = next_push})) {
            return false;
        }
        ++next_push;
        return true;
    };
    auto pop_one = [&]() {
        Payload payload{};
        if (!ring.pop(payload) || payload.sequence != next_pop) {
            return false;
        }
        ++next_pop;
        return true;
    };

    for (std::size_t cycle = 0; cycle < kCycles; ++cycle) {
        for (int i = 0; i < 4; ++i) {
            if (!push_one()) {
                return fail("Interleaved push failed");
            }
        }
        for (int i = 0; i < 2; ++i) {
            if (!pop_one()) {
                return fail("Interleaved pop failed");
            }
        }
        for (int i = 0; i < 2; ++i) {
            if (!push_one()) {
                return fail("Interleaved second push failed");
            }
        }
        for (int i = 0; i < 4; ++i) {
            if (!pop_one()) {
                return fail("Interleaved final pop failed");
            }
        }
    }

    Payload payload{};
    return next_push == next_pop && !ring.pop(payload) ? true
                                                       : fail("Interleaved wraparound failed");
}

}  // namespace

int main() {
    using Check = bool (*)();
    constexpr std::array<Check, 2> checks{
        check_fill_and_drain_exact_capacity,
        check_interleaved_wraparound,
    };

    for (Check check : checks) {
        if (!check()) {
            return 1;
        }
    }

    std::printf("spsc_ring_test OK\n");
    return 0;
}
