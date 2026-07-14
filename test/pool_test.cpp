// pool_test.cpp - Focused checks for the fixed-capacity order pool.

#include <cstdio>
#include <random>
#include <set>
#include <vector>

#include "orderbook/pool.hpp"
#include "orderbook/types.hpp"

namespace {

[[nodiscard]] bool fail(const char* message) {
    std::fprintf(stderr, "%s\n", message);
    return false;
}

// Covers the direct, hand-written edge cases that should be easy to reason
// about before the randomized churn test runs.
[[nodiscard]] bool check_basic_pool_behavior() {
    ob::Pool<4> pool;
    if (pool.capacity() != 4) {
        return fail("Pool capacity mismatch");
    }

    // Allocation starts at slot 0, stamps a nonzero generational handle, and
    // stores the same handle on the order itself.
    auto first_alloc = pool.alloc();
    if (!first_alloc || first_alloc->slot != 0 || first_alloc->handle == 0 ||
        first_alloc->order->handle != first_alloc->handle) {
        return fail("Pool first allocation failed");
    }
    if (first_alloc->handle != (ob::Handle{1} << 32)) {
        return fail("Pool first handle mismatch");
    }

    // Live handles resolve to their order; malformed handles resolve to null.
    if (pool.resolve(first_alloc->handle) != first_alloc->order) {
        return fail("Pool valid resolve failed");
    }
    if (pool.resolve(0) != nullptr) {
        return fail("Pool null-handle resolve failed");
    }
    const ob::Handle out_of_range_handle = (ob::Handle{1} << 32) | ob::Handle{99};
    if (pool.resolve(out_of_range_handle) != nullptr) {
        return fail("Pool out-of-range resolve failed");
    }

    // The initial free list is ordered by increasing slot index.
    for (uint32_t expected_slot = 1; expected_slot < 4; ++expected_slot) {
        auto alloc = pool.alloc();
        if (!alloc || alloc->slot != expected_slot) {
            return fail("Pool allocation order mismatch");
        }
    }
    if (pool.alloc()) {
        return fail("Pool exhaustion check failed");
    }

    // Freed slots are reused first, with a bumped generation that invalidates
    // stale handles.
    pool.free(first_alloc->slot);
    auto reused_alloc = pool.alloc();
    if (!reused_alloc || reused_alloc->slot != first_alloc->slot) {
        return fail("Pool LIFO reuse failed");
    }
    if (reused_alloc->handle == first_alloc->handle ||
        reused_alloc->handle != ((ob::Handle{2} << 32) | first_alloc->slot)) {
        return fail("Pool generation bump failed");
    }
    if (pool.resolve(first_alloc->handle) != nullptr) {
        return fail("Pool stale-handle resolve failed");
    }

    // A handle forged with a free slot's current generation still fails because
    // free slots have handle 0 stored in the order.
    pool.free(1);
    const ob::Handle freed_slot_current_generation = (ob::Handle{2} << 32) | ob::Handle{1};
    if (pool.resolve(freed_slot_current_generation) != nullptr) {
        return fail("Pool free-slot forged resolve failed");
    }

    return true;
}

[[nodiscard]] bool check_generation_wrap() {
    // Generation 0 is never used, even when uint32_t wraparound occurs.
    ob::Pool<1> wrap_pool;
    wrap_pool.set_generation_for_test(0, UINT32_MAX);
    auto near_wrap_alloc = wrap_pool.alloc();
    if (!near_wrap_alloc || near_wrap_alloc->handle != (ob::Handle{UINT32_MAX} << 32)) {
        return fail("Pool near-wrap allocation failed");
    }
    wrap_pool.free(near_wrap_alloc->slot);
    auto wrapped_alloc = wrap_pool.alloc();
    if (!wrapped_alloc || wrapped_alloc->handle != (ob::Handle{1} << 32)) {
        return fail("Pool generation zero-skip failed");
    }
    if (wrap_pool.resolve(near_wrap_alloc->handle) != nullptr) {
        return fail("Pool near-wrap stale handle resolved");
    }

    return true;
}

[[nodiscard]] bool check_slot_access() {
    ob::Pool<2> pool;
    auto alloc = pool.alloc();
    if (!alloc) {
        return fail("Slot access setup allocation failed");
    }

    // Slot-indexed access is what the book uses while walking intrusive lists;
    // it should address the same storage returned by allocation.
    if (&pool.at(alloc->slot) != alloc->order) {
        return fail("Mutable slot access did not return allocated order");
    }
    pool.at(alloc->slot).remaining = 123;

    const ob::Pool<2>& const_pool = pool;
    if (&const_pool.at(alloc->slot) != alloc->order ||
        const_pool.at(alloc->slot).remaining != 123) {
        return fail("Const slot access did not see allocated order");
    }

    return true;
}

[[nodiscard]] bool check_randomized_churn() {
    struct LiveOrder {
        uint32_t slot;
        ob::Handle handle;
    };

    constexpr std::size_t kCapacity = 64;
    constexpr std::size_t kOperations = 250'000;

    ob::Pool<kCapacity> pool;

    // The oracle tracks which handles should be live independently of the
    // pool's own storage. If the pool loses, duplicates, or resurrects a
    // handle, the oracle checks below should catch it.
    std::vector<LiveOrder> live;
    live.reserve(kCapacity);
    std::set<ob::Handle> live_handles;
    std::vector<ob::Handle> stale_handles;
    stale_handles.reserve(kOperations / 2);

    std::mt19937_64 rng{0xC0FFEE};

    for (std::size_t op = 0; op < kOperations; ++op) {
        // Keep the operation stream valid while still forcing both boundary
        // states: completely empty and completely full.
        const bool must_alloc = live.empty();
        const bool must_free = live.size() == kCapacity;
        const bool do_alloc = must_alloc || (!must_free && ((rng() & 1u) == 0));

        if (do_alloc) {
            auto alloc = pool.alloc();
            if (!alloc) {
                return fail("Churn alloc failed before capacity");
            }
            if (alloc->handle == 0 || alloc->order->handle != alloc->handle) {
                return fail("Churn alloc produced invalid handle");
            }
            if (!live_handles.insert(alloc->handle).second) {
                return fail("Churn alloc produced duplicate live handle");
            }
            if (pool.resolve(alloc->handle) != alloc->order) {
                return fail("Churn live handle did not resolve");
            }
            live.push_back(LiveOrder{.slot = alloc->slot, .handle = alloc->handle});
        } else {
            const std::size_t index = static_cast<std::size_t>(rng() % live.size());
            const LiveOrder victim = live[index];

            pool.free(victim.slot);
            live_handles.erase(victim.handle);
            stale_handles.push_back(victim.handle);

            live[index] = live.back();
            live.pop_back();

            if (pool.resolve(victim.handle) != nullptr) {
                return fail("Churn stale handle resolved after free");
            }
        }

        if ((op % 4096) == 0) {
            // Periodically audit every live handle instead of only the one
            // touched by this operation.
            for (const LiveOrder& order : live) {
                if (live_handles.find(order.handle) == live_handles.end()) {
                    return fail("Churn oracle lost a live handle");
                }
                if (pool.resolve(order.handle) == nullptr) {
                    return fail("Churn live handle stopped resolving");
                }
            }
        }
    }

    for (const LiveOrder& order : live) {
        pool.free(order.slot);
        if (pool.resolve(order.handle) != nullptr) {
            return fail("Churn final stale handle resolved");
        }
    }
    live.clear();
    live_handles.clear();

    // After draining, every slot should be reusable exactly once before
    // exhaustion. This catches cycles, duplicate free-list entries, and lost
    // slots in the intrusive free list.
    std::set<uint32_t> allocated_slots;
    for (std::size_t i = 0; i < kCapacity; ++i) {
        auto alloc = pool.alloc();
        if (!alloc) {
            return fail("Churn final drain lost a free slot");
        }
        if (!allocated_slots.insert(alloc->slot).second) {
            return fail("Churn final drain reused a slot twice");
        }
        if (pool.resolve(alloc->handle) != alloc->order) {
            return fail("Churn final drain handle did not resolve");
        }
    }
    if (pool.alloc()) {
        return fail("Churn final drain did not exhaust exactly at capacity");
    }

    for (ob::Handle stale_handle : stale_handles) {
        if (pool.resolve(stale_handle) != nullptr) {
            return fail("Churn historical stale handle resolved");
        }
    }

    return true;
}

}  // namespace

int main() {
    if (!check_basic_pool_behavior()) {
        return 1;
    }
    if (!check_generation_wrap()) {
        return 1;
    }
    if (!check_slot_access()) {
        return 1;
    }
    if (!check_randomized_churn()) {
        return 1;
    }

    std::printf("pool_test OK\n");
    return 0;
}
