// pool.hpp - Fixed-capacity storage for live orders.
//
// The pool owns a compile-time-sized array of Order objects. Free slots are
// linked through Order::next, so allocation and release are constant-time free
// list operations with no heap allocation on the matching path.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>

#include "orderbook/types.hpp"

namespace ob {

template <std::size_t Capacity>
class Pool {
    static_assert(Capacity > 0, "Pool capacity must be nonzero");
    static_assert(Capacity <= UINT32_MAX, "Pool slots are stored as uint32_t indices");

  public:
    // Information returned when a slot is successfully allocated. The caller
    // gets both the stable handle used by future cancels and direct access to
    // the storage that should be populated.
    struct Allocation {
        uint32_t slot;
        Handle handle;
        Order* order;
    };

    // Build the initial free list and start every slot at generation 1. Writing
    // every slot here also faults in the pool's backing pages at startup, before
    // allocation enters the matching path.
    Pool() noexcept {
        for (std::size_t i = 0; i < Capacity; ++i) {
            orders_[i].handle = 0;
            orders_[i].prev = NULL_SLOT;
            orders_[i].next = (i + 1 < Capacity) ? static_cast<uint32_t>(i + 1) : NULL_SLOT;
            generations_[i] = 1;
        }
    }

    [[nodiscard]] static constexpr std::size_t capacity() noexcept {
        return Capacity;
    }

    // Reserve one free slot and stamp it with the current generational handle.
    // Returns std::nullopt when the fixed pool is exhausted.
    [[nodiscard]] std::optional<Allocation> alloc() noexcept {
        if (free_head_ == NULL_SLOT) {
            return std::nullopt;
        }

        const uint32_t slot = free_head_;
        Order& order = orders_[slot];
        free_head_ = order.next;

        const Handle handle = make_handle(slot);
        order.handle = handle;
        order.prev = NULL_SLOT;
        order.next = NULL_SLOT;

        return Allocation{
            .slot = slot,
            .handle = handle,
            .order = &order,
        };
    }

    // Release a slot back to the pool. Clearing the handle makes stale handles
    // fail immediately, and advancing the generation makes the next allocation
    // from this slot produce a different handle.
    void free(uint32_t slot) noexcept {
        Order& order = orders_[slot];
        order.handle = 0;

        if (++generations_[slot] == 0) {
            generations_[slot] = 1;
        }

        order.prev = NULL_SLOT;
        order.next = free_head_;
        free_head_ = slot;
    }

    // Convert a handle back to its live order. Null handles, out-of-range
    // slots, stale generations, and already-freed slots all resolve to nullptr.
    [[nodiscard]] Order* resolve(Handle handle) noexcept {
        const uint32_t slot = handle_slot(handle);
        if (!is_live_handle(handle, slot)) {
            return nullptr;
        }
        return &orders_[slot];
    }
    [[nodiscard]] const Order* resolve(Handle handle) const noexcept {
        const uint32_t slot = handle_slot(handle);
        if (!is_live_handle(handle, slot)) {
            return nullptr;
        }
        return &orders_[slot];
    }

#ifdef OB_ENABLE_TEST_HOOKS
    // Test hook for forcing rare generation states such as uint32_t wraparound.
    // Production code should only change generations through free().
    void set_generation_for_test(uint32_t slot, uint32_t generation) noexcept {
        generations_[slot] = generation;
    }
#endif

  private:
    // Handles pack the current slot generation in the high 32 bits and the
    // slot index in the low 32 bits.
    [[nodiscard]] Handle make_handle(uint32_t slot) const noexcept {
        return (static_cast<Handle>(generations_[slot]) << 32) | slot;
    }

    [[nodiscard]] static uint32_t handle_slot(Handle handle) noexcept {
        return static_cast<uint32_t>(handle);
    }

    [[nodiscard]] bool is_live_handle(Handle handle, uint32_t slot) const noexcept {
        return handle != 0 && slot < Capacity && orders_[slot].handle == handle;
    }

    std::array<Order, Capacity> orders_{};
    std::array<uint32_t, Capacity> generations_{};
    uint32_t free_head_{0};
};

}  // namespace ob
