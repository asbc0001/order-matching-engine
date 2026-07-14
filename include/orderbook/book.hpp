// book.hpp - Passive book structures and intrusive level-list operations.
//
// Matching semantics land later. This header starts with the owned, packed
// data structures the matching thread will mutate directly.

#pragma once

#include <array>
#include <bitset>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <optional>

#include "orderbook/bitmap.hpp"
#include "orderbook/config.hpp"
#include "orderbook/pool.hpp"
#include "orderbook/types.hpp"

namespace ob {

// One price level's FIFO queue. The book stores slot indices rather than
// pointers so pool storage can move through handles/free-list logic without
// invalidating links.
struct Level {
    uint32_t head{NULL_SLOT};  // Oldest resting order at this price.
    uint32_t tail{NULL_SLOT};  // Newest resting order at this price.
    AggQty total_qty{0};       // Sum of Order::remaining across the list.
    uint32_t order_count{0};
};

// Append assumes the caller has already populated price, side, and remaining
// on the order. total_qty is accumulated from the current remaining value.
template <std::size_t Capacity>
void append_tail(Pool<Capacity>& pool, Level& level, uint32_t slot) noexcept {
    Order& order = pool.at(slot);
    assert(order.handle != 0);
    assert(order.remaining > 0);
    assert(order.prev == NULL_SLOT);
    assert(order.next == NULL_SLOT);
    assert((level.order_count == 0) == (level.head == NULL_SLOT && level.tail == NULL_SLOT));

    // New arrivals join the back of the FIFO queue to preserve time priority
    // within a price level.
    order.prev = level.tail;
    order.next = NULL_SLOT;

    if (level.tail == NULL_SLOT) {
        level.head = slot;
    } else {
        pool.at(level.tail).next = slot;
    }

    level.tail = slot;
    level.total_qty += order.remaining;
    ++level.order_count;
}

// Unlink assumes slot is currently a member of level and has not already been
// unlinked. Later audit machinery will prove whole-list membership globally.
template <std::size_t Capacity>
void unlink(Pool<Capacity>& pool, Level& level, uint32_t slot) noexcept {
    assert(level.order_count > 0);

    Order& order = pool.at(slot);
    assert(order.handle != 0);
    assert(level.total_qty >= order.remaining);
    assert(slot == level.head || order.prev != NULL_SLOT);
    assert(slot == level.tail || order.next != NULL_SLOT);

    const uint32_t prev = order.prev;
    const uint32_t next = order.next;

    // Repair the neighboring links first, then scrub the removed order's own
    // links so accidental reuse is easier to spot while debugging.
    if (prev == NULL_SLOT) {
        assert(level.head == slot);
        level.head = next;
    } else {
        pool.at(prev).next = next;
    }

    if (next == NULL_SLOT) {
        assert(level.tail == slot);
        level.tail = prev;
    } else {
        pool.at(next).prev = prev;
    }

    order.prev = NULL_SLOT;
    order.next = NULL_SLOT;
    level.total_qty -= order.remaining;
    --level.order_count;

    assert((level.order_count == 0) == (level.head == NULL_SLOT && level.tail == NULL_SLOT));
}

template <std::size_t Capacity>
[[nodiscard]] std::optional<uint32_t> pop_head(Pool<Capacity>& pool, Level& level) noexcept {
    // Matching will consume resting orders from the head, so this is just the
    // FIFO-specific wrapper around unlink.
    if (level.head == NULL_SLOT) {
        assert(level.tail == NULL_SLOT);
        assert(level.order_count == 0);
        assert(level.total_qty == 0);
        return std::nullopt;
    }

    const uint32_t slot = level.head;
    unlink(pool, level, slot);
    return slot;
}

template <std::size_t NumLevels, std::size_t PoolCapacity, Price BasePrice = config::BASE_PRICE>
class Book {
    static_assert(NumLevels > 0, "Book must contain at least one price level");
    static_assert((NumLevels % kBitmapWordBits) == 0,
                  "Book level count must match the bitmap word layout");
    static_assert(PoolCapacity > 0, "Book pool capacity must be nonzero");

  public:
    Book() noexcept {
        // Touch every level at construction so the passive book state is fully
        // initialized before the matching path starts mutating it.
        for (Level& level : levels_) {
            level = Level{};
        }
    }

    static constexpr std::size_t num_levels() noexcept {
        return NumLevels;
    }

    static constexpr std::size_t pool_capacity() noexcept {
        return PoolCapacity;
    }

    // Structural rest operation only: Phase 3 will wrap this with matching and
    // event/reject semantics. A null result means the order did not rest.
    [[nodiscard]] std::optional<Handle> insert(Side side, Price price, Qty qty,
                                               uint64_t client_seq) noexcept {
        const auto idx = price_to_index(price);
        if (!idx || qty == 0 || would_cross(side, *idx)) {
            return std::nullopt;
        }

        Level& target = levels_[*idx];
        // levels_ is shared by both sides. In an uncrossed passive book, an
        // occupied level can only ever contain orders from one side.
        if (target.order_count > 0 && pool_.at(target.head).side != side) {
            return std::nullopt;
        }

        auto alloc = pool_.alloc();
        if (!alloc) {
            return std::nullopt;
        }

        Order& order = *alloc->order;
        order.client_seq = client_seq;
        order.price = price;
        order.remaining = qty;
        order.side = side;

        const bool was_empty = target.order_count == 0;
        append_tail(pool_, target, alloc->slot);
        if (was_empty) {
            bitmap_.set(*idx);
        }
        update_best_on_insert(side, *idx);
        assert(check_local(*idx));

        return alloc->handle;
    }

    [[nodiscard]] bool remove(Handle handle) noexcept {
        Order* order = pool_.resolve(handle);
        if (order == nullptr) {
            return false;
        }

        const auto idx = price_to_index(order->price);
        assert(idx.has_value());

        const Side side = order->side;
        const uint32_t slot = static_cast<uint32_t>(handle);
        Level& level = levels_[*idx];

        unlink(pool_, level, slot);
        if (level.order_count == 0) {
            bitmap_.clear(*idx);
            update_best_on_empty(side, *idx);
        }

        pool_.free(slot);
        assert(check_local(*idx));
        return true;
    }

    // Cheap local invariant entry point for operation-adjacent checks. Passing
    // a level index verifies that level's bitmap/list endpoints plus global
    // best-pointer sanity.
    [[nodiscard]] bool check_local(std::size_t idx) const noexcept {
        assert(idx < NumLevels);
        return check_level_local(idx) && check_best_local();
    }

    [[nodiscard]] bool check_local() const noexcept {
        return check_best_local();
    }

    // Full structural audit. This is intentionally heavier than check_local():
    // it walks every level, every resting order, and the pool free list.
    [[nodiscard]] bool audit() const noexcept {
        std::bitset<PoolCapacity> seen_slots;
        std::optional<std::size_t> expected_best_bid;
        std::optional<std::size_t> expected_best_ask;
        std::size_t allocated_count = 0;

        for (std::size_t idx = 0; idx < NumLevels; ++idx) {
            // First prove the level and bitmap agree on whether this price is
            // occupied. Empty levels must be completely scrubbed.
            const Level& current = levels_[idx];
            const bool occupied_level = current.order_count > 0;
            if (bitmap_.test(idx) != occupied_level) {
                return false;
            }
            if (!occupied_level) {
                if (current.head != NULL_SLOT || current.tail != NULL_SLOT ||
                    current.total_qty != 0) {
                    return false;
                }
                continue;
            }

            if (!check_level_local(idx)) {
                return false;
            }

            // Track the true extrema while walking; cached best pointers are
            // checked against these derived values after the level pass.
            const Side side = pool_.at(current.head).side;
            if (side == Side::Bid) {
                if (!expected_best_bid || idx > *expected_best_bid) {
                    expected_best_bid = idx;
                }
            } else if (!expected_best_ask || idx < *expected_best_ask) {
                expected_best_ask = idx;
            }

            AggQty total_qty = 0;
            uint32_t count = 0;
            uint32_t previous = NULL_SLOT;
            uint32_t slot = current.head;

            // Forward traversal verifies membership, price/side consistency,
            // aggregate quantity, count, prev links, and duplicate/cycle bugs.
            while (slot != NULL_SLOT) {
                if (slot >= PoolCapacity || seen_slots.test(slot)) {
                    return false;
                }

                const Order& order = pool_.at(slot);
                if (order.handle == 0 || order.price != index_to_price(idx) || order.side != side ||
                    order.prev != previous) {
                    return false;
                }

                seen_slots.set(slot);
                total_qty += order.remaining;
                ++count;
                previous = slot;
                slot = order.next;
            }

            if (previous != current.tail || count != current.order_count ||
                total_qty != current.total_qty) {
                return false;
            }

            uint32_t next = NULL_SLOT;
            slot = current.tail;
            uint32_t reverse_count = 0;
            // Backward traversal is separate so a list with correct forward
            // links but stale reverse links cannot pass the audit.
            while (slot != NULL_SLOT) {
                if (slot >= PoolCapacity || ++reverse_count > current.order_count) {
                    return false;
                }

                const Order& order = pool_.at(slot);
                if (order.next != next) {
                    return false;
                }
                next = slot;
                slot = order.prev;
            }
            if (next != current.head || reverse_count != current.order_count) {
                return false;
            }

            allocated_count += count;
        }

        if (best_bid_idx_ != expected_best_bid || best_ask_idx_ != expected_best_ask) {
            return false;
        }
        if (!check_best_local()) {
            return false;
        }

        // Finally walk the pool free list. Each slot must appear exactly once
        // across resting level lists and the free list.
        std::size_t free_count = 0;
        uint32_t slot = pool_.free_head_for_audit();
        while (slot != NULL_SLOT) {
            if (slot >= PoolCapacity || seen_slots.test(slot)) {
                return false;
            }

            const Order& order = pool_.at(slot);
            if (order.handle != 0) {
                return false;
            }

            seen_slots.set(slot);
            ++free_count;
            slot = order.next;
        }

        return allocated_count + free_count == PoolCapacity && seen_slots.all();
    }

    [[nodiscard]] std::optional<std::size_t> best_bid_idx() const noexcept {
        return best_bid_idx_;
    }

    [[nodiscard]] std::optional<std::size_t> best_ask_idx() const noexcept {
        return best_ask_idx_;
    }

    [[nodiscard]] const Level& level(std::size_t idx) const noexcept {
        assert(idx < NumLevels);
        return levels_[idx];
    }

    [[nodiscard]] bool occupied(std::size_t idx) const noexcept {
        assert(idx < NumLevels);
        return bitmap_.test(idx);
    }

    [[nodiscard]] Pool<PoolCapacity>& pool() noexcept {
        return pool_;
    }

    [[nodiscard]] const Pool<PoolCapacity>& pool() const noexcept {
        return pool_;
    }

  private:
    [[nodiscard]] static Price index_to_price(std::size_t idx) noexcept {
        return BasePrice + static_cast<Price>(idx);
    }

    [[nodiscard]] static std::optional<std::size_t> price_to_index(Price price) noexcept {
        if (price < BasePrice) {
            return std::nullopt;
        }

        const auto idx = static_cast<std::size_t>(price - BasePrice);
        if (idx >= NumLevels) {
            return std::nullopt;
        }
        return idx;
    }

    [[nodiscard]] bool check_level_local(std::size_t idx) const noexcept {
        const Level& current = levels_[idx];
        const bool occupied_level = current.order_count > 0;
        // Local checks intentionally stop at endpoints; full traversal belongs
        // to audit(), which is scheduled less frequently.
        if (bitmap_.test(idx) != occupied_level) {
            return false;
        }

        if (!occupied_level) {
            return current.head == NULL_SLOT && current.tail == NULL_SLOT && current.total_qty == 0;
        }

        if (current.head == NULL_SLOT || current.tail == NULL_SLOT || current.total_qty == 0) {
            return false;
        }

        const Order& head = pool_.at(current.head);
        const Order& tail = pool_.at(current.tail);
        return head.prev == NULL_SLOT && tail.next == NULL_SLOT && head.handle != 0 &&
               tail.handle != 0;
    }

    [[nodiscard]] bool check_best_local() const noexcept {
        if (best_bid_idx_) {
            if (*best_bid_idx_ >= NumLevels || !bitmap_.test(*best_bid_idx_)) {
                return false;
            }
            const Level& best_bid = levels_[*best_bid_idx_];
            if (best_bid.head == NULL_SLOT || pool_.at(best_bid.head).side != Side::Bid) {
                return false;
            }
        }

        if (best_ask_idx_) {
            if (*best_ask_idx_ >= NumLevels || !bitmap_.test(*best_ask_idx_)) {
                return false;
            }
            const Level& best_ask = levels_[*best_ask_idx_];
            if (best_ask.head == NULL_SLOT || pool_.at(best_ask.head).side != Side::Ask) {
                return false;
            }
        }

        return !best_bid_idx_ || !best_ask_idx_ || *best_bid_idx_ < *best_ask_idx_;
    }

    [[nodiscard]] bool would_cross(Side side, std::size_t idx) const noexcept {
        // Phase 2 only rests passive orders. If an order would trade through
        // the opposite best, Phase 3 matching must consume it before rest.
        if (side == Side::Bid) {
            return best_ask_idx_.has_value() && idx >= *best_ask_idx_;
        }
        return best_bid_idx_.has_value() && idx <= *best_bid_idx_;
    }

    void update_best_on_insert(Side side, std::size_t idx) noexcept {
        if (side == Side::Bid) {
            if (!best_bid_idx_ || idx > *best_bid_idx_) {
                best_bid_idx_ = idx;
            }
            return;
        }

        if (!best_ask_idx_ || idx < *best_ask_idx_) {
            best_ask_idx_ = idx;
        }
    }

    void update_best_on_empty(Side side, std::size_t emptied_idx) noexcept {
        if (side == Side::Bid) {
            if (best_bid_idx_ && *best_bid_idx_ == emptied_idx) {
                best_bid_idx_ = bitmap_.scan_down(emptied_idx);
            }
            return;
        }

        if (best_ask_idx_ && *best_ask_idx_ == emptied_idx) {
            best_ask_idx_ = bitmap_.scan_up(emptied_idx);
        }
    }

    std::array<Level, NumLevels> levels_{};
    Bitmap<NumLevels> bitmap_{};
    Pool<PoolCapacity> pool_{};
    std::optional<std::size_t> best_bid_idx_{};
    std::optional<std::size_t> best_ask_idx_{};
};

}  // namespace ob
