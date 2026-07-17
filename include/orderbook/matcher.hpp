// matcher.hpp - Single-threaded matching boundary.
//
// The matcher owns the passive book and turns inbound commands into outbound
// events. It does not know about threads, rings, files, or sockets; callers
// provide a tiny output sink. Tests can record events in an array, while a
// threaded runner can send those same events to an output queue without
// changing the matching logic.

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <optional>
#include <type_traits>

#include "orderbook/book.hpp"
#include "orderbook/config.hpp"
#include "orderbook/types.hpp"

namespace ob {

template <std::size_t NumLevels, std::size_t PoolCapacity, Price BasePrice = config::BASE_PRICE>
class Matcher {
  public:
    using BookType = Book<NumLevels, PoolCapacity, BasePrice>;

    static constexpr Price base_price() noexcept {
        return BasePrice;
    }

    static constexpr Price limit_price() noexcept {
        return BasePrice + static_cast<Price>(NumLevels);
    }

    // Process one inbound command synchronously. The matcher calls emit once
    // for each outbound event caused by this command. Emit is templated so
    // callers can provide allocation-free sinks instead of std::function.
    template <typename Emit>
    void process(const InboundMsg& msg, Emit&& emit) noexcept {
        OperationEvents<std::remove_reference_t<Emit>> events{emit};

        switch (msg.type) {
            case MsgType::NewLimit:
                process_new_limit(msg, events);
                break;
            case MsgType::NewMarket:
                process_new_market(msg, events);
                break;
            case MsgType::Cancel:
                process_cancel(msg, events);
                break;
            case MsgType::StopEngine:
                events.finish(make_stop_engine(msg));
                break;
        }
    }

    BookType& book() noexcept {
        return book_;
    }

    const BookType& book() const noexcept {
        return book_;
    }

  private:
    // Per-command event wrapper. push() emits ordinary events; finish() emits
    // the single RequestComplete event required for every command. Keeping this
    // rule in one helper avoids setting the flag by hand at each branch.
    template <typename Emit>
    class OperationEvents {
      public:
        explicit OperationEvents(Emit& emit) noexcept : emit_(emit) {}

        ~OperationEvents() noexcept {
            assert(completed_);
        }

        void push(const OutboundEvent& event) noexcept {
            assert((event.flags & RequestComplete) == 0);
            emit_(event);
        }

        void finish(const OutboundEvent& event) noexcept {
            assert(!completed_);
            OutboundEvent completed_event = event;
            completed_event.flags = static_cast<uint8_t>(completed_event.flags | RequestComplete);
            completed_ = true;
            emit_(completed_event);
        }

      private:
        Emit& emit_;
        bool completed_{false};
    };

    static bool price_in_band(Price price) noexcept {
        return price >= BasePrice && price < limit_price();
    }

    // Participant 0 means "not assigned". Existing tests and simple tools use
    // that default, so they do not accidentally block every match as self-trade.
    static bool is_self_trade(ParticipantId incoming_id, ParticipantId resting_id) noexcept {
        return incoming_id != 0 && resting_id != 0 && incoming_id == resting_id;
    }

    // Start every event from the inbound request identity. Specific builders
    // fill in the fields meaningful for each event type. tsc_egress is left at
    // zero until timing support is added.
    static OutboundEvent base_event(const InboundMsg& msg) noexcept {
        return OutboundEvent{
            .client_seq = msg.client_seq,
            .handle = 0,
            .price = 0,
            .qty = 0,
            .side = msg.side,
            .type = EventType::Reject,
            .reason = RejectReason::None,
            .flags = 0,
            .tsc_intended = msg.tsc_intended,
            .tsc_egress = 0,
        };
    }

    // AckNew reports the handle and quantity of a newly resting order.
    static OutboundEvent make_ack_new(const InboundMsg& msg, Handle handle, Qty rested) noexcept {
        OutboundEvent event = base_event(msg);
        event.handle = handle;
        event.price = msg.price;
        event.qty = rested;
        event.type = EventType::AckNew;
        return event;
    }

    // AckCancel must be built before the order is returned to the pool.
    static OutboundEvent make_ack_cancel(const InboundMsg& msg, const Order& order) noexcept {
        OutboundEvent event = base_event(msg);
        event.handle = order.handle;
        event.price = order.price;
        event.qty = order.remaining;
        event.side = order.side;
        event.type = EventType::AckCancel;
        return event;
    }

    // For remainder rejects, qty is unfilled quantity; for validation rejects,
    // qty is the rejected input quantity.
    static OutboundEvent make_reject(const InboundMsg& msg, RejectReason reason, Qty qty) noexcept {
        OutboundEvent event = base_event(msg);
        event.qty = qty;
        event.type = EventType::Reject;
        event.reason = reason;
        return event;
    }

    // The resting fill describes the older order already in the book.
    static OutboundEvent make_resting_fill(const InboundMsg& msg, const Order& resting,
                                           Qty qty) noexcept {
        OutboundEvent event = base_event(msg);
        event.client_seq = resting.client_seq;
        event.handle = resting.handle;
        event.price = resting.price;
        event.qty = qty;
        event.side = resting.side;
        event.type = EventType::Fill;
        return event;
    }

    // The aggressor fill describes the inbound order; it has no resting handle.
    static OutboundEvent make_aggressor_fill(const InboundMsg& msg, Price price, Qty qty) noexcept {
        OutboundEvent event = base_event(msg);
        event.price = price;
        event.qty = qty;
        event.type = EventType::Fill;
        return event;
    }

    static OutboundEvent make_stop_engine(const InboundMsg& msg) noexcept {
        OutboundEvent event = base_event(msg);
        event.side = Side::Bid;
        event.type = EventType::StopEngine;
        return event;
    }

    // Match a limit order first; rest any remainder at its limit price. A
    // limit buy only trades at its price or lower, and a limit sell only trades
    // at its price or higher.
    template <typename Emit>
    void process_new_limit(const InboundMsg& msg, OperationEvents<Emit>& events) noexcept {
        if (msg.qty == 0) {
            events.finish(make_reject(msg, RejectReason::ZeroQty, msg.qty));
            return;
        }
        if (!price_in_band(msg.price)) {
            events.finish(make_reject(msg, RejectReason::PriceOutOfBand, msg.qty));
            return;
        }

        Qty remaining = msg.qty;
        const auto limit_idx = BookType::price_to_index(msg.price);
        assert(limit_idx.has_value());

        if (msg.time_in_force == TimeInForce::FOK) {
            switch (check_fill_or_kill(msg, *limit_idx)) {
                case FillOrKillCheck::CanFill:
                    break;
                case FillOrKillCheck::NotEnoughQuantity:
                    events.finish(make_reject(msg, RejectReason::FillOrKillNotFilled, msg.qty));
                    return;
                case FillOrKillCheck::SelfTrade:
                    events.finish(make_reject(msg, RejectReason::SelfTrade, msg.qty));
                    return;
            }
        }

        if (cross(msg, limit_idx, remaining, events)) {
            return;
        }

        if (msg.time_in_force == TimeInForce::IOC) {
            events.finish(make_reject(msg, RejectReason::ImmediateOrCancelRemainder, remaining));
            return;
        }

        const auto handle =
            book_.insert(msg.side, msg.price, remaining, msg.client_seq, msg.participant_id);
        if (!handle) {
            assert(book_.pool().free_head_for_audit() == NULL_SLOT);
            events.finish(make_reject(msg, RejectReason::PoolExhausted, remaining));
            return;
        }

        events.finish(make_ack_new(msg, *handle, remaining));
    }

    // Match a market order until filled or the opposite side is empty. Market
    // orders have no price limit and never rest in the book.
    template <typename Emit>
    void process_new_market(const InboundMsg& msg, OperationEvents<Emit>& events) noexcept {
        if (msg.qty == 0) {
            events.finish(make_reject(msg, RejectReason::ZeroQty, msg.qty));
            return;
        }

        Qty remaining = msg.qty;
        if (cross(msg, std::nullopt, remaining, events)) {
            return;
        }

        events.finish(make_reject(msg, RejectReason::InsufficientLiquidity, remaining));
    }

    // Remove a resting order by handle. Bad, stale, and already-filled handles
    // all become UnknownHandle rejects.
    template <typename Emit>
    void process_cancel(const InboundMsg& msg, OperationEvents<Emit>& events) noexcept {
        const Order* order = book_.pool().resolve(msg.handle);
        if (order == nullptr) {
            events.finish(make_reject(msg, RejectReason::UnknownHandle, msg.qty));
            return;
        }

        const OutboundEvent ack = make_ack_cancel(msg, *order);
        const bool removed = book_.remove(msg.handle);
        (void)removed;
        assert(removed);
        events.finish(ack);
    }

    // Walk the opposite side best-first and emit fill pairs. limit_idx contains
    // the incoming limit price as a level index; std::nullopt means market
    // order. Returns true when this function emitted the command's
    // RequestComplete event, and false when the caller still has a remainder to
    // rest or reject.
    template <typename Emit>
    bool cross(const InboundMsg& msg, std::optional<std::size_t> limit_idx, Qty& remaining,
               OperationEvents<Emit>& events) noexcept {
        while (remaining > 0) {
            const Side opposite = msg.side == Side::Bid ? Side::Ask : Side::Bid;
            const auto best = book_.best_idx(opposite);
            // Stop when the book is empty or a limit order's price no longer
            // reaches the best available opposite price.
            if (!best || (limit_idx && !crosses(msg.side, *limit_idx, *best))) {
                return false;
            }

            const Level& level = book_.level(*best);
            assert(level.head != NULL_SLOT);
            const Order& resting = book_.pool().at(level.head);
            if (is_self_trade(msg.participant_id, resting.participant_id)) {
                events.finish(make_reject(msg, RejectReason::SelfTrade, remaining));
                return true;
            }

            const Qty fill_qty = std::min(remaining, resting.remaining);

            OutboundEvent resting_fill = make_resting_fill(msg, resting, fill_qty);
            OutboundEvent aggressor_fill =
                make_aggressor_fill(msg, BookType::index_to_price(*best), fill_qty);

            // Build both events before mutating the book. A complete fill frees
            // the resting order, so reading its fields afterwards would be a
            // use-after-free bug in disguise.
            book_.fill_head(*best, fill_qty);
            remaining -= fill_qty;

            events.push(resting_fill);
            if (remaining == 0) {
                events.finish(aggressor_fill);
                return true;
            }
            events.push(aggressor_fill);
        }

        return false;
    }

    enum class FillOrKillCheck {
        CanFill,
        NotEnoughQuantity,
        SelfTrade,
    };

    FillOrKillCheck check_fill_or_kill(const InboundMsg& msg,
                                       std::size_t limit_idx) const noexcept {
        AggQty available = 0;

        if (msg.side == Side::Bid) {
            for (std::size_t idx = 0; idx <= limit_idx; ++idx) {
                const FillOrKillCheck result = add_fillable_level(msg, idx, available);
                if (result != FillOrKillCheck::NotEnoughQuantity) {
                    return result;
                }
            }
            return FillOrKillCheck::NotEnoughQuantity;
        }

        for (std::size_t idx = NumLevels; idx-- > limit_idx;) {
            const FillOrKillCheck result = add_fillable_level(msg, idx, available);
            if (result != FillOrKillCheck::NotEnoughQuantity) {
                return result;
            }
        }
        return FillOrKillCheck::NotEnoughQuantity;
    }

    FillOrKillCheck add_fillable_level(const InboundMsg& msg, std::size_t idx,
                                       AggQty& available) const noexcept {
        if (!book_.occupied(idx)) {
            return FillOrKillCheck::NotEnoughQuantity;
        }

        const Level& level = book_.level(idx);
        if (level.head == NULL_SLOT) {
            return FillOrKillCheck::NotEnoughQuantity;
        }
        const Side opposite = msg.side == Side::Bid ? Side::Ask : Side::Bid;
        if (book_.pool().at(level.head).side != opposite) {
            return FillOrKillCheck::NotEnoughQuantity;
        }

        // Walk FIFO order. A same-participant order blocks the incoming order;
        // the matcher must not skip it to reach later orders at the same price.
        uint32_t slot = level.head;
        while (slot != NULL_SLOT) {
            const Order& order = book_.pool().at(slot);
            if (is_self_trade(msg.participant_id, order.participant_id)) {
                return FillOrKillCheck::SelfTrade;
            }
            available += order.remaining;
            if (available >= msg.qty) {
                return FillOrKillCheck::CanFill;
            }
            slot = order.next;
        }

        return FillOrKillCheck::NotEnoughQuantity;
    }

    // Return whether an incoming limit order can trade at opposite_idx. Indices
    // increase with price, so a bid crosses when bid_idx >= ask_idx; an ask
    // crosses when ask_idx <= bid_idx.
    static bool crosses(Side aggressor_side, std::size_t limit_idx,
                        std::size_t opposite_idx) noexcept {
        if (aggressor_side == Side::Bid) {
            return limit_idx >= opposite_idx;
        }
        return limit_idx <= opposite_idx;
    }

    BookType book_{};
};

}  // namespace ob
