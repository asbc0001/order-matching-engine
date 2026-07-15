// reference_book.hpp - Slow, simple matcher used for expected test behavior.
//
// This book deliberately uses standard containers and linear scans. It is not
// meant to be fast; it is meant to be independent from the optimized intrusive
// book so tests can compare behavior, not implementation details.

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <optional>
#include <unordered_map>

#include "orderbook/config.hpp"
#include "orderbook/types.hpp"

namespace ob::test {

template <std::size_t NumLevels, std::size_t Capacity, Price BasePrice = config::BASE_PRICE>
class ReferenceBook {
  public:
    ReferenceBook() {
        handle_index_.reserve(Capacity);
    }

    static constexpr Price limit_price() noexcept {
        return BasePrice + static_cast<Price>(NumLevels);
    }

    template <typename Emit>
    void process(const InboundMsg& msg, Emit&& emit) {
        OperationEvents<Emit> events{emit};

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

  private:
    struct RefOrder {
        uint64_t client_seq;
        Handle handle;
        Price price;
        Qty remaining;
        Side side;
    };

    template <typename Emit>
    class OperationEvents {
      public:
        explicit OperationEvents(Emit& emit) : emit_(emit) {}

        ~OperationEvents() {
            assert(completed_);
        }

        void push(const OutboundEvent& event) {
            assert((event.flags & RequestComplete) == 0);
            emit_(event);
        }

        void finish(const OutboundEvent& event) {
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

    using AskBook = std::map<Price, std::deque<RefOrder>>;
    using BidBook = std::map<Price, std::deque<RefOrder>, std::greater<Price>>;

    struct HandleLocation {
        Price price;
        Side side;
    };

    static bool price_in_band(Price price) noexcept {
        return price >= BasePrice && price < limit_price();
    }

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

    static OutboundEvent make_ack_new(const InboundMsg& msg, Handle handle, Qty rested) noexcept {
        OutboundEvent event = base_event(msg);
        event.handle = handle;
        event.price = msg.price;
        event.qty = rested;
        event.type = EventType::AckNew;
        return event;
    }

    static OutboundEvent make_ack_cancel(const InboundMsg& msg, const RefOrder& order) noexcept {
        OutboundEvent event = base_event(msg);
        event.handle = order.handle;
        event.price = order.price;
        event.qty = order.remaining;
        event.side = order.side;
        event.type = EventType::AckCancel;
        return event;
    }

    static OutboundEvent make_reject(const InboundMsg& msg, RejectReason reason, Qty qty) noexcept {
        OutboundEvent event = base_event(msg);
        event.qty = qty;
        event.type = EventType::Reject;
        event.reason = reason;
        return event;
    }

    static OutboundEvent make_resting_fill(const InboundMsg& msg, const RefOrder& resting,
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

    template <typename Emit>
    void process_new_limit(const InboundMsg& msg, OperationEvents<Emit>& events) {
        if (msg.qty == 0) {
            events.finish(make_reject(msg, RejectReason::ZeroQty, msg.qty));
            return;
        }
        if (!price_in_band(msg.price)) {
            events.finish(make_reject(msg, RejectReason::PriceOutOfBand, msg.qty));
            return;
        }

        Qty remaining = msg.qty;
        if (cross(msg, msg.price, remaining, events)) {
            return;
        }

        if (live_count_ == Capacity) {
            events.finish(make_reject(msg, RejectReason::PoolExhausted, remaining));
            return;
        }

        const Handle handle = next_handle_++;
        RefOrder order{
            .client_seq = msg.client_seq,
            .handle = handle,
            .price = msg.price,
            .remaining = remaining,
            .side = msg.side,
        };
        rest(order);
        events.finish(make_ack_new(msg, handle, remaining));
    }

    template <typename Emit>
    void process_new_market(const InboundMsg& msg, OperationEvents<Emit>& events) {
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

    template <typename Emit>
    void process_cancel(const InboundMsg& msg, OperationEvents<Emit>& events) {
        std::optional<RefOrder> removed = remove(msg.handle);
        if (!removed) {
            events.finish(make_reject(msg, RejectReason::UnknownHandle, msg.qty));
            return;
        }

        events.finish(make_ack_cancel(msg, *removed));
    }

    template <typename Emit>
    bool cross(const InboundMsg& msg, std::optional<Price> limit_price, Qty& remaining,
               OperationEvents<Emit>& events) {
        while (remaining > 0) {
            RefOrder* resting = best_opposite(msg.side, limit_price);
            if (resting == nullptr) {
                return false;
            }

            const Qty fill_qty = std::min(remaining, resting->remaining);
            OutboundEvent resting_fill = make_resting_fill(msg, *resting, fill_qty);
            OutboundEvent aggressor_fill = make_aggressor_fill(msg, resting->price, fill_qty);

            fill_resting(*resting, fill_qty);
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

    RefOrder* best_opposite(Side aggressor_side, std::optional<Price> limit_price) {
        if (aggressor_side == Side::Bid) {
            if (asks_.empty()) {
                return nullptr;
            }
            auto it = asks_.begin();
            if (limit_price && it->first > *limit_price) {
                return nullptr;
            }
            return &it->second.front();
        }

        if (bids_.empty()) {
            return nullptr;
        }
        auto it = bids_.begin();
        if (limit_price && it->first < *limit_price) {
            return nullptr;
        }
        return &it->second.front();
    }

    void fill_resting(RefOrder& order, Qty fill_qty) {
        assert(fill_qty > 0 && fill_qty <= order.remaining);
        order.remaining -= fill_qty;
        if (order.remaining == 0) {
            const bool erased = erase(order.handle);
            (void)erased;
            assert(erased);
        }
    }

    void rest(const RefOrder& order) {
        assert(live_count_ < Capacity);
        if (order.side == Side::Bid) {
            bids_[order.price].push_back(order);
        } else {
            asks_[order.price].push_back(order);
        }
        const bool indexed =
            handle_index_.emplace(order.handle, HandleLocation{order.price, order.side}).second;
        (void)indexed;
        assert(indexed);
        ++live_count_;
    }

    std::optional<RefOrder> remove(Handle handle) {
        std::optional<RefOrder> order = find(handle);
        if (!order || !erase(handle)) {
            return std::nullopt;
        }
        return order;
    }

    std::optional<RefOrder> find(Handle handle) const {
        if (handle == 0) {
            return std::nullopt;
        }

        // Cancels should jump to the right level, not scan the whole book.
        const auto location = handle_index_.find(handle);
        if (location == handle_index_.end()) {
            return std::nullopt;
        }

        const auto order = location->second.side == Side::Bid
                               ? find_in(bids_, location->second.price, handle)
                               : find_in(asks_, location->second.price, handle);
        if (order == nullptr) {
            return std::nullopt;
        }

        return *order;
    }

    bool erase(Handle handle) {
        const auto location = handle_index_.find(handle);
        if (location == handle_index_.end()) {
            return false;
        }

        const bool erased = location->second.side == Side::Bid
                                ? erase_from(bids_, location->second.price, handle)
                                : erase_from(asks_, location->second.price, handle);
        if (!erased) {
            return false;
        }

        handle_index_.erase(location);
        --live_count_;
        return true;
    }

    template <typename Book>
    static const RefOrder* find_in(const Book& book, Price price, Handle handle) {
        const auto level = book.find(price);
        if (level == book.end()) {
            return nullptr;
        }

        const auto& orders = level->second;
        const auto order = std::find_if(
            orders.begin(), orders.end(),
            [handle](const RefOrder& candidate) { return candidate.handle == handle; });
        return order == orders.end() ? nullptr : &*order;
    }

    template <typename Book>
    static bool erase_from(Book& book, Price price, Handle handle) {
        auto level = book.find(price);
        if (level == book.end()) {
            return false;
        }

        auto& orders = level->second;
        auto order = std::find_if(
            orders.begin(), orders.end(),
            [handle](const RefOrder& candidate) { return candidate.handle == handle; });
        if (order == orders.end()) {
            return false;
        }

        orders.erase(order);
        if (orders.empty()) {
            book.erase(level);
        }
        return true;
    }

    BidBook bids_;
    AskBook asks_;
    std::unordered_map<Handle, HandleLocation> handle_index_;
    std::size_t live_count_{0};
    Handle next_handle_{1};
};

}  // namespace ob::test
