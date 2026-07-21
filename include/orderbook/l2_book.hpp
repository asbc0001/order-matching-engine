// l2_book.hpp - Aggregated market-data view rebuilt from matcher events.
//
// "L2" means price-level data: total resting quantity at each visible price,
// not the individual orders inside the matching book. This class lives outside
// the matcher and updates only from OutboundEvent records, the same stream an
// external market-data feed will see.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <vector>

#include "orderbook/types.hpp"

namespace ob {

struct L2Level {
    Price price{0};
    AggQty qty{0};
};

class L2Book {
  public:
    void apply(const OutboundEvent& event) {
        switch (event.type) {
            case EventType::AckNew:
                add_level_qty(event.side, event.price, event.qty);
                break;
            case EventType::AckCancel:
                remove_level_qty(event.side, event.price, event.qty);
                break;
            case EventType::Fill:
                // The matcher emits two fill events: one for the resting order
                // and one for the incoming order. Only the resting-order event
                // has a handle, and only that quantity was already visible in L2.
                if (event.handle != 0) {
                    remove_level_qty(event.side, event.price, event.qty);
                }
                break;
            case EventType::Reject:
            case EventType::StopEngine:
                break;
        }
    }

    [[nodiscard]] AggQty quantity_at(Side side, Price price) const {
        const auto& levels = side == Side::Bid ? bids_ : asks_;
        const auto found = levels.find(price);
        return found == levels.end() ? 0 : found->second;
    }

    [[nodiscard]] std::vector<L2Level> top(Side side, std::size_t depth) const {
        std::vector<L2Level> result;
        result.reserve(depth);

        if (side == Side::Bid) {
            for (auto it = bids_.rbegin(); it != bids_.rend() && result.size() < depth; ++it) {
                result.push_back(L2Level{.price = it->first, .qty = it->second});
            }
            return result;
        }

        for (auto it = asks_.begin(); it != asks_.end() && result.size() < depth; ++it) {
            result.push_back(L2Level{.price = it->first, .qty = it->second});
        }
        return result;
    }

    [[nodiscard]] std::vector<L2Level> levels(Side side) const {
        const auto& levels = side == Side::Bid ? bids_ : asks_;
        std::vector<L2Level> result;
        result.reserve(levels.size());

        if (side == Side::Bid) {
            for (auto it = bids_.rbegin(); it != bids_.rend(); ++it) {
                result.push_back(L2Level{.price = it->first, .qty = it->second});
            }
            return result;
        }

        for (const auto& [price, qty] : asks_) {
            result.push_back(L2Level{.price = price, .qty = qty});
        }
        return result;
    }

  private:
    void add_level_qty(Side side, Price price, Qty qty) {
        auto& levels = side == Side::Bid ? bids_ : asks_;
        levels[price] += qty;
    }

    void remove_level_qty(Side side, Price price, Qty qty) {
        auto& levels = side == Side::Bid ? bids_ : asks_;
        const auto found = levels.find(price);
        if (found == levels.end()) {
            return;
        }

        if (found->second <= qty) {
            levels.erase(found);
            return;
        }
        found->second -= qty;
    }

    std::map<Price, AggQty> bids_;
    std::map<Price, AggQty> asks_;
};

}  // namespace ob
