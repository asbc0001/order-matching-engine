// demo_renderer.hpp - Small terminal view of the live book built from events.
//
// This is for human-facing demo mode, not the hot matching path. It consumes the
// same OutboundEvent stream as the loggers and keeps just enough state to draw
// the current visible book.

#pragma once

#include <array>
#include <cstddef>
#include <cstdio>

#include "orderbook/types.hpp"

namespace ob {

template <std::size_t MaxOrders, std::size_t TopLevels = 5>
class DemoRenderer {
  public:
    static_assert(MaxOrders > 0);
    static_assert(TopLevels > 0);

    // Apply one matcher event to the demo's local book view. AckNew adds a
    // resting order, AckCancel removes it, and a fill with a nonzero handle
    // reduces the already-resting order that traded.
    bool apply(const OutboundEvent& event) noexcept {
        switch (event.type) {
            case EventType::AckNew:
                return add_order(event);
            case EventType::AckCancel:
                return remove_order(event.handle);
            case EventType::Fill:
                return apply_fill(event);
            case EventType::Reject:
                return true;
            case EventType::StopEngine:
                stopped_ = true;
                return true;
        }
        return false;
    }

    // Render a compact snapshot. Asks are printed best-first above bids, and
    // the bar length is capped so large quantities do not flood the terminal.
    bool render(std::FILE* file) const noexcept {
        if (file == nullptr) {
            return false;
        }

        LevelRows asks{};
        LevelRows bids{};
        for (const OrderState& order : orders_) {
            if (!order.live) {
                continue;
            }
            LevelRows& rows = order.side == Side::Ask ? asks : bids;
            upsert_level(rows, order.side, order.price, order.remaining);
        }

        return std::fprintf(file, "ASKS\n") >= 0 && write_rows(file, asks) &&
               std::fprintf(file, "BIDS\n") >= 0 && write_rows(file, bids);
    }

    [[nodiscard]] bool stopped() const noexcept {
        return stopped_;
    }

    [[nodiscard]] std::size_t live_orders() const noexcept {
        std::size_t count = 0;
        for (const OrderState& order : orders_) {
            count += order.live ? 1U : 0U;
        }
        return count;
    }

  private:
    struct OrderState {
        Handle handle{0};
        Price price{0};
        Qty remaining{0};
        Side side{Side::Bid};
        bool live{false};
    };

    struct LevelRow {
        Price price{0};
        AggQty qty{0};
        bool used{false};
    };

    using LevelRows = std::array<LevelRow, TopLevels>;

    static bool better_level(Side side, Price candidate, Price current) noexcept {
        return side == Side::Bid ? candidate > current : candidate < current;
    }

    static void insert_level(LevelRows& rows, std::size_t index, Price price, AggQty qty) noexcept {
        for (std::size_t i = rows.size() - 1; i > index; --i) {
            rows[i] = rows[i - 1];
        }
        rows[index] = LevelRow{.price = price, .qty = qty, .used = true};
    }

    static void upsert_level(LevelRows& rows, Side side, Price price, Qty qty) noexcept {
        for (LevelRow& row : rows) {
            if (row.used && row.price == price) {
                row.qty += qty;
                return;
            }
        }

        for (std::size_t i = 0; i < rows.size(); ++i) {
            if (!rows[i].used || better_level(side, price, rows[i].price)) {
                insert_level(rows, i, price, qty);
                return;
            }
        }
    }

    static bool write_bar(std::FILE* file, AggQty qty) noexcept {
        constexpr std::size_t kMaxBar = 32;
        const std::size_t width = qty < kMaxBar ? static_cast<std::size_t>(qty) : kMaxBar;
        for (std::size_t i = 0; i < width; ++i) {
            if (std::fputc('#', file) == EOF) {
                return false;
            }
        }
        return true;
    }

    static bool write_rows(std::FILE* file, const LevelRows& rows) noexcept {
        bool wrote_any = false;
        for (const LevelRow& row : rows) {
            if (!row.used) {
                continue;
            }
            wrote_any = true;
            if (std::fprintf(file, "  %lld | %llu | ", static_cast<long long>(row.price),
                             static_cast<unsigned long long>(row.qty)) < 0 ||
                !write_bar(file, row.qty) || std::fputc('\n', file) == EOF) {
                return false;
            }
        }

        if (!wrote_any && std::fprintf(file, "  <empty>\n") < 0) {
            return false;
        }
        return true;
    }

    OrderState* find_order(Handle handle) noexcept {
        for (OrderState& order : orders_) {
            if (order.live && order.handle == handle) {
                return &order;
            }
        }
        return nullptr;
    }

    bool add_order(const OutboundEvent& event) noexcept {
        if (event.handle == 0 || event.qty == 0 || find_order(event.handle) != nullptr) {
            return false;
        }

        for (OrderState& order : orders_) {
            if (!order.live) {
                order = OrderState{
                    .handle = event.handle,
                    .price = event.price,
                    .remaining = event.qty,
                    .side = event.side,
                    .live = true,
                };
                return true;
            }
        }
        return false;
    }

    bool remove_order(Handle handle) noexcept {
        OrderState* order = find_order(handle);
        if (order == nullptr) {
            return false;
        }
        *order = OrderState{};
        return true;
    }

    bool apply_fill(const OutboundEvent& event) noexcept {
        // Aggressor fill events describe the incoming command and have no
        // resting handle, so they do not change the displayed resting book.
        if (event.handle == 0) {
            return true;
        }

        OrderState* order = find_order(event.handle);
        if (order == nullptr || event.qty > order->remaining) {
            return false;
        }
        order->remaining -= event.qty;
        if (order->remaining == 0) {
            *order = OrderState{};
        }
        return true;
    }

    std::array<OrderState, MaxOrders> orders_{};
    bool stopped_{false};
};

}  // namespace ob
