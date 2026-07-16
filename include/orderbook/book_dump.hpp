// book_dump.hpp - Deterministic text dump of a finished book.
//
// This is internal debugging/test output, not a wire format. It exists so two
// runs over the same input can compare the final book state byte-for-byte.

#pragma once

#include <cstdio>
#include <optional>

#include "orderbook/book.hpp"
#include "orderbook/types.hpp"

namespace ob {

inline const char* book_dump_side_name(Side side) noexcept {
    return side == Side::Bid ? "Bid" : "Ask";
}

inline bool write_best_price(std::FILE* file, const char* name, std::optional<std::size_t> best_idx,
                             Price (*price_at)(std::size_t)) {
    if (best_idx) {
        return std::fprintf(file, "%s=%lld", name, static_cast<long long>(price_at(*best_idx))) >=
               0;
    }
    return std::fprintf(file, "%s=NONE", name) >= 0;
}

template <std::size_t NumLevels, std::size_t PoolCapacity, Price BasePrice = config::BASE_PRICE>
bool dump_book(const Book<NumLevels, PoolCapacity, BasePrice>& book, std::FILE* file) {
    if (file == nullptr) {
        return false;
    }

    using BookType = Book<NumLevels, PoolCapacity, BasePrice>;
    if (!write_best_price(file, "BEST bid", book.best_bid_idx(), BookType::index_to_price) ||
        std::fprintf(file, " ") < 0 ||
        !write_best_price(file, "ask", book.best_ask_idx(), BookType::index_to_price) ||
        std::fprintf(file, "\n") < 0) {
        return false;
    }

    // Mirror audit()'s forward walk: levels are visited by ascending index, and
    // each level's orders are visited from head to tail, which is FIFO order.
    for (std::size_t idx = 0; idx < NumLevels; ++idx) {
        if (!book.occupied(idx)) {
            continue;
        }

        const Level& level = book.level(idx);
        if (std::fprintf(file, "LEVEL price=%lld count=%u qty=%llu\n",
                         static_cast<long long>(BookType::index_to_price(idx)),
                         static_cast<unsigned>(level.order_count),
                         static_cast<unsigned long long>(level.total_qty)) < 0) {
            return false;
        }

        std::uint32_t slot = level.head;
        while (slot != NULL_SLOT) {
            const Order& order = book.pool().at(slot);
            if (std::fprintf(file, "  ORDER handle=%llu client_seq=%llu remaining=%u side=%s\n",
                             static_cast<unsigned long long>(order.handle),
                             static_cast<unsigned long long>(order.client_seq),
                             static_cast<unsigned>(order.remaining),
                             book_dump_side_name(order.side)) < 0) {
                return false;
            }
            slot = order.next;
        }
    }

    return true;
}

}  // namespace ob
