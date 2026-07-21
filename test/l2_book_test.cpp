// l2_book_test.cpp - Checks for rebuilding price-level market data from events.

#include <array>
#include <cstdio>

#include "orderbook/l2_book.hpp"

namespace {

bool fail(const char* message) {
    std::fprintf(stderr, "%s\n", message);
    return false;
}

ob::OutboundEvent event(ob::EventType type, ob::Side side, ob::Price price, ob::Qty qty,
                        ob::Handle handle = 0) {
    return ob::OutboundEvent{
        .client_seq = 1,
        .handle = handle,
        .price = price,
        .qty = qty,
        .side = side,
        .type = type,
        .reason = ob::RejectReason::None,
        .flags = ob::RequestComplete,
        .participant_id = 0,
        .tsc_intended = 0,
        .tsc_egress = 0,
    };
}

bool check_new_orders_add_visible_quantity() {
    ob::L2Book book;
    book.apply(event(ob::EventType::AckNew, ob::Side::Bid, 100, 10));
    book.apply(event(ob::EventType::AckNew, ob::Side::Bid, 101, 5));
    book.apply(event(ob::EventType::AckNew, ob::Side::Ask, 103, 7));

    const auto bids = book.top(ob::Side::Bid, 2);
    const auto asks = book.top(ob::Side::Ask, 1);
    if (bids.size() != 2 || bids[0].price != 101 || bids[0].qty != 5 || bids[1].price != 100 ||
        bids[1].qty != 10 || asks.size() != 1 || asks[0].price != 103 || asks[0].qty != 7) {
        return fail("L2 top levels were not ordered correctly");
    }
    return true;
}

bool check_fills_update_only_resting_side() {
    ob::L2Book book;
    book.apply(event(ob::EventType::AckNew, ob::Side::Ask, 105, 12));

    // Nonzero handle means this fill belongs to the resting order already shown
    // in L2. The matching aggressor fill has handle 0 and must not subtract
    // from visible depth a second time.
    book.apply(event(ob::EventType::Fill, ob::Side::Ask, 105, 4, 0x100000001ULL));
    book.apply(event(ob::EventType::Fill, ob::Side::Bid, 105, 4));

    if (book.quantity_at(ob::Side::Ask, 105) != 8 || book.quantity_at(ob::Side::Bid, 105) != 0) {
        return fail("L2 fill handling was wrong");
    }
    return true;
}

bool check_cancels_remove_visible_quantity() {
    ob::L2Book book;
    book.apply(event(ob::EventType::AckNew, ob::Side::Bid, 99, 6));
    book.apply(event(ob::EventType::AckCancel, ob::Side::Bid, 99, 6, 0x100000001ULL));

    if (book.quantity_at(ob::Side::Bid, 99) != 0 || !book.top(ob::Side::Bid, 1).empty()) {
        return fail("L2 cancel handling was wrong");
    }
    return true;
}

}  // namespace

int main() {
    using Check = bool (*)();
    constexpr std::array<Check, 3> checks{
        check_new_orders_add_visible_quantity,
        check_fills_update_only_resting_side,
        check_cancels_remove_visible_quantity,
    };

    for (Check check : checks) {
        if (!check()) {
            return 1;
        }
    }

    std::printf("l2_book_test OK\n");
    return 0;
}
