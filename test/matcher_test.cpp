// matcher_test.cpp - First single-threaded matching-boundary checks.
//
// These tests pin the matcher API and core event rules before broader
// randomized comparison tests exist. Each scenario checks both the logical
// event stream and the passive book state left behind by the operation.

#include <array>
#include <cstdio>
#include <optional>

#include "orderbook/config.hpp"
#include "orderbook/matcher.hpp"
#include "orderbook/types.hpp"

namespace {

using TestMatcher = ob::Matcher<ob::config::kFuzz.num_levels, 4>;

struct RecordingSink {
    // Fixed-size by design: tests should fail loudly if a scenario emits more
    // events than expected, and production matching must not allocate a vector
    // on the hot path.
    std::array<ob::OutboundEvent, 16> events{};
    std::size_t count{0};

    void operator()(const ob::OutboundEvent& event) noexcept {
        if (count < events.size()) {
            events[count] = event;
        }
        ++count;
    }
};

bool fail(const char* message) {
    std::fprintf(stderr, "%s\n", message);
    return false;
}

ob::InboundMsg limit_msg(uint64_t client_seq, ob::Side side, ob::Price price,
                         ob::Qty qty) noexcept {
    return ob::InboundMsg{
        .client_seq = client_seq,
        .handle = 0,
        .price = price,
        .qty = qty,
        .side = side,
        .type = ob::MsgType::NewLimit,
        .tsc_intended = 0,
        .tsc_ready = 0,
        .tsc_enqueue = 0,
    };
}

ob::InboundMsg market_msg(uint64_t client_seq, ob::Side side, ob::Qty qty) noexcept {
    return ob::InboundMsg{
        .client_seq = client_seq,
        .handle = 0,
        .price = 0,
        .qty = qty,
        .side = side,
        .type = ob::MsgType::NewMarket,
        .tsc_intended = 0,
        .tsc_ready = 0,
        .tsc_enqueue = 0,
    };
}

ob::InboundMsg cancel_msg(uint64_t client_seq, ob::Handle handle) noexcept {
    return ob::InboundMsg{
        .client_seq = client_seq,
        .handle = handle,
        .price = 0,
        .qty = 0,
        .side = ob::Side::Bid,
        .type = ob::MsgType::Cancel,
        .tsc_intended = 0,
        .tsc_ready = 0,
        .tsc_enqueue = 0,
    };
}

ob::InboundMsg stop_engine_msg(uint64_t client_seq) noexcept {
    return ob::InboundMsg{
        .client_seq = client_seq,
        .handle = 0,
        .price = 0,
        .qty = 0,
        .side = ob::Side::Bid,
        .type = ob::MsgType::StopEngine,
        .tsc_intended = 0,
        .tsc_ready = 0,
        .tsc_enqueue = 0,
    };
}

bool has_one_complete_event(const RecordingSink& sink) {
    // RequestComplete is the operation boundary marker used later for latency
    // sampling. Every inbound message must produce exactly one such event.
    std::size_t complete = 0;
    for (std::size_t i = 0; i < sink.count && i < sink.events.size(); ++i) {
        if ((sink.events[i].flags & ob::RequestComplete) != 0) {
            ++complete;
        }
    }
    return complete == 1;
}

bool expect_single_reject(const RecordingSink& sink, ob::RejectReason reason, ob::Qty qty,
                          const char* message) {
    if (sink.count != 1 || !has_one_complete_event(sink)) {
        return fail(message);
    }

    const ob::OutboundEvent& event = sink.events[0];
    if (event.type != ob::EventType::Reject || event.reason != reason || event.qty != qty) {
        return fail(message);
    }
    return true;
}

// Bad limit inputs should reject immediately and leave the book unchanged.
bool check_limit_validation_rejects() {
    TestMatcher matcher;

    {
        // Invalid quantity is rejected before price or book state matters.
        RecordingSink sink;
        matcher.process(limit_msg(1, ob::Side::Bid, 10, 0), sink);
        if (!expect_single_reject(sink, ob::RejectReason::ZeroQty, 0,
                                  "zero-qty limit reject failed")) {
            return false;
        }
    }

    {
        // The upper price bound is exclusive: [base, base + num_levels).
        RecordingSink sink;
        matcher.process(limit_msg(2, ob::Side::Ask, TestMatcher::limit_price(), 10), sink);
        if (!expect_single_reject(sink, ob::RejectReason::PriceOutOfBand, 10,
                                  "out-of-band limit reject failed")) {
            return false;
        }
    }

    return matcher.book().audit();
}

// A non-crossing limit order should rest and return a usable engine handle.
bool check_limit_rests_and_acknowledges_handle() {
    TestMatcher matcher;
    RecordingSink sink;

    matcher.process(limit_msg(10, ob::Side::Bid, 42, 75), sink);
    if (sink.count != 1 || !has_one_complete_event(sink)) {
        return fail("resting limit did not emit exactly one complete event");
    }

    const ob::OutboundEvent& ack = sink.events[0];
    if (ack.type != ob::EventType::AckNew || ack.handle == 0 || ack.price != 42 || ack.qty != 75 ||
        ack.side != ob::Side::Bid || ack.client_seq != 10) {
        return fail("resting limit AckNew fields failed");
    }

    // AckNew returns the engine-assigned handle that future cancels use. The
    // resting order must also retain client_seq for later resting-party fills.
    const ob::Order* order = matcher.book().pool().resolve(ack.handle);
    if (order == nullptr || order->client_seq != 10 || order->price != 42 ||
        order->remaining != 75 || order->side != ob::Side::Bid) {
        return fail("resting order was not populated from inbound message");
    }

    if (!matcher.book().occupied(42) || matcher.book().level(42).order_count != 1 ||
        matcher.book().best_bid_idx() != std::optional<std::size_t>{42}) {
        return fail("resting limit did not update book state");
    }

    return matcher.book().audit();
}

// A market order into an empty opposite side should reject the full quantity.
bool check_market_empty_rejects() {
    TestMatcher matcher;
    RecordingSink sink;

    matcher.process(market_msg(20, ob::Side::Ask, 60), sink);
    return expect_single_reject(sink, ob::RejectReason::InsufficientLiquidity, 60,
                                "empty-market reject failed") &&
           matcher.book().audit();
}

// A small crossing limit order should partially fill the resting head order.
bool check_single_fill_event_order() {
    TestMatcher matcher;
    RecordingSink rest_sink;
    matcher.process(limit_msg(50, ob::Side::Ask, 100, 40), rest_sink);
    if (rest_sink.count != 1 || rest_sink.events[0].type != ob::EventType::AckNew) {
        return fail("single-fill setup rest failed");
    }

    const ob::Handle resting_handle = rest_sink.events[0].handle;
    RecordingSink fill_sink;
    matcher.process(limit_msg(51, ob::Side::Bid, 100, 10), fill_sink);

    if (fill_sink.count != 2 || !has_one_complete_event(fill_sink)) {
        return fail("single fill did not emit two events with one completion");
    }

    // Event order is contractual: resting party first, aggressor second. Only
    // the aggressor fill completes this inbound request.
    const ob::OutboundEvent& resting_fill = fill_sink.events[0];
    if (resting_fill.type != ob::EventType::Fill || resting_fill.client_seq != 50 ||
        resting_fill.handle != resting_handle || resting_fill.price != 100 ||
        resting_fill.qty != 10 || resting_fill.side != ob::Side::Ask ||
        (resting_fill.flags & ob::RequestComplete) != 0) {
        return fail("resting fill fields/order failed");
    }

    const ob::OutboundEvent& aggressor_fill = fill_sink.events[1];
    if (aggressor_fill.type != ob::EventType::Fill || aggressor_fill.client_seq != 51 ||
        aggressor_fill.handle != 0 || aggressor_fill.price != 100 || aggressor_fill.qty != 10 ||
        aggressor_fill.side != ob::Side::Bid || (aggressor_fill.flags & ob::RequestComplete) == 0) {
        return fail("aggressor fill fields/order failed");
    }

    const ob::Order* resting = matcher.book().pool().resolve(resting_handle);
    if (resting == nullptr || resting->remaining != 30) {
        return fail("partial resting order quantity after single fill failed");
    }

    return matcher.book().audit();
}

// A limit order can trade first, then rest only its leftover quantity.
bool check_partial_cross_then_rest() {
    TestMatcher matcher;
    RecordingSink ask_sink;
    matcher.process(limit_msg(60, ob::Side::Ask, 100, 10), ask_sink);
    if (ask_sink.count != 1 || ask_sink.events[0].type != ob::EventType::AckNew) {
        return fail("partial-cross setup rest failed");
    }

    RecordingSink bid_sink;
    matcher.process(limit_msg(61, ob::Side::Bid, 100, 25), bid_sink);
    if (bid_sink.count != 3 || !has_one_complete_event(bid_sink)) {
        return fail("partial-cross did not emit fills plus final ack");
    }

    if (bid_sink.events[0].type != ob::EventType::Fill || bid_sink.events[0].client_seq != 60 ||
        bid_sink.events[0].side != ob::Side::Ask || bid_sink.events[0].qty != 10) {
        return fail("partial-cross resting fill failed");
    }
    if (bid_sink.events[1].type != ob::EventType::Fill || bid_sink.events[1].client_seq != 61 ||
        bid_sink.events[1].side != ob::Side::Bid || bid_sink.events[1].qty != 10 ||
        (bid_sink.events[1].flags & ob::RequestComplete) != 0) {
        return fail("partial-cross aggressor fill failed");
    }

    // The inbound bid consumed the ask, then rested its remaining 15 shares at
    // its own limit price. The final AckNew is the completion event.
    const ob::OutboundEvent& ack = bid_sink.events[2];
    if (ack.type != ob::EventType::AckNew || ack.client_seq != 61 || ack.handle == 0 ||
        ack.price != 100 || ack.qty != 15 || ack.side != ob::Side::Bid ||
        (ack.flags & ob::RequestComplete) == 0) {
        return fail("partial-cross final AckNew failed");
    }

    const ob::Order* rested_bid = matcher.book().pool().resolve(ack.handle);
    if (rested_bid == nullptr || rested_bid->remaining != 15 || rested_bid->side != ob::Side::Bid) {
        return fail("partial-cross rested remainder failed");
    }

    return matcher.book().audit();
}

// A crossing order should walk price levels best-first.
bool check_multi_level_walk() {
    TestMatcher matcher;

    RecordingSink ask100_sink;
    RecordingSink ask101_sink;
    matcher.process(limit_msg(70, ob::Side::Ask, 100, 10), ask100_sink);
    matcher.process(limit_msg(71, ob::Side::Ask, 101, 20), ask101_sink);
    if (ask100_sink.count != 1 || ask101_sink.count != 1 ||
        ask100_sink.events[0].type != ob::EventType::AckNew ||
        ask101_sink.events[0].type != ob::EventType::AckNew) {
        return fail("multi-level setup rest failed");
    }

    RecordingSink bid_sink;
    matcher.process(limit_msg(72, ob::Side::Bid, 101, 30), bid_sink);
    if (bid_sink.count != 4 || !has_one_complete_event(bid_sink)) {
        return fail("multi-level walk did not emit two fill pairs with one completion");
    }

    // A bid crossing multiple asks must trade cheapest first. Within each
    // match, the resting-party fill still precedes the aggressor fill.
    if (bid_sink.events[0].type != ob::EventType::Fill || bid_sink.events[0].client_seq != 70 ||
        bid_sink.events[0].price != 100 || bid_sink.events[0].qty != 10 ||
        bid_sink.events[0].side != ob::Side::Ask) {
        return fail("multi-level first resting fill failed");
    }
    if (bid_sink.events[1].type != ob::EventType::Fill || bid_sink.events[1].client_seq != 72 ||
        bid_sink.events[1].price != 100 || bid_sink.events[1].qty != 10 ||
        bid_sink.events[1].side != ob::Side::Bid ||
        (bid_sink.events[1].flags & ob::RequestComplete) != 0) {
        return fail("multi-level first aggressor fill failed");
    }
    if (bid_sink.events[2].type != ob::EventType::Fill || bid_sink.events[2].client_seq != 71 ||
        bid_sink.events[2].price != 101 || bid_sink.events[2].qty != 20 ||
        bid_sink.events[2].side != ob::Side::Ask ||
        (bid_sink.events[2].flags & ob::RequestComplete) != 0) {
        return fail("multi-level second resting fill failed");
    }
    if (bid_sink.events[3].type != ob::EventType::Fill || bid_sink.events[3].client_seq != 72 ||
        bid_sink.events[3].price != 101 || bid_sink.events[3].qty != 20 ||
        bid_sink.events[3].side != ob::Side::Bid ||
        (bid_sink.events[3].flags & ob::RequestComplete) == 0) {
        return fail("multi-level final aggressor fill failed");
    }

    if (matcher.book().best_ask_idx().has_value() || matcher.book().best_bid_idx().has_value()) {
        return fail("multi-level full sweep left resting liquidity");
    }

    return matcher.book().audit();
}

// Multiple orders at one price should fill FIFO by arrival time.
bool check_same_price_fifo_walk() {
    TestMatcher matcher;

    RecordingSink first_ask_sink;
    RecordingSink second_ask_sink;
    matcher.process(limit_msg(80, ob::Side::Ask, 100, 10), first_ask_sink);
    matcher.process(limit_msg(81, ob::Side::Ask, 100, 15), second_ask_sink);
    if (first_ask_sink.count != 1 || second_ask_sink.count != 1 ||
        first_ask_sink.events[0].type != ob::EventType::AckNew ||
        second_ask_sink.events[0].type != ob::EventType::AckNew) {
        return fail("same-price FIFO setup rest failed");
    }

    RecordingSink bid_sink;
    matcher.process(limit_msg(82, ob::Side::Bid, 100, 25), bid_sink);
    if (bid_sink.count != 4 || !has_one_complete_event(bid_sink)) {
        return fail("same-price FIFO did not emit two fill pairs with one completion");
    }

    if (bid_sink.events[0].type != ob::EventType::Fill || bid_sink.events[0].client_seq != 80 ||
        bid_sink.events[0].price != 100 || bid_sink.events[0].qty != 10 ||
        bid_sink.events[0].side != ob::Side::Ask) {
        return fail("same-price FIFO first resting fill failed");
    }
    if (bid_sink.events[1].type != ob::EventType::Fill || bid_sink.events[1].client_seq != 82 ||
        bid_sink.events[1].price != 100 || bid_sink.events[1].qty != 10 ||
        bid_sink.events[1].side != ob::Side::Bid ||
        (bid_sink.events[1].flags & ob::RequestComplete) != 0) {
        return fail("same-price FIFO first aggressor fill failed");
    }
    if (bid_sink.events[2].type != ob::EventType::Fill || bid_sink.events[2].client_seq != 81 ||
        bid_sink.events[2].price != 100 || bid_sink.events[2].qty != 15 ||
        bid_sink.events[2].side != ob::Side::Ask ||
        (bid_sink.events[2].flags & ob::RequestComplete) != 0) {
        return fail("same-price FIFO second resting fill failed");
    }
    if (bid_sink.events[3].type != ob::EventType::Fill || bid_sink.events[3].client_seq != 82 ||
        bid_sink.events[3].price != 100 || bid_sink.events[3].qty != 15 ||
        bid_sink.events[3].side != ob::Side::Bid ||
        (bid_sink.events[3].flags & ob::RequestComplete) == 0) {
        return fail("same-price FIFO final aggressor fill failed");
    }

    if (matcher.book().occupied(100)) {
        return fail("same-price FIFO full sweep left ask liquidity");
    }

    return matcher.book().audit();
}

// A market order should consume available prices best-first and never rest.
bool check_market_fills_available_liquidity() {
    TestMatcher matcher;

    RecordingSink ask100_sink;
    RecordingSink ask101_sink;
    matcher.process(limit_msg(90, ob::Side::Ask, 100, 10), ask100_sink);
    matcher.process(limit_msg(91, ob::Side::Ask, 101, 20), ask101_sink);
    if (ask100_sink.count != 1 || ask101_sink.count != 1 ||
        ask100_sink.events[0].type != ob::EventType::AckNew ||
        ask101_sink.events[0].type != ob::EventType::AckNew) {
        return fail("market-fill setup rest failed");
    }

    RecordingSink market_sink;
    matcher.process(market_msg(92, ob::Side::Bid, 30), market_sink);
    if (market_sink.count != 4 || !has_one_complete_event(market_sink)) {
        return fail("market-fill did not emit two fill pairs with one completion");
    }

    if (market_sink.events[0].type != ob::EventType::Fill ||
        market_sink.events[0].client_seq != 90 || market_sink.events[0].price != 100 ||
        market_sink.events[0].qty != 10 || market_sink.events[0].side != ob::Side::Ask) {
        return fail("market-fill first resting fill failed");
    }
    if (market_sink.events[1].type != ob::EventType::Fill ||
        market_sink.events[1].client_seq != 92 || market_sink.events[1].price != 100 ||
        market_sink.events[1].qty != 10 || market_sink.events[1].side != ob::Side::Bid ||
        (market_sink.events[1].flags & ob::RequestComplete) != 0) {
        return fail("market-fill first aggressor fill failed");
    }
    if (market_sink.events[2].type != ob::EventType::Fill ||
        market_sink.events[2].client_seq != 91 || market_sink.events[2].price != 101 ||
        market_sink.events[2].qty != 20 || market_sink.events[2].side != ob::Side::Ask ||
        (market_sink.events[2].flags & ob::RequestComplete) != 0) {
        return fail("market-fill second resting fill failed");
    }
    if (market_sink.events[3].type != ob::EventType::Fill ||
        market_sink.events[3].client_seq != 92 || market_sink.events[3].price != 101 ||
        market_sink.events[3].qty != 20 || market_sink.events[3].side != ob::Side::Bid ||
        (market_sink.events[3].flags & ob::RequestComplete) == 0) {
        return fail("market-fill final aggressor fill failed");
    }

    if (matcher.book().best_ask_idx().has_value() || matcher.book().best_bid_idx().has_value()) {
        return fail("market-fill left resting liquidity");
    }

    return matcher.book().audit();
}

// If a market order runs out of liquidity, prior fills stand and the remainder
// is rejected.
bool check_market_partial_fill_then_reject() {
    TestMatcher matcher;

    RecordingSink ask_sink;
    matcher.process(limit_msg(100, ob::Side::Ask, 100, 10), ask_sink);
    if (ask_sink.count != 1 || ask_sink.events[0].type != ob::EventType::AckNew) {
        return fail("market-reject setup rest failed");
    }

    RecordingSink market_sink;
    matcher.process(market_msg(101, ob::Side::Bid, 25), market_sink);
    if (market_sink.count != 3 || !has_one_complete_event(market_sink)) {
        return fail("market-reject did not emit fill pair plus reject");
    }

    if (market_sink.events[0].type != ob::EventType::Fill ||
        market_sink.events[0].client_seq != 100 || market_sink.events[0].price != 100 ||
        market_sink.events[0].qty != 10 || market_sink.events[0].side != ob::Side::Ask) {
        return fail("market-reject resting fill failed");
    }
    if (market_sink.events[1].type != ob::EventType::Fill ||
        market_sink.events[1].client_seq != 101 || market_sink.events[1].price != 100 ||
        market_sink.events[1].qty != 10 || market_sink.events[1].side != ob::Side::Bid ||
        (market_sink.events[1].flags & ob::RequestComplete) != 0) {
        return fail("market-reject aggressor fill failed");
    }
    if (market_sink.events[2].type != ob::EventType::Reject ||
        market_sink.events[2].reason != ob::RejectReason::InsufficientLiquidity ||
        market_sink.events[2].client_seq != 101 || market_sink.events[2].qty != 15 ||
        (market_sink.events[2].flags & ob::RequestComplete) == 0) {
        return fail("market-reject final reject failed");
    }

    return matcher.book().audit();
}

// Pool exhaustion is hit when a new limit order needs to rest without freeing a
// slot first.
bool check_pool_exhausted_rejects_resting_remainder() {
    ob::Matcher<ob::config::kFuzz.num_levels, 1> matcher;

    RecordingSink first_sink;
    matcher.process(limit_msg(110, ob::Side::Bid, 90, 10), first_sink);
    if (first_sink.count != 1 || first_sink.events[0].type != ob::EventType::AckNew) {
        return fail("pool-exhaust setup rest failed");
    }

    RecordingSink second_sink;
    matcher.process(limit_msg(111, ob::Side::Bid, 91, 20), second_sink);
    if (!expect_single_reject(second_sink, ob::RejectReason::PoolExhausted, 20,
                              "pool-exhaust reject failed")) {
        return false;
    }

    return matcher.book().audit();
}

// If a crossing fill frees a slot, the incoming order can use that slot to rest
// its remainder even when the pool was full at entry.
bool check_full_pool_cross_can_rest_after_freeing_slot() {
    ob::Matcher<ob::config::kFuzz.num_levels, 1> matcher;

    RecordingSink ask_sink;
    matcher.process(limit_msg(120, ob::Side::Ask, 100, 10), ask_sink);
    if (ask_sink.count != 1 || ask_sink.events[0].type != ob::EventType::AckNew) {
        return fail("full-pool cross setup rest failed");
    }

    RecordingSink bid_sink;
    matcher.process(limit_msg(121, ob::Side::Bid, 100, 25), bid_sink);
    if (bid_sink.count != 3 || !has_one_complete_event(bid_sink)) {
        return fail("full-pool cross did not emit fill pair plus ack");
    }
    if (bid_sink.events[0].type != ob::EventType::Fill || bid_sink.events[0].client_seq != 120 ||
        bid_sink.events[0].qty != 10) {
        return fail("full-pool cross resting fill failed");
    }
    if (bid_sink.events[1].type != ob::EventType::Fill || bid_sink.events[1].client_seq != 121 ||
        bid_sink.events[1].qty != 10 || (bid_sink.events[1].flags & ob::RequestComplete) != 0) {
        return fail("full-pool cross aggressor fill failed");
    }

    const ob::OutboundEvent& ack = bid_sink.events[2];
    if (ack.type != ob::EventType::AckNew || ack.client_seq != 121 || ack.handle == 0 ||
        ack.price != 100 || ack.qty != 15 || ack.side != ob::Side::Bid ||
        (ack.flags & ob::RequestComplete) == 0) {
        return fail("full-pool cross final AckNew failed");
    }

    const ob::Order* rested = matcher.book().pool().resolve(ack.handle);
    if (rested == nullptr || rested->remaining != 15 || rested->side != ob::Side::Bid) {
        return fail("full-pool cross rested remainder failed");
    }

    return matcher.book().audit();
}

// A valid cancel should remove the resting order and report its removed fields.
bool check_cancel_acknowledges_and_removes() {
    TestMatcher matcher;
    RecordingSink rest_sink;

    matcher.process(limit_msg(30, ob::Side::Ask, 100, 25), rest_sink);
    if (rest_sink.count != 1 || rest_sink.events[0].type != ob::EventType::AckNew) {
        return fail("cancel setup rest failed");
    }

    const ob::Handle handle = rest_sink.events[0].handle;
    RecordingSink cancel_sink;
    matcher.process(cancel_msg(31, handle), cancel_sink);

    if (cancel_sink.count != 1 || !has_one_complete_event(cancel_sink)) {
        return fail("cancel did not emit exactly one complete event");
    }

    const ob::OutboundEvent& ack = cancel_sink.events[0];
    if (ack.type != ob::EventType::AckCancel || ack.handle != handle || ack.price != 100 ||
        ack.qty != 25 || ack.side != ob::Side::Ask || ack.client_seq != 31) {
        return fail("cancel AckCancel fields failed");
    }
    if (matcher.book().pool().resolve(handle) != nullptr || matcher.book().occupied(100)) {
        return fail("cancel did not remove resting order");
    }

    return matcher.book().audit();
}

// Unknown, stale, or null handles all surface as UnknownHandle rejects.
bool check_unknown_cancel_rejects() {
    TestMatcher matcher;
    RecordingSink sink;

    matcher.process(cancel_msg(40, 0), sink);
    return expect_single_reject(sink, ob::RejectReason::UnknownHandle, 0,
                                "unknown cancel reject failed") &&
           matcher.book().audit();
}

// StopEngine is an internal shutdown marker, not a trading stop order.
bool check_stop_engine_emits_internal_event() {
    TestMatcher matcher;
    RecordingSink sink;

    matcher.process(stop_engine_msg(90), sink);
    if (sink.count != 1 || !has_one_complete_event(sink)) {
        return fail("StopEngine did not emit exactly one complete event");
    }

    const ob::OutboundEvent& event = sink.events[0];
    if (event.type != ob::EventType::StopEngine || event.client_seq != 90 ||
        event.reason != ob::RejectReason::None || event.qty != 0 || event.handle != 0) {
        return fail("StopEngine event fields failed");
    }

    return matcher.book().audit();
}

}  // namespace

int main() {
    if (!check_limit_validation_rejects()) {
        return 1;
    }
    if (!check_limit_rests_and_acknowledges_handle()) {
        return 1;
    }
    if (!check_market_empty_rejects()) {
        return 1;
    }
    if (!check_single_fill_event_order()) {
        return 1;
    }
    if (!check_partial_cross_then_rest()) {
        return 1;
    }
    if (!check_multi_level_walk()) {
        return 1;
    }
    if (!check_same_price_fifo_walk()) {
        return 1;
    }
    if (!check_market_fills_available_liquidity()) {
        return 1;
    }
    if (!check_market_partial_fill_then_reject()) {
        return 1;
    }
    if (!check_pool_exhausted_rejects_resting_remainder()) {
        return 1;
    }
    if (!check_full_pool_cross_can_rest_after_freeing_slot()) {
        return 1;
    }
    if (!check_cancel_acknowledges_and_removes()) {
        return 1;
    }
    if (!check_unknown_cancel_rejects()) {
        return 1;
    }
    if (!check_stop_engine_emits_internal_event()) {
        return 1;
    }

    std::printf("matcher_test OK\n");
    return 0;
}
