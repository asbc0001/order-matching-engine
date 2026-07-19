// matcher_test.cpp - Focused single-threaded matcher scenarios.

#include <array>
#include <cstdio>
#include <optional>

#include "orderbook/config.hpp"
#include "orderbook/matcher.hpp"
#include "orderbook/types.hpp"

namespace {

using TestMatcher = ob::Matcher<ob::config::kFuzz.num_levels, 4>;

struct RecordingSink {
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

ob::InboundMsg msg(uint64_t seq, ob::MsgType type, ob::Side side, ob::Price price, ob::Qty qty,
                   ob::Handle handle = 0) noexcept {
    return ob::InboundMsg{
        .client_seq = seq,
        .handle = handle,
        .price = price,
        .qty = qty,
        .side = side,
        .type = type,
        .tsc_intended = 0,
        .tsc_ready = 0,
        .tsc_enqueue = 0,
    };
}

ob::InboundMsg limit_msg(uint64_t seq, ob::Side side, ob::Price price, ob::Qty qty,
                         ob::TimeInForce time_in_force = ob::TimeInForce::GTC,
                         ob::ParticipantId participant_id = 0) noexcept {
    ob::InboundMsg command = msg(seq, ob::MsgType::NewLimit, side, price, qty);
    command.time_in_force = time_in_force;
    command.participant_id = participant_id;
    return command;
}

ob::InboundMsg market_msg(uint64_t seq, ob::Side side, ob::Qty qty) noexcept {
    return msg(seq, ob::MsgType::NewMarket, side, 0, qty);
}

ob::InboundMsg cancel_msg(uint64_t seq, ob::Handle handle) noexcept {
    return msg(seq, ob::MsgType::Cancel, ob::Side::Bid, 0, 0, handle);
}

ob::InboundMsg stop_engine_msg(uint64_t seq) noexcept {
    return msg(seq, ob::MsgType::StopEngine, ob::Side::Bid, 0, 0);
}

bool complete(const ob::OutboundEvent& event) {
    return (event.flags & ob::RequestComplete) != 0;
}

bool one_complete(const RecordingSink& sink) {
    // Every inbound command must have exactly one final event.
    std::size_t completed = 0;
    for (std::size_t i = 0; i < sink.count && i < sink.events.size(); ++i) {
        completed += complete(sink.events[i]) ? 1 : 0;
    }
    return completed == 1;
}

bool expect_count(const RecordingSink& sink, std::size_t count, const char* message) {
    return sink.count == count && one_complete(sink) ? true : fail(message);
}

bool expect_reject(const ob::OutboundEvent& event, uint64_t seq, ob::RejectReason reason,
                   ob::Qty qty, bool done = true, ob::ParticipantId participant_id = 0) {
    if (event.type != ob::EventType::Reject || event.client_seq != seq || event.reason != reason ||
        event.handle != 0 || event.price != 0 || event.qty != qty || complete(event) != done ||
        event.participant_id != participant_id) {
        return fail("Reject event mismatch");
    }
    return true;
}

bool expect_ack_new(const ob::OutboundEvent& event, uint64_t seq, ob::Price price, ob::Qty qty,
                    ob::Side side, ob::ParticipantId participant_id = 0) {
    if (event.type != ob::EventType::AckNew || event.client_seq != seq || event.handle == 0 ||
        event.price != price || event.qty != qty || event.side != side ||
        event.reason != ob::RejectReason::None || !complete(event) ||
        event.participant_id != participant_id) {
        return fail("AckNew event mismatch");
    }
    return true;
}

bool expect_ack_cancel(const ob::OutboundEvent& event, uint64_t seq, ob::Handle handle,
                       ob::Price price, ob::Qty qty, ob::Side side,
                       ob::ParticipantId participant_id = 0) {
    if (event.type != ob::EventType::AckCancel || event.client_seq != seq ||
        event.handle != handle || event.price != price || event.qty != qty || event.side != side ||
        event.reason != ob::RejectReason::None || !complete(event) ||
        event.participant_id != participant_id) {
        return fail("AckCancel event mismatch");
    }
    return true;
}

bool expect_fill(const ob::OutboundEvent& event, uint64_t seq, ob::Handle handle, ob::Price price,
                 ob::Qty qty, ob::Side side, bool done, ob::ParticipantId participant_id = 0) {
    if (event.type != ob::EventType::Fill || event.client_seq != seq || event.handle != handle ||
        event.price != price || event.qty != qty || event.side != side ||
        event.reason != ob::RejectReason::None || complete(event) != done ||
        event.participant_id != participant_id) {
        return fail("Fill event mismatch");
    }
    return true;
}

template <typename Matcher>
std::optional<ob::Handle> rest_limit(Matcher& matcher, uint64_t seq, ob::Side side, ob::Price price,
                                     ob::Qty qty, ob::ParticipantId participant_id = 0) {
    RecordingSink sink;
    matcher.process(limit_msg(seq, side, price, qty, ob::TimeInForce::GTC, participant_id), sink);
    if (!expect_count(sink, 1, "rest setup did not emit one complete event") ||
        !expect_ack_new(sink.events[0], seq, price, qty, side, participant_id)) {
        return std::nullopt;
    }
    return sink.events[0].handle;
}

template <typename Matcher>
bool process_has_one_complete(Matcher& matcher, const ob::InboundMsg& command) {
    RecordingSink sink;
    matcher.process(command, sink);
    return one_complete(sink);
}

// Bad limit inputs should reject immediately and leave the book unchanged.
bool check_limit_validation_rejects() {
    TestMatcher matcher;
    RecordingSink zero_qty;
    RecordingSink out_of_band;

    matcher.process(limit_msg(1, ob::Side::Bid, 10, 0), zero_qty);
    matcher.process(limit_msg(2, ob::Side::Ask, TestMatcher::limit_price(), 10), out_of_band);

    return expect_count(zero_qty, 1, "zero-qty limit event count failed") &&
           expect_reject(zero_qty.events[0], 1, ob::RejectReason::ZeroQty, 0) &&
           expect_count(out_of_band, 1, "out-of-band limit event count failed") &&
           expect_reject(out_of_band.events[0], 2, ob::RejectReason::PriceOutOfBand, 10) &&
           matcher.book().audit();
}

// A non-crossing limit order should rest and return a usable engine handle.
bool check_limit_rests_and_acknowledges_handle() {
    TestMatcher matcher;
    const auto handle = rest_limit(matcher, 10, ob::Side::Bid, 42, 75);
    if (!handle) {
        return false;
    }

    const ob::Order* order = matcher.book().pool().resolve(*handle);
    if (order == nullptr || order->client_seq != 10 || order->price != 42 ||
        order->remaining != 75 || order->participant_id != 0 || order->side != ob::Side::Bid) {
        return fail("resting order fields failed");
    }
    return matcher.book().occupied(42) && matcher.book().level(42).order_count == 1 &&
           matcher.book().best_bid_idx() == std::optional<std::size_t>{42} &&
           matcher.book().audit();
}

// A market order into an empty opposite side should reject the full quantity.
bool check_market_empty_rejects() {
    TestMatcher matcher;
    RecordingSink sink;
    matcher.process(market_msg(20, ob::Side::Ask, 60), sink);
    return expect_count(sink, 1, "empty-market event count failed") &&
           expect_reject(sink.events[0], 20, ob::RejectReason::InsufficientLiquidity, 60) &&
           matcher.book().audit();
}

// A small crossing limit order should partially fill the resting head order.
bool check_single_fill_event_order() {
    TestMatcher matcher;
    const auto ask = rest_limit(matcher, 50, ob::Side::Ask, 100, 40);
    if (!ask) {
        return false;
    }

    RecordingSink sink;
    matcher.process(limit_msg(51, ob::Side::Bid, 100, 10), sink);
    const ob::Order* resting = matcher.book().pool().resolve(*ask);
    return expect_count(sink, 2, "single-fill event count failed") &&
           expect_fill(sink.events[0], 50, *ask, 100, 10, ob::Side::Ask, false) &&
           expect_fill(sink.events[1], 51, 0, 100, 10, ob::Side::Bid, true) && resting != nullptr &&
           resting->remaining == 30 && matcher.book().audit();
}

// A limit order can trade first, then rest only its leftover quantity.
bool check_partial_cross_then_rest() {
    TestMatcher matcher;
    const auto ask = rest_limit(matcher, 60, ob::Side::Ask, 100, 10);
    if (!ask) {
        return false;
    }

    RecordingSink sink;
    matcher.process(limit_msg(61, ob::Side::Bid, 100, 25), sink);
    return expect_count(sink, 3, "partial-cross event count failed") &&
           expect_fill(sink.events[0], 60, *ask, 100, 10, ob::Side::Ask, false) &&
           expect_fill(sink.events[1], 61, 0, 100, 10, ob::Side::Bid, false) &&
           expect_ack_new(sink.events[2], 61, 100, 15, ob::Side::Bid) && matcher.book().audit();
}

// A crossing order should walk price levels best-first.
bool check_multi_level_walk() {
    TestMatcher matcher;
    const auto ask100 = rest_limit(matcher, 70, ob::Side::Ask, 100, 10);
    const auto ask101 = rest_limit(matcher, 71, ob::Side::Ask, 101, 20);
    if (!ask100 || !ask101) {
        return false;
    }

    RecordingSink sink;
    matcher.process(limit_msg(72, ob::Side::Bid, 101, 30), sink);
    return expect_count(sink, 4, "multi-level event count failed") &&
           expect_fill(sink.events[0], 70, *ask100, 100, 10, ob::Side::Ask, false) &&
           expect_fill(sink.events[1], 72, 0, 100, 10, ob::Side::Bid, false) &&
           expect_fill(sink.events[2], 71, *ask101, 101, 20, ob::Side::Ask, false) &&
           expect_fill(sink.events[3], 72, 0, 101, 20, ob::Side::Bid, true) &&
           !matcher.book().best_ask_idx() && !matcher.book().best_bid_idx() &&
           matcher.book().audit();
}

// Multiple orders at one price should fill FIFO by arrival time.
bool check_same_price_fifo_walk() {
    TestMatcher matcher;
    const auto first = rest_limit(matcher, 80, ob::Side::Ask, 100, 10);
    const auto second = rest_limit(matcher, 81, ob::Side::Ask, 100, 15);
    if (!first || !second) {
        return false;
    }

    RecordingSink sink;
    matcher.process(limit_msg(82, ob::Side::Bid, 100, 25), sink);
    return expect_count(sink, 4, "same-price FIFO event count failed") &&
           expect_fill(sink.events[0], 80, *first, 100, 10, ob::Side::Ask, false) &&
           expect_fill(sink.events[1], 82, 0, 100, 10, ob::Side::Bid, false) &&
           expect_fill(sink.events[2], 81, *second, 100, 15, ob::Side::Ask, false) &&
           expect_fill(sink.events[3], 82, 0, 100, 15, ob::Side::Bid, true) &&
           !matcher.book().occupied(100) && matcher.book().audit();
}

// A market order should consume available prices best-first and never rest.
bool check_market_fills_available_liquidity() {
    TestMatcher matcher;
    const auto ask100 = rest_limit(matcher, 90, ob::Side::Ask, 100, 10);
    const auto ask101 = rest_limit(matcher, 91, ob::Side::Ask, 101, 20);
    if (!ask100 || !ask101) {
        return false;
    }

    RecordingSink sink;
    matcher.process(market_msg(92, ob::Side::Bid, 30), sink);
    return expect_count(sink, 4, "market-fill event count failed") &&
           expect_fill(sink.events[0], 90, *ask100, 100, 10, ob::Side::Ask, false) &&
           expect_fill(sink.events[1], 92, 0, 100, 10, ob::Side::Bid, false) &&
           expect_fill(sink.events[2], 91, *ask101, 101, 20, ob::Side::Ask, false) &&
           expect_fill(sink.events[3], 92, 0, 101, 20, ob::Side::Bid, true) &&
           !matcher.book().best_ask_idx() && !matcher.book().best_bid_idx() &&
           matcher.book().audit();
}

// If a market order runs out of liquidity, prior fills stand and the remainder is rejected.
bool check_market_partial_fill_then_reject() {
    TestMatcher matcher;
    const auto ask = rest_limit(matcher, 100, ob::Side::Ask, 100, 10);
    if (!ask) {
        return false;
    }

    RecordingSink sink;
    matcher.process(market_msg(101, ob::Side::Bid, 25), sink);
    return expect_count(sink, 3, "market-reject event count failed") &&
           expect_fill(sink.events[0], 100, *ask, 100, 10, ob::Side::Ask, false) &&
           expect_fill(sink.events[1], 101, 0, 100, 10, ob::Side::Bid, false) &&
           expect_reject(sink.events[2], 101, ob::RejectReason::InsufficientLiquidity, 15) &&
           matcher.book().audit();
}

// Pool exhaustion is hit when a new limit order needs to rest without freeing a slot first.
bool check_pool_exhausted_rejects_resting_remainder() {
    ob::Matcher<ob::config::kFuzz.num_levels, 1> matcher;
    if (!rest_limit(matcher, 110, ob::Side::Bid, 90, 10)) {
        return false;
    }

    RecordingSink sink;
    matcher.process(limit_msg(111, ob::Side::Bid, 91, 20), sink);
    return expect_count(sink, 1, "pool-exhaust event count failed") &&
           expect_reject(sink.events[0], 111, ob::RejectReason::PoolExhausted, 20) &&
           matcher.book().audit();
}

// If a fill frees a slot, the incoming order can use that slot to rest its remainder.
bool check_full_pool_cross_can_rest_after_freeing_slot() {
    ob::Matcher<ob::config::kFuzz.num_levels, 1> matcher;
    const auto ask = rest_limit(matcher, 120, ob::Side::Ask, 100, 10);
    if (!ask) {
        return false;
    }

    RecordingSink sink;
    matcher.process(limit_msg(121, ob::Side::Bid, 100, 25), sink);
    return expect_count(sink, 3, "full-pool cross event count failed") &&
           expect_fill(sink.events[0], 120, *ask, 100, 10, ob::Side::Ask, false) &&
           expect_fill(sink.events[1], 121, 0, 100, 10, ob::Side::Bid, false) &&
           expect_ack_new(sink.events[2], 121, 100, 15, ob::Side::Bid) && matcher.book().audit();
}

// IOC means "trade what is available now, but do not rest the leftover".
bool check_ioc_partial_fill_rejects_remainder() {
    TestMatcher matcher;
    const auto ask = rest_limit(matcher, 130, ob::Side::Ask, 100, 10);
    if (!ask) {
        return false;
    }

    RecordingSink sink;
    matcher.process(limit_msg(131, ob::Side::Bid, 100, 25, ob::TimeInForce::IOC), sink);
    return expect_count(sink, 3, "IOC partial-fill event count failed") &&
           expect_fill(sink.events[0], 130, *ask, 100, 10, ob::Side::Ask, false) &&
           expect_fill(sink.events[1], 131, 0, 100, 10, ob::Side::Bid, false) &&
           expect_reject(sink.events[2], 131, ob::RejectReason::ImmediateOrCancelRemainder, 15) &&
           !matcher.book().best_ask_idx() && !matcher.book().best_bid_idx() &&
           matcher.book().audit();
}

// FOK means "fill the whole order immediately, or leave the book untouched".
bool check_fok_insufficient_liquidity_fills_nothing() {
    TestMatcher matcher;
    const auto ask = rest_limit(matcher, 140, ob::Side::Ask, 100, 10);
    if (!ask) {
        return false;
    }

    RecordingSink sink;
    matcher.process(limit_msg(141, ob::Side::Bid, 100, 25, ob::TimeInForce::FOK), sink);
    const ob::Order* resting = matcher.book().pool().resolve(*ask);
    return expect_count(sink, 1, "FOK insufficient event count failed") &&
           expect_reject(sink.events[0], 141, ob::RejectReason::FillOrKillNotFilled, 25) &&
           resting != nullptr && resting->remaining == 10 && matcher.book().audit();
}

// When enough quantity is immediately available, FOK uses the normal fill path.
bool check_fok_exact_liquidity_fills_fully() {
    TestMatcher matcher;
    const auto ask100 = rest_limit(matcher, 150, ob::Side::Ask, 100, 10);
    const auto ask101 = rest_limit(matcher, 151, ob::Side::Ask, 101, 15);
    if (!ask100 || !ask101) {
        return false;
    }

    RecordingSink sink;
    matcher.process(limit_msg(152, ob::Side::Bid, 101, 25, ob::TimeInForce::FOK), sink);
    return expect_count(sink, 4, "FOK full-fill event count failed") &&
           expect_fill(sink.events[0], 150, *ask100, 100, 10, ob::Side::Ask, false) &&
           expect_fill(sink.events[1], 152, 0, 100, 10, ob::Side::Bid, false) &&
           expect_fill(sink.events[2], 151, *ask101, 101, 15, ob::Side::Ask, false) &&
           expect_fill(sink.events[3], 152, 0, 101, 15, ob::Side::Bid, true) &&
           !matcher.book().best_ask_idx() && !matcher.book().best_bid_idx() &&
           matcher.book().audit();
}

// Same non-zero participant on both sides rejects the newer incoming order.
bool check_self_trade_rejects_incoming_order() {
    TestMatcher matcher;
    const auto ask = rest_limit(matcher, 160, ob::Side::Ask, 100, 10, 7);
    if (!ask) {
        return false;
    }

    RecordingSink sink;
    matcher.process(limit_msg(161, ob::Side::Bid, 100, 5, ob::TimeInForce::GTC, 7), sink);
    const ob::Order* resting = matcher.book().pool().resolve(*ask);
    return expect_count(sink, 1, "self-trade reject event count failed") &&
           expect_reject(sink.events[0], 161, ob::RejectReason::SelfTrade, 5, true, 7) &&
           resting != nullptr && resting->remaining == 10 && matcher.book().audit();
}

// Different non-zero participants should trade normally.
bool check_different_participants_can_cross() {
    TestMatcher matcher;
    const auto ask = rest_limit(matcher, 170, ob::Side::Ask, 100, 10, 7);
    if (!ask) {
        return false;
    }

    RecordingSink sink;
    matcher.process(limit_msg(171, ob::Side::Bid, 100, 5, ob::TimeInForce::GTC, 8), sink);
    const ob::Order* resting = matcher.book().pool().resolve(*ask);
    return expect_count(sink, 2, "different-participant fill event count failed") &&
           expect_fill(sink.events[0], 170, *ask, 100, 5, ob::Side::Ask, false, 7) &&
           expect_fill(sink.events[1], 171, 0, 100, 5, ob::Side::Bid, true, 8) &&
           resting != nullptr && resting->remaining == 5 && matcher.book().audit();
}

// TCP responses will be routed by participant_id. Resting-party fills must go
// to the resting order's participant, while aggressor events go to the sender.
bool check_events_carry_routing_participant() {
    TestMatcher matcher;
    const auto ask = rest_limit(matcher, 180, ob::Side::Ask, 100, 10, 11);
    if (!ask) {
        return false;
    }

    RecordingSink fill_sink;
    matcher.process(limit_msg(181, ob::Side::Bid, 100, 5, ob::TimeInForce::GTC, 22), fill_sink);

    const auto bid = rest_limit(matcher, 182, ob::Side::Bid, 99, 7, 33);
    if (!bid) {
        return false;
    }

    RecordingSink cancel_sink;
    ob::InboundMsg cancel = cancel_msg(183, *bid);
    cancel.participant_id = 33;
    matcher.process(cancel, cancel_sink);

    return expect_count(fill_sink, 2, "participant-routed fill event count failed") &&
           expect_fill(fill_sink.events[0], 180, *ask, 100, 5, ob::Side::Ask, false, 11) &&
           expect_fill(fill_sink.events[1], 181, 0, 100, 5, ob::Side::Bid, true, 22) &&
           expect_count(cancel_sink, 1, "participant-routed cancel event count failed") &&
           expect_ack_cancel(cancel_sink.events[0], 183, *bid, 99, 7, ob::Side::Bid, 33) &&
           matcher.book().audit();
}

// A valid cancel should remove the resting order and report its removed fields.
bool check_cancel_acknowledges_and_removes() {
    TestMatcher matcher;
    const auto handle = rest_limit(matcher, 30, ob::Side::Ask, 100, 25);
    if (!handle) {
        return false;
    }

    RecordingSink sink;
    matcher.process(cancel_msg(31, *handle), sink);
    return expect_count(sink, 1, "cancel event count failed") &&
           expect_ack_cancel(sink.events[0], 31, *handle, 100, 25, ob::Side::Ask) &&
           matcher.book().pool().resolve(*handle) == nullptr && !matcher.book().occupied(100) &&
           matcher.book().audit();
}

// Null handles surface as UnknownHandle rejects.
bool check_unknown_cancel_rejects() {
    TestMatcher matcher;
    RecordingSink sink;
    matcher.process(cancel_msg(40, 0), sink);
    return expect_count(sink, 1, "unknown-cancel event count failed") &&
           expect_reject(sink.events[0], 40, ob::RejectReason::UnknownHandle, 0) &&
           matcher.book().audit();
}

// StopEngine is an internal shutdown marker, not a trading stop order.
bool check_stop_engine_emits_internal_event() {
    TestMatcher matcher;
    RecordingSink sink;
    matcher.process(stop_engine_msg(90), sink);
    return expect_count(sink, 1, "StopEngine event count failed") &&
           sink.events[0].type == ob::EventType::StopEngine && sink.events[0].client_seq == 90 &&
           sink.events[0].reason == ob::RejectReason::None && sink.events[0].qty == 0 &&
           sink.events[0].handle == 0 && sink.events[0].price == 0 && matcher.book().audit();
}

// Representative success, reject, fill, cancel, and shutdown paths all complete once.
bool check_request_complete_across_representative_paths() {
    TestMatcher matcher;

    const auto ask = rest_limit(matcher, 200, ob::Side::Ask, 100, 10);
    if (!ask) {
        return false;
    }

    // Rest, reject, fill, and market-reject paths.
    if (!process_has_one_complete(matcher, limit_msg(201, ob::Side::Bid, 90, 5)) ||
        !process_has_one_complete(matcher, limit_msg(202, ob::Side::Bid, 90, 0)) ||
        !process_has_one_complete(matcher, market_msg(203, ob::Side::Bid, 5)) ||
        !process_has_one_complete(matcher, market_msg(204, ob::Side::Ask, 5))) {
        return false;
    }

    const auto cancel_handle = rest_limit(matcher, 205, ob::Side::Ask, 101, 10);
    if (!cancel_handle) {
        return false;
    }

    // Cancel success, cancel reject, and shutdown paths.
    return process_has_one_complete(matcher, cancel_msg(206, *cancel_handle)) &&
           process_has_one_complete(matcher, cancel_msg(207, 0)) &&
           process_has_one_complete(matcher, stop_engine_msg(208)) && matcher.book().audit();
}

}  // namespace

int main() {
    using Check = bool (*)();
    constexpr std::array<Check, 21> checks{
        check_limit_validation_rejects,
        check_limit_rests_and_acknowledges_handle,
        check_market_empty_rejects,
        check_single_fill_event_order,
        check_partial_cross_then_rest,
        check_multi_level_walk,
        check_same_price_fifo_walk,
        check_market_fills_available_liquidity,
        check_market_partial_fill_then_reject,
        check_pool_exhausted_rejects_resting_remainder,
        check_full_pool_cross_can_rest_after_freeing_slot,
        check_ioc_partial_fill_rejects_remainder,
        check_fok_insufficient_liquidity_fills_nothing,
        check_fok_exact_liquidity_fills_fully,
        check_self_trade_rejects_incoming_order,
        check_different_participants_can_cross,
        check_events_carry_routing_participant,
        check_cancel_acknowledges_and_removes,
        check_unknown_cancel_rejects,
        check_stop_engine_emits_internal_event,
        check_request_complete_across_representative_paths,
    };

    for (Check check : checks) {
        if (!check()) {
            return 1;
        }
    }

    std::printf("matcher_test OK\n");
    return 0;
}
