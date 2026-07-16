// matching_loop_test.cpp - Ring-to-matcher-to-ring checks for the engine loop.

#include <array>
#include <cstdio>

#include "orderbook/config.hpp"
#include "orderbook/matching_loop.hpp"
#include "orderbook/types.hpp"

namespace {

using InboundRing = ob::SpscRing<ob::InboundMsg, 16>;
using OutboundRing = ob::SpscRing<ob::OutboundEvent, 16>;
using TestLoop = ob::MatchingLoop<ob::config::kFuzz.num_levels, 8, 16, 16>;

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

ob::InboundMsg limit_msg(uint64_t seq, ob::Side side, ob::Price price, ob::Qty qty) noexcept {
    return msg(seq, ob::MsgType::NewLimit, side, price, qty);
}

ob::InboundMsg stop_msg(uint64_t seq) noexcept {
    return msg(seq, ob::MsgType::StopEngine, ob::Side::Bid, 0, 0);
}

bool pop_event(OutboundRing& outbound, ob::OutboundEvent& event) {
    return outbound.pop(event) ? true : fail("Expected outbound event was missing");
}

bool check_loop_forwards_matcher_events_and_stop() {
    InboundRing inbound;
    OutboundRing outbound;
    TestLoop loop{inbound, outbound};

    if (!inbound.push(limit_msg(1, ob::Side::Ask, 100, 10)) ||
        !inbound.push(limit_msg(2, ob::Side::Bid, 100, 4)) || !inbound.push(stop_msg(3))) {
        return fail("Inbound setup push failed");
    }

    const ob::MatchingLoopStats stats = loop.run_until_stop(ob::WaitMode::Yield);
    if (!stats.stopped || stats.processed != 3 || stats.emitted != 4) {
        return fail("Matching loop stats mismatch");
    }

    ob::OutboundEvent event{};
    if (!pop_event(outbound, event) || event.type != ob::EventType::AckNew ||
        event.client_seq != 1 || event.qty != 10) {
        return fail("Resting ack event mismatch");
    }
    if (event.tsc_egress == 0) {
        return fail("Matching loop did not stamp event egress time");
    }

    if (!pop_event(outbound, event) || event.type != ob::EventType::Fill || event.client_seq != 1 ||
        event.qty != 4 || event.flags != 0) {
        return fail("Resting fill event mismatch");
    }

    if (!pop_event(outbound, event) || event.type != ob::EventType::Fill || event.client_seq != 2 ||
        event.qty != 4 || (event.flags & ob::RequestComplete) == 0) {
        return fail("Aggressor fill event mismatch");
    }

    if (!pop_event(outbound, event) || event.type != ob::EventType::StopEngine ||
        event.client_seq != 3 || (event.flags & ob::RequestComplete) == 0) {
        return fail("StopEngine event mismatch");
    }

    if (outbound.pop(event)) {
        return fail("Unexpected extra outbound event");
    }
    return loop.matcher().book().audit();
}

}  // namespace

int main() {
    using Check = bool (*)();
    constexpr std::array<Check, 1> checks{
        check_loop_forwards_matcher_events_and_stop,
    };

    for (Check check : checks) {
        if (!check()) {
            return 1;
        }
    }

    std::printf("matching_loop_test OK\n");
    return 0;
}
