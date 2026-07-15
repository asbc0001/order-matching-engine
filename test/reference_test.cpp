// reference_test.cpp - Compare the optimized matcher with a simple reference book.

#include <array>
#include <cstdio>
#include <optional>
#include <unordered_map>
#include <vector>

#include "orderbook/config.hpp"
#include "orderbook/matcher.hpp"
#include "orderbook/types.hpp"
#include "reference_book.hpp"

namespace {

using Engine = ob::Matcher<ob::config::kFuzz.num_levels, 8>;
using Reference = ob::test::ReferenceBook<ob::config::kFuzz.num_levels, 8>;

struct RecordingSink {
    std::vector<ob::OutboundEvent> events;

    void operator()(const ob::OutboundEvent& event) {
        events.push_back(event);
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

class EventComparator {
  public:
    // Compare each operation's events in emission order. Engine/reference
    // handles differ, so AckNew records which handles mean the same order.
    bool streams_match(const std::vector<ob::OutboundEvent>& engine_events,
                       const std::vector<ob::OutboundEvent>& reference_events) {
        if (engine_events.size() != reference_events.size()) {
            return fail("event stream sizes differed");
        }

        for (std::size_t i = 0; i < engine_events.size(); ++i) {
            if (!event_matches(engine_events[i], reference_events[i])) {
                std::fprintf(stderr, "event mismatch at index %zu\n", i);
                return false;
            }
        }

        return true;
    }

    std::optional<ob::Handle> reference_handle_for(ob::Handle engine_handle) const {
        const auto mapped = engine_to_reference_.find(engine_handle);
        if (mapped == engine_to_reference_.end()) {
            return std::nullopt;
        }
        return mapped->second;
    }

  private:
    bool event_matches(const ob::OutboundEvent& engine, const ob::OutboundEvent& reference) {
        if (engine.client_seq != reference.client_seq || engine.price != reference.price ||
            engine.qty != reference.qty || engine.side != reference.side ||
            engine.type != reference.type || engine.reason != reference.reason ||
            engine.flags != reference.flags) {
            return false;
        }

        if (engine.type == ob::EventType::AckNew) {
            if (engine.handle == 0 || reference.handle == 0) {
                return false;
            }
            // AckNew is where the two handle values become comparable.
            engine_to_reference_[engine.handle] = reference.handle;
            return true;
        }

        if (engine.handle == 0 || reference.handle == 0) {
            return engine.handle == reference.handle;
        }

        const auto mapped = engine_to_reference_.find(engine.handle);
        return mapped != engine_to_reference_.end() && mapped->second == reference.handle;
    }

    std::unordered_map<ob::Handle, ob::Handle> engine_to_reference_;
};

bool process_and_compare(Engine& engine, Reference& reference, const ob::InboundMsg& engine_msg,
                         const ob::InboundMsg& reference_msg, EventComparator& comparator) {
    RecordingSink engine_sink;
    RecordingSink reference_sink;

    engine.process(engine_msg, engine_sink);
    reference.process(reference_msg, reference_sink);

    return comparator.streams_match(engine_sink.events, reference_sink.events) &&
           engine.book().audit();
}

bool check_deterministic_sequence_matches_reference() {
    Engine engine;
    Reference reference;
    EventComparator comparator;

    // Rest asks, cross them, then verify market consume/rest/cancel behavior.
    const std::array sequence{
        limit_msg(1, ob::Side::Ask, 100, 10), limit_msg(2, ob::Side::Ask, 101, 20),
        limit_msg(3, ob::Side::Bid, 101, 25), market_msg(4, ob::Side::Ask, 10),
        limit_msg(5, ob::Side::Bid, 99, 7),
    };
    for (const ob::InboundMsg& command : sequence) {
        if (!process_and_compare(engine, reference, command, command, comparator)) {
            return false;
        }
    }

    const ob::Handle engine_handle =
        engine.book().level(99).head == ob::NULL_SLOT
            ? 0
            : engine.book().pool().at(engine.book().level(99).head).handle;
    if (engine_handle == 0) {
        return fail("engine cancel setup handle missing");
    }

    // Cancel inputs must use each implementation's own handle.
    const std::optional<ob::Handle> reference_handle =
        comparator.reference_handle_for(engine_handle);
    if (!reference_handle) {
        return fail("reference cancel setup handle missing");
    }
    return process_and_compare(engine, reference, cancel_msg(6, engine_handle),
                               cancel_msg(6, *reference_handle), comparator);
}

}  // namespace

int main() {
    if (!check_deterministic_sequence_matches_reference()) {
        return 1;
    }

    std::printf("reference_test OK\n");
    return 0;
}
