// reference_test.cpp - Compare the optimized matcher with a simple oracle.

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
using Oracle = ob::test::ReferenceBook<ob::config::kFuzz.num_levels, 8>;

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
    bool streams_match(const std::vector<ob::OutboundEvent>& engine_events,
                       const std::vector<ob::OutboundEvent>& oracle_events) {
        if (engine_events.size() != oracle_events.size()) {
            return fail("event stream sizes differed");
        }

        for (std::size_t i = 0; i < engine_events.size(); ++i) {
            if (!event_matches(engine_events[i], oracle_events[i])) {
                std::fprintf(stderr, "event mismatch at index %zu\n", i);
                return false;
            }
        }

        return true;
    }

    std::optional<ob::Handle> oracle_handle_for(ob::Handle engine_handle) const {
        const auto mapped = engine_to_oracle_.find(engine_handle);
        if (mapped == engine_to_oracle_.end()) {
            return std::nullopt;
        }
        return mapped->second;
    }

  private:
    bool event_matches(const ob::OutboundEvent& engine, const ob::OutboundEvent& oracle) {
        if (engine.client_seq != oracle.client_seq || engine.price != oracle.price ||
            engine.qty != oracle.qty || engine.side != oracle.side || engine.type != oracle.type ||
            engine.reason != oracle.reason || engine.flags != oracle.flags) {
            return false;
        }

        if (engine.type == ob::EventType::AckNew) {
            if (engine.handle == 0 || oracle.handle == 0) {
                return false;
            }
            // AckNew is where the two independent handle schemes become comparable.
            engine_to_oracle_[engine.handle] = oracle.handle;
            return true;
        }

        if (engine.handle == 0 || oracle.handle == 0) {
            return engine.handle == oracle.handle;
        }

        const auto mapped = engine_to_oracle_.find(engine.handle);
        return mapped != engine_to_oracle_.end() && mapped->second == oracle.handle;
    }

    std::unordered_map<ob::Handle, ob::Handle> engine_to_oracle_;
};

bool process_and_compare(Engine& engine, Oracle& oracle, const ob::InboundMsg& engine_msg,
                         const ob::InboundMsg& oracle_msg, EventComparator& comparator) {
    RecordingSink engine_sink;
    RecordingSink oracle_sink;

    engine.process(engine_msg, engine_sink);
    oracle.process(oracle_msg, oracle_sink);

    return comparator.streams_match(engine_sink.events, oracle_sink.events) &&
           engine.book().audit();
}

bool check_deterministic_sequence_matches_reference() {
    Engine engine;
    Oracle oracle;
    EventComparator comparator;

    // Rest asks, cross them, then verify market consume/rest/cancel behavior.
    const std::array sequence{
        limit_msg(1, ob::Side::Ask, 100, 10), limit_msg(2, ob::Side::Ask, 101, 20),
        limit_msg(3, ob::Side::Bid, 101, 25), market_msg(4, ob::Side::Ask, 10),
        limit_msg(5, ob::Side::Bid, 99, 7),
    };
    for (const ob::InboundMsg& command : sequence) {
        if (!process_and_compare(engine, oracle, command, command, comparator)) {
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

    // The oracle uses its own handle scheme; AckNew correspondence is checked
    // in event comparison, while cancel inputs must use each engine's handle.
    const std::optional<ob::Handle> oracle_handle = comparator.oracle_handle_for(engine_handle);
    if (!oracle_handle) {
        return fail("oracle cancel setup handle missing");
    }
    return process_and_compare(engine, oracle, cancel_msg(6, engine_handle),
                               cancel_msg(6, *oracle_handle), comparator);
}

}  // namespace

int main() {
    if (!check_deterministic_sequence_matches_reference()) {
        return 1;
    }

    std::printf("reference_test OK\n");
    return 0;
}
