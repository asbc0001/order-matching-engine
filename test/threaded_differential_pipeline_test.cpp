// threaded_differential_pipeline_test.cpp - Compare threaded engine output to the
// simple reference book on a reduced generated stream.
//
// The full differential fuzzer already checks many seeds synchronously. This
// reduced test keeps the count small and sends each command through the inbound
// ring and matching thread, so it proves the threaded path preserves the same
// logical behavior.

#include <array>
#include <cstdio>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

#include "orderbook/config.hpp"
#include "orderbook/matching_loop.hpp"
#include "orderbook/synthetic_generator.hpp"
#include "reference_book.hpp"

namespace {

constexpr std::size_t kNumLevels = ob::config::kFuzz.num_levels;
constexpr std::size_t kPoolCapacity = 32;
constexpr std::size_t kRingCapacity = ob::config::kFuzz.ring_capacity;
constexpr std::size_t kOpsPerSeed = 300;

using InboundRing = ob::SpscRing<ob::InboundMsg, kRingCapacity>;
using OutboundRing = ob::SpscRing<ob::OutboundEvent, kRingCapacity>;
using EngineLoop = ob::MatchingLoop<kNumLevels, kPoolCapacity, kRingCapacity, kRingCapacity>;
using Reference = ob::test::ReferenceBook<kNumLevels, kPoolCapacity>;
using Generator = ob::synthetic::CommandGenerator<kNumLevels, kPoolCapacity>;

struct RecordingSink {
    std::vector<ob::OutboundEvent> events;

    void operator()(const ob::OutboundEvent& event) {
        events.push_back(event);
    }
};

struct LiveOrder {
    ob::Handle reference_handle{0};
    ob::Qty remaining{0};
};

class HandleMap {
  public:
    // The optimized engine and the reference book assign different handle
    // values. A cancel must use the matching handle for whichever book is
    // processing it, so engine handles are translated before reference replay.
    void translate_cancel_for_reference(ob::InboundMsg& command) const {
        if (command.type != ob::MsgType::Cancel || command.handle == 0) {
            return;
        }

        const auto found = live_.find(command.handle);
        if (found != live_.end()) {
            command.handle = found->second.reference_handle;
        }
    }

    bool events_match(const std::vector<ob::OutboundEvent>& engine,
                      const std::vector<ob::OutboundEvent>& reference) const {
        if (engine.size() != reference.size()) {
            std::fprintf(stderr, "event count differed: engine=%zu reference=%zu\n", engine.size(),
                         reference.size());
            return false;
        }

        for (std::size_t i = 0; i < engine.size(); ++i) {
            if (!event_matches(engine[i], reference[i])) {
                std::fprintf(stderr, "event mismatch at index %zu\n", i);
                return false;
            }
        }
        return true;
    }

    void apply(const std::vector<ob::OutboundEvent>& engine,
               const std::vector<ob::OutboundEvent>& reference) {
        for (std::size_t i = 0; i < engine.size(); ++i) {
            apply_one(engine[i], reference[i]);
        }
    }

  private:
    // Timing fields are intentionally ignored here. They depend on thread
    // scheduling, while this test is checking logical matching behavior.
    bool event_matches(const ob::OutboundEvent& engine, const ob::OutboundEvent& reference) const {
        if (engine.client_seq != reference.client_seq || engine.price != reference.price ||
            engine.qty != reference.qty || engine.side != reference.side ||
            engine.type != reference.type || engine.reason != reference.reason ||
            engine.flags != reference.flags) {
            return false;
        }

        if (engine.type == ob::EventType::AckNew) {
            return engine.handle != 0 && reference.handle != 0;
        }
        if (engine.handle == 0 || reference.handle == 0) {
            return engine.handle == reference.handle;
        }

        const auto found = live_.find(engine.handle);
        return found != live_.end() && found->second.reference_handle == reference.handle;
    }

    void apply_one(const ob::OutboundEvent& engine, const ob::OutboundEvent& reference) {
        if (engine.type == ob::EventType::AckNew) {
            live_[engine.handle] = LiveOrder{
                .reference_handle = reference.handle,
                .remaining = engine.qty,
            };
            return;
        }

        if (engine.type == ob::EventType::AckCancel) {
            live_.erase(engine.handle);
            return;
        }

        // Resting-side fills carry the resting order handle. Aggressor fills
        // use handle 0 and do not identify a live order to update.
        if (engine.type == ob::EventType::Fill && engine.handle != 0) {
            reduce_or_remove(engine.handle, engine.qty);
        }
    }

    void reduce_or_remove(ob::Handle handle, ob::Qty qty) {
        const auto found = live_.find(handle);
        if (found == live_.end()) {
            return;
        }
        if (qty >= found->second.remaining) {
            live_.erase(found);
            return;
        }
        found->second.remaining -= qty;
    }

    std::unordered_map<ob::Handle, LiveOrder> live_;
};

void push_command(InboundRing& inbound, const ob::InboundMsg& command) {
    while (!inbound.push(command)) {
        ob::matching_loop_wait(ob::WaitMode::Yield);
    }
}

std::vector<ob::OutboundEvent> drain_one_command(OutboundRing& outbound) {
    std::vector<ob::OutboundEvent> events;
    ob::OutboundEvent event{};

    // RequestComplete is the command boundary. A crossing order may emit
    // several fills first, so the test keeps draining until that final marker.
    for (;;) {
        if (!outbound.pop(event)) {
            ob::matching_loop_wait(ob::WaitMode::Yield);
            continue;
        }

        events.push_back(event);
        if ((event.flags & ob::RequestComplete) != 0) {
            return events;
        }
    }
}

bool run_seed(std::uint64_t seed) {
    auto inbound = std::make_unique<InboundRing>();
    auto outbound = std::make_unique<OutboundRing>();
    EngineLoop loop{*inbound, *outbound};
    Reference reference;
    Generator generator{seed};
    HandleMap handles;

    std::thread matcher_thread{
        [&]() { (void)loop.run_until_stop(ob::WaitMode::Yield); },
    };

    for (std::uint64_t seq = 1; seq <= kOpsPerSeed; ++seq) {
        const ob::InboundMsg engine_command = generator.next(seq);
        ob::InboundMsg reference_command = engine_command;
        handles.translate_cancel_for_reference(reference_command);

        push_command(*inbound, engine_command);
        std::vector<ob::OutboundEvent> engine_events = drain_one_command(*outbound);

        RecordingSink reference_sink;
        reference.process(reference_command, reference_sink);

        if (!handles.events_match(engine_events, reference_sink.events)) {
            std::fprintf(stderr, "threaded differential pipeline failed: seed=%llu seq=%llu\n",
                         static_cast<unsigned long long>(seed),
                         static_cast<unsigned long long>(seq));
            ob::InboundMsg stop{
                .client_seq = kOpsPerSeed + 1,
                .handle = 0,
                .price = 0,
                .qty = 0,
                .side = ob::Side::Bid,
                .type = ob::MsgType::StopEngine,
                .tsc_intended = 0,
                .tsc_ready = 0,
                .tsc_enqueue = 0,
            };
            push_command(*inbound, stop);
            (void)drain_one_command(*outbound);
            matcher_thread.join();
            return false;
        }

        handles.apply(engine_events, reference_sink.events);
        // Feed the real threaded output back into the generator so later
        // generated cancels can target handles the engine actually assigned.
        for (const ob::OutboundEvent& event : engine_events) {
            generator.observe(event);
        }
    }

    ob::InboundMsg stop{
        .client_seq = kOpsPerSeed + 1,
        .handle = 0,
        .price = 0,
        .qty = 0,
        .side = ob::Side::Bid,
        .type = ob::MsgType::StopEngine,
        .tsc_intended = 0,
        .tsc_ready = 0,
        .tsc_enqueue = 0,
    };
    push_command(*inbound, stop);
    const std::vector<ob::OutboundEvent> stop_events = drain_one_command(*outbound);
    matcher_thread.join();

    // After join, the matching thread is no longer writing the book. The main
    // thread can audit it directly without locks or atomics.
    if (stop_events.size() != 1 || stop_events.front().type != ob::EventType::StopEngine) {
        std::fprintf(stderr, "threaded engine did not emit a single StopEngine event\n");
        return false;
    }

    return loop.matcher().book().audit();
}

}  // namespace

int main() {
    constexpr std::array<std::uint64_t, 2> seeds{0xC0FFEEu, 0x51DE5u};
    for (std::uint64_t seed : seeds) {
        if (!run_seed(seed)) {
            return 1;
        }
    }

    std::printf("threaded_differential_pipeline_test OK\n");
    return 0;
}
