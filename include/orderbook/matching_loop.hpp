// matching_loop.hpp - Thread-runner boundary around the synchronous matcher.
//
// The matcher itself is deliberately single-threaded. This wrapper is the
// cross-thread shell: it drains inbound commands from one SPSC ring, lets the
// matcher process each command, and forwards emitted events to the outbound
// ring.

#pragma once

#include <cstddef>
#include <cstdint>
#include <thread>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

#include "orderbook/matcher.hpp"
#include "orderbook/spsc_ring.hpp"
#include "orderbook/time.hpp"
#include "orderbook/types.hpp"

namespace ob {

enum class WaitMode : std::uint8_t {
    Spin,
    Yield,
};

struct MatchingLoopStats {
    std::uint64_t processed{0};
    std::uint64_t emitted{0};
    std::uint64_t inbound_empty_waits{0};
    std::uint64_t outbound_full_waits{0};
    bool stopped{false};
};

inline void matching_loop_wait(WaitMode mode) noexcept {
    if (mode == WaitMode::Yield) {
        std::this_thread::yield();
        return;
    }

#if defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#else
    std::this_thread::yield();
#endif
}

template <std::size_t NumLevels, std::size_t PoolCapacity, std::size_t InboundCapacity,
          std::size_t OutboundCapacity, Price BasePrice = config::BASE_PRICE>
class MatchingLoop {
  public:
    using MatcherType = Matcher<NumLevels, PoolCapacity, BasePrice>;
    using InboundRing = SpscRing<InboundMsg, InboundCapacity>;
    using OutboundRing = SpscRing<OutboundEvent, OutboundCapacity>;

    MatchingLoop(InboundRing& inbound, OutboundRing& outbound) noexcept
        : inbound_(inbound), outbound_(outbound) {}

    // Run until an internal StopEngine command is consumed. The matcher emits
    // the corresponding StopEngine event first, so the logger can drain cleanly.
    [[nodiscard]] MatchingLoopStats run_until_stop(WaitMode wait_mode = WaitMode::Spin) noexcept {
        MatchingLoopStats stats;
        InboundMsg msg{};

        for (;;) {
            if (!inbound_.pop(msg)) {
                ++stats.inbound_empty_waits;
                matching_loop_wait(wait_mode);
                continue;
            }

            ++stats.processed;
            auto sink = [&](const OutboundEvent& event) noexcept {
                OutboundEvent stamped_event = event;
                stamped_event.tsc_egress = engine_time_nanos();
                // The matching thread owns the event until the outbound ring
                // accepts it; no event is dropped when the logger falls behind.
                while (!outbound_.push(stamped_event)) {
                    ++stats.outbound_full_waits;
                    matching_loop_wait(wait_mode);
                }
                ++stats.emitted;
            };

            matcher_.process(msg, sink);
            if (msg.type == MsgType::StopEngine) {
                stats.stopped = true;
                return stats;
            }
        }
    }

    MatcherType& matcher() noexcept {
        return matcher_;
    }

    const MatcherType& matcher() const noexcept {
        return matcher_;
    }

  private:
    InboundRing& inbound_;
    OutboundRing& outbound_;
    MatcherType matcher_;
};

}  // namespace ob
