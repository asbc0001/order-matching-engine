// event_logger.hpp - Small consumers for the matcher's outbound event stream.
//
// The matching engine emits OutboundEvent records. Benchmark runs usually only
// need cheap counters, while replay/debug runs need deterministic text that can
// be compared across runs. These sinks cover those two cases without changing
// the matcher itself.

#pragma once

#include <cstdint>
#include <cstdio>

#include "orderbook/types.hpp"

namespace ob {

struct MemoryLogStats {
    std::uint64_t total_events{0};
    std::uint64_t completed_requests{0};
    std::uint64_t fills{0};
    std::uint64_t rejects{0};
    std::uint64_t accepted_new_orders{0};
    std::uint64_t accepted_cancels{0};
    std::uint64_t stop_events{0};
};

// Counts event kinds without doing file I/O. This is the logger used when a
// benchmark wants the engine to drain normally but does not need a replay log.
class MemoryEventSink {
  public:
    void operator()(const OutboundEvent& event) noexcept {
        ++stats_.total_events;

        if ((event.flags & RequestComplete) != 0) {
            ++stats_.completed_requests;
        }

        switch (event.type) {
            case EventType::AckNew:
                ++stats_.accepted_new_orders;
                break;
            case EventType::AckCancel:
                ++stats_.accepted_cancels;
                break;
            case EventType::Fill:
                ++stats_.fills;
                break;
            case EventType::Reject:
                ++stats_.rejects;
                break;
            case EventType::StopEngine:
                ++stats_.stop_events;
                break;
        }
    }

    [[nodiscard]] const MemoryLogStats& stats() const noexcept {
        return stats_;
    }

  private:
    MemoryLogStats stats_{};
};

inline const char* side_name(Side side) noexcept {
    switch (side) {
        case Side::Bid:
            return "Bid";
        case Side::Ask:
            return "Ask";
    }
    return "UnknownSide";
}

inline const char* event_type_name(EventType type) noexcept {
    switch (type) {
        case EventType::AckNew:
            return "AckNew";
        case EventType::AckCancel:
            return "AckCancel";
        case EventType::Fill:
            return "Fill";
        case EventType::Reject:
            return "Reject";
        case EventType::StopEngine:
            return "StopEngine";
    }
    return "UnknownEvent";
}

inline const char* reject_reason_name(RejectReason reason) noexcept {
    switch (reason) {
        case RejectReason::None:
            return "None";
        case RejectReason::PriceOutOfBand:
            return "PriceOutOfBand";
        case RejectReason::ZeroQty:
            return "ZeroQty";
        case RejectReason::PoolExhausted:
            return "PoolExhausted";
        case RejectReason::UnknownHandle:
            return "UnknownHandle";
        case RejectReason::InsufficientLiquidity:
            return "InsufficientLiquidity";
        case RejectReason::ImmediateOrCancelRemainder:
            return "ImmediateOrCancelRemainder";
        case RejectReason::FillOrKillNotFilled:
            return "FillOrKillNotFilled";
        case RejectReason::SelfTrade:
            return "SelfTrade";
    }
    return "UnknownRejectReason";
}

// Writes one stable text line per replayable event. Timing fields are
// deliberately omitted, and the internal StopEngine event is drained but not
// written, so two runs over the same commands can compare log files directly.
class CanonicalEventLog {
  public:
    explicit CanonicalEventLog(std::FILE* file) noexcept : file_(file) {}

    bool operator()(const OutboundEvent& event) noexcept {
        return write(event);
    }

    bool write(const OutboundEvent& event) noexcept {
        if (event.type == EventType::StopEngine) {
            return true;
        }
        if (file_ == nullptr) {
            ok_ = false;
            return false;
        }

        const int complete = (event.flags & RequestComplete) != 0 ? 1 : 0;
        const int written = std::fprintf(
            file_,
            "seq=%llu event=%s side=%s price=%lld qty=%u handle=%llu reason=%s complete=%d\n",
            static_cast<unsigned long long>(event.client_seq), event_type_name(event.type),
            side_name(event.side), static_cast<long long>(event.price),
            static_cast<unsigned>(event.qty), static_cast<unsigned long long>(event.handle),
            reject_reason_name(event.reason), complete);

        if (written < 0) {
            ok_ = false;
            return false;
        }
        return true;
    }

    [[nodiscard]] bool ok() const noexcept {
        return ok_;
    }

  private:
    std::FILE* file_;
    bool ok_{true};
};

}  // namespace ob
