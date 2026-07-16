// event_logger_test.cpp - Focused checks for outbound event sinks.

#include <array>
#include <cstdio>
#include <cstring>

#include "orderbook/event_logger.hpp"

namespace {

bool fail(const char* message) {
    std::fprintf(stderr, "%s\n", message);
    return false;
}

ob::OutboundEvent event(ob::EventType type, std::uint64_t seq) {
    // Timing fields are set on purpose. The canonical log test below proves
    // they do not leak into replayable text output.
    return ob::OutboundEvent{
        .client_seq = seq,
        .handle = 0x100000001ULL,
        .price = 101,
        .qty = 25,
        .side = ob::Side::Bid,
        .type = type,
        .reason =
            type == ob::EventType::Reject ? ob::RejectReason::ZeroQty : ob::RejectReason::None,
        .flags = static_cast<std::uint8_t>(type == ob::EventType::Fill ? 0 : ob::RequestComplete),
        .tsc_intended = 111,
        .tsc_egress = 222,
    };
}

bool check_memory_sink_counts_event_kinds() {
    ob::MemoryEventSink sink;
    sink(event(ob::EventType::AckNew, 1));
    sink(event(ob::EventType::Fill, 2));
    sink(event(ob::EventType::Reject, 3));
    sink(event(ob::EventType::StopEngine, 4));

    const ob::MemoryLogStats& stats = sink.stats();
    if (stats.total_events != 4 || stats.completed_requests != 3 ||
        stats.accepted_new_orders != 1 || stats.fills != 1 || stats.rejects != 1 ||
        stats.stop_events != 1) {
        return fail("Memory sink counts were wrong");
    }
    return true;
}

bool read_all(std::FILE* file, char* buffer, std::size_t capacity) {
    if (std::fflush(file) != 0 || std::fseek(file, 0, SEEK_SET) != 0) {
        return false;
    }
    const std::size_t used = std::fread(buffer, 1, capacity - 1, file);
    buffer[used] = '\0';
    return std::ferror(file) == 0;
}

bool check_canonical_log_is_stable_text() {
    std::FILE* file = std::tmpfile();
    if (file == nullptr) {
        return fail("tmpfile failed");
    }

    ob::CanonicalEventLog log{file};
    const bool wrote_ack = log(event(ob::EventType::AckNew, 10));
    // StopEngine tells internal threads to shut down. It should be consumed by
    // the logger but omitted from logs that are meant for replay comparison.
    const bool wrote_stop = log(event(ob::EventType::StopEngine, 11));

    char buffer[256]{};
    const bool read_ok = read_all(file, buffer, sizeof(buffer));
    std::fclose(file);

    constexpr char expected[] =
        "seq=10 event=AckNew side=Bid price=101 qty=25 handle=4294967297 reason=None "
        "complete=1\n";
    if (!wrote_ack || !wrote_stop || !log.ok() || !read_ok || std::strcmp(buffer, expected) != 0) {
        return fail("Canonical log text was wrong");
    }
    return true;
}

}  // namespace

int main() {
    using Check = bool (*)();
    constexpr std::array<Check, 2> checks{
        check_memory_sink_counts_event_kinds,
        check_canonical_log_is_stable_text,
    };

    for (Check check : checks) {
        if (!check()) {
            return 1;
        }
    }

    std::printf("event_logger_test OK\n");
    return 0;
}
