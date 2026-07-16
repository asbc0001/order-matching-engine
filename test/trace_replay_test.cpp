// trace_replay_test.cpp - Encoded trace replay boundary checks.

#include <unistd.h>
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <initializer_list>
#include <string>

#include "orderbook/codec.hpp"
#include "orderbook/spsc_ring.hpp"
#include "orderbook/trace_replay.hpp"

namespace {

using InboundRing = ob::SpscRing<ob::InboundMsg, 8>;

bool fail(const char* message) {
    std::fprintf(stderr, "%s\n", message);
    return false;
}

bool expect_error(ob::trace::ReplayError actual, ob::trace::ReplayError expected,
                  const char* message) {
    if (actual != expected) {
        std::fprintf(stderr, "%s: got %s, expected %s\n", message,
                     ob::trace::replay_error_name(actual), ob::trace::replay_error_name(expected));
        return false;
    }
    return true;
}

// Keep tests independent without needing a checked-in trace fixture.
std::string temp_path(const char* name) {
    char path[256]{};
    std::snprintf(path, sizeof(path), "/tmp/orderbook_%s_%ld.trace", name,
                  static_cast<long>(::getpid()));
    return path;
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

// Write exact bytes for malformed-file tests, where write_trace would be too
// well-formed by construction.
bool write_bytes(const std::string& path, const std::uint8_t* data, std::size_t size) {
    std::FILE* file = std::fopen(path.c_str(), "wb");
    if (file == nullptr) {
        return fail("Failed to create temp trace");
    }
    const bool ok = std::fwrite(data, 1, size, file) == size;
    std::fclose(file);
    return ok ? true : fail("Failed to write temp trace");
}

// A normal trace file is one trace header followed by zero or more encoded
// inbound command records.
bool write_trace(const std::string& path, const ob::trace::TraceHeader& header,
                 std::initializer_list<ob::codec::EncodedInbound> records) {
    std::FILE* file = std::fopen(path.c_str(), "wb");
    if (file == nullptr) {
        return fail("Failed to create temp trace");
    }

    bool ok = std::fwrite(header.data(), 1, header.size(), file) == header.size();
    for (const auto& record : records) {
        ok = ok && std::fwrite(record.data(), 1, record.size(), file) == record.size();
    }
    std::fclose(file);
    return ok ? true : fail("Failed to write trace records");
}

ob::trace::TraceReplayResult replay_temp_trace(const std::string& path, InboundRing& inbound) {
    const auto result = ob::trace::replay_trace_file(path.c_str(), inbound);
    std::remove(path.c_str());
    return result;
}

bool check_success_replays_messages_and_stop() {
    const std::string path = temp_path("replay_success");
    const ob::InboundMsg first = msg(10, ob::MsgType::NewLimit, ob::Side::Bid, 101, 5);
    const ob::InboundMsg second = msg(11, ob::MsgType::Cancel, ob::Side::Bid, 0, 0, 1234);
    if (!write_trace(path, ob::trace::make_inbound_trace_header(),
                     {ob::codec::encode_inbound(first), ob::codec::encode_inbound(second)})) {
        return false;
    }

    InboundRing inbound;
    // Replay reads the saved command records and appends an internal shutdown
    // command when it reaches a clean end-of-file.
    const auto result =
        ob::trace::replay_trace_file(path.c_str(), inbound, ob::trace::ReplayWaitMode::Yield);
    std::remove(path.c_str());

    if (result.error != ob::trace::ReplayError::None || result.records_replayed != 2) {
        return fail("Replay success result mismatch");
    }

    ob::InboundMsg out{};
    if (!inbound.pop(out) || out.client_seq != 10 || out.type != ob::MsgType::NewLimit ||
        out.price != 101 || out.qty != 5) {
        return fail("First replayed message mismatch");
    }
    if (!inbound.pop(out) || out.client_seq != 11 || out.type != ob::MsgType::Cancel ||
        out.handle != 1234) {
        return fail("Second replayed message mismatch");
    }
    return inbound.pop(out) && out.type == ob::MsgType::StopEngine && out.client_seq == 3
               ? true
               : fail("StopEngine message missing after clean EOF");
}

bool check_bad_headers() {
    struct Case {
        const char* name;
        std::size_t corrupt_offset;
        std::uint8_t corrupt_value;
        ob::trace::ReplayError expected;
    };

    constexpr std::array<Case, 3> cases{{
        {.name = "bad_magic",
         .corrupt_offset = 0,
         .corrupt_value = 0,
         .expected = ob::trace::ReplayError::BadHeaderMagic},
        {.name = "bad_version",
         .corrupt_offset = 4,
         .corrupt_value = 2,
         .expected = ob::trace::ReplayError::BadHeaderVersion},
        {.name = "bad_kind",
         .corrupt_offset = 6,
         .corrupt_value = 99,
         .expected = ob::trace::ReplayError::BadHeaderKind},
    }};

    for (const Case& test_case : cases) {
        const std::string path = temp_path(test_case.name);
        auto header = ob::trace::make_inbound_trace_header();
        header[test_case.corrupt_offset] = test_case.corrupt_value;
        if (!write_trace(path, header, {})) {
            return false;
        }

        InboundRing inbound;
        const auto result = replay_temp_trace(path, inbound);
        if (!expect_error(result.error, test_case.expected, "Bad header check failed") ||
            result.byte_offset != 0) {
            return false;
        }
    }
    return true;
}

bool check_bad_record_reports_location() {
    const std::string path = temp_path("bad_record");
    auto bad_record =
        ob::codec::encode_inbound(msg(1, ob::MsgType::NewLimit, ob::Side::Bid, 100, 1));
    // StopEngine is legal inside the engine, but not as a saved external trace
    // command. The codec should reject it during replay.
    bad_record[37] = static_cast<std::uint8_t>(ob::MsgType::StopEngine);
    if (!write_trace(path, ob::trace::make_inbound_trace_header(), {bad_record})) {
        return false;
    }

    InboundRing inbound;
    const auto result = replay_temp_trace(path, inbound);
    return expect_error(result.error, ob::trace::ReplayError::BadRecord,
                        "Bad record check failed") &&
           result.decode_error == ob::codec::DecodeError::InvalidMsgType &&
           result.record_index == 0 && result.byte_offset == ob::trace::TRACE_HEADER_SIZE;
}

bool check_truncated_record_reports_location() {
    const std::string path = temp_path("truncated_record");
    std::array<std::uint8_t, ob::trace::TRACE_HEADER_SIZE + 3> bytes{};
    const auto header = ob::trace::make_inbound_trace_header();
    std::copy(header.begin(), header.end(), bytes.begin());
    if (!write_bytes(path, bytes.data(), bytes.size())) {
        return false;
    }

    InboundRing inbound;
    const auto result = replay_temp_trace(path, inbound);
    return expect_error(result.error, ob::trace::ReplayError::TruncatedRecord,
                        "Truncated record check failed") &&
           result.record_index == 0 && result.byte_offset == ob::trace::TRACE_HEADER_SIZE;
}

}  // namespace

int main() {
    using Check = bool (*)();
    constexpr std::array<Check, 4> checks{
        check_success_replays_messages_and_stop,
        check_bad_headers,
        check_bad_record_reports_location,
        check_truncated_record_reports_location,
    };

    for (Check check : checks) {
        if (!check()) {
            return 1;
        }
    }

    std::printf("trace_replay_test OK\n");
    return 0;
}
