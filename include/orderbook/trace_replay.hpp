// trace_replay.hpp - Replay saved command streams into an SPSC ring.
//
// A "trace" is a file containing the same inbound commands every time it is
// replayed. The producer side uses traces for demos, regression checks, and
// repeatable benchmarks. This file is the synchronous core used by future
// producer threads: validate the trace header, decode one fixed-size inbound
// record at a time, and push only valid messages into the inbound ring.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <thread>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

#include "orderbook/codec.hpp"
#include "orderbook/spsc_ring.hpp"
#include "orderbook/types.hpp"

namespace ob::trace {

// File header layout:
//   bytes 0..3: magic   - fixed signature that identifies this as an orderbook trace
//   bytes 4..5: version - format version, so incompatible files fail early
//   byte  6:    kind    - what records the file contains; currently inbound commands
//   byte  7:    reserved, always zero
//

inline constexpr std::uint32_t TRACE_MAGIC = 0x5254424Fu;  // "OBTR" in little-endian bytes.
inline constexpr std::uint16_t TRACE_VERSION = 1;
inline constexpr std::uint8_t TRACE_KIND_INBOUND = 1;
inline constexpr std::size_t TRACE_HEADER_SIZE = 8;

using TraceHeader = std::array<std::uint8_t, TRACE_HEADER_SIZE>;

enum class ReplayWaitMode : std::uint8_t {
    Spin,
    Yield,
};

enum class ReplayError : std::uint8_t {
    None,
    OpenFailed,
    TruncatedHeader,
    BadHeaderMagic,
    BadHeaderVersion,
    BadHeaderKind,
    BadRecord,
    TruncatedRecord,
    ReadFailed,
};

struct TraceReplayResult {
    ReplayError error{ReplayError::None};
    codec::DecodeError decode_error{codec::DecodeError::None};
    std::uint64_t records_replayed{0};
    std::uint64_t inbound_full_waits{0};
    // On record-level failures, these point to the bad zero-based record and
    // its starting byte in the file. Header failures use byte_offset 0.
    std::uint64_t record_index{0};
    std::uint64_t byte_offset{0};
};

namespace detail {

inline void trace_replay_wait(ReplayWaitMode mode) noexcept {
    if (mode == ReplayWaitMode::Yield) {
        std::this_thread::yield();
        return;
    }

#if defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#else
    std::this_thread::yield();
#endif
}

inline void write_u16(TraceHeader& header, std::size_t offset, std::uint16_t value) noexcept {
    header[offset] = static_cast<std::uint8_t>(value & 0xFFu);
    header[offset + 1] = static_cast<std::uint8_t>((value >> 8) & 0xFFu);
}

inline void write_u32(TraceHeader& header, std::size_t offset, std::uint32_t value) noexcept {
    for (std::size_t i = 0; i < 4; ++i) {
        header[offset + i] = static_cast<std::uint8_t>((value >> (8 * i)) & 0xFFu);
    }
}

inline std::uint16_t read_u16(const TraceHeader& header, std::size_t offset) noexcept {
    return static_cast<std::uint16_t>(header[offset]) |
           static_cast<std::uint16_t>(static_cast<std::uint16_t>(header[offset + 1]) << 8);
}

inline std::uint32_t read_u32(const TraceHeader& header, std::size_t offset) noexcept {
    std::uint32_t value = 0;
    for (std::size_t i = 0; i < 4; ++i) {
        value |= static_cast<std::uint32_t>(header[offset + i]) << (8 * i);
    }
    return value;
}

}  // namespace detail

inline TraceHeader make_inbound_trace_header() noexcept {
    TraceHeader header{};
    detail::write_u32(header, 0, TRACE_MAGIC);
    detail::write_u16(header, 4, TRACE_VERSION);
    header[6] = TRACE_KIND_INBOUND;
    header[7] = 0;
    return header;
}

// Reject the whole file before reading records if this is not the expected
// format. A bad header means none of the payload bytes are trustworthy.
inline ReplayError validate_inbound_trace_header(const TraceHeader& header) noexcept {
    if (detail::read_u32(header, 0) != TRACE_MAGIC) {
        return ReplayError::BadHeaderMagic;
    }
    if (detail::read_u16(header, 4) != TRACE_VERSION) {
        return ReplayError::BadHeaderVersion;
    }
    if (header[6] != TRACE_KIND_INBOUND) {
        return ReplayError::BadHeaderKind;
    }
    return ReplayError::None;
}

inline const char* replay_error_name(ReplayError error) noexcept {
    switch (error) {
        case ReplayError::None:
            return "None";
        case ReplayError::OpenFailed:
            return "OpenFailed";
        case ReplayError::TruncatedHeader:
            return "TruncatedHeader";
        case ReplayError::BadHeaderMagic:
            return "BadHeaderMagic";
        case ReplayError::BadHeaderVersion:
            return "BadHeaderVersion";
        case ReplayError::BadHeaderKind:
            return "BadHeaderKind";
        case ReplayError::BadRecord:
            return "BadRecord";
        case ReplayError::TruncatedRecord:
            return "TruncatedRecord";
        case ReplayError::ReadFailed:
            return "ReadFailed";
    }
    return "Unknown";
}

namespace detail {

inline InboundMsg make_stop_engine_msg(std::uint64_t client_seq) noexcept {
    return InboundMsg{
        .client_seq = client_seq,
        .handle = 0,
        .price = 0,
        .qty = 0,
        .side = Side::Bid,
        .type = MsgType::StopEngine,
        .tsc_intended = 0,
        .tsc_ready = 0,
        .tsc_enqueue = 0,
    };
}

// A full inbound ring is normal backpressure, not a replay error. The
// producer keeps ownership of the message and retries until it is accepted.
template <std::size_t RingCapacity>
void push_replayed_msg(SpscRing<InboundMsg, RingCapacity>& inbound, const InboundMsg& msg,
                       TraceReplayResult& result, ReplayWaitMode wait_mode) noexcept {
    while (!inbound.push(msg)) {
        ++result.inbound_full_waits;
        trace_replay_wait(wait_mode);
    }
}

}  // namespace detail

template <std::size_t RingCapacity>
TraceReplayResult replay_trace_file(const char* path, SpscRing<InboundMsg, RingCapacity>& inbound,
                                    ReplayWaitMode wait_mode = ReplayWaitMode::Spin) noexcept {
    TraceReplayResult result;

    std::FILE* file = std::fopen(path, "rb");
    if (file == nullptr) {
        result.error = ReplayError::OpenFailed;
        return result;
    }

    TraceHeader header{};
    const std::size_t header_read = std::fread(header.data(), 1, header.size(), file);
    if (header_read != header.size()) {
        // Clean EOF is only valid after the full header and zero or more full
        // records. A short header is a malformed trace file.
        result.error =
            std::ferror(file) != 0 ? ReplayError::ReadFailed : ReplayError::TruncatedHeader;
        std::fclose(file);
        return result;
    }

    result.error = validate_inbound_trace_header(header);
    if (result.error != ReplayError::None) {
        result.byte_offset = 0;
        std::fclose(file);
        return result;
    }

    codec::EncodedInbound bytes{};
    for (;;) {
        const std::uint64_t record_index = result.records_replayed;
        const std::uint64_t byte_offset =
            TRACE_HEADER_SIZE + record_index * codec::ENCODED_INBOUND_SIZE;
        const std::size_t read = std::fread(bytes.data(), 1, bytes.size(), file);
        if (read == 0) {
            if (std::ferror(file) != 0) {
                result.error = ReplayError::ReadFailed;
                result.record_index = record_index;
                result.byte_offset = byte_offset;
                std::fclose(file);
                return result;
            }
            break;
        }
        if (read != bytes.size()) {
            // Do not treat a partial final record as normal EOF. Replaying it
            // would mean silently changing the command stream.
            result.error = ReplayError::TruncatedRecord;
            result.record_index = record_index;
            result.byte_offset = byte_offset;
            std::fclose(file);
            return result;
        }

        const auto decoded = codec::decode_inbound(bytes);
        if (!decoded) {
            // Abort on the first bad record. Skipping malformed commands would
            // make deterministic replay lie about what was actually in the file.
            result.error = ReplayError::BadRecord;
            result.decode_error = decoded.error;
            result.record_index = record_index;
            result.byte_offset = byte_offset;
            std::fclose(file);
            return result;
        }

        detail::push_replayed_msg(inbound, decoded.value, result, wait_mode);
        ++result.records_replayed;
    }

    // StopEngine is internal control flow, not something stored in trace files.
    // A clean EOF is translated into the shutdown command for the matching loop.
    detail::push_replayed_msg(inbound, detail::make_stop_engine_msg(result.records_replayed + 1),
                              result, wait_mode);
    std::fclose(file);
    return result;
}

}  // namespace ob::trace
