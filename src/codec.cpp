#include "orderbook/codec.hpp"

#include <cstddef>
#include <cstdint>

namespace ob::codec {
namespace {

enum class RecordKind : std::uint8_t {
    Inbound = 1,
    Outbound = 2,
};

// Both record types use the same 8-byte header and the same logical field
// positions where possible. Bytes not named here stay zero because the encoded
// arrays are value-initialized.
namespace offset {
constexpr std::size_t MAGIC = 0;
constexpr std::size_t VERSION = 4;
constexpr std::size_t RECORD_KIND = 6;
constexpr std::size_t HEADER_RESERVED = 7;

constexpr std::size_t CLIENT_SEQ = 8;
constexpr std::size_t HANDLE = 16;
constexpr std::size_t PRICE = 24;
constexpr std::size_t QTY = 32;
constexpr std::size_t SIDE = 36;

constexpr std::size_t INBOUND_TYPE = 37;

constexpr std::size_t OUTBOUND_TYPE = 37;
constexpr std::size_t OUTBOUND_REASON = 38;
constexpr std::size_t OUTBOUND_FLAGS = 39;
}  // namespace offset

// Write integers one byte at a time so the file format is always little-endian,
// even if the host CPU is not.
void write_u16(std::span<std::uint8_t> bytes, std::size_t offset, std::uint16_t value) noexcept {
    bytes[offset] = static_cast<std::uint8_t>(value & 0xFFu);
    bytes[offset + 1] = static_cast<std::uint8_t>((value >> 8) & 0xFFu);
}

void write_u32(std::span<std::uint8_t> bytes, std::size_t offset, std::uint32_t value) noexcept {
    for (std::size_t i = 0; i < 4; ++i) {
        bytes[offset + i] = static_cast<std::uint8_t>((value >> (8 * i)) & 0xFFu);
    }
}

void write_u64(std::span<std::uint8_t> bytes, std::size_t offset, std::uint64_t value) noexcept {
    for (std::size_t i = 0; i < 8; ++i) {
        bytes[offset + i] = static_cast<std::uint8_t>((value >> (8 * i)) & 0xFFu);
    }
}

std::uint16_t read_u16(std::span<const std::uint8_t> bytes, std::size_t offset) noexcept {
    return static_cast<std::uint16_t>(bytes[offset]) |
           static_cast<std::uint16_t>(static_cast<std::uint16_t>(bytes[offset + 1]) << 8);
}

std::uint32_t read_u32(std::span<const std::uint8_t> bytes, std::size_t offset) noexcept {
    std::uint32_t value = 0;
    for (std::size_t i = 0; i < 4; ++i) {
        value |= static_cast<std::uint32_t>(bytes[offset + i]) << (8 * i);
    }
    return value;
}

std::uint64_t read_u64(std::span<const std::uint8_t> bytes, std::size_t offset) noexcept {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        value |= static_cast<std::uint64_t>(bytes[offset + i]) << (8 * i);
    }
    return value;
}

// The header lets a reader reject the wrong file, an old/new incompatible
// version, or an event where it expected a command.
void write_header(std::span<std::uint8_t> bytes, RecordKind kind) noexcept {
    write_u32(bytes, offset::MAGIC, MAGIC);
    write_u16(bytes, offset::VERSION, VERSION);
    bytes[offset::RECORD_KIND] = static_cast<std::uint8_t>(kind);
    bytes[offset::HEADER_RESERVED] = 0;
}

DecodeError check_header(std::span<const std::uint8_t> bytes, std::size_t expected_size,
                         RecordKind expected_kind) noexcept {
    if (bytes.size() != expected_size) {
        return DecodeError::WrongLength;
    }
    if (read_u32(bytes, offset::MAGIC) != MAGIC) {
        return DecodeError::BadMagic;
    }
    if (read_u16(bytes, offset::VERSION) != VERSION) {
        return DecodeError::BadVersion;
    }
    if (bytes[offset::RECORD_KIND] != static_cast<std::uint8_t>(expected_kind)) {
        return DecodeError::BadRecordKind;
    }
    return DecodeError::None;
}

bool valid_side(std::uint8_t value) noexcept {
    return value == static_cast<std::uint8_t>(Side::Bid) ||
           value == static_cast<std::uint8_t>(Side::Ask);
}

// StopEngine is created inside the process during shutdown. It is never allowed
// to arrive from a trace file or socket.
bool valid_external_msg_type(std::uint8_t value) noexcept {
    return value == static_cast<std::uint8_t>(MsgType::NewLimit) ||
           value == static_cast<std::uint8_t>(MsgType::NewMarket) ||
           value == static_cast<std::uint8_t>(MsgType::Cancel);
}

// StopEngine events are useful between engine threads, but replayable logs only
// contain trading events.
bool valid_external_event_type(std::uint8_t value) noexcept {
    return value == static_cast<std::uint8_t>(EventType::AckNew) ||
           value == static_cast<std::uint8_t>(EventType::AckCancel) ||
           value == static_cast<std::uint8_t>(EventType::Fill) ||
           value == static_cast<std::uint8_t>(EventType::Reject);
}

bool valid_reject_reason(std::uint8_t value) noexcept {
    return value <= static_cast<std::uint8_t>(RejectReason::InsufficientLiquidity);
}

}  // namespace

EncodedInbound encode_inbound(const InboundMsg& msg) noexcept {
    EncodedInbound bytes{};
    write_header(bytes, RecordKind::Inbound);
    // Timing fields are intentionally skipped; they are measurements of one
    // run, not part of the command being replayed.
    write_u64(bytes, offset::CLIENT_SEQ, msg.client_seq);
    write_u64(bytes, offset::HANDLE, msg.handle);
    write_u64(bytes, offset::PRICE, static_cast<std::uint64_t>(msg.price));
    write_u32(bytes, offset::QTY, msg.qty);
    bytes[offset::SIDE] = static_cast<std::uint8_t>(msg.side);
    bytes[offset::INBOUND_TYPE] = static_cast<std::uint8_t>(msg.type);
    return bytes;
}

DecodeResult<InboundMsg> decode_inbound(std::span<const std::uint8_t> bytes) noexcept {
    const DecodeError header_error = check_header(bytes, ENCODED_INBOUND_SIZE, RecordKind::Inbound);
    if (header_error != DecodeError::None) {
        return {.error = header_error};
    }
    if (!valid_side(bytes[offset::SIDE])) {
        return {.error = DecodeError::InvalidSide};
    }
    if (!valid_external_msg_type(bytes[offset::INBOUND_TYPE])) {
        return {.error = DecodeError::InvalidMsgType};
    }

    // Decoded timing fields start empty and are filled by the producer/engine
    // when this command is run again.
    return {
        .value =
            InboundMsg{
                .client_seq = read_u64(bytes, offset::CLIENT_SEQ),
                .handle = read_u64(bytes, offset::HANDLE),
                .price = static_cast<Price>(read_u64(bytes, offset::PRICE)),
                .qty = read_u32(bytes, offset::QTY),
                .side = static_cast<Side>(bytes[offset::SIDE]),
                .type = static_cast<MsgType>(bytes[offset::INBOUND_TYPE]),
                .tsc_intended = 0,
                .tsc_ready = 0,
                .tsc_enqueue = 0,
            },
        .error = DecodeError::None,
    };
}

EncodedOutbound encode_outbound(const OutboundEvent& event) noexcept {
    EncodedOutbound bytes{};
    write_header(bytes, RecordKind::Outbound);
    // The canonical event bytes describe what happened, not when this
    // particular run emitted it.
    write_u64(bytes, offset::CLIENT_SEQ, event.client_seq);
    write_u64(bytes, offset::HANDLE, event.handle);
    write_u64(bytes, offset::PRICE, static_cast<std::uint64_t>(event.price));
    write_u32(bytes, offset::QTY, event.qty);
    bytes[offset::SIDE] = static_cast<std::uint8_t>(event.side);
    bytes[offset::OUTBOUND_TYPE] = static_cast<std::uint8_t>(event.type);
    bytes[offset::OUTBOUND_REASON] = static_cast<std::uint8_t>(event.reason);
    bytes[offset::OUTBOUND_FLAGS] = event.flags;
    return bytes;
}

DecodeResult<OutboundEvent> decode_outbound(std::span<const std::uint8_t> bytes) noexcept {
    const DecodeError header_error =
        check_header(bytes, ENCODED_OUTBOUND_SIZE, RecordKind::Outbound);
    if (header_error != DecodeError::None) {
        return {.error = header_error};
    }
    if (!valid_side(bytes[offset::SIDE])) {
        return {.error = DecodeError::InvalidSide};
    }
    if (!valid_external_event_type(bytes[offset::OUTBOUND_TYPE])) {
        return {.error = DecodeError::InvalidEventType};
    }
    if (!valid_reject_reason(bytes[offset::OUTBOUND_REASON])) {
        return {.error = DecodeError::InvalidRejectReason};
    }
    if ((bytes[offset::OUTBOUND_FLAGS] & ~static_cast<std::uint8_t>(RequestComplete)) != 0) {
        return {.error = DecodeError::InvalidFlags};
    }

    // tsc_* is deliberately zero on decode for the same reason it is not
    // encoded: canonical logs should be repeatable across runs.
    return {
        .value =
            OutboundEvent{
                .client_seq = read_u64(bytes, offset::CLIENT_SEQ),
                .handle = read_u64(bytes, offset::HANDLE),
                .price = static_cast<Price>(read_u64(bytes, offset::PRICE)),
                .qty = read_u32(bytes, offset::QTY),
                .side = static_cast<Side>(bytes[offset::SIDE]),
                .type = static_cast<EventType>(bytes[offset::OUTBOUND_TYPE]),
                .reason = static_cast<RejectReason>(bytes[offset::OUTBOUND_REASON]),
                .flags = bytes[offset::OUTBOUND_FLAGS],
                .tsc_intended = 0,
                .tsc_egress = 0,
            },
        .error = DecodeError::None,
    };
}

const char* decode_error_name(DecodeError error) noexcept {
    switch (error) {
        case DecodeError::None:
            return "None";
        case DecodeError::WrongLength:
            return "WrongLength";
        case DecodeError::BadMagic:
            return "BadMagic";
        case DecodeError::BadVersion:
            return "BadVersion";
        case DecodeError::BadRecordKind:
            return "BadRecordKind";
        case DecodeError::InvalidSide:
            return "InvalidSide";
        case DecodeError::InvalidMsgType:
            return "InvalidMsgType";
        case DecodeError::InvalidEventType:
            return "InvalidEventType";
        case DecodeError::InvalidRejectReason:
            return "InvalidRejectReason";
        case DecodeError::InvalidFlags:
            return "InvalidFlags";
    }
    return "Unknown";
}

}  // namespace ob::codec
