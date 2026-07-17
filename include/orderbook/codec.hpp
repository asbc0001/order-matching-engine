// codec.hpp - Convert engine commands and events to/from saved binary records.
//
// The in-memory structs are padded and carry timing fields. The codec writes
// only the logical fields at fixed offsets, so traces and logs do not depend on
// compiler layout or uninitialized padding bytes.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

#include "orderbook/types.hpp"

namespace ob::codec {

inline constexpr std::uint32_t MAGIC = 0x314B424Fu;  // "OBK1" in little-endian bytes.
inline constexpr std::uint16_t VERSION = 1;

inline constexpr std::size_t ENCODED_INBOUND_SIZE = 64;
inline constexpr std::size_t ENCODED_OUTBOUND_SIZE = 64;

enum class DecodeError : std::uint8_t {
    None,
    WrongLength,
    BadMagic,
    BadVersion,
    BadRecordKind,
    InvalidSide,
    InvalidMsgType,
    InvalidTimeInForce,
    InvalidEventType,
    InvalidRejectReason,
    InvalidFlags,
};

template <typename T>
struct DecodeResult {
    T value{};
    DecodeError error{DecodeError::None};

    explicit constexpr operator bool() const noexcept {
        return error == DecodeError::None;
    }
};

using EncodedInbound = std::array<std::uint8_t, ENCODED_INBOUND_SIZE>;
using EncodedOutbound = std::array<std::uint8_t, ENCODED_OUTBOUND_SIZE>;

EncodedInbound encode_inbound(const InboundMsg& msg) noexcept;
DecodeResult<InboundMsg> decode_inbound(std::span<const std::uint8_t> bytes) noexcept;

EncodedOutbound encode_outbound(const OutboundEvent& event) noexcept;
DecodeResult<OutboundEvent> decode_outbound(std::span<const std::uint8_t> bytes) noexcept;

const char* decode_error_name(DecodeError error) noexcept;

}  // namespace ob::codec
