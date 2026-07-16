// text_trace_parser.hpp - Small parser for hand-written command traces.
//
// This parser is shared by tools that turn readable trace lines into
// InboundMsg records. It understands only the command grammar, not matching
// rules; semantic rejects such as zero quantity still belong to the matcher.

#pragma once

#include <cstdint>
#include <string_view>

#include "orderbook/types.hpp"

namespace ob::trace {

enum class ParseError : std::uint8_t {
    None,
    EmptyLine,
    UnknownCommand,
    BadSide,
    BadPrice,
    BadQty,
    BadHandle,
    WrongFieldCount,
};

struct ParseResult {
    InboundMsg value{};
    ParseError error{ParseError::None};

    explicit constexpr operator bool() const noexcept {
        return error == ParseError::None;
    }
};

ParseResult parse_line(std::uint64_t client_seq, std::string_view line) noexcept;

const char* parse_error_name(ParseError error) noexcept;

}  // namespace ob::trace
