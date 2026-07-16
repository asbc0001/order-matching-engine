#include "orderbook/text_trace_parser.hpp"

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>

namespace ob::trace {
namespace {

struct Tokens {
    std::string_view values[4]{};
    std::size_t count{0};
    bool too_many{false};
};

bool is_space(char ch) noexcept {
    return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n';
}

// The grammar has at most four fields, so a fixed token array is enough and
// keeps parsing allocation-free.
Tokens split_tokens(std::string_view line) noexcept {
    Tokens tokens;
    std::size_t pos = 0;

    while (pos < line.size()) {
        while (pos < line.size() && is_space(line[pos])) {
            ++pos;
        }
        if (pos == line.size()) {
            break;
        }

        const std::size_t begin = pos;
        while (pos < line.size() && !is_space(line[pos])) {
            ++pos;
        }

        if (tokens.count == std::size(tokens.values)) {
            tokens.too_many = true;
            break;
        }
        tokens.values[tokens.count++] = line.substr(begin, pos - begin);
    }

    return tokens;
}

bool parse_side(std::string_view token, Side& side) noexcept {
    if (token == "BID") {
        side = Side::Bid;
        return true;
    }
    if (token == "ASK") {
        side = Side::Ask;
        return true;
    }
    return false;
}

template <typename Int>
bool parse_int(std::string_view token, Int& value) noexcept {
    if (token.empty()) {
        return false;
    }

    // from_chars is locale-independent and reports partial parses. "12x" is
    // rejected instead of silently becoming 12.
    Int parsed{};
    const char* const begin = token.data();
    const char* const end = begin + token.size();
    const auto result = std::from_chars(begin, end, parsed, 10);
    if (result.ec != std::errc{} || result.ptr != end) {
        return false;
    }

    value = parsed;
    return true;
}

// Parse wide first so oversized quantities are rejected cleanly before the
// final narrowing cast to Qty.
bool parse_qty(std::string_view token, Qty& qty) noexcept {
    std::uint64_t parsed = 0;
    if (!parse_int(token, parsed) || parsed > std::numeric_limits<Qty>::max()) {
        return false;
    }

    qty = static_cast<Qty>(parsed);
    return true;
}

InboundMsg make_msg(std::uint64_t client_seq, MsgType type, Side side, Price price, Qty qty,
                    Handle handle = 0) noexcept {
    return InboundMsg{
        .client_seq = client_seq,
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

}  // namespace

// Validate command shape and tokens only. Trading rules such as zero-quantity
// rejects still belong to the matcher.
ParseResult parse_line(std::uint64_t client_seq, std::string_view line) noexcept {
    const Tokens tokens = split_tokens(line);
    if (tokens.count == 0) {
        return {.error = ParseError::EmptyLine};
    }
    if (tokens.too_many) {
        return {.error = ParseError::WrongFieldCount};
    }

    if (tokens.values[0] == "LIMIT") {
        if (tokens.count != 4) {
            return {.error = ParseError::WrongFieldCount};
        }

        Side side{};
        Price price = 0;
        Qty qty = 0;
        if (!parse_side(tokens.values[1], side)) {
            return {.error = ParseError::BadSide};
        }
        if (!parse_int(tokens.values[2], price)) {
            return {.error = ParseError::BadPrice};
        }
        if (!parse_qty(tokens.values[3], qty)) {
            return {.error = ParseError::BadQty};
        }

        return {.value = make_msg(client_seq, MsgType::NewLimit, side, price, qty),
                .error = ParseError::None};
    }

    if (tokens.values[0] == "MARKET") {
        if (tokens.count != 3) {
            return {.error = ParseError::WrongFieldCount};
        }

        Side side{};
        Qty qty = 0;
        if (!parse_side(tokens.values[1], side)) {
            return {.error = ParseError::BadSide};
        }
        if (!parse_qty(tokens.values[2], qty)) {
            return {.error = ParseError::BadQty};
        }

        return {.value = make_msg(client_seq, MsgType::NewMarket, side, 0, qty),
                .error = ParseError::None};
    }

    if (tokens.values[0] == "CANCEL") {
        if (tokens.count != 2) {
            return {.error = ParseError::WrongFieldCount};
        }

        Handle handle = 0;
        if (!parse_int(tokens.values[1], handle)) {
            return {.error = ParseError::BadHandle};
        }

        return {.value = make_msg(client_seq, MsgType::Cancel, Side::Bid, 0, 0, handle),
                .error = ParseError::None};
    }

    return {.error = ParseError::UnknownCommand};
}

const char* parse_error_name(ParseError error) noexcept {
    switch (error) {
        case ParseError::None:
            return "None";
        case ParseError::EmptyLine:
            return "EmptyLine";
        case ParseError::UnknownCommand:
            return "UnknownCommand";
        case ParseError::BadSide:
            return "BadSide";
        case ParseError::BadPrice:
            return "BadPrice";
        case ParseError::BadQty:
            return "BadQty";
        case ParseError::BadHandle:
            return "BadHandle";
        case ParseError::WrongFieldCount:
            return "WrongFieldCount";
    }
    return "Unknown";
}

}  // namespace ob::trace
