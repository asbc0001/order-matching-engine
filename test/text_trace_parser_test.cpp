// text_trace_parser_test.cpp - Checks for readable trace command parsing.

#include <array>
#include <cstdio>

#include "orderbook/text_trace_parser.hpp"

namespace {

bool fail(const char* message) {
    std::fprintf(stderr, "%s\n", message);
    return false;
}

bool expect_error(ob::trace::ParseError actual, ob::trace::ParseError expected,
                  const char* message) {
    if (actual != expected) {
        std::fprintf(stderr, "%s: got %s, expected %s\n", message,
                     ob::trace::parse_error_name(actual), ob::trace::parse_error_name(expected));
        return false;
    }
    return true;
}

bool check_valid_lines() {
    const auto limit = ob::trace::parse_line(10, "LIMIT BID 123 45");
    if (!limit || limit.value.client_seq != 10 || limit.value.type != ob::MsgType::NewLimit ||
        limit.value.side != ob::Side::Bid || limit.value.price != 123 || limit.value.qty != 45) {
        return fail("LIMIT parse failed");
    }

    const auto market = ob::trace::parse_line(11, "\tMARKET ASK 7  ");
    if (!market || market.value.type != ob::MsgType::NewMarket ||
        market.value.side != ob::Side::Ask || market.value.price != 0 || market.value.qty != 7) {
        return fail("MARKET parse failed");
    }

    const auto cancel = ob::trace::parse_line(12, "CANCEL 987654321");
    return cancel && cancel.value.type == ob::MsgType::Cancel && cancel.value.client_seq == 12 &&
                   cancel.value.handle == 987654321 && cancel.value.qty == 0
               ? true
               : fail("CANCEL parse failed");
}

bool check_malformed_line() {
    const auto missing_qty = ob::trace::parse_line(1, "LIMIT BID 100");
    if (!expect_error(missing_qty.error, ob::trace::ParseError::WrongFieldCount,
                      "Missing field check failed")) {
        return false;
    }

    const auto bad_qty = ob::trace::parse_line(2, "MARKET ASK lots");
    return expect_error(bad_qty.error, ob::trace::ParseError::BadQty, "Bad quantity check failed");
}

bool check_unknown_command() {
    const auto parsed = ob::trace::parse_line(1, "AMEND 12 99");
    return expect_error(parsed.error, ob::trace::ParseError::UnknownCommand,
                        "Unknown command check failed");
}

bool check_bad_side_token() {
    const auto parsed = ob::trace::parse_line(1, "LIMIT BUY 100 5");
    return expect_error(parsed.error, ob::trace::ParseError::BadSide, "Bad side check failed");
}

}  // namespace

int main() {
    using Check = bool (*)();
    constexpr std::array<Check, 4> checks{
        check_valid_lines,
        check_malformed_line,
        check_unknown_command,
        check_bad_side_token,
    };

    for (Check check : checks) {
        if (!check()) {
            return 1;
        }
    }

    std::printf("text_trace_parser_test OK\n");
    return 0;
}
