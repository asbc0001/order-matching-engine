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
        limit.value.side != ob::Side::Bid || limit.value.price != 123 || limit.value.qty != 45 ||
        limit.value.time_in_force != ob::TimeInForce::GTC || limit.value.participant_id != 0) {
        return fail("LIMIT parse failed");
    }

    const auto ioc = ob::trace::parse_line(13, "LIMIT ASK 124 46 IOC PARTICIPANT=7");
    if (!ioc || ioc.value.type != ob::MsgType::NewLimit || ioc.value.side != ob::Side::Ask ||
        ioc.value.price != 124 || ioc.value.qty != 46 ||
        ioc.value.time_in_force != ob::TimeInForce::IOC || ioc.value.participant_id != 7) {
        return fail("LIMIT IOC participant parse failed");
    }

    const auto fok = ob::trace::parse_line(14, "LIMIT BID 125 47 PARTICIPANT=8 FOK");
    if (!fok || fok.value.type != ob::MsgType::NewLimit || fok.value.side != ob::Side::Bid ||
        fok.value.price != 125 || fok.value.qty != 47 ||
        fok.value.time_in_force != ob::TimeInForce::FOK || fok.value.participant_id != 8) {
        return fail("LIMIT FOK participant parse failed");
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
    if (!expect_error(bad_qty.error, ob::trace::ParseError::BadQty, "Bad quantity check failed")) {
        return false;
    }

    const auto bad_tif = ob::trace::parse_line(3, "LIMIT BID 100 5 DAY");
    if (!expect_error(bad_tif.error, ob::trace::ParseError::BadTimeInForce,
                      "Bad time-in-force check failed")) {
        return false;
    }

    const auto bad_participant = ob::trace::parse_line(4, "LIMIT BID 100 5 PARTICIPANT=abc");
    return expect_error(bad_participant.error, ob::trace::ParseError::BadParticipant,
                        "Bad participant check failed");
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
