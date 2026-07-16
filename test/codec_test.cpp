// codec_test.cpp - Boundary checks for the stable file/wire byte format.

#include <array>
#include <cstdint>
#include <cstdio>

#include "orderbook/codec.hpp"
#include "orderbook/types.hpp"

namespace {

bool fail(const char* message) {
    std::fprintf(stderr, "%s\n", message);
    return false;
}

bool expect_error(ob::codec::DecodeError actual, ob::codec::DecodeError expected,
                  const char* message) {
    if (actual != expected) {
        std::fprintf(stderr, "%s: got %s, expected %s\n", message,
                     ob::codec::decode_error_name(actual), ob::codec::decode_error_name(expected));
        return false;
    }
    return true;
}

ob::InboundMsg inbound_sample() {
    // Non-round numbers make byte-order mistakes obvious in the offset checks.
    return ob::InboundMsg{
        .client_seq = 0x0102030405060708ULL,
        .handle = 0x1112131415161718ULL,
        .price = -12345,
        .qty = 42,
        .side = ob::Side::Ask,
        .type = ob::MsgType::NewLimit,
        .tsc_intended = 100,
        .tsc_ready = 200,
        .tsc_enqueue = 300,
    };
}

ob::OutboundEvent outbound_sample() {
    return ob::OutboundEvent{
        .client_seq = 99,
        .handle = 0x2122232425262728ULL,
        .price = 54321,
        .qty = 17,
        .side = ob::Side::Bid,
        .type = ob::EventType::Fill,
        .reason = ob::RejectReason::None,
        .flags = ob::RequestComplete,
        .tsc_intended = 400,
        .tsc_egress = 500,
    };
}

bool check_inbound_round_trip_and_offsets() {
    const ob::InboundMsg msg = inbound_sample();
    const auto bytes = ob::codec::encode_inbound(msg);

    // Fixed little-endian offsets make traces independent of host byte order
    // and C++ struct padding.
    if (bytes[8] != 0x08 || bytes[9] != 0x07 || bytes[10] != 0x06 || bytes[11] != 0x05 ||
        bytes[36] != static_cast<std::uint8_t>(ob::Side::Ask) ||
        bytes[37] != static_cast<std::uint8_t>(ob::MsgType::NewLimit)) {
        return fail("Inbound fixed offsets failed");
    }

    const auto decoded = ob::codec::decode_inbound(bytes);
    if (!decoded) {
        return fail("Inbound decode rejected encoded message");
    }

    const ob::InboundMsg& out = decoded.value;
    if (out.client_seq != msg.client_seq || out.handle != msg.handle || out.price != msg.price ||
        out.qty != msg.qty || out.side != msg.side || out.type != msg.type) {
        return fail("Inbound round trip changed logical fields");
    }
    if (out.tsc_intended != 0 || out.tsc_ready != 0 || out.tsc_enqueue != 0) {
        return fail("Inbound codec preserved internal timing fields");
    }
    return true;
}

bool check_outbound_round_trip() {
    const ob::OutboundEvent event = outbound_sample();
    const auto bytes = ob::codec::encode_outbound(event);
    const auto decoded = ob::codec::decode_outbound(bytes);
    if (!decoded) {
        return fail("Outbound decode rejected encoded event");
    }

    const ob::OutboundEvent& out = decoded.value;
    if (out.client_seq != event.client_seq || out.handle != event.handle ||
        out.price != event.price || out.qty != event.qty || out.side != event.side ||
        out.type != event.type || out.reason != event.reason || out.flags != event.flags) {
        return fail("Outbound round trip changed logical fields");
    }
    if (out.tsc_intended != 0 || out.tsc_egress != 0) {
        return fail("Outbound codec preserved internal timing fields");
    }
    return true;
}

bool check_inbound_validation() {
    auto bytes = ob::codec::encode_inbound(inbound_sample());

    // Decode is the trust boundary: malformed bytes must fail before they can
    // become commands in the inbound ring.
    if (!expect_error(ob::codec::decode_inbound({bytes.data(), bytes.size() - 1}).error,
                      ob::codec::DecodeError::WrongLength, "Inbound length check failed")) {
        return false;
    }

    bytes = ob::codec::encode_inbound(inbound_sample());
    bytes[0] = 0;
    if (!expect_error(ob::codec::decode_inbound(bytes).error, ob::codec::DecodeError::BadMagic,
                      "Inbound magic check failed")) {
        return false;
    }

    bytes = ob::codec::encode_inbound(inbound_sample());
    bytes[4] = 2;
    if (!expect_error(ob::codec::decode_inbound(bytes).error, ob::codec::DecodeError::BadVersion,
                      "Inbound version check failed")) {
        return false;
    }

    bytes = ob::codec::encode_inbound(inbound_sample());
    bytes[36] = 99;
    if (!expect_error(ob::codec::decode_inbound(bytes).error, ob::codec::DecodeError::InvalidSide,
                      "Inbound side check failed")) {
        return false;
    }

    bytes = ob::codec::encode_inbound(inbound_sample());
    bytes[37] = static_cast<std::uint8_t>(ob::MsgType::StopEngine);
    return expect_error(ob::codec::decode_inbound(bytes).error,
                        ob::codec::DecodeError::InvalidMsgType,
                        "Inbound StopEngine rejection failed");
}

bool check_outbound_validation() {
    auto bytes = ob::codec::encode_outbound(outbound_sample());

    // Replayable logs exclude internal shutdown events and unknown flag bits.
    bytes[37] = static_cast<std::uint8_t>(ob::EventType::StopEngine);
    if (!expect_error(ob::codec::decode_outbound(bytes).error,
                      ob::codec::DecodeError::InvalidEventType,
                      "Outbound StopEngine rejection failed")) {
        return false;
    }

    bytes = ob::codec::encode_outbound(outbound_sample());
    bytes[38] = 99;
    if (!expect_error(ob::codec::decode_outbound(bytes).error,
                      ob::codec::DecodeError::InvalidRejectReason,
                      "Outbound reject reason check failed")) {
        return false;
    }

    bytes = ob::codec::encode_outbound(outbound_sample());
    bytes[39] = 0x80;
    return expect_error(ob::codec::decode_outbound(bytes).error,
                        ob::codec::DecodeError::InvalidFlags, "Outbound flags check failed");
}

}  // namespace

int main() {
    using Check = bool (*)();
    constexpr std::array<Check, 4> checks{
        check_inbound_round_trip_and_offsets,
        check_outbound_round_trip,
        check_inbound_validation,
        check_outbound_validation,
    };

    for (Check check : checks) {
        if (!check()) {
            return 1;
        }
    }

    std::printf("codec_test OK\n");
    return 0;
}
