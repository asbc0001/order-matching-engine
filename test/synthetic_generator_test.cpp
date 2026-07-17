// synthetic_generator_test.cpp - Checks for repeatable synthetic command streams.

#include <array>
#include <cstdio>

#include "orderbook/config.hpp"
#include "orderbook/synthetic_generator.hpp"

namespace {

using Generator = ob::synthetic::CommandGenerator<ob::config::kFuzz.num_levels, 16>;

bool fail(const char* message) {
    std::fprintf(stderr, "%s\n", message);
    return false;
}

bool same_command(const ob::InboundMsg& lhs, const ob::InboundMsg& rhs) {
    return lhs.client_seq == rhs.client_seq && lhs.handle == rhs.handle && lhs.price == rhs.price &&
           lhs.qty == rhs.qty && lhs.side == rhs.side && lhs.type == rhs.type;
}

bool check_same_seed_repeats_stream() {
    Generator first{12345};
    Generator second{12345};

    for (std::uint64_t seq = 1; seq <= 64; ++seq) {
        if (!same_command(first.next(seq), second.next(seq))) {
            return fail("Same seed produced different command stream");
        }
    }
    return true;
}

bool check_observed_ack_new_enables_valid_cancel() {
    ob::synthetic::GeneratorConfig only_cancel{
        .limit_weight = 0,
        .market_weight = 0,
        .cancel_weight = 1,
    };
    Generator generator{7, only_cancel};
    constexpr ob::Handle kHandle = 0x100000001ULL;

    generator.observe(ob::OutboundEvent{
        .client_seq = 1,
        .handle = kHandle,
        .price = 100,
        .qty = 50,
        .side = ob::Side::Bid,
        .type = ob::EventType::AckNew,
        .reason = ob::RejectReason::None,
        .flags = ob::RequestComplete,
        .tsc_intended = 0,
        .tsc_egress = 0,
    });

    const ob::InboundMsg cancel = generator.next(2);
    return cancel.type == ob::MsgType::Cancel && cancel.handle == kHandle
               ? true
               : fail("Valid cancel failed");
}

bool check_valid_cancel_mode_creates_real_cancel_path() {
    ob::synthetic::GeneratorConfig valid_cancel_only{
        .limit_weight = 0,
        .market_weight = 0,
        .cancel_weight = 1,
        .valid_cancels_only = true,
    };
    Generator generator{7, valid_cancel_only};

    // With no known handles yet, the generator must create a real resting
    // limit order first. A forged cancel would fail the replay gate.
    const ob::InboundMsg setup = generator.next(1);
    if (setup.type != ob::MsgType::NewLimit || setup.qty == 0) {
        return fail("Valid-cancel mode did not create a setup limit");
    }

    constexpr ob::Handle kHandle = 0x300000001ULL;
    generator.observe(ob::OutboundEvent{
        .client_seq = setup.client_seq,
        .handle = kHandle,
        .price = setup.price,
        .qty = setup.qty,
        .side = setup.side,
        .type = ob::EventType::AckNew,
        .reason = ob::RejectReason::None,
        .flags = ob::RequestComplete,
        .tsc_intended = 0,
        .tsc_egress = 0,
    });

    const ob::InboundMsg cancel = generator.next(2);
    return cancel.type == ob::MsgType::Cancel && cancel.handle == kHandle
               ? true
               : fail("Valid-cancel mode did not use the observed handle");
}

bool check_fill_removes_exhausted_handle() {
    Generator generator{9};
    constexpr ob::Handle kHandle = 0x200000001ULL;

    generator.observe(ob::OutboundEvent{
        .client_seq = 1,
        .handle = kHandle,
        .price = 101,
        .qty = 10,
        .side = ob::Side::Ask,
        .type = ob::EventType::AckNew,
        .reason = ob::RejectReason::None,
        .flags = ob::RequestComplete,
        .tsc_intended = 0,
        .tsc_egress = 0,
    });
    generator.observe(ob::OutboundEvent{
        .client_seq = 2,
        .handle = kHandle,
        .price = 101,
        .qty = 10,
        .side = ob::Side::Ask,
        .type = ob::EventType::Fill,
        .reason = ob::RejectReason::None,
        .flags = 0,
        .tsc_intended = 0,
        .tsc_egress = 0,
    });

    return generator.live_count() == 0 ? true : fail("Filled handle stayed live");
}

}  // namespace

int main() {
    using Check = bool (*)();
    constexpr std::array<Check, 4> checks{
        check_same_seed_repeats_stream,
        check_observed_ack_new_enables_valid_cancel,
        check_valid_cancel_mode_creates_real_cancel_path,
        check_fill_removes_exhausted_handle,
    };

    for (Check check : checks) {
        if (!check()) {
            return 1;
        }
    }

    std::printf("synthetic_generator_test OK\n");
    return 0;
}
