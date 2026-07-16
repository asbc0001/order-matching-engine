// demo_renderer_test.cpp - Checks for the terminal book-view renderer.

#include <array>
#include <cstdio>
#include <cstring>

#include "orderbook/demo_renderer.hpp"

namespace {

bool fail(const char* message) {
    std::fprintf(stderr, "%s\n", message);
    return false;
}

ob::OutboundEvent event(ob::EventType type, ob::Handle handle, ob::Side side, ob::Price price,
                        ob::Qty qty) {
    return ob::OutboundEvent{
        .client_seq = 1,
        .handle = handle,
        .price = price,
        .qty = qty,
        .side = side,
        .type = type,
        .reason = ob::RejectReason::None,
        .flags = ob::RequestComplete,
        .tsc_intended = 0,
        .tsc_egress = 0,
    };
}

bool read_rendered_book(ob::DemoRenderer<8, 2>& renderer, char* buffer, std::size_t capacity) {
    std::FILE* file = std::tmpfile();
    if (file == nullptr) {
        return false;
    }

    const bool rendered = renderer.render(file);
    const bool rewound = std::fflush(file) == 0 && std::fseek(file, 0, SEEK_SET) == 0;
    const std::size_t used = rewound ? std::fread(buffer, 1, capacity - 1, file) : 0;
    buffer[used] = '\0';
    const bool read_ok = std::ferror(file) == 0;
    std::fclose(file);
    return rendered && rewound && read_ok;
}

bool check_renderer_updates_visible_book() {
    ob::DemoRenderer<8, 2> renderer;

    // Two asks at the same price aggregate into one displayed level; only the
    // best two bid prices are shown because this renderer is configured for 2.
    if (!renderer.apply(event(ob::EventType::AckNew, 10, ob::Side::Ask, 105, 3)) ||
        !renderer.apply(event(ob::EventType::AckNew, 11, ob::Side::Ask, 105, 2)) ||
        !renderer.apply(event(ob::EventType::AckNew, 20, ob::Side::Bid, 99, 4)) ||
        !renderer.apply(event(ob::EventType::AckNew, 21, ob::Side::Bid, 101, 6)) ||
        !renderer.apply(event(ob::EventType::AckNew, 22, ob::Side::Bid, 100, 5))) {
        return fail("Renderer failed to apply resting orders");
    }

    char buffer[256]{};
    constexpr char expected[] =
        "ASKS\n"
        "  105 | 5 | #####\n"
        "BIDS\n"
        "  101 | 6 | ######\n"
        "  100 | 5 | #####\n";
    return read_rendered_book(renderer, buffer, sizeof(buffer)) &&
                   std::strcmp(buffer, expected) == 0
               ? true
               : fail("Rendered book did not match expected top levels");
}

bool check_fills_cancels_and_stop() {
    ob::DemoRenderer<8, 2> renderer;

    if (!renderer.apply(event(ob::EventType::AckNew, 10, ob::Side::Ask, 105, 7)) ||
        !renderer.apply(event(ob::EventType::Fill, 10, ob::Side::Ask, 105, 4)) ||
        !renderer.apply(event(ob::EventType::Fill, 0, ob::Side::Bid, 105, 4)) ||
        !renderer.apply(event(ob::EventType::AckCancel, 10, ob::Side::Ask, 105, 3))) {
        return fail("Renderer failed fill/cancel sequence");
    }

    // The fill with handle 0 was the incoming order's trade report, not a
    // resting order, so only the handled fill and cancel change the book view.
    if (renderer.live_orders() != 0) {
        return fail("Renderer kept a removed order live");
    }

    if (!renderer.apply(event(ob::EventType::StopEngine, 0, ob::Side::Bid, 0, 0)) ||
        !renderer.stopped()) {
        return fail("Renderer did not record StopEngine");
    }
    return true;
}

}  // namespace

int main() {
    using Check = bool (*)();
    constexpr std::array<Check, 2> checks{
        check_renderer_updates_visible_book,
        check_fills_cancels_and_stop,
    };

    for (Check check : checks) {
        if (!check()) {
            return 1;
        }
    }

    std::printf("demo_renderer_test OK\n");
    return 0;
}
