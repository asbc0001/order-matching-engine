// smoke_test.cpp - Minimal end-to-end check for the build system, headers, and
// CTest wiring. Component-specific unit tests will be added alongside each
// implementation.

#include <cstdio>

#include "orderbook/config.hpp"
#include "orderbook/pool.hpp"
#include "orderbook/types.hpp"

int main() {
    // Core type and configuration sanity checks.
    // types.hpp's static_asserts already ran at compile time; this is
    // a runtime sanity check so the test actually does something.
    if (ob::NULL_SLOT != 0xFFFFFFFFu) {
        std::fprintf(stderr, "NULL_SLOT mismatch\n");
        return 1;
    }
    if (ob::config::kProduction.num_levels != 65536) {
        std::fprintf(stderr, "kProduction.num_levels mismatch\n");
        return 1;
    }
    if (ob::Handle{0} != 0) {
        std::fprintf(stderr, "Handle zero-init sanity failed\n");
        return 1;
    }

    ob::Pool<4> pool;
    if (pool.capacity() != 4) {
        std::fprintf(stderr, "Pool capacity mismatch\n");
        return 1;
    }

    std::printf("smoke_test OK\n");
    return 0;
}
