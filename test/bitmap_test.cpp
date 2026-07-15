// bitmap_test.cpp - Focused checks for the fixed-size level occupancy bitmap.

#include <array>
#include <cstdio>
#include <optional>

#include "orderbook/bitmap.hpp"
#include "orderbook/config.hpp"

namespace {

using TestBitmap = ob::Bitmap<ob::config::kFuzz.num_levels>;

bool fail(const char* message) {
    std::fprintf(stderr, "%s\n", message);
    return false;
}

bool expect_eq(std::optional<std::size_t> actual, std::optional<std::size_t> expected,
               const char* message) {
    return actual == expected ? true : fail(message);
}

bool check_basic_set_clear_test() {
    TestBitmap bitmap;

    bitmap.set(7);
    bitmap.set(129);
    if (!bitmap.test(7) || !bitmap.test(129) || bitmap.test(8)) {
        return fail("Bitmap set/test failed");
    }

    bitmap.clear(7);
    return !bitmap.test(7) && bitmap.test(129) ? true : fail("Bitmap clear failed");
}

bool check_empty_scans() {
    TestBitmap bitmap;
    constexpr std::size_t kPastEnd = ob::config::kFuzz.num_levels;
    constexpr std::size_t kLast = kPastEnd - 1;

    return expect_eq(bitmap.scan_up(0), std::nullopt, "Empty scan_up at start") &&
           expect_eq(bitmap.scan_down(kLast), std::nullopt, "Empty scan_down at end") &&
           expect_eq(bitmap.scan_up(kPastEnd), std::nullopt, "Out-of-range scan_up failed") &&
           expect_eq(bitmap.scan_down(kPastEnd), std::nullopt, "Out-of-range scan_down failed");
}

bool check_inclusive_scans() {
    TestBitmap bitmap;
    bitmap.set(42);

    return expect_eq(bitmap.scan_up(42), 42, "scan_up was not inclusive") &&
           expect_eq(bitmap.scan_up(41), 42, "scan_up missed next higher bit") &&
           expect_eq(bitmap.scan_down(42), 42, "scan_down was not inclusive") &&
           expect_eq(bitmap.scan_down(43), 42, "scan_down missed next lower bit");
}

bool check_word_boundary_scans() {
    TestBitmap bitmap;
    for (std::size_t idx : {63u, 64u, 65u, 127u, 128u}) {
        bitmap.set(idx);
    }

    if (!expect_eq(bitmap.scan_up(62), 63, "scan_up missed bit 63") ||
        !expect_eq(bitmap.scan_up(64), 64, "scan_up missed inclusive bit 64") ||
        !expect_eq(bitmap.scan_up(126), 127, "scan_up missed bit 127") ||
        !expect_eq(bitmap.scan_up(128), 128, "scan_up missed inclusive bit 128") ||
        !expect_eq(bitmap.scan_down(66), 65, "scan_down missed bit 65") ||
        !expect_eq(bitmap.scan_down(128), 128, "scan_down missed inclusive bit 128")) {
        return false;
    }

    bitmap.clear(64);
    return expect_eq(bitmap.scan_up(64), 65, "scan_up failed after clearing boundary bit") &&
           expect_eq(bitmap.scan_down(64), 63, "scan_down failed after clearing boundary bit");
}

bool check_band_edges() {
    TestBitmap bitmap;
    constexpr std::size_t kLast = ob::config::kFuzz.num_levels - 1;
    bitmap.set(0);
    bitmap.set(kLast);

    if (!expect_eq(bitmap.scan_up(0), 0, "scan_up missed first level") ||
        !expect_eq(bitmap.scan_down(0), 0, "scan_down missed first level") ||
        !expect_eq(bitmap.scan_up(kLast), kLast, "scan_up missed last level") ||
        !expect_eq(bitmap.scan_down(kLast), kLast, "scan_down missed last level")) {
        return false;
    }

    bitmap.clear(0);
    bitmap.clear(kLast);
    return expect_eq(bitmap.scan_down(0), std::nullopt,
                     "scan_down below first level should be empty") &&
           expect_eq(bitmap.scan_up(kLast), std::nullopt,
                     "scan_up above last level should be empty");
}

bool check_sparse_word_skips() {
    TestBitmap bitmap;
    constexpr std::size_t kLast = ob::config::kFuzz.num_levels - 1;
    bitmap.set(0);
    bitmap.set(130);
    bitmap.set(kLast);

    if (!expect_eq(bitmap.scan_up(1), 130, "scan_up failed to skip empty words") ||
        !expect_eq(bitmap.scan_down(kLast - 1), 130, "scan_down failed to skip empty words")) {
        return false;
    }

    bitmap.clear(130);
    return expect_eq(bitmap.scan_up(1), kLast, "scan_up failed after clearing sparse bit") &&
           expect_eq(bitmap.scan_down(kLast - 1), 0, "scan_down failed after clearing sparse bit");
}

}  // namespace

int main() {
    using Check = bool (*)();
    constexpr std::array<Check, 6> checks{
        check_basic_set_clear_test, check_empty_scans, check_inclusive_scans,
        check_word_boundary_scans,  check_band_edges,  check_sparse_word_skips,
    };

    for (Check check : checks) {
        if (!check()) {
            return 1;
        }
    }

    std::printf("bitmap_test OK\n");
    return 0;
}
