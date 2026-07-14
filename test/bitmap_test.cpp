// bitmap_test.cpp - Focused checks for the fixed-size level occupancy bitmap.

#include <cstdio>
#include <optional>

#include "orderbook/bitmap.hpp"
#include "orderbook/config.hpp"

namespace {

[[nodiscard]] bool fail(const char* message) {
    std::fprintf(stderr, "%s\n", message);
    return false;
}

template <typename T>
[[nodiscard]] bool expect_eq(std::optional<T> actual, std::optional<T> expected,
                             const char* message) {
    if (actual != expected) {
        return fail(message);
    }
    return true;
}

[[nodiscard]] bool check_basic_set_clear_test() {
    ob::Bitmap<ob::config::kFuzz.num_levels> bitmap;

    if (bitmap.test(7)) {
        return fail("Fresh bitmap unexpectedly had bit set");
    }

    bitmap.set(7);
    bitmap.set(129);
    if (!bitmap.test(7) || !bitmap.test(129)) {
        return fail("Bitmap set/test failed");
    }
    if (bitmap.test(8)) {
        return fail("Bitmap test saw neighboring bit");
    }

    bitmap.clear(7);
    if (bitmap.test(7) || !bitmap.test(129)) {
        return fail("Bitmap clear affected wrong bit");
    }

    return true;
}

[[nodiscard]] bool check_empty_scans() {
    ob::Bitmap<ob::config::kFuzz.num_levels> bitmap;

    if (!expect_eq(bitmap.scan_up(0), std::optional<std::size_t>{}, "Empty scan_up at start")) {
        return false;
    }
    if (!expect_eq(bitmap.scan_down(ob::config::kFuzz.num_levels - 1), std::optional<std::size_t>{},
                   "Empty scan_down at end")) {
        return false;
    }
    if (!expect_eq(bitmap.scan_up(ob::config::kFuzz.num_levels), std::optional<std::size_t>{},
                   "Out-of-range scan_up failed")) {
        return false;
    }
    if (!expect_eq(bitmap.scan_down(ob::config::kFuzz.num_levels), std::optional<std::size_t>{},
                   "Out-of-range scan_down failed")) {
        return false;
    }

    return true;
}

[[nodiscard]] bool check_inclusive_scans() {
    ob::Bitmap<ob::config::kFuzz.num_levels> bitmap;
    bitmap.set(42);

    // The book can ask the bitmap about a known candidate level without first
    // offsetting by one; callers choose exclusive behavior explicitly.
    if (!expect_eq(bitmap.scan_up(42), std::optional<std::size_t>{42},
                   "scan_up was not inclusive")) {
        return false;
    }
    if (!expect_eq(bitmap.scan_down(42), std::optional<std::size_t>{42},
                   "scan_down was not inclusive")) {
        return false;
    }
    if (!expect_eq(bitmap.scan_up(41), std::optional<std::size_t>{42},
                   "scan_up missed next higher bit")) {
        return false;
    }
    if (!expect_eq(bitmap.scan_down(43), std::optional<std::size_t>{42},
                   "scan_down missed next lower bit")) {
        return false;
    }

    return true;
}

[[nodiscard]] bool check_word_boundary_scans() {
    ob::Bitmap<ob::config::kFuzz.num_levels> bitmap;

    // These indices straddle 64-bit word boundaries, where mask direction and
    // shift counts are easiest to get subtly wrong.
    bitmap.set(63);
    bitmap.set(64);
    bitmap.set(65);
    bitmap.set(127);
    bitmap.set(128);

    if (!expect_eq(bitmap.scan_up(62), std::optional<std::size_t>{63}, "scan_up missed bit 63")) {
        return false;
    }
    if (!expect_eq(bitmap.scan_up(64), std::optional<std::size_t>{64},
                   "scan_up missed inclusive bit 64")) {
        return false;
    }
    if (!expect_eq(bitmap.scan_down(66), std::optional<std::size_t>{65},
                   "scan_down missed bit 65")) {
        return false;
    }
    if (!expect_eq(bitmap.scan_up(126), std::optional<std::size_t>{127},
                   "scan_up missed bit 127")) {
        return false;
    }
    if (!expect_eq(bitmap.scan_up(128), std::optional<std::size_t>{128},
                   "scan_up missed inclusive bit 128")) {
        return false;
    }
    if (!expect_eq(bitmap.scan_down(128), std::optional<std::size_t>{128},
                   "scan_down missed inclusive bit 128")) {
        return false;
    }

    bitmap.clear(64);
    // Clearing the boundary bit forces both scans to cross from word 1 back to
    // word 0 or forward within word 1.
    if (!expect_eq(bitmap.scan_up(64), std::optional<std::size_t>{65},
                   "scan_up failed after clearing word-boundary bit")) {
        return false;
    }
    if (!expect_eq(bitmap.scan_down(64), std::optional<std::size_t>{63},
                   "scan_down failed after clearing word-boundary bit")) {
        return false;
    }

    return true;
}

[[nodiscard]] bool check_band_edges() {
    ob::Bitmap<ob::config::kFuzz.num_levels> bitmap;
    constexpr std::size_t kLast = ob::config::kFuzz.num_levels - 1;

    bitmap.set(0);
    bitmap.set(kLast);

    // Edge bits cover the exact price band ends the book will expose.
    if (!expect_eq(bitmap.scan_up(0), std::optional<std::size_t>{0},
                   "scan_up missed first level")) {
        return false;
    }
    if (!expect_eq(bitmap.scan_down(0), std::optional<std::size_t>{0},
                   "scan_down missed first level")) {
        return false;
    }
    if (!expect_eq(bitmap.scan_up(kLast), std::optional<std::size_t>{kLast},
                   "scan_up missed last level")) {
        return false;
    }
    if (!expect_eq(bitmap.scan_down(kLast), std::optional<std::size_t>{kLast},
                   "scan_down missed last level")) {
        return false;
    }

    bitmap.clear(0);
    if (!expect_eq(bitmap.scan_down(0), std::optional<std::size_t>{},
                   "scan_down below first level should be empty")) {
        return false;
    }

    bitmap.clear(kLast);
    if (!expect_eq(bitmap.scan_up(kLast), std::optional<std::size_t>{},
                   "scan_up above last level should be empty")) {
        return false;
    }

    return true;
}

[[nodiscard]] bool check_sparse_word_skips() {
    ob::Bitmap<ob::config::kFuzz.num_levels> bitmap;
    constexpr std::size_t kLast = ob::config::kFuzz.num_levels - 1;

    bitmap.set(0);
    bitmap.set(130);
    bitmap.set(kLast);

    // These scans cross one or more completely empty words before finding the
    // next occupied level.
    if (!expect_eq(bitmap.scan_up(1), std::optional<std::size_t>{130},
                   "scan_up failed to skip empty words")) {
        return false;
    }
    if (!expect_eq(bitmap.scan_down(kLast - 1), std::optional<std::size_t>{130},
                   "scan_down failed to skip empty words")) {
        return false;
    }

    bitmap.clear(130);
    if (!expect_eq(bitmap.scan_up(1), std::optional<std::size_t>{kLast},
                   "scan_up failed after clearing sparse middle bit")) {
        return false;
    }
    if (!expect_eq(bitmap.scan_down(kLast - 1), std::optional<std::size_t>{0},
                   "scan_down failed after clearing sparse middle bit")) {
        return false;
    }

    return true;
}

}  // namespace

int main() {
    if (!check_basic_set_clear_test()) {
        return 1;
    }
    if (!check_empty_scans()) {
        return 1;
    }
    if (!check_inclusive_scans()) {
        return 1;
    }
    if (!check_word_boundary_scans()) {
        return 1;
    }
    if (!check_band_edges()) {
        return 1;
    }
    if (!check_sparse_word_skips()) {
        return 1;
    }

    std::printf("bitmap_test OK\n");
    return 0;
}
