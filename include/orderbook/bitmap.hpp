// bitmap.hpp - Fixed-size occupancy bitmap for price levels.
//
// The book uses one bit per price level to find the next occupied level without
// tree nodes or heap allocation. Scans are inclusive: asking from an occupied
// index returns that same index.

#pragma once

#include <array>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace ob {

inline constexpr std::size_t kBitmapWordBits = 64;

template <std::size_t NumLevels>
class Bitmap {
    static_assert(NumLevels > 0, "Bitmap must contain at least one level");
    static_assert((NumLevels % kBitmapWordBits) == 0,
                  "Bitmap level count must be a whole number of 64-bit words");

  public:
    static constexpr std::size_t num_levels() noexcept {
        return NumLevels;
    }

    static constexpr std::size_t word_count() noexcept {
        return NumLevels / kBitmapWordBits;
    }

    void set(std::size_t idx) noexcept {
        assert(idx < NumLevels);
        words_[word_index(idx)] |= bit_mask(idx);
    }

    void clear(std::size_t idx) noexcept {
        assert(idx < NumLevels);
        words_[word_index(idx)] &= ~bit_mask(idx);
    }

    [[nodiscard]] bool test(std::size_t idx) const noexcept {
        assert(idx < NumLevels);
        return (words_[word_index(idx)] & bit_mask(idx)) != 0;
    }

    // Find the first occupied level at or above idx.
    [[nodiscard]] std::optional<std::size_t> scan_up(std::size_t idx) const noexcept {
        if (idx >= NumLevels) {
            return std::nullopt;
        }

        std::size_t word = word_index(idx);
        // Ignore lower bits in the starting word so the scan is inclusive at
        // idx, then whole-word after that.
        uint64_t bits = words_[word] & (~uint64_t{0} << bit_offset(idx));

        while (true) {
            if (bits != 0) {
                // countr_zero gives the least-significant set bit, which is
                // the next occupied level when scanning upward.
                return (word * kBitmapWordBits) + static_cast<std::size_t>(std::countr_zero(bits));
            }
            if (++word == word_count()) {
                return std::nullopt;
            }
            bits = words_[word];
        }
    }

    // Find the first occupied level at or below idx.
    [[nodiscard]] std::optional<std::size_t> scan_down(std::size_t idx) const noexcept {
        if (idx >= NumLevels) {
            return std::nullopt;
        }

        std::size_t word = word_index(idx);
        const unsigned offset = bit_offset(idx);
        // Keep only bits at or below idx in the starting word. The subtraction
        // stays in range because offset is always 0..63.
        uint64_t bits = words_[word] & (~uint64_t{0} >> (kBitmapWordBits - 1 - offset));

        while (true) {
            if (bits != 0) {
                // countl_zero gives the most-significant set bit, which is
                // the next occupied level when scanning downward.
                return (word * kBitmapWordBits) +
                       static_cast<std::size_t>((kBitmapWordBits - 1) - std::countl_zero(bits));
            }
            if (word == 0) {
                return std::nullopt;
            }
            bits = words_[--word];
        }
    }

  private:
    static constexpr std::size_t word_index(std::size_t idx) noexcept {
        return idx / kBitmapWordBits;
    }

    static constexpr unsigned bit_offset(std::size_t idx) noexcept {
        return static_cast<unsigned>(idx % kBitmapWordBits);
    }

    static constexpr uint64_t bit_mask(std::size_t idx) noexcept {
        return uint64_t{1} << bit_offset(idx);
    }

    std::array<uint64_t, word_count()> words_{};
};

}  // namespace ob
