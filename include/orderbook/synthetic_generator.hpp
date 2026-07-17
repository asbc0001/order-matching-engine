// synthetic_generator.hpp - Repeatable command generator for tests and runs.
//
// Given the same seed, this generator emits the same stream of InboundMsg
// commands. It uses the same command-shape ideas as the differential fuzzer:
// mostly limit orders, some market orders, occasional cancels, edge prices, hot
// prices, zero quantities, and large quantities.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <random>

#include "orderbook/config.hpp"
#include "orderbook/types.hpp"

namespace ob::synthetic {

// Weights are relative, not percentages. For example, 70/20/10 means roughly
// 70 limit commands for every 20 market commands and 10 cancel commands.
struct GeneratorConfig {
    std::uint8_t limit_weight{70};
    std::uint8_t market_weight{20};
    std::uint8_t cancel_weight{10};
    // When true, CANCEL commands are only made from handles learned through
    // observe(). This is used for saved command files that should replay
    // without UnknownHandle rejects.
    bool valid_cancels_only{false};
};

template <std::size_t NumLevels, std::size_t MaxTrackedHandles,
          Price BasePrice = config::BASE_PRICE>
class CommandGenerator {
  public:
    explicit CommandGenerator(std::uint64_t seed, GeneratorConfig config = {}) noexcept
        : rng_(seed), config_(config) {}

    // Create one command with the caller-provided sequence number. If the
    // caller also feeds outbound events to observe(), future CANCEL commands can
    // use real assigned handles; otherwise cancels are intentionally invalid.
    InboundMsg next(std::uint64_t client_seq) noexcept {
        const std::uint64_t total_weight = static_cast<std::uint64_t>(config_.limit_weight) +
                                           config_.market_weight + config_.cancel_weight;
        const std::uint64_t pick = total_weight == 0 ? 0 : rng_() % total_weight;

        if (pick < config_.limit_weight) {
            if (config_.valid_cancels_only) {
                return safe_resting_limit(client_seq);
            }
            return make_msg(client_seq, MsgType::NewLimit, random_side(), random_price(),
                            random_qty());
        }
        if (pick < static_cast<std::uint64_t>(config_.limit_weight) + config_.market_weight) {
            return make_msg(client_seq, MsgType::NewMarket, random_side(), 0, random_qty());
        }
        if (config_.valid_cancels_only && live_count_ == 0) {
            return safe_resting_limit(client_seq);
        }
        return random_cancel(client_seq);
    }

    // Learn real handles and remaining quantities from matcher output. This is
    // optional, but it lets synthetic streams include meaningful cancels instead
    // of only forged handles.
    void observe(const OutboundEvent& event) noexcept {
        if (event.type == EventType::AckNew && event.handle != 0 && event.qty > 0) {
            add_or_update(event.handle, event.qty);
            return;
        }
        if (event.type == EventType::AckCancel && event.handle != 0) {
            remove(event.handle);
            remember_removed(event.handle);
            return;
        }
        if (event.type == EventType::Fill && event.handle != 0) {
            reduce_or_remove(event.handle, event.qty);
        }
    }

    std::size_t live_count() const noexcept {
        return live_count_;
    }

  private:
    // The generator only needs enough state to issue plausible cancels. It is
    // not a second book and does not try to reconstruct price levels.
    struct LiveHandle {
        Handle handle{0};
        Qty remaining{0};
    };

    static InboundMsg make_msg(std::uint64_t client_seq, MsgType type, Side side, Price price,
                               Qty qty, Handle handle = 0) noexcept {
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

    // Choose Bid/Ask evenly so generated traffic exercises both books sides.
    Side random_side() noexcept {
        return (rng_() & 1u) == 0 ? Side::Bid : Side::Ask;
    }

    // Mix ordinary sizes with zero and very large quantities. The matcher, not
    // the generator, decides whether those are accepted or rejected.
    Qty random_qty() noexcept {
        const std::uint64_t pick = rng_() % 20;
        if (pick == 0) {
            return 0;
        }
        if (pick == 1) {
            return 1'000'000;
        }
        return static_cast<Qty>((rng_() % 200) + 1);
    }

    // Include out-of-band prices, edge prices, and a couple of repeated hot
    // prices so one stream reaches validation and FIFO paths.
    Price random_price() noexcept {
        constexpr Price kLast = BasePrice + static_cast<Price>(NumLevels - 1);
        switch (rng_() % 10) {
            case 0:
                return BasePrice - 1;
            case 1:
                return kLast + 1;
            case 2:
                return BasePrice;
            case 3:
                return kLast;
            case 4:
                return BasePrice + 100;
            case 5:
                return BasePrice + 101;
            default:
                return BasePrice + static_cast<Price>(rng_() % NumLevels);
        }
    }

    // The cancel-heavy saved-command path needs setup orders that really rest
    // in the book. These prices stay inside the configured band and away from
    // each other, so generated bids and asks do not immediately cross.
    InboundMsg safe_resting_limit(std::uint64_t client_seq) noexcept {
        static_assert(NumLevels >= 2, "valid cancel generation needs at least two price levels");

        const Side side = random_side();
        const std::size_t bid_index = NumLevels / 3;
        const std::size_t ask_index = (2 * NumLevels) / 3;
        const std::size_t price_index = side == Side::Bid ? bid_index : ask_index;
        const Qty qty = static_cast<Qty>((rng_() % 200) + 1);
        return make_msg(client_seq, MsgType::NewLimit, side,
                        BasePrice + static_cast<Price>(price_index), qty);
    }

    // Prefer real live handles when observe() has seen them. Otherwise generate
    // stale/null/forged handles to keep UnknownHandle paths active.
    InboundMsg random_cancel(std::uint64_t client_seq) noexcept {
        if (config_.valid_cancels_only && live_count_ > 0) {
            const std::size_t idx = static_cast<std::size_t>(rng_() % live_count_);
            return make_msg(client_seq, MsgType::Cancel, Side::Bid, 0, 0, live_[idx].handle);
        }

        const std::uint64_t mode = rng_() % 10;
        if (mode < 6 && live_count_ > 0) {
            const std::size_t idx = static_cast<std::size_t>(rng_() % live_count_);
            return make_msg(client_seq, MsgType::Cancel, Side::Bid, 0, 0, live_[idx].handle);
        }
        if (mode < 8 && removed_count_ > 0) {
            const std::size_t idx = static_cast<std::size_t>(rng_() % removed_count_);
            return make_msg(client_seq, MsgType::Cancel, Side::Bid, 0, 0, removed_[idx]);
        }

        const Handle forged = (Handle{0xFFFFu} << 32) | Handle{rng_() % 2048};
        const Handle handle = (rng_() & 1u) == 0 ? 0 : forged;
        return make_msg(client_seq, MsgType::Cancel, Side::Bid, 0, 0, handle);
    }

    // Store a newly accepted resting order, or refresh its remaining quantity
    // if a later event reports the same handle.
    void add_or_update(Handle handle, Qty remaining) noexcept {
        for (std::size_t i = 0; i < live_count_; ++i) {
            if (live_[i].handle == handle) {
                live_[i].remaining = remaining;
                return;
            }
        }
        if (live_count_ < live_.size()) {
            live_[live_count_++] = LiveHandle{.handle = handle, .remaining = remaining};
        }
    }

    // Removing by swap keeps the tracked-handle array dense without preserving
    // order, which is fine because random selection has no FIFO requirement.
    void remove(Handle handle) noexcept {
        for (std::size_t i = 0; i < live_count_; ++i) {
            if (live_[i].handle == handle) {
                live_[i] = live_[live_count_ - 1];
                --live_count_;
                return;
            }
        }
    }

    // Resting fills reduce the tracked remaining quantity; a full fill removes
    // the handle so future valid cancels do not target a dead order.
    void reduce_or_remove(Handle handle, Qty filled) noexcept {
        for (std::size_t i = 0; i < live_count_; ++i) {
            if (live_[i].handle != handle) {
                continue;
            }
            if (filled >= live_[i].remaining) {
                const Handle removed = live_[i].handle;
                live_[i] = live_[live_count_ - 1];
                --live_count_;
                remember_removed(removed);
            } else {
                live_[i].remaining -= filled;
            }
            return;
        }
    }

    // Keep a bounded sample of removed handles so the generator can also create
    // stale-cancel traffic without allocating.
    void remember_removed(Handle handle) noexcept {
        if (removed_count_ < removed_.size()) {
            removed_[removed_count_++] = handle;
        } else if (!removed_.empty()) {
            removed_[static_cast<std::size_t>(rng_() % removed_.size())] = handle;
        }
    }

    std::mt19937_64 rng_;
    GeneratorConfig config_;
    std::array<LiveHandle, MaxTrackedHandles> live_{};
    std::array<Handle, MaxTrackedHandles> removed_{};
    std::size_t live_count_{0};
    std::size_t removed_count_{0};
};

}  // namespace ob::synthetic
