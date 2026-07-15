// book_test.cpp - Structural checks for passive book data structures.

#include <cstdio>
#include <optional>
#include <random>
#include <vector>

#include "orderbook/book.hpp"
#include "orderbook/config.hpp"
#include "orderbook/types.hpp"

namespace {

bool fail(const char* message) {
    std::fprintf(stderr, "%s\n", message);
    return false;
}

bool expect_best(std::optional<std::size_t> actual, std::optional<std::size_t> expected,
                 const char* message) {
    if (actual != expected) {
        return fail(message);
    }
    return true;
}

template <std::size_t Capacity>
uint32_t make_order(ob::Pool<Capacity>& pool, ob::Price price, ob::Qty qty,
                    ob::Side side = ob::Side::Bid) {
    // Mirror the future insert path: populate semantic order fields before
    // append_tail reads remaining and threads the slot into a level.
    auto alloc = pool.alloc();
    if (!alloc) {
        return ob::NULL_SLOT;
    }

    alloc->order->client_seq = alloc->slot + 1;
    alloc->order->price = price;
    alloc->order->remaining = qty;
    alloc->order->side = side;
    return alloc->slot;
}

template <std::size_t Capacity>
bool expect_level(const ob::Pool<Capacity>& pool, const ob::Level& level, uint32_t head,
                  uint32_t tail, ob::AggQty total_qty, uint32_t order_count, const char* message) {
    // Shared check for the local invariants these list operations must keep
    // true after every mutation.
    if (level.head != head || level.tail != tail || level.total_qty != total_qty ||
        level.order_count != order_count) {
        return fail(message);
    }
    if (head != ob::NULL_SLOT && pool.at(head).prev != ob::NULL_SLOT) {
        return fail("Level head prev was not NULL_SLOT");
    }
    if (tail != ob::NULL_SLOT && pool.at(tail).next != ob::NULL_SLOT) {
        return fail("Level tail next was not NULL_SLOT");
    }
    return true;
}

bool check_append_tail_fifo_links() {
    ob::Pool<4> pool;
    ob::Level level;

    const uint32_t first = make_order(pool, 100, 10);
    const uint32_t second = make_order(pool, 100, 20);
    const uint32_t third = make_order(pool, 100, 30);
    if (first == ob::NULL_SLOT || second == ob::NULL_SLOT || third == ob::NULL_SLOT) {
        return fail("append setup allocation failed");
    }

    ob::append_tail(pool, level, first);
    if (!expect_level(pool, level, first, first, 10, 1, "append first level state failed")) {
        return false;
    }

    ob::append_tail(pool, level, second);
    ob::append_tail(pool, level, third);

    // Appends preserve FIFO order by linking each new slot after the old tail.
    if (!expect_level(pool, level, first, third, 60, 3, "append multi level state failed")) {
        return false;
    }
    if (pool.at(first).next != second || pool.at(second).prev != first ||
        pool.at(second).next != third || pool.at(third).prev != second) {
        return fail("append FIFO links failed");
    }

    return true;
}

bool check_unlink_head_middle_tail() {
    ob::Pool<6> pool;
    ob::Level level;
    const uint32_t first = make_order(pool, 102, 10);
    const uint32_t second = make_order(pool, 102, 20);
    const uint32_t third = make_order(pool, 102, 30);
    const uint32_t fourth = make_order(pool, 102, 40);
    if (first == ob::NULL_SLOT || second == ob::NULL_SLOT || third == ob::NULL_SLOT ||
        fourth == ob::NULL_SLOT) {
        return fail("unlink setup allocation failed");
    }

    ob::append_tail(pool, level, first);
    ob::append_tail(pool, level, second);
    ob::append_tail(pool, level, third);
    ob::append_tail(pool, level, fourth);

    // Exercise all non-only removal shapes on one list so neighbor repairs are
    // checked after each mutation.
    ob::unlink(pool, level, first);
    if (!expect_level(pool, level, second, fourth, 90, 3, "head unlink level state failed")) {
        return false;
    }
    if (pool.at(second).prev != ob::NULL_SLOT || pool.at(first).next != ob::NULL_SLOT) {
        return fail("head unlink links failed");
    }

    ob::unlink(pool, level, third);
    if (!expect_level(pool, level, second, fourth, 60, 2, "middle unlink level state failed")) {
        return false;
    }
    if (pool.at(second).next != fourth || pool.at(fourth).prev != second ||
        pool.at(third).prev != ob::NULL_SLOT || pool.at(third).next != ob::NULL_SLOT) {
        return fail("middle unlink links failed");
    }

    ob::unlink(pool, level, fourth);
    if (!expect_level(pool, level, second, second, 20, 1, "tail unlink level state failed")) {
        return false;
    }
    if (pool.at(second).next != ob::NULL_SLOT || pool.at(fourth).prev != ob::NULL_SLOT) {
        return fail("tail unlink links failed");
    }

    return true;
}

bool check_pop_head() {
    ob::Pool<4> pool;
    ob::Level level;
    const uint32_t first = make_order(pool, 103, 11);
    const uint32_t second = make_order(pool, 103, 22);
    if (first == ob::NULL_SLOT || second == ob::NULL_SLOT) {
        return fail("pop setup allocation failed");
    }

    if (ob::pop_head(pool, level)) {
        return fail("pop_head returned a slot for an empty level");
    }

    ob::append_tail(pool, level, first);
    ob::append_tail(pool, level, second);

    const auto popped_first = ob::pop_head(pool, level);
    if (!popped_first || *popped_first != first) {
        return fail("pop_head did not return first slot");
    }
    if (pool.at(first).prev != ob::NULL_SLOT || pool.at(first).next != ob::NULL_SLOT) {
        return fail("pop_head did not clear popped links");
    }
    if (!expect_level(pool, level, second, second, 22, 1, "pop first level state failed")) {
        return false;
    }

    const auto popped_second = ob::pop_head(pool, level);
    if (!popped_second || *popped_second != second) {
        return fail("pop_head did not return second slot");
    }
    if (!expect_level(pool, level, ob::NULL_SLOT, ob::NULL_SLOT, 0, 0,
                      "pop second level state failed")) {
        return false;
    }

    return true;
}

bool check_book_insert_updates_best_and_levels() {
    ob::Book<ob::config::kFuzz.num_levels, 8> book;

    const auto first_bid = book.insert(ob::Side::Bid, 10, 100, 1);
    const auto worse_bid = book.insert(ob::Side::Bid, 9, 200, 2);
    const auto better_bid = book.insert(ob::Side::Bid, 12, 300, 3);
    if (!first_bid || !worse_bid || !better_bid) {
        return fail("Book bid insert failed");
    }
    if (!expect_best(book.best_bid_idx(), std::optional<std::size_t>{12},
                     "Book best bid did not improve upward")) {
        return false;
    }
    if (book.level(10).order_count != 1 || book.level(9).order_count != 1 ||
        book.level(12).total_qty != 300) {
        return fail("Book bid level aggregates failed");
    }

    const auto first_ask = book.insert(ob::Side::Ask, 20, 400, 4);
    const auto worse_ask = book.insert(ob::Side::Ask, 22, 500, 5);
    const auto better_ask = book.insert(ob::Side::Ask, 18, 600, 6);
    if (!first_ask || !worse_ask || !better_ask) {
        return fail("Book ask insert failed");
    }
    if (!expect_best(book.best_ask_idx(), std::optional<std::size_t>{18},
                     "Book best ask did not improve downward")) {
        return false;
    }
    if (book.level(18).total_qty != 600 || book.level(20).order_count != 1 ||
        book.level(22).order_count != 1) {
        return fail("Book ask level aggregates failed");
    }

    const auto same_level = book.insert(ob::Side::Bid, 12, 25, 7);
    if (!same_level) {
        return fail("Book same-side same-level insert failed");
    }
    if (book.level(12).order_count != 2 || book.level(12).total_qty != 325) {
        return fail("Book same-level aggregate failed");
    }

    return true;
}

bool check_book_remove_repairs_best_only_when_needed() {
    ob::Book<ob::config::kFuzz.num_levels, 8> book;

    const auto low_bid = book.insert(ob::Side::Bid, 5, 10, 1);
    const auto best_bid = book.insert(ob::Side::Bid, 10, 20, 2);
    const auto second_best_bid = book.insert(ob::Side::Bid, 10, 25, 3);
    const auto mid_bid = book.insert(ob::Side::Bid, 7, 30, 3);
    if (!low_bid || !best_bid || !second_best_bid || !mid_bid) {
        return fail("Book remove bid setup failed");
    }

    if (!book.remove(*low_bid)) {
        return fail("Book failed to remove non-best bid");
    }
    if (!expect_best(book.best_bid_idx(), std::optional<std::size_t>{10},
                     "Book changed best bid after non-best empty")) {
        return false;
    }
    if (book.occupied(5)) {
        return fail("Book did not clear non-best bid bitmap bit");
    }

    // Removing one order from a multi-order best level must not trigger a best
    // rescan or clear that level's bitmap bit.
    if (!book.remove(*best_bid)) {
        return fail("Book failed to remove best bid");
    }
    if (!expect_best(book.best_bid_idx(), std::optional<std::size_t>{10},
                     "Book moved best bid while best level still had orders")) {
        return false;
    }
    if (!book.occupied(10) || book.level(10).order_count != 1) {
        return fail("Book cleared best bid level too early");
    }

    if (!book.remove(*second_best_bid)) {
        return fail("Book failed to empty best bid level");
    }
    // Once the best level is truly empty, the bid pointer repairs downward to
    // the next occupied bid level.
    if (!expect_best(book.best_bid_idx(), std::optional<std::size_t>{7},
                     "Book did not rescan best bid downward")) {
        return false;
    }

    const auto best_ask = book.insert(ob::Side::Ask, 20, 40, 4);
    const auto second_best_ask = book.insert(ob::Side::Ask, 20, 45, 5);
    const auto high_ask = book.insert(ob::Side::Ask, 25, 50, 5);
    if (!best_ask || !second_best_ask || !high_ask) {
        return fail("Book remove ask setup failed");
    }
    if (!book.remove(*high_ask)) {
        return fail("Book failed to remove non-best ask");
    }
    if (!expect_best(book.best_ask_idx(), std::optional<std::size_t>{20},
                     "Book changed best ask after non-best empty")) {
        return false;
    }
    if (!book.remove(*best_ask)) {
        return fail("Book failed to remove best ask");
    }
    // Same contract for asks: partial emptying of the best level leaves the
    // cached best alone until the last order at that level is removed.
    if (!expect_best(book.best_ask_idx(), std::optional<std::size_t>{20},
                     "Book moved best ask while best level still had orders")) {
        return false;
    }
    if (!book.occupied(20) || book.level(20).order_count != 1) {
        return fail("Book cleared best ask level too early");
    }
    if (!book.remove(*second_best_ask)) {
        return fail("Book failed to empty best ask level");
    }
    if (!expect_best(book.best_ask_idx(), std::optional<std::size_t>{},
                     "Book best ask should be empty")) {
        return false;
    }
    if (!book.remove(*mid_bid)) {
        return fail("Book failed to drain final bid");
    }
    if (!expect_best(book.best_bid_idx(), std::optional<std::size_t>{},
                     "Book best bid should be empty")) {
        return false;
    }
    if (!book.audit()) {
        return fail("Book final drain audit failed");
    }

    return true;
}

bool check_book_best_repairs_across_bitmap_words() {
    ob::Book<ob::config::kFuzz.num_levels, 8> bid_book;

    const auto bid_63 = bid_book.insert(ob::Side::Bid, 63, 10, 1);
    const auto bid_64 = bid_book.insert(ob::Side::Bid, 64, 10, 2);
    const auto bid_65 = bid_book.insert(ob::Side::Bid, 65, 10, 3);
    const auto bid_127 = bid_book.insert(ob::Side::Bid, 127, 10, 4);
    const auto bid_128 = bid_book.insert(ob::Side::Bid, 128, 10, 5);
    if (!bid_63 || !bid_64 || !bid_65 || !bid_127 || !bid_128) {
        return fail("Book word-boundary bid setup failed");
    }

    // Emptying best bids walks downward across the same 127/128 and 63/64
    // boundaries the bitmap unit tests cover directly.
    if (!bid_book.remove(*bid_128) ||
        !expect_best(bid_book.best_bid_idx(), std::optional<std::size_t>{127},
                     "Book best bid did not repair from 128 to 127")) {
        return false;
    }
    if (!bid_book.remove(*bid_127) ||
        !expect_best(bid_book.best_bid_idx(), std::optional<std::size_t>{65},
                     "Book best bid did not repair from 127 to 65")) {
        return false;
    }
    if (!bid_book.remove(*bid_65) ||
        !expect_best(bid_book.best_bid_idx(), std::optional<std::size_t>{64},
                     "Book best bid did not repair from 65 to 64")) {
        return false;
    }
    if (!bid_book.remove(*bid_64) ||
        !expect_best(bid_book.best_bid_idx(), std::optional<std::size_t>{63},
                     "Book best bid did not repair from 64 to 63")) {
        return false;
    }

    ob::Book<ob::config::kFuzz.num_levels, 8> ask_book;
    const auto ask_63 = ask_book.insert(ob::Side::Ask, 63, 10, 1);
    const auto ask_64 = ask_book.insert(ob::Side::Ask, 64, 10, 2);
    const auto ask_65 = ask_book.insert(ob::Side::Ask, 65, 10, 3);
    const auto ask_127 = ask_book.insert(ob::Side::Ask, 127, 10, 4);
    const auto ask_128 = ask_book.insert(ob::Side::Ask, 128, 10, 5);
    if (!ask_63 || !ask_64 || !ask_65 || !ask_127 || !ask_128) {
        return fail("Book word-boundary ask setup failed");
    }

    // Emptying best asks walks upward across those boundaries.
    if (!ask_book.remove(*ask_63) ||
        !expect_best(ask_book.best_ask_idx(), std::optional<std::size_t>{64},
                     "Book best ask did not repair from 63 to 64")) {
        return false;
    }
    if (!ask_book.remove(*ask_64) ||
        !expect_best(ask_book.best_ask_idx(), std::optional<std::size_t>{65},
                     "Book best ask did not repair from 64 to 65")) {
        return false;
    }
    if (!ask_book.remove(*ask_65) ||
        !expect_best(ask_book.best_ask_idx(), std::optional<std::size_t>{127},
                     "Book best ask did not repair from 65 to 127")) {
        return false;
    }
    if (!ask_book.remove(*ask_127) ||
        !expect_best(ask_book.best_ask_idx(), std::optional<std::size_t>{128},
                     "Book best ask did not repair from 127 to 128")) {
        return false;
    }

    return bid_book.audit() && ask_book.audit();
}

bool check_book_edges_and_failures() {
    ob::Book<ob::config::kFuzz.num_levels, 2> book;
    constexpr ob::Price kLastPrice =
        ob::config::BASE_PRICE + static_cast<ob::Price>(ob::config::kFuzz.num_levels - 1);

    const auto first_level = book.insert(ob::Side::Bid, ob::config::BASE_PRICE, 10, 1);
    const auto last_level = book.insert(ob::Side::Ask, kLastPrice, 20, 2);
    if (!first_level || !last_level) {
        return fail("Book failed to insert band-edge prices");
    }
    if (!book.occupied(0) || !book.occupied(ob::config::kFuzz.num_levels - 1)) {
        return fail("Book band-edge bitmap bits were not set");
    }

    // With two successful edge inserts and capacity 2, later valid inserts
    // also prove the pool-exhaustion path is clean.
    if (book.insert(ob::Side::Bid, ob::config::BASE_PRICE - 1, 1, 3)) {
        return fail("Book accepted below-band price");
    }
    if (book.insert(ob::Side::Ask, kLastPrice + 1, 1, 4)) {
        return fail("Book accepted above-band price");
    }
    if (book.insert(ob::Side::Bid, 1, 0, 5)) {
        return fail("Book accepted zero quantity");
    }
    if (book.insert(ob::Side::Bid, 1, 1, 6)) {
        return fail("Book accepted insert after pool exhaustion");
    }

    if (!book.remove(*first_level)) {
        return fail("Book failed to remove edge bid");
    }
    if (book.remove(*first_level)) {
        return fail("Book accepted stale handle removal");
    }
    if (book.remove(0)) {
        return fail("Book accepted null handle removal");
    }

    return true;
}

bool check_book_rejects_crossed_resting_orders() {
    ob::Book<ob::config::kFuzz.num_levels, 4> book;

    const auto bid = book.insert(ob::Side::Bid, 10, 10, 1);
    const auto ask = book.insert(ob::Side::Ask, 12, 10, 2);
    if (!bid || !ask) {
        return fail("Book crossing setup failed");
    }
    // Crossed orders belong to matching logic; this structural book rejects
    // them rather than storing an invalid resting state.
    if (book.insert(ob::Side::Bid, 12, 10, 3)) {
        return fail("Book accepted bid crossing best ask");
    }
    if (book.insert(ob::Side::Ask, 10, 10, 4)) {
        return fail("Book accepted ask crossing best bid");
    }

    return true;
}

bool check_book_invariant_entry_points() {
    ob::Book<ob::config::kFuzz.num_levels, 8> book;
    // The public invariant entry points should be useful on both fresh and
    // mutated books; later fuzzing can call the same functions directly.
    if (!book.check_local() || !book.audit()) {
        return fail("Fresh book invariants failed");
    }

    const auto bid = book.insert(ob::Side::Bid, 10, 100, 1);
    const auto ask = book.insert(ob::Side::Ask, 20, 200, 2);
    if (!bid || !ask) {
        return fail("Invariant setup insert failed");
    }
    if (!book.check_local(10) || !book.check_local(20) || !book.audit()) {
        return fail("Invariant checks failed after insert");
    }

    if (!book.remove(*bid)) {
        return fail("Invariant setup remove failed");
    }
    if (!book.check_local(10) || !book.audit()) {
        return fail("Invariant checks failed after remove");
    }

    return true;
}

bool check_book_mixed_side_randomized_churn() {
    struct LiveOrder {
        ob::Handle handle;
        std::size_t idx;
    };

    constexpr std::size_t kLevels = ob::config::kFuzz.num_levels;
    constexpr std::size_t kCapacity = 64;
    constexpr std::size_t kOperations = 8'000;
    constexpr std::size_t kBidBand = 96;
    constexpr std::size_t kAskBase = 160;

    ob::Book<kLevels, kCapacity> book;
    std::vector<LiveOrder> live;
    live.reserve(kCapacity);
    std::mt19937_64 rng{0x51DE5};

    // The gap between kBidBand and kAskBase guarantees the random stream never
    // crosses, while still exercising both cached best pointers.
    for (std::size_t op = 0; op < kOperations; ++op) {
        const bool must_insert = live.empty();
        const bool must_remove = live.size() == kCapacity;
        const bool do_insert = must_insert || (!must_remove && ((rng() & 3u) != 0));

        if (do_insert) {
            // Keep bids and asks in separated bands so this test exercises both
            // sides without invoking matching behavior.
            const bool bid = (rng() & 1u) == 0;
            const std::size_t idx =
                bid ? static_cast<std::size_t>(rng() % kBidBand)
                    : kAskBase + static_cast<std::size_t>(rng() % (kLevels - kAskBase));
            const ob::Side side = bid ? ob::Side::Bid : ob::Side::Ask;
            const ob::Qty qty = static_cast<ob::Qty>((rng() % 500) + 1);
            const auto handle = book.insert(side, static_cast<ob::Price>(idx), qty, op + 1);
            if (!handle) {
                return fail("Mixed-side randomized insert failed");
            }
            live.push_back(LiveOrder{.handle = *handle, .idx = idx});
            if (!book.check_local(idx)) {
                return fail("Mixed-side local invariant failed after insert");
            }
        } else {
            const std::size_t victim_idx = static_cast<std::size_t>(rng() % live.size());
            const LiveOrder victim = live[victim_idx];

            if (!book.remove(victim.handle)) {
                return fail("Mixed-side randomized remove failed");
            }
            live[victim_idx] = live.back();
            live.pop_back();

            if (!book.check_local(victim.idx)) {
                return fail("Mixed-side local invariant failed after remove");
            }
        }

        if ((op % 131) == 0 && !book.audit()) {
            return fail("Mixed-side full audit failed during churn");
        }
    }

    if (!book.audit()) {
        return fail("Mixed-side final audit failed");
    }

    return true;
}

}  // namespace

int main() {
    if (!check_append_tail_fifo_links()) {
        return 1;
    }
    if (!check_unlink_head_middle_tail()) {
        return 1;
    }
    if (!check_pop_head()) {
        return 1;
    }
    if (!check_book_insert_updates_best_and_levels()) {
        return 1;
    }
    if (!check_book_remove_repairs_best_only_when_needed()) {
        return 1;
    }
    if (!check_book_best_repairs_across_bitmap_words()) {
        return 1;
    }
    if (!check_book_edges_and_failures()) {
        return 1;
    }
    if (!check_book_rejects_crossed_resting_orders()) {
        return 1;
    }
    if (!check_book_invariant_entry_points()) {
        return 1;
    }
    if (!check_book_mixed_side_randomized_churn()) {
        return 1;
    }

    std::printf("book_test OK\n");
    return 0;
}
