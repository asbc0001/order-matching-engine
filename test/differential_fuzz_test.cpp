// differential_fuzz_test.cpp - Seeded randomized comparison against the reference book.

#include <array>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <random>
#include <unordered_map>
#include <vector>

#include "orderbook/config.hpp"
#include "orderbook/matcher.hpp"
#include "orderbook/types.hpp"
#include "reference_book.hpp"

namespace {

constexpr std::size_t kPoolCapacity = 32;
constexpr std::size_t kProductionAuditEveryOps = 100'000;

struct FuzzConfig {
    std::size_t ops_per_seed;
    std::size_t audit_every_ops;
    uint32_t limit_weight;
    uint32_t market_weight;
    uint32_t cancel_weight;
};

// This small-book test compares and audits after every operation.
constexpr FuzzConfig kConfig{
    .ops_per_seed = 5'000,
    .audit_every_ops = 1,
    .limit_weight = 55,
    .market_weight = 20,
    .cancel_weight = 25,
};

static_assert(kConfig.limit_weight + kConfig.market_weight + kConfig.cancel_weight == 100);

struct RecordingSink {
    std::vector<ob::OutboundEvent> events;

    void operator()(const ob::OutboundEvent& event) {
        events.push_back(event);
    }
};

// Counts important paths so the seeded run cannot silently miss them.
struct Coverage {
    std::size_t pool_exhausted{0};
    std::size_t price_out_of_band{0};
    std::size_t unknown_handle{0};
    std::size_t zero_qty{0};
};

// The fuzzer tracks only what a client could learn from events, not internals.
struct LiveOrder {
    ob::Handle engine_handle;
    ob::Handle reference_handle;
    ob::Qty remaining;
};

struct CommandPair {
    ob::InboundMsg engine;
    ob::InboundMsg reference;
};

ob::InboundMsg msg(uint64_t seq, ob::MsgType type, ob::Side side, ob::Price price, ob::Qty qty,
                   ob::Handle handle = 0) noexcept {
    return ob::InboundMsg{
        .client_seq = seq,
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

const char* side_name(ob::Side side) {
    return side == ob::Side::Bid ? "Bid" : "Ask";
}

const char* type_name(ob::MsgType type) {
    switch (type) {
        case ob::MsgType::NewLimit:
            return "NewLimit";
        case ob::MsgType::NewMarket:
            return "NewMarket";
        case ob::MsgType::Cancel:
            return "Cancel";
        case ob::MsgType::StopEngine:
            return "StopEngine";
    }
    return "?";
}

const char* event_name(ob::EventType type) {
    switch (type) {
        case ob::EventType::AckNew:
            return "AckNew";
        case ob::EventType::AckCancel:
            return "AckCancel";
        case ob::EventType::Fill:
            return "Fill";
        case ob::EventType::Reject:
            return "Reject";
        case ob::EventType::StopEngine:
            return "StopEngine";
    }
    return "?";
}

void print_command(const char* label, const ob::InboundMsg& command) {
    std::fprintf(stderr,
                 "%s{seq=%" PRIu64 ", type=%s, side=%s, price=%" PRId64 ", qty=%" PRIu32
                 ", handle=%" PRIu64 "}\n",
                 label, command.client_seq, type_name(command.type), side_name(command.side),
                 command.price, command.qty, command.handle);
}

void print_event(const char* label, const ob::OutboundEvent& event) {
    std::fprintf(stderr,
                 "%s{seq=%" PRIu64 ", type=%s, side=%s, price=%" PRId64 ", qty=%" PRIu32
                 ", handle=%" PRIu64 ", reason=%u, flags=%u}\n",
                 label, event.client_seq, event_name(event.type), side_name(event.side),
                 event.price, event.qty, event.handle, static_cast<unsigned>(event.reason),
                 static_cast<unsigned>(event.flags));
}

bool has_one_request_complete(const std::vector<ob::OutboundEvent>& events) {
    std::size_t count = 0;
    for (const ob::OutboundEvent& event : events) {
        count += (event.flags & ob::RequestComplete) != 0 ? 1 : 0;
    }
    return count == 1;
}

std::size_t read_size_env(const char* name, std::size_t fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return fallback;
    }
    return static_cast<std::size_t>(std::strtoull(value, nullptr, 10));
}

std::optional<uint64_t> read_seed_env() {
    const char* value = std::getenv("OB_FUZZ_SEED");
    if (value == nullptr || *value == '\0') {
        return std::nullopt;
    }
    return static_cast<uint64_t>(std::strtoull(value, nullptr, 0));
}

bool has_env_value(const char* name) {
    const char* value = std::getenv(name);
    return value != nullptr && *value != '\0';
}

void record_coverage(const std::vector<ob::OutboundEvent>& events, Coverage& coverage) {
    // Engine and reference streams already matched; counting one side is enough.
    for (const ob::OutboundEvent& event : events) {
        if (event.type != ob::EventType::Reject) {
            continue;
        }

        switch (event.reason) {
            case ob::RejectReason::PoolExhausted:
                ++coverage.pool_exhausted;
                break;
            case ob::RejectReason::PriceOutOfBand:
                ++coverage.price_out_of_band;
                break;
            case ob::RejectReason::UnknownHandle:
                ++coverage.unknown_handle;
                break;
            case ob::RejectReason::ZeroQty:
                ++coverage.zero_qty;
                break;
            default:
                break;
        }
    }
}

bool check_coverage(const Coverage& coverage) {
    if (coverage.pool_exhausted == 0 || coverage.price_out_of_band == 0 ||
        coverage.unknown_handle == 0 || coverage.zero_qty == 0) {
        std::fprintf(stderr,
                     "fuzzer missed required paths: PoolExhausted=%zu PriceOutOfBand=%zu "
                     "UnknownHandle=%zu ZeroQty=%zu\n",
                     coverage.pool_exhausted, coverage.price_out_of_band, coverage.unknown_handle,
                     coverage.zero_qty);
        return false;
    }
    return true;
}

// State the test learns from events. It lets later random commands cancel real
// orders, cancel already-removed orders, and compare different handle values.
class FuzzerState {
  public:
    std::optional<ob::Handle> reference_handle_for(ob::Handle engine_handle) const {
        const auto found = engine_to_reference_.find(engine_handle);
        if (found == engine_to_reference_.end()) {
            return std::nullopt;
        }
        return found->second;
    }

    const std::vector<LiveOrder>& live() const {
        return live_;
    }

    const std::vector<ob::Handle>& removed_engine_handles() const {
        return removed_engine_handles_;
    }

    const std::vector<ob::Handle>& removed_reference_handles() const {
        return removed_reference_handles_;
    }

    // Pre-allocate storage for the largest run. This avoids rehashing and
    // vector growth while the production-size fuzzer fills the book.
    void reserve(std::size_t capacity) {
        engine_to_reference_.reserve(capacity);
        live_index_by_engine_.reserve(capacity);
        live_.reserve(capacity);
        removed_engine_handles_.reserve(capacity);
        removed_reference_handles_.reserve(capacity);
    }

    // Compare one command's events, then update the test's view of live and
    // removed orders only after the command is known to match.
    bool compare_and_apply(const std::vector<ob::OutboundEvent>& engine_events,
                           const std::vector<ob::OutboundEvent>& reference_events) {
        // RequestComplete is the per-command boundary marker; every path gets one.
        if (!has_one_request_complete(engine_events) ||
            !has_one_request_complete(reference_events)) {
            std::fprintf(stderr, "RequestComplete count was not exactly one\n");
            return false;
        }

        if (engine_events.size() != reference_events.size()) {
            std::fprintf(stderr, "event stream sizes differed: engine=%zu reference=%zu\n",
                         engine_events.size(), reference_events.size());
            return false;
        }

        for (std::size_t i = 0; i < engine_events.size(); ++i) {
            if (!events_match(engine_events[i], reference_events[i])) {
                std::fprintf(stderr, "event mismatch at index %zu\n", i);
                return false;
            }
            apply(engine_events[i], reference_events[i]);
        }

        return true;
    }

  private:
    // AckNew handles differ between implementations, so later nonzero handles
    // are compared through the mapping learned from earlier AckNew events.
    bool events_match(const ob::OutboundEvent& engine, const ob::OutboundEvent& reference) const {
        if (engine.client_seq != reference.client_seq || engine.price != reference.price ||
            engine.qty != reference.qty || engine.side != reference.side ||
            engine.type != reference.type || engine.reason != reference.reason ||
            engine.flags != reference.flags) {
            return false;
        }

        if (engine.type == ob::EventType::AckNew) {
            return engine.handle != 0 && reference.handle != 0;
        }
        if (engine.handle == 0 || reference.handle == 0) {
            return engine.handle == reference.handle;
        }

        const auto mapped = reference_handle_for(engine.handle);
        return mapped && *mapped == reference.handle;
    }

    // Update the fuzzer's client-side view after a matching event pair.
    void apply(const ob::OutboundEvent& engine, const ob::OutboundEvent& reference) {
        if (engine.type == ob::EventType::AckNew) {
            // AckNew tells us which handle each implementation assigned.
            engine_to_reference_[engine.handle] = reference.handle;
            live_index_by_engine_[engine.handle] = live_.size();
            live_.push_back(LiveOrder{engine.handle, reference.handle, engine.qty});
            return;
        }

        if (engine.type == ob::EventType::AckCancel) {
            remove_live(engine.handle);
            return;
        }

        if (engine.type == ob::EventType::Fill && engine.handle != 0) {
            // Only resting-party fills carry handles; aggressor fills use handle 0.
            reduce_live(engine.handle, engine.qty);
        }
    }

    // Move an order from live to removed so future cancels can test rejection.
    void remove_live(ob::Handle engine_handle) {
        // The index keeps production-size fuzzing from scanning every live order.
        const auto found = live_index_by_engine_.find(engine_handle);
        if (found == live_index_by_engine_.end()) {
            return;
        }

        const std::size_t index = found->second;
        removed_engine_handles_.push_back(live_[index].engine_handle);
        removed_reference_handles_.push_back(live_[index].reference_handle);
        engine_to_reference_.erase(live_[index].engine_handle);
        live_index_by_engine_.erase(found);

        if (index != live_.size() - 1) {
            live_[index] = live_.back();
            live_index_by_engine_[live_[index].engine_handle] = index;
        }
        live_.pop_back();
    }

    // Resting fills reduce client-visible remaining quantity; full fills remove.
    void reduce_live(ob::Handle engine_handle, ob::Qty fill_qty) {
        const auto found = live_index_by_engine_.find(engine_handle);
        if (found == live_index_by_engine_.end()) {
            return;
        }

        LiveOrder& order = live_[found->second];
        if (fill_qty >= order.remaining) {
            remove_live(engine_handle);
        } else {
            order.remaining -= fill_qty;
        }
    }

    std::unordered_map<ob::Handle, ob::Handle> engine_to_reference_;
    // Maps live engine handles to their slot in live_; swap-delete keeps it current.
    std::unordered_map<ob::Handle, std::size_t> live_index_by_engine_;
    std::vector<LiveOrder> live_;
    std::vector<ob::Handle> removed_engine_handles_;
    std::vector<ob::Handle> removed_reference_handles_;
};

ob::Side random_side(std::mt19937_64& rng) {
    return (rng() & 1u) == 0 ? ob::Side::Bid : ob::Side::Ask;
}

bool should_audit(std::size_t op, const FuzzConfig& config) {
    return config.audit_every_ops != 0 && (op % config.audit_every_ops) == 0;
}

// Generate a mix of invalid, ordinary, and very large quantities.
ob::Qty random_qty(std::mt19937_64& rng) {
    const uint64_t pick = rng() % 20;
    if (pick == 0) {
        // Invalid zero-quantity requests should reject without changing state.
        return 0;
    }
    if (pick == 1) {
        // Large quantities tend to walk several levels or exhaust liquidity.
        return 1'000'000;
    }
    return static_cast<ob::Qty>((rng() % 200) + 1);
}

// Generate prices that hit validation, band edges, random levels, and FIFO hot spots.
template <std::size_t NumLevels>
ob::Price random_price(std::mt19937_64& rng) {
    constexpr ob::Price kLast = ob::config::BASE_PRICE + static_cast<ob::Price>(NumLevels - 1);
    switch (rng() % 10) {
        case 0:
            // Mix in out-of-band prices to keep validation paths covered.
            return ob::config::BASE_PRICE - 1;
        case 1:
            return kLast + 1;
        case 2:
            return ob::config::BASE_PRICE;
        case 3:
            return kLast;
        case 4:
            // Reusing a few prices creates many orders at the same price.
            return 100;
        case 5:
            return 101;
        default:
            return static_cast<ob::Price>(rng() % NumLevels);
    }
}

// Build paired cancel commands. Valid cancels use each implementation's own
// handle; invalid cancels use removed, null, or made-up handles.
CommandPair random_cancel(uint64_t seq, std::mt19937_64& rng, const FuzzerState& state) {
    const uint64_t mode = rng() % 10;
    if (mode < 6 && !state.live().empty()) {
        // Valid cancels use the matching handle for each implementation.
        const LiveOrder& order =
            state.live()[static_cast<std::size_t>(rng() % state.live().size())];
        return {msg(seq, ob::MsgType::Cancel, ob::Side::Bid, 0, 0, order.engine_handle),
                msg(seq, ob::MsgType::Cancel, ob::Side::Bid, 0, 0, order.reference_handle)};
    }
    if (mode < 8 && !state.removed_engine_handles().empty()) {
        // Cancels for already-removed orders should reject.
        const std::size_t idx =
            static_cast<std::size_t>(rng() % state.removed_engine_handles().size());
        return {
            msg(seq, ob::MsgType::Cancel, ob::Side::Bid, 0, 0, state.removed_engine_handles()[idx]),
            msg(seq, ob::MsgType::Cancel, ob::Side::Bid, 0, 0,
                state.removed_reference_handles()[idx]),
        };
    }

    // These invalid cancels should all reject as UnknownHandle.
    const ob::Handle forged = (ob::Handle{0xFFFFu} << 32) | ob::Handle{rng() % 2048};
    const ob::Handle handle = (rng() & 1u) == 0 ? 0 : forged;
    return {msg(seq, ob::MsgType::Cancel, ob::Side::Bid, 0, 0, handle),
            msg(seq, ob::MsgType::Cancel, ob::Side::Bid, 0, 0, handle)};
}

// Choose the next operation. The same logical command goes to both books,
// except cancels may need different mapped handles.
template <std::size_t NumLevels>
CommandPair random_command(uint64_t seq, std::mt19937_64& rng, const FuzzerState& state,
                           const FuzzConfig& config) {
    const uint64_t kind = rng() % 100;
    // Bias toward limits so the book usually has liquidity for markets/cancels.
    if (kind < config.limit_weight) {
        const ob::InboundMsg command = msg(seq, ob::MsgType::NewLimit, random_side(rng),
                                           random_price<NumLevels>(rng), random_qty(rng));
        return {command, command};
    }
    if (kind < config.limit_weight + config.market_weight) {
        const ob::InboundMsg command =
            msg(seq, ob::MsgType::NewMarket, random_side(rng), 0, random_qty(rng));
        return {command, command};
    }
    return random_cancel(seq, rng, state);
}

// Process one command and stop immediately if behavior or book state diverges.
template <typename Engine, typename Reference>
bool process_and_check(Engine& engine, Reference& reference, FuzzerState& state, Coverage& coverage,
                       const CommandPair& command, uint64_t seed, std::size_t op,
                       const FuzzConfig& config) {
    RecordingSink engine_sink;
    RecordingSink reference_sink;

    engine.process(command.engine, engine_sink);
    reference.process(command.reference, reference_sink);

    record_coverage(engine_sink.events, coverage);

    const bool streams_match = state.compare_and_apply(engine_sink.events, reference_sink.events);
    const bool audit_ok = !should_audit(op, config) || engine.book().audit();
    if (!streams_match || !audit_ok) {
        std::fprintf(stderr, "differential fuzz failed: seed=%" PRIu64 " op=%zu\n", seed, op);
        std::fprintf(stderr, "exact replay: seed=%" PRIu64 " ops=%zu exact_replay=1\n", seed,
                     op + 1);
        print_command("engine command=", command.engine);
        print_command("reference command=", command.reference);
        for (const ob::OutboundEvent& event : engine_sink.events) {
            print_event("engine event=", event);
        }
        for (const ob::OutboundEvent& event : reference_sink.events) {
            print_event("reference event=", event);
        }
        return false;
    }
    return true;
}

// Run one reproducible random stream.
template <std::size_t NumLevels, std::size_t PoolCapacity>
bool run_seed(uint64_t seed, const FuzzConfig& config, Coverage& coverage) {
    using Engine = ob::Matcher<NumLevels, PoolCapacity>;
    using Reference = ob::test::ReferenceBook<NumLevels, PoolCapacity>;

    auto engine = std::make_unique<Engine>();
    auto reference = std::make_unique<Reference>();
    FuzzerState state;
    state.reserve(PoolCapacity);
    std::mt19937_64 rng{seed};
    std::size_t op = 0;

    // Start with a full book once so PoolExhausted is guaranteed, not random.
    // Spread orders across prices so production-size runs do not create one
    // enormous reference-book level.
    for (std::size_t i = 0; i < PoolCapacity; ++i, ++op) {
        const ob::Price price = ob::config::BASE_PRICE + static_cast<ob::Price>(i % NumLevels);
        const ob::InboundMsg command = msg(op + 1, ob::MsgType::NewLimit, ob::Side::Bid, price, 1);
        if (!process_and_check(*engine, *reference, state, coverage, {command, command}, seed, op,
                               config)) {
            return false;
        }
    }
    const ob::InboundMsg exhausted =
        msg(op + 1, ob::MsgType::NewLimit, ob::Side::Bid, ob::config::BASE_PRICE, 1);
    if (!process_and_check(*engine, *reference, state, coverage, {exhausted, exhausted}, seed, op,
                           config)) {
        return false;
    }
    ++op;

    for (std::size_t i = 0; i < config.ops_per_seed; ++i, ++op) {
        // A fixed seed makes any random failure exactly reproducible.
        const CommandPair command = random_command<NumLevels>(op + 1, rng, state, config);
        if (!process_and_check(*engine, *reference, state, coverage, command, seed, op, config)) {
            return false;
        }
    }

    return true;
}

std::vector<uint64_t> seeds_for_run(bool production_profile) {
    if (const auto seed = read_seed_env()) {
        return {*seed};
    }

    constexpr std::array<uint64_t, 10> seeds{
        0xC0FFEEu,   0x51DE5u,    0xBADC0DEu,   0x12345678u, 0xA11CEu,
        0xFEEDFACEu, 0xDEADBEEFu, 0x987654321u, 0x31415926u, 0x27182818u,
    };

    if (production_profile) {
        return {seeds[0]};
    }
    return std::vector<uint64_t>{seeds.begin(), seeds.end()};
}

// Environment knobs keep the normal test quick while allowing the full gate:
// OB_FUZZ_PROFILE=production OB_FUZZ_OPS=N OB_FUZZ_AUDIT_EVERY=N OB_FUZZ_SEED=N.
FuzzConfig config_for_run(bool production_profile) {
    FuzzConfig config = kConfig;
    if (production_profile) {
        config.audit_every_ops = kProductionAuditEveryOps;
    }

    config.ops_per_seed = read_size_env("OB_FUZZ_OPS", config.ops_per_seed);
    config.audit_every_ops = read_size_env("OB_FUZZ_AUDIT_EVERY", config.audit_every_ops);
    return config;
}

template <std::size_t NumLevels, std::size_t PoolCapacity>
bool check_seeded_differential_fuzz(bool production_profile) {
    const FuzzConfig config = config_for_run(production_profile);
    Coverage coverage;

    for (uint64_t seed : seeds_for_run(production_profile)) {
        if (!run_seed<NumLevels, PoolCapacity>(seed, config, coverage)) {
            return false;
        }
    }

    // Short explicit runs are useful for smoke checks and exact replay; the
    // normal configured run still has to prove it hit the required paths.
    if (has_env_value("OB_FUZZ_OPS") || has_env_value("OB_FUZZ_SEED")) {
        return true;
    }
    return check_coverage(coverage);
}

bool production_profile_requested() {
    const char* profile = std::getenv("OB_FUZZ_PROFILE");
    if (profile == nullptr || *profile == '\0') {
        return false;
    }
    if (std::strcmp(profile, "small") == 0) {
        return false;
    }
    if (std::strcmp(profile, "production") != 0) {
        std::fprintf(stderr, "OB_FUZZ_PROFILE must be 'small' or 'production'\n");
        std::exit(1);
    }
    return true;
}

}  // namespace

int main() {
    const bool production_profile = production_profile_requested();
    const bool ok =
        production_profile
            ? check_seeded_differential_fuzz<ob::config::kProduction.num_levels,
                                             ob::config::kProduction.pool_capacity>(true)
            : check_seeded_differential_fuzz<ob::config::kFuzz.num_levels, kPoolCapacity>(false);
    if (!ok) {
        return 1;
    }

    std::printf("differential_fuzz_test OK\n");
    return 0;
}
