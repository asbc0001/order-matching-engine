// engine_bench.cpp - Fixed-rate benchmark for the threaded engine pipeline.
//
// This benchmark sends synthetic commands through the same producer -> matcher
// -> logger thread shape as run_engine. It is intentionally small: one run,
// one requested send rate, and a no-cancel workload so the producer does not
// need to learn engine-assigned handles from the logger thread.

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string_view>
#include <thread>
#include <vector>

#include "orderbook/config.hpp"
#include "orderbook/matching_loop.hpp"
#include "orderbook/spsc_ring.hpp"
#include "orderbook/synthetic_generator.hpp"
#include "orderbook/time.hpp"
#include "orderbook/types.hpp"

namespace {

using InboundRing = ob::SpscRing<ob::InboundMsg, ob::config::kProduction.ring_capacity>;
using OutboundRing = ob::SpscRing<ob::OutboundEvent, ob::config::kProduction.ring_capacity>;
using EngineLoop =
    ob::MatchingLoop<ob::config::kProduction.num_levels, ob::config::kProduction.pool_capacity,
                     ob::config::kProduction.ring_capacity, ob::config::kProduction.ring_capacity>;
using Generator = ob::synthetic::CommandGenerator<ob::config::kProduction.num_levels, 1>;

constexpr std::uint64_t kDefaultCommands = 1'000'000;
constexpr std::uint64_t kDefaultRatePerSecond = 100'000;
constexpr std::uint64_t kDefaultSeed = 12345;
constexpr std::uint64_t kStartDelayNanos = 10'000'000;

struct Options {
    std::uint64_t commands{kDefaultCommands};
    std::uint64_t rate_per_second{kDefaultRatePerSecond};
    std::uint64_t seed{kDefaultSeed};
    ob::WaitMode wait_mode{ob::WaitMode::Spin};
};

struct ProducerStats {
    std::uint64_t inbound_full_waits{0};
    std::uint64_t late_sends{0};
};

struct LoggerStats {
    std::uint64_t events{0};
    std::uint64_t completed_requests{0};
    std::uint64_t outbound_empty_waits{0};
    bool saw_stop{false};
};

int usage(const char* program) {
    std::fprintf(stderr,
                 "usage: %s [commands] [rate_per_second] [seed] [--yield]\n"
                 "defaults: commands=%llu rate_per_second=%llu seed=%llu\n",
                 program, static_cast<unsigned long long>(kDefaultCommands),
                 static_cast<unsigned long long>(kDefaultRatePerSecond),
                 static_cast<unsigned long long>(kDefaultSeed));
    return 2;
}

bool parse_u64(const char* text, std::uint64_t& value) {
    errno = 0;
    char* end = nullptr;
    const unsigned long long parsed = std::strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || parsed == 0) {
        return false;
    }

    value = static_cast<std::uint64_t>(parsed);
    return true;
}

bool parse_options(int argc, char** argv, Options& options) {
    // Positional arguments keep quick local runs simple:
    //   engine_bench [commands] [rate_per_second] [seed]
    int positional = 0;
    for (int i = 1; i < argc; ++i) {
        if (std::string_view{argv[i]} == "--yield") {
            options.wait_mode = ob::WaitMode::Yield;
            continue;
        }

        std::uint64_t parsed = 0;
        if (!parse_u64(argv[i], parsed)) {
            return false;
        }
        if (positional == 0) {
            options.commands = parsed;
        } else if (positional == 1) {
            options.rate_per_second = parsed;
        } else if (positional == 2) {
            options.seed = parsed;
        } else {
            return false;
        }
        ++positional;
    }
    return true;
}

void wait_once(ob::WaitMode mode) noexcept {
    ob::matching_loop_wait(mode);
}

void wait_until(std::uint64_t target_time, ob::WaitMode mode) noexcept {
    while (ob::engine_time_nanos() < target_time) {
        wait_once(mode);
    }
}

// Each command gets its own planned send time. If one send is late, the next
// command still uses its original planned time instead of sliding later.
std::uint64_t intended_time(std::uint64_t start_time, std::uint64_t zero_based_index,
                            std::uint64_t rate_per_second) noexcept {
    // Fractional math avoids bunching sends when the requested rate does not
    // divide evenly into one billion nanoseconds.
    const auto offset =
        static_cast<std::uint64_t>((static_cast<long double>(zero_based_index) * 1'000'000'000.0L) /
                                   static_cast<long double>(rate_per_second));
    return start_time + offset;
}

void push_command(InboundRing& inbound, const ob::InboundMsg& input, ProducerStats& stats,
                  ob::WaitMode wait_mode) noexcept {
    ob::InboundMsg msg = input;
    msg.tsc_ready = ob::engine_time_nanos();
    for (;;) {
        // Record the timestamp for the push attempt that succeeds.
        msg.tsc_enqueue = ob::engine_time_nanos();
        if (inbound.push(msg)) {
            return;
        }
        ++stats.inbound_full_waits;
        wait_once(wait_mode);
    }
}

ob::InboundMsg stop_command(std::uint64_t client_seq) noexcept {
    const std::uint64_t now = ob::engine_time_nanos();
    return ob::InboundMsg{
        .client_seq = client_seq,
        .handle = 0,
        .price = 0,
        .qty = 0,
        .side = ob::Side::Bid,
        .type = ob::MsgType::StopEngine,
        .tsc_intended = now,
        .tsc_ready = now,
        .tsc_enqueue = now,
    };
}

void run_producer(InboundRing& inbound, const Options& options, std::uint64_t start_time,
                  ProducerStats& stats) {
    // Live threaded generation cannot safely learn real handles from the logger,
    // so this benchmark still disables cancels. IOC/FOK and participants are
    // included so the matching path is closer to the finished command mix.
    ob::synthetic::GeneratorConfig config{
        .limit_weight = 80,
        .market_weight = 20,
        .cancel_weight = 0,
        .gtc_weight = 8,
        .ioc_weight = 1,
        .fok_weight = 1,
        .participant_count = 3,
    };
    Generator generator{options.seed, config};

    for (std::uint64_t i = 0; i < options.commands; ++i) {
        const std::uint64_t intended = intended_time(start_time, i, options.rate_per_second);
        if (ob::engine_time_nanos() > intended) {
            ++stats.late_sends;
        }
        wait_until(intended, options.wait_mode);

        ob::InboundMsg msg = generator.next(i + 1);
        msg.tsc_intended = intended;
        push_command(inbound, msg, stats, options.wait_mode);
    }

    push_command(inbound, stop_command(options.commands + 1), stats, options.wait_mode);
}

LoggerStats run_logger(OutboundRing& outbound, std::uint64_t command_count,
                       std::vector<std::uint64_t>& latencies, ob::WaitMode wait_mode) {
    LoggerStats stats;
    ob::OutboundEvent event{};
    for (;;) {
        if (!outbound.pop(event)) {
            ++stats.outbound_empty_waits;
            wait_once(wait_mode);
            continue;
        }

        ++stats.events;
        // Latency is sampled once per generated command. The shutdown command
        // uses the next sequence number, so it is excluded here.
        if (event.client_seq <= command_count && (event.flags & ob::RequestComplete) != 0) {
            ++stats.completed_requests;
            const std::uint64_t now = ob::engine_time_nanos();
            if (event.tsc_intended != 0 && now >= event.tsc_intended) {
                latencies.push_back(now - event.tsc_intended);
            }
        }

        if (event.type == ob::EventType::StopEngine) {
            stats.saw_stop = true;
            return stats;
        }
    }
}

std::uint64_t percentile(const std::vector<std::uint64_t>& sorted, std::uint64_t pct) {
    if (sorted.empty()) {
        return 0;
    }
    const std::size_t index = static_cast<std::size_t>(((sorted.size() - 1) * pct + 99) / 100);
    return sorted[index];
}

int run_benchmark(const Options& options) {
    auto inbound = std::make_unique<InboundRing>();
    auto outbound = std::make_unique<OutboundRing>();
    auto loop = std::make_unique<EngineLoop>(*inbound, *outbound);
    auto latencies = std::make_unique<std::vector<std::uint64_t>>();
    latencies->reserve(static_cast<std::size_t>(options.commands + 1));

    ProducerStats producer_stats{};
    ob::MatchingLoopStats matching_stats{};
    LoggerStats logger_stats{};
    const std::uint64_t start_time = ob::engine_time_nanos() + kStartDelayNanos;

    std::thread producer{[&] { run_producer(*inbound, options, start_time, producer_stats); }};
    std::thread matcher{[&] { matching_stats = loop->run_until_stop(options.wait_mode); }};
    logger_stats = run_logger(*outbound, options.commands, *latencies, options.wait_mode);

    producer.join();
    matcher.join();

    if (!matching_stats.stopped || !logger_stats.saw_stop) {
        std::fprintf(stderr, "benchmark did not shut down cleanly\n");
        return 1;
    }

    std::sort(latencies->begin(), latencies->end());
    const std::uint64_t p50 = percentile(*latencies, 50);
    const std::uint64_t p99 = percentile(*latencies, 99);
    const std::uint64_t max = latencies->empty() ? 0 : latencies->back();

    std::printf("commands=%llu rate_per_second=%llu seed=%llu\n",
                static_cast<unsigned long long>(options.commands),
                static_cast<unsigned long long>(options.rate_per_second),
                static_cast<unsigned long long>(options.seed));
    std::printf("events=%llu completed=%llu latency_samples=%zu\n",
                static_cast<unsigned long long>(logger_stats.events),
                static_cast<unsigned long long>(logger_stats.completed_requests),
                latencies->size());
    std::printf("latency_ns: p50=%llu p99=%llu max=%llu\n", static_cast<unsigned long long>(p50),
                static_cast<unsigned long long>(p99), static_cast<unsigned long long>(max));
    std::printf(
        "waits: inbound_full=%llu inbound_empty=%llu outbound_full=%llu outbound_empty=%llu\n",
        static_cast<unsigned long long>(producer_stats.inbound_full_waits),
        static_cast<unsigned long long>(matching_stats.inbound_empty_waits),
        static_cast<unsigned long long>(matching_stats.outbound_full_waits),
        static_cast<unsigned long long>(logger_stats.outbound_empty_waits));
    std::printf("late_sends=%llu\n", static_cast<unsigned long long>(producer_stats.late_sends));
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    if (!parse_options(argc, argv, options)) {
        return usage(argv[0]);
    }
    return run_benchmark(options);
}
