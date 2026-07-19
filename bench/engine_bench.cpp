// engine_bench.cpp - Benchmark for the threaded engine pipeline.
//
// This benchmark sends synthetic commands through the same producer -> matcher
// -> logger thread shape as run_engine. Fixed-rate mode measures latency at a
// requested send rate. Closed-loop mode sends as fast as possible and reports
// throughput only.

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <span>
#include <string_view>
#include <thread>
#include <vector>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

#include "orderbook/codec.hpp"
#include "orderbook/config.hpp"
#include "orderbook/matching_loop.hpp"
#include "orderbook/spsc_ring.hpp"
#include "orderbook/synthetic_generator.hpp"
#include "orderbook/time.hpp"
#include "orderbook/trace_replay.hpp"
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

struct CoreAssignment {
    // Command-line order is producer, matcher, logger: --cores A,B,C.
    int producer{-1};
    int matcher{-1};
    int logger{-1};
};

struct Options {
    // commands is the measured portion of the run. --warmup adds extra commands
    // before it, instead of reducing the measured command count.
    std::uint64_t commands{kDefaultCommands};
    std::uint64_t rate_per_second{kDefaultRatePerSecond};
    std::uint64_t seed{kDefaultSeed};
    std::uint64_t warmup_commands{0};
    const char* input_path{nullptr};
    bool closed_loop{false};
    ob::WaitMode wait_mode{ob::WaitMode::Spin};
    CoreAssignment cores{};
};

struct ProducerStats {
    std::uint64_t inbound_full_waits{0};
    std::uint64_t late_sends{0};
};

struct ProducerTiming {
    std::vector<std::uint64_t> scheduler_lateness;
    std::vector<std::uint64_t> admission_wait;
    std::uint64_t measured_start{0};
};

struct LoggerStats {
    std::uint64_t events{0};
    std::uint64_t completed_requests{0};
    std::uint64_t outbound_empty_waits{0};
    std::uint64_t measured_end{0};
    bool saw_stop{false};
};

struct SavedCommands {
    std::vector<ob::InboundMsg> commands;
};

int usage(const char* program) {
    std::fprintf(stderr,
                 "usage: %s [commands] [rate_per_second] [seed] [--yield] [--warmup N] "
                 "[--cores A,B,C] [--input file.commands] [--closed-loop]\n"
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

bool parse_u64_allow_zero(const char* text, std::uint64_t& value) {
    errno = 0;
    char* end = nullptr;
    const unsigned long long parsed = std::strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        return false;
    }

    value = static_cast<std::uint64_t>(parsed);
    return true;
}

bool parse_core_id(const char*& cursor, int& core) {
    errno = 0;
    char* end = nullptr;
    const unsigned long parsed = std::strtoul(cursor, &end, 10);
    if (errno != 0 || end == cursor || parsed > static_cast<unsigned long>(INT_MAX)) {
        return false;
    }

    core = static_cast<int>(parsed);
    cursor = end;
    return true;
}

// Parse --cores A,B,C as producer CPU, matcher CPU, logger CPU.
bool parse_core_assignment(const char* text, CoreAssignment& cores) {
    const char* cursor = text;
    std::array<int*, 3> outputs{&cores.producer, &cores.matcher, &cores.logger};
    for (std::size_t i = 0; i < outputs.size(); ++i) {
        if (!parse_core_id(cursor, *outputs[i])) {
            return false;
        }
        if (i + 1 < outputs.size()) {
            if (*cursor != ',') {
                return false;
            }
            ++cursor;
        }
    }
    return *cursor == '\0';
}

bool has_core_assignment(const Options& options) noexcept {
    return options.cores.producer >= 0 || options.cores.matcher >= 0 || options.cores.logger >= 0;
}

bool parse_options(int argc, char** argv, Options& options) {
    // Positional arguments keep quick local runs simple:
    //   engine_bench [commands] [rate_per_second] [seed]
    int positional = 0;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg{argv[i]};
        if (arg == "--yield") {
            options.wait_mode = ob::WaitMode::Yield;
            continue;
        }
        if (arg == "--closed-loop") {
            options.closed_loop = true;
            continue;
        }
        if (arg == "--cores") {
            if (i + 1 >= argc || !parse_core_assignment(argv[++i], options.cores)) {
                return false;
            }
            continue;
        }
        constexpr std::string_view kCoresPrefix = "--cores=";
        if (arg.rfind(kCoresPrefix, 0) == 0) {
            if (!parse_core_assignment(argv[i] + kCoresPrefix.size(), options.cores)) {
                return false;
            }
            continue;
        }
        if (arg == "--warmup") {
            if (i + 1 >= argc || !parse_u64_allow_zero(argv[++i], options.warmup_commands)) {
                return false;
            }
            continue;
        }
        constexpr std::string_view kWarmupPrefix = "--warmup=";
        if (arg.rfind(kWarmupPrefix, 0) == 0) {
            if (!parse_u64_allow_zero(argv[i] + kWarmupPrefix.size(), options.warmup_commands)) {
                return false;
            }
            continue;
        }
        if (arg == "--input") {
            if (i + 1 >= argc) {
                return false;
            }
            options.input_path = argv[++i];
            continue;
        }
        constexpr std::string_view kInputPrefix = "--input=";
        if (arg.rfind(kInputPrefix, 0) == 0) {
            options.input_path = argv[i] + kInputPrefix.size();
            if (options.input_path[0] == '\0') {
                return false;
            }
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
    if (options.warmup_commands > UINT64_MAX - options.commands) {
        return false;
    }
    return true;
}

// Read saved commands before the timed threads start. File I/O is useful for
// repeatable workloads, but it is not part of the engine latency measurement.
bool load_saved_commands(const char* path, SavedCommands& saved) {
    std::FILE* file = std::fopen(path, "rb");
    if (file == nullptr) {
        std::fprintf(stderr, "failed to open input '%s': %s\n", path, std::strerror(errno));
        return false;
    }

    ob::trace::TraceHeader header{};
    if (std::fread(header.data(), 1, header.size(), file) != header.size()) {
        std::fprintf(stderr, "failed to read saved-command header from '%s'\n", path);
        std::fclose(file);
        return false;
    }

    const ob::trace::ReplayError header_error = ob::trace::validate_inbound_trace_header(header);
    if (header_error != ob::trace::ReplayError::None) {
        std::fprintf(stderr, "bad saved-command header in '%s': %s\n", path,
                     ob::trace::replay_error_name(header_error));
        std::fclose(file);
        return false;
    }

    ob::codec::EncodedInbound bytes{};
    for (;;) {
        const std::size_t read = std::fread(bytes.data(), 1, bytes.size(), file);
        if (read == 0) {
            if (std::ferror(file) != 0) {
                std::fprintf(stderr, "failed while reading '%s'\n", path);
                std::fclose(file);
                return false;
            }
            break;
        }
        if (read != bytes.size()) {
            std::fprintf(stderr, "truncated command record in '%s'\n", path);
            std::fclose(file);
            return false;
        }

        const auto decoded = ob::codec::decode_inbound(std::span<const std::uint8_t>{bytes});
        if (!decoded) {
            std::fprintf(stderr, "bad command record %zu in '%s': %s\n", saved.commands.size(),
                         path, ob::codec::decode_error_name(decoded.error));
            std::fclose(file);
            return false;
        }
        saved.commands.push_back(decoded.value);
    }

    std::fclose(file);
    return true;
}

// Pin one benchmark role to one CPU. The VM setup still has to choose real
// separate physical cores; this only applies the requested Linux affinity.
bool pin_thread(pthread_t thread, int core, const char* name) noexcept {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    const int rc = pthread_setaffinity_np(thread, sizeof(set), &set);
    if (rc != 0) {
        std::fprintf(stderr, "failed to pin %s to core %d: %s\n", name, core, std::strerror(rc));
        return false;
    }
    return true;
#else
    (void)thread;
    (void)core;
    std::fprintf(stderr, "failed to pin %s: CPU pinning is only implemented on Linux\n", name);
    return false;
#endif
}

bool pin_current_thread(int core, const char* name) noexcept {
#if defined(__linux__)
    return pin_thread(pthread_self(), core, name);
#else
    (void)core;
    std::fprintf(stderr, "failed to pin %s: CPU pinning is only implemented on Linux\n", name);
    return false;
#endif
}

void wait_once(ob::WaitMode mode) noexcept {
    ob::matching_loop_wait(mode);
}

// Producer and matcher wait here while the main thread applies pinning. If
// setup fails, they exit without touching either ring.
bool wait_for_start(const std::atomic<bool>& start_threads, const std::atomic<bool>& cancel_start,
                    ob::WaitMode wait_mode) noexcept {
    while (!start_threads.load(std::memory_order_acquire)) {
        wait_once(wait_mode);
    }
    return !cancel_start.load(std::memory_order_acquire);
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

// Stamp a command just before it enters the inbound ring. If the ring is full,
// the producer waits and retries instead of dropping the command.
void push_command(InboundRing& inbound, const ob::InboundMsg& input, ProducerStats& stats,
                  ProducerTiming* timing, ob::WaitMode wait_mode) noexcept {
    ob::InboundMsg msg = input;
    msg.tsc_ready = ob::engine_time_nanos();
    for (;;) {
        // The in-ring copy gets this pre-push stamp. The local benchmark sample
        // below uses a post-success stamp, so it is slightly more precise.
        msg.tsc_enqueue = ob::engine_time_nanos();
        if (inbound.push(msg)) {
            const std::uint64_t accepted = ob::engine_time_nanos();
            if (timing != nullptr && msg.tsc_intended != 0) {
                const std::uint64_t lateness =
                    msg.tsc_ready > msg.tsc_intended ? msg.tsc_ready - msg.tsc_intended : 0;
                timing->scheduler_lateness.push_back(lateness);
                timing->admission_wait.push_back(accepted - msg.tsc_ready);
                if (lateness > 0) {
                    ++stats.late_sends;
                }
            }
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

// Generate commands directly in the producer thread for insert/crossing-heavy
// runs. Cancel-heavy runs will use saved command files so cancels have real
// handles assigned by an earlier engine pass.
void run_live_producer(InboundRing& inbound, const Options& options, std::uint64_t start_time,
                       ProducerStats& stats, ProducerTiming& timing) {
    // Live threaded generation cannot safely learn real handles from the logger,
    // so this benchmark still disables cancels. IOC/FOK and participants are
    // included, with enough participants that self-trade stays uncommon.
    ob::synthetic::GeneratorConfig config{
        .limit_weight = 80,
        .market_weight = 20,
        .cancel_weight = 0,
        .gtc_weight = 8,
        .ioc_weight = 1,
        .fok_weight = 1,
        .participant_count = 50,
    };
    Generator generator{options.seed, config};

    const std::uint64_t total_commands = options.warmup_commands + options.commands;
    for (std::uint64_t i = 0; i < total_commands; ++i) {
        const bool measured = i >= options.warmup_commands;
        const std::uint64_t intended =
            options.closed_loop ? 0 : intended_time(start_time, i, options.rate_per_second);
        if (!options.closed_loop) {
            wait_until(intended, options.wait_mode);
        } else if (measured && timing.measured_start == 0) {
            // Closed-loop mode measures throughput only. The first measured
            // command starts the throughput window.
            timing.measured_start = ob::engine_time_nanos();
        }

        ob::InboundMsg msg = generator.next(i + 1);
        msg.tsc_intended = intended;
        ProducerTiming* sample_target = measured && !options.closed_loop ? &timing : nullptr;
        push_command(inbound, msg, stats, sample_target, options.wait_mode);
    }

    push_command(inbound, stop_command(total_commands + 1), stats, nullptr, options.wait_mode);
}

// Replay commands loaded from a saved file. This is the path used for
// cancel-heavy runs, because the file can contain real assigned handles.
void run_saved_producer(InboundRing& inbound, const Options& options, const SavedCommands& saved,
                        std::uint64_t start_time, ProducerStats& stats, ProducerTiming& timing) {
    const std::uint64_t total_commands = options.warmup_commands + options.commands;
    for (std::uint64_t i = 0; i < total_commands; ++i) {
        const bool measured = i >= options.warmup_commands;
        const std::uint64_t intended =
            options.closed_loop ? 0 : intended_time(start_time, i, options.rate_per_second);
        if (!options.closed_loop) {
            wait_until(intended, options.wait_mode);
        } else if (measured && timing.measured_start == 0) {
            timing.measured_start = ob::engine_time_nanos();
        }

        ob::InboundMsg msg = saved.commands[static_cast<std::size_t>(i)];
        // Use one continuous sequence so warmup and measured commands line up
        // with the benchmark's sample filtering.
        msg.client_seq = i + 1;
        msg.tsc_intended = intended;
        ProducerTiming* sample_target = measured && !options.closed_loop ? &timing : nullptr;
        push_command(inbound, msg, stats, sample_target, options.wait_mode);
    }

    push_command(inbound, stop_command(total_commands + 1), stats, nullptr, options.wait_mode);
}

// Drain events on the logger side and record one latency sample per completed
// command. Fill events from the same command are not counted separately.
LoggerStats run_logger(OutboundRing& outbound, std::uint64_t command_count,
                       std::uint64_t warmup_commands, std::vector<std::uint64_t>& latencies,
                       bool closed_loop, ob::WaitMode wait_mode) {
    LoggerStats stats;
    ob::OutboundEvent event{};
    const std::uint64_t measured_end = warmup_commands + command_count;
    for (;;) {
        if (!outbound.pop(event)) {
            ++stats.outbound_empty_waits;
            wait_once(wait_mode);
            continue;
        }

        ++stats.events;
        // Warmup commands still run through the engine, but their timing is not
        // included in the reported sample set.
        if (event.client_seq > warmup_commands && event.client_seq <= measured_end &&
            (event.flags & ob::RequestComplete) != 0) {
            ++stats.completed_requests;
            const std::uint64_t now = ob::engine_time_nanos();
            if (!closed_loop && event.tsc_intended != 0 && now >= event.tsc_intended) {
                latencies.push_back(now - event.tsc_intended);
            }
            if (event.client_seq == measured_end) {
                stats.measured_end = now;
            }
        }

        if (event.type == ob::EventType::StopEngine) {
            stats.saw_stop = true;
            return stats;
        }
    }
}

// Percentiles are read from sorted samples. per_mille lets us print p99.9
// without introducing floating-point formatting into the report.
std::uint64_t percentile_per_mille(const std::vector<std::uint64_t>& sorted,
                                   std::uint64_t per_mille) {
    if (sorted.empty()) {
        return 0;
    }
    const std::size_t index =
        static_cast<std::size_t>(((sorted.size() - 1) * per_mille + 999) / 1000);
    return sorted[index];
}

void print_distribution(const char* name, const std::vector<std::uint64_t>& sorted) {
    const std::uint64_t p50 = percentile_per_mille(sorted, 500);
    const std::uint64_t p99 = percentile_per_mille(sorted, 990);
    const std::uint64_t p999 = percentile_per_mille(sorted, 999);
    const std::uint64_t max = sorted.empty() ? 0 : sorted.back();

    std::printf("%s: p50=%llu p99=%llu p99.9=%llu max=%llu samples=%zu\n", name,
                static_cast<unsigned long long>(p50), static_cast<unsigned long long>(p99),
                static_cast<unsigned long long>(p999), static_cast<unsigned long long>(max),
                sorted.size());
}

int run_benchmark(const Options& options) {
    const std::uint64_t total_commands = options.warmup_commands + options.commands;
    auto saved = std::make_unique<SavedCommands>();
    if (options.input_path != nullptr) {
        if (!load_saved_commands(options.input_path, *saved)) {
            return 1;
        }
        if (saved->commands.size() < total_commands) {
            std::fprintf(stderr,
                         "input '%s' has %zu commands, but this run needs %llu including warmup\n",
                         options.input_path, saved->commands.size(),
                         static_cast<unsigned long long>(total_commands));
            return 1;
        }
    }

    auto inbound = std::make_unique<InboundRing>();
    auto outbound = std::make_unique<OutboundRing>();
    auto loop = std::make_unique<EngineLoop>(*inbound, *outbound);
    auto latencies = std::make_unique<std::vector<std::uint64_t>>();
    latencies->reserve(static_cast<std::size_t>(options.commands + 1));
    auto producer_timing = std::make_unique<ProducerTiming>();
    producer_timing->scheduler_lateness.reserve(static_cast<std::size_t>(options.commands));
    producer_timing->admission_wait.reserve(static_cast<std::size_t>(options.commands));

    ProducerStats producer_stats{};
    ob::MatchingLoopStats matching_stats{};
    LoggerStats logger_stats{};
    std::atomic<std::uint64_t> start_time{0};
    std::atomic<bool> start_threads{false};
    std::atomic<bool> cancel_start{false};

    // Start the threads first so their OS thread handles exist, then release
    // them after optional pinning has finished.
    std::thread producer{[&] {
        if (wait_for_start(start_threads, cancel_start, options.wait_mode)) {
            if (options.input_path != nullptr) {
                run_saved_producer(*inbound, options, *saved,
                                   start_time.load(std::memory_order_acquire), producer_stats,
                                   *producer_timing);
            } else {
                run_live_producer(*inbound, options, start_time.load(std::memory_order_acquire),
                                  producer_stats, *producer_timing);
            }
        }
    }};
    std::thread matcher{[&] {
        if (wait_for_start(start_threads, cancel_start, options.wait_mode)) {
            matching_stats = loop->run_until_stop(options.wait_mode);
        }
    }};

    if (has_core_assignment(options)) {
        if (!pin_thread(producer.native_handle(), options.cores.producer, "producer") ||
            !pin_thread(matcher.native_handle(), options.cores.matcher, "matcher") ||
            !pin_current_thread(options.cores.logger, "logger")) {
            cancel_start.store(true, std::memory_order_release);
            start_threads.store(true, std::memory_order_release);
            producer.join();
            matcher.join();
            return 1;
        }
    }

    // The send schedule starts after setup, so pinning work does not make the
    // first commands look late.
    start_time.store(ob::engine_time_nanos() + kStartDelayNanos, std::memory_order_release);
    start_threads.store(true, std::memory_order_release);
    logger_stats = run_logger(*outbound, options.commands, options.warmup_commands, *latencies,
                              options.closed_loop, options.wait_mode);

    producer.join();
    matcher.join();

    if (!matching_stats.stopped || !logger_stats.saw_stop) {
        std::fprintf(stderr, "benchmark did not shut down cleanly\n");
        return 1;
    }

    std::sort(latencies->begin(), latencies->end());
    std::sort(producer_timing->scheduler_lateness.begin(),
              producer_timing->scheduler_lateness.end());
    std::sort(producer_timing->admission_wait.begin(), producer_timing->admission_wait.end());

    std::printf(
        "measured_commands=%llu warmup=%llu total_commands=%llu rate_per_second=%llu "
        "seed=%llu\n",
        static_cast<unsigned long long>(options.commands),
        static_cast<unsigned long long>(options.warmup_commands),
        static_cast<unsigned long long>(total_commands),
        static_cast<unsigned long long>(options.rate_per_second),
        static_cast<unsigned long long>(options.seed));
    if (options.closed_loop) {
        std::printf("mode=closed_loop\n");
    }
    if (options.input_path != nullptr) {
        std::printf("input=%s\n", options.input_path);
    }
    if (has_core_assignment(options)) {
        std::printf("cores: producer=%d matcher=%d logger=%d\n", options.cores.producer,
                    options.cores.matcher, options.cores.logger);
    }
    std::printf("events=%llu completed=%llu latency_samples=%zu\n",
                static_cast<unsigned long long>(logger_stats.events),
                static_cast<unsigned long long>(logger_stats.completed_requests),
                latencies->size());
    if (options.closed_loop) {
        // Guard the unsigned subtraction in case shutdown failed to mark both ends.
        const std::uint64_t elapsed =
            logger_stats.measured_end > producer_timing->measured_start
                ? logger_stats.measured_end - producer_timing->measured_start
                : 0;
        // Convert measured commands over nanoseconds into commands per second.
        const std::uint64_t commands_per_second =
            elapsed == 0 ? 0
                         : static_cast<std::uint64_t>(
                               (static_cast<long double>(options.commands) * 1'000'000'000.0L) /
                               static_cast<long double>(elapsed));
        std::printf("throughput: elapsed_ns=%llu commands_per_second=%llu\n",
                    static_cast<unsigned long long>(elapsed),
                    static_cast<unsigned long long>(commands_per_second));
    } else {
        print_distribution("response_latency_ns", *latencies);
        print_distribution("scheduler_lateness_ns", producer_timing->scheduler_lateness);
        print_distribution("admission_wait_ns", producer_timing->admission_wait);
    }
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
