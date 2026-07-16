// run_engine.cpp - Run saved commands through the threaded order-book engine.
//
// This is an end-to-end runtime tool: a producer replays a saved command
// file into the inbound ring, the matching thread processes commands, and the
// logger side drains outbound events into one selected output mode.

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

#include "orderbook/book_dump.hpp"
#include "orderbook/config.hpp"
#include "orderbook/demo_renderer.hpp"
#include "orderbook/event_logger.hpp"
#include "orderbook/matching_loop.hpp"
#include "orderbook/time.hpp"
#include "orderbook/trace_replay.hpp"

namespace {

using InboundRing = ob::SpscRing<ob::InboundMsg, ob::config::kProduction.ring_capacity>;
using OutboundRing = ob::SpscRing<ob::OutboundEvent, ob::config::kProduction.ring_capacity>;
using EngineLoop =
    ob::MatchingLoop<ob::config::kProduction.num_levels, ob::config::kProduction.pool_capacity,
                     ob::config::kProduction.ring_capacity, ob::config::kProduction.ring_capacity>;
using Renderer = ob::DemoRenderer<65'536>;

enum class OutputMode {
    Memory,
    CanonicalLog,
    Demo,
};

struct CoreConfig {
    bool enabled{false};
    int producer{-1};
    int matcher{-1};
    int logger{-1};
};

struct Options {
    const char* input_path{nullptr};
    const char* log_path{nullptr};
    const char* book_path{nullptr};
    OutputMode output_mode{OutputMode::Memory};
    ob::WaitMode wait_mode{ob::WaitMode::Spin};
    CoreConfig cores{};
};

struct LoggerStats {
    std::uint64_t dequeued{0};
    std::uint64_t outbound_empty_waits{0};
    std::uint64_t full_response_ns_total{0};
    bool saw_stop{false};
    bool output_ok{true};
};

int usage(const char* program) {
    std::fprintf(stderr,
                 "usage: %s <saved.commands> [--memory|--log <file>|--demo] [--yield] "
                 "[--cores producer,matcher,logger] [--book <file>]\n",
                 program);
    return 2;
}

bool parse_core_triple(std::string_view text, CoreConfig& cores) {
    int producer = -1;
    int matcher = -1;
    int logger = -1;
    char tail = '\0';
    if (std::sscanf(std::string{text}.c_str(), "%d,%d,%d%c", &producer, &matcher, &logger, &tail) !=
            3 ||
        producer < 0 || matcher < 0 || logger < 0) {
        return false;
    }

    cores = CoreConfig{
        .enabled = true,
        .producer = producer,
        .matcher = matcher,
        .logger = logger,
    };
    return true;
}

// Parse only the options this runner actually supports: one saved command file,
// one output mode, an optional yield mode, and optional CPU numbers for the
// three threads.
bool parse_options(int argc, char** argv, Options& options) {
    if (argc < 2) {
        return false;
    }
    options.input_path = argv[1];

    for (int i = 2; i < argc; ++i) {
        const std::string_view arg{argv[i]};
        if (arg == "--memory") {
            options.output_mode = OutputMode::Memory;
            options.log_path = nullptr;
        } else if (arg == "--demo") {
            options.output_mode = OutputMode::Demo;
            options.log_path = nullptr;
        } else if (arg == "--yield") {
            options.wait_mode = ob::WaitMode::Yield;
        } else if (arg == "--log") {
            if (i + 1 >= argc) {
                return false;
            }
            options.output_mode = OutputMode::CanonicalLog;
            options.log_path = argv[++i];
        } else if (arg == "--cores") {
            if (i + 1 >= argc || !parse_core_triple(argv[++i], options.cores)) {
                return false;
            }
        } else if (arg == "--book") {
            if (i + 1 >= argc) {
                return false;
            }
            options.book_path = argv[++i];
        } else {
            return false;
        }
    }
    return true;
}

// Try to keep the current thread on one requested CPU. If this fails, the run
// can still finish, but the program exits nonzero so the user knows the thread
// placement request was not honored.
bool pin_current_thread(int core, const char* role) {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    const int rc = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    if (rc != 0) {
        std::fprintf(stderr, "failed to pin %s thread to core %d: %s\n", role, core,
                     std::strerror(rc));
        return false;
    }
    return true;
#else
    (void)core;
    std::fprintf(stderr, "--cores is only supported on Linux for now (%s thread)\n", role);
    return false;
#endif
}

ob::trace::ReplayWaitMode replay_wait_mode(ob::WaitMode mode) {
    return mode == ob::WaitMode::Yield ? ob::trace::ReplayWaitMode::Yield
                                       : ob::trace::ReplayWaitMode::Spin;
}

void wait_for_output(ob::WaitMode mode) noexcept {
    ob::matching_loop_wait(mode);
}

// If the input file is bad, trace replay may stop before it sends the normal
// StopEngine command. Send one here anyway so the matcher and logger threads
// have a clear "no more commands are coming" message.
void push_internal_stop(InboundRing& inbound, ob::WaitMode wait_mode) noexcept {
    ob::InboundMsg stop{
        .client_seq = 0,
        .handle = 0,
        .price = 0,
        .qty = 0,
        .side = ob::Side::Bid,
        .type = ob::MsgType::StopEngine,
        .tsc_intended = ob::engine_time_nanos(),
        .tsc_ready = ob::engine_time_nanos(),
        .tsc_enqueue = 0,
    };

    for (;;) {
        stop.tsc_enqueue = ob::engine_time_nanos();
        if (inbound.push(stop)) {
            return;
        }
        ob::matching_loop_wait(wait_mode);
    }
}

// Pop events from the outbound ring until StopEngine arrives. The local time
// read here is not stored in the event; it is only used to report a simple
// response-time counter for this run.
template <typename Sink>
LoggerStats drain_output(OutboundRing& outbound, Sink& sink, ob::WaitMode wait_mode) {
    LoggerStats stats;
    ob::OutboundEvent event{};
    for (;;) {
        if (!outbound.pop(event)) {
            ++stats.outbound_empty_waits;
            wait_for_output(wait_mode);
            continue;
        }

        ++stats.dequeued;
        const std::uint64_t dequeued_at = ob::engine_time_nanos();
        if (event.tsc_intended != 0 && dequeued_at >= event.tsc_intended) {
            stats.full_response_ns_total += dequeued_at - event.tsc_intended;
        }

        if (!sink(event)) {
            stats.output_ok = false;
        }
        if (event.type == ob::EventType::StopEngine) {
            stats.saw_stop = true;
            return stats;
        }
    }
}

// Print the event that caused the next book redraw. DemoRenderer only draws the
// book state, so this keeps the "what just happened" text near the CLI code.
bool write_demo_event_header(const ob::OutboundEvent& event) noexcept {
    if (event.type == ob::EventType::Reject) {
        return std::printf("\nEVENT %s seq=%llu side=%s qty=%u reason=%s\n",
                           ob::event_type_name(event.type),
                           static_cast<unsigned long long>(event.client_seq),
                           ob::side_name(event.side), static_cast<unsigned>(event.qty),
                           ob::reject_reason_name(event.reason)) >= 0;
    }

    return std::printf("\nEVENT %s seq=%llu side=%s price=%lld qty=%u handle=%llu\n",
                       ob::event_type_name(event.type),
                       static_cast<unsigned long long>(event.client_seq), ob::side_name(event.side),
                       static_cast<long long>(event.price), static_cast<unsigned>(event.qty),
                       static_cast<unsigned long long>(event.handle)) >= 0;
}

struct DemoSink {
    Renderer& renderer;

    bool operator()(const ob::OutboundEvent& event) noexcept {
        if (!renderer.apply(event)) {
            return false;
        }
        if (event.type == ob::EventType::StopEngine) {
            return true;
        }
        return write_demo_event_header(event) && renderer.render(stdout);
    }
};

struct MemorySinkAdapter {
    ob::MemoryEventSink& sink;

    bool operator()(const ob::OutboundEvent& event) noexcept {
        sink(event);
        return true;
    }
};

bool write_final_book(const EngineLoop& loop, const char* path) {
    if (path == nullptr) {
        return true;
    }

    std::FILE* file = std::fopen(path, "wb");
    if (file == nullptr) {
        std::fprintf(stderr, "failed to open book dump '%s': %s\n", path, std::strerror(errno));
        return false;
    }

    // The matching thread has already joined before this is called, so reading
    // the book here has no concurrent writer.
    const bool ok = ob::dump_book(loop.matcher().book(), file);
    const bool close_ok = std::fclose(file) == 0;
    if (!ok || !close_ok) {
        std::fprintf(stderr, "failed to write book dump '%s'\n", path);
        return false;
    }
    return true;
}

int run_engine(const Options& options) {
    // These objects contain large fixed arrays. Allocate them once here instead
    // of putting them on a thread stack.
    auto inbound = std::make_unique<InboundRing>();
    auto outbound = std::make_unique<OutboundRing>();
    auto loop = std::make_unique<EngineLoop>(*inbound, *outbound);

    ob::trace::TraceReplayResult replay_result{};
    ob::MatchingLoopStats matching_stats{};
    std::atomic<bool> pinning_ok{true};

    // Producer thread: read saved commands from disk, turn bytes into InboundMsg
    // values, timestamp them, and push them to the matcher.
    std::thread producer{[&] {
        if (options.cores.enabled && !pin_current_thread(options.cores.producer, "producer")) {
            pinning_ok.store(false, std::memory_order_relaxed);
        }
        replay_result = ob::trace::replay_trace_file(options.input_path, *inbound,
                                                     replay_wait_mode(options.wait_mode));
        if (replay_result.error != ob::trace::ReplayError::None) {
            push_internal_stop(*inbound, options.wait_mode);
        }
    }};

    // Matching thread: pop commands, run the matcher, timestamp each result,
    // and push those events to the logger side.
    std::thread matcher{[&] {
        if (options.cores.enabled && !pin_current_thread(options.cores.matcher, "matcher")) {
            pinning_ok.store(false, std::memory_order_relaxed);
        }
        matching_stats = loop->run_until_stop(options.wait_mode);
    }};

    if (options.cores.enabled && !pin_current_thread(options.cores.logger, "logger")) {
        pinning_ok.store(false, std::memory_order_relaxed);
    }

    LoggerStats logger_stats{};
    ob::MemoryEventSink memory_sink;
    Renderer renderer;
    std::FILE* log_file = nullptr;

    // Main thread: act as the logger/output thread. StopEngine is the final
    // event, so once it is seen there is nothing left to drain.
    if (options.output_mode == OutputMode::CanonicalLog) {
        log_file = std::fopen(options.log_path, "wb");
        if (log_file == nullptr) {
            std::fprintf(stderr, "failed to open log '%s': %s\n", options.log_path,
                         std::strerror(errno));
            producer.join();
            matcher.join();
            return 1;
        }
        ob::CanonicalEventLog log_sink{log_file};
        logger_stats = drain_output(*outbound, log_sink, options.wait_mode);
        logger_stats.output_ok = logger_stats.output_ok && log_sink.ok();
        std::fclose(log_file);
    } else if (options.output_mode == OutputMode::Demo) {
        DemoSink demo_sink{renderer};
        logger_stats = drain_output(*outbound, demo_sink, options.wait_mode);
    } else {
        MemorySinkAdapter memory_adapter{memory_sink};
        logger_stats = drain_output(*outbound, memory_adapter, options.wait_mode);
    }

    producer.join();
    matcher.join();

    if (replay_result.error != ob::trace::ReplayError::None) {
        std::fprintf(stderr, "replay failed: %s at record %llu byte %llu\n",
                     ob::trace::replay_error_name(replay_result.error),
                     static_cast<unsigned long long>(replay_result.record_index),
                     static_cast<unsigned long long>(replay_result.byte_offset));
        return 1;
    }
    if (!matching_stats.stopped || !logger_stats.saw_stop || !logger_stats.output_ok) {
        std::fprintf(stderr, "engine did not shut down cleanly\n");
        return 1;
    }
    if (!pinning_ok.load(std::memory_order_relaxed)) {
        std::fprintf(stderr, "engine completed, but at least one requested core pin failed\n");
        return 1;
    }
    if (!write_final_book(*loop, options.book_path)) {
        return 1;
    }

    const ob::MemoryLogStats& memory = memory_sink.stats();
    const std::uint64_t avg_response_ns =
        logger_stats.dequeued == 0 ? 0
                                   : logger_stats.full_response_ns_total / logger_stats.dequeued;
    std::printf("commands=%llu events=%llu matched=%llu emitted=%llu avg_response_ns=%llu\n",
                static_cast<unsigned long long>(replay_result.records_replayed),
                static_cast<unsigned long long>(logger_stats.dequeued),
                static_cast<unsigned long long>(matching_stats.processed),
                static_cast<unsigned long long>(matching_stats.emitted),
                static_cast<unsigned long long>(avg_response_ns));
    std::printf(
        "waits: inbound_full=%llu inbound_empty=%llu outbound_full=%llu outbound_empty=%llu\n",
        static_cast<unsigned long long>(replay_result.inbound_full_waits),
        static_cast<unsigned long long>(matching_stats.inbound_empty_waits),
        static_cast<unsigned long long>(matching_stats.outbound_full_waits),
        static_cast<unsigned long long>(logger_stats.outbound_empty_waits));

    if (options.output_mode == OutputMode::Memory) {
        std::printf("memory_log: completed=%llu fills=%llu rejects=%llu stops=%llu\n",
                    static_cast<unsigned long long>(memory.completed_requests),
                    static_cast<unsigned long long>(memory.fills),
                    static_cast<unsigned long long>(memory.rejects),
                    static_cast<unsigned long long>(memory.stop_events));
    }

    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    if (!parse_options(argc, argv, options)) {
        return usage(argv[0]);
    }
    return run_engine(options);
}
