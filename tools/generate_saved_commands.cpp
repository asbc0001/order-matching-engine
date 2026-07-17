// generate_saved_commands.cpp - Create seeded saved command files.
//
// The generator runs commands through the matcher as it writes them. Observing
// AckNew/Fill/AckCancel events lets future generated CANCEL commands reference
// real handles that are live at that point in the stream.

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string_view>

#include "orderbook/codec.hpp"
#include "orderbook/config.hpp"
#include "orderbook/matcher.hpp"
#include "orderbook/synthetic_generator.hpp"
#include "orderbook/trace_replay.hpp"

namespace {

using ToolMatcher =
    ob::Matcher<ob::config::kProduction.num_levels, ob::config::kProduction.pool_capacity>;
using ToolGenerator = ob::synthetic::CommandGenerator<ob::config::kProduction.num_levels, 65'536>;

// Command count and seed are decimal CLI arguments. Reject partial parses such
// as "100abc" rather than silently accepting the prefix.
bool parse_u64(const char* text, std::uint64_t& value) {
    errno = 0;
    char* end = nullptr;
    const unsigned long long parsed = std::strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        return false;
    }

    value = static_cast<std::uint64_t>(parsed);
    return true;
}

// Write one complete binary chunk. Saved command files are fixed-format, so a
// short write means the output file cannot be trusted.
bool write_bytes(std::FILE* file, const std::uint8_t* bytes, std::size_t size, const char* what) {
    if (std::fwrite(bytes, 1, size, file) != size) {
        std::fprintf(stderr, "failed to write %s\n", what);
        return false;
    }
    return true;
}

// Keep the interface minimal: output path, number of commands, seed, and an
// optional workload mode fully define a repeatable generated command file.
int usage(const char* program) {
    std::fprintf(stderr, "usage: %s <output.commands> <command_count> <seed> [--cancel-heavy]\n",
                 program);
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 4 && argc != 5) {
        return usage(argv[0]);
    }

    std::uint64_t command_count = 0;
    std::uint64_t seed = 0;
    if (!parse_u64(argv[2], command_count) || command_count == 0 || !parse_u64(argv[3], seed)) {
        return usage(argv[0]);
    }

    ob::synthetic::GeneratorConfig config;
    if (argc == 5) {
        if (std::string_view{argv[4]} != "--cancel-heavy") {
            return usage(argv[0]);
        }
        // Bias toward cancels while still generating enough new limits for the
        // generator to learn real live handles from matcher output.
        config = ob::synthetic::GeneratorConfig{
            .limit_weight = 50,
            .market_weight = 0,
            .cancel_weight = 50,
            .valid_cancels_only = true,
        };
    }

    std::FILE* output = std::fopen(argv[1], "wb");
    if (output == nullptr) {
        std::fprintf(stderr, "failed to open output '%s': %s\n", argv[1], std::strerror(errno));
        return 1;
    }

    // The replay side validates this header before reading command records.
    const auto header = ob::trace::make_inbound_trace_header();
    if (!write_bytes(output, header.data(), header.size(), "saved-command header")) {
        std::fclose(output);
        return 1;
    }

    auto matcher = std::make_unique<ToolMatcher>();
    ToolGenerator generator{seed, config};

    // The matcher run is only for handle discovery. The written file still
    // contains commands, not matcher output.
    for (std::uint64_t seq = 1; seq <= command_count; ++seq) {
        const ob::InboundMsg command = generator.next(seq);
        const auto encoded = ob::codec::encode_inbound(command);
        if (!write_bytes(output, encoded.data(), encoded.size(), "command record")) {
            std::fclose(output);
            return 1;
        }

        // Feed matcher output back into the generator so future cancels can use
        // handles assigned by earlier accepted orders.
        auto observe = [&](const ob::OutboundEvent& event) noexcept { generator.observe(event); };
        matcher->process(command, observe);
    }

    std::fclose(output);
    std::printf("wrote %llu generated command records to %s\n",
                static_cast<unsigned long long>(command_count), argv[1]);
    return 0;
}
