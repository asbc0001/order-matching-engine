// text_to_saved_commands.cpp - Convert readable command lines to saved commands.

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "orderbook/codec.hpp"
#include "orderbook/text_trace_parser.hpp"
#include "orderbook/trace_replay.hpp"

namespace {

// Write one complete binary chunk. Saved command files are fixed-format, so a
// short write means the output file cannot be trusted.
bool write_bytes(std::FILE* file, const std::uint8_t* bytes, std::size_t size, const char* what) {
    if (std::fwrite(bytes, 1, size, file) != size) {
        std::fprintf(stderr, "failed to write %s\n", what);
        return false;
    }
    return true;
}

// Keep the CLI deliberately small: this tool only converts one readable input
// file into one saved binary command file.
int usage(const char* program) {
    std::fprintf(stderr, "usage: %s <input.txt> <output.commands>\n", program);
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        return usage(argv[0]);
    }

    std::FILE* input = std::fopen(argv[1], "r");
    if (input == nullptr) {
        std::fprintf(stderr, "failed to open input '%s': %s\n", argv[1], std::strerror(errno));
        return 1;
    }

    std::FILE* output = std::fopen(argv[2], "wb");
    if (output == nullptr) {
        std::fprintf(stderr, "failed to open output '%s': %s\n", argv[2], std::strerror(errno));
        std::fclose(input);
        return 1;
    }

    // The replay side refuses files without this header, so every saved command
    // file starts by identifying its format and version.
    const auto header = ob::trace::make_inbound_trace_header();
    if (!write_bytes(output, header.data(), header.size(), "saved-command header")) {
        std::fclose(output);
        std::fclose(input);
        return 1;
    }

    char* line = nullptr;
    std::size_t capacity = 0;
    std::uint64_t client_seq = 1;
    std::uint64_t line_number = 0;

    // Each input line is expected to contain one command. The parser validates
    // the text shape; the codec writes the stable binary record.
    while (getline(&line, &capacity, input) != -1) {
        ++line_number;
        const ob::trace::ParseResult parsed = ob::trace::parse_line(client_seq, line);
        if (!parsed) {
            std::fprintf(stderr, "parse error at line %llu: %s\n",
                         static_cast<unsigned long long>(line_number),
                         ob::trace::parse_error_name(parsed.error));
            std::free(line);
            std::fclose(output);
            std::fclose(input);
            return 1;
        }

        const auto encoded = ob::codec::encode_inbound(parsed.value);
        if (!write_bytes(output, encoded.data(), encoded.size(), "command record")) {
            std::free(line);
            std::fclose(output);
            std::fclose(input);
            return 1;
        }
        ++client_seq;
    }

    if (std::ferror(input) != 0) {
        std::fprintf(stderr, "failed while reading input '%s'\n", argv[1]);
        std::free(line);
        std::fclose(output);
        std::fclose(input);
        return 1;
    }

    std::free(line);
    std::fclose(output);
    std::fclose(input);
    std::printf("wrote %llu command records to %s\n",
                static_cast<unsigned long long>(client_seq - 1), argv[2]);
    return 0;
}
