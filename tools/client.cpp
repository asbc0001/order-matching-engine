// client.cpp - Tiny text client for the TCP proof server.
//
// Lines typed on stdin use the same grammar as saved command files. In trader
// mode, one thread sends commands while another prints server responses as they
// arrive. In spectator mode, the client only prints L2 market-data lines.

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "orderbook/codec.hpp"
#include "orderbook/event_logger.hpp"
#include "orderbook/text_trace_parser.hpp"

namespace {

constexpr const char* kDefaultHost = "127.0.0.1";
constexpr std::uint16_t kDefaultPort = 9001;

struct Options {
    const char* host{kDefaultHost};
    std::uint16_t port{kDefaultPort};
    bool spectator{false};
};

// The client is a small manual tool, so positional host/port are enough.
int usage(const char* program) {
    std::fprintf(stderr, "usage: %s [--spectator] [host] [port]\n", program);
    return 2;
}

bool parse_port(std::string_view text, std::uint16_t& port) {
    unsigned parsed = 0;
    char tail = '\0';
    if (std::sscanf(std::string{text}.c_str(), "%u%c", &parsed, &tail) != 1 || parsed == 0 ||
        parsed > 65535) {
        return false;
    }
    port = static_cast<std::uint16_t>(parsed);
    return true;
}

bool parse_options(int argc, char** argv, Options& options) {
    if (argc > 3) {
        if (argc != 4 || std::string_view{argv[1]} != "--spectator") {
            return false;
        }
        options.spectator = true;
        options.host = argv[2];
        return parse_port(argv[3], options.port);
    }

    int next = 1;
    if (argc >= 2 && std::string_view{argv[1]} == "--spectator") {
        options.spectator = true;
        next = 2;
    }
    if (argc > next) {
        options.host = argv[next++];
    }
    if (argc > next && !parse_port(argv[next++], options.port)) {
        return false;
    }
    return next == argc;
}

// Connect to the proof server. The server listens on IPv4 loopback only, so the
// client keeps address handling equally small.
int connect_to_server(const Options& options) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::perror("socket");
        return -1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(options.port);
    if (::inet_pton(AF_INET, options.host, &addr.sin_addr) != 1) {
        std::fprintf(stderr, "bad IPv4 address: %s\n", options.host);
        ::close(fd);
        return -1;
    }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::perror("connect");
        ::close(fd);
        return -1;
    }
    return fd;
}

// Write the whole fixed-size command record before accepting the next command.
bool write_all(int fd, std::span<const std::uint8_t> bytes) {
    std::size_t written = 0;
    while (written < bytes.size()) {
        const ssize_t n = ::send(fd, bytes.data() + written, bytes.size() - written, 0);
        if (n > 0) {
            written += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        std::perror("send");
        return false;
    }
    return true;
}

// The wire format has fixed-size records. Reads can still arrive split across
// TCP packets, so keep reading until one whole encoded event is available.
bool read_exact(int fd, std::span<std::uint8_t> bytes) {
    std::size_t read = 0;
    while (read < bytes.size()) {
        const ssize_t n = ::recv(fd, bytes.data() + read, bytes.size() - read, 0);
        if (n > 0) {
            read += static_cast<std::size_t>(n);
            continue;
        }
        if (n == 0) {
            return false;
        }
        if (errno == EINTR) {
            continue;
        }
        std::perror("recv");
        return false;
    }
    return true;
}

// Print the same logical fields as the canonical log. Timing/internal routing
// fields are intentionally omitted so the output is easy to read.
bool print_event(const ob::OutboundEvent& event) {
    const int complete = (event.flags & ob::RequestComplete) != 0 ? 1 : 0;
    return std::printf(
               "seq=%llu event=%s side=%s price=%lld qty=%u handle=%llu reason=%s "
               "complete=%d\n",
               static_cast<unsigned long long>(event.client_seq), ob::event_type_name(event.type),
               ob::side_name(event.side), static_cast<long long>(event.price),
               static_cast<unsigned>(event.qty), static_cast<unsigned long long>(event.handle),
               ob::reject_reason_name(event.reason), complete) >= 0;
}

// Trading responses can arrive at any time, including fills for old resting
// orders. Keep this reader running while the main thread accepts stdin.
void read_trading_events(int fd, std::atomic_bool& ok) {
    for (;;) {
        ob::codec::EncodedOutbound bytes{};
        if (!read_exact(fd, bytes)) {
            return;
        }

        auto decoded = ob::codec::decode_outbound(bytes);
        if (!decoded) {
            std::fprintf(stderr, "bad outbound record: %s\n",
                         ob::codec::decode_error_name(decoded.error));
            ok.store(false, std::memory_order_release);
            return;
        }
        if (!print_event(decoded.value)) {
            ok.store(false, std::memory_order_release);
            return;
        }
    }
}

// Spectators receive text market-data lines and do not send trading commands.
int run_spectator(const Options& options) {
    const int fd = connect_to_server(options);
    if (fd < 0) {
        return 1;
    }

    constexpr std::string_view handshake = "SPECTATOR\n";
    if (!write_all(
            fd, std::span<const std::uint8_t>{
                    reinterpret_cast<const std::uint8_t*>(handshake.data()), handshake.size()})) {
        ::close(fd);
        return 1;
    }

    std::array<char, 512> buffer{};
    for (;;) {
        const ssize_t n = ::recv(fd, buffer.data(), buffer.size() - 1, 0);
        if (n > 0) {
            buffer[static_cast<std::size_t>(n)] = '\0';
            std::fputs(buffer.data(), stdout);
            continue;
        }
        if (n == 0) {
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        std::perror("recv");
        ::close(fd);
        return 1;
    }

    ::close(fd);
    return 0;
}

bool is_blank(std::string_view line) {
    for (char ch : line) {
        if (ch != ' ' && ch != '\t' && ch != '\r' && ch != '\n') {
            return false;
        }
    }
    return true;
}

int run_client(const Options& options) {
    const int fd = connect_to_server(options);
    if (fd < 0) {
        return 1;
    }

    std::atomic_bool reader_ok{true};
    std::thread reader{[&] { read_trading_events(fd, reader_ok); }};

    std::string line;
    std::uint64_t seq = 1;
    while (std::getline(std::cin, line)) {
        if (is_blank(line)) {
            continue;
        }

        auto parsed = ob::trace::parse_line(seq, line);
        if (!parsed) {
            std::fprintf(stderr, "parse error on seq %llu: %s\n",
                         static_cast<unsigned long long>(seq),
                         ob::trace::parse_error_name(parsed.error));
            ::close(fd);
            return 1;
        }

        const auto bytes = ob::codec::encode_inbound(parsed.value);
        if (!reader_ok.load(std::memory_order_acquire) || !write_all(fd, bytes)) {
            (void)::shutdown(fd, SHUT_RDWR);
            reader.join();
            ::close(fd);
            return 1;
        }
        ++seq;
    }

    // EOF on stdin means no more commands. Closing the write side lets the
    // server push StopEngine internally, flush responses, and close cleanly.
    (void)::shutdown(fd, SHUT_WR);
    reader.join();
    ::close(fd);
    return reader_ok.load(std::memory_order_acquire) ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    if (!parse_options(argc, argv, options)) {
        return usage(argv[0]);
    }
    if (options.spectator) {
        return run_spectator(options);
    }
    return run_client(options);
}
