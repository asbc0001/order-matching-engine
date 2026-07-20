// tcp_server.cpp - Minimal one-client TCP proof for the threaded engine.
//
// This is not the final multi-client server. It proves the basic path first:
// encoded socket command -> inbound ring -> matcher -> outbound ring -> socket
// response. The producer reads; the logger writes and owns close().

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "orderbook/codec.hpp"
#include "orderbook/config.hpp"
#include "orderbook/matching_loop.hpp"
#include "orderbook/spsc_ring.hpp"
#include "orderbook/tcp/connection.hpp"
#include "orderbook/time.hpp"

namespace {

using InboundRing = ob::SpscRing<ob::InboundMsg, ob::config::kProduction.ring_capacity>;
using OutboundRing = ob::SpscRing<ob::OutboundEvent, ob::config::kProduction.ring_capacity>;
using EngineLoop =
    ob::MatchingLoop<ob::config::kProduction.num_levels, ob::config::kProduction.pool_capacity,
                     ob::config::kProduction.ring_capacity, ob::config::kProduction.ring_capacity>;

constexpr std::uint16_t kDefaultPort = 9001;
constexpr int kEpollEvents = 4;

struct Options {
    std::uint16_t port{kDefaultPort};
    ob::WaitMode wait_mode{ob::WaitMode::Spin};
};

struct SharedConnection {
    std::mutex mutex;
    ob::tcp::ConnectionState state;
};

// Keep this proof tool intentionally small: one optional port plus --yield.
int usage(const char* program) {
    std::fprintf(stderr, "usage: %s [port] [--yield]\n", program);
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
    bool saw_port = false;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg{argv[i]};
        if (arg == "--yield") {
            options.wait_mode = ob::WaitMode::Yield;
            continue;
        }
        if (saw_port || !parse_port(arg, options.port)) {
            return false;
        }
        saw_port = true;
    }
    return true;
}

// Create a loopback-only listener. This is for local development and tests, not
// for accepting public network traffic.
int make_listener(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) {
        std::perror("socket");
        return -1;
    }

    const int yes = 1;
    if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) != 0) {
        std::perror("setsockopt(SO_REUSEADDR)");
        ::close(fd);
        return -1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::perror("bind");
        ::close(fd);
        return -1;
    }
    if (::listen(fd, 16) != 0) {
        std::perror("listen");
        ::close(fd);
        return -1;
    }
    return fd;
}

// The producer thread watches sockets for readable bytes. The final server will
// add many client fds here; this proof only adds the listener and then one fd.
bool add_epoll_in(int epoll_fd, int fd) {
    epoll_event event{};
    event.events = EPOLLIN;
    event.data.fd = fd;
    return ::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event) == 0;
}

// Push into the engine's inbound ring without dropping commands. A full ring is
// backpressure inside the process, so this waits until the matcher catches up.
void push_command(InboundRing& inbound, const ob::InboundMsg& input,
                  ob::WaitMode wait_mode) noexcept {
    ob::InboundMsg msg = input;
    msg.tsc_ready = ob::engine_time_nanos();
    for (;;) {
        msg.tsc_enqueue = ob::engine_time_nanos();
        if (inbound.push(msg)) {
            return;
        }
        ob::matching_loop_wait(wait_mode);
    }
}

void push_stop(InboundRing& inbound, ob::WaitMode wait_mode) noexcept {
    const std::uint64_t now = ob::engine_time_nanos();
    push_command(inbound,
                 ob::InboundMsg{
                     .client_seq = 0,
                     .handle = 0,
                     .price = 0,
                     .qty = 0,
                     .side = ob::Side::Bid,
                     .type = ob::MsgType::StopEngine,
                     .tsc_intended = now,
                     .tsc_ready = now,
                     .tsc_enqueue = now,
                 },
                 wait_mode);
}

// The producer does not close the socket. It only stops watching reads and
// tells the logger that input has ended, preserving one close owner.
void mark_read_closed(int epoll_fd, SharedConnection& shared) {
    std::lock_guard lock{shared.mutex};
    if (shared.state.fd >= 0) {
        (void)::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, shared.state.fd, nullptr);
    }
    shared.state.read_closed.store(true, std::memory_order_release);
}

int accepted_fd(const SharedConnection& shared) {
    return shared.state.fd;
}

bool accept_one_client(int listen_fd, int epoll_fd, SharedConnection& shared,
                       ob::tcp::ParticipantIdSource& ids) {
    sockaddr_in addr{};
    socklen_t addr_len = sizeof(addr);
    const int client_fd =
        ::accept4(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len, SOCK_NONBLOCK);
    if (client_fd < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            std::perror("accept4");
        }
        return false;
    }

    const int yes = 1;
    if (::setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) != 0) {
        std::perror("setsockopt(TCP_NODELAY)");
        ::close(client_fd);
        return false;
    }

    {
        std::lock_guard lock{shared.mutex};
        shared.state.fd = client_fd;
        shared.state.participant_id = ids.take();
        shared.state.read_buf.assign(ob::config::READ_BUF_INITIAL_BYTES, 0);
        shared.state.read_size = 0;
        shared.state.write_buf.reserve(ob::config::WRITE_BUF_CAP_BYTES);
    }

    if (!add_epoll_in(epoll_fd, client_fd)) {
        std::perror("epoll_ctl(client)");
        ::close(client_fd);
        return false;
    }

    // This is the deliberate one-client limit. The multi-client version should
    // keep listening and store each accepted client in a connection table.
    (void)::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, listen_fd, nullptr);
    std::printf("accepted client participant=%u\n", shared.state.participant_id);
    return true;
}

// Decode every complete fixed-size command already buffered. Any leftover bytes
// stay at the front of read_buf for the next recv(), which is what lets this
// handle TCP fragmentation.
bool decode_buffered_commands(SharedConnection& shared, InboundRing& inbound,
                              ob::WaitMode wait_mode) {
    ob::tcp::ConnectionState& connection = shared.state;
    constexpr std::size_t record_size = ob::codec::ENCODED_INBOUND_SIZE;
    std::size_t offset = 0;

    while (connection.read_size - offset >= record_size) {
        const std::span<const std::uint8_t> record{connection.read_buf.data() + offset,
                                                   record_size};
        auto decoded = ob::codec::decode_inbound(record);
        if (!decoded) {
            std::fprintf(stderr, "bad inbound record: %s\n",
                         ob::codec::decode_error_name(decoded.error));
            return false;
        }

        ob::InboundMsg msg = decoded.value;
        // TCP assigns identity from the accepted connection, not from client
        // bytes. That keeps self-trade and response routing under server control.
        msg.participant_id = connection.participant_id;
        msg.tsc_intended = ob::engine_time_nanos();
        push_command(inbound, msg, wait_mode);
        offset += record_size;
    }

    if (offset > 0) {
        const std::size_t remaining = connection.read_size - offset;
        std::memmove(connection.read_buf.data(), connection.read_buf.data() + offset, remaining);
        connection.read_size = remaining;
    }
    return true;
}

// Drain currently-readable bytes from the accepted client. Level-triggered
// epoll will wake us again if more bytes remain later.
bool read_client_commands(int epoll_fd, SharedConnection& shared, InboundRing& inbound,
                          ob::WaitMode wait_mode) {
    ob::tcp::ConnectionState& connection = shared.state;
    for (;;) {
        if (connection.read_size == connection.read_buf.size()) {
            connection.read_buf.resize(connection.read_buf.size() * 2);
        }

        const ssize_t n = ::recv(connection.fd, connection.read_buf.data() + connection.read_size,
                                 connection.read_buf.size() - connection.read_size, 0);
        if (n > 0) {
            connection.read_size += static_cast<std::size_t>(n);
            if (!decode_buffered_commands(shared, inbound, wait_mode)) {
                mark_read_closed(epoll_fd, shared);
                push_stop(inbound, wait_mode);
                return false;
            }
            continue;
        }
        if (n == 0) {
            mark_read_closed(epoll_fd, shared);
            push_stop(inbound, wait_mode);
            return true;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return true;
        }

        std::perror("recv");
        mark_read_closed(epoll_fd, shared);
        push_stop(inbound, wait_mode);
        return false;
    }
}

bool run_producer(int listen_fd, SharedConnection& shared, InboundRing& inbound,
                  ob::WaitMode wait_mode) {
    const int epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd < 0) {
        std::perror("epoll_create1");
        push_stop(inbound, wait_mode);
        return false;
    }
    if (!add_epoll_in(epoll_fd, listen_fd)) {
        std::perror("epoll_ctl(listen)");
        ::close(epoll_fd);
        push_stop(inbound, wait_mode);
        return false;
    }

    ob::tcp::ParticipantIdSource ids;
    epoll_event events[kEpollEvents]{};
    bool ok = true;
    bool done = false;
    while (!done) {
        const int count = ::epoll_wait(epoll_fd, events, kEpollEvents, -1);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::perror("epoll_wait");
            push_stop(inbound, wait_mode);
            ok = false;
            break;
        }

        for (int i = 0; i < count && !done; ++i) {
            if (events[i].data.fd == listen_fd) {
                ok = accept_one_client(listen_fd, epoll_fd, shared, ids);
                if (!ok) {
                    push_stop(inbound, wait_mode);
                    done = true;
                }
            } else {
                ok = read_client_commands(epoll_fd, shared, inbound, wait_mode);
                if (shared.state.read_closed.load(std::memory_order_acquire)) {
                    done = true;
                }
            }
        }
    }

    ::close(epoll_fd);
    return ok;
}

int current_client_fd(SharedConnection& shared) {
    std::lock_guard lock{shared.mutex};
    return accepted_fd(shared);
}

// Proof-only writer: send this one response before processing the next event.
// The final multi-client logger needs bounded per-client buffers plus EPOLLOUT
// so a slow reader cannot delay the outbound ring drain.
bool write_all(int fd, std::span<const std::uint8_t> bytes, ob::WaitMode wait_mode) {
    std::size_t written = 0;
    while (written < bytes.size()) {
        const ssize_t n = ::send(fd, bytes.data() + written, bytes.size() - written, 0);
        if (n > 0) {
            written += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            ob::matching_loop_wait(wait_mode);
            continue;
        }
        std::perror("send");
        return false;
    }
    return true;
}

// Logger owns close(). That remains true even in this one-client proof, because
// it is the rule that prevents fd reuse bugs once multiple clients exist.
void logger_close_client(SharedConnection& shared) {
    std::lock_guard lock{shared.mutex};
    if (shared.state.fd >= 0 && !shared.state.closed.exchange(true, std::memory_order_acq_rel)) {
        ::close(shared.state.fd);
        shared.state.fd = -1;
    }
}

// Logger thread side: consume matcher events and write them back to the single
// accepted client. Routing by participant_id comes in the multi-client version.
bool run_logger(OutboundRing& outbound, SharedConnection& shared, ob::WaitMode wait_mode) {
    ob::OutboundEvent event{};
    bool ok = true;
    for (;;) {
        if (!outbound.pop(event)) {
            ob::matching_loop_wait(wait_mode);
            continue;
        }

        if (event.type == ob::EventType::StopEngine) {
            logger_close_client(shared);
            return ok;
        }

        const int fd = current_client_fd(shared);
        if (fd < 0) {
            ok = false;
            continue;
        }

        const auto bytes = ob::codec::encode_outbound(event);
        if (!write_all(fd, bytes, wait_mode)) {
            ok = false;
            logger_close_client(shared);
        }
    }
}

int run_server(const Options& options) {
    ::signal(SIGPIPE, SIG_IGN);

    const int listen_fd = make_listener(options.port);
    if (listen_fd < 0) {
        return 1;
    }
    std::printf("listening on 127.0.0.1:%u\n", options.port);

    auto inbound = std::make_unique<InboundRing>();
    auto outbound = std::make_unique<OutboundRing>();
    auto loop = std::make_unique<EngineLoop>(*inbound, *outbound);
    SharedConnection shared;
    ob::MatchingLoopStats matching_stats{};
    bool producer_ok = false;

    std::thread producer{
        [&] { producer_ok = run_producer(listen_fd, shared, *inbound, options.wait_mode); }};
    std::thread matcher{[&] { matching_stats = loop->run_until_stop(options.wait_mode); }};

    const bool logger_ok = run_logger(*outbound, shared, options.wait_mode);

    producer.join();
    matcher.join();
    ::close(listen_fd);

    if (!producer_ok || !logger_ok || !matching_stats.stopped) {
        std::fprintf(stderr, "tcp_server did not shut down cleanly\n");
        return 1;
    }

    std::printf("processed=%llu emitted=%llu\n",
                static_cast<unsigned long long>(matching_stats.processed),
                static_cast<unsigned long long>(matching_stats.emitted));
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    if (!parse_options(argc, argv, options)) {
        return usage(argv[0]);
    }
    return run_server(options);
}
