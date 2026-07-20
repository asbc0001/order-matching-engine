// tcp_server.cpp - Small TCP server for exercising the threaded engine.
//
// This is still a development tool, not the final production server. It accepts
// a fixed number of local clients, decodes fixed-size command records from each
// socket, runs the existing matcher pipeline, and sends fixed-size event records
// back to the participant that owns each event.

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
#include <unordered_map>

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
constexpr std::size_t kDefaultClientCount = 1;
constexpr int kEpollEvents = 16;

struct Options {
    std::uint16_t port{kDefaultPort};
    std::size_t clients{kDefaultClientCount};
    ob::WaitMode wait_mode{ob::WaitMode::Spin};
};

// The table has two views of the same connections:
//   fd -> participant_id for the producer read path
//   participant_id -> connection for the logger response path
struct ConnectionTable {
    std::mutex mutex;
    std::unordered_map<int, ob::ParticipantId> participant_by_fd;
    std::unordered_map<ob::ParticipantId, std::unique_ptr<ob::tcp::ConnectionState>> by_participant;
    std::size_t accepted{0};
    std::size_t active_readers{0};
    bool accepting_done{false};
};

int usage(const char* program) {
    std::fprintf(stderr, "usage: %s [port] [--clients N] [--yield]\n", program);
    return 2;
}

bool parse_u16(std::string_view text, std::uint16_t& value) {
    unsigned parsed = 0;
    char tail = '\0';
    if (std::sscanf(std::string{text}.c_str(), "%u%c", &parsed, &tail) != 1 || parsed == 0 ||
        parsed > 65535) {
        return false;
    }
    value = static_cast<std::uint16_t>(parsed);
    return true;
}

bool parse_size(std::string_view text, std::size_t& value) {
    unsigned long long parsed = 0;
    char tail = '\0';
    if (std::sscanf(std::string{text}.c_str(), "%llu%c", &parsed, &tail) != 1 || parsed == 0) {
        return false;
    }
    value = static_cast<std::size_t>(parsed);
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
        if (arg == "--clients") {
            if (i + 1 >= argc || !parse_size(argv[++i], options.clients) ||
                options.clients > ob::config::MAX_CLIENTS) {
                return false;
            }
            continue;
        }
        if (saw_port || !parse_u16(arg, options.port)) {
            return false;
        }
        saw_port = true;
    }
    return true;
}

// Create a loopback-only listener. This keeps the tool local while still using
// real TCP sockets and the kernel's normal stream behavior.
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

bool add_epoll_in(int epoll_fd, int fd) {
    epoll_event event{};
    event.events = EPOLLIN;
    event.data.fd = fd;
    return ::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event) == 0;
}

// Push into the engine without dropping commands. If the ring is temporarily
// full, the producer waits until the matcher catches up.
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

// epoll gives the producer an fd. The connection itself is stored by
// participant_id so the logger can route events the same way.
ob::tcp::ConnectionState* connection_for_fd(ConnectionTable& table, int fd) {
    std::lock_guard lock{table.mutex};
    const auto id = table.participant_by_fd.find(fd);
    if (id == table.participant_by_fd.end()) {
        return nullptr;
    }
    const auto connection = table.by_participant.find(id->second);
    return connection == table.by_participant.end() ? nullptr : connection->second.get();
}

// Producer-side close handling: stop watching reads and mark the connection.
// The actual close still belongs to the logger side.
bool mark_read_closed(int epoll_fd, ConnectionTable& table, int fd) {
    (void)::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);

    std::lock_guard lock{table.mutex};
    const auto id = table.participant_by_fd.find(fd);
    if (id == table.participant_by_fd.end()) {
        return table.accepting_done && table.active_readers == 0;
    }

    auto& connection = *table.by_participant.at(id->second);
    if (!connection.read_closed.exchange(true, std::memory_order_acq_rel)) {
        --table.active_readers;
    }
    table.participant_by_fd.erase(id);
    return table.accepting_done && table.active_readers == 0;
}

bool set_tcp_nodelay(int fd) {
    const int yes = 1;
    if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) != 0) {
        std::perror("setsockopt(TCP_NODELAY)");
        return false;
    }
    return true;
}

bool add_client(ConnectionTable& table, int client_fd, ob::ParticipantId participant_id) {
    auto connection = std::make_unique<ob::tcp::ConnectionState>(client_fd, participant_id);

    std::lock_guard lock{table.mutex};
    table.participant_by_fd[client_fd] = participant_id;
    table.by_participant.emplace(participant_id, std::move(connection));
    ++table.accepted;
    ++table.active_readers;
    return true;
}

void stop_accepting(int epoll_fd, int listen_fd, ConnectionTable& table) {
    (void)::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, listen_fd, nullptr);
    std::lock_guard lock{table.mutex};
    table.accepting_done = true;
}

// Accept every queued client until the kernel says there are no more ready.
// Participant ids are assigned by the server, never by fd or client input.
bool accept_ready_clients(int listen_fd, int epoll_fd, ConnectionTable& table,
                          ob::tcp::ParticipantIdSource& ids, std::size_t target_clients) {
    for (;;) {
        if (table.accepted >= target_clients) {
            stop_accepting(epoll_fd, listen_fd, table);
            return true;
        }

        sockaddr_in addr{};
        socklen_t addr_len = sizeof(addr);
        const int client_fd =
            ::accept4(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len, SOCK_NONBLOCK);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return true;
            }
            std::perror("accept4");
            return false;
        }

        if (!set_tcp_nodelay(client_fd)) {
            ::close(client_fd);
            return false;
        }
        const ob::ParticipantId participant_id = ids.take();
        if (participant_id == 0) {
            std::fprintf(stderr, "participant id space exhausted\n");
            ::close(client_fd);
            return false;
        }
        if (!add_epoll_in(epoll_fd, client_fd)) {
            std::perror("epoll_ctl(client)");
            ::close(client_fd);
            return false;
        }
        add_client(table, client_fd, participant_id);
        std::printf("accepted client participant=%u\n", participant_id);
    }
}

// Decode every complete fixed-size command already buffered. Any leftover bytes
// stay at the front of read_buf for the next recv(), which handles fragmented
// TCP input naturally.
bool decode_buffered_commands(ob::tcp::ConnectionState& connection, InboundRing& inbound,
                              ob::WaitMode wait_mode) {
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
        // TCP identity comes from accept(), not from bytes supplied by clients.
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

// Drain the currently-readable bytes for one client. Level-triggered epoll will
// wake us again if more bytes remain later.
bool read_client_commands(int epoll_fd, ConnectionTable& table, int fd, InboundRing& inbound,
                          ob::WaitMode wait_mode, bool& all_clients_closed) {
    ob::tcp::ConnectionState* connection = connection_for_fd(table, fd);
    if (connection == nullptr) {
        return true;
    }

    for (;;) {
        if (connection->read_size == connection->read_buf.size()) {
            connection->read_buf.resize(connection->read_buf.size() * 2);
        }

        const ssize_t n = ::recv(fd, connection->read_buf.data() + connection->read_size,
                                 connection->read_buf.size() - connection->read_size, 0);
        if (n > 0) {
            connection->read_size += static_cast<std::size_t>(n);
            if (!decode_buffered_commands(*connection, inbound, wait_mode)) {
                all_clients_closed = mark_read_closed(epoll_fd, table, fd);
                return false;
            }
            continue;
        }
        if (n == 0) {
            all_clients_closed = mark_read_closed(epoll_fd, table, fd);
            return true;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return true;
        }

        std::perror("recv");
        all_clients_closed = mark_read_closed(epoll_fd, table, fd);
        return false;
    }
}

bool run_producer(int listen_fd, ConnectionTable& table, InboundRing& inbound,
                  const Options& options) {
    const int epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd < 0) {
        std::perror("epoll_create1");
        push_stop(inbound, options.wait_mode);
        return false;
    }
    if (!add_epoll_in(epoll_fd, listen_fd)) {
        std::perror("epoll_ctl(listen)");
        ::close(epoll_fd);
        push_stop(inbound, options.wait_mode);
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
            ok = false;
            break;
        }

        for (int i = 0; i < count && !done; ++i) {
            if (events[i].data.fd == listen_fd) {
                ok = accept_ready_clients(listen_fd, epoll_fd, table, ids, options.clients);
                if (!ok) {
                    done = true;
                }
            } else {
                bool all_clients_closed = false;
                ok = read_client_commands(epoll_fd, table, events[i].data.fd, inbound,
                                          options.wait_mode, all_clients_closed);
                done = all_clients_closed || !ok;
            }
        }
    }

    ::close(epoll_fd);
    push_stop(inbound, options.wait_mode);
    return ok;
}

int fd_for_participant(ConnectionTable& table, ob::ParticipantId participant_id) {
    std::lock_guard lock{table.mutex};
    const auto found = table.by_participant.find(participant_id);
    if (found == table.by_participant.end() ||
        found->second->closed.load(std::memory_order_acquire)) {
        return -1;
    }
    return found->second->fd;
}

// Temporary writer for this stage: route one event and write it immediately.
// The next TCP slice replaces this with bounded buffers plus EPOLLOUT so slow
// clients cannot delay the logger.
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

// Logger owns socket close. The producer has already removed closed readers
// from its epoll set before StopEngine is sent, so closing here cannot race with
// producer reads in the normal shutdown path.
void close_all_clients(ConnectionTable& table) {
    std::lock_guard lock{table.mutex};
    for (auto& [_, connection] : table.by_participant) {
        if (connection->fd >= 0 && !connection->closed.exchange(true, std::memory_order_acq_rel)) {
            ::close(connection->fd);
            connection->fd = -1;
        }
    }
}

bool run_logger(OutboundRing& outbound, ConnectionTable& table, ob::WaitMode wait_mode) {
    ob::OutboundEvent event{};
    bool ok = true;
    for (;;) {
        if (!outbound.pop(event)) {
            ob::matching_loop_wait(wait_mode);
            continue;
        }

        if (event.type == ob::EventType::StopEngine) {
            close_all_clients(table);
            return ok;
        }

        const int fd = fd_for_participant(table, event.participant_id);
        if (fd < 0) {
            ok = false;
            continue;
        }

        const auto bytes = ob::codec::encode_outbound(event);
        ok = write_all(fd, bytes, wait_mode) && ok;
    }
}

int run_server(const Options& options) {
    ::signal(SIGPIPE, SIG_IGN);

    const int listen_fd = make_listener(options.port);
    if (listen_fd < 0) {
        return 1;
    }
    std::printf("listening on 127.0.0.1:%u for %zu client(s)\n", options.port, options.clients);

    auto inbound = std::make_unique<InboundRing>();
    auto outbound = std::make_unique<OutboundRing>();
    auto loop = std::make_unique<EngineLoop>(*inbound, *outbound);
    ConnectionTable table;
    ob::MatchingLoopStats matching_stats{};
    bool producer_ok = false;

    std::thread producer{[&] { producer_ok = run_producer(listen_fd, table, *inbound, options); }};
    std::thread matcher{[&] { matching_stats = loop->run_until_stop(options.wait_mode); }};

    const bool logger_ok = run_logger(*outbound, table, options.wait_mode);

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
