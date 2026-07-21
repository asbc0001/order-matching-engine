// tcp_server.cpp - Small TCP server for exercising the threaded engine.
//
// This is still a development tool, not the final production server. It accepts
// a fixed number of local clients, decodes fixed-size command records from each
// socket, runs the existing matcher pipeline, and sends fixed-size event records
// back to the participant that owns each event.

#include <algorithm>
#include <array>
#include <atomic>
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
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "orderbook/codec.hpp"
#include "orderbook/config.hpp"
#include "orderbook/event_logger.hpp"
#include "orderbook/l2_book.hpp"
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
constexpr int kListenBacklog = 16;
constexpr int kProducerPollMillis = 100;
constexpr std::uint64_t kShutdownFlushNanos = 500'000'000;
constexpr std::uint64_t kPendingClientTimeoutNanos = 1'000'000'000;

// Keep each readable socket to a small batch before returning to epoll, so one
// busy client does not dominate the producer thread.
constexpr std::size_t kMaxCommandsPerRead = 64;
constexpr std::size_t kMaxMalformedRecords = 1;
constexpr std::string_view kSpectatorHandshake = "SPECTATOR\n";

std::atomic_bool shutdown_requested{false};

struct Options {
    const char* bind_address{"127.0.0.1"};
    std::uint16_t port{kDefaultPort};
    std::size_t max_traders{kDefaultClientCount};
    std::size_t max_spectators{0};
    ob::WaitMode wait_mode{ob::WaitMode::Spin};
};

// The signal handler only records the request. Normal code does the actual
// socket cleanup, ring push, and thread joins from safe places.
void request_shutdown(int) noexcept {
    shutdown_requested.store(true, std::memory_order_release);
}

// The table has two views of the same connections:
//   fd -> participant_id for producer reads and logger write-ready events
//   participant_id -> connection for the logger response path
struct ConnectionTable {
    std::mutex mutex;
    std::unordered_map<int, ob::ParticipantId> participant_by_fd;
    std::unordered_map<ob::ParticipantId, std::unique_ptr<ob::tcp::ConnectionState>> by_participant;
    std::size_t accepted{0};
    std::size_t traders{0};
    std::size_t spectators{0};

    // Caps are stored with the table because the connection's role is only
    // known after it sends either a binary command or the spectator handshake.
    std::size_t max_traders{0};
    std::size_t max_spectators{0};
    std::size_t active_readers{0};
    bool accepting_done{false};
};

int usage(const char* program) {
    std::fprintf(stderr,
                 "usage: %s [port] [--bind IPv4] [--clients N] [--traders N] "
                 "[--spectators N] [--yield]\n",
                 program);
    return 2;
}

// Parse a TCP port without accepting trailing text such as "9001abc".
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

// Parse a count used for client-role caps.
bool parse_size(std::string_view text, std::size_t& value) {
    unsigned long long parsed = 0;
    char tail = '\0';
    if (std::sscanf(std::string{text}.c_str(), "%llu%c", &parsed, &tail) != 1) {
        return false;
    }
    value = static_cast<std::size_t>(parsed);
    return true;
}

// Keep the tool deliberately small: one optional port, an optional bind
// address, role caps, and a local-friendly wait mode.
bool parse_options(int argc, char** argv, Options& options) {
    bool saw_port = false;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg{argv[i]};
        if (arg == "--yield") {
            options.wait_mode = ob::WaitMode::Yield;
            continue;
        }
        if (arg == "--bind") {
            if (i + 1 >= argc) {
                return false;
            }
            options.bind_address = argv[++i];
            continue;
        }
        if (arg == "--clients") {
            std::size_t clients = 0;
            if (i + 1 >= argc || !parse_size(argv[++i], clients) ||
                clients > ob::config::MAX_CLIENTS) {
                return false;
            }
            options.max_traders = clients;
            options.max_spectators = 0;
            continue;
        }
        if (arg == "--traders") {
            if (i + 1 >= argc || !parse_size(argv[++i], options.max_traders)) {
                return false;
            }
            continue;
        }
        if (arg == "--spectators") {
            if (i + 1 >= argc || !parse_size(argv[++i], options.max_spectators)) {
                return false;
            }
            continue;
        }
        if (saw_port || !parse_u16(arg, options.port)) {
            return false;
        }
        saw_port = true;
    }
    return options.max_traders + options.max_spectators > 0 &&
           options.max_traders + options.max_spectators <= ob::config::MAX_CLIENTS;
}

// Create the listening socket. Binding defaults to 127.0.0.1; external access
// requires an explicit --bind address.
int make_listener(const char* bind_address, std::uint16_t port) {
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
    if (::inet_pton(AF_INET, bind_address, &addr.sin_addr) != 1) {
        std::fprintf(stderr, "bad bind address: %s\n", bind_address);
        ::close(fd);
        return -1;
    }
    addr.sin_port = htons(port);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::perror("bind");
        ::close(fd);
        return -1;
    }
    if (::listen(fd, kListenBacklog) != 0) {
        std::perror("listen");
        ::close(fd);
        return -1;
    }
    return fd;
}

// Register an fd for readable events in the producer's epoll set.
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

// Stop the engine by sending the same internal command a normal producer uses
// at EOF. The matcher then emits StopEngine on its usual outbound path.
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

bool mark_trader(ConnectionTable& table, ob::tcp::ConnectionState& connection) {
    std::lock_guard lock{table.mutex};
    if (connection.role == ob::tcp::ConnectionRole::Trader) {
        return true;
    }
    if (connection.role != ob::tcp::ConnectionRole::Pending || table.traders >= table.max_traders) {
        return false;
    }
    connection.role = ob::tcp::ConnectionRole::Trader;
    connection.spectator_handshake_possible = false;
    ++table.traders;
    return true;
}

// A spectator reads market data only. After the handshake, the producer stops
// watching it for input but leaves the fd known so the logger can write to it.
bool mark_spectator(int epoll_fd, ConnectionTable& table, ob::tcp::ConnectionState& connection) {
    (void)::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, connection.fd, nullptr);

    std::lock_guard lock{table.mutex};
    if (connection.role != ob::tcp::ConnectionRole::Pending ||
        table.spectators >= table.max_spectators) {
        return false;
    }
    connection.role = ob::tcp::ConnectionRole::Spectator;
    ++table.spectators;
    connection.read_size = 0;
    connection.snapshot_pending.store(true, std::memory_order_release);
    if (!connection.read_closed.exchange(true, std::memory_order_acq_rel) &&
        table.active_readers > 0) {
        --table.active_readers;
    }
    std::printf("client participant=%u is spectator\n", connection.participant_id);
    return table.accepting_done && table.active_readers == 0;
}

// The producer can exit once accepting is closed and every trading client has
// either disconnected or become a spectator.
bool all_readers_finished(ConnectionTable& table) {
    std::lock_guard lock{table.mutex};
    return table.accepting_done && table.active_readers == 0;
}

// Pending clients have not sent a valid command or SPECTATOR line yet. Close
// them after a short timeout so they cannot occupy a role slot forever.
bool close_timed_out_pending_clients(int epoll_fd, ConnectionTable& table) {
    std::vector<int> timed_out_fds;
    const std::uint64_t now = ob::engine_time_nanos();
    {
        std::lock_guard lock{table.mutex};
        for (const auto& [fd, participant_id] : table.participant_by_fd) {
            const auto found = table.by_participant.find(participant_id);
            if (found == table.by_participant.end()) {
                continue;
            }
            const auto& connection = *found->second;
            if (connection.role == ob::tcp::ConnectionRole::Pending &&
                now - connection.accepted_at_nanos >= kPendingClientTimeoutNanos) {
                timed_out_fds.push_back(fd);
            }
        }
    }

    bool finished = false;
    for (int fd : timed_out_fds) {
        std::fprintf(stderr, "pending client timed out\n");
        finished = mark_read_closed(epoll_fd, table, fd);
    }
    return finished;
}

// Disable Nagle's algorithm so small responses are sent promptly during local
// interactive runs.
bool set_tcp_nodelay(int fd) {
    const int yes = 1;
    if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) != 0) {
        std::perror("setsockopt(TCP_NODELAY)");
        return false;
    }
    return true;
}

// Store a newly accepted socket in both routing maps.
bool add_client(ConnectionTable& table, int client_fd, ob::ParticipantId participant_id) {
    auto connection = std::make_unique<ob::tcp::ConnectionState>(client_fd, participant_id,
                                                                 ob::engine_time_nanos());

    std::lock_guard lock{table.mutex};
    table.participant_by_fd[client_fd] = participant_id;
    table.by_participant.emplace(participant_id, std::move(connection));
    ++table.accepted;
    ++table.active_readers;
    return true;
}

// Stop listening for new clients. Existing clients may still drain normally.
void stop_accepting(int epoll_fd, int listen_fd, ConnectionTable& table) {
    (void)::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, listen_fd, nullptr);
    std::lock_guard lock{table.mutex};
    table.accepting_done = true;
}

// Accept every queued client until the kernel says there are no more ready.
// Participant ids are assigned by the server, never by fd or client input.
bool accept_ready_clients(int listen_fd, int epoll_fd, ConnectionTable& table,
                          ob::tcp::ParticipantIdSource& ids) {
    const std::size_t target_clients = table.max_traders + table.max_spectators;
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
                              ConnectionTable& table, ob::WaitMode wait_mode) {
    constexpr std::size_t record_size = ob::codec::ENCODED_INBOUND_SIZE;
    std::size_t offset = 0;
    std::size_t decoded_records = 0;

    while (connection.read_size - offset >= record_size && decoded_records < kMaxCommandsPerRead) {
        const std::span<const std::uint8_t> record{connection.read_buf.data() + offset,
                                                   record_size};
        auto decoded = ob::codec::decode_inbound(record);
        if (!decoded) {
            std::fprintf(stderr, "bad inbound record: %s\n",
                         ob::codec::decode_error_name(decoded.error));
            ++connection.malformed_records;
            return connection.malformed_records < kMaxMalformedRecords;
        }
        if (!mark_trader(table, connection)) {
            std::fprintf(stderr, "trader limit reached\n");
            return false;
        }

        ob::InboundMsg msg = decoded.value;
        // TCP identity comes from accept(), not from bytes supplied by clients.
        msg.participant_id = connection.participant_id;
        msg.tsc_intended = ob::engine_time_nanos();
        push_command(inbound, msg, wait_mode);
        offset += record_size;
        ++decoded_records;
    }

    if (offset > 0) {
        const std::size_t remaining = connection.read_size - offset;
        std::memmove(connection.read_buf.data(), connection.read_buf.data() + offset, remaining);
        connection.read_size = remaining;
    }
    return true;
}

// Detect the optional read-only client handshake. Once the buffered bytes no
// longer match "SPECTATOR\n", this connection is treated as binary input only.
bool try_read_spectator_handshake(int epoll_fd, ConnectionTable& table,
                                  ob::tcp::ConnectionState& connection, bool& all_clients_closed) {
    if (!connection.spectator_handshake_possible) {
        return false;
    }
    if (connection.read_size > kSpectatorHandshake.size()) {
        connection.spectator_handshake_possible = false;
        return false;
    }
    const std::string_view buffered{reinterpret_cast<const char*>(connection.read_buf.data()),
                                    connection.read_size};
    if (kSpectatorHandshake.substr(0, buffered.size()) != buffered) {
        connection.spectator_handshake_possible = false;
        return false;
    }
    if (buffered.size() != kSpectatorHandshake.size()) {
        return true;
    }

    if (!mark_spectator(epoll_fd, table, connection)) {
        std::fprintf(stderr, "spectator limit reached\n");
        all_clients_closed = mark_read_closed(epoll_fd, table, connection.fd);
        return true;
    }
    all_clients_closed = all_readers_finished(table);
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
            if (connection->read_buf.size() >= ob::config::READ_BUF_CAP_BYTES) {
                std::fprintf(stderr, "read buffer cap reached\n");
                all_clients_closed = mark_read_closed(epoll_fd, table, fd);
                return true;
            }
            connection->read_buf.resize(
                std::min(connection->read_buf.size() * 2, ob::config::READ_BUF_CAP_BYTES));
        }

        // Bound the amount read from this socket in one producer pass. If more
        // bytes remain in the kernel, level-triggered epoll will report the fd
        // as readable again.
        const std::size_t max_read =
            std::min(connection->read_buf.size() - connection->read_size,
                     kMaxCommandsPerRead * ob::codec::ENCODED_INBOUND_SIZE);
        const ssize_t n =
            ::recv(fd, connection->read_buf.data() + connection->read_size, max_read, 0);
        if (n > 0) {
            connection->read_size += static_cast<std::size_t>(n);
            if (try_read_spectator_handshake(epoll_fd, table, *connection, all_clients_closed)) {
                return true;
            }
            if (!decode_buffered_commands(*connection, inbound, table, wait_mode)) {
                all_clients_closed = mark_read_closed(epoll_fd, table, fd);
                return true;
            }
            if (static_cast<std::size_t>(n) == max_read) {
                return true;
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
        if (errno == ECONNRESET || errno == EBADF) {
            // A client can reset, or the logger can close a slow client while
            // the producer still has a stale read event. Either way, treat it
            // as that client's read side ending, not as a server failure.
            all_clients_closed = mark_read_closed(epoll_fd, table, fd);
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
        if (shutdown_requested.load(std::memory_order_acquire)) {
            stop_accepting(epoll_fd, listen_fd, table);
            break;
        }
        if (close_timed_out_pending_clients(epoll_fd, table)) {
            break;
        }
        if (all_readers_finished(table)) {
            break;
        }

        // A finite timeout keeps Ctrl-C responsive even if no sockets are busy
        // enough to wake epoll by themselves.
        const int count = ::epoll_wait(epoll_fd, events, kEpollEvents, kProducerPollMillis);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::perror("epoll_wait");
            ok = false;
            break;
        }
        if (count == 0) {
            continue;
        }

        for (int i = 0; i < count && !done; ++i) {
            if (events[i].data.fd == listen_fd) {
                ok = accept_ready_clients(listen_fd, epoll_fd, table, ids);
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

// Find a still-open trading client for a private matcher response.
ob::tcp::ConnectionState* connection_for_participant(ConnectionTable& table,
                                                     ob::ParticipantId participant_id) {
    std::lock_guard lock{table.mutex};
    const auto found = table.by_participant.find(participant_id);
    if (found == table.by_participant.end() ||
        found->second->closed.load(std::memory_order_acquire)) {
        return nullptr;
    }
    return found->second.get();
}

// Start watching a client only when it has buffered bytes that could not be
// written immediately.
bool add_epoll_out(int epoll_fd, ob::tcp::ConnectionState& connection) {
    if (connection.write_registered) {
        return true;
    }

    epoll_event event{};
    event.events = EPOLLOUT;
    event.data.fd = connection.fd;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, connection.fd, &event) != 0) {
        std::perror("epoll_ctl(EPOLLOUT add)");
        return false;
    }
    connection.write_registered = true;
    return true;
}

// Stop write-ready notifications once the client's pending buffer is empty.
bool remove_epoll_out(int epoll_fd, ob::tcp::ConnectionState& connection) {
    if (!connection.write_registered) {
        return true;
    }

    if (::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, connection.fd, nullptr) != 0 && errno != ENOENT) {
        std::perror("epoll_ctl(EPOLLOUT del)");
        return false;
    }
    connection.write_registered = false;
    return true;
}

// Logger owns socket close. During the run we keep the ConnectionState object
// alive because the producer may already have a non-owning pointer to its read
// buffer. Removing the fd lookup is enough to prevent stale socket use.
void close_client(ConnectionTable& table, int write_epoll_fd,
                  ob::tcp::ConnectionState& connection) {
    const int fd = connection.fd;

    (void)remove_epoll_out(write_epoll_fd, connection);
    if (fd >= 0 && !connection.closed.exchange(true, std::memory_order_acq_rel)) {
        ::close(fd);
        connection.fd = -1;
    }

    std::lock_guard lock{table.mutex};
    if (!connection.read_closed.exchange(true, std::memory_order_acq_rel) &&
        table.active_readers > 0) {
        --table.active_readers;
    }
    table.participant_by_fd.erase(fd);
}

// Try to write whatever is already buffered for this client. If the socket
// would block, keep the remaining bytes and wait for EPOLLOUT.
bool flush_client(ConnectionTable& table, int write_epoll_fd,
                  ob::tcp::ConnectionState& connection) {
    while (connection.write_offset < connection.write_buf.size()) {
        const std::size_t remaining = connection.write_buf.size() - connection.write_offset;
        const ssize_t n =
            ::send(connection.fd, connection.write_buf.data() + connection.write_offset, remaining,
                   MSG_NOSIGNAL);
        if (n > 0) {
            connection.write_offset += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return add_epoll_out(write_epoll_fd, connection);
        }

        std::perror("send");
        close_client(table, write_epoll_fd, connection);
        return true;
    }

    connection.write_buf.clear();
    connection.write_offset = 0;
    return remove_epoll_out(write_epoll_fd, connection);
}

// Queue bytes for one client and try to write them immediately. If the client
// falls too far behind, close it so memory use stays bounded.
bool append_bytes(ConnectionTable& table, int write_epoll_fd, ob::tcp::ConnectionState& connection,
                  std::span<const std::uint8_t> bytes) {
    if (connection.closed.load(std::memory_order_acquire)) {
        return true;
    }

    // Drop bytes already written before checking the cap. This keeps long-lived
    // connections from carrying dead buffer space.
    if (connection.write_offset > 0) {
        const std::size_t pending = connection.write_buf.size() - connection.write_offset;
        std::memmove(connection.write_buf.data(),
                     connection.write_buf.data() + connection.write_offset, pending);
        connection.write_buf.resize(pending);
        connection.write_offset = 0;
    }

    if (connection.write_buf.size() + bytes.size() > ob::config::WRITE_BUF_CAP_BYTES) {
        std::fprintf(stderr, "closing slow client participant=%u\n", connection.participant_id);
        close_client(table, write_epoll_fd, connection);
        return true;
    }

    connection.write_buf.insert(connection.write_buf.end(), bytes.begin(), bytes.end());
    return flush_client(table, write_epoll_fd, connection);
}

// Private execution reports use the fixed-size binary event codec.
bool append_event(ConnectionTable& table, int write_epoll_fd, ob::tcp::ConnectionState& connection,
                  const ob::codec::EncodedOutbound& bytes) {
    return append_bytes(table, write_epoll_fd, connection,
                        std::span<const std::uint8_t>{bytes.data(), bytes.size()});
}

bool append_text(ConnectionTable& table, int write_epoll_fd, ob::tcp::ConnectionState& connection,
                 std::string_view text) {
    return append_bytes(table, write_epoll_fd, connection,
                        std::span<const std::uint8_t>{
                            reinterpret_cast<const std::uint8_t*>(text.data()), text.size()});
}

// Market-data clients receive simple text lines for now. The line reports the
// new total quantity at the price level changed by this event.
bool broadcast_l2_line(ConnectionTable& table, int write_epoll_fd, const ob::OutboundEvent& event,
                       const ob::L2Book& l2_book, std::uint64_t l2_sequence) {
    std::array<char, 96> line{};
    const int written = std::snprintf(
        line.data(), line.size(), "L2 seq=%llu side=%s price=%lld qty=%llu\n",
        static_cast<unsigned long long>(l2_sequence), ob::side_name(event.side),
        static_cast<long long>(event.price),
        static_cast<unsigned long long>(l2_book.quantity_at(event.side, event.price)));
    if (written <= 0 || static_cast<std::size_t>(written) >= line.size()) {
        return false;
    }

    std::vector<ob::tcp::ConnectionState*> spectators;
    {
        std::lock_guard lock{table.mutex};
        for (auto& [_, connection] : table.by_participant) {
            if (connection->role == ob::tcp::ConnectionRole::Spectator &&
                !connection->closed.load(std::memory_order_acquire)) {
                spectators.push_back(connection.get());
            }
        }
    }

    bool ok = true;
    for (ob::tcp::ConnectionState* spectator : spectators) {
        ok = append_text(table, write_epoll_fd, *spectator,
                         std::string_view{line.data(), static_cast<std::size_t>(written)}) &&
             ok;
    }
    return ok;
}

// Write the current visible depth for one side of the book.
bool append_snapshot_side(ConnectionTable& table, int write_epoll_fd,
                          ob::tcp::ConnectionState& spectator, const ob::L2Book& l2_book,
                          ob::Side side, std::uint64_t l2_sequence) {
    bool ok = true;
    for (const ob::L2Level& level : l2_book.levels(side)) {
        std::array<char, 96> line{};
        const int written = std::snprintf(
            line.data(), line.size(), "SNAPSHOT seq=%llu side=%s price=%lld qty=%llu\n",
            static_cast<unsigned long long>(l2_sequence), ob::side_name(side),
            static_cast<long long>(level.price), static_cast<unsigned long long>(level.qty));
        if (written <= 0 || static_cast<std::size_t>(written) >= line.size()) {
            return false;
        }
        ok = append_text(table, write_epoll_fd, spectator,
                         std::string_view{line.data(), static_cast<std::size_t>(written)}) &&
             ok;
    }
    return ok;
}

// Send a complete text snapshot to a newly subscribed spectator.
bool append_l2_snapshot(ConnectionTable& table, int write_epoll_fd,
                        ob::tcp::ConnectionState& spectator, const ob::L2Book& l2_book,
                        std::uint64_t l2_sequence) {
    std::array<char, 64> begin{};
    int written = std::snprintf(begin.data(), begin.size(), "SNAPSHOT_BEGIN seq=%llu\n",
                                static_cast<unsigned long long>(l2_sequence));
    if (written <= 0 || static_cast<std::size_t>(written) >= begin.size()) {
        return false;
    }

    bool ok = append_text(table, write_epoll_fd, spectator,
                          std::string_view{begin.data(), static_cast<std::size_t>(written)});
    ok = append_snapshot_side(table, write_epoll_fd, spectator, l2_book, ob::Side::Bid,
                              l2_sequence) &&
         ok;
    ok = append_snapshot_side(table, write_epoll_fd, spectator, l2_book, ob::Side::Ask,
                              l2_sequence) &&
         ok;

    std::array<char, 64> end{};
    written = std::snprintf(end.data(), end.size(), "SNAPSHOT_END seq=%llu\n",
                            static_cast<unsigned long long>(l2_sequence));
    if (written <= 0 || static_cast<std::size_t>(written) >= end.size()) {
        return false;
    }
    ok = append_text(table, write_epoll_fd, spectator,
                     std::string_view{end.data(), static_cast<std::size_t>(written)}) &&
         ok;
    return ok;
}

// New spectators are marked by the producer, but only the logger has the L2
// state and write ownership, so it sends snapshots from here.
bool send_pending_snapshots(ConnectionTable& table, int write_epoll_fd, const ob::L2Book& l2_book,
                            std::uint64_t l2_sequence) {
    std::vector<ob::tcp::ConnectionState*> spectators;
    {
        std::lock_guard lock{table.mutex};
        for (auto& [_, connection] : table.by_participant) {
            if (connection->role == ob::tcp::ConnectionRole::Spectator &&
                !connection->closed.load(std::memory_order_acquire) &&
                connection->snapshot_pending.exchange(false, std::memory_order_acq_rel)) {
                spectators.push_back(connection.get());
            }
        }
    }

    bool ok = true;
    for (ob::tcp::ConnectionState* spectator : spectators) {
        ok = append_l2_snapshot(table, write_epoll_fd, *spectator, l2_book, l2_sequence) && ok;
    }
    return ok;
}

// Only events that change resting visible quantity should publish L2 updates.
bool changes_l2(const ob::OutboundEvent& event) noexcept {
    return event.type == ob::EventType::AckNew || event.type == ob::EventType::AckCancel ||
           (event.type == ob::EventType::Fill && event.handle != 0);
}

// EPOLLOUT reports only an fd, so look it back up before flushing bytes.
ob::tcp::ConnectionState* connection_for_write_fd(ConnectionTable& table, int fd) {
    std::lock_guard lock{table.mutex};
    const auto id = table.participant_by_fd.find(fd);
    if (id == table.participant_by_fd.end()) {
        return nullptr;
    }
    const auto connection = table.by_participant.find(id->second);
    return connection == table.by_participant.end() ? nullptr : connection->second.get();
}

// Trading clients close after their reads end and pending replies drain.
// Spectators are write-only after their handshake, so read_closed is normal for
// them and does not mean their socket should be closed.
void close_drained_readers(ConnectionTable& table, int write_epoll_fd) {
    for (;;) {
        ob::tcp::ConnectionState* to_close = nullptr;
        {
            std::lock_guard lock{table.mutex};
            for (auto& [_, connection] : table.by_participant) {
                if (connection->read_closed.load(std::memory_order_acquire) &&
                    connection->role != ob::tcp::ConnectionRole::Spectator &&
                    connection->write_buf.empty() &&
                    !connection->closed.load(std::memory_order_acquire)) {
                    to_close = connection.get();
                    break;
                }
            }
        }

        if (to_close == nullptr) {
            return;
        }
        close_client(table, write_epoll_fd, *to_close);
    }
}

// Service clients whose sockets are ready for more outgoing bytes.
bool flush_writable_clients(ConnectionTable& table, int write_epoll_fd) {
    epoll_event events[kEpollEvents]{};
    const int count = ::epoll_wait(write_epoll_fd, events, kEpollEvents, 1);
    if (count < 0) {
        if (errno == EINTR) {
            return true;
        }
        std::perror("epoll_wait(EPOLLOUT)");
        return false;
    }

    bool ok = true;
    for (int i = 0; i < count; ++i) {
        ob::tcp::ConnectionState* connection = connection_for_write_fd(table, events[i].data.fd);
        if (connection == nullptr) {
            continue;
        }
        ok = flush_client(table, write_epoll_fd, *connection) && ok;
    }
    return ok;
}

// Used during shutdown to decide whether a brief final flush is still useful.
bool has_pending_writes(ConnectionTable& table) {
    std::lock_guard lock{table.mutex};
    for (const auto& [_, connection] : table.by_participant) {
        if (!connection->closed.load(std::memory_order_acquire) && !connection->write_buf.empty()) {
            return true;
        }
    }
    return false;
}

bool flush_before_shutdown(ConnectionTable& table, int write_epoll_fd) {
    bool ok = true;
    const std::uint64_t deadline = ob::engine_time_nanos() + kShutdownFlushNanos;

    // StopEngine means no more events will arrive. Give already-buffered
    // responses a short chance to leave before closing the client sockets.
    while (has_pending_writes(table) && ob::engine_time_nanos() < deadline) {
        ok = flush_writable_clients(table, write_epoll_fd) && ok;
        close_drained_readers(table, write_epoll_fd);
    }
    return ok;
}

// Close every socket from the logger side. Called only after StopEngine, when
// no more private events or L2 updates will be generated.
void close_all_clients(ConnectionTable& table, int write_epoll_fd) {
    std::vector<ob::tcp::ConnectionState*> connections;
    {
        std::lock_guard lock{table.mutex};
        connections.reserve(table.by_participant.size());
        for (auto& [_, connection] : table.by_participant) {
            connections.push_back(connection.get());
        }
    }

    for (ob::tcp::ConnectionState* connection : connections) {
        close_client(table, write_epoll_fd, *connection);
    }
}

// Logger thread: route private binary events to traders and publish L2 text
// updates to spectators. It owns all socket writes and closes.
bool run_logger(OutboundRing& outbound, ConnectionTable& table) {
    const int write_epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
    if (write_epoll_fd < 0) {
        std::perror("epoll_create1(EPOLLOUT)");
        return false;
    }

    ob::OutboundEvent event{};
    ob::L2Book l2_book;
    std::uint64_t l2_sequence = 0;
    bool ok = true;
    for (;;) {
        if (!outbound.pop(event)) {
            ok = send_pending_snapshots(table, write_epoll_fd, l2_book, l2_sequence) && ok;
            ok = flush_writable_clients(table, write_epoll_fd) && ok;
            close_drained_readers(table, write_epoll_fd);
            continue;
        }

        // A snapshot uses the last completed L2 sequence. Sending pending
        // snapshots before this event avoids including this change twice.
        ok = send_pending_snapshots(table, write_epoll_fd, l2_book, l2_sequence) && ok;

        if (event.type == ob::EventType::StopEngine) {
            close_drained_readers(table, write_epoll_fd);
            ok = flush_before_shutdown(table, write_epoll_fd) && ok;
            close_all_clients(table, write_epoll_fd);
            ::close(write_epoll_fd);
            return ok;
        }

        const bool publish_l2 = changes_l2(event);
        if (publish_l2) {
            l2_book.apply(event);
            ++l2_sequence;
            ok = broadcast_l2_line(table, write_epoll_fd, event, l2_book, l2_sequence) && ok;
        }

        ob::tcp::ConnectionState* connection =
            connection_for_participant(table, event.participant_id);
        if (connection == nullptr) {
            continue;
        }

        const auto bytes = ob::codec::encode_outbound(event);
        ok = append_event(table, write_epoll_fd, *connection, bytes) && ok;
    }
}

int run_server(const Options& options) {
    shutdown_requested.store(false, std::memory_order_release);
    ::signal(SIGPIPE, SIG_IGN);
    ::signal(SIGINT, request_shutdown);

    const int listen_fd = make_listener(options.bind_address, options.port);
    if (listen_fd < 0) {
        return 1;
    }
    std::printf("listening on %s:%u for %zu trader(s) and %zu spectator(s)\n", options.bind_address,
                options.port, options.max_traders, options.max_spectators);
    std::printf("limits: read_buf_cap=%zu write_buf_cap=%zu commands_per_read=%zu\n",
                ob::config::READ_BUF_CAP_BYTES, ob::config::WRITE_BUF_CAP_BYTES,
                kMaxCommandsPerRead);

    auto inbound = std::make_unique<InboundRing>();
    auto outbound = std::make_unique<OutboundRing>();
    auto loop = std::make_unique<EngineLoop>(*inbound, *outbound);
    ConnectionTable table;
    table.max_traders = options.max_traders;
    table.max_spectators = options.max_spectators;
    ob::MatchingLoopStats matching_stats{};
    bool producer_ok = false;

    std::thread producer{[&] { producer_ok = run_producer(listen_fd, table, *inbound, options); }};
    std::thread matcher{[&] { matching_stats = loop->run_until_stop(options.wait_mode); }};

    const bool logger_ok = run_logger(*outbound, table);

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
