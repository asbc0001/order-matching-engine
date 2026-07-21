// tcp_pipeline_test.cpp - Small end-to-end checks for the TCP server.
//
// These tests start the real tcp_server tool, connect over loopback, and send
// encoded binary command records. They cover TCP behavior that unit tests cannot:
// fragmented input and a client that stops reading responses.

#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>

#include "orderbook/codec.hpp"
#include "orderbook/types.hpp"

namespace {

constexpr std::uint16_t kFragmentPort = 19101;
constexpr std::uint16_t kSlowClientPort = 19102;
constexpr std::uint16_t kSpectatorPort = 19103;
constexpr std::uint16_t kSnapshotPort = 19104;
constexpr std::uint16_t kMalformedPort = 19105;
constexpr std::uint16_t kPendingTimeoutPort = 19106;

struct ServerProcess {
    pid_t pid{-1};

    ~ServerProcess() {
        if (pid > 0) {
            (void)::kill(pid, SIGTERM);
            (void)::waitpid(pid, nullptr, 0);
        }
    }

    bool wait_for_exit() {
        if (pid <= 0) {
            return false;
        }
        int status = 0;
        const pid_t waited = ::waitpid(pid, &status, 0);
        pid = -1;
        return waited > 0 && WIFEXITED(status) && WEXITSTATUS(status) == 0;
    }

    bool wait_for_exit_for(std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            int status = 0;
            const pid_t waited = ::waitpid(pid, &status, WNOHANG);
            if (waited == pid) {
                pid = -1;
                return WIFEXITED(status) && WEXITSTATUS(status) == 0;
            }
            if (waited < 0) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{20});
        }
        return false;
    }
};

bool send_all(int fd, const std::uint8_t* data, std::size_t size) {
    std::size_t offset = 0;
    while (offset < size) {
        const ssize_t n = ::send(fd, data + offset, size - offset, MSG_NOSIGNAL);
        if (n > 0) {
            offset += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

bool recv_all(int fd, std::uint8_t* data, std::size_t size) {
    std::size_t offset = 0;
    while (offset < size) {
        const ssize_t n = ::recv(fd, data + offset, size - offset, 0);
        if (n > 0) {
            offset += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

ServerProcess start_server(std::string_view server_path, std::uint16_t port,
                           std::size_t traders = 1, std::size_t spectators = 0) {
    ServerProcess server;
    server.pid = ::fork();
    if (server.pid == 0) {
        const std::string port_text = std::to_string(port);
        const std::string traders_text = std::to_string(traders);
        const std::string spectators_text = std::to_string(spectators);
        ::execl(server_path.data(), server_path.data(), port_text.c_str(), "--traders",
                traders_text.c_str(), "--spectators", spectators_text.c_str(), "--yield",
                static_cast<char*>(nullptr));
        std::_Exit(127);
    }
    return server;
}

int connect_with_retry(std::uint16_t port, int receive_buffer_bytes = 0) {
    for (int attempt = 0; attempt < 100; ++attempt) {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            return -1;
        }
        if (receive_buffer_bytes > 0) {
            (void)::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &receive_buffer_bytes,
                               sizeof(receive_buffer_bytes));
        }

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = htons(port);
        if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
            return fd;
        }

        ::close(fd);
        std::this_thread::sleep_for(std::chrono::milliseconds{20});
    }
    return -1;
}

ob::codec::EncodedInbound limit_bid(std::uint64_t client_seq) {
    return ob::codec::encode_inbound(ob::InboundMsg{
        .client_seq = client_seq,
        .handle = 0,
        .price = 100,
        .qty = 1,
        .side = ob::Side::Bid,
        .type = ob::MsgType::NewLimit,
        .tsc_intended = 0,
        .tsc_ready = 0,
        .tsc_enqueue = 0,
    });
}

ob::codec::EncodedInbound limit_ask(std::uint64_t client_seq, ob::Price price, ob::Qty qty) {
    return ob::codec::encode_inbound(ob::InboundMsg{
        .client_seq = client_seq,
        .handle = 0,
        .price = price,
        .qty = qty,
        .side = ob::Side::Ask,
        .type = ob::MsgType::NewLimit,
        .tsc_intended = 0,
        .tsc_ready = 0,
        .tsc_enqueue = 0,
    });
}

ob::codec::EncodedInbound market_bid(std::uint64_t client_seq, ob::Qty qty) {
    return ob::codec::encode_inbound(ob::InboundMsg{
        .client_seq = client_seq,
        .handle = 0,
        .price = 0,
        .qty = qty,
        .side = ob::Side::Bid,
        .type = ob::MsgType::NewMarket,
        .tsc_intended = 0,
        .tsc_ready = 0,
        .tsc_enqueue = 0,
    });
}

ob::codec::EncodedInbound cancel_order(std::uint64_t client_seq, ob::Handle handle) {
    return ob::codec::encode_inbound(ob::InboundMsg{
        .client_seq = client_seq,
        .handle = handle,
        .price = 0,
        .qty = 0,
        .side = ob::Side::Bid,
        .type = ob::MsgType::Cancel,
        .tsc_intended = 0,
        .tsc_ready = 0,
        .tsc_enqueue = 0,
    });
}

bool send_record(int fd, const ob::codec::EncodedInbound& record) {
    return send_all(fd, record.data(), record.size());
}

bool read_event(int fd, ob::OutboundEvent& event) {
    ob::codec::EncodedOutbound encoded{};
    if (!recv_all(fd, encoded.data(), encoded.size())) {
        return false;
    }
    const auto decoded = ob::codec::decode_outbound(encoded);
    if (!decoded) {
        return false;
    }
    event = decoded.value;
    return true;
}

bool read_until_contains(int fd, std::string_view expected) {
    std::string collected;
    std::array<char, 128> buffer{};
    for (int attempt = 0; attempt < 20; ++attempt) {
        const ssize_t n = ::recv(fd, buffer.data(), buffer.size() - 1, 0);
        if (n <= 0) {
            return false;
        }
        buffer[static_cast<std::size_t>(n)] = '\0';
        collected.append(buffer.data(), static_cast<std::size_t>(n));
        if (collected.find(expected) != std::string::npos) {
            return true;
        }
    }
    return false;
}

bool read_snapshot_containing(int fd, std::string_view expected_level) {
    std::string collected;
    std::array<char, 128> buffer{};
    for (int attempt = 0; attempt < 20; ++attempt) {
        const ssize_t n = ::recv(fd, buffer.data(), buffer.size() - 1, 0);
        if (n <= 0) {
            return false;
        }
        buffer[static_cast<std::size_t>(n)] = '\0';
        collected.append(buffer.data(), static_cast<std::size_t>(n));
        if (collected.find("SNAPSHOT_END seq=") != std::string::npos) {
            return collected.find(expected_level) != std::string::npos;
        }
    }
    return false;
}

bool fragmented_record_is_decoded(std::string_view server_path) {
    auto server = start_server(server_path, kFragmentPort);
    const int fd = connect_with_retry(kFragmentPort);
    if (fd < 0) {
        std::fprintf(stderr, "could not connect to fragmented-input server\n");
        return false;
    }

    const auto record = limit_bid(1);

    // TCP is a byte stream: the server must wait until all 64 bytes have
    // arrived, even if the command is split across several sends.
    if (!send_all(fd, record.data(), 5) || !send_all(fd, record.data() + 5, 17) ||
        !send_all(fd, record.data() + 22, record.size() - 22)) {
        std::fprintf(stderr, "fragmented send failed\n");
        ::close(fd);
        return false;
    }

    ob::codec::EncodedOutbound outbound{};
    if (!recv_all(fd, outbound.data(), outbound.size())) {
        std::fprintf(stderr, "did not receive response to fragmented command\n");
        ::close(fd);
        return false;
    }
    const auto decoded = ob::codec::decode_outbound(outbound);
    ::close(fd);

    if (!server.wait_for_exit()) {
        std::fprintf(stderr, "fragmented-input server did not exit cleanly\n");
        return false;
    }
    return decoded && decoded.value.type == ob::EventType::AckNew &&
           (decoded.value.flags & ob::RequestComplete) != 0;
}

bool slow_client_is_disconnected(std::string_view server_path) {
    auto server = start_server(server_path, kSlowClientPort);
    const int small_buffer = 4096;
    const int fd = connect_with_retry(kSlowClientPort, small_buffer);
    if (fd < 0) {
        std::fprintf(stderr, "could not connect to slow-client server\n");
        return false;
    }

    timeval send_timeout{
        .tv_sec = 1,
        .tv_usec = 0,
    };
    (void)::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &send_timeout, sizeof(send_timeout));

    for (std::uint64_t seq = 1; seq <= 50'000; ++seq) {
        const auto record = limit_bid(seq);
        if (!send_all(fd, record.data(), record.size())) {
            break;
        }
    }

    // The client deliberately does not read while the server is producing
    // responses. A bounded write buffer should make the server close it.
    const bool server_disconnected_client = server.wait_for_exit_for(std::chrono::seconds{5});
    ::close(fd);

    if (!server_disconnected_client) {
        std::fprintf(stderr, "slow client was not disconnected by the server\n");
        return false;
    }
    return true;
}

bool spectator_receives_l2_updates(std::string_view server_path) {
    // Three clients: one market-data spectator, one resting trader, and one
    // crossing trader so the fill is not blocked by self-trade prevention.
    auto server = start_server(server_path, kSpectatorPort, 2, 1);
    const int spectator_fd = connect_with_retry(kSpectatorPort);
    if (spectator_fd < 0) {
        std::fprintf(stderr, "could not connect spectator\n");
        return false;
    }

    constexpr std::string_view handshake = "SPECTATOR\n";
    if (!send_all(spectator_fd, reinterpret_cast<const std::uint8_t*>(handshake.data()),
                  handshake.size())) {
        std::fprintf(stderr, "could not send spectator handshake\n");
        ::close(spectator_fd);
        return false;
    }
    timeval receive_timeout{
        .tv_sec = 2,
        .tv_usec = 0,
    };
    (void)::setsockopt(spectator_fd, SOL_SOCKET, SO_RCVTIMEO, &receive_timeout,
                       sizeof(receive_timeout));
    if (!read_until_contains(spectator_fd, "SNAPSHOT_END seq=0\n")) {
        std::fprintf(stderr, "spectator did not receive initial snapshot\n");
        ::close(spectator_fd);
        return false;
    }

    const int trader_fd = connect_with_retry(kSpectatorPort);
    if (trader_fd < 0) {
        std::fprintf(stderr, "could not connect trader\n");
        ::close(spectator_fd);
        return false;
    }
    (void)::setsockopt(trader_fd, SOL_SOCKET, SO_RCVTIMEO, &receive_timeout,
                       sizeof(receive_timeout));

    if (!send_record(trader_fd, limit_bid(1))) {
        std::fprintf(stderr, "could not send resting bid\n");
        ::close(trader_fd);
        ::close(spectator_fd);
        return false;
    }

    ob::OutboundEvent bid_ack{};
    if (!read_event(trader_fd, bid_ack) || bid_ack.type != ob::EventType::AckNew ||
        !read_until_contains(spectator_fd, "L2 seq=1 side=Bid price=100 qty=1\n")) {
        std::fprintf(stderr, "spectator did not receive resting-bid L2 update\n");
        ::close(trader_fd);
        ::close(spectator_fd);
        return false;
    }

    if (!send_record(trader_fd, cancel_order(2, bid_ack.handle))) {
        std::fprintf(stderr, "could not send cancel\n");
        ::close(trader_fd);
        ::close(spectator_fd);
        return false;
    }
    ob::OutboundEvent cancel_ack{};
    if (!read_event(trader_fd, cancel_ack) || cancel_ack.type != ob::EventType::AckCancel ||
        !read_until_contains(spectator_fd, "L2 seq=2 side=Bid price=100 qty=0\n")) {
        std::fprintf(stderr, "spectator did not receive cancel L2 update\n");
        ::close(trader_fd);
        ::close(spectator_fd);
        return false;
    }

    if (!send_record(trader_fd, limit_ask(3, 105, 5))) {
        std::fprintf(stderr, "could not send resting ask\n");
        ::close(trader_fd);
        ::close(spectator_fd);
        return false;
    }
    ob::OutboundEvent ask_ack{};
    if (!read_event(trader_fd, ask_ack) || ask_ack.type != ob::EventType::AckNew ||
        !read_until_contains(spectator_fd, "L2 seq=3 side=Ask price=105 qty=5\n")) {
        std::fprintf(stderr, "spectator did not receive resting-ask L2 update\n");
        ::close(trader_fd);
        ::close(spectator_fd);
        return false;
    }

    // Use a second trader for the market buy. The server assigns participant
    // ids per connection, so one socket trading with itself would be rejected.
    const int buyer_fd = connect_with_retry(kSpectatorPort);
    if (buyer_fd < 0) {
        std::fprintf(stderr, "could not connect buyer\n");
        ::close(trader_fd);
        ::close(spectator_fd);
        return false;
    }
    (void)::setsockopt(buyer_fd, SOL_SOCKET, SO_RCVTIMEO, &receive_timeout,
                       sizeof(receive_timeout));

    if (!send_record(buyer_fd, market_bid(4, 2))) {
        std::fprintf(stderr, "could not send crossing market order\n");
        ::close(buyer_fd);
        ::close(trader_fd);
        ::close(spectator_fd);
        return false;
    }
    ob::OutboundEvent resting_fill{};
    ob::OutboundEvent aggressor_fill{};
    if (!read_event(trader_fd, resting_fill) || !read_event(buyer_fd, aggressor_fill) ||
        resting_fill.type != ob::EventType::Fill || aggressor_fill.type != ob::EventType::Fill ||
        !read_until_contains(spectator_fd, "L2 seq=4 side=Ask price=105 qty=3\n")) {
        std::fprintf(stderr, "spectator did not receive fill L2 update\n");
        ::close(buyer_fd);
        ::close(trader_fd);
        ::close(spectator_fd);
        return false;
    }

    ::close(buyer_fd);
    ::close(trader_fd);
    ::close(spectator_fd);

    if (!server.wait_for_exit()) {
        std::fprintf(stderr, "spectator server did not exit cleanly\n");
        return false;
    }
    return true;
}

bool spectator_receives_existing_depth_snapshot(std::string_view server_path) {
    auto server = start_server(server_path, kSnapshotPort, 1, 1);
    const int trader_fd = connect_with_retry(kSnapshotPort);
    if (trader_fd < 0) {
        std::fprintf(stderr, "could not connect snapshot trader\n");
        return false;
    }

    timeval receive_timeout{
        .tv_sec = 2,
        .tv_usec = 0,
    };
    (void)::setsockopt(trader_fd, SOL_SOCKET, SO_RCVTIMEO, &receive_timeout,
                       sizeof(receive_timeout));

    if (!send_record(trader_fd, limit_ask(1, 106, 9))) {
        std::fprintf(stderr, "could not seed snapshot depth\n");
        ::close(trader_fd);
        return false;
    }
    ob::OutboundEvent ack{};
    if (!read_event(trader_fd, ack) || ack.type != ob::EventType::AckNew) {
        std::fprintf(stderr, "snapshot seed order was not accepted\n");
        ::close(trader_fd);
        return false;
    }

    const int spectator_fd = connect_with_retry(kSnapshotPort);
    if (spectator_fd < 0) {
        std::fprintf(stderr, "could not connect snapshot spectator\n");
        ::close(trader_fd);
        return false;
    }
    (void)::setsockopt(spectator_fd, SOL_SOCKET, SO_RCVTIMEO, &receive_timeout,
                       sizeof(receive_timeout));

    constexpr std::string_view handshake = "SPECTATOR\n";
    if (!send_all(spectator_fd, reinterpret_cast<const std::uint8_t*>(handshake.data()),
                  handshake.size()) ||
        !read_snapshot_containing(spectator_fd, "SNAPSHOT seq=1 side=Ask price=106 qty=9\n")) {
        std::fprintf(stderr, "spectator did not receive existing-depth snapshot\n");
        ::close(spectator_fd);
        ::close(trader_fd);
        return false;
    }

    ::close(spectator_fd);
    ::close(trader_fd);
    if (!server.wait_for_exit()) {
        std::fprintf(stderr, "snapshot server did not exit cleanly\n");
        return false;
    }
    return true;
}

bool malformed_record_closes_only_that_client(std::string_view server_path) {
    auto server = start_server(server_path, kMalformedPort, 1);
    const int fd = connect_with_retry(kMalformedPort);
    if (fd < 0) {
        std::fprintf(stderr, "could not connect malformed-record client\n");
        return false;
    }

    timeval receive_timeout{
        .tv_sec = 2,
        .tv_usec = 0,
    };
    (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &receive_timeout, sizeof(receive_timeout));

    std::array<std::uint8_t, ob::codec::ENCODED_INBOUND_SIZE> bad_record{};
    if (!send_all(fd, bad_record.data(), bad_record.size())) {
        std::fprintf(stderr, "could not send malformed record\n");
        ::close(fd);
        return false;
    }

    std::array<std::uint8_t, 1> byte{};
    const ssize_t n = ::recv(fd, byte.data(), byte.size(), 0);
    const bool closed = n == 0 || (n < 0 && errno == ECONNRESET);
    ::close(fd);
    if (!closed) {
        std::fprintf(stderr, "malformed-record client was not closed\n");
        return false;
    }
    if (!server.wait_for_exit()) {
        std::fprintf(stderr, "malformed-record server did not exit cleanly\n");
        return false;
    }
    return true;
}

bool idle_pending_client_is_closed(std::string_view server_path) {
    auto server = start_server(server_path, kPendingTimeoutPort, 1);
    const int fd = connect_with_retry(kPendingTimeoutPort);
    if (fd < 0) {
        std::fprintf(stderr, "could not connect idle pending client\n");
        return false;
    }

    timeval receive_timeout{
        .tv_sec = 3,
        .tv_usec = 0,
    };
    (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &receive_timeout, sizeof(receive_timeout));

    std::array<std::uint8_t, 1> byte{};
    const ssize_t n = ::recv(fd, byte.data(), byte.size(), 0);
    const bool closed = n == 0 || (n < 0 && errno == ECONNRESET);
    ::close(fd);
    if (!closed) {
        std::fprintf(stderr, "idle pending client was not closed\n");
        return false;
    }
    if (!server.wait_for_exit()) {
        std::fprintf(stderr, "pending-timeout server did not exit cleanly\n");
        return false;
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::fprintf(stderr, "usage: tcp_pipeline_test <tcp_server>\n");
        return 2;
    }

    if (!fragmented_record_is_decoded(argv[1])) {
        return 1;
    }
    if (!slow_client_is_disconnected(argv[1])) {
        return 1;
    }
    if (!spectator_receives_l2_updates(argv[1])) {
        return 1;
    }
    if (!spectator_receives_existing_depth_snapshot(argv[1])) {
        return 1;
    }
    if (!malformed_record_closes_only_that_client(argv[1])) {
        return 1;
    }
    if (!idle_pending_client_is_closed(argv[1])) {
        return 1;
    }

    std::printf("tcp_pipeline_test OK\n");
    return 0;
}
