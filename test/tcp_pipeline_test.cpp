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

ServerProcess start_server(std::string_view server_path, std::uint16_t port) {
    ServerProcess server;
    server.pid = ::fork();
    if (server.pid == 0) {
        const std::string port_text = std::to_string(port);
        ::execl(server_path.data(), server_path.data(), port_text.c_str(), "--clients", "1",
                "--yield", static_cast<char*>(nullptr));
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

    std::printf("tcp_pipeline_test OK\n");
    return 0;
}
