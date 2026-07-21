// tcp/connection.hpp - Shared state for one TCP client connection.
//
// The producer thread reads client commands. The logger thread writes responses
// and is the only thread allowed to close sockets. Keeping close ownership in
// one place avoids fd reuse bugs where a new client inherits an old fd number.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

#include "orderbook/config.hpp"
#include "orderbook/types.hpp"

namespace ob::tcp {

enum class ConnectionRole : std::uint8_t {
    Trader,
    Spectator,
};

struct ConnectionState {
    int fd{-1};
    ParticipantId participant_id{0};
    ConnectionRole role{ConnectionRole::Trader};

    // Producer-owned buffer. It keeps partial fixed-size command records until
    // enough bytes have arrived for codec decoding.
    std::vector<std::uint8_t> read_buf{};
    std::size_t read_size{0};

    // Logger-owned buffer. If a client reads slowly, responses wait here up to
    // WRITE_BUF_CAP_BYTES; after that the logger disconnects the client.
    std::vector<std::uint8_t> write_buf{};
    std::size_t write_offset{0};
    bool write_registered{false};

    // Producer sets read_closed after EOF, a read error, or malformed input.
    // Logger observes it and closes the fd once any queued writes are flushed.
    std::atomic<bool> read_closed{false};
    std::atomic<bool> closed{false};

    // Set when a client becomes a spectator. The logger consumes this flag and
    // sends the current L2 state before live updates continue.
    std::atomic<bool> snapshot_pending{false};

    ConnectionState() = default;

    ConnectionState(int socket_fd, ParticipantId id)
        : fd(socket_fd), participant_id(id), read_buf(config::READ_BUF_INITIAL_BYTES) {
        write_buf.reserve(config::WRITE_BUF_CAP_BYTES);
    }
};

// Participant 0 means "not assigned" in the rest of the engine. TCP clients
// start at 1 so self-trade checks and response routing can distinguish them.
struct ParticipantIdSource {
    ParticipantId next{1};

    ParticipantId take() noexcept {
        if (next == 0) {
            return 0;
        }

        const ParticipantId id = next;
        if (next == std::numeric_limits<ParticipantId>::max()) {
            next = 0;
        } else {
            ++next;
        }
        return id;
    }
};

}  // namespace ob::tcp
