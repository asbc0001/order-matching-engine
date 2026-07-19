// types.hpp - Fixed-size in-memory types shared by the order book, rings, and
// tests.
//
// These types are cheap to copy through queues and have compile-time layout
// checks below. They are internal representations only: files, sockets, logs,
// and hashes must use an explicit codec that writes named fields rather than
// raw struct bytes, because padding bytes are not part of the data model.

#pragma once

#include <cstdint>
#include <type_traits>

namespace ob {

enum class Side : uint8_t { Bid = 0, Ask = 1 };

enum class MsgType : uint8_t {
    NewLimit,
    NewMarket,
    Cancel,
    StopEngine  // Internal-only shutdown sentinel; external decoders must reject it.
};

// Limit-order lifetime. GTC is zero so older saved command files, whose unused
// bytes decode as zero, keep their original "rest any remainder" behavior.
enum class TimeInForce : uint8_t {
    GTC = 0,  // Good-till-cancel: match now, then rest any remaining quantity.
    IOC = 1,  // Immediate-or-cancel: match now, reject any remaining quantity.
    FOK = 2,  // Fill-or-kill: fill the whole quantity now, or do nothing.
};

using Price = int64_t;           // integer ticks: no floats in matching state/decisions
using Qty = uint32_t;            // per-order quantity
using AggQty = uint64_t;         // aggregate quantity
using Handle = uint64_t;         // (generation << 32) | pool_slot; 0 is never valid
using ParticipantId = uint32_t;  // trading identity used for self-trade checks

// Command submitted to the matching engine. Producers write these into the
// inbound ring; the matching thread consumes them in sequence.
struct alignas(64) InboundMsg {  // one cache line, internal representation
    uint64_t client_seq;         // producer-assigned, for replay/tracing
    Handle handle;               // Cancel only; 0 otherwise
    Price price;                 // NewLimit only
    Qty qty;
    Side side;
    MsgType type;
    TimeInForce time_in_force{TimeInForce::GTC};
    ParticipantId participant_id{0};
    uint64_t tsc_intended;  // open-loop: scheduled arrival t_i
    uint64_t tsc_ready;     // producer reached the push attempt
    uint64_t tsc_enqueue;   // ring accepted the message
    // Timing fields are internal-only and must not be serialized into external
    // file or wire formats.
};

// Events are the matcher's output stream. One input command can produce several
// events: fills for trades, then one final ack or reject that completes it.
enum class EventType : uint8_t {
    AckNew,     // New order accepted and any remainder rested in the book
    AckCancel,  // Cancel accepted and the resting order was removed
    Fill,       // Trade report for one side of a match
    Reject,     // Request failed validation or could not be fully completed
    StopEngine  // Internal-only shutdown event; loggers should drain it but exclude
                // it from replayable event logs
};

enum class RejectReason : uint8_t {
    None,
    PriceOutOfBand,
    ZeroQty,
    PoolExhausted,
    UnknownHandle,
    InsufficientLiquidity,       // market order remainder unfilled; qty carries the remainder
    ImmediateOrCancelRemainder,  // IOC matched what it could and rejected the rest
    FillOrKillNotFilled,         // FOK could not fill the full requested quantity
    SelfTrade                    // aggressing order would trade against the same participant
};

enum EventFlags : uint8_t { RequestComplete = 1 << 0 };

// Result emitted by the matching engine. Acks confirm accepted state changes,
// fills report trades, rejects explain failed requests, and StopEngine shuts
// down internal runners.
struct alignas(64) OutboundEvent {
    uint64_t client_seq;
    Handle handle;
    Price price;
    Qty qty;
    Side side;  // lets the event stream reconstruct book
                // evolution without extra side-channel data
    EventType type;
    RejectReason reason;
    uint8_t flags;                    // RequestComplete marks the final event for
                                      // an inbound request.
    ParticipantId participant_id{0};  // internal routing key for TCP responses;
                                      // files and sockets do not serialize it
    uint64_t tsc_intended;            // copied through
    uint64_t tsc_egress;              // taken at event creation
};

// Active order stored in the engine-owned pool while it rests in the book. The
// prev/next links chain orders at the same price level in FIFO order, and the
// one-line layout avoids split-line fetches when resolving handles.
struct alignas(64) Order {
    uint64_t client_seq;  // Retained so later fills can report the order's
                          // original client sequence.
    Handle handle;        // Cleared to 0 on free so stale handles fail lookup.
    Price price;
    Qty remaining;
    ParticipantId participant_id{0};
    Side side;
    uint32_t prev, next;  // pool slot indices, not pointers
};

inline constexpr uint32_t NULL_SLOT = 0xFFFFFFFFu;

// Compile-time layout contracts:

static_assert(sizeof(InboundMsg) == 64, "InboundMsg must be one cache line");
static_assert(sizeof(OutboundEvent) == 64, "OutboundEvent must be one cache line");
static_assert(sizeof(Order) == 64, "Order must be one cache line");

static_assert(std::is_trivially_copyable_v<InboundMsg>);
static_assert(std::is_trivially_copyable_v<OutboundEvent>);
static_assert(std::is_trivially_copyable_v<Order>);

static_assert(std::is_same_v<std::underlying_type_t<Side>, uint8_t>);
static_assert(std::is_same_v<std::underlying_type_t<MsgType>, uint8_t>);
static_assert(std::is_same_v<std::underlying_type_t<TimeInForce>, uint8_t>);
static_assert(std::is_same_v<std::underlying_type_t<EventType>, uint8_t>);
static_assert(std::is_same_v<std::underlying_type_t<RejectReason>, uint8_t>);
static_assert(std::is_same_v<std::underlying_type_t<EventFlags>, uint8_t>);
static_assert(sizeof(ParticipantId) == 4);

static_assert(alignof(InboundMsg) == 64);
static_assert(alignof(OutboundEvent) == 64);
static_assert(alignof(Order) == 64);

}  // namespace ob
