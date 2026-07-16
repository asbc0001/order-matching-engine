// time.hpp - Monotonic nanosecond timestamps for pipeline timing fields.
//
// The engine uses std::chrono::steady_clock for elapsed-time measurements. One
// shared function keeps every stamp site consistent in clock and units, so
// intervals like (egress - enqueue) stay comparable.

#pragma once

#include <chrono>
#include <cstdint>

namespace ob {

inline std::uint64_t engine_time_nanos() noexcept {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

}  // namespace ob
