// config.hpp - Compile-time capacity presets for the engine.
//
// Two capacity sets:
//   - Production: real default sizes, used for real runs and benchmarks.
//   - Fuzz: small sizes so exhaustion paths and full consistency audits are
//     cheap enough to run frequently.
//
// Build configuration can later select one of these presets; for now both sets
// are available to tests and future components.

#pragma once

#include <cstddef>
#include <cstdint>

#include "orderbook/types.hpp"

namespace ob::config {

struct Capacities {
    std::size_t num_levels;     // price band width, in ticks
    std::size_t pool_capacity;  // max concurrently-live orders
    std::size_t ring_capacity;  // SPSC ring capacity; must be power of 2
};

inline constexpr Capacities kProduction{
    .num_levels = 65'536,
    .pool_capacity = 1'048'576,  // 2^20
    .ring_capacity = 65'536,     // 2^16
};

inline constexpr Capacities kFuzz{
    .num_levels = 256,
    .pool_capacity = 1'024,
    .ring_capacity = 1'024,
};

inline constexpr Price BASE_PRICE = 0;  // Placeholder; real deployments should
                                        // choose this per instrument.

}  // namespace ob::config
