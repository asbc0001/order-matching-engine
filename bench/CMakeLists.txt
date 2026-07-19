add_executable(spsc_ring_bench spsc_ring_bench.cpp)
target_link_libraries(spsc_ring_bench PRIVATE orderbook_core)

add_executable(engine_bench engine_bench.cpp)
target_link_libraries(engine_bench PRIVATE orderbook_codec orderbook_core)
target_compile_definitions(engine_bench PRIVATE
  OB_BUILD_TYPE="$<CONFIG>"
  OB_CXX_FLAGS="${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${CMAKE_BUILD_TYPE}}"
)
