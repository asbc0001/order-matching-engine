add_executable(pool_test pool_test.cpp)
target_link_libraries(pool_test PRIVATE orderbook_core)
target_compile_definitions(pool_test PRIVATE OB_ENABLE_TEST_HOOKS)
add_test(NAME pool_test COMMAND pool_test)

add_executable(bitmap_test bitmap_test.cpp)
target_link_libraries(bitmap_test PRIVATE orderbook_core)
add_test(NAME bitmap_test COMMAND bitmap_test)

add_executable(spsc_ring_test spsc_ring_test.cpp)
target_link_libraries(spsc_ring_test PRIVATE orderbook_core)
add_test(NAME spsc_ring_test COMMAND spsc_ring_test)

add_executable(spsc_ring_stress spsc_ring_stress.cpp)
target_link_libraries(spsc_ring_stress PRIVATE orderbook_core)
add_test(NAME spsc_ring_stress_smoke COMMAND spsc_ring_stress 100000)
if(CMAKE_CXX_FLAGS MATCHES "fsanitize=thread")
  find_program(SETARCH_EXE setarch)
  if(SETARCH_EXE)
    add_test(NAME spsc_ring_stress_tsan
      COMMAND ${SETARCH_EXE} -R $<TARGET_FILE:spsc_ring_stress> 1000000)
  else()
    add_test(NAME spsc_ring_stress_tsan COMMAND spsc_ring_stress 1000000)
  endif()
endif()

add_executable(codec_test codec_test.cpp)
target_link_libraries(codec_test PRIVATE orderbook_codec)
add_test(NAME codec_test COMMAND codec_test)

add_executable(text_trace_parser_test text_trace_parser_test.cpp)
target_link_libraries(text_trace_parser_test PRIVATE orderbook_trace_parser)
add_test(NAME text_trace_parser_test COMMAND text_trace_parser_test)

add_executable(trace_replay_test trace_replay_test.cpp)
target_link_libraries(trace_replay_test PRIVATE orderbook_codec)
add_test(NAME trace_replay_test COMMAND trace_replay_test)

add_executable(synthetic_generator_test synthetic_generator_test.cpp)
target_link_libraries(synthetic_generator_test PRIVATE orderbook_core)
add_test(NAME synthetic_generator_test COMMAND synthetic_generator_test)

add_executable(event_logger_test event_logger_test.cpp)
target_link_libraries(event_logger_test PRIVATE orderbook_core)
add_test(NAME event_logger_test COMMAND event_logger_test)

add_executable(demo_renderer_test demo_renderer_test.cpp)
target_link_libraries(demo_renderer_test PRIVATE orderbook_core)
add_test(NAME demo_renderer_test COMMAND demo_renderer_test)

add_executable(matching_loop_test matching_loop_test.cpp)
target_link_libraries(matching_loop_test PRIVATE orderbook_core)
add_test(NAME matching_loop_test COMMAND matching_loop_test)

add_executable(book_test book_test.cpp)
target_link_libraries(book_test PRIVATE orderbook_core)
add_test(NAME book_test COMMAND book_test)

add_executable(book_dump_test book_dump_test.cpp)
target_link_libraries(book_dump_test PRIVATE orderbook_core)
add_test(NAME book_dump_test COMMAND book_dump_test)

add_executable(matcher_test matcher_test.cpp)
target_link_libraries(matcher_test PRIVATE orderbook_core)
add_test(NAME matcher_test COMMAND matcher_test)

add_executable(differential_fuzz_test differential_fuzz_test.cpp)
target_link_libraries(differential_fuzz_test PRIVATE orderbook_core)
add_test(NAME differential_fuzz_test COMMAND differential_fuzz_test)

add_test(
  NAME deterministic_replay_test
  COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/deterministic_replay_test.sh
          $<TARGET_FILE:generate_saved_commands>
          $<TARGET_FILE:run_engine>
)

add_test(
  NAME cancel_heavy_replay_test
  COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/cancel_heavy_replay_test.sh
          $<TARGET_FILE:generate_saved_commands>
          $<TARGET_FILE:run_engine>
)
