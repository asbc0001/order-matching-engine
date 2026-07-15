add_executable(pool_test pool_test.cpp)
target_link_libraries(pool_test PRIVATE orderbook_core)
target_compile_definitions(pool_test PRIVATE OB_ENABLE_TEST_HOOKS)
add_test(NAME pool_test COMMAND pool_test)

add_executable(bitmap_test bitmap_test.cpp)
target_link_libraries(bitmap_test PRIVATE orderbook_core)
add_test(NAME bitmap_test COMMAND bitmap_test)

add_executable(book_test book_test.cpp)
target_link_libraries(book_test PRIVATE orderbook_core)
add_test(NAME book_test COMMAND book_test)

add_executable(matcher_test matcher_test.cpp)
target_link_libraries(matcher_test PRIVATE orderbook_core)
add_test(NAME matcher_test COMMAND matcher_test)

add_executable(reference_test reference_test.cpp)
target_link_libraries(reference_test PRIVATE orderbook_core)
add_test(NAME reference_test COMMAND reference_test)
