add_executable(smoke_test smoke_test.cpp)
target_link_libraries(smoke_test PRIVATE orderbook_core)
add_test(NAME smoke_test COMMAND smoke_test)

add_executable(pool_test pool_test.cpp)
target_link_libraries(pool_test PRIVATE orderbook_core)
target_compile_definitions(pool_test PRIVATE OB_ENABLE_TEST_HOOKS)
add_test(NAME pool_test COMMAND pool_test)
