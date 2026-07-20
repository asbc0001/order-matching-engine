add_executable(text_to_saved_commands text_to_saved_commands.cpp)
    target_link_libraries(text_to_saved_commands PRIVATE orderbook_codec orderbook_trace_parser)

        add_executable(generate_saved_commands generate_saved_commands.cpp)
            target_link_libraries(generate_saved_commands PRIVATE orderbook_codec orderbook_core)

                add_executable(run_engine run_engine.cpp)
                    target_link_libraries(run_engine PRIVATE orderbook_codec orderbook_core)

                        add_executable(tcp_server tcp_server.cpp)
                            target_link_libraries(tcp_server PRIVATE orderbook_codec orderbook_core)

                                add_executable(client client.cpp)
                                    target_link_libraries(client PRIVATE orderbook_codec
                                                              orderbook_trace_parser orderbook_core)
