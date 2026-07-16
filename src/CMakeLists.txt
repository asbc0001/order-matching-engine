add_library(orderbook_codec STATIC codec.cpp)
target_link_libraries(orderbook_codec PUBLIC orderbook_core)
