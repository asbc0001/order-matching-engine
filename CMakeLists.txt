cmake_minimum_required(VERSION 3.20)
project(orderbook LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

enable_testing()

add_library(orderbook_core INTERFACE)
target_compile_features(orderbook_core INTERFACE cxx_std_20)
target_include_directories(orderbook_core INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_compile_options(orderbook_core INTERFACE
  -Wall
  -Wextra
  -Werror
)

add_subdirectory(src)
add_subdirectory(test)

# bench/ and tools/ become build targets once executable tooling lands. Until
# then their directories are kept as documented placeholders.
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/bench/CMakeLists.txt)
  add_subdirectory(bench)
endif()
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/tools/CMakeLists.txt)
  add_subdirectory(tools)
endif()
