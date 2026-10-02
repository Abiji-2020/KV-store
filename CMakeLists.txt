cmake_minimum_required(VERSION 3.16)
project(cpp_kv_store VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Strict warnings and performance optimizations per R5 & PROJECT.md
add_compile_options(-Wall -Wextra -pthread -O3)

find_package(Threads REQUIRED)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)
file(MAKE_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)

# Core static library encompassing engine, WAL, protocol, server, client, and benchmark
add_library(kvstore_lib STATIC
    src/lru_shard.cpp
    src/kvstore.cpp
    src/crc32.cpp
    src/wal.cpp
    src/protocol.cpp
    src/server.cpp
    src/client.cpp
    src/benchmark.cpp
)

target_include_directories(kvstore_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(kvstore_lib PUBLIC Threads::Threads)

# Convenient alias for alternative naming
add_library(kv_store_lib ALIAS kvstore_lib)

# Standalone Executables
add_executable(kv_server src/server_main.cpp)
target_link_libraries(kv_server PRIVATE kvstore_lib)

add_executable(kv_client src/client_main.cpp)
target_link_libraries(kv_client PRIVATE kvstore_lib)

add_executable(kv_bench src/bench_main.cpp)
target_link_libraries(kv_bench PRIVATE kvstore_lib)

# Benchmark convenience target
add_custom_target(bench DEPENDS kv_bench)

# Automated Unit Tests
enable_testing()

set(UNIT_TEST_TARGETS
    test_engine
    test_stress_challenger
    test_adversarial_m1_2
    test_wal
    test_adversarial_wal_torn
    test_protocol
    test_adversarial_protocol
    test_server
    test_adversarial_server
    test_benchmark
    test_adversarial_benchmark
)

foreach(TGT IN LISTS UNIT_TEST_TARGETS)
    add_executable(${TGT} tests/unit/${TGT}.cpp)
    target_link_libraries(${TGT} PRIVATE kvstore_lib)
    add_test(NAME ${TGT} COMMAND $<TARGET_FILE:${TGT}>)
endforeach()
