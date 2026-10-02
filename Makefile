# ==============================================================================
# Makefile: High-Performance Concurrent In-Memory Key-Value Store (cpp_kv_store)
# Supports Milestones 1, 2, 3 & 4 (Engine, WAL, Server, Client, Benchmark)
# ==============================================================================

CXX         := g++
CXXFLAGS    := -std=c++20 -O3 -Wall -Wextra -pthread
INCLUDES    := -Iinclude

SRC_DIR     := src
TEST_DIR    := tests/unit
OBJ_DIR     := obj
BIN_DIR     := bin

ENGINE_SRCS := $(SRC_DIR)/lru_shard.cpp $(SRC_DIR)/kvstore.cpp
ENGINE_OBJS := $(OBJ_DIR)/lru_shard.o $(OBJ_DIR)/kvstore.o

WAL_SRCS    := $(SRC_DIR)/crc32.cpp $(SRC_DIR)/wal.cpp
WAL_OBJS    := $(OBJ_DIR)/crc32.o $(OBJ_DIR)/wal.o

NET_SRCS    := $(SRC_DIR)/protocol.cpp $(SRC_DIR)/server.cpp $(SRC_DIR)/client.cpp
NET_OBJS    := $(OBJ_DIR)/protocol.o $(OBJ_DIR)/server.o $(OBJ_DIR)/client.o

BENCH_SRCS  := $(SRC_DIR)/benchmark.cpp
BENCH_OBJS  := $(OBJ_DIR)/benchmark.o
BENCH_BIN   := $(BIN_DIR)/kv_bench

SERVER_BIN  := $(BIN_DIR)/kv_server
CLIENT_BIN  := $(BIN_DIR)/kv_client

TEST_SRC    := $(TEST_DIR)/test_engine.cpp
TEST_OBJ    := $(OBJ_DIR)/test_engine.o
TEST_BIN    := $(BIN_DIR)/test_engine

STRESS_SRC  := $(TEST_DIR)/test_stress_challenger.cpp
STRESS_OBJ  := $(OBJ_DIR)/test_stress_challenger.o
STRESS_BIN  := $(BIN_DIR)/test_stress_challenger

ADV2_SRC    := $(TEST_DIR)/test_adversarial_m1_2.cpp
ADV2_OBJ    := $(OBJ_DIR)/test_adversarial_m1_2.o
ADV2_BIN    := $(BIN_DIR)/test_adversarial_m1_2

TEST_WAL_SRC := $(TEST_DIR)/test_wal.cpp
TEST_WAL_OBJ := $(OBJ_DIR)/test_wal.o
TEST_WAL_BIN := $(BIN_DIR)/test_wal

ADV_WAL_SRC := $(TEST_DIR)/test_adversarial_wal_torn.cpp
ADV_WAL_OBJ := $(OBJ_DIR)/test_adversarial_wal_torn.o
ADV_WAL_BIN := $(BIN_DIR)/test_adversarial_wal_torn

TEST_PROTO_SRC  := $(TEST_DIR)/test_protocol.cpp
TEST_PROTO_OBJ  := $(OBJ_DIR)/test_protocol.o
TEST_PROTO_BIN  := $(BIN_DIR)/test_protocol

ADV_PROTO_SRC   := $(TEST_DIR)/test_adversarial_protocol.cpp
ADV_PROTO_OBJ   := $(OBJ_DIR)/test_adversarial_protocol.o
ADV_PROTO_BIN   := $(BIN_DIR)/test_adversarial_protocol

TEST_SERVER_SRC := $(TEST_DIR)/test_server.cpp
TEST_SERVER_OBJ := $(OBJ_DIR)/test_server.o
TEST_SERVER_BIN := $(BIN_DIR)/test_server

ADV_SERVER_SRC  := $(TEST_DIR)/test_adversarial_server.cpp
ADV_SERVER_OBJ  := $(OBJ_DIR)/test_adversarial_server.o
ADV_SERVER_BIN  := $(BIN_DIR)/test_adversarial_server

TEST_BENCH_SRC  := $(TEST_DIR)/test_benchmark.cpp
TEST_BENCH_OBJ  := $(OBJ_DIR)/test_benchmark.o
TEST_BENCH_BIN  := $(BIN_DIR)/test_benchmark

ADV_BENCH_SRC   := $(TEST_DIR)/test_adversarial_benchmark.cpp
ADV_BENCH_OBJ   := $(OBJ_DIR)/test_adversarial_benchmark.o
ADV_BENCH_BIN   := $(BIN_DIR)/test_adversarial_benchmark

.PHONY: all test test_engine test_stress test_adversarial test_wal test_adversarial_wal \
        test_protocol test_adversarial_protocol test_server test_adversarial_server \
        test_benchmark test_adversarial_benchmark bench clean dirs

all: dirs $(TEST_BIN) $(STRESS_BIN) $(ADV2_BIN) $(TEST_WAL_BIN) $(ADV_WAL_BIN) \
     $(SERVER_BIN) $(CLIENT_BIN) $(TEST_PROTO_BIN) $(ADV_PROTO_BIN) $(TEST_SERVER_BIN) \
     $(ADV_SERVER_BIN) $(BENCH_BIN) $(TEST_BENCH_BIN) $(ADV_BENCH_BIN)

dirs:
	@mkdir -p $(OBJ_DIR) $(BIN_DIR)

bench: dirs $(BENCH_BIN)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp | dirs
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

$(OBJ_DIR)/%.o: $(TEST_DIR)/%.cpp | dirs
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

$(TEST_BIN): $(ENGINE_OBJS) $(TEST_OBJ) | dirs
	$(CXX) $(CXXFLAGS) $^ -o $@

$(STRESS_BIN): $(ENGINE_OBJS) $(STRESS_OBJ) | dirs
	$(CXX) $(CXXFLAGS) $^ -o $@

$(ADV2_BIN): $(ENGINE_OBJS) $(ADV2_OBJ) | dirs
	$(CXX) $(CXXFLAGS) $^ -o $@

$(TEST_WAL_BIN): $(ENGINE_OBJS) $(WAL_OBJS) $(TEST_WAL_OBJ) | dirs
	$(CXX) $(CXXFLAGS) $^ -o $@

$(ADV_WAL_BIN): $(ENGINE_OBJS) $(WAL_OBJS) $(ADV_WAL_OBJ) | dirs
	$(CXX) $(CXXFLAGS) $^ -o $@

$(SERVER_BIN): $(ENGINE_OBJS) $(WAL_OBJS) $(OBJ_DIR)/protocol.o $(OBJ_DIR)/server.o $(OBJ_DIR)/server_main.o | dirs
	$(CXX) $(CXXFLAGS) $^ -o $@

$(CLIENT_BIN): $(OBJ_DIR)/protocol.o $(OBJ_DIR)/client.o $(OBJ_DIR)/client_main.o | dirs
	$(CXX) $(CXXFLAGS) $^ -o $@

$(TEST_PROTO_BIN): $(OBJ_DIR)/protocol.o $(TEST_PROTO_OBJ) | dirs
	$(CXX) $(CXXFLAGS) $^ -o $@

$(ADV_PROTO_BIN): $(OBJ_DIR)/protocol.o $(ADV_PROTO_OBJ) | dirs
	$(CXX) $(CXXFLAGS) $^ -o $@

$(TEST_SERVER_BIN): $(ENGINE_OBJS) $(WAL_OBJS) $(OBJ_DIR)/protocol.o $(OBJ_DIR)/server.o $(OBJ_DIR)/client.o $(TEST_SERVER_OBJ) | dirs
	$(CXX) $(CXXFLAGS) $^ -o $@

$(ADV_SERVER_BIN): $(ENGINE_OBJS) $(WAL_OBJS) $(OBJ_DIR)/protocol.o $(OBJ_DIR)/server.o $(OBJ_DIR)/client.o $(ADV_SERVER_OBJ) | dirs
	$(CXX) $(CXXFLAGS) $^ -o $@

$(BENCH_BIN): $(ENGINE_OBJS) $(WAL_OBJS) $(OBJ_DIR)/protocol.o $(OBJ_DIR)/client.o $(BENCH_OBJS) $(OBJ_DIR)/bench_main.o | dirs
	$(CXX) $(CXXFLAGS) $^ -o $@

$(TEST_BENCH_BIN): $(ENGINE_OBJS) $(WAL_OBJS) $(OBJ_DIR)/protocol.o $(OBJ_DIR)/server.o $(OBJ_DIR)/client.o $(BENCH_OBJS) $(TEST_BENCH_OBJ) | dirs
	$(CXX) $(CXXFLAGS) $^ -o $@

$(ADV_BENCH_BIN): $(ENGINE_OBJS) $(WAL_OBJS) $(OBJ_DIR)/protocol.o $(OBJ_DIR)/server.o $(OBJ_DIR)/client.o $(BENCH_OBJS) $(ADV_BENCH_OBJ) | dirs
	$(CXX) $(CXXFLAGS) $^ -o $@

test_engine: $(TEST_BIN)
	@echo "Running Milestone 1 Unit & Concurrency Test Suite..."
	./$(TEST_BIN)

test_stress: $(STRESS_BIN)
	@echo "Running Milestone 1 Adversarial Concurrency Stress Suite..."
	./$(STRESS_BIN)

test_adversarial: $(ADV2_BIN)
	@echo "Running Milestone 1 Adversarial Boundary & Recency Suite..."
	./$(ADV2_BIN)

test_wal: $(TEST_WAL_BIN)
	@echo "Running Milestone 2 Write-Ahead Log & Crash Recovery Test Suite..."
	./$(TEST_WAL_BIN)

test_adversarial_wal: $(ADV_WAL_BIN)
	@echo "Running Milestone 2 Adversarial Torn Write & Tail Repair Suite..."
	./$(ADV_WAL_BIN)

test_protocol: $(TEST_PROTO_BIN)
	@echo "Running Milestone 3 Protocol Unit Test Suite..."
	./$(TEST_PROTO_BIN)

test_adversarial_protocol: $(ADV_PROTO_BIN)
	@echo "Running Milestone 3 Adversarial Protocol Test Suite..."
	./$(ADV_PROTO_BIN)

test_server: $(TEST_SERVER_BIN)
	@echo "Running Milestone 3 TCP Server Unit Test Suite..."
	./$(TEST_SERVER_BIN)

test_adversarial_server: $(ADV_SERVER_BIN)
	@echo "Running Milestone 3 Adversarial Server Concurrency & Resilience Suite..."
	./$(ADV_SERVER_BIN)

test_benchmark: $(TEST_BENCH_BIN)
	@echo "Running Milestone 4 Benchmark Suite..."
	./$(TEST_BENCH_BIN)

test_adversarial_benchmark: $(ADV_BENCH_BIN)
	@echo "Running Milestone 4 Adversarial Benchmark Suite..."
	./$(ADV_BENCH_BIN)

test: test_engine test_stress test_adversarial test_wal test_adversarial_wal \
      test_protocol test_adversarial_protocol test_server test_adversarial_server \
      test_benchmark test_adversarial_benchmark

clean:
	@rm -rf $(OBJ_DIR) $(BIN_DIR)
	@echo "Cleaned build artifacts."
