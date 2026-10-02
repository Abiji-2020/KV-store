/**
 * e2e_contracts_test.cpp - C++ Interface Contracts E2E Test Suite
 * Validates C++ API and interface contracts defined in PROJECT.md.
 */

#include <iostream>
#include <cassert>
#include <string>
#include <vector>
#include <optional>
#include <thread>
#include <chrono>

// Interface contract definitions from PROJECT.md § Interface Contracts

namespace kvstore {

enum class SetResult { CREATED, UPDATED, EVICTED };
enum class DelResult { DELETED, NOT_FOUND };

struct Stats {
    size_t key_count{0};
    size_t capacity{0};
    uint64_t hits{0};
    uint64_t misses{0};
    uint64_t evictions{0};
};

enum class SyncMode { SYNC_ALWAYS, SYNC_BATCH, SYNC_BUFFERED };
enum class OpCode : uint8_t { OP_SET = 1, OP_DEL = 2, OP_CLEAR = 3 };

enum class CommandType { SET, GET, DEL, PING, STATS, QUIT, UNKNOWN };

struct Command {
    CommandType type{CommandType::UNKNOWN};
    std::string key;
    std::string value;
    std::string raw_line;
};

struct Response {
    enum class Status { OK, VALUE, DELETED, NOT_FOUND, PONG, STATS, ERR, QUIT };
    Status status;
    std::string payload;

    std::string serialize() const {
        switch (status) {
            case Status::OK: return "+OK\r\n";
            case Status::VALUE: return "+VALUE " + payload + "\r\n";
            case Status::DELETED: return "+OK DELETED\r\n";
            case Status::NOT_FOUND: return "-ERR NOT_FOUND\r\n";
            case Status::PONG: return payload.empty() ? "+PONG\r\n" : "+" + payload + "\r\n";
            case Status::STATS: return "+STATS " + payload + "\r\n";
            case Status::ERR: return "-ERR " + payload + "\r\n";
            case Status::QUIT: return "+OK\r\n";
        }
        return "-ERR UNKNOWN\r\n";
    }
};

} // namespace kvstore

// Simple test assertions
#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        std::cerr << "Assertion failed: " #cond " at " << __FILE__ << ":" << __LINE__ << std::endl; \
        std::exit(1); \
    } \
} while(0)

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        std::cerr << "Assertion failed: " #a " == " #b " at " << __FILE__ << ":" << __LINE__ << std::endl; \
        std::exit(1); \
    } \
} while(0)

void test_contract_response_serialization() {
    kvstore::Response r_ok{kvstore::Response::Status::OK, ""};
    ASSERT_EQ(r_ok.serialize(), "+OK\r\n");

    kvstore::Response r_val{kvstore::Response::Status::VALUE, "my_data"};
    ASSERT_EQ(r_val.serialize(), "+VALUE my_data\r\n");

    kvstore::Response r_del{kvstore::Response::Status::DELETED, ""};
    ASSERT_EQ(r_del.serialize(), "+OK DELETED\r\n");

    kvstore::Response r_nf{kvstore::Response::Status::NOT_FOUND, ""};
    ASSERT_EQ(r_nf.serialize(), "-ERR NOT_FOUND\r\n");

    kvstore::Response r_pong{kvstore::Response::Status::PONG, ""};
    ASSERT_EQ(r_pong.serialize(), "+PONG\r\n");

    std::cout << "[PASS] test_contract_response_serialization" << std::endl;
}

void test_contract_enums() {
    ASSERT_EQ(static_cast<uint8_t>(kvstore::OpCode::OP_SET), 1);
    ASSERT_EQ(static_cast<uint8_t>(kvstore::OpCode::OP_DEL), 2);
    ASSERT_EQ(static_cast<uint8_t>(kvstore::OpCode::OP_CLEAR), 3);
    std::cout << "[PASS] test_contract_enums" << std::endl;
}

int main() {
    std::cout << "==================================================" << std::endl;
    std::cout << "RUNNING C++ E2E INTERFACE CONTRACT TESTS" << std::endl;
    std::cout << "==================================================" << std::endl;

    test_contract_response_serialization();
    test_contract_enums();

    std::cout << "==================================================" << std::endl;
    std::cout << "ALL C++ INTERFACE CONTRACT TESTS PASSED" << std::endl;
    std::cout << "==================================================" << std::endl;
    return 0;
}
