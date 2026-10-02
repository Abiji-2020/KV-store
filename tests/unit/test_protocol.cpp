#include "kvstore/protocol.hpp"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        std::cerr << "Assertion failed: " #cond " at " << __FILE__ << ":" << __LINE__ << std::endl; \
        std::exit(1); \
    } \
} while(0)

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        std::cerr << "Assertion failed: " #a " == " #b " at " << __FILE__ << ":" << __LINE__ \
                  << " [Actual: '" << (a) << "' vs Expected: '" << (b) << "']" << std::endl; \
        std::exit(1); \
    } \
} while(0)

void test_inline_basic_commands() {
    kvstore::Command cmd;
    size_t consumed = 0;

    // 1. SET
    ASSERT_TRUE(kvstore::Protocol::parse("SET mykey myvalue\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::SET);
    ASSERT_EQ(cmd.key, "mykey");
    ASSERT_EQ(cmd.value, "myvalue");
    ASSERT_EQ(consumed, 19);

    // 2. GET
    ASSERT_TRUE(kvstore::Protocol::parse("GET mykey\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::GET);
    ASSERT_EQ(cmd.key, "mykey");
    ASSERT_EQ(consumed, 11);

    // 3. DEL
    ASSERT_TRUE(kvstore::Protocol::parse("DEL mykey\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::DEL);
    ASSERT_EQ(cmd.key, "mykey");
    ASSERT_EQ(consumed, 11);

    // 4. PING (bare)
    ASSERT_TRUE(kvstore::Protocol::parse("PING\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::PING);
    ASSERT_EQ(cmd.key, "");
    ASSERT_EQ(consumed, 6);

    // 5. PING (with message)
    ASSERT_TRUE(kvstore::Protocol::parse("PING hello_world\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::PING);
    ASSERT_EQ(cmd.key, "hello_world");
    ASSERT_EQ(consumed, 18);

    // 6. STATS
    ASSERT_TRUE(kvstore::Protocol::parse("STATS\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::STATS);
    ASSERT_EQ(consumed, 7);

    // 7. QUIT
    ASSERT_TRUE(kvstore::Protocol::parse("QUIT\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::QUIT);
    ASSERT_EQ(consumed, 6);

    // 8. CLEAR
    ASSERT_TRUE(kvstore::Protocol::parse("CLEAR\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::CLEAR);
    ASSERT_EQ(consumed, 7);

    // 9. SYNC
    ASSERT_TRUE(kvstore::Protocol::parse("SYNC\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::SYNC);
    ASSERT_EQ(consumed, 6);

    std::cout << "[PASS] test_inline_basic_commands" << std::endl;
}

void test_inline_case_and_whitespace() {
    kvstore::Command cmd;
    size_t consumed = 0;

    // Case-insensitivity on verbs, case-preservation on keys and values
    ASSERT_TRUE(kvstore::Protocol::parse("sEt MyCaSeKey MyCaSeValue\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::SET);
    ASSERT_EQ(cmd.key, "MyCaSeKey");
    ASSERT_EQ(cmd.value, "MyCaSeValue");

    ASSERT_TRUE(kvstore::Protocol::parse("gEt MyCaSeKey\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::GET);
    ASSERT_EQ(cmd.key, "MyCaSeKey");

    ASSERT_TRUE(kvstore::Protocol::parse("dEl MyCaSeKey\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::DEL);
    ASSERT_EQ(cmd.key, "MyCaSeKey");

    // Multiple contiguous spaces and tabs
    ASSERT_TRUE(kvstore::Protocol::parse("SET    space_k   \t  space_v   \r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::SET);
    ASSERT_EQ(cmd.key, "space_k");
    ASSERT_EQ(cmd.value, "space_v");

    std::cout << "[PASS] test_inline_case_and_whitespace" << std::endl;
}

void test_inline_quotes_handling() {
    kvstore::Command cmd;
    size_t consumed = 0;

    ASSERT_TRUE(kvstore::Protocol::parse("SET \"hello world\" \"foo bar baz\"\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::SET);
    ASSERT_EQ(cmd.key, "hello world");
    ASSERT_EQ(cmd.value, "foo bar baz");

    ASSERT_TRUE(kvstore::Protocol::parse("GET \"hello world\"\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::GET);
    ASSERT_EQ(cmd.key, "hello world");

    std::cout << "[PASS] test_inline_quotes_handling" << std::endl;
}

void test_fragmented_stream_accumulation() {
    kvstore::Command cmd;
    size_t consumed = 0;

    // Incomplete inline command (no CRLF yet)
    ASSERT_TRUE(!kvstore::Protocol::parse("SE", cmd, consumed));
    ASSERT_EQ(consumed, 0);

    ASSERT_TRUE(!kvstore::Protocol::parse("SET part_k", cmd, consumed));
    ASSERT_EQ(consumed, 0);

    // Complete line arrives
    ASSERT_TRUE(kvstore::Protocol::parse("SET part_k part_v\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::SET);
    ASSERT_EQ(cmd.key, "part_k");
    ASSERT_EQ(cmd.value, "part_v");
    ASSERT_EQ(consumed, 19);

    // Byte-by-byte streaming
    std::string stream;
    std::string full_cmd = "PING stream_probe\r\n";
    for (size_t i = 0; i < full_cmd.size(); ++i) {
        stream.push_back(full_cmd[i]);
        if (i < full_cmd.size() - 1) {
            ASSERT_TRUE(!kvstore::Protocol::parse(stream, cmd, consumed));
            ASSERT_EQ(consumed, 0);
        } else {
            ASSERT_TRUE(kvstore::Protocol::parse(stream, cmd, consumed));
            ASSERT_EQ(cmd.type, kvstore::CommandType::PING);
            ASSERT_EQ(cmd.key, "stream_probe");
            ASSERT_EQ(consumed, full_cmd.size());
        }
    }

    std::cout << "[PASS] test_fragmented_stream_accumulation" << std::endl;
}

void test_pipelined_commands() {
    std::string buffer = "SET pipe1 v1\r\nSET pipe2 v2\r\nGET pipe1\r\nQUIT\r\n";
    kvstore::Command cmd;
    size_t consumed = 0;

    // 1. SET pipe1 v1
    ASSERT_TRUE(kvstore::Protocol::parse(buffer, cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::SET);
    ASSERT_EQ(cmd.key, "pipe1");
    ASSERT_EQ(cmd.value, "v1");
    buffer.erase(0, consumed);

    // 2. SET pipe2 v2
    ASSERT_TRUE(kvstore::Protocol::parse(buffer, cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::SET);
    ASSERT_EQ(cmd.key, "pipe2");
    ASSERT_EQ(cmd.value, "v2");
    buffer.erase(0, consumed);

    // 3. GET pipe1
    ASSERT_TRUE(kvstore::Protocol::parse(buffer, cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::GET);
    ASSERT_EQ(cmd.key, "pipe1");
    buffer.erase(0, consumed);

    // 4. QUIT
    ASSERT_TRUE(kvstore::Protocol::parse(buffer, cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::QUIT);
    buffer.erase(0, consumed);

    ASSERT_TRUE(buffer.empty());
    ASSERT_TRUE(!kvstore::Protocol::parse(buffer, cmd, consumed));

    std::cout << "[PASS] test_pipelined_commands" << std::endl;
}

void test_resp2_parsing() {
    kvstore::Command cmd;
    size_t consumed = 0;

    // 1. Basic RESP2 SET
    std::string resp_set = "*3\r\n$3\r\nSET\r\n$6\r\nresp_k\r\n$6\r\nresp_v\r\n";
    ASSERT_TRUE(kvstore::Protocol::parse(resp_set, cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::SET);
    ASSERT_EQ(cmd.key, "resp_k");
    ASSERT_EQ(cmd.value, "resp_v");
    ASSERT_TRUE(cmd.is_resp);
    ASSERT_EQ(consumed, resp_set.size());

    // 2. RESP2 GET
    std::string resp_get = "*2\r\n$3\r\nGET\r\n$6\r\nresp_k\r\n";
    ASSERT_TRUE(kvstore::Protocol::parse(resp_get, cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::GET);
    ASSERT_EQ(cmd.key, "resp_k");
    ASSERT_TRUE(cmd.is_resp);
    ASSERT_EQ(consumed, resp_get.size());

    // 3. RESP2 DEL
    std::string resp_del = "*2\r\n$3\r\nDEL\r\n$6\r\nresp_k\r\n";
    ASSERT_TRUE(kvstore::Protocol::parse(resp_del, cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::DEL);
    ASSERT_EQ(cmd.key, "resp_k");
    ASSERT_TRUE(cmd.is_resp);
    ASSERT_EQ(consumed, resp_del.size());

    // 4. RESP2 with embedded CRLF in value
    std::string val_with_crlf = "part1\r\npart2\r\npart3";
    std::string resp_crlf = "*3\r\n$3\r\nSET\r\n$5\r\ncrlfk\r\n$" +
                            std::to_string(val_with_crlf.size()) + "\r\n" +
                            val_with_crlf + "\r\n";
    ASSERT_TRUE(kvstore::Protocol::parse(resp_crlf, cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::SET);
    ASSERT_EQ(cmd.key, "crlfk");
    ASSERT_EQ(cmd.value, val_with_crlf);
    ASSERT_TRUE(cmd.is_resp);
    ASSERT_EQ(consumed, resp_crlf.size());

    // 5. RESP2 empty string value ($0\r\n\r\n)
    std::string resp_empty = "*3\r\n$3\r\nSET\r\n$3\r\nevk\r\n$0\r\n\r\n";
    ASSERT_TRUE(kvstore::Protocol::parse(resp_empty, cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::SET);
    ASSERT_EQ(cmd.key, "evk");
    ASSERT_EQ(cmd.value, "");
    ASSERT_TRUE(cmd.is_resp);
    ASSERT_EQ(consumed, resp_empty.size());

    // 6. Standalone Bulk String Command ($4\r\nPING\r\n)
    std::string bulk_ping = "$4\r\nPING\r\n";
    ASSERT_TRUE(kvstore::Protocol::parse(bulk_ping, cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::PING);
    ASSERT_TRUE(cmd.is_resp);
    ASSERT_EQ(consumed, bulk_ping.size());

    std::cout << "[PASS] test_resp2_parsing" << std::endl;
}

void test_response_serialization() {
    // Inline serialization contract tests
    kvstore::Response r_ok{kvstore::Response::Status::OK, "", false, false};
    ASSERT_EQ(r_ok.serialize(), "+OK\r\n");

    kvstore::Response r_val{kvstore::Response::Status::VALUE, "my_data", false, false};
    ASSERT_EQ(r_val.serialize(), "+VALUE my_data\r\n");

    kvstore::Response r_del{kvstore::Response::Status::DELETED, "", false, true};
    ASSERT_EQ(r_del.serialize(), "+OK DELETED\r\n");

    kvstore::Response r_nf{kvstore::Response::Status::NOT_FOUND, "", false, false};
    ASSERT_EQ(r_nf.serialize(), "-ERR NOT_FOUND\r\n");

    kvstore::Response r_pong{kvstore::Response::Status::PONG, "", false, false};
    ASSERT_EQ(r_pong.serialize(), "+PONG\r\n");

    kvstore::Response r_pong_msg{kvstore::Response::Status::PONG, "hello", false, false};
    ASSERT_EQ(r_pong_msg.serialize(), "+hello\r\n");

    kvstore::Response r_stats{kvstore::Response::Status::STATS, "keys=10", false, false};
    ASSERT_EQ(r_stats.serialize(), "+STATS keys=10\r\n");

    kvstore::Response r_err{kvstore::Response::Status::ERR, "unknown command", false, false};
    ASSERT_EQ(r_err.serialize(), "-ERR unknown command\r\n");

    kvstore::Response r_quit{kvstore::Response::Status::QUIT, "", false, false};
    ASSERT_EQ(r_quit.serialize(), "+OK\r\n");

    // RESP2 serialization tests
    kvstore::Response r_resp_val{kvstore::Response::Status::VALUE, "resp_val", true, false};
    ASSERT_EQ(r_resp_val.serialize_resp2(), "$8\r\nresp_val\r\n");

    kvstore::Response r_resp_nf{kvstore::Response::Status::NOT_FOUND, "", true, false};
    ASSERT_EQ(r_resp_nf.serialize_resp2(), "$-1\r\n");

    kvstore::Response r_resp_del_ok{kvstore::Response::Status::DELETED, "", true, true};
    ASSERT_EQ(r_resp_del_ok.serialize_resp2(), ":1\r\n");

    kvstore::Response r_resp_del_nf{kvstore::Response::Status::NOT_FOUND, "", true, true};
    ASSERT_EQ(r_resp_del_nf.serialize_resp2(), ":0\r\n");

    kvstore::Response r_resp_ok{kvstore::Response::Status::OK, "", true, false};
    ASSERT_EQ(r_resp_ok.serialize_resp2(), "+OK\r\n");

    std::cout << "[PASS] test_response_serialization" << std::endl;
}

int main() {
    std::cout << "==================================================" << std::endl;
    std::cout << "RUNNING PROTOCOL UNIT TESTS" << std::endl;
    std::cout << "==================================================" << std::endl;

    test_inline_basic_commands();
    test_inline_case_and_whitespace();
    test_inline_quotes_handling();
    test_fragmented_stream_accumulation();
    test_pipelined_commands();
    test_resp2_parsing();
    test_response_serialization();

    std::cout << "==================================================" << std::endl;
    std::cout << "ALL PROTOCOL UNIT TESTS PASSED" << std::endl;
    std::cout << "==================================================" << std::endl;
    return 0;
}
