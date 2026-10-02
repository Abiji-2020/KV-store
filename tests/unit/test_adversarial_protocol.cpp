#include "kvstore/protocol.hpp"

#include <cassert>
#include <chrono>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        std::cerr << "Assertion failed: " #cond " at " << __FILE__ << ":" << __LINE__ << std::endl; \
        std::exit(1); \
    } \
} while(0)

#define ASSERT_FALSE(cond) do { \
    if (cond) { \
        std::cerr << "Assertion failed (expected false): " #cond " at " << __FILE__ << ":" << __LINE__ << std::endl; \
        std::exit(1); \
    } \
} while(0)

#define ASSERT_EQ(a, b) do { \
    if (!((a) == (b))) { \
        std::cerr << "Assertion failed: " #a " == " #b " at " << __FILE__ << ":" << __LINE__ << std::endl; \
        std::exit(1); \
    } \
} while(0)

// ==============================================================================
// Scenario 1: Fragmented Byte-by-Byte Streaming Across 10,000 Bytes
// ==============================================================================
void test_adversarial_fragmented_streaming_10k() {
    std::cout << "--- [Scenario 1] Fragmented Byte-by-Byte Streaming (10,000 bytes) ---" << std::endl;

    // 1.1 RESP2 Byte-by-Byte Streaming of a 10,000 byte command
    std::string val_10k(9900, 'X');
    std::string full_cmd = "*3\r\n$3\r\nSET\r\n$8\r\nfrag_key\r\n$" +
                           std::to_string(val_10k.size()) + "\r\n" +
                           val_10k + "\r\n";

    ASSERT_TRUE(full_cmd.size() > 9900);
    const size_t total_len = full_cmd.size();

    std::string stream_acc;
    stream_acc.reserve(total_len + 16);
    kvstore::Command cmd;
    size_t consumed = 0;

    // Feed one byte at a time up to total_len - 1. Must return false and consumed == 0.
    for (size_t i = 0; i < total_len - 1; ++i) {
        stream_acc.push_back(full_cmd[i]);
        bool ok = kvstore::Protocol::parse(stream_acc, cmd, consumed);
        ASSERT_FALSE(ok);
        ASSERT_EQ(consumed, 0);
    }

    // Append the final byte ('\n'). Now it must return true and consume all bytes.
    stream_acc.push_back(full_cmd[total_len - 1]);
    bool ok = kvstore::Protocol::parse(stream_acc, cmd, consumed);
    ASSERT_TRUE(ok);
    ASSERT_EQ(consumed, total_len);
    ASSERT_EQ(cmd.type, kvstore::CommandType::SET);
    ASSERT_EQ(cmd.key, "frag_key");
    ASSERT_EQ(cmd.value.size(), val_10k.size());
    ASSERT_EQ(cmd.value, val_10k);
    ASSERT_TRUE(cmd.is_resp);

    // 1.2 Consecutive multi-command byte-by-byte streaming
    std::vector<std::string> cmds = {
        "SET k1 v1\r\n",
        "GET k1\r\n",
        "DEL k1\r\n",
        "PING hello\r\n",
        "STATS\r\n"
    };

    std::string multi_stream;
    size_t parsed_count = 0;
    for (const auto& c : cmds) {
        for (char ch : c) {
            multi_stream.push_back(ch);
            if (kvstore::Protocol::parse(multi_stream, cmd, consumed)) {
                multi_stream.erase(0, consumed);
                ++parsed_count;
            }
        }
    }
    ASSERT_EQ(parsed_count, cmds.size());
    ASSERT_TRUE(multi_stream.empty());

    std::cout << "  Passed: 10,000+ byte fragmented stream accumulated cleanly without premature triggers." << std::endl;
}

// ==============================================================================
// Scenario 2: Pipelined Bursts of 1,000+ Commands in a Single TCP Buffer
// ==============================================================================
void test_adversarial_pipelined_burst_1500() {
    std::cout << "--- [Scenario 2] Pipelined Bursts (1,500 commands in single buffer) ---" << std::endl;

    const size_t NUM_COMMANDS = 1500;
    std::string big_buffer;
    big_buffer.reserve(NUM_COMMANDS * 48);

    // Interleave inline and RESP2 commands
    for (size_t i = 0; i < NUM_COMMANDS; ++i) {
        if (i % 3 == 0) {
            // Inline SET
            big_buffer += "SET k" + std::to_string(i) + " v" + std::to_string(i) + "\r\n";
        } else if (i % 3 == 1) {
            // RESP2 GET
            std::string k = "k" + std::to_string(i - 1);
            big_buffer += "*2\r\n$3\r\nGET\r\n$" + std::to_string(k.size()) + "\r\n" + k + "\r\n";
        } else {
            // Inline PING
            big_buffer += "PING burst" + std::to_string(i) + "\r\n";
        }
    }

    auto start_time = std::chrono::steady_clock::now();

    size_t count = 0;
    kvstore::Command cmd;
    size_t consumed = 0;
    std::string_view buf_view(big_buffer);

    while (kvstore::Protocol::parse(buf_view, cmd, consumed)) {
        buf_view.remove_prefix(consumed);

        if (count % 3 == 0) {
            ASSERT_EQ(cmd.type, kvstore::CommandType::SET);
            ASSERT_EQ(cmd.key, "k" + std::to_string(count));
            ASSERT_EQ(cmd.value, "v" + std::to_string(count));
            ASSERT_FALSE(cmd.is_resp);
        } else if (count % 3 == 1) {
            ASSERT_EQ(cmd.type, kvstore::CommandType::GET);
            ASSERT_EQ(cmd.key, "k" + std::to_string(count - 1));
            ASSERT_TRUE(cmd.is_resp);
        } else {
            ASSERT_EQ(cmd.type, kvstore::CommandType::PING);
            ASSERT_EQ(cmd.key, "burst" + std::to_string(count));
        }

        ++count;
    }

    auto end_time = std::chrono::steady_clock::now();
    auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

    ASSERT_EQ(count, NUM_COMMANDS);
    ASSERT_TRUE(buf_view.empty());

    std::cout << "  Passed: " << count << " pipelined commands parsed in " << elapsed_us << " us ("
              << (count * 1000000ULL / (elapsed_us + 1)) << " cmds/sec)." << std::endl;
}

// ==============================================================================
// Scenario 3: Embedded \r\n Characters Inside RESP2 Bulk Strings
// ==============================================================================
void test_adversarial_embedded_crlf_resp2() {
    std::cout << "--- [Scenario 3] Embedded \\r\\n Characters in RESP2 Bulk Strings ---" << std::endl;

    // Value containing combinations of \r, \n, \r\n, \r\n\r\n, and null bytes
    std::string tricky_val = std::string("line1\r\nline2\rline3\nline4\r\n\r\n\r\nEND\0AFTER_NULL", 44);
    std::string tricky_cmd = "*3\r\n$3\r\nSET\r\n$9\r\ntrickykey\r\n$" +
                             std::to_string(tricky_val.size()) + "\r\n" +
                             tricky_val + "\r\n";
    kvstore::Command tricky_out;
    size_t tricky_consumed = 0;
    bool tricky_ok = kvstore::Protocol::parse(tricky_cmd, tricky_out, tricky_consumed);
    ASSERT_TRUE(tricky_ok);
    ASSERT_EQ(tricky_consumed, tricky_cmd.size());
    ASSERT_EQ(tricky_out.value, tricky_val);
    ASSERT_EQ(tricky_out.value.size(), 44);

    std::string raw_val = "PREFIX\r\nMIDDLE\r\n\r\nSUFFIX\r\n";
    std::string resp_cmd = "*3\r\n$3\r\nSET\r\n$7\r\ncrlfkey\r\n$" +
                           std::to_string(raw_val.size()) + "\r\n" +
                           raw_val + "\r\n";

    kvstore::Command cmd;
    size_t consumed = 0;
    bool ok = kvstore::Protocol::parse(resp_cmd, cmd, consumed);
    ASSERT_TRUE(ok);
    ASSERT_EQ(consumed, resp_cmd.size());
    ASSERT_EQ(cmd.type, kvstore::CommandType::SET);
    ASSERT_EQ(cmd.key, "crlfkey");
    ASSERT_EQ(cmd.value, raw_val);
    ASSERT_TRUE(cmd.is_resp);

    // Serialization round-trip
    kvstore::Response resp{kvstore::Response::Status::VALUE, raw_val, true, false};
    std::string serialized = resp.serialize_resp2();
    std::string expected_ser = "$" + std::to_string(raw_val.size()) + "\r\n" + raw_val + "\r\n";
    ASSERT_EQ(serialized, expected_ser);

    // Verify key itself can contain \r\n in RESP2
    std::string key_with_crlf = "key\r\nwith\r\nnewline";
    std::string cmd_crlf_key = "*3\r\n$3\r\nSET\r\n$" +
                               std::to_string(key_with_crlf.size()) + "\r\n" +
                               key_with_crlf + "\r\n$5\r\nvalue\r\n";
    ok = kvstore::Protocol::parse(cmd_crlf_key, cmd, consumed);
    ASSERT_TRUE(ok);
    ASSERT_EQ(cmd.key, key_with_crlf);
    ASSERT_EQ(cmd.value, "value");

    std::cout << "  Passed: Embedded \\r\\n within RESP2 bulk strings preserved with exact length-prefix fidelity." << std::endl;
}

// ==============================================================================
// Scenario 4: 1MB to 4MB Large Payload Handling
// ==============================================================================
void test_adversarial_large_payload_1mb_to_4mb() {
    std::cout << "--- [Scenario 4] 1MB to 4MB Large Payload Handling ---" << std::endl;

    const std::vector<size_t> test_sizes = {
        1 * 1024 * 1024, // 1 MB
        2 * 1024 * 1024, // 2 MB
        4 * 1024 * 1024  // 4 MB
    };

    for (size_t sz : test_sizes) {
        std::string large_val(sz, 'A' + static_cast<char>((sz / (1024 * 1024)) % 26));
        // Add distinctive pattern at start and end
        large_val.replace(0, 5, "START");
        large_val.replace(sz - 3, 3, "END");

        std::string resp_cmd = "*3\r\n$3\r\nSET\r\n$7\r\nlarge_k\r\n$" +
                               std::to_string(sz) + "\r\n" +
                               large_val + "\r\n";

        kvstore::Command cmd;
        size_t consumed = 0;
        auto t0 = std::chrono::steady_clock::now();
        bool ok = kvstore::Protocol::parse(resp_cmd, cmd, consumed);
        auto t1 = std::chrono::steady_clock::now();
        auto dur_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

        ASSERT_TRUE(ok);
        ASSERT_EQ(consumed, resp_cmd.size());
        ASSERT_EQ(cmd.type, kvstore::CommandType::SET);
        ASSERT_EQ(cmd.key, "large_k");
        ASSERT_EQ(cmd.value.size(), sz);
        ASSERT_EQ(cmd.value.substr(0, 5), "START");
        ASSERT_EQ(cmd.value.substr(sz - 3, 3), "END");

        // Verify RESP2 serialization round trip
        kvstore::Response r{kvstore::Response::Status::VALUE, cmd.value, true, false};
        std::string ser = r.serialize_resp2();
        ASSERT_EQ(ser.size(), sz + std::to_string(sz).size() + 5);

        std::cout << "  Passed: " << (sz / (1024 * 1024)) << " MB payload parsed and serialized in "
                  << dur_ms << " ms." << std::endl;
    }

    // Test inline command length boundary (>64KB without newline)
    std::string giant_inline(65 * 1024, 'Z');
    kvstore::Command cmd;
    size_t consumed = 0;
    bool ok = kvstore::Protocol::parse(giant_inline, cmd, consumed);
    // Should gracefully consume and reject with UNKNOWN
    ASSERT_TRUE(ok);
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);
    ASSERT_EQ(consumed, giant_inline.size());
    std::cout << "  Passed: Inline command exceeding MAX_INLINE_LINE_LEN rejected cleanly without hanging." << std::endl;
}

// ==============================================================================
// Scenario 5: Malformed Protocol Inputs
// ==============================================================================
void test_adversarial_malformed_inputs() {
    std::cout << "--- [Scenario 5] Malformed Protocol Inputs ---" << std::endl;

    kvstore::Command cmd;
    size_t consumed = 0;

    // 5.1 Partial lines (must return false, consumed == 0)
    ASSERT_FALSE(kvstore::Protocol::parse("SET key_without_val", cmd, consumed));
    ASSERT_EQ(consumed, 0);

    ASSERT_FALSE(kvstore::Protocol::parse("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n", cmd, consumed));
    ASSERT_EQ(consumed, 0);

    ASSERT_FALSE(kvstore::Protocol::parse("*1\r\n$10\r\nshort\r\n", cmd, consumed));
    ASSERT_EQ(consumed, 0);

    // 5.2 Invalid RESP2 array prefixes and counts
    // Non-numeric multibulk count
    ASSERT_TRUE(kvstore::Protocol::parse("*abc\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);
    ASSERT_EQ(consumed, 6);

    // Zero multibulk count (*0\r\n)
    ASSERT_TRUE(kvstore::Protocol::parse("*0\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);
    ASSERT_EQ(consumed, 4);

    // Negative multibulk count (*-1\r\n)
    ASSERT_TRUE(kvstore::Protocol::parse("*-1\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);
    ASSERT_EQ(consumed, 5);

    // Negative multibulk count (*-999\r\n)
    ASSERT_TRUE(kvstore::Protocol::parse("*-999\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);
    ASSERT_EQ(consumed, 7);

    // Int64 overflow in array count
    ASSERT_TRUE(kvstore::Protocol::parse("*99999999999999999999999999999999\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);

    // Array count exceeding MAX_RESP_ARRAY_LEN (64)
    ASSERT_TRUE(kvstore::Protocol::parse("*100\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);
    ASSERT_EQ(consumed, 6);

    // 5.3 Non-numeric and invalid bulk lengths
    // Non-numeric bulk length inside array
    ASSERT_TRUE(kvstore::Protocol::parse("*1\r\n$abc\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);

    // Negative bulk length inside array
    ASSERT_TRUE(kvstore::Protocol::parse("*1\r\n$-1\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);

    // Int64 overflow in bulk length
    ASSERT_TRUE(kvstore::Protocol::parse("*1\r\n$99999999999999999999999999999999\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);

    // Bulk length exceeding MAX_RESP_BULK_LEN (64 MB)
    ASSERT_TRUE(kvstore::Protocol::parse("*1\r\n$70000000\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);

    // Non-numeric in standalone bulk string
    ASSERT_TRUE(kvstore::Protocol::parse("$notanumber\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);

    // 5.4 Corrupted bulk terminator
    // Missing bulk CRLF terminator (has XX instead of \r\n)
    ASSERT_TRUE(kvstore::Protocol::parse("*1\r\n$3\r\nFOOXX", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);

    // Expected '$' delimiter missing
    ASSERT_TRUE(kvstore::Protocol::parse("*2\r\n$3\r\nGET\r\n:123\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);

    // 5.5 Unknown command verbs
    // Inline unknown verb
    ASSERT_TRUE(kvstore::Protocol::parse("FLUSHALL\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);
    ASSERT_EQ(cmd.raw_line, "unknown command 'FLUSHALL'");

    ASSERT_TRUE(kvstore::Protocol::parse("HSET myhash field val\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);
    ASSERT_EQ(cmd.raw_line, "unknown command 'HSET'");

    // RESP2 unknown verb
    ASSERT_TRUE(kvstore::Protocol::parse("*1\r\n$8\r\nFLUSHALL\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);
    ASSERT_EQ(cmd.raw_line, "unknown command 'FLUSHALL'");

    // 5.6 Wrong number of arguments
    // SET missing key & value
    ASSERT_TRUE(kvstore::Protocol::parse("SET\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);

    // SET missing value
    ASSERT_TRUE(kvstore::Protocol::parse("SET single_arg\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);

    // GET missing key
    ASSERT_TRUE(kvstore::Protocol::parse("GET\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);

    // GET too many arguments
    ASSERT_TRUE(kvstore::Protocol::parse("GET key1 key2\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);

    // DEL missing key
    ASSERT_TRUE(kvstore::Protocol::parse("DEL\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);

    // RESP2 SET wrong args (2 args instead of 3)
    ASSERT_TRUE(kvstore::Protocol::parse("*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);

    // RESP2 GET wrong args (3 args instead of 2)
    ASSERT_TRUE(kvstore::Protocol::parse("*3\r\n$3\r\nGET\r\n$1\r\na\r\n$1\r\nb\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);

    // 5.7 Bare garbage inputs
    ASSERT_TRUE(kvstore::Protocol::parse("!@#$%^&*()_+~`\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);

    // Leading bare newlines (empty lines)
    ASSERT_TRUE(kvstore::Protocol::parse("\r\n\r\n\r\n", cmd, consumed));
    ASSERT_EQ(cmd.type, kvstore::CommandType::UNKNOWN);
    ASSERT_TRUE(cmd.raw_line.empty());
    ASSERT_EQ(consumed, 6);

    std::cout << "  Passed: All malformed inputs (invalid prefixes, overflows, negative counts, bad lengths, unknown verbs) cleanly rejected." << std::endl;
}

// ==============================================================================
// Scenario 6: Response Serialization Contract & Round-Trip Oracles
// ==============================================================================
void test_adversarial_response_oracles() {
    std::cout << "--- [Scenario 6] Response Serialization & Oracle Invariants ---" << std::endl;

    // Contract tests for Protocol::format_response
    kvstore::Response r_ok{kvstore::Response::Status::OK, "", false, false};
    ASSERT_EQ(kvstore::Protocol::format_response(r_ok, false), "+OK\r\n");
    ASSERT_EQ(kvstore::Protocol::format_response(r_ok, true), "+OK\r\n");

    kvstore::Response r_val{kvstore::Response::Status::VALUE, "sample_val", false, false};
    ASSERT_EQ(kvstore::Protocol::format_response(r_val, false), "+VALUE sample_val\r\n");
    ASSERT_EQ(kvstore::Protocol::format_response(r_val, true), "$10\r\nsample_val\r\n");

    kvstore::Response r_del_inline{kvstore::Response::Status::DELETED, "", false, true};
    ASSERT_EQ(kvstore::Protocol::format_response(r_del_inline, false), "+OK DELETED\r\n");
    ASSERT_EQ(kvstore::Protocol::format_response(r_del_inline, true), ":1\r\n");

    kvstore::Response r_nf_inline{kvstore::Response::Status::NOT_FOUND, "NOT_FOUND", false, false};
    ASSERT_EQ(kvstore::Protocol::format_response(r_nf_inline, false), "-ERR NOT_FOUND\r\n");
    ASSERT_EQ(kvstore::Protocol::format_response(r_nf_inline, true), "$-1\r\n");

    kvstore::Response r_nf_del{kvstore::Response::Status::NOT_FOUND, "NOT_FOUND", false, true};
    ASSERT_EQ(kvstore::Protocol::format_response(r_nf_del, false), "-ERR NOT_FOUND\r\n");
    ASSERT_EQ(kvstore::Protocol::format_response(r_nf_del, true), ":0\r\n");

    kvstore::Response r_pong_bare{kvstore::Response::Status::PONG, "", false, false};
    ASSERT_EQ(kvstore::Protocol::format_response(r_pong_bare, false), "+PONG\r\n");
    ASSERT_EQ(kvstore::Protocol::format_response(r_pong_bare, true), "+PONG\r\n");

    kvstore::Response r_pong_msg{kvstore::Response::Status::PONG, "custom_pong", false, false};
    ASSERT_EQ(kvstore::Protocol::format_response(r_pong_msg, false), "+custom_pong\r\n");
    ASSERT_EQ(kvstore::Protocol::format_response(r_pong_msg, true), "$11\r\ncustom_pong\r\n");

    std::cout << "  Passed: All response serializers match PROJECT.md and e2e_contracts_test.cpp specification." << std::endl;
}

int main() {
    std::cout << "=======================================================" << std::endl;
    std::cout << "  KVStore Milestone 3 Adversarial Protocol Test Suite  " << std::endl;
    std::cout << "=======================================================" << std::endl;

    auto start_all = std::chrono::steady_clock::now();

    test_adversarial_fragmented_streaming_10k();
    test_adversarial_pipelined_burst_1500();
    test_adversarial_embedded_crlf_resp2();
    test_adversarial_large_payload_1mb_to_4mb();
    test_adversarial_malformed_inputs();
    test_adversarial_response_oracles();

    auto end_all = std::chrono::steady_clock::now();
    auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_all - start_all).count();

    std::cout << "-------------------------------------------------------" << std::endl;
    std::cout << "Adversarial Protocol Suite Elapsed Time: " << total_ms << " ms" << std::endl;
    std::cout << ">>> ALL ADVERSARIAL PROTOCOL TESTS PASSED! <<<" << std::endl;
    std::cout << "-------------------------------------------------------" << std::endl;
    return 0;
}
