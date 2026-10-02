#pragma once

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

namespace kvstore {

/// Wire protocol format types
enum class ProtocolType : uint8_t {
    UNKNOWN = 0,
    INLINE  = 1,
    RESP2   = 2
};

/// Command types supported by the key-value store coordinator
enum class CommandType {
    SET,
    GET,
    DEL,
    PING,
    STATS,
    QUIT,
    CLEAR,
    SYNC,
    UNKNOWN
};

inline std::ostream& operator<<(std::ostream& os, CommandType type) {
    switch (type) {
        case CommandType::SET: return os << "SET";
        case CommandType::GET: return os << "GET";
        case CommandType::DEL: return os << "DEL";
        case CommandType::PING: return os << "PING";
        case CommandType::STATS: return os << "STATS";
        case CommandType::QUIT: return os << "QUIT";
        case CommandType::CLEAR: return os << "CLEAR";
        case CommandType::SYNC: return os << "SYNC";
        case CommandType::UNKNOWN: return os << "UNKNOWN";
    }
    return os << "UNKNOWN";
}

/// Parsed client command representation.
/// Conforms strictly to PROJECT.md § Interface Contracts.
struct Command {
    CommandType type{CommandType::UNKNOWN};
    std::string key;
    std::string value;
    std::string raw_line;
    bool is_resp{false};
    ProtocolType protocol{ProtocolType::UNKNOWN};
};

/// Structured response representation.
/// Conforms strictly to PROJECT.md § Interface Contracts and e2e_contracts_test.cpp.
struct Response {
    enum class Status {
        OK,
        VALUE,
        DELETED,
        NOT_FOUND,
        PONG,
        STATS,
        ERR,
        QUIT
    };

    Status status{Status::OK};
    std::string payload;
    bool is_resp{false}; // Set to true when response is directed to a RESP2 client
    bool is_del{false};  // Set to true if originating command was DEL (for RESP2 integer reply)

    /// Serializes response to inline text wire format.
    /// Strictly verifies ASSERT_EQ expectations in e2e_contracts_test.cpp.
    std::string serialize() const {
        switch (status) {
            case Status::OK:        return "+OK\r\n";
            case Status::VALUE:     return "+VALUE " + payload + "\r\n";
            case Status::DELETED:   return "+OK DELETED\r\n";
            case Status::NOT_FOUND: return "-ERR NOT_FOUND\r\n";
            case Status::PONG:      return payload.empty() ? "+PONG\r\n" : "+" + payload + "\r\n";
            case Status::STATS:     return "+STATS " + payload + "\r\n";
            case Status::ERR:       return "-ERR " + payload + "\r\n";
            case Status::QUIT:      return "+OK\r\n";
        }
        return "-ERR UNKNOWN\r\n";
    }

    /// Serializes response to RESP2 wire format.
    /// Uses length-prefixed bulk strings for GET values ($len\r\nval\r\n) and nil ($-1\r\n),
    /// and integer replies for DEL (:1\r\n or :0\r\n).
    std::string serialize_resp2() const {
        switch (status) {
            case Status::OK:
                return "+OK\r\n";
            case Status::VALUE:
                return "$" + std::to_string(payload.size()) + "\r\n" + payload + "\r\n";
            case Status::DELETED:
                return ":1\r\n";
            case Status::NOT_FOUND:
                return is_del ? ":0\r\n" : "$-1\r\n";
            case Status::PONG:
                return payload.empty() ? "+PONG\r\n" : "$" + std::to_string(payload.size()) + "\r\n" + payload + "\r\n";
            case Status::STATS:
                return "+STATS " + payload + "\r\n";
            case Status::ERR:
                return "-ERR " + payload + "\r\n";
            case Status::QUIT:
                return "+OK\r\n";
        }
        return "-ERR UNKNOWN\r\n";
    }
};

/// High-performance Wire Protocol Parser and Response Serializer.
/// Supports dual-protocol parsing (RESP2 bulk arrays vs inline text commands),
/// partial read stream accumulation, pipelining, quotes, and zero-copy string scanning.
class Protocol {
public:
    static constexpr size_t MAX_INLINE_LINE_LEN = 64 * 1024;        // 64 KB inline limit
    static constexpr size_t MAX_RESP_BULK_LEN   = 64 * 1024 * 1024; // 64 MB RESP2 bulk limit
    static constexpr size_t MAX_RESP_ARRAY_LEN  = 64;               // Maximum arguments per command

    /// Primary interface required by PROJECT.md contract:
    /// Parses next command from buffer. Returns true if complete command was extracted,
    /// or false if buffer contains an incomplete command (partial read).
    static bool parse(std::string_view buffer, Command& out_cmd, size_t& bytes_consumed);

    /// Formats response adhering to PROJECT.md contract
    static std::string format_response(const Response& resp);

    /// Protocol-aware response formatter
    static std::string format_response(const Response& resp, bool is_resp2);

    // Static helper builders for standard response objects
    static Response make_ok();
    static Response make_pong(std::string msg = "");
    static Response make_value(std::string val);
    static Response make_not_found(std::string msg = "NOT_FOUND");
    static Response make_deleted();
    static Response make_stats(std::string stats_payload);
    static Response make_err(std::string error_message);
    static Response make_quit();

private:
    static bool parse_inline(std::string_view buffer, Command& out_cmd, size_t& bytes_consumed);
    static bool parse_resp2(std::string_view buffer, Command& out_cmd, size_t& bytes_consumed);
    static std::vector<std::string> tokenize_inline(std::string_view line);
};

} // namespace kvstore
