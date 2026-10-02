#include "kvstore/protocol.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <string>
#include <string_view>
#include <vector>

namespace kvstore {

namespace {

inline std::string to_upper(std::string_view s) {
    std::string result;
    result.reserve(s.size());
    for (char c : s) {
        result.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
    }
    return result;
}

template <typename T>
inline bool parse_integer(std::string_view s, T& out_val) {
    if (s.empty()) return false;
    const char* start = s.data();
    const char* end = start + s.size();
    auto [ptr, ec] = std::from_chars(start, end, out_val);
    return ec == std::errc{} && ptr == end;
}

} // namespace

Response Protocol::make_ok() {
    return Response{Response::Status::OK, "", false, false};
}

Response Protocol::make_pong(std::string msg) {
    return Response{Response::Status::PONG, std::move(msg), false, false};
}

Response Protocol::make_value(std::string val) {
    return Response{Response::Status::VALUE, std::move(val), false, false};
}

Response Protocol::make_not_found(std::string msg) {
    return Response{Response::Status::NOT_FOUND, std::move(msg), false, false};
}

Response Protocol::make_deleted() {
    return Response{Response::Status::DELETED, "", false, true};
}

Response Protocol::make_stats(std::string stats_payload) {
    return Response{Response::Status::STATS, std::move(stats_payload), false, false};
}

Response Protocol::make_err(std::string error_message) {
    return Response{Response::Status::ERR, std::move(error_message), false, false};
}

Response Protocol::make_quit() {
    return Response{Response::Status::QUIT, "", false, false};
}

std::string Protocol::format_response(const Response& resp) {
    return resp.is_resp ? resp.serialize_resp2() : resp.serialize();
}

std::string Protocol::format_response(const Response& resp, bool is_resp2) {
    return is_resp2 ? resp.serialize_resp2() : resp.serialize();
}

bool Protocol::parse(std::string_view buffer, Command& out_cmd, size_t& bytes_consumed) {
    bytes_consumed = 0;
    out_cmd.type = CommandType::UNKNOWN;
    out_cmd.key.clear();
    out_cmd.value.clear();
    out_cmd.raw_line.clear();
    out_cmd.is_resp = false;
    out_cmd.protocol = ProtocolType::UNKNOWN;

    // Skip leading blank lines / bare CRLF
    size_t lead = 0;
    while (lead < buffer.size() && (buffer[lead] == '\r' || buffer[lead] == '\n')) {
        ++lead;
    }

    if (lead > 0) {
        if (lead == buffer.size()) {
            bytes_consumed = lead;
            out_cmd.type = CommandType::UNKNOWN;
            out_cmd.raw_line.clear();
            return true; // Consume empty line
        }
        buffer.remove_prefix(lead);
    }

    if (buffer.empty()) {
        return false;
    }

    size_t sub_consumed = 0;
    bool ok = false;

    // Discriminate protocol: RESP2 begins with '*' (array) or '$' (bulk)
    if (buffer.front() == '*' || buffer.front() == '$') {
        ok = parse_resp2(buffer, out_cmd, sub_consumed);
    } else {
        ok = parse_inline(buffer, out_cmd, sub_consumed);
    }

    if (ok) {
        bytes_consumed = lead + sub_consumed;
        return true;
    }

    bytes_consumed = 0;
    return false;
}

std::vector<std::string> Protocol::tokenize_inline(std::string_view line) {
    std::vector<std::string> tokens;
    size_t i = 0;
    const size_t len = line.size();

    while (i < len) {
        // Skip whitespace
        while (i < len && (line[i] == ' ' || line[i] == '\t')) {
            ++i;
        }
        if (i >= len) break;

        // Quoted token
        if (line[i] == '"' || line[i] == '\'') {
            char quote = line[i++];
            size_t start = i;
            while (i < len && line[i] != quote) {
                ++i;
            }
            tokens.emplace_back(line.substr(start, i - start));
            if (i < len && line[i] == quote) {
                ++i;
            }
        } else {
            // Unquoted token
            size_t start = i;
            while (i < len && line[i] != ' ' && line[i] != '\t') {
                ++i;
            }
            tokens.emplace_back(line.substr(start, i - start));
        }
    }

    return tokens;
}

bool Protocol::parse_inline(std::string_view buffer, Command& out_cmd, size_t& bytes_consumed) {
    bytes_consumed = 0;
    out_cmd.protocol = ProtocolType::INLINE;
    out_cmd.is_resp = false;

    // Find CRLF or LF line terminator
    size_t newline_pos = buffer.find('\n');
    if (newline_pos == std::string_view::npos) {
        if (buffer.size() > MAX_INLINE_LINE_LEN) {
            bytes_consumed = buffer.size();
            out_cmd.type = CommandType::UNKNOWN;
            out_cmd.raw_line = "inline command line exceeds maximum length";
            return true;
        }
        return false; // Incomplete line, wait for more data
    }

    size_t line_end = newline_pos;
    if (line_end > 0 && buffer[line_end - 1] == '\r') {
        --line_end;
    }

    bytes_consumed = newline_pos + 1;
    std::string_view line = buffer.substr(0, line_end);
    out_cmd.raw_line = std::string(line);

    auto tokens = tokenize_inline(line);
    if (tokens.empty()) {
        out_cmd.type = CommandType::UNKNOWN;
        out_cmd.raw_line.clear();
        return true;
    }

    std::string verb = to_upper(tokens[0]);

    if (verb == "SET") {
        if (tokens.size() < 3) {
            out_cmd.type = CommandType::UNKNOWN;
            out_cmd.raw_line = "wrong number of arguments for 'SET'";
            return true;
        }
        out_cmd.type = CommandType::SET;
        out_cmd.key = std::move(tokens[1]);
        if (tokens.size() == 3) {
            out_cmd.value = std::move(tokens[2]);
        } else {
            std::string joined;
            for (size_t k = 2; k < tokens.size(); ++k) {
                if (k > 2) joined += " ";
                joined += tokens[k];
            }
            out_cmd.value = std::move(joined);
        }
        return true;
    } else if (verb == "GET") {
        if (tokens.size() != 2) {
            out_cmd.type = CommandType::UNKNOWN;
            out_cmd.raw_line = "wrong number of arguments for 'GET'";
            return true;
        }
        out_cmd.type = CommandType::GET;
        out_cmd.key = std::move(tokens[1]);
        return true;
    } else if (verb == "DEL") {
        if (tokens.size() != 2) {
            out_cmd.type = CommandType::UNKNOWN;
            out_cmd.raw_line = "wrong number of arguments for 'DEL'";
            return true;
        }
        out_cmd.type = CommandType::DEL;
        out_cmd.key = std::move(tokens[1]);
        return true;
    } else if (verb == "PING") {
        out_cmd.type = CommandType::PING;
        if (tokens.size() == 2) {
            out_cmd.key = std::move(tokens[1]);
        } else if (tokens.size() > 2) {
            std::string joined;
            for (size_t k = 1; k < tokens.size(); ++k) {
                if (k > 1) joined += " ";
                joined += tokens[k];
            }
            out_cmd.key = std::move(joined);
        }
        return true;
    } else if (verb == "STATS") {
        out_cmd.type = CommandType::STATS;
        return true;
    } else if (verb == "QUIT") {
        out_cmd.type = CommandType::QUIT;
        return true;
    } else if (verb == "CLEAR") {
        out_cmd.type = CommandType::CLEAR;
        return true;
    } else if (verb == "SYNC") {
        out_cmd.type = CommandType::SYNC;
        return true;
    } else {
        out_cmd.type = CommandType::UNKNOWN;
        out_cmd.raw_line = "unknown command '" + tokens[0] + "'";
        return true;
    }
}

bool Protocol::parse_resp2(std::string_view buffer, Command& out_cmd, size_t& bytes_consumed) {
    bytes_consumed = 0;
    out_cmd.protocol = ProtocolType::RESP2;
    out_cmd.is_resp = true;

    if (buffer.empty()) {
        return false;
    }

    // Standalone Bulk String Command ($<len>\r\n<cmd>\r\n)
    if (buffer[0] == '$') {
        size_t crlf = buffer.find("\r\n");
        if (crlf == std::string_view::npos) return false;

        int64_t bulk_len = 0;
        if (!parse_integer(buffer.substr(1, crlf - 1), bulk_len) || bulk_len < 0) {
            bytes_consumed = crlf + 2;
            out_cmd.type = CommandType::UNKNOWN;
            out_cmd.raw_line = "Protocol error: invalid bulk length";
            return true;
        }

        if (buffer.size() < crlf + 2 + static_cast<size_t>(bulk_len) + 2) {
            return false;
        }

        std::string_view cmd_str = buffer.substr(crlf + 2, static_cast<size_t>(bulk_len));
        bytes_consumed = crlf + 2 + static_cast<size_t>(bulk_len) + 2;

        std::string verb = to_upper(cmd_str);
        if (verb == "PING")  out_cmd.type = CommandType::PING;
        else if (verb == "STATS") out_cmd.type = CommandType::STATS;
        else if (verb == "QUIT")  out_cmd.type = CommandType::QUIT;
        else if (verb == "CLEAR") out_cmd.type = CommandType::CLEAR;
        else if (verb == "SYNC")  out_cmd.type = CommandType::SYNC;
        else {
            out_cmd.type = CommandType::UNKNOWN;
            out_cmd.raw_line = "unknown command '" + verb + "'";
        }
        return true;
    }

    // Array of Bulk Strings (*<count>\r\n...)
    if (buffer[0] != '*') {
        return false;
    }

    size_t line_end = buffer.find("\r\n");
    if (line_end == std::string_view::npos) {
        return false;
    }

    int64_t num_elements = 0;
    if (!parse_integer(buffer.substr(1, line_end - 1), num_elements) || num_elements <= 0) {
        bytes_consumed = line_end + 2;
        out_cmd.type = CommandType::UNKNOWN;
        out_cmd.raw_line = "Protocol error: invalid multibulk length";
        return true;
    }

    if (num_elements > static_cast<int64_t>(MAX_RESP_ARRAY_LEN)) {
        bytes_consumed = line_end + 2;
        out_cmd.type = CommandType::UNKNOWN;
        out_cmd.raw_line = "Protocol error: too many arguments";
        return true;
    }

    size_t offset = line_end + 2;
    std::vector<std::string> elements;
    elements.reserve(static_cast<size_t>(num_elements));

    for (int64_t i = 0; i < num_elements; ++i) {
        if (offset >= buffer.size()) {
            return false;
        }

        if (buffer[offset] != '$') {
            bytes_consumed = offset + 1;
            out_cmd.type = CommandType::UNKNOWN;
            out_cmd.raw_line = "Protocol error: expected '$'";
            return true;
        }

        size_t next_crlf = buffer.find("\r\n", offset);
        if (next_crlf == std::string_view::npos) {
            return false;
        }

        int64_t elem_len = 0;
        if (!parse_integer(buffer.substr(offset + 1, next_crlf - (offset + 1)), elem_len) || elem_len < 0) {
            bytes_consumed = next_crlf + 2;
            out_cmd.type = CommandType::UNKNOWN;
            out_cmd.raw_line = "Protocol error: invalid bulk length";
            return true;
        }

        if (elem_len > static_cast<int64_t>(MAX_RESP_BULK_LEN)) {
            bytes_consumed = next_crlf + 2;
            out_cmd.type = CommandType::UNKNOWN;
            out_cmd.raw_line = "Protocol error: bulk string length exceeds limit";
            return true;
        }

        size_t data_start = next_crlf + 2;
        size_t data_end = data_start + static_cast<size_t>(elem_len);
        if (buffer.size() < data_end + 2) {
            return false;
        }

        if (buffer[data_end] != '\r' || buffer[data_end + 1] != '\n') {
            bytes_consumed = data_end + 2;
            out_cmd.type = CommandType::UNKNOWN;
            out_cmd.raw_line = "Protocol error: missing bulk CRLF terminator";
            return true;
        }

        elements.emplace_back(buffer.substr(data_start, static_cast<size_t>(elem_len)));
        offset = data_end + 2;
    }

    bytes_consumed = offset;

    if (elements.empty()) {
        out_cmd.type = CommandType::UNKNOWN;
        return true;
    }

    std::string verb = to_upper(elements[0]);

    if (verb == "SET") {
        if (elements.size() != 3) {
            out_cmd.type = CommandType::UNKNOWN;
            out_cmd.raw_line = "wrong number of arguments for 'SET'";
            return true;
        }
        out_cmd.type = CommandType::SET;
        out_cmd.key = std::move(elements[1]);
        out_cmd.value = std::move(elements[2]);
        return true;
    } else if (verb == "GET") {
        if (elements.size() != 2) {
            out_cmd.type = CommandType::UNKNOWN;
            out_cmd.raw_line = "wrong number of arguments for 'GET'";
            return true;
        }
        out_cmd.type = CommandType::GET;
        out_cmd.key = std::move(elements[1]);
        return true;
    } else if (verb == "DEL") {
        if (elements.size() != 2) {
            out_cmd.type = CommandType::UNKNOWN;
            out_cmd.raw_line = "wrong number of arguments for 'DEL'";
            return true;
        }
        out_cmd.type = CommandType::DEL;
        out_cmd.key = std::move(elements[1]);
        return true;
    } else if (verb == "PING") {
        out_cmd.type = CommandType::PING;
        if (elements.size() >= 2) {
            out_cmd.key = std::move(elements[1]);
        }
        return true;
    } else if (verb == "STATS") {
        out_cmd.type = CommandType::STATS;
        return true;
    } else if (verb == "QUIT") {
        out_cmd.type = CommandType::QUIT;
        return true;
    } else if (verb == "CLEAR") {
        out_cmd.type = CommandType::CLEAR;
        return true;
    } else if (verb == "SYNC") {
        out_cmd.type = CommandType::SYNC;
        return true;
    } else {
        out_cmd.type = CommandType::UNKNOWN;
        out_cmd.raw_line = "unknown command '" + elements[0] + "'";
        return true;
    }
}

} // namespace kvstore
