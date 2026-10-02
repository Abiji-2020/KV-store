#pragma once

#include <chrono>
#include <string>

namespace kvstore {

/// Companion TCP client library for communicating with kv_server.
/// Provides connection management, low-latency command dispatch,
/// and helper methods for standard KV operations.
class Client {
public:
    explicit Client(const std::string& host = "127.0.0.1", int port = 7379,
                    double timeout_sec = 3.0);
    ~Client();

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;
    Client(Client&& other) noexcept;
    Client& operator=(Client&& other) noexcept;

    /// Connects to the server with retries. Returns true on success.
    bool connect(int retries = 5, int retry_delay_ms = 50);

    /// Closes the connection. Safe to call multiple times.
    void disconnect();

    /// Returns true if currently connected.
    bool is_connected() const noexcept;

    /// Sends raw text or serialized command line and returns server's response.
    std::string send_command(const std::string& command_line);

    // High-level operational wrappers
    std::string set(const std::string& key, const std::string& value);
    std::string get(const std::string& key);
    std::string del(const std::string& key);
    std::string ping(const std::string& message = "");
    std::string stats();
    std::string quit();

    const std::string& last_error() const noexcept;

private:
    bool send_all(const std::string& data);
    std::string read_line();
    std::string read_exact(size_t num_bytes);

    std::string host_;
    int port_;
    double timeout_sec_;
    int sock_fd_{-1};
    std::string last_error_;
    std::string stream_buffer_;
};

} // namespace kvstore
