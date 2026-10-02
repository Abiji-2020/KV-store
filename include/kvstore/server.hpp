#pragma once

#include "kvstore/kvstore.hpp"
#include "kvstore/wal.hpp"
#include "kvstore/protocol.hpp"

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace kvstore {

/// Standalone multi-threaded TCP network server.
/// Features POSIX socket listener, non-blocking / concurrent client handling,
/// dynamic port assignment via getsockname(), graceful shutdown with WAL flush,
/// and deadlock-free integration with KVStore and WalManager.
class Server {
public:
    static constexpr size_t DEFAULT_WORKER_THREADS = 4;
    static constexpr size_t READ_BUFFER_SIZE = 8192;

    /// Constructs the server with reference to storage engine and optional WAL manager.
    /// @param store Reference to in-memory KVStore.
    /// @param wal Pointer to WalManager (nullptr if persistence disabled).
    /// @param host Hostname or IPv4 address to bind (e.g. "127.0.0.1").
    /// @param port TCP port to bind (0 for OS dynamic ephemeral allocation).
    /// @param worker_threads Worker threads configuration parameter.
    Server(KVStore& store, WalManager* wal, const std::string& host, int port,
           size_t worker_threads = DEFAULT_WORKER_THREADS);

    /// Destructor stops the server and releases all socket and thread resources.
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;
    Server(Server&&) = delete;
    Server& operator=(Server&&) = delete;

    /// Binds listener socket, resolves port, and launches accept loop.
    /// Returns once the listening socket is bound and ready to accept connections.
    /// @throws std::runtime_error on socket/bind/listen failure.
    void start();

    /// Gracefully stops the server, disconnects clients, terminates threads, and syncs WAL.
    void stop();

    /// Returns the active bound TCP port (useful when port 0 was passed).
    int port() const noexcept;

    /// Returns true if server is currently running and accepting connections.
    bool is_running() const noexcept;

private:
    struct ClientSession {
        std::atomic<int> fd{-1};
        std::thread th;
        std::atomic<bool> done{false};
    };

    void accept_loop();
    void handle_client_connection(ClientSession* session);
    Response execute_command(const Command& cmd);
    void reap_sessions_unlocked();
    static bool send_all(int fd, const std::string& data);

    KVStore& store_;
    WalManager* wal_{nullptr};
    std::string host_;
    int configured_port_{0};
    int bound_port_{0};
    size_t worker_threads_count_{DEFAULT_WORKER_THREADS};

    std::atomic<bool> running_{false};
    int listen_fd_{-1};
    int stop_pipe_[2]{-1, -1};

    std::thread listener_thread_;
    std::mutex sessions_mtx_;
    std::vector<std::unique_ptr<ClientSession>> sessions_;
};

} // namespace kvstore
