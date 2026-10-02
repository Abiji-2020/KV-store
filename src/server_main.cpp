#include "kvstore/kvstore.hpp"
#include "kvstore/server.hpp"
#include "kvstore/wal.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

static std::atomic<bool> g_stop_requested{false};

static void sig_handler(int sig) {
    (void)sig;
    g_stop_requested.store(true, std::memory_order_release);
}

static void print_usage(const char* prog) {
    std::cout << "Usage: " << prog << " [OPTIONS]\n\n"
              << "High-Performance In-Memory Key-Value Server (cpp_kv_store)\n\n"
              << "Options:\n"
              << "  -h, --host <ip>        Bind IP address (default: 127.0.0.1)\n"
              << "  -p, --port <port>      TCP port to bind (default: 7379, 0 for dynamic)\n"
              << "  -c, --capacity <n>     LRU item capacity (default: 0 for unlimited)\n"
              << "  -s, --sync <mode>      WAL sync policy: always|batch|buffered (default: always)\n"
              << "  -w, --wal <path>       WAL log file path (optional)\n"
              << "  -t, --threads <n>      Worker threads pool size (default: 4)\n"
              << "      --port-file <path> Write assigned bound port to file\n"
              << "      --help             Display this help message and exit\n";
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    int port = 7379;
    size_t capacity = 0;
    std::string sync_str = "always";
    std::string wal_path;
    size_t threads = 4;
    std::string port_file;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else if ((arg == "-h" || arg == "--host") && i + 1 < argc) {
            host = argv[++i];
        } else if ((arg == "-p" || arg == "--port") && i + 1 < argc) {
            try {
                port = std::stoi(argv[++i]);
            } catch (...) {
                std::cerr << "Error: Invalid port number: " << argv[i] << std::endl;
                return 1;
            }
        } else if ((arg == "-c" || arg == "--capacity") && i + 1 < argc) {
            try {
                capacity = std::stoull(argv[++i]);
            } catch (...) {
                std::cerr << "Error: Invalid capacity: " << argv[i] << std::endl;
                return 1;
            }
        } else if ((arg == "-s" || arg == "--sync") && i + 1 < argc) {
            sync_str = argv[++i];
        } else if ((arg == "-w" || arg == "--wal") && i + 1 < argc) {
            wal_path = argv[++i];
        } else if ((arg == "-t" || arg == "--threads") && i + 1 < argc) {
            try {
                threads = std::stoull(argv[++i]);
            } catch (...) {
                std::cerr << "Error: Invalid thread count: " << argv[i] << std::endl;
                return 1;
            }
        } else if (arg == "--port-file" && i + 1 < argc) {
            port_file = argv[++i];
        } else {
            std::cerr << "Error: Unknown or invalid argument '" << arg << "'" << std::endl;
            print_usage(argv[0]);
            return 1;
        }
    }

    kvstore::SyncMode sync_mode = kvstore::SyncMode::SYNC_ALWAYS;
    if (sync_str == "batch") {
        sync_mode = kvstore::SyncMode::SYNC_BATCH;
    } else if (sync_str == "buffered") {
        sync_mode = kvstore::SyncMode::SYNC_BUFFERED;
    } else if (sync_str != "always") {
        std::cerr << "Error: Invalid sync mode '" << sync_str << "' (must be 'always', 'batch', or 'buffered')" << std::endl;
        return 1;
    }

    // Install POSIX signal handlers for graceful shutdown
    struct sigaction sa{};
    sa.sa_handler = sig_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    try {
        kvstore::KVStore store(capacity);

        std::unique_ptr<kvstore::WalManager> wal;
        if (!wal_path.empty()) {
            wal = std::make_unique<kvstore::WalManager>(wal_path, sync_mode);
            size_t restored = wal->recover(store);
            std::cout << "[kv_server] WAL recovery: restored " << restored << " records from " << wal_path << std::endl;
        }

        kvstore::Server server(store, wal.get(), host, port, threads);
        server.start();

        int bound_port = server.port();
        std::cout << "[kv_server] Listening on " << host << ":" << bound_port
                  << " (capacity=" << capacity << ", sync=" << sync_str << ")" << std::endl;
        std::cout << "PORT: " << bound_port << std::endl;
        std::cout.flush();

        if (!port_file.empty()) {
            std::ofstream pf(port_file);
            if (pf.is_open()) {
                pf << bound_port << "\n";
                pf.close();
            }
        }

        while (!g_stop_requested.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        std::cout << "[kv_server] Stopping server..." << std::endl;
        server.stop();
        std::cout << "[kv_server] Server stopped cleanly." << std::endl;

    } catch (const std::exception& ex) {
        std::cerr << "Server fatal error: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
