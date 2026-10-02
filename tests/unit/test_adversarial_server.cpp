#include "kvstore/client.hpp"
#include "kvstore/kvstore.hpp"
#include "kvstore/protocol.hpp"
#include "kvstore/server.hpp"
#include "kvstore/wal.hpp"

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

// ============================================================================
// Lightweight Zero-Dependency Test Framework
// ============================================================================

struct TestCase {
    std::string name;
    std::function<void()> func;
};

inline std::vector<TestCase>& get_adversarial_server_registry() {
    static std::vector<TestCase> registry;
    return registry;
}

struct AdversarialServerTestRegistrar {
    AdversarialServerTestRegistrar(const std::string& name, std::function<void()> func) {
        get_adversarial_server_registry().push_back({name, std::move(func)});
    }
};

#include <type_traits>
#include <utility>

class TestFailureException : public std::runtime_error {
public:
    explicit TestFailureException(const std::string& msg) : std::runtime_error(msg) {}
};

template <typename T, typename U>
inline bool test_equal(const T& a, const U& b) {
    if constexpr (std::is_integral_v<T> && std::is_integral_v<U> &&
                  !std::is_same_v<T, char> && !std::is_same_v<U, char> &&
                  !std::is_same_v<T, bool> && !std::is_same_v<U, bool>) {
        return std::cmp_equal(a, b);
    } else {
        return a == b;
    }
}

#define ADV_SERVER_TEST(name) \
    void test_##name(); \
    static AdversarialServerTestRegistrar registrar_adv_##name(#name, test_##name); \
    void test_##name()

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        std::ostringstream oss; \
        oss << "Assertion failed: (" #cond ") at " << __FILE__ << ":" << __LINE__; \
        throw TestFailureException(oss.str()); \
    } \
} while(0)

#define ASSERT_FALSE(cond) do { \
    if (cond) { \
        std::ostringstream oss; \
        oss << "Assertion failed (expected false): (" #cond ") at " << __FILE__ << ":" << __LINE__; \
        throw TestFailureException(oss.str()); \
    } \
} while(0)

#define ASSERT_EQ(a, b) do { \
    auto _a = (a); \
    auto _b = (b); \
    if (!test_equal(_a, _b)) { \
        std::ostringstream oss; \
        oss << "Assertion failed: (" #a " == " #b ") [" << _a << " != " << _b << "] at " << __FILE__ << ":" << __LINE__; \
        throw TestFailureException(oss.str()); \
    } \
} while(0)

#define ASSERT_NE(a, b) do { \
    auto _a = (a); \
    auto _b = (b); \
    if (test_equal(_a, _b)) { \
        std::ostringstream oss; \
        oss << "Assertion failed: (" #a " != " #b ") [" << _a << " == " << _b << "] at " << __FILE__ << ":" << __LINE__; \
        throw TestFailureException(oss.str()); \
    } \
} while(0)

// Helper: count open file descriptors in /proc/self/fd
static size_t count_open_fds() {
    size_t count = 0;
    DIR* dir = ::opendir("/proc/self/fd");
    if (!dir) return 0;
    struct dirent* entry = nullptr;
    while ((entry = ::readdir(dir)) != nullptr) {
        if (std::strcmp(entry->d_name, ".") != 0 && std::strcmp(entry->d_name, "..") != 0) {
            count++;
        }
    }
    ::closedir(dir);
    return count;
}

// Helper: establish raw socket connection to host:port
static int raw_connect(const std::string& host, int port) {
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        ::close(sock);
        return -1;
    }

    if (::connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(sock);
        return -1;
    }
    return sock;
}

// ============================================================================
// Adversarial Stress Tests
// ============================================================================

// ----------------------------------------------------------------------------
// Scenario 1: High Connection Concurrency (36 simultaneous clients)
// Connects 36 simultaneous clients, holds all sockets open before sending data,
// synchronizes burst requests via atomic latch, and asserts zero starvation.
// ----------------------------------------------------------------------------
ADV_SERVER_TEST(high_connection_concurrency_32_plus) {
    kvstore::KVStore store;
    kvstore::Server server(store, nullptr, "127.0.0.1", 0, 4);
    server.start();
    int port = server.port();
    ASSERT_TRUE(port > 0);

    constexpr size_t CLIENT_COUNT = 36;
    std::vector<std::unique_ptr<kvstore::Client>> clients;
    clients.reserve(CLIENT_COUNT);

    // 1. Connect all 36 clients simultaneously and hold open without sending
    for (size_t i = 0; i < CLIENT_COUNT; ++i) {
        auto c = std::make_unique<kvstore::Client>("127.0.0.1", port);
        ASSERT_TRUE(c->connect());
        ASSERT_TRUE(c->is_connected());
        clients.push_back(std::move(c));
    }

    // 2. Synchronized simultaneous ping burst
    std::atomic<bool> start_gate{false};
    std::atomic<size_t> success_count{0};
    std::vector<std::thread> workers;
    workers.reserve(CLIENT_COUNT);

    for (size_t i = 0; i < CLIENT_COUNT; ++i) {
        workers.emplace_back([i, &clients, &start_gate, &success_count]() {
            while (!start_gate.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            // Standard PING
            std::string res = clients[i]->ping();
            if (res == "+PONG\r\n") {
                // Custom PING
                std::string custom_msg = "adv_client_" + std::to_string(i);
                std::string custom_res = clients[i]->ping(custom_msg);
                if (custom_res == "+" + custom_msg + "\r\n") {
                    success_count.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    // Trigger all threads simultaneously
    start_gate.store(true, std::memory_order_release);

    for (auto& th : workers) {
        th.join();
    }

    ASSERT_EQ(success_count.load(), CLIENT_COUNT);

    // 3. Disconnect all clients cleanly
    for (auto& c : clients) {
        c->disconnect();
        ASSERT_FALSE(c->is_connected());
    }

    server.stop();
    ASSERT_FALSE(server.is_running());
}

// ----------------------------------------------------------------------------
// Scenario 2: Abrupt Disconnect Storm (50 clients)
// 50 clients connect, send truncated/half-written command chunks, and abruptly
// close sockets (some with SO_LINGER RST). Verifies server neither crashes, hangs,
// nor leaks file descriptors.
// ----------------------------------------------------------------------------
ADV_SERVER_TEST(abrupt_disconnect_storm_50_clients) {
    kvstore::KVStore store;
    kvstore::Server server(store, nullptr, "127.0.0.1", 0, 4);
    server.start();
    int port = server.port();

    size_t fds_before = count_open_fds();

    constexpr size_t STORM_COUNT = 50;
    std::vector<std::thread> storm_threads;
    storm_threads.reserve(STORM_COUNT);

    for (size_t i = 0; i < STORM_COUNT; ++i) {
        storm_threads.emplace_back([port, i]() {
            int sock = raw_connect("127.0.0.1", port);
            if (sock < 0) return;

            // Varied partial payload attacks
            if (i % 4 == 0) {
                // Partial inline command without newline
                const char* partial = "SET partial_key partial_";
                ::send(sock, partial, std::strlen(partial), MSG_NOSIGNAL);
            } else if (i % 4 == 1) {
                // Truncated RESP2 multibulk array header
                const char* partial = "*3\r\n$3\r\nSET\r\n$4\r\nkey_";
                ::send(sock, partial, std::strlen(partial), MSG_NOSIGNAL);
            } else if (i % 4 == 2) {
                // Bare prefix byte
                const char* partial = "*";
                ::send(sock, partial, 1, MSG_NOSIGNAL);
            } else {
                // Send nothing at all, immediate disconnect
            }

            if (i % 2 == 0) {
                // Force TCP RST via SO_LINGER (timeout = 0)
                struct linger sl;
                sl.l_onoff = 1;
                sl.l_linger = 0;
                ::setsockopt(sock, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));
            }

            // Abrupt immediate close
            ::close(sock);
        });
    }

    for (auto& th : storm_threads) {
        th.join();
    }

    // Give server worker threads a brief moment to process the EOF / RST
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify server remains fully functional after the storm
    kvstore::Client verifier("127.0.0.1", port);
    ASSERT_TRUE(verifier.connect());
    ASSERT_EQ(verifier.ping(), "+PONG\r\n");
    ASSERT_EQ(verifier.set("storm_survivor", "alive"), "+OK\r\n");
    ASSERT_EQ(verifier.get("storm_survivor"), "+VALUE alive\r\n");
    verifier.disconnect();

    server.stop();

    size_t fds_after = count_open_fds();
    // After server stop, open FDs should be restored (no leaked client or listen sockets)
    // Allow up to small directory handle differences
    if (fds_before > 0 && fds_after > 0) {
        ASSERT_TRUE(fds_after <= fds_before + 3);
    }
}

// ----------------------------------------------------------------------------
// Scenario 3: High Contention Multi-Threaded Mutations (16 threads, same keys)
// 16 concurrent client threads perform SET, GET, DEL operations targeting the
// exact same key under heavy contention with WAL logging enabled.
// ----------------------------------------------------------------------------
ADV_SERVER_TEST(high_contention_mutations_16_threads_same_keys) {
    const std::string wal_path = "test_adv_contention.wal";
    std::remove(wal_path.c_str());

    {
        kvstore::KVStore store;
        auto wal = std::make_unique<kvstore::WalManager>(wal_path, kvstore::SyncMode::SYNC_BATCH);
        kvstore::Server server(store, wal.get(), "127.0.0.1", 0, 4);
        server.start();
        int port = server.port();

        constexpr size_t THREADS = 16;
        constexpr size_t OPS_PER_THREAD = 40;

        std::atomic<bool> start_gate{false};
        std::atomic<size_t> completed_threads{0};
        std::atomic<size_t> successful_ops{0};
        std::vector<std::thread> workers;
        workers.reserve(THREADS);

        for (size_t t = 0; t < THREADS; ++t) {
            workers.emplace_back([t, port, &start_gate, &completed_threads, &successful_ops]() {
                kvstore::Client client("127.0.0.1", port);
                if (!client.connect()) return;

                while (!start_gate.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }

                for (size_t op = 0; op < OPS_PER_THREAD; ++op) {
                    std::string key = "hot_contested_key";
                    std::string val = "val_t" + std::to_string(t) + "_op" + std::to_string(op);

                    // 1. SET
                    std::string set_res = client.set(key, val);
                    if (set_res == "+OK\r\n") {
                        successful_ops.fetch_add(1, std::memory_order_relaxed);
                    }

                    // 2. GET
                    std::string get_res = client.get(key);
                    if (get_res.rfind("+VALUE", 0) == 0 || get_res == "-ERR NOT_FOUND\r\n") {
                        successful_ops.fetch_add(1, std::memory_order_relaxed);
                    }

                    // 3. DEL (every 4th op)
                    if (op % 4 == 0) {
                        std::string del_res = client.del(key);
                        if (del_res == "+OK DELETED\r\n" || del_res == "-ERR NOT_FOUND\r\n") {
                            successful_ops.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                }

                client.disconnect();
                completed_threads.fetch_add(1, std::memory_order_release);
            });
        }

        start_gate.store(true, std::memory_order_release);

        for (auto& th : workers) {
            th.join();
        }

        ASSERT_EQ(completed_threads.load(), THREADS);
        ASSERT_TRUE(successful_ops.load() > 0);

        // Verify server stats command works under load
        kvstore::Client stats_client("127.0.0.1", port);
        ASSERT_TRUE(stats_client.connect());
        std::string stats_res = stats_client.stats();
        ASSERT_TRUE(stats_res.rfind("+STATS", 0) == 0);
        stats_client.disconnect();

        server.stop();
    }

    std::remove(wal_path.c_str());
}

// ----------------------------------------------------------------------------
// Scenario 4: Server Stop During Active Client Communication
// Bombards server with 16 continuous streaming client threads, calls server.stop()
// abruptly during full throughput, and verifies clean teardown without hung threads.
// ----------------------------------------------------------------------------
ADV_SERVER_TEST(server_stop_during_active_traffic) {
    kvstore::KVStore store;
    kvstore::Server server(store, nullptr, "127.0.0.1", 0, 4);
    server.start();
    int port = server.port();

    constexpr size_t CLIENT_COUNT = 16;
    std::atomic<bool> stop_flag{false};
    std::atomic<size_t> active_workers{0};
    std::atomic<size_t> total_queries{0};
    std::vector<std::thread> workers;
    workers.reserve(CLIENT_COUNT);

    for (size_t i = 0; i < CLIENT_COUNT; ++i) {
        workers.emplace_back([i, port, &stop_flag, &active_workers, &total_queries]() {
            kvstore::Client client("127.0.0.1", port);
            if (!client.connect()) return;

            active_workers.fetch_add(1, std::memory_order_relaxed);

            while (!stop_flag.load(std::memory_order_acquire)) {
                std::string k = "stream_k_" + std::to_string(i);
                std::string v = "stream_v_" + std::to_string(i);
                std::string set_res = client.set(k, v);
                if (set_res != "+OK\r\n") break;

                std::string get_res = client.get(k);
                if (get_res.empty() || get_res.rfind("-ERR connection", 0) == 0) break;

                total_queries.fetch_add(2, std::memory_order_relaxed);
            }

            client.disconnect();
            active_workers.fetch_sub(1, std::memory_order_relaxed);
        });
    }

    // Wait until all workers are actively bombarding the server
    while (active_workers.load() < CLIENT_COUNT) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    // Let high-speed traffic flow for 50 milliseconds
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Abruptly call stop() from main thread while clients are actively sending
    auto stop_start = std::chrono::steady_clock::now();
    server.stop();
    auto stop_end = std::chrono::steady_clock::now();

    auto stop_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(stop_end - stop_start).count();

    // Signal client threads to exit if they haven't already noticed disconnection
    stop_flag.store(true, std::memory_order_release);

    for (auto& th : workers) {
        th.join();
    }

    // Assert server stopped quickly (< 2000 ms) without hanging
    ASSERT_TRUE(stop_duration_ms < 2000);
    ASSERT_FALSE(server.is_running());
    ASSERT_TRUE(total_queries.load() > 0);

    // Verify immediate restart on dynamic port succeeds cleanly
    kvstore::KVStore restart_store;
    kvstore::Server restart_server(restart_store, nullptr, "127.0.0.1", 0);
    restart_server.start();
    ASSERT_TRUE(restart_server.is_running());
    ASSERT_TRUE(restart_server.port() > 0);
    restart_server.stop();
}

// ----------------------------------------------------------------------------
// Scenario 5: Fuzzing & Malformed Payload Resilience
// Injects adversarial corrupt frames, oversized lines, and invalid protocol tokens.
// Verifies server returns error responses or cleanly drops client without crashing.
// ----------------------------------------------------------------------------
ADV_SERVER_TEST(fuzzing_and_malformed_payload_resilience) {
    kvstore::KVStore store;
    kvstore::Server server(store, nullptr, "127.0.0.1", 0);
    server.start();
    int port = server.port();

    std::vector<std::string> adversarial_payloads = {
        // Massive multibulk count
        "*999999999\r\n",
        // Negative bulk length
        "*1\r\n$-10\r\n",
        // Invalid character in array count
        "*NaN\r\n",
        // Truncated payload missing CRLF
        "*1\r\n$4\r\ntest",
        // Unknown command verb
        "BOGUS_VERB_XYZ123 param1 param2\r\n",
        // SET with missing key and value
        "SET\r\n",
        // GET with missing key
        "GET\r\n",
        // DEL with missing key
        "DEL\r\n",
        // Excessive empty CRLF lines flood
        "\r\n\r\n\r\n\r\n\r\n",
        // Binary garbage bytes
        std::string("\x00\xFF\xDE\xAD\xBE\xEF\x01\x02\r\n", 10)
    };

    for (const auto& payload : adversarial_payloads) {
        int sock = raw_connect("127.0.0.1", port);
        if (sock >= 0) {
            struct timeval tv{0, 20000}; // 20ms timeout
            ::setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            ::send(sock, payload.data(), payload.size(), MSG_NOSIGNAL);

            // Read response if available
            char resp_buf[512];
            (void)::recv(sock, resp_buf, sizeof(resp_buf), 0);
            ::close(sock);
        }
    }

    // Verify server process survives all fuzzing injections undamaged
    kvstore::Client verifier("127.0.0.1", port);
    ASSERT_TRUE(verifier.connect());
    ASSERT_EQ(verifier.ping(), "+PONG\r\n");
    ASSERT_EQ(verifier.set("fuzz_check", "valid_data"), "+OK\r\n");
    ASSERT_EQ(verifier.get("fuzz_check"), "+VALUE valid_data\r\n");
    verifier.disconnect();

    server.stop();
}

// ----------------------------------------------------------------------------
// Scenario 6: Concurrent Command Pipelining Burst
// Multiple clients send bursts of 25 pipelined commands in single TCP payloads.
// Verifies response order preservation and pipelining stream demarcation.
// ----------------------------------------------------------------------------
ADV_SERVER_TEST(concurrent_command_pipelining_burst) {
    kvstore::KVStore store;
    kvstore::Server server(store, nullptr, "127.0.0.1", 0);
    server.start();
    int port = server.port();

    constexpr size_t CLIENTS = 8;
    constexpr size_t PIPELINED_OPS = 25;

    std::vector<std::thread> workers;
    workers.reserve(CLIENTS);
    std::atomic<size_t> passed_clients{0};

    for (size_t c = 0; c < CLIENTS; ++c) {
        workers.emplace_back([c, port, &passed_clients]() {
            int sock = raw_connect("127.0.0.1", port);
            if (sock < 0) return;

            // Construct single batch payload with 25 pipelined commands
            std::string batch_payload;
            for (size_t i = 0; i < PIPELINED_OPS; ++i) {
                batch_payload += "PING pipe_" + std::to_string(c) + "_" + std::to_string(i) + "\r\n";
            }

            // Send entire batch in a single system call
            ssize_t sent = ::send(sock, batch_payload.data(), batch_payload.size(), MSG_NOSIGNAL);
            if (sent != static_cast<ssize_t>(batch_payload.size())) {
                ::close(sock);
                return;
            }

            // Read all 25 responses
            std::string recv_buf;
            char chunk[2048];
            size_t responses_received = 0;

            while (responses_received < PIPELINED_OPS) {
                ssize_t n = ::recv(sock, chunk, sizeof(chunk), 0);
                if (n <= 0) break;
                recv_buf.append(chunk, static_cast<size_t>(n));

                while (true) {
                    size_t crlf = recv_buf.find("\r\n");
                    if (crlf == std::string::npos) break;
                    std::string line = recv_buf.substr(0, crlf);
                    recv_buf.erase(0, crlf + 2);

                    std::string expected = "+pipe_" + std::to_string(c) + "_" + std::to_string(responses_received);
                    if (line == expected) {
                        responses_received++;
                    }
                }
            }

            ::close(sock);
            if (responses_received == PIPELINED_OPS) {
                passed_clients.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& th : workers) {
        th.join();
    }

    ASSERT_EQ(passed_clients.load(), CLIENTS);

    server.stop();
}

// ----------------------------------------------------------------------------
// Scenario 7: Rapid Lifecycle Start/Stop Churn
// Rapidly starts and stops the server 10 times consecutively, checking for
// thread leaks, socket leaks, or port binding failure.
// ----------------------------------------------------------------------------
ADV_SERVER_TEST(rapid_server_lifecycle_churn) {
    constexpr size_t ITERATIONS = 10;

    for (size_t i = 0; i < ITERATIONS; ++i) {
        kvstore::KVStore store;
        kvstore::Server srv(store, nullptr, "127.0.0.1", 0);
        srv.start();
        int port = srv.port();
        ASSERT_TRUE(port > 0);
        ASSERT_TRUE(srv.is_running());

        kvstore::Client client("127.0.0.1", port);
        ASSERT_TRUE(client.connect());
        ASSERT_EQ(client.ping(), "+PONG\r\n");
        client.disconnect();

        srv.stop();
        ASSERT_FALSE(srv.is_running());
    }
}

// ============================================================================
// Test Suite Runner
// ============================================================================

int main() {
    std::cout << "=======================================================\n";
    std::cout << "RUNNING MILESTONE 3 ADVERSARIAL SERVER STRESS SUITE\n";
    std::cout << "=======================================================\n\n";

    auto& registry = get_adversarial_server_registry();
    size_t passed = 0;
    size_t failed = 0;
    auto t_start = std::chrono::steady_clock::now();

    for (const auto& test : registry) {
        std::cout << "[ RUN      ] " << test.name << std::endl;
        auto t0 = std::chrono::steady_clock::now();
        try {
            test.func();
            auto t1 = std::chrono::steady_clock::now();
            auto us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
            std::cout << "[  PASSED  ] " << test.name << " (" << us / 1000.0 << " ms)" << std::endl;
            passed++;
        } catch (const TestFailureException& e) {
            std::cout << "[  FAILED  ] " << test.name << std::endl;
            std::cout << "             " << e.what() << std::endl;
            failed++;
        } catch (const std::exception& e) {
            std::cout << "[  FAILED  ] " << test.name << " (unexpected std::exception: " << e.what() << ")" << std::endl;
            failed++;
        } catch (...) {
            std::cout << "[  FAILED  ] " << test.name << " (unknown exception caught)" << std::endl;
            failed++;
        }
    }

    auto t_end = std::chrono::steady_clock::now();
    auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_start).count();

    std::cout << "\n-------------------------------------------------------\n";
    std::cout << "Total Adversarial Server Tests : " << (passed + failed) << "\n";
    std::cout << "Passed                         : " << passed << "\n";
    std::cout << "Failed                         : " << failed << "\n";
    std::cout << "Elapsed Time                   : " << total_ms << " ms\n";
    std::cout << "-------------------------------------------------------\n";

    if (failed == 0) {
        std::cout << ">>> ALL ADVERSARIAL SERVER CONCURRENCY TESTS PASSED SUCCESSFULLY! <<<\n\n";
        return 0;
    } else {
        std::cout << ">>> ADVERSARIAL SERVER STRESS DETECTED FAILURES! <<<\n\n";
        return 1;
    }
}
