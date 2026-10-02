#include "kvstore/client.hpp"
#include "kvstore/kvstore.hpp"
#include "kvstore/server.hpp"
#include "kvstore/wal.hpp"

#include <arpa/inet.h>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
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

void test_server_dynamic_port() {
    kvstore::KVStore store;
    kvstore::Server server(store, nullptr, "127.0.0.1", 0);
    server.start();

    int port = server.port();
    ASSERT_TRUE(port > 0);
    ASSERT_TRUE(server.is_running());

    server.stop();
    ASSERT_TRUE(!server.is_running());

    std::cout << "[PASS] test_server_dynamic_port" << std::endl;
}

void test_server_reuseaddr_immediate_restart() {
    kvstore::KVStore store1;
    kvstore::Server srv1(store1, nullptr, "127.0.0.1", 0);
    srv1.start();
    int assigned_port = srv1.port();
    srv1.stop();

    // Immediately restart on the exact same port
    kvstore::KVStore store2;
    kvstore::Server srv2(store2, nullptr, "127.0.0.1", assigned_port);
    srv2.start();
    ASSERT_EQ(srv2.port(), assigned_port);
    srv2.stop();

    std::cout << "[PASS] test_server_reuseaddr_immediate_restart" << std::endl;
}

void test_server_ping_pong() {
    kvstore::KVStore store;
    kvstore::Server server(store, nullptr, "127.0.0.1", 0);
    server.start();

    kvstore::Client client("127.0.0.1", server.port());
    ASSERT_TRUE(client.connect());

    std::string pong = client.ping();
    ASSERT_EQ(pong, "+PONG\r\n");

    std::string custom_pong = client.ping("hello_test");
    ASSERT_EQ(custom_pong, "+hello_test\r\n");

    client.disconnect();
    server.stop();

    std::cout << "[PASS] test_server_ping_pong" << std::endl;
}

void test_server_mutations() {
    kvstore::KVStore store;
    kvstore::Server server(store, nullptr, "127.0.0.1", 0);
    server.start();

    kvstore::Client client("127.0.0.1", server.port());
    ASSERT_TRUE(client.connect());

    // 1. SET
    std::string set_res = client.set("k1", "v1");
    ASSERT_EQ(set_res, "+OK\r\n");

    // 2. GET
    std::string get_res = client.get("k1");
    ASSERT_EQ(get_res, "+VALUE v1\r\n");

    // 3. STATS
    std::string stats_res = client.stats();
    ASSERT_TRUE(stats_res.find("keys=1") != std::string::npos);
    ASSERT_TRUE(stats_res.find("hits=1") != std::string::npos);

    // 4. DEL
    std::string del_res = client.del("k1");
    ASSERT_EQ(del_res, "+OK DELETED\r\n");

    // 5. GET missing key
    std::string miss_res = client.get("k1");
    ASSERT_EQ(miss_res, "-ERR NOT_FOUND\r\n");

    // 6. DEL missing key
    std::string del_miss_res = client.del("k1");
    ASSERT_EQ(del_miss_res, "-ERR NOT_FOUND\r\n");

    client.disconnect();
    server.stop();

    std::cout << "[PASS] test_server_mutations" << std::endl;
}

void test_server_concurrent_16_clients() {
    kvstore::KVStore store;
    kvstore::Server server(store, nullptr, "127.0.0.1", 0, 4);
    server.start();

    // Connect 16 clients simultaneously before issuing any request
    std::vector<std::unique_ptr<kvstore::Client>> clients;
    for (int i = 0; i < 16; ++i) {
        auto c = std::make_unique<kvstore::Client>("127.0.0.1", server.port());
        ASSERT_TRUE(c->connect());
        clients.push_back(std::move(c));
    }

    // Now ping each client
    for (size_t i = 0; i < clients.size(); ++i) {
        std::string res = clients[i]->ping();
        ASSERT_EQ(res, "+PONG\r\n");
    }

    // Concurrent read/writes across 16 clients
    std::vector<std::thread> workers;
    for (int i = 0; i < 16; ++i) {
        workers.emplace_back([&clients, i]() {
            for (int k = 0; k < 20; ++k) {
                std::string key = "key_" + std::to_string(i) + "_" + std::to_string(k);
                std::string val = "val_" + std::to_string(i) + "_" + std::to_string(k);
                clients[i]->set(key, val);
                std::string got = clients[i]->get(key);
                ASSERT_EQ(got, "+VALUE " + val + "\r\n");
            }
        });
    }

    for (auto& th : workers) {
        th.join();
    }

    for (auto& c : clients) {
        c->disconnect();
    }

    server.stop();
    std::cout << "[PASS] test_server_concurrent_16_clients" << std::endl;
}

void test_server_abrupt_disconnect_isolation() {
    kvstore::KVStore store;
    kvstore::Server server(store, nullptr, "127.0.0.1", 0);
    server.start();

    // Open client 1 and client 2
    int sock1 = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(server.port()));
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    ASSERT_TRUE(::connect(sock1, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0);

    kvstore::Client client2("127.0.0.1", server.port());
    ASSERT_TRUE(client2.connect());

    // Abruptly close client 1
    ::close(sock1);

    // Client 2 should continue operating without disruption
    std::string res = client2.ping();
    ASSERT_EQ(res, "+PONG\r\n");

    client2.disconnect();
    server.stop();

    std::cout << "[PASS] test_server_abrupt_disconnect_isolation" << std::endl;
}

void test_server_broken_pipe_immunity() {
    kvstore::KVStore store;
    kvstore::Server server(store, nullptr, "127.0.0.1", 0);
    server.start();

    // Client connects, sends request, and immediately closes before reading response
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(server.port()));
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    ASSERT_TRUE(::connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0);

    const char* cmd = "SET broken_pipe_k broken_pipe_v\r\n";
    ::send(sock, cmd, std::strlen(cmd), 0);
    ::close(sock);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Server process must stay alive and service subsequent requests
    kvstore::Client client("127.0.0.1", server.port());
    ASSERT_TRUE(client.connect());
    ASSERT_EQ(client.ping(), "+PONG\r\n");
    ASSERT_EQ(client.get("broken_pipe_k"), "+VALUE broken_pipe_v\r\n");

    client.disconnect();
    server.stop();

    std::cout << "[PASS] test_server_broken_pipe_immunity" << std::endl;
}

void test_server_wal_persistence_and_recovery() {
    std::string wal_path = "test_server_wal_recovery.wal";
    std::remove(wal_path.c_str());

    int server_port = 0;
    {
        kvstore::KVStore store;
        auto wal = std::make_unique<kvstore::WalManager>(wal_path, kvstore::SyncMode::SYNC_ALWAYS);
        kvstore::Server server(store, wal.get(), "127.0.0.1", 0);
        server.start();
        server_port = server.port();

        kvstore::Client client("127.0.0.1", server_port);
        ASSERT_TRUE(client.connect());

        client.set("wal_k1", "wal_v1");
        client.set("wal_k2", "wal_v2");
        client.del("wal_k1");
        client.set("wal_k3", "wal_v3");

        client.disconnect();
        server.stop();
    }

    // Restart server with same WAL file and verify recovery
    {
        kvstore::KVStore store;
        auto wal = std::make_unique<kvstore::WalManager>(wal_path, kvstore::SyncMode::SYNC_ALWAYS);
        size_t restored = wal->recover(store);
        ASSERT_EQ(restored, 4);

        kvstore::Server server(store, wal.get(), "127.0.0.1", 0);
        server.start();

        kvstore::Client client("127.0.0.1", server.port());
        ASSERT_TRUE(client.connect());

        // k1 was deleted
        ASSERT_EQ(client.get("wal_k1"), "-ERR NOT_FOUND\r\n");
        // k2 and k3 are intact
        ASSERT_EQ(client.get("wal_k2"), "+VALUE wal_v2\r\n");
        ASSERT_EQ(client.get("wal_k3"), "+VALUE wal_v3\r\n");

        client.disconnect();
        server.stop();
    }

    std::remove(wal_path.c_str());
    std::cout << "[PASS] test_server_wal_persistence_and_recovery" << std::endl;
}

int main() {
    std::cout << "==================================================" << std::endl;
    std::cout << "RUNNING SERVER UNIT TESTS" << std::endl;
    std::cout << "==================================================" << std::endl;

    test_server_dynamic_port();
    test_server_reuseaddr_immediate_restart();
    test_server_ping_pong();
    test_server_mutations();
    test_server_concurrent_16_clients();
    test_server_abrupt_disconnect_isolation();
    test_server_broken_pipe_immunity();
    test_server_wal_persistence_and_recovery();

    std::cout << "==================================================" << std::endl;
    std::cout << "ALL SERVER UNIT TESTS PASSED" << std::endl;
    std::cout << "==================================================" << std::endl;
    return 0;
}
