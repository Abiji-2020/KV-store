# KV-Store

A high-performance, concurrent, in-memory key-value store written in modern **C++20**, built from scratch with zero external dependencies.

> **TL;DR** — Think Redis, but written from the ground up in C++20 using only the standard library and POSIX APIs.

---

## ✨ Features

| Feature | Details |
|---|---|
| **Concurrent Storage Engine** | 32 cache-line-aligned shards, global LRU eviction, deadlock-free lock ordering |
| **Write-Ahead Log (WAL)** | CRC32 integrity checks, configurable sync modes, deterministic crash recovery |
| **TCP Server** | Multi-threaded POSIX server, supports RESP2 and inline text protocols |
| **Client CLI** | Interactive REPL and single-shot command modes |
| **Benchmark Harness** | Multi-threaded load generator with p50/p95/p99/p99.9 latency reporting |
| **Test Suite** | 115 unit tests + 280 E2E tests, all passing |
| **Zero Dependencies** | C++20 standard library + POSIX only — no Boost, no external libs |

---

## 📁 Project Structure

```
cpp_kv_store/
├── include/kvstore/        # Public headers
│   ├── kvstore.hpp         # KVStore class — the core storage engine
│   ├── lru_shard.hpp       # Single LRU shard (mutex + doubly-linked list)
│   ├── wal.hpp             # Write-Ahead Log manager
│   ├── protocol.hpp        # RESP2 / inline protocol parser & serializer
│   ├── server.hpp          # TCP server
│   ├── client.hpp          # TCP client library
│   ├── benchmark.hpp       # Benchmark runner & config
│   └── crc32.hpp           # CRC32 (slicing-by-8 implementation)
│
├── src/                    # Implementation files
│   ├── kvstore.cpp         # Global LRU eviction logic
│   ├── lru_shard.cpp       # Per-shard LRU operations
│   ├── wal.cpp             # WAL framing, CRC, sync, replay
│   ├── protocol.cpp        # RESP2 + inline command parsing
│   ├── server.cpp          # Threaded TCP server
│   ├── client.cpp          # TCP client library
│   ├── benchmark.cpp       # Benchmark runner (latency sampling, reporting)
│   ├── server_main.cpp     # kv_server binary entry point
│   ├── client_main.cpp     # kv_client binary entry point
│   └── bench_main.cpp      # kv_bench binary entry point
│
├── tests/
│   ├── unit/               # 11 C++ unit/adversarial test suites
│   └── e2e/                # 4-tier Python E2E test harness (280 tests)
│
├── Makefile                # Primary build system
├── CMakeLists.txt          # CMake alternative
└── .gitignore
```

---

## 🔧 Prerequisites

- **Compiler:** GCC 11+ or Clang 14+ with C++20 support
- **Build tool:** `make` (or `cmake`)
- **OS:** Linux (uses POSIX threading and networking)
- **Python 3.8+** (only needed for the E2E test suite)

Check your compiler version:
```bash
g++ --version    # need 11+
clang++ --version # need 14+
```

---

## 🚀 Build

### Quick build (recommended)

```bash
git clone https://github.com/Abiji-2020/KV-store.git
cd KV-store
make all
```

This compiles **14 binaries** into `bin/`:

| Binary | Description |
|---|---|
| `bin/kv_server` | The key-value server |
| `bin/kv_client` | Interactive CLI client |
| `bin/kv_bench` | Benchmark harness |
| `bin/test_engine` | Storage engine unit tests |
| `bin/test_wal` | WAL unit tests |
| `bin/test_protocol` | Protocol parser tests |
| `bin/test_server` | Server unit tests |
| `bin/test_benchmark` | Benchmark unit tests |
| `bin/test_stress_challenger` | Concurrency stress tests |
| `bin/test_adversarial_m1_2` | Engine + WAL adversarial tests |
| `bin/test_adversarial_protocol` | Protocol adversarial tests |
| `bin/test_adversarial_server` | Server adversarial tests |
| `bin/test_adversarial_wal_torn` | Torn-write WAL recovery tests |
| `bin/test_adversarial_benchmark` | Benchmark adversarial tests |

### Using CMake

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

---

## 🖥️ Running the Server

```bash
./bin/kv_server [OPTIONS]
```

| Flag | Default | Description |
|---|---|---|
| `-h, --host <ip>` | `127.0.0.1` | Bind IP address |
| `-p, --port <port>` | `7379` | TCP port |
| `-c, --capacity <n>` | `0` (unlimited) | Max items before LRU eviction kicks in |
| `-s, --sync <mode>` | `always` | WAL sync policy: `always` / `batch` / `buffered` |
| `-w, --wal <path>` | _(none)_ | Path to WAL file (enables durability) |
| `-t, --threads <n>` | `4` | Worker thread pool size |

### Examples

```bash
# Start with defaults (port 7379, no WAL, unlimited capacity)
./bin/kv_server

# Production-like: 8 threads, 1 million item capacity, WAL durability
./bin/kv_server --port 7379 --threads 8 --capacity 1000000 --wal /var/data/kv.wal --sync always

# Development: fast, no durability, small capacity
./bin/kv_server --port 7379 --capacity 10000 --sync buffered
```

Stop the server gracefully with `Ctrl+C` (SIGINT/SIGTERM).

---

## 💻 Using the Client

### Interactive REPL mode

```bash
./bin/kv_client --host 127.0.0.1 --port 7379
```

```
kv> SET hello world
+OK
kv> GET hello
world
kv> SET counter 42
+OK
kv> GET counter
42
kv> DEL hello
:1
kv> GET hello
(nil)
kv> PING
+PONG
kv> quit
```

### Single-shot mode

```bash
./bin/kv_client SET mykey myvalue
./bin/kv_client GET mykey
./bin/kv_client DEL mykey
./bin/kv_client PING
```

### Client flags

| Flag | Default | Description |
|---|---|---|
| `-h, --host <ip>` | `127.0.0.1` | Server IP |
| `-p, --port <port>` | `7379` | Server port |

---

## 📊 Running the Benchmark

The benchmark requires a running `kv_server`:

```bash
# Terminal 1 — start the server
./bin/kv_server --port 7379

# Terminal 2 — run the benchmark
./bin/kv_bench [OPTIONS]
```

| Flag | Default | Description |
|---|---|---|
| `-h, --host <ip>` | `127.0.0.1` | Server IP |
| `-p, --port <port>` | `7379` | Server port |
| `-c, --threads <n>` | `4` | Concurrent client threads |
| `-n, --requests <n>` | `1000` | Total operations |
| `-r, --ratio <f>` | `0.8` | Read fraction (0.0–1.0) |
| `-k, --keys <n>` | `100` | Keyspace cardinality |
| `-s, --valsize <n>` | `64` | Value size in bytes |

### Example benchmark run

```bash
# 4 threads, 100k ops, 80% reads, 1k key space, 128-byte values
./bin/kv_bench --threads 4 --requests 100000 --ratio 0.8 --keys 1000 --valsize 128
```

Sample output:
```
=== KV-Store Benchmark Results ===
Threads:      4
Operations:   100000  (80000 GET / 20000 SET)
Elapsed:      1.243 s
Throughput:   80451 ops/sec

Latency Percentiles:
  p50:    0.041 ms
  p95:    0.089 ms
  p99:    0.143 ms
  p99.9:  0.312 ms
```

---

## ✅ Running Tests

### Unit tests (all 11 suites)

```bash
make test
```

Expected output: **115/115 tests pass**.

### End-to-end tests (4-tier Python harness)

```bash
cd tests/e2e
./run_e2e.sh -v
```

Expected output: **280/280 tests pass**.

The E2E suite automatically starts/stops `kv_server` instances and tests:
- **Tier 1:** Core feature correctness (SET/GET/DEL/PING, persistence, eviction)
- **Tier 2:** Boundary conditions (empty values, large payloads, max keys)
- **Tier 3:** Combination workloads (concurrent ops, mixed workloads)
- **Tier 4:** Adversarial scenarios (crash recovery, malformed input, reconnects)

---

## 🏗️ Architecture

### Storage Engine

```
KVStore (32 shards)
├── LruShard[0]  (alignas(64))  ← prevents false sharing
├── LruShard[1]
├── ...
└── LruShard[31]
```

- Each shard owns a `std::mutex` and a doubly-linked LRU list
- `GET`/`DEL` lock only the relevant shard (key hashed to shard index)
- `SET` with capacity: acquires all 32 shards in ascending order (deadlock-free), finds the globally oldest entry across all shards, evicts it, then sets the new key

### Write-Ahead Log

Each WAL record is framed as:

```
[ 2B magic "WL" | 8B timestamp | 1B opcode | 4B key_len | 4B val_len | payload | 4B CRC32 ]
```

- **CRC32** covers the entire record (slicing-by-8 for speed)
- **Sync modes:** `always` (fsync per write), `batch` (fsync periodically), `buffered` (OS-managed)
- On startup, replays the WAL from beginning, skipping any torn/corrupt records

### Protocol

Supports two wire formats on the same port:

| Format | Example | Notes |
|---|---|---|
| **Inline** | `SET foo bar\r\n` | Human-readable, great for `telnet` |
| **RESP2** | `*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` | Redis-compatible binary-safe |

Commands supported: `SET`, `GET`, `DEL`, `PING`

---

## 🔗 Protocol Compatibility

The server speaks **RESP2** — the same wire protocol used by Redis. This means you can connect using:

```bash
# Using netcat (inline protocol)
echo -e "PING\r\n" | nc 127.0.0.1 7379

# Using redis-cli
redis-cli -p 7379 SET foo bar
redis-cli -p 7379 GET foo
```

---

## 🛠️ Development

### Build only source (no tests)

```bash
make kv_server kv_client kv_bench
```

### Run a single test suite

```bash
./bin/test_engine
./bin/test_wal
./bin/test_server
./bin/test_stress_challenger
```

### Clean build artifacts

```bash
make clean
```

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

## 👤 Author

**Abinand P** — [github.com/Abiji-2020](https://github.com/Abiji-2020)
