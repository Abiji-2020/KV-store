#!/usr/bin/env python3
"""
Tier 3: Cross-Feature Combinations Test Suite (20 Pairwise & Cross-Cutting Tests).
Verifies complex multi-feature interactions, race conditions across modules, and protocol/storage/WAL coupling.
"""

import os
import time
import socket
import tempfile
import threading
from harness_utils import (
    KVServerProcess, KVClient, KVClientCLI, KVBenchCLI, WalDecoder,
    ReferenceOracle, compute_crc32, find_free_port,
    SERVER_BIN, CLIENT_BIN, BENCH_BIN, MAKEFILE_PATH,
    OP_SET, OP_DEL, OP_CLEAR
)

TESTS = []

def register_test(test_id, name, milestone, description):
    def decorator(fn):
        TESTS.append({
            "id": test_id,
            "tier": 3,
            "name": name,
            "milestone": milestone,
            "description": description,
            "fn": fn
        })
        return fn
    return decorator


@register_test("T3_COMB_01", "test_comb_concurrent_set_and_del", "M1", "Concurrent interleaved SET and DEL on intersecting keys")
def test_comb_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        errors = []
        def worker(tid):
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(50):
                        k = f"shared_{i % 10}"
                        c.set(k, f"val_{tid}_{i}")
                        if i % 2 == 0:
                            c.del_key(k)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0

@register_test("T3_COMB_02", "test_comb_eviction_during_concurrent_gets", "M1", "Eviction occurring while readers are actively doing GETs")
def test_comb_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=20) as srv:
        stop_event = threading.Event()
        errors = []
        def reader():
            try:
                with KVClient(port=srv.port) as c:
                    while not stop_event.is_set():
                        c.get("target_key")
            except Exception as e:
                errors.append(e)

        def evictor():
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(100):
                        c.set(f"evict_k_{i}", f"val_{i}")
            except Exception as e:
                errors.append(e)

        r = threading.Thread(target=reader)
        e = threading.Thread(target=evictor)
        r.start()
        e.start()
        e.join()
        stop_event.set()
        r.join()
        assert len(errors) == 0

@register_test("T3_COMB_03", "test_comb_wal_append_under_concurrent_writes", "M2", "WAL sequentially appends with CRC32 under 4 concurrent writer threads")
def test_comb_03(ctx):
    wal = ctx.get_temp_path("comb_wal.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal) as srv:
        errors = []
        def writer(tid):
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(25):
                        c.set(f"t_{tid}_k_{i}", f"v_{i}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0
    records, _, has_torn = WalDecoder.decode_file(wal)
    assert len(records) == 100 and not has_torn

@register_test("T3_COMB_04", "test_comb_recovery_with_eviction_capacity", "M2", "WAL replay into store with lower capacity than log entries")
def test_comb_04(ctx):
    wal = ctx.get_temp_path("comb_rec_cap.wal")
    with open(wal, "wb") as f:
        for i in range(50):
            f.write(WalDecoder.encode_record(OP_SET, f"key_{i}", f"val_{i}"))
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal, capacity=10) as srv:
        with KVClient(port=srv.port) as c:
            s = c.stats()
            assert "keys=10" in s or "key_count=10" in s
            # The last 10 keys (key_40 .. key_49) should be present
            assert "val_49" in c.get("key_49")

@register_test("T3_COMB_05", "test_comb_client_disconnect_during_large_set", "M3", "Client disconnects abruptly mid-upload; server recovers cleanly")
def test_comb_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", srv.port))
        # Send partial large set
        s.sendall(b"*3\r\n$3\r\nSET\r\n$7\r\nlarge_k\r\n$100000\r\nabcdef")
        s.close()
        time.sleep(0.05)
        # Server should remain operational for new clients
        with KVClient(port=srv.port) as c:
            assert "+PONG" in c.ping() or "PONG" in c.ping()

@register_test("T3_COMB_06", "test_comb_interleaved_resp2_and_inline_clients", "M3", "Simultaneous RESP2 and inline clients against same store")
def test_comb_06(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c_inline:
            with KVClient(port=srv.port) as c_resp:
                c_inline.cmd_inline("SET shared_proto val1")
                got_resp = c_resp.cmd_resp2("GET", "shared_proto")
                assert "val1" in got_resp
                c_resp.cmd_resp2("SET", "shared_proto", "val2")
                got_inline = c_inline.cmd_inline("GET shared_proto")
                assert "val2" in got_inline

@register_test("T3_COMB_07", "test_comb_bench_load_with_active_wal_sync", "M4", "Benchmark generates high concurrent load with SYNC_ALWAYS active")
def test_comb_07(ctx):
    wal = ctx.get_temp_path("comb_bench_sync.wal")
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal, sync_mode="always") as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=4, requests=200, ratio=0.5)
        assert code == 0

@register_test("T3_COMB_08", "test_comb_stats_accuracy_under_concurrent_eviction", "M1", "Conservation of keys + evictions under concurrent storm")
def test_comb_08(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=50) as srv:
        errors = []
        def worker(tid):
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(50):
                        c.set(f"cons_{tid}_{i}", "v")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()
        with KVClient(port=srv.port) as c:
            s = c.stats()
            assert "keys=50" in s or "key_count=50" in s

@register_test("T3_COMB_09", "test_comb_del_reinsert_same_key_across_recovery", "M2", "SET, DEL, re-SET across crash recovery replay")
def test_comb_09(ctx):
    wal = ctx.get_temp_path("comb_del_reinsert.wal")
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port, wal_path=wal, sync_mode="always")
    srv1.start()
    with KVClient(port=srv1.port) as c:
        c.set("cycle", "first")
        c.del_key("cycle")
        c.set("cycle", "second")
    srv1.kill_crash()

    with KVServerProcess(port=find_free_port(), wal_path=wal) as srv2:
        with KVClient(port=srv2.port) as c:
            assert "second" in c.get("cycle")

@register_test("T3_COMB_10", "test_comb_rapid_restart_and_reconnect", "M3", "Server killed and immediately restarted on same port; client reconnects")
def test_comb_10(ctx):
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port)
    srv1.start()
    srv1.kill_crash()
    time.sleep(0.05)
    srv2 = KVServerProcess(port=ctx.port)
    started = srv2.start()
    assert started
    with KVClient(port=ctx.port) as c:
        assert "+PONG" in c.ping() or "PONG" in c.ping()
    srv2.stop()

@register_test("T3_COMB_11", "test_comb_eviction_with_multiple_shards", "M1", "Distribution across 32 shards with independent LRU eviction")
def test_comb_11(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=64) as srv:
        with KVClient(port=srv.port) as c:
            for i in range(128):
                c.set(f"shard_key_{i}", f"val_{i}")
            s = c.stats()
            assert "keys=64" in s or "key_count=64" in s

@register_test("T3_COMB_12", "test_comb_bench_varying_val_sizes", "M4", "Benchmark operates smoothly across varying payload sizes")
def test_comb_12(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        for size in [32, 128, 512]:
            code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=100, val_size=size)
            assert code == 0

@register_test("T3_COMB_13", "test_comb_pipelined_commands_with_quit", "M3", "Pipelined commands terminated by QUIT in same stream")
def test_comb_13(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.send_raw(b"SET pipe_q v\r\nGET pipe_q\r\nQUIT\r\n")
            r1 = c.read_line()
            r2 = c.read_line()
            r3 = c.read_line()
            assert "+OK" in r1 or "OK" in r1
            assert "v" in r2
            assert "+OK" in r3 or "OK" in r3 or r3 == ""

@register_test("T3_COMB_14", "test_comb_wal_crash_during_del", "M2", "Crash recovery handles DEL as the final recorded mutation")
def test_comb_14(ctx):
    wal = ctx.get_temp_path("comb_del_crash.wal")
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port, wal_path=wal, sync_mode="always")
    srv1.start()
    with KVClient(port=srv1.port) as c:
        c.set("k1", "v1")
        c.del_key("k1")
    srv1.kill_crash()

    with KVServerProcess(port=find_free_port(), wal_path=wal) as srv2:
        with KVClient(port=srv2.port) as c:
            assert "NOT_FOUND" in c.get("k1") or "$-1" in c.get("k1")

@register_test("T3_COMB_15", "test_comb_concurrent_reads_on_evicted_key", "M1", "Multiple readers requesting a key concurrently with its eviction")
def test_comb_15(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=5) as srv:
        with KVClient(port=srv.port) as c:
            c.set("target_evict", "initial")
        errors = []
        def reader():
            try:
                with KVClient(port=srv.port) as c:
                    for _ in range(50):
                        c.get("target_evict")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in threads: t.start()
        with KVClient(port=srv.port) as c:
            for i in range(20):
                c.set(f"flood_{i}", "val")
        for t in threads: t.join()
        assert len(errors) == 0

@register_test("T3_COMB_16", "test_comb_clear_and_wal_replay", "M2", "CLEAR mutation logged and accurately replayed")
def test_comb_16(ctx):
    wal = ctx.get_temp_path("comb_clear_rec.wal")
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port, wal_path=wal, sync_mode="always")
    srv1.start()
    with KVClient(port=srv1.port) as c:
        c.set("before_clear", "1")
        c.cmd_inline("CLEAR")
        c.set("after_clear", "2")
    srv1.stop()

    with KVServerProcess(port=find_free_port(), wal_path=wal) as srv2:
        with KVClient(port=srv2.port) as c:
            assert "NOT_FOUND" in c.get("before_clear") or "$-1" in c.get("before_clear")
            assert "2" in c.get("after_clear")

@register_test("T3_COMB_17", "test_comb_multiple_clients_repl_and_single_shot", "M3", "Interactive REPL client and single-shot CLI query simultaneously")
def test_comb_17(ctx):
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code1, _, _ = KVClientCLI.run_single_shot(srv.port, ["SET", "cli_sync", "val"])
        code2, out2, _ = KVClientCLI.run_repl(srv.port, ["GET cli_sync", "QUIT"])
        assert code1 == 0 and code2 == 0
        assert "val" in out2

@register_test("T3_COMB_18", "test_comb_extreme_payload_and_eviction", "M1", "Large 512KB payload inserted and subsequently evicted")
def test_comb_18(ctx):
    ctx.require_server()
    large_payload = "E" * 524288
    with KVServerProcess(port=ctx.port, capacity=2) as srv:
        with KVClient(port=srv.port) as c:
            c.cmd_resp2("SET", "huge_k", large_payload)
            c.set("k2", "v2")
            c.set("k3", "v3")  # huge_k evicted
            assert "NOT_FOUND" in c.get("huge_k") or "$-1" in c.get("huge_k")

@register_test("T3_COMB_19", "test_comb_corrupted_wal_tail_and_subsequent_server_run", "M2", "Server recovers from torn WAL, accepts new writes, and shuts down cleanly")
def test_comb_19(ctx):
    wal = ctx.get_temp_path("comb_torn_sub.wal")
    with open(wal, "wb") as f:
        f.write(WalDecoder.encode_record(OP_SET, "clean_1", "v1"))
        f.write(b"\x57\x4C\x01\x00\xFF\xAA")  # torn tail
    ctx.require_server()
    srv = KVServerProcess(port=ctx.port, wal_path=wal, sync_mode="always")
    srv.start()
    with KVClient(port=srv.port) as c:
        assert "v1" in c.get("clean_1")
        c.set("new_after_torn", "v2")
    srv.stop()

    records, _, has_torn = WalDecoder.decode_file(wal)
    assert len(records) >= 2

@register_test("T3_COMB_20", "test_comb_concurrent_bench_and_cli", "M4", "Benchmark runs continuous load while CLI queries STATS in loop")
def test_comb_20(ctx):
    ctx.require_bench()
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        bench_thread = threading.Thread(target=lambda: KVBenchCLI.run_bench(srv.port, threads=4, requests=300))
        bench_thread.start()
        time.sleep(0.05)
        code, out, _ = KVClientCLI.run_single_shot(srv.port, ["STATS"])
        assert code == 0
        bench_thread.join()
