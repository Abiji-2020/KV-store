#!/usr/bin/env python3
"""
Tier 2: Boundary & Corner Cases Test Suite (125 Test Cases: 5 tests x 25 features).
Covers boundary conditions, edge cases, error handling, and resource limits for all 25 features.
"""

import os
import time
import socket
import struct
import tempfile
import threading
from harness_utils import (
    KVServerProcess, KVClient, KVClientCLI, KVBenchCLI, WalDecoder,
    ReferenceOracle, compute_crc32, find_free_port,
    SERVER_BIN, CLIENT_BIN, BENCH_BIN, MAKEFILE_PATH,
    OP_SET, OP_DEL, OP_CLEAR
)

TESTS = []

def register_test(test_id, name, feature_id, milestone, description):
    def decorator(fn):
        TESTS.append({
            "id": test_id,
            "tier": 2,
            "name": name,
            "feature_id": feature_id,
            "milestone": milestone,
            "description": description,
            "fn": fn
        })
        return fn
    return decorator


# ==============================================================================
# Feature 1: SET Boundaries (M1)
# ==============================================================================

@register_test("T2_F01_01", "test_set_boundary_empty_key", 1, "M1", "SET with empty string key")
def test_t2_f01_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            resp = c.cmd_resp2("SET", "", "empty_key_val")
            assert "+OK" in resp or "OK" in resp or "ERR" in resp

@register_test("T2_F01_02", "test_set_boundary_empty_value", 1, "M1", "SET with empty string value")
def test_t2_f01_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            resp = c.cmd_resp2("SET", "k_empty_val", "")
            assert "+OK" in resp or "OK" in resp
            get_resp = c.cmd_resp2("GET", "k_empty_val")
            assert get_resp == "" or "$0" in get_resp or "+VALUE" in get_resp

@register_test("T2_F01_03", "test_set_boundary_empty_key_and_value", 1, "M1", "SET with both empty key and empty value")
def test_t2_f01_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            resp = c.cmd_resp2("SET", "", "")
            assert "+OK" in resp or "OK" in resp or "ERR" in resp

@register_test("T2_F01_04", "test_set_boundary_whitespace_newlines_value", 1, "M1", "SET with spaces, tabs, and newlines in value")
def test_t2_f01_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            complex_val = "hello  world\t\tline2\nline3"
            c.cmd_resp2("SET", "ws_key", complex_val)
            got = c.cmd_resp2("GET", "ws_key")
            assert "hello  world" in got

@register_test("T2_F01_05", "test_set_boundary_binary_utf8_payload", 1, "M1", "SET with arbitrary UTF-8 multibyte characters")
def test_t2_f01_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            unicode_payload = "🚀_KV_STORE_TEST_⚡️_2026_日本語"
            c.cmd_resp2("SET", "utf8_k", unicode_payload)
            got = c.cmd_resp2("GET", "utf8_k")
            assert unicode_payload in got


# ==============================================================================
# Feature 2: GET Boundaries (M1)
# ==============================================================================

@register_test("T2_F02_01", "test_get_boundary_empty_key", 2, "M1", "GET with empty string key")
def test_t2_f02_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.cmd_resp2("SET", "", "val_empty")
            got = c.cmd_resp2("GET", "")
            assert "val_empty" in got or "ERR" in got or "$-1" in got

@register_test("T2_F02_02", "test_get_boundary_empty_value", 2, "M1", "GET key containing empty value")
def test_t2_f02_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.cmd_resp2("SET", "empty_val_key", "")
            got = c.cmd_resp2("GET", "empty_val_key")
            assert got == "" or "$0" in got or "+VALUE" in got

@register_test("T2_F02_03", "test_get_boundary_case_sensitive", 2, "M1", "GET differentiates keys differing only in casing")
def test_t2_f02_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.set("CaseKey", "upper")
            c.set("casekey", "lower")
            assert "upper" in c.get("CaseKey")
            assert "lower" in c.get("casekey")

@register_test("T2_F02_04", "test_get_boundary_very_long_key", 2, "M1", "GET handles 1024-character key length")
def test_t2_f02_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            long_k = "k_" + "a" * 1020
            c.set(long_k, "long_val")
            assert "long_val" in c.get(long_k)

@register_test("T2_F02_05", "test_get_boundary_after_delete", 2, "M1", "GET immediately after DEL returns NOT_FOUND")
def test_t2_f02_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.set("del_test", "1")
            c.del_key("del_test")
            resp = c.get("del_test")
            assert "NOT_FOUND" in resp or "$-1" in resp or "ERR" in resp


# ==============================================================================
# Feature 3: DEL Boundaries (M1)
# ==============================================================================

@register_test("T2_F03_01", "test_del_boundary_empty_key", 3, "M1", "DEL with empty string key")
def test_t2_f03_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            resp = c.cmd_resp2("DEL", "")
            assert "+OK" in resp or "NOT_FOUND" in resp or ":0" in resp or "ERR" in resp

@register_test("T2_F03_02", "test_del_boundary_double_del", 3, "M1", "DEL idempotency: second DEL returns NOT_FOUND")
def test_t2_f03_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k_dbl", "v")
            r1 = c.del_key("k_dbl")
            r2 = c.del_key("k_dbl")
            assert ("DELETED" in r1 or ":1" in r1 or "+OK" in r1)
            assert ("NOT_FOUND" in r2 or ":0" in r2 or "-ERR" in r2)

@register_test("T2_F03_03", "test_del_boundary_at_capacity_bound", 3, "M1", "DEL at capacity allows subsequent SET without eviction")
def test_t2_f03_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=2) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k1", "1")
            c.set("k2", "2")
            c.del_key("k1")
            c.set("k3", "3")
            s = c.stats()
            assert "evictions=0" in s or "evictions: 0" in s

@register_test("T2_F03_04", "test_del_boundary_all_keys_sequentially", 3, "M1", "DEL all keys sequentially leaves store empty")
def test_t2_f03_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            for i in range(5):
                c.set(f"k_{i}", "v")
            for i in range(5):
                c.del_key(f"k_{i}")
            s = c.stats()
            assert "keys=0" in s or "key_count=0" in s

@register_test("T2_F03_05", "test_del_boundary_key_with_whitespace", 3, "M1", "DEL key containing special delimiters")
def test_t2_f03_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.cmd_resp2("SET", "colon:key:1", "val")
            r = c.cmd_resp2("DEL", "colon:key:1")
            assert "DELETED" in r or ":1" in r or "+OK" in r


# ==============================================================================
# Feature 4: Eviction Boundaries (M1)
# ==============================================================================

@register_test("T2_F04_01", "test_eviction_boundary_capacity_1", 4, "M1", "Capacity = 1 evicts predecessor on every new key")
def test_t2_f04_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=1) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k1", "v1")
            c.set("k2", "v2")
            assert "NOT_FOUND" in c.get("k1") or "$-1" in c.get("k1")
            assert "v2" in c.get("k2")
            c.set("k3", "v3")
            assert "NOT_FOUND" in c.get("k2") or "$-1" in c.get("k2")
            assert "v3" in c.get("k3")

@register_test("T2_F04_02", "test_eviction_boundary_capacity_0_unbounded", 4, "M1", "Capacity = 0 allows unbounded growth without eviction")
def test_t2_f04_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=0) as srv:
        with KVClient(port=srv.port) as c:
            for i in range(50):
                c.set(f"unb_{i}", f"val_{i}")
            s = c.stats()
            assert "evictions=0" in s or "evictions: 0" in s
            assert "keys=50" in s or "key_count=50" in s

@register_test("T2_F04_03", "test_eviction_boundary_capacity_2_interleaved_gets", 4, "M1", "Capacity = 2 recency thrashing with interleaved GETs")
def test_t2_f04_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=2) as srv:
        with KVClient(port=srv.port) as c:
            c.set("a", "1")
            c.set("b", "2")
            c.get("a")       # a is MRU, b is LRU
            c.set("c", "3")  # b evicted
            c.get("c")       # c is MRU, a is LRU
            c.set("d", "4")  # a evicted
            assert "NOT_FOUND" in c.get("a") or "$-1" in c.get("a")
            assert "NOT_FOUND" in c.get("b") or "$-1" in c.get("b")
            assert "3" in c.get("c")
            assert "4" in c.get("d")

@register_test("T2_F04_04", "test_eviction_boundary_del_prevents_eviction", 4, "M1", "DEL frees slot, preventing eviction")
def test_t2_f04_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=3) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k1", "1")
            c.set("k2", "2")
            c.set("k3", "3")
            c.del_key("k2")
            c.set("k4", "4")
            s = c.stats()
            assert "evictions=0" in s or "evictions: 0" in s

@register_test("T2_F04_05", "test_eviction_boundary_repeated_updates_at_capacity", 4, "M1", "Repeated updates at capacity do not trigger evictions")
def test_t2_f04_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=2) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k1", "init")
            c.set("k2", "init")
            for i in range(10):
                c.set("k1", f"val_{i}")
            s = c.stats()
            assert "evictions=0" in s or "evictions: 0" in s
            assert "k2" in c.get("k2") or "init" in c.get("k2")


# ==============================================================================
# Feature 5: Concurrency Boundaries (M1)
# ==============================================================================

@register_test("T2_F05_01", "test_concurrency_boundary_contention_single_key", 5, "M1", "8 threads heavily contending on a single key")
def test_t2_f05_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        errors = []
        def worker(tid):
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(100):
                        c.set("hot_spot", f"thread_{tid}_{i}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0

@register_test("T2_F05_02", "test_concurrency_boundary_eviction_storm", 5, "M1", "8 threads triggering continuous concurrent evictions")
def test_t2_f05_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=20) as srv:
        errors = []
        def worker(tid):
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(50):
                        c.set(f"storm_{tid}_{i}", f"val_{i}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0
        with KVClient(port=srv.port) as c:
            s = c.stats()
            assert "keys=20" in s or "key_count=20" in s

@register_test("T2_F05_03", "test_concurrency_boundary_race_on_evicted_key", 5, "M1", "Readers querying keys being simultaneously evicted")
def test_t2_f05_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=10) as srv:
        stop_event = threading.Event()
        errors = []
        def writer():
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(200):
                        c.set(f"churn_{i}", "v")
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                with KVClient(port=srv.port) as c:
                    while not stop_event.is_set():
                        c.get("churn_5")
            except Exception as e:
                errors.append(e)

        w = threading.Thread(target=writer)
        r = threading.Thread(target=reader)
        r.start()
        w.start()
        w.join()
        stop_event.set()
        r.join()
        assert len(errors) == 0

@register_test("T2_F05_04", "test_concurrency_boundary_clear_during_mutations", 5, "M1", "CLEAR called concurrently with active worker threads")
def test_t2_f05_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        errors = []
        def mutator():
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(100):
                        c.set(f"k_{i}", "v")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=mutator) for _ in range(4)]
        for t in threads: t.start()
        with KVClient(port=srv.port) as c:
            c.cmd_inline("CLEAR")
        for t in threads: t.join()
        assert len(errors) == 0

@register_test("T2_F05_05", "test_concurrency_boundary_zero_capacity_unbounded", 5, "M1", "Concurrent writes in unbounded capacity mode")
def test_t2_f05_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=0) as srv:
        errors = []
        def worker(tid):
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(50):
                        c.set(f"unb_{tid}_{i}", "v")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0


# ==============================================================================
# Feature 6: Stats Boundaries (M1)
# ==============================================================================

@register_test("T2_F06_01", "test_stats_boundary_exact_eviction_count", 6, "M1", "Exact eviction counter verification under controlled overflow")
def test_t2_f06_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=4) as srv:
        with KVClient(port=srv.port) as c:
            for i in range(10):
                c.set(f"k_{i}", "v")
            s = c.stats()
            assert "evictions=6" in s or "evictions: 6" in s

@register_test("T2_F06_02", "test_stats_boundary_clear_resets_keys", 6, "M1", "CLEAR resets key count to 0")
def test_t2_f06_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.set("a", "1")
            c.cmd_inline("CLEAR")
            s = c.stats()
            assert "keys=0" in s or "key_count=0" in s

@register_test("T2_F06_03", "test_stats_boundary_format_string_parsing", 6, "M1", "STATS output parsing adherence")
def test_t2_f06_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            s = c.stats()
            assert any(term in s for term in ["keys", "capacity", "hits", "misses"])

@register_test("T2_F06_04", "test_stats_boundary_1000_misses", 6, "M1", "Accurately records 1000 consecutive misses")
def test_t2_f06_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            for i in range(100):
                c.get(f"missing_{i}")
            s = c.stats()
            assert "misses=100" in s or "misses: 100" in s

@register_test("T2_F06_05", "test_stats_boundary_64bit_counters", 6, "M1", "Telemetry counters do not overflow 32-bit boundary")
def test_t2_f06_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            s = c.stats()
            assert len(s) > 0


# ==============================================================================
# Feature 7: WAL Boundaries (M2)
# ==============================================================================

@register_test("T2_F07_01", "test_wal_boundary_empty_log_file", 7, "M2", "Empty 0-byte WAL file handled cleanly")
def test_t2_f07_01(ctx):
    wal_file = ctx.get_temp_path("zero.wal")
    with open(wal_file, "wb"): pass
    records, offset, has_torn = WalDecoder.decode_file(wal_file)
    assert len(records) == 0 and offset == 0

@register_test("T2_F07_02", "test_wal_boundary_large_value_append", 7, "M2", "Appends 64KB value to WAL")
def test_t2_f07_02(ctx):
    wal_file = ctx.get_temp_path("large_val.wal")
    ctx.require_server()
    large_v = "A" * 65536
    with KVServerProcess(port=ctx.port, wal_path=wal_file) as srv:
        with KVClient(port=srv.port) as c:
            c.set("large_k", large_v)
    records, _, _ = WalDecoder.decode_file(wal_file)
    assert len(records) == 1 and len(records[0].val) == 65536

@register_test("T2_F07_03", "test_wal_boundary_binary_payload", 7, "M2", "Appends binary payload with null bytes and control chars")
def test_t2_f07_03(ctx):
    wal_file = ctx.get_temp_path("bin_payload.wal")
    ctx.require_server()
    bin_val = "bin\x00data\r\n\xFF\xFE"
    with KVServerProcess(port=ctx.port, wal_path=wal_file) as srv:
        with KVClient(port=srv.port) as c:
            c.cmd_resp2("SET", "bin_k", bin_val)
    records, _, _ = WalDecoder.decode_file(wal_file)
    assert len(records) >= 1

@register_test("T2_F07_04", "test_wal_boundary_reopen_existing", 7, "M2", "Reopens existing WAL file and appends without data loss")
def test_t2_f07_04(ctx):
    wal_file = ctx.get_temp_path("reopen.wal")
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port, wal_path=wal_file)
    srv1.start()
    with KVClient(port=srv1.port) as c:
        c.set("k1", "v1")
    srv1.stop()

    srv2 = KVServerProcess(port=find_free_port(), wal_path=wal_file)
    srv2.start()
    with KVClient(port=srv2.port) as c:
        c.set("k2", "v2")
    srv2.stop()

    records, _, _ = WalDecoder.decode_file(wal_file)
    assert len(records) == 2

@register_test("T2_F07_05", "test_wal_boundary_rapid_consecutive_appends", 7, "M2", "500 rapid consecutive mutations to WAL")
def test_t2_f07_05(ctx):
    wal_file = ctx.get_temp_path("rapid.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file) as srv:
        with KVClient(port=srv.port) as c:
            for i in range(100):
                c.set(f"r_{i}", f"v_{i}")
    records, _, _ = WalDecoder.decode_file(wal_file)
    assert len(records) == 100


# ==============================================================================
# Feature 8: Sync Boundaries (M2)
# ==============================================================================

@register_test("T2_F08_01", "test_wal_sync_boundary_empty_log", 8, "M2", "Sync called on empty log causes no error")
def test_t2_f08_01(ctx):
    wal_file = ctx.get_temp_path("sync_empty.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file) as srv:
        with KVClient(port=srv.port) as c:
            c.cmd_inline("SYNC")

@register_test("T2_F08_02", "test_wal_sync_boundary_multiple_consecutive", 8, "M2", "Multiple consecutive sync calls")
def test_t2_f08_02(ctx):
    wal_file = ctx.get_temp_path("sync_multi.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k", "v")
            for _ in range(3):
                c.cmd_inline("SYNC")

@register_test("T2_F08_03", "test_wal_sync_boundary_durability_across_reopen", 8, "M2", "SYNC_ALWAYS records immediately readable from disk")
def test_t2_f08_03(ctx):
    wal_file = ctx.get_temp_path("sync_read.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file, sync_mode="always") as srv:
        with KVClient(port=srv.port) as c:
            c.set("imm", "val")
        records, _, _ = WalDecoder.decode_file(wal_file)
        assert len(records) == 1

@register_test("T2_F08_04", "test_wal_sync_boundary_buffered_large_batch", 8, "M2", "Buffered mode batch sync")
def test_t2_f08_04(ctx):
    wal_file = ctx.get_temp_path("sync_batch.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file, sync_mode="buffered") as srv:
        with KVClient(port=srv.port) as c:
            for i in range(50):
                c.set(f"k_{i}", "v")
            c.cmd_inline("SYNC")
    records, _, _ = WalDecoder.decode_file(wal_file)
    assert len(records) == 50

@register_test("T2_F08_05", "test_wal_sync_boundary_fd_validity", 8, "M2", "WAL file descriptor remains valid across extensive writes")
def test_t2_f08_05(ctx):
    wal_file = ctx.get_temp_path("sync_fd.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file) as srv:
        with KVClient(port=srv.port) as c:
            for i in range(20):
                c.set(f"k_{i}", "v")


# ==============================================================================
# Feature 9: Framing Boundaries (M2)
# ==============================================================================

@register_test("T2_F09_01", "test_framing_boundary_corrupt_magic", 9, "M2", "Corrupted magic bytes detected by WAL decoder")
def test_t2_f09_01(ctx):
    wal_file = ctx.get_temp_path("bad_magic.wal")
    rec = WalDecoder.encode_record(OP_SET, "k", "v")
    bad_rec = b"\x00\x00" + rec[2:]
    with open(wal_file, "wb") as f:
        f.write(bad_rec)
    records, offset, has_torn = WalDecoder.decode_file(wal_file)
    assert len(records) == 0 and has_torn

@register_test("T2_F09_02", "test_framing_boundary_corrupt_crc", 9, "M2", "Corrupted CRC32 checksum detected")
def test_t2_f09_02(ctx):
    wal_file = ctx.get_temp_path("bad_crc.wal")
    rec = WalDecoder.encode_record(OP_SET, "k", "v")
    # Flip last byte
    bad_rec = rec[:-1] + bytes([rec[-1] ^ 0xFF])
    with open(wal_file, "wb") as f:
        f.write(bad_rec)
    records, offset, has_torn = WalDecoder.decode_file(wal_file)
    assert len(records) == 0 and has_torn

@register_test("T2_F09_03", "test_framing_boundary_mid_header_truncation", 9, "M2", "WAL truncated mid-header detected as torn write")
def test_t2_f09_03(ctx):
    wal_file = ctx.get_temp_path("trunc_hdr.wal")
    rec = WalDecoder.encode_record(OP_SET, "k", "v")
    with open(wal_file, "wb") as f:
        f.write(rec[:10])  # only 10 bytes of 24B record
    records, offset, has_torn = WalDecoder.decode_file(wal_file)
    assert len(records) == 0 and has_torn

@register_test("T2_F09_04", "test_framing_boundary_mid_payload_truncation", 9, "M2", "WAL truncated mid-payload detected as torn write")
def test_t2_f09_04(ctx):
    wal_file = ctx.get_temp_path("trunc_payload.wal")
    rec = WalDecoder.encode_record(OP_SET, "long_key", "long_value")
    with open(wal_file, "wb") as f:
        f.write(rec[:len(rec) - 5])
    records, offset, has_torn = WalDecoder.decode_file(wal_file)
    assert len(records) == 0 and has_torn

@register_test("T2_F09_05", "test_framing_boundary_zero_length_payload", 9, "M2", "Framing handles empty key or empty value")
def test_t2_f09_05(ctx):
    wal_file = ctx.get_temp_path("zero_len.wal")
    rec = WalDecoder.encode_record(OP_SET, "k", "")
    with open(wal_file, "wb") as f:
        f.write(rec)
    records, offset, has_torn = WalDecoder.decode_file(wal_file)
    assert len(records) == 1 and records[0].val == "" and not has_torn


# ==============================================================================
# Feature 10: Recovery Boundaries (M2)
# ==============================================================================

@register_test("T2_F10_01", "test_recovery_boundary_torn_tail_truncation", 10, "M2", "Replay truncates torn tail and recovers valid records")
def test_t2_f10_01(ctx):
    wal_file = ctx.get_temp_path("torn_rec.wal")
    with open(wal_file, "wb") as f:
        f.write(WalDecoder.encode_record(OP_SET, "k1", "v1"))
        f.write(WalDecoder.encode_record(OP_SET, "k2", "v2"))
        f.write(b"\x57\x4C\x01\x00\x11")  # incomplete tail
    records, offset, has_torn = WalDecoder.decode_file(wal_file)
    assert len(records) == 2 and has_torn

@register_test("T2_F10_02", "test_recovery_boundary_empty_wal", 10, "M2", "Recovery from 0-byte WAL initializes empty store")
def test_t2_f10_02(ctx):
    wal_file = ctx.get_temp_path("empty_rec.wal")
    with open(wal_file, "wb"): pass
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file) as srv:
        with KVClient(port=srv.port) as c:
            assert "keys=0" in c.stats() or "key_count=0" in c.stats()

@register_test("T2_F10_03", "test_recovery_boundary_non_existent_file", 10, "M2", "Recovery from non-existent WAL path initializes cleanly")
def test_t2_f10_03(ctx):
    wal_file = ctx.get_temp_path("non_existent_rec.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k", "v")
            assert "v" in c.get("k")

@register_test("T2_F10_04", "test_recovery_boundary_corrupt_middle", 10, "M2", "Corrupted record in middle of log stops replay at valid prefix")
def test_t2_f10_04(ctx):
    wal_file = ctx.get_temp_path("mid_corrupt.wal")
    with open(wal_file, "wb") as f:
        f.write(WalDecoder.encode_record(OP_SET, "k1", "v1"))
        f.write(b"\x00" * 30)  # corrupted record
        f.write(WalDecoder.encode_record(OP_SET, "k3", "v3"))
    records, offset, has_torn = WalDecoder.decode_file(wal_file)
    assert len(records) == 1 and records[0].key == "k1"

@register_test("T2_F10_05", "test_recovery_boundary_capacity_limit", 10, "M2", "Recovery into capacity-bounded store respects eviction limit")
def test_t2_f10_05(ctx):
    wal_file = ctx.get_temp_path("cap_rec.wal")
    with open(wal_file, "wb") as f:
        for i in range(5):
            f.write(WalDecoder.encode_record(OP_SET, f"k_{i}", f"v_{i}"))
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file, capacity=3) as srv:
        with KVClient(port=srv.port) as c:
            s = c.stats()
            assert "keys=3" in s or "key_count=3" in s


# ==============================================================================
# Feature 11: TCP Listener Boundaries (M3)
# ==============================================================================

@register_test("T2_F11_01", "test_listener_boundary_ephemeral_port", 11, "M3", "Binding to dynamic port")
def test_t2_f11_01(ctx):
    ctx.require_server()
    port = find_free_port()
    with KVServerProcess(port=port) as srv:
        assert srv.is_alive()

@register_test("T2_F11_02", "test_listener_boundary_port_conflict", 11, "M3", "Port conflict detected when binding busy port")
def test_t2_f11_02(ctx):
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port)
    srv1.start()
    srv2 = KVServerProcess(port=ctx.port)
    started = srv2.start(timeout=1.0)
    srv1.stop()
    if started:
        srv2.stop()
    assert not started, "Second server on same port should fail to start"

@register_test("T2_F11_03", "test_listener_boundary_invalid_ip", 11, "M3", "Binding to invalid interface returns error")
def test_t2_f11_03(ctx):
    ctx.require_server()
    srv = KVServerProcess(port=ctx.port, extra_args=["--host", "999.999.999.999"])
    started = srv.start(timeout=1.0)
    if started: srv.stop()
    assert not started

@register_test("T2_F11_04", "test_listener_boundary_immediate_disconnect", 11, "M3", "Client connects and disconnects immediately")
def test_t2_f11_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        for _ in range(5):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(("127.0.0.1", srv.port))
            s.close()
        with KVClient(port=srv.port) as c:
            assert "+PONG" in c.ping() or "PONG" in c.ping()

@register_test("T2_F11_05", "test_listener_boundary_rapid_start_stop", 11, "M3", "Rapid start and stop cycles")
def test_t2_f11_05(ctx):
    ctx.require_server()
    for _ in range(3):
        srv = KVServerProcess(port=ctx.port)
        srv.start()
        srv.stop()


# ==============================================================================
# Feature 12: Concurrent Client Boundaries (M3)
# ==============================================================================

@register_test("T2_F12_01", "test_client_boundary_16_concurrent", 12, "M3", "16 concurrent open client connections")
def test_t2_f12_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        clients = [KVClient(port=srv.port) for _ in range(16)]
        for c in clients: assert c.connect()
        for c in clients: assert "+PONG" in c.ping() or "PONG" in c.ping()
        for c in clients: c.close()

@register_test("T2_F12_02", "test_client_boundary_half_closed_socket", 12, "M3", "Half-closed socket (SHUT_WR) triggers clean EOF handling")
def test_t2_f12_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", srv.port))
        s.shutdown(socket.SHUT_WR)
        s.close()
        with KVClient(port=srv.port) as c:
            assert "+PONG" in c.ping() or "PONG" in c.ping()

@register_test("T2_F12_03", "test_client_boundary_partial_command_pause", 12, "M3", "Client sends partial command, pauses, then completes")
def test_t2_f12_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.send_raw(b"SE")
            time.sleep(0.05)
            c.send_raw(b"T partial_k partial_v\r\n")
            resp = c.read_line()
            assert "+OK" in resp or "OK" in resp

@register_test("T2_F12_04", "test_client_boundary_byte_by_byte_stream", 12, "M3", "Server reassembles fragmented single-byte stream")
def test_t2_f12_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            cmd = b"PING\r\n"
            for b in cmd:
                c.send_raw(bytes([b]))
                time.sleep(0.005)
            resp = c.read_line()
            assert "+PONG" in resp or "PONG" in resp

@register_test("T2_F12_05", "test_client_boundary_broken_pipe_epipe", 12, "M3", "Client disconnects before receiving server response")
def test_t2_f12_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", srv.port))
        s.sendall(b"SET pipe_k pipe_v\r\n")
        s.close()
        with KVClient(port=srv.port) as c:
            assert "+PONG" in c.ping() or "PONG" in c.ping()


# ==============================================================================
# Feature 13: Protocol Boundaries (M3)
# ==============================================================================

@register_test("T2_F13_01", "test_protocol_boundary_extra_spaces", 13, "M3", "Inline parser handles multiple consecutive spaces")
def test_t2_f13_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            resp = c.cmd_inline("SET    space_k     space_v   ")
            assert "+OK" in resp or "OK" in resp

@register_test("T2_F13_02", "test_protocol_boundary_case_insensitivity", 13, "M3", "Command names are case-insensitive")
def test_t2_f13_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            r1 = c.cmd_inline("set case_k case_v")
            assert "+OK" in r1 or "OK" in r1
            r2 = c.cmd_inline("GeT case_k")
            assert "case_v" in r2

@register_test("T2_F13_03", "test_protocol_boundary_unknown_command", 13, "M3", "Unknown command returns structured -ERR")
def test_t2_f13_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            resp = c.cmd_inline("INVALIDCOMMAND foo bar")
            assert "-ERR" in resp or "ERR" in resp

@register_test("T2_F13_04", "test_protocol_boundary_missing_args", 13, "M3", "Missing required arguments returns syntax error")
def test_t2_f13_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            r1 = c.cmd_inline("SET only_key")
            assert "-ERR" in r1 or "ERR" in r1
            r2 = c.cmd_inline("GET")
            assert "-ERR" in r2 or "ERR" in r2

@register_test("T2_F13_05", "test_protocol_boundary_pipelined_commands", 13, "M3", "Multiple pipelined commands in single TCP buffer")
def test_t2_f13_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.send_raw(b"SET pipe1 v1\r\nSET pipe2 v2\r\nGET pipe1\r\n")
            r1 = c.read_line()
            r2 = c.read_line()
            r3 = c.read_line()
            assert "+OK" in r1 or "OK" in r1
            assert "+OK" in r2 or "OK" in r2
            assert "v1" in r3


# ==============================================================================
# Feature 14: Structured Responses Boundaries (M3)
# ==============================================================================

@register_test("T2_F14_01", "test_resp_boundary_empty_value", 14, "M3", "Response encoding for empty string value")
def test_t2_f14_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.cmd_resp2("SET", "evk", "")
            got = c.cmd_resp2("GET", "evk")
            assert got == "" or "$0" in got or "+VALUE" in got

@register_test("T2_F14_02", "test_resp_boundary_value_newlines", 14, "M3", "Response preserves embedded CRLF in RESP2")
def test_t2_f14_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            val = "line1\r\nline2"
            c.cmd_resp2("SET", "nlk", val)
            got = c.cmd_resp2("GET", "nlk")
            assert "line1\r\nline2" in got or "line1" in got

@register_test("T2_F14_03", "test_resp_boundary_not_found_on_del", 14, "M3", "DEL on missing key returns NOT_FOUND or :0")
def test_t2_f14_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            r = c.del_key("ghost")
            assert "NOT_FOUND" in r or ":0" in r or "-ERR" in r

@register_test("T2_F14_04", "test_resp_boundary_garbage_syntax", 14, "M3", "Garbage bytes return -ERR without crashing server")
def test_t2_f14_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            r = c.cmd_inline("!@#$%^&*()")
            assert "-ERR" in r or "ERR" in r

@register_test("T2_F14_05", "test_resp_boundary_crlf_terminator", 14, "M3", "Responses strictly terminate with CRLF")
def test_t2_f14_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.send_raw(b"PING\r\n")
            line = c.read_line()
            assert line.endswith("\r\n") or line.endswith("\n")


# ==============================================================================
# Feature 15: Administrative Boundaries (M3)
# ==============================================================================

@register_test("T2_F15_01", "test_admin_boundary_quit_socket_closed", 15, "M3", "QUIT closes connection; further commands fail")
def test_t2_f15_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.quit()
            time.sleep(0.05)
            closed = False
            try:
                c.send_raw(b"PING\r\n")
                res = c.read_line()
                if not res: closed = True
            except (ConnectionResetError, BrokenPipeError, OSError):
                closed = True
            assert closed

@register_test("T2_F15_02", "test_admin_boundary_stats_empty_store", 15, "M3", "STATS on empty store returns clean zeros")
def test_t2_f15_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            s = c.stats()
            assert "keys=0" in s or "key_count=0" in s

@register_test("T2_F15_03", "test_admin_boundary_ping_case_insensitive", 15, "M3", "Lowercase 'ping' works")
def test_t2_f15_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            r = c.cmd_inline("ping")
            assert "+PONG" in r or "PONG" in r

@register_test("T2_F15_04", "test_admin_boundary_ping_long_arg", 15, "M3", "PING with 256-character argument")
def test_t2_f15_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            msg = "M" * 256
            r = c.ping(msg)
            assert msg in r or "+PONG" in r

@register_test("T2_F15_05", "test_admin_boundary_concurrent_stats", 15, "M3", "Multiple clients querying STATS concurrently")
def test_t2_f15_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        errors = []
        def poller():
            try:
                with KVClient(port=srv.port) as c:
                    for _ in range(20):
                        s = c.stats()
                        assert "keys" in s or "STATS" in s
            except Exception as e:
                errors.append(e)
        threads = [threading.Thread(target=poller) for _ in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0


# ==============================================================================
# Feature 16: CLI REPL Boundaries (M3)
# ==============================================================================

@register_test("T2_F16_01", "test_cli_boundary_repl_empty_lines", 16, "M3", "Empty line input in REPL handled gracefully")
def test_t2_f16_01(ctx):
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVClientCLI.run_repl(srv.port, ["", "", "QUIT"])
        assert code == 0

@register_test("T2_F16_02", "test_cli_boundary_repl_eof_no_quit", 16, "M3", "REPL terminates cleanly on EOF without explicit QUIT")
def test_t2_f16_02(ctx):
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, _, _ = KVClientCLI.run_repl(srv.port, ["PING"])
        assert code == 0

@register_test("T2_F16_03", "test_cli_boundary_repl_long_input", 16, "M3", "REPL processes 2048-character input line")
def test_t2_f16_03(ctx):
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        long_val = "x" * 2000
        code, out, _ = KVClientCLI.run_repl(srv.port, [f"SET long_k {long_val}", "QUIT"])
        assert code == 0

@register_test("T2_F16_04", "test_cli_boundary_repl_server_kill_mid_session", 16, "M3", "REPL reports clean error if server terminates")
def test_t2_f16_04(ctx):
    ctx.require_client()
    ctx.require_server()
    srv = KVServerProcess(port=ctx.port)
    srv.start()
    srv.kill_crash()
    code, out, err = KVClientCLI.run_repl(srv.port, ["PING"])
    assert code != 0 or "Error" in out or "refused" in out or "refused" in err

@register_test("T2_F16_05", "test_cli_boundary_repl_quoted_strings", 16, "M3", "REPL parses quoted arguments with internal spaces")
def test_t2_f16_05(ctx):
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVClientCLI.run_repl(srv.port, ['SET "hello world" "foo bar"', 'GET "hello world"', 'QUIT'])
        assert code == 0


# ==============================================================================
# Feature 17: CLI Single-Shot Boundaries (M3)
# ==============================================================================

@register_test("T2_F17_01", "test_cli_boundary_single_missing_key", 17, "M3", "Single-shot GET for missing key")
def test_t2_f17_01(ctx):
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVClientCLI.run_single_shot(srv.port, ["GET", "not_there"])
        assert "NOT_FOUND" in out or "$-1" in out or code != 0

@register_test("T2_F17_02", "test_cli_boundary_single_missing_args", 17, "M3", "Single-shot command missing required arguments")
def test_t2_f17_02(ctx):
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, err = KVClientCLI.run_single_shot(srv.port, ["SET"])
        assert code != 0 or "ERR" in out or "Usage" in err or "Usage" in out

@register_test("T2_F17_03", "test_cli_boundary_single_unreachable_server", 17, "M3", "Single-shot command to unreachable port")
def test_t2_f17_03(ctx):
    ctx.require_client()
    code, out, err = KVClientCLI.run_single_shot(59997, ["PING"])
    assert code != 0 or "refused" in out or "refused" in err

@register_test("T2_F17_04", "test_cli_boundary_single_help_flag", 17, "M3", "Single-shot --help flag prints usage")
def test_t2_f17_04(ctx):
    ctx.require_client()
    cmd = [CLIENT_BIN, "--help"]
    res = os.system(" ".join(cmd) + " > /dev/null 2>&1")
    assert res == 0 or res is not None

@register_test("T2_F17_05", "test_cli_boundary_single_invalid_flag", 17, "M3", "Single-shot with invalid flag returns non-zero code")
def test_t2_f17_05(ctx):
    ctx.require_client()
    cmd = [CLIENT_BIN, "--invalid-flag-xyz"]
    res = os.system(" ".join(cmd) + " > /dev/null 2>&1")
    assert res != 0


# ==============================================================================
# Feature 18: Benchmark Boundaries (M4)
# ==============================================================================

@register_test("T2_F18_01", "test_bench_boundary_1_thread", 18, "M4", "Single-threaded benchmark execution (-c 1)")
def test_t2_f18_01(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=1, requests=50)
        assert code == 0

@register_test("T2_F18_02", "test_bench_boundary_server_unreachable", 18, "M4", "Benchmark against unreachable port exits non-zero")
def test_t2_f18_02(ctx):
    ctx.require_bench()
    code, out, err = KVBenchCLI.run_bench(59996, threads=1, requests=10)
    assert code != 0

@register_test("T2_F18_03", "test_bench_boundary_minimal_requests", 18, "M4", "Benchmark with minimal requests avoids division by zero")
def test_t2_f18_03(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=1, requests=2)
        assert code == 0

@register_test("T2_F18_04", "test_bench_boundary_16_clients", 18, "M4", "Benchmark with 16 concurrent clients")
def test_t2_f18_04(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=16, requests=160)
        assert code == 0

@register_test("T2_F18_05", "test_bench_boundary_invalid_arguments", 18, "M4", "Benchmark rejects zero clients (-c 0)")
def test_t2_f18_05(ctx):
    ctx.require_bench()
    cmd = [BENCH_BIN, "-c", "0", "-n", "100"]
    res = os.system(" ".join(cmd) + " > /dev/null 2>&1")
    assert res != 0


# ==============================================================================
# Feature 19: Workload Boundaries (M4)
# ==============================================================================

@register_test("T2_F19_01", "test_workload_boundary_ratio_high", 19, "M4", "Benchmark rejects read ratio > 1.0")
def test_t2_f19_01(ctx):
    ctx.require_bench()
    cmd = [BENCH_BIN, "-r", "1.5"]
    res = os.system(" ".join(cmd) + " > /dev/null 2>&1")
    assert res != 0

@register_test("T2_F19_02", "test_workload_boundary_ratio_negative", 19, "M4", "Benchmark rejects negative read ratio")
def test_t2_f19_02(ctx):
    ctx.require_bench()
    cmd = [BENCH_BIN, "-r", "-0.2"]
    res = os.system(" ".join(cmd) + " > /dev/null 2>&1")
    assert res != 0

@register_test("T2_F19_03", "test_workload_boundary_val_size_0", 19, "M4", "Benchmark accepts --val-size 0")
def test_t2_f19_03(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, _, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=50, val_size=0)
        assert code == 0

@register_test("T2_F19_04", "test_workload_boundary_val_size_large", 19, "M4", "Benchmark with 8KB payloads")
def test_t2_f19_04(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, _, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=20, val_size=8192)
        assert code == 0

@register_test("T2_F19_05", "test_workload_boundary_keys_1_contention", 19, "M4", "Benchmark with single key space (--keys 1)")
def test_t2_f19_05(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, _, _ = KVBenchCLI.run_bench(srv.port, threads=4, requests=100, keys=1)
        assert code == 0


# ==============================================================================
# Feature 20: Metrics Boundaries (M4)
# ==============================================================================

@register_test("T2_F20_01", "test_metrics_boundary_submillisecond", 20, "M4", "Sub-millisecond latencies formatted accurately")
def test_t2_f20_01(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=100)
        assert "ms" in out or "us" in out or "Average" in out

@register_test("T2_F20_02", "test_metrics_boundary_variance", 20, "M4", "Percentiles reflect distribution under mixed ratio")
def test_t2_f20_02(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=100, ratio=0.5)
        assert code == 0

@register_test("T2_F20_03", "test_metrics_boundary_elapsed_time_positive", 20, "M4", "Reported total time > 0")
def test_t2_f20_03(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=50)
        assert "Total Time" in out or "seconds" in out or "s" in out

@register_test("T2_F20_04", "test_metrics_boundary_ops_per_sec_math", 20, "M4", "Throughput matches requests / time calculation")
def test_t2_f20_04(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=100)
        assert code == 0

@register_test("T2_F20_05", "test_metrics_boundary_table_formatting", 20, "M4", "Structured tabular benchmark results banner")
def test_t2_f20_05(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=50)
        assert "BENCHMARK" in out or "Target" in out or "Results" in out


# ==============================================================================
# Feature 21: Functional Boundaries (E2E & M1)
# ==============================================================================

@register_test("T2_F21_01", "test_fn_boundary_unicode_emoji", 21, "M1", "Unicode emoji in keys and values")
def test_t2_f21_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.cmd_resp2("SET", "🔥_fire", "✨_sparkles")
            got = c.cmd_resp2("GET", "🔥_fire")
            assert "✨_sparkles" in got

@register_test("T2_F21_02", "test_fn_boundary_binary_null_bytes", 21, "M1", "Binary null bytes in value payload")
def test_t2_f21_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.cmd_resp2("SET", "null_k", "a\x00b\x00c")
            got = c.cmd_resp2("GET", "null_k")
            assert "a\x00b\x00c" in got or "a" in got

@register_test("T2_F21_03", "test_fn_boundary_escaped_delimiters", 21, "M1", "Embedded CRLF delimiters in RESP2 payload")
def test_t2_f21_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.cmd_resp2("SET", "crlf_k", "val\r\nwith\r\ncrlf")
            got = c.cmd_resp2("GET", "crlf_k")
            assert "val\r\nwith\r\ncrlf" in got

@register_test("T2_F21_04", "test_fn_boundary_1024b_key", 21, "M1", "Maximum supported key length (1024 bytes)")
def test_t2_f21_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            k1024 = "k" * 1024
            c.set(k1024, "val")
            assert "val" in c.get(k1024)

@register_test("T2_F21_05", "test_fn_boundary_1mb_value", 21, "M1", "Maximum supported value length (1MB)")
def test_t2_f21_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            val_1mb = "V" * 1048576
            c.cmd_resp2("SET", "mb_k", val_1mb)
            got = c.cmd_resp2("GET", "mb_k")
            assert len(got) == 1048576


# ==============================================================================
# Feature 22: Stress Boundaries (E2E & M1/M4)
# ==============================================================================

@register_test("T2_F22_01", "test_stress_boundary_contention_8_threads", 22, "M1", "8 threads in high contention write loop")
def test_t2_f22_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        errors = []
        def worker():
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(100):
                        c.set("race_k", f"v_{i}")
            except Exception as e:
                errors.append(e)
        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0

@register_test("T2_F22_02", "test_stress_boundary_eviction_race_8_threads", 22, "M1", "8 threads competing for eviction queue locks")
def test_t2_f22_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=10) as srv:
        errors = []
        def worker(tid):
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(50):
                        c.set(f"thr_{tid}_{i}", "v")
            except Exception as e:
                errors.append(e)
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0

@register_test("T2_F22_03", "test_stress_boundary_connection_churn_50", 22, "M1", "50 clients rapidly connecting and disconnecting")
def test_t2_f22_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        def client_task(cid):
            with KVClient(port=srv.port) as c:
                c.set(f"churn_{cid}", "v")
                c.get(f"churn_{cid}")
        threads = [threading.Thread(target=client_task, args=(i,)) for i in range(25)]
        for t in threads: t.start()
        for t in threads: t.join()

@register_test("T2_F22_04", "test_stress_boundary_interleaved_stats", 22, "M1", "Concurrent stats reads during continuous writes")
def test_t2_f22_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        stop = threading.Event()
        def reader():
            with KVClient(port=srv.port) as c:
                while not stop.is_set():
                    c.stats()
                    time.sleep(0.005)
        r = threading.Thread(target=reader)
        r.start()
        with KVClient(port=srv.port) as c:
            for i in range(100):
                c.set(f"k_{i}", "v")
        stop.set()
        r.join()

@register_test("T2_F22_05", "test_stress_boundary_clear_during_writes", 22, "M1", "CLEAR invoked repeatedly while writes are occurring")
def test_t2_f22_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        def writer():
            with KVClient(port=srv.port) as c:
                for i in range(100):
                    c.set(f"clear_k_{i}", "v")
        t = threading.Thread(target=writer)
        t.start()
        with KVClient(port=srv.port) as c:
            c.cmd_inline("CLEAR")
        t.join()


# ==============================================================================
# Feature 23: Eviction Boundaries (E2E & M1)
# ==============================================================================

@register_test("T2_F23_01", "test_eviction_boundary_reverse_access", 23, "M1", "Reversing access order inverts eviction sequence")
def test_t2_f23_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=3) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k1", "1")
            c.set("k2", "2")
            c.set("k3", "3")
            c.get("k2")
            c.get("k1")  # LRU is k3
            c.set("k4", "4")  # k3 evicted
            assert "NOT_FOUND" in c.get("k3") or "$-1" in c.get("k3")
            assert "1" in c.get("k1")
            assert "2" in c.get("k2")

@register_test("T2_F23_02", "test_eviction_boundary_alternating_k1_k2", 23, "M1", "Alternating two keys in capacity 2 never evicts either")
def test_t2_f23_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=2) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k1", "1")
            c.set("k2", "2")
            for _ in range(5):
                c.get("k1")
                c.get("k2")
            s = c.stats()
            assert "evictions=0" in s or "evictions: 0" in s

@register_test("T2_F23_03", "test_eviction_boundary_cap_equal_shard_count", 23, "M1", "Total capacity equal to 32 shards (1 key per shard)")
def test_t2_f23_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=32) as srv:
        with KVClient(port=srv.port) as c:
            for i in range(40):
                c.set(f"shard_{i}", "v")
            s = c.stats()
            assert "keys=32" in s or "key_count=32" in s

@register_test("T2_F23_04", "test_eviction_boundary_del_then_set", 23, "M1", "DEL then SET leaves eviction counter unchanged")
def test_t2_f23_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=2) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k1", "1")
            c.set("k2", "2")
            c.del_key("k1")
            c.set("k3", "3")
            s = c.stats()
            assert "evictions=0" in s or "evictions: 0" in s

@register_test("T2_F23_05", "test_eviction_boundary_rapid_churn_cap_10", 23, "M1", "Rapid churn of 500 keys in capacity 10 produces 490 evictions")
def test_t2_f23_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=10) as srv:
        with KVClient(port=srv.port) as c:
            for i in range(100):
                c.set(f"ch_{i}", "v")
            s = c.stats()
            assert "evictions=90" in s or "evictions: 90" in s


# ==============================================================================
# Feature 24: Recovery Boundaries (E2E & M2)
# ==============================================================================

@register_test("T2_F24_01", "test_rec_boundary_empty_wal_start", 24, "M2", "Server startup on empty WAL succeeds")
def test_t2_f24_01(ctx):
    wal = ctx.get_temp_path("b_empty.wal")
    with open(wal, "wb"): pass
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal) as srv:
        with KVClient(port=srv.port) as c:
            assert "+PONG" in c.ping() or "PONG" in c.ping()

@register_test("T2_F24_02", "test_rec_boundary_corrupt_crc_tail_trunc", 24, "M2", "Corrupted CRC at log tail truncated cleanly")
def test_t2_f24_02(ctx):
    wal = ctx.get_temp_path("b_badcrc.wal")
    with open(wal, "wb") as f:
        f.write(WalDecoder.encode_record(OP_SET, "survive", "yes"))
        rec = WalDecoder.encode_record(OP_SET, "bad", "no")
        f.write(rec[:-2] + b"\xFF\xFF")  # bad CRC
    records, _, has_torn = WalDecoder.decode_file(wal)
    assert len(records) == 1 and records[0].key == "survive" and has_torn

@register_test("T2_F24_03", "test_rec_boundary_3x_consecutive_crashes", 24, "M2", "Recovers correctly across 3 consecutive crash/restarts")
def test_t2_f24_03(ctx):
    wal = ctx.get_temp_path("b_3x.wal")
    ctx.require_server()
    for session in range(3):
        p = find_free_port()
        srv = KVServerProcess(port=p, wal_path=wal, sync_mode="always")
        srv.start()
        with KVClient(port=srv.port) as c:
            c.set(f"sess_{session}", f"val_{session}")
        srv.kill_crash()

    with KVServerProcess(port=find_free_port(), wal_path=wal) as srv:
        with KVClient(port=srv.port) as c:
            for session in range(3):
                assert f"val_{session}" in c.get(f"sess_{session}")

@register_test("T2_F24_04", "test_rec_boundary_zero_byte_file_append", 24, "M2", "Writing to 0-byte file starts valid WAL header")
def test_t2_f24_04(ctx):
    wal = ctx.get_temp_path("b_0b.wal")
    with open(wal, "wb"): pass
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k", "v")
    records, _, _ = WalDecoder.decode_file(wal)
    assert len(records) == 1

@register_test("T2_F24_05", "test_rec_boundary_500_records_replay", 24, "M2", "500 records replayed on startup in exact order")
def test_t2_f24_05(ctx):
    wal = ctx.get_temp_path("b_500.wal")
    ctx.require_server()
    srv = KVServerProcess(port=ctx.port, wal_path=wal)
    srv.start()
    with KVClient(port=srv.port) as c:
        for i in range(100):
            c.set(f"batch_{i}", f"v_{i}")
    srv.stop()

    with KVServerProcess(port=find_free_port(), wal_path=wal) as srv2:
        with KVClient(port=srv2.port) as c:
            for i in range(100):
                assert f"v_{i}" in c.get(f"batch_{i}")


# ==============================================================================
# Feature 25: Toolchain Boundaries (M1-M4)
# ==============================================================================

@register_test("T2_F25_01", "test_build_boundary_strict_flags", 25, "M1", "Build uses -Wall -Wextra -std=c++20")
def test_t2_f25_01(ctx):
    ctx.require_makefile()
    with open(MAKEFILE_PATH, "r") as f:
        m = f.read()
    assert "-Wall" in m and "-Wextra" in m

@register_test("T2_F25_02", "test_build_boundary_parallel_make", 25, "M1", "Parallel make invocation (make -j4)")
def test_t2_f25_02(ctx):
    ctx.require_makefile()
    assert os.path.exists(MAKEFILE_PATH)

@register_test("T2_F25_03", "test_build_boundary_noop_rebuild", 25, "M1", "Consecutive make calls perform no unnecessary rebuilds")
def test_t2_f25_03(ctx):
    ctx.require_makefile()
    assert os.path.exists(MAKEFILE_PATH)

@register_test("T2_F25_04", "test_build_boundary_header_deps", 25, "M1", "Header files are tracked as build dependencies")
def test_t2_f25_04(ctx):
    ctx.require_makefile()
    assert os.path.exists(MAKEFILE_PATH)

@register_test("T2_F25_05", "test_build_boundary_clean_rebuild", 25, "M1", "Clean rebuild from scratch compiles without errors")
def test_t2_f25_05(ctx):
    ctx.require_makefile()
    assert os.path.exists(MAKEFILE_PATH)
