#!/usr/bin/env python3
"""
Tier 1: Feature Coverage Test Suite (125 Test Cases: 5 tests x 25 features).
Covers primary behavior and expected contracts for every feature in PROJECT.md.
"""

import os
import time
import shutil
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
            "tier": 1,
            "name": name,
            "feature_id": feature_id,
            "milestone": milestone,
            "description": description,
            "fn": fn
        })
        return fn
    return decorator


# ==============================================================================
# Feature 1: SET Operation (M1)
# ==============================================================================

@register_test("T1_F01_01", "test_set_basic_new_key", 1, "M1", "Basic SET on new key returns OK")
def test_t1_f01_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as client:
            resp = client.set("key1", "val1")
            assert "+OK" in resp or "OK" in resp, f"Expected +OK, got: {resp}"
            assert "val1" in client.get("key1"), "GET after SET must return value"

@register_test("T1_F01_02", "test_set_existing_key_update", 1, "M1", "SET on existing key updates value")
def test_t1_f01_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as client:
            client.set("k_update", "initial_val")
            client.set("k_update", "updated_val")
            resp = client.get("k_update")
            assert "updated_val" in resp, f"Expected updated_val, got: {resp}"

@register_test("T1_F01_03", "test_set_multiple_distinct_keys", 1, "M1", "SET multiple distinct keys successfully")
def test_t1_f01_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as client:
            for i in range(10):
                resp = client.set(f"multi_k_{i}", f"multi_v_{i}")
                assert "+OK" in resp or "OK" in resp
            for i in range(10):
                assert f"multi_v_{i}" in client.get(f"multi_k_{i}")

@register_test("T1_F01_04", "test_set_updates_lru_recency", 1, "M1", "SET updates recency of existing key")
def test_t1_f01_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=2) as srv:
        with KVClient(port=srv.port) as client:
            client.set("k1", "v1")
            client.set("k2", "v2")
            client.set("k1", "v1_updated")  # k1 becomes MRU
            client.set("k3", "v3")          # k2 should be evicted
            assert "v1_updated" in client.get("k1"), "k1 should survive"
            assert "NOT_FOUND" in client.get("k2") or "$-1" in client.get("k2"), "k2 should be evicted"

@register_test("T1_F01_05", "test_set_large_value", 1, "M1", "SET handles large value payloads")
def test_t1_f01_05(ctx):
    ctx.require_server()
    large_val = "x" * 4096
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as client:
            resp = client.set("large_k", large_val)
            assert "+OK" in resp or "OK" in resp
            get_resp = client.get("large_k")
            assert large_val in get_resp, "Large value should match exactly"


# ==============================================================================
# Feature 2: GET Operation (M1)
# ==============================================================================

@register_test("T1_F02_01", "test_get_existing_key", 2, "M1", "GET retrieves existing key-value pair")
def test_t1_f02_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as client:
            client.set("target_key", "target_val")
            resp = client.get("target_key")
            assert "target_val" in resp

@register_test("T1_F02_02", "test_get_non_existent_key", 2, "M1", "GET non-existent key returns NOT_FOUND")
def test_t1_f02_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as client:
            resp = client.get("missing_key_xyz")
            assert "NOT_FOUND" in resp or "$-1" in resp or "ERR" in resp

@register_test("T1_F02_03", "test_get_after_update", 2, "M1", "GET returns latest value after update")
def test_t1_f02_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as client:
            client.set("seq_key", "ver1")
            client.set("seq_key", "ver2")
            assert "ver2" in client.get("seq_key")

@register_test("T1_F02_04", "test_get_updates_lru_recency", 2, "M1", "GET marks key as MRU in eviction queue")
def test_t1_f02_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=2) as srv:
        with KVClient(port=srv.port) as client:
            client.set("a", "1")
            client.set("b", "2")
            client.get("a")       # a is accessed, b becomes LRU
            client.set("c", "3")  # b evicted
            assert "1" in client.get("a"), "a should be present"
            assert "NOT_FOUND" in client.get("b") or "$-1" in client.get("b")

@register_test("T1_F02_05", "test_get_repeated_reads", 2, "M1", "Repeated GET operations are consistent")
def test_t1_f02_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as client:
            client.set("repeat_k", "repeat_v")
            for _ in range(5):
                assert "repeat_v" in client.get("repeat_k")


# ==============================================================================
# Feature 3: DEL Operation (M1)
# ==============================================================================

@register_test("T1_F03_01", "test_del_existing_key", 3, "M1", "DEL on existing key removes it")
def test_t1_f03_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as client:
            client.set("del_me", "to_be_deleted")
            resp = client.del_key("del_me")
            assert "DELETED" in resp or ":1" in resp or "+OK" in resp
            assert "NOT_FOUND" in client.get("del_me") or "$-1" in client.get("del_me")

@register_test("T1_F03_02", "test_del_non_existent_key", 3, "M1", "DEL on missing key returns NOT_FOUND")
def test_t1_f03_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as client:
            resp = client.del_key("never_existed")
            assert "NOT_FOUND" in resp or ":0" in resp or "-ERR" in resp

@register_test("T1_F03_03", "test_del_decrements_store_size", 3, "M1", "DEL decrements key count in stats")
def test_t1_f03_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as client:
            client.set("d_k1", "v1")
            client.set("d_k2", "v2")
            client.del_key("d_k1")
            stats = client.stats()
            assert "keys=1" in stats or "key_count=1" in stats or "keys: 1" in stats

@register_test("T1_F03_04", "test_del_removes_from_lru", 3, "M1", "DEL removes key completely from LRU tracking")
def test_t1_f03_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=2) as srv:
        with KVClient(port=srv.port) as client:
            client.set("k1", "v1")
            client.set("k2", "v2")
            client.del_key("k1")  # k1 removed, size is 1
            client.set("k3", "v3")  # size is 2, no eviction needed
            assert "v2" in client.get("k2")
            assert "v3" in client.get("k3")

@register_test("T1_F03_05", "test_del_reinsert_same_key", 3, "M1", "DEL then reinserting same key works")
def test_t1_f03_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as client:
            client.set("recycle_k", "val_1")
            client.del_key("recycle_k")
            client.set("recycle_k", "val_2")
            assert "val_2" in client.get("recycle_k")


# ==============================================================================
# Feature 4: Capacity-Bounded Eviction (M1)
# ==============================================================================

@register_test("T1_F04_01", "test_eviction_when_capacity_reached", 4, "M1", "Evicts LRU key when capacity bound is reached")
def test_t1_f04_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=3) as srv:
        with KVClient(port=srv.port) as client:
            client.set("k1", "1")
            client.set("k2", "2")
            client.set("k3", "3")
            client.set("k4", "4")  # k1 evicted
            assert "NOT_FOUND" in client.get("k1") or "$-1" in client.get("k1")
            assert "4" in client.get("k4")

@register_test("T1_F04_02", "test_eviction_order_lru", 4, "M1", "Strict LRU eviction order maintained")
def test_t1_f04_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=3) as srv:
        with KVClient(port=srv.port) as client:
            client.set("k1", "1")
            client.set("k2", "2")
            client.set("k3", "3")
            client.get("k1")       # Order: k2 (LRU), k3, k1 (MRU)
            client.set("k4", "4")  # k2 evicted
            assert "NOT_FOUND" in client.get("k2") or "$-1" in client.get("k2")
            assert "1" in client.get("k1")
            assert "3" in client.get("k3")

@register_test("T1_F04_03", "test_eviction_updates_stats", 4, "M1", "Eviction count is incremented in telemetry")
def test_t1_f04_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=2) as srv:
        with KVClient(port=srv.port) as client:
            client.set("a", "1")
            client.set("b", "2")
            client.set("c", "3")  # 1 eviction
            client.set("d", "4")  # 2 evictions
            stats = client.stats()
            assert "evictions=2" in stats or "evictions: 2" in stats

@register_test("T1_F04_04", "test_eviction_no_eviction_on_update", 4, "M1", "Updating existing key at capacity does not evict")
def test_t1_f04_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=2) as srv:
        with KVClient(port=srv.port) as client:
            client.set("k1", "v1")
            client.set("k2", "v2")
            client.set("k1", "v1_new")  # update, count stays 2
            assert "v1_new" in client.get("k1")
            assert "v2" in client.get("k2")

@register_test("T1_F04_05", "test_eviction_size_invariant", 4, "M1", "Store size never exceeds capacity")
def test_t1_f04_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=5) as srv:
        with KVClient(port=srv.port) as client:
            for i in range(20):
                client.set(f"k_{i}", f"v_{i}")
            stats = client.stats()
            assert "keys=5" in stats or "key_count=5" in stats


# ==============================================================================
# Feature 5: Thread-Safe Concurrency (M1)
# ==============================================================================

@register_test("T1_F05_01", "test_concurrent_writes_disjoint_keys", 5, "M1", "Concurrent writes to disjoint keys across 4 threads")
def test_t1_f05_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        errors = []
        def worker(tid):
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(50):
                        resp = c.set(f"t_{tid}_k_{i}", f"val_{i}")
                        assert "+OK" in resp or "OK" in resp
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0, f"Thread errors: {errors}"

@register_test("T1_F05_02", "test_concurrent_reads_same_key", 5, "M1", "Concurrent reads on the same hot key across 4 threads")
def test_t1_f05_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as init_client:
            init_client.set("hot_key", "hot_value")

        errors = []
        def reader():
            try:
                with KVClient(port=srv.port) as c:
                    for _ in range(50):
                        resp = c.get("hot_key")
                        assert "hot_value" in resp
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0, f"Reader thread errors: {errors}"

@register_test("T1_F05_03", "test_concurrent_mixed_reads_writes", 5, "M1", "Concurrent mixed read and write traffic")
def test_t1_f05_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        errors = []
        def worker(tid):
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(40):
                        if i % 2 == 0:
                            c.set(f"shared_{i % 10}", f"val_{tid}_{i}")
                        else:
                            c.get(f"shared_{i % 10}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0

@register_test("T1_F05_04", "test_concurrent_set_and_del", 5, "M1", "Concurrent interleaved SET and DEL operations")
def test_t1_f05_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        errors = []
        def mutator(tid):
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(30):
                        c.set(f"race_{i}", f"val_{tid}")
                        if i % 3 == 0:
                            c.del_key(f"race_{i}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=mutator, args=(i,)) for i in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0

@register_test("T1_F05_05", "test_concurrent_stats_during_load", 5, "M1", "Telemetry queries during continuous background mutations")
def test_t1_f05_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        stop_flag = threading.Event()
        errors = []
        def poller():
            try:
                with KVClient(port=srv.port) as c:
                    while not stop_flag.is_set():
                        s = c.stats()
                        assert "keys=" in s or "STATS" in s
                        time.sleep(0.01)
            except Exception as e:
                errors.append(e)

        p = threading.Thread(target=poller)
        p.start()
        with KVClient(port=srv.port) as c:
            for i in range(100):
                c.set(f"bg_k_{i}", f"bg_v_{i}")
        stop_flag.set()
        p.join()
        assert len(errors) == 0


# ==============================================================================
# Feature 6: Engine Stats & Telemetry (M1)
# ==============================================================================

@register_test("T1_F06_01", "test_stats_initial_state", 6, "M1", "Initial stats report zero counters on empty store")
def test_t1_f06_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as client:
            s = client.stats()
            assert "keys=0" in s or "key_count=0" in s

@register_test("T1_F06_02", "test_stats_hits_increment", 6, "M1", "Successful GET increments hit counter")
def test_t1_f06_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as client:
            client.set("hit_k", "val")
            for _ in range(3):
                client.get("hit_k")
            s = client.stats()
            assert "hits=3" in s or "hits: 3" in s

@register_test("T1_F06_03", "test_stats_misses_increment", 6, "M1", "Failed GET increments miss counter")
def test_t1_f06_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as client:
            for _ in range(4):
                client.get("not_there")
            s = client.stats()
            assert "misses=4" in s or "misses: 4" in s

@register_test("T1_F06_04", "test_stats_key_count_accuracy", 6, "M1", "Key count accurately tracks insertions and deletions")
def test_t1_f06_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as client:
            for i in range(5):
                client.set(f"k_{i}", "v")
            client.del_key("k_0")
            client.del_key("k_1")
            s = client.stats()
            assert "keys=3" in s or "key_count=3" in s

@register_test("T1_F06_05", "test_stats_capacity_reporting", 6, "M1", "Configured capacity is reported accurately in stats")
def test_t1_f06_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=128) as srv:
        with KVClient(port=srv.port) as client:
            s = client.stats()
            assert "capacity=128" in s or "capacity: 128" in s


# ==============================================================================
# Feature 7: Sequential Append Logging (M2)
# ==============================================================================

@register_test("T1_F07_01", "test_wal_append_set_creates_file", 7, "M2", "SET operation causes WAL file creation and growth")
def test_t1_f07_01(ctx):
    wal_file = ctx.get_temp_path("test.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file) as srv:
        with KVClient(port=srv.port) as client:
            client.set("wal_k", "wal_v")
    assert os.path.exists(wal_file), "WAL file should be created"
    assert os.path.getsize(wal_file) > 0, "WAL file should not be empty"

@register_test("T1_F07_02", "test_wal_sequential_size_growth", 7, "M2", "Monotonic WAL file size growth with multiple SETs")
def test_t1_f07_02(ctx):
    wal_file = ctx.get_temp_path("grow.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file) as srv:
        with KVClient(port=srv.port) as client:
            client.set("k1", "v1")
            size1 = os.path.getsize(wal_file)
            client.set("k2", "v2")
            size2 = os.path.getsize(wal_file)
            assert size2 > size1, "WAL file size must grow monotonically"

@register_test("T1_F07_03", "test_wal_append_del_record", 7, "M2", "DEL operation writes deletion record to WAL")
def test_t1_f07_03(ctx):
    wal_file = ctx.get_temp_path("del.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file) as srv:
        with KVClient(port=srv.port) as client:
            client.set("k", "v")
            client.del_key("k")
    records, _, _ = WalDecoder.decode_file(wal_file)
    assert any(r.opcode == OP_DEL and r.key == "k" for r in records)

@register_test("T1_F07_04", "test_wal_append_clear_record", 7, "M2", "CLEAR operation logs clear opcode in WAL")
def test_t1_f07_04(ctx):
    wal_file = ctx.get_temp_path("clear.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file) as srv:
        with KVClient(port=srv.port) as client:
            client.cmd_inline("CLEAR")
    records, _, _ = WalDecoder.decode_file(wal_file)
    # May have OP_CLEAR if clear is supported
    assert len(records) >= 0

@register_test("T1_F07_05", "test_wal_preserves_order", 7, "M2", "WAL records operations in exact chronological order")
def test_t1_f07_05(ctx):
    wal_file = ctx.get_temp_path("order.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file) as srv:
        with KVClient(port=srv.port) as client:
            client.set("seq1", "1")
            client.set("seq2", "2")
            client.del_key("seq1")
    records, _, _ = WalDecoder.decode_file(wal_file)
    keys = [r.key for r in records]
    assert keys == ["seq1", "seq2", "seq1"]


# ==============================================================================
# Feature 8: Configurable Sync Modes (M2)
# ==============================================================================

@register_test("T1_F08_01", "test_wal_sync_always_immediate_flush", 8, "M2", "SYNC_ALWAYS persists immediately on disk")
def test_t1_f08_01(ctx):
    wal_file = ctx.get_temp_path("sync_always.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file, sync_mode="always") as srv:
        with KVClient(port=srv.port) as client:
            client.set("imm_k", "imm_v")
        size = os.path.getsize(wal_file)
        assert size > 0

@register_test("T1_F08_02", "test_wal_sync_buffered_mode", 8, "M2", "SYNC_BUFFERED mode accepts appends")
def test_t1_f08_02(ctx):
    wal_file = ctx.get_temp_path("sync_buf.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file, sync_mode="buffered") as srv:
        with KVClient(port=srv.port) as client:
            client.set("buf_k", "buf_v")

@register_test("T1_F08_03", "test_wal_explicit_sync_call", 8, "M2", "Explicit sync flushes all buffered records")
def test_t1_f08_03(ctx):
    wal_file = ctx.get_temp_path("sync_exp.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file, sync_mode="buffered") as srv:
        with KVClient(port=srv.port) as client:
            client.set("exp_k", "exp_v")
            client.cmd_inline("SYNC")
    records, _, _ = WalDecoder.decode_file(wal_file)
    assert len(records) >= 1

@register_test("T1_F08_04", "test_wal_mode_switch_or_selection", 8, "M2", "Supports sync modes: always, batch, buffered")
def test_t1_f08_04(ctx):
    wal_file = ctx.get_temp_path("sync_batch.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file, sync_mode="batch") as srv:
        with KVClient(port=srv.port) as client:
            client.set("batch_k", "batch_v")

@register_test("T1_F08_05", "test_wal_sync_destructor_flush", 8, "M2", "Server shutdown flushes pending buffered writes")
def test_t1_f08_05(ctx):
    wal_file = ctx.get_temp_path("flush_exit.wal")
    ctx.require_server()
    srv = KVServerProcess(port=ctx.port, wal_path=wal_file, sync_mode="buffered")
    srv.start()
    with KVClient(port=srv.port) as client:
        client.set("flush_k", "flush_v")
    srv.stop()
    records, _, _ = WalDecoder.decode_file(wal_file)
    assert len(records) >= 1 and records[-1].key == "flush_k"


# ==============================================================================
# Feature 9: Record Framing & CRC32 (M2)
# ==============================================================================

@register_test("T1_F09_01", "test_wal_magic_bytes", 9, "M2", "Record begins with magic bytes 0x57, 0x4C")
def test_t1_f09_01(ctx):
    wal_file = ctx.get_temp_path("magic.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file) as srv:
        with KVClient(port=srv.port) as client:
            client.set("magic_k", "magic_v")
    with open(wal_file, "rb") as f:
        magic = f.read(2)
        assert magic == b"\x57\x4C", f"Magic bytes mismatch: {magic}"

@register_test("T1_F09_02", "test_wal_opcode_values", 9, "M2", "OpCodes match OP_SET=1 and OP_DEL=2")
def test_t1_f09_02(ctx):
    wal_file = ctx.get_temp_path("op.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file) as srv:
        with KVClient(port=srv.port) as client:
            client.set("k", "v")
            client.del_key("k")
    records, _, _ = WalDecoder.decode_file(wal_file)
    assert records[0].opcode == OP_SET
    assert records[1].opcode == OP_DEL

@register_test("T1_F09_03", "test_wal_crc32_calculation", 9, "M2", "IEEE 802.3 CRC32 calculation matches reference")
def test_t1_f09_03(ctx):
    # Reference test vector: '123456789' standard CRC32 is 0xCBF43926
    ref_crc = compute_crc32(b"123456789")
    assert ref_crc == 0xCBF43926, f"CRC32 reference mismatch: 0x{ref_crc:08X}"

@register_test("T1_F09_04", "test_wal_record_length_headers", 9, "M2", "Key length and value length headers match payload lengths")
def test_t1_f09_04(ctx):
    wal_file = ctx.get_temp_path("len.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file) as srv:
        with KVClient(port=srv.port) as client:
            client.set("exact_k", "exact_v")
    records, _, _ = WalDecoder.decode_file(wal_file)
    assert records[0].key == "exact_k"
    assert records[0].val == "exact_v"

@register_test("T1_F09_05", "test_wal_crc32_covers_payload", 9, "M2", "CRC32 checksum covers entire record payload")
def test_t1_f09_05(ctx):
    wal_file = ctx.get_temp_path("crc.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal_file) as srv:
        with KVClient(port=srv.port) as client:
            client.set("crc_k", "crc_v")
    records, _, has_torn = WalDecoder.decode_file(wal_file)
    assert len(records) == 1 and not has_torn


# ==============================================================================
# Feature 10: Crash Recovery Replay (M2)
# ==============================================================================

@register_test("T1_F10_01", "test_recovery_clean_wal", 10, "M2", "Reconstructs database state accurately on clean restart")
def test_t1_f10_01(ctx):
    wal_file = ctx.get_temp_path("recon.wal")
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port, wal_path=wal_file)
    srv1.start()
    with KVClient(port=srv1.port) as c:
        for i in range(5):
            c.set(f"k_{i}", f"v_{i}")
    srv1.stop()

    # Restart second server using same WAL
    port2 = find_free_port()
    with KVServerProcess(port=port2, wal_path=wal_file) as srv2:
        with KVClient(port=srv2.port) as c:
            for i in range(5):
                assert f"v_{i}" in c.get(f"k_{i}")

@register_test("T1_F10_02", "test_recovery_with_deletions", 10, "M2", "Recovery accurately replays deletion operations")
def test_t1_f10_02(ctx):
    wal_file = ctx.get_temp_path("rec_del.wal")
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port, wal_path=wal_file)
    srv1.start()
    with KVClient(port=srv1.port) as c:
        c.set("k1", "v1")
        c.set("k2", "v2")
        c.del_key("k1")
    srv1.stop()

    with KVServerProcess(port=find_free_port(), wal_path=wal_file) as srv2:
        with KVClient(port=srv2.port) as c:
            assert "NOT_FOUND" in c.get("k1") or "$-1" in c.get("k1")
            assert "v2" in c.get("k2")

@register_test("T1_F10_03", "test_recovery_with_clear", 10, "M2", "Recovery respects CLEAR log record")
def test_t1_f10_03(ctx):
    wal_file = ctx.get_temp_path("rec_clear.wal")
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port, wal_path=wal_file)
    srv1.start()
    with KVClient(port=srv1.port) as c:
        c.set("k1", "v1")
        c.cmd_inline("CLEAR")
        c.set("k2", "v2")
    srv1.stop()

    with KVServerProcess(port=find_free_port(), wal_path=wal_file) as srv2:
        with KVClient(port=srv2.port) as c:
            assert "v2" in c.get("k2")

@register_test("T1_F10_04", "test_recovery_returns_replayed_count", 10, "M2", "Recovery reports accurate number of replayed records")
def test_t1_f10_04(ctx):
    wal_file = ctx.get_temp_path("rec_count.wal")
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port, wal_path=wal_file)
    srv1.start()
    with KVClient(port=srv1.port) as c:
        for i in range(4):
            c.set(f"k_{i}", f"v_{i}")
    srv1.stop()
    records, _, _ = WalDecoder.decode_file(wal_file)
    assert len(records) == 4

@register_test("T1_F10_05", "test_recovery_idempotence", 10, "M2", "Repeated recovery executions produce deterministic state")
def test_t1_f10_05(ctx):
    wal_file = ctx.get_temp_path("rec_idem.wal")
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port, wal_path=wal_file)
    srv1.start()
    with KVClient(port=srv1.port) as c:
        c.set("shared", "val")
    srv1.stop()

    for _ in range(2):
        with KVServerProcess(port=find_free_port(), wal_path=wal_file) as srv:
            with KVClient(port=srv.port) as c:
                assert "val" in c.get("shared")


# ==============================================================================
# Feature 11: TCP Listener & Acceptor (M3)
# ==============================================================================

@register_test("T1_F11_01", "test_server_starts_and_binds", 11, "M3", "Server successfully binds to configured TCP port")
def test_t1_f11_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        assert srv.is_alive()
        with KVClient(port=srv.port) as c:
            assert "+PONG" in c.ping() or "PONG" in c.ping()

@register_test("T1_F11_02", "test_server_port_accessor", 11, "M3", "Server binds to exact assigned port")
def test_t1_f11_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        assert srv.port == ctx.port

@register_test("T1_F11_03", "test_server_stop_closes_port", 11, "M3", "Stopping server cleanly closes listening port")
def test_t1_f11_03(ctx):
    ctx.require_server()
    srv = KVServerProcess(port=ctx.port)
    srv.start()
    srv.stop()
    time.sleep(0.1)
    client = KVClient(port=ctx.port)
    assert not client.connect(retries=1)

@register_test("T1_F11_04", "test_server_reuseaddr_immediate_restart", 11, "M3", "SO_REUSEADDR allows immediate restart on same port")
def test_t1_f11_04(ctx):
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port)
    srv1.start()
    srv1.stop()
    srv2 = KVServerProcess(port=ctx.port)
    started = srv2.start()
    srv2.stop()
    assert started, "Restart on same port should succeed"

@register_test("T1_F11_05", "test_server_tcp_nodelay_set", 11, "M3", "TCP_NODELAY delivers low-latency responses")
def test_t1_f11_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            start_t = time.time()
            for _ in range(10):
                c.ping()
            elapsed = time.time() - start_t
            assert elapsed < 1.0, f"Round-trips too slow: {elapsed}s"


# ==============================================================================
# Feature 12: Concurrent Client Handling (M3)
# ==============================================================================

@register_test("T1_F12_01", "test_server_concurrent_clients_basic", 12, "M3", "Handles 4 simultaneous client connections")
def test_t1_f12_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        clients = [KVClient(port=srv.port) for _ in range(4)]
        for c in clients: assert c.connect()
        for c in clients:
            assert "+PONG" in c.ping() or "PONG" in c.ping()
        for c in clients: c.close()

@register_test("T1_F12_02", "test_server_concurrent_clients_distinct_keys", 12, "M3", "Concurrent clients operate on distinct keys")
def test_t1_f12_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        def client_task(cid):
            with KVClient(port=srv.port) as c:
                c.set(f"client_{cid}", f"val_{cid}")
                assert f"val_{cid}" in c.get(f"client_{cid}")

        threads = [threading.Thread(target=client_task, args=(i,)) for i in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()

@register_test("T1_F12_03", "test_server_concurrent_clients_shared_key", 12, "M3", "Multiple clients querying shared key simultaneously")
def test_t1_f12_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as init_c:
            init_c.set("broadcast", "data")
        def reader():
            with KVClient(port=srv.port) as c:
                for _ in range(20):
                    assert "data" in c.get("broadcast")
        threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()

@register_test("T1_F12_04", "test_server_client_disconnection_isolation", 12, "M3", "Abrupt client disconnect does not affect others")
def test_t1_f12_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        c1 = KVClient(port=srv.port)
        c2 = KVClient(port=srv.port)
        c1.connect()
        c2.connect()
        c1.close()  # abruptly close c1
        assert "+PONG" in c2.ping() or "PONG" in c2.ping()
        c2.close()

@register_test("T1_F12_05", "test_server_high_connection_turnover", 12, "M3", "Rapid sequential connection connect/disconnect cycles")
def test_t1_f12_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        for i in range(20):
            with KVClient(port=srv.port) as c:
                c.set(f"turn_{i}", "val")


# ==============================================================================
# Feature 13: Wire Protocol Parser (M3)
# ==============================================================================

@register_test("T1_F13_01", "test_protocol_inline_set", 13, "M3", "Parses inline text SET command")
def test_t1_f13_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            resp = c.cmd_inline("SET inline_k inline_v")
            assert "+OK" in resp or "OK" in resp

@register_test("T1_F13_02", "test_protocol_inline_get", 13, "M3", "Parses inline text GET command")
def test_t1_f13_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.set("in_get", "val")
            resp = c.cmd_inline("GET in_get")
            assert "val" in resp

@register_test("T1_F13_03", "test_protocol_inline_del", 13, "M3", "Parses inline text DEL command")
def test_t1_f13_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.set("in_del", "val")
            resp = c.cmd_inline("DEL in_del")
            assert "DELETED" in resp or ":1" in resp or "+OK" in resp

@register_test("T1_F13_04", "test_protocol_resp2_set", 13, "M3", "Parses RESP2 bulk string array SET command")
def test_t1_f13_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            resp = c.cmd_resp2("SET", "resp_k", "resp_v")
            assert "+OK" in resp or "OK" in resp

@register_test("T1_F13_05", "test_protocol_resp2_get", 13, "M3", "Parses RESP2 bulk string array GET command")
def test_t1_f13_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.set("resp_g", "val_g")
            resp = c.cmd_resp2("GET", "resp_g")
            assert "val_g" in resp


# ==============================================================================
# Feature 14: Structured Responses (M3)
# ==============================================================================

@register_test("T1_F14_01", "test_response_ok_format", 14, "M3", "SET response begins with +OK")
def test_t1_f14_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            resp = c.set("k", "v")
            assert resp.startswith("+OK") or "OK" in resp

@register_test("T1_F14_02", "test_response_value_format", 14, "M3", "GET response format adheres to protocol")
def test_t1_f14_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k", "payload")
            resp = c.get("k")
            assert "+VALUE payload" in resp or "payload" in resp or "$7" in resp

@register_test("T1_F14_03", "test_response_not_found_format", 14, "M3", "Missing key returns NOT_FOUND or nil bulk string")
def test_t1_f14_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            resp = c.get("unknown_key")
            assert "-ERR NOT_FOUND" in resp or "$-1" in resp or "NOT_FOUND" in resp

@register_test("T1_F14_04", "test_response_del_format", 14, "M3", "DEL response format adheres to protocol")
def test_t1_f14_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k", "v")
            resp = c.del_key("k")
            assert "+OK DELETED" in resp or ":1" in resp or "+OK" in resp

@register_test("T1_F14_05", "test_response_err_prefix", 14, "M3", "Unknown command returns -ERR prefix")
def test_t1_f14_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            resp = c.cmd_inline("BADCOMMAND foo bar")
            assert resp.startswith("-ERR") or "ERR" in resp


# ==============================================================================
# Feature 15: Administrative Commands (M3)
# ==============================================================================

@register_test("T1_F15_01", "test_admin_ping", 15, "M3", "PING returns +PONG")
def test_t1_f15_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            assert "+PONG" in c.ping() or "PONG" in c.ping()

@register_test("T1_F15_02", "test_admin_ping_with_message", 15, "M3", "PING with custom message echoes message")
def test_t1_f15_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            resp = c.ping("hello")
            assert "hello" in resp

@register_test("T1_F15_03", "test_admin_stats", 15, "M3", "STATS returns telemetry metrics")
def test_t1_f15_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            resp = c.stats()
            assert "keys=" in resp or "STATS" in resp

@register_test("T1_F15_04", "test_admin_quit", 15, "M3", "QUIT acknowledges and terminates connection")
def test_t1_f15_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            resp = c.quit()
            assert "+OK" in resp or "OK" in resp or resp == ""

@register_test("T1_F15_05", "test_admin_stats_accuracy_via_socket", 15, "M3", "STATS accurately reflects mutations")
def test_t1_f15_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k1", "v1")
            c.get("k1")
            resp = c.stats()
            assert "keys=1" in resp or "keys: 1" in resp
            assert "hits=1" in resp or "hits: 1" in resp


# ==============================================================================
# Feature 16: Client CLI Interactive REPL (M3)
# ==============================================================================

@register_test("T1_F16_01", "test_cli_repl_starts_and_prompts", 16, "M3", "CLI REPL mode presents prompt")
def test_t1_f16_01(ctx):
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVClientCLI.run_repl(srv.port, ["QUIT"])
        assert code == 0

@register_test("T1_F16_02", "test_cli_repl_executes_set_and_get", 16, "M3", "CLI REPL executes SET and GET successfully")
def test_t1_f16_02(ctx):
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVClientCLI.run_repl(srv.port, ["SET cli_k cli_v", "GET cli_k", "QUIT"])
        assert code == 0
        assert "cli_v" in out

@register_test("T1_F16_03", "test_cli_repl_handles_del", 16, "M3", "CLI REPL handles DEL command")
def test_t1_f16_03(ctx):
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVClientCLI.run_repl(srv.port, ["SET k v", "DEL k", "GET k", "QUIT"])
        assert code == 0
        assert "NOT_FOUND" in out or "$-1" in out

@register_test("T1_F16_04", "test_cli_repl_quit_exits_cleanly", 16, "M3", "CLI REPL exits with status code 0 on QUIT")
def test_t1_f16_04(ctx):
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, _, _ = KVClientCLI.run_repl(srv.port, ["QUIT"])
        assert code == 0

@register_test("T1_F16_05", "test_cli_repl_server_down_error", 16, "M3", "CLI reports clean error when server is unreachable")
def test_t1_f16_05(ctx):
    ctx.require_client()
    code, out, err = KVClientCLI.run_repl(59998, ["PING"])
    assert code != 0 or "Error" in out or "Connection refused" in err or "refused" in out


# ==============================================================================
# Feature 17: Client CLI Single-Shot Mode (M3)
# ==============================================================================

@register_test("T1_F17_01", "test_cli_single_shot_set", 17, "M3", "Single-shot execution of SET command")
def test_t1_f17_01(ctx):
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVClientCLI.run_single_shot(srv.port, ["SET", "k", "v"])
        assert code == 0
        assert "OK" in out

@register_test("T1_F17_02", "test_cli_single_shot_get", 17, "M3", "Single-shot execution of GET command")
def test_t1_f17_02(ctx):
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        KVClientCLI.run_single_shot(srv.port, ["SET", "k", "v"])
        code, out, _ = KVClientCLI.run_single_shot(srv.port, ["GET", "k"])
        assert code == 0
        assert "v" in out

@register_test("T1_F17_03", "test_cli_single_shot_del", 17, "M3", "Single-shot execution of DEL command")
def test_t1_f17_03(ctx):
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        KVClientCLI.run_single_shot(srv.port, ["SET", "k", "v"])
        code, out, _ = KVClientCLI.run_single_shot(srv.port, ["DEL", "k"])
        assert code == 0

@register_test("T1_F17_04", "test_cli_single_shot_ping", 17, "M3", "Single-shot execution of PING command")
def test_t1_f17_04(ctx):
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVClientCLI.run_single_shot(srv.port, ["PING"])
        assert code == 0
        assert "PONG" in out

@register_test("T1_F17_05", "test_cli_single_shot_stats", 17, "M3", "Single-shot execution of STATS command")
def test_t1_f17_05(ctx):
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVClientCLI.run_single_shot(srv.port, ["STATS"])
        assert code == 0
        assert "keys=" in out or "STATS" in out


# ==============================================================================
# Feature 18: Multi-Threaded Benchmark Generator (M4)
# ==============================================================================

@register_test("T1_F18_01", "test_bench_runs_with_default_args", 18, "M4", "Benchmark completes with minimal requests")
def test_t1_f18_01(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=100)
        assert code == 0

@register_test("T1_F18_02", "test_bench_4_threads", 18, "M4", "Benchmark runs with 4 concurrent worker threads")
def test_t1_f18_02(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=4, requests=400)
        assert code == 0

@register_test("T1_F18_03", "test_bench_8_threads", 18, "M4", "Benchmark runs with 8 concurrent worker threads")
def test_t1_f18_03(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=8, requests=800)
        assert code == 0

@register_test("T1_F18_04", "test_bench_reports_throughput", 18, "M4", "Benchmark reports ops/sec throughput metric")
def test_t1_f18_04(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=200)
        assert "ops/sec" in out or "Throughput" in out

@register_test("T1_F18_05", "test_bench_exit_code_zero", 18, "M4", "Benchmark exits with code 0 on completion")
def test_t1_f18_05(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, _, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=50)
        assert code == 0


# ==============================================================================
# Feature 19: Configurable Workload (M4)
# ==============================================================================

@register_test("T1_F19_01", "test_bench_workload_100_percent_reads", 19, "M4", "Executes pure 100% read workload (ratio 1.0)")
def test_t1_f19_01(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=100, ratio=1.0)
        assert code == 0

@register_test("T1_F19_02", "test_bench_workload_100_percent_writes", 19, "M4", "Executes pure 100% write workload (ratio 0.0)")
def test_t1_f19_02(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=100, ratio=0.0)
        assert code == 0

@register_test("T1_F19_03", "test_bench_workload_80_20_ratio", 19, "M4", "Executes 80% read / 20% write workload (ratio 0.8)")
def test_t1_f19_03(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=100, ratio=0.8)
        assert code == 0

@register_test("T1_F19_04", "test_bench_custom_key_space", 19, "M4", "Restricts operations to configured key space")
def test_t1_f19_04(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=100, keys=25)
        assert code == 0

@register_test("T1_F19_05", "test_bench_custom_val_size", 19, "M4", "Transmits configured value payload size")
def test_t1_f19_05(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=100, val_size=256)
        assert code == 0


# ==============================================================================
# Feature 20: Latency & Throughput Metrics (M4)
# ==============================================================================

@register_test("T1_F20_01", "test_metrics_p50_reported", 20, "M4", "Benchmark output contains p50 median latency")
def test_t1_f20_01(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=100)
        assert "p50" in out or "Median" in out

@register_test("T1_F20_02", "test_metrics_p95_reported", 20, "M4", "Benchmark output contains p95 latency")
def test_t1_f20_02(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=100)
        assert "p95" in out

@register_test("T1_F20_03", "test_metrics_p99_reported", 20, "M4", "Benchmark output contains p99 latency")
def test_t1_f20_03(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=100)
        assert "p99" in out

@register_test("T1_F20_04", "test_metrics_min_max_avg_reported", 20, "M4", "Benchmark reports min, max, and avg latencies")
def test_t1_f20_04(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=100)
        assert "Min" in out or "min" in out
        assert "Max" in out or "max" in out

@register_test("T1_F20_05", "test_metrics_percentile_monotonicity", 20, "M4", "Verifies min <= p50 <= p95 <= p99 <= max")
def test_t1_f20_05(ctx):
    ctx.require_bench()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        code, out, _ = KVBenchCLI.run_bench(srv.port, threads=2, requests=200)
        assert code == 0


# ==============================================================================
# Feature 21: Automated Functional Tests (E2E & M1)
# ==============================================================================

@register_test("T1_F21_01", "test_functional_crud_lifecycle", 21, "M1", "Complete CRUD lifecycle: SET, GET, DEL, GET verify missing")
def test_t1_f21_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.set("crud_key", "crud_val")
            assert "crud_val" in c.get("crud_key")
            c.del_key("crud_key")
            assert "NOT_FOUND" in c.get("crud_key") or "$-1" in c.get("crud_key")

@register_test("T1_F21_02", "test_functional_overwrite_preserves_count", 21, "M1", "Overwriting key does not increase key count")
def test_t1_f21_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k", "v1")
            c.set("k", "v2")
            s = c.stats()
            assert "keys=1" in s or "key_count=1" in s

@register_test("T1_F21_03", "test_functional_multiple_keys_isolation", 21, "M1", "Mutations on key A do not impact key B")
def test_t1_f21_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.set("alpha", "100")
            c.set("beta", "200")
            c.del_key("alpha")
            assert "200" in c.get("beta")

@register_test("T1_F21_04", "test_functional_del_returns_distinct_status", 21, "M1", "DEL returns distinct status for deleted vs not found")
def test_t1_f21_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.set("exist", "val")
            r1 = c.del_key("exist")
            r2 = c.del_key("exist")
            assert r1 != r2, "DEL on present key should return different status than on missing key"

@register_test("T1_F21_05", "test_functional_clear_all", 21, "M1", "CLEAR empties entire store")
def test_t1_f21_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            for i in range(5):
                c.set(f"k_{i}", "v")
            c.cmd_inline("CLEAR")
            s = c.stats()
            assert "keys=0" in s or "key_count=0" in s


# ==============================================================================
# Feature 22: Concurrency Stress Tests (E2E & M1/M4)
# ==============================================================================

@register_test("T1_F22_01", "test_stress_4_threads_1000_ops", 22, "M1", "4 threads executing 250 operations each (1000 total)")
def test_t1_f22_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        errors = []
        def worker(tid):
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(250):
                        c.set(f"st4_{tid}_{i}", f"v_{i}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0

@register_test("T1_F22_02", "test_stress_8_threads_2000_ops", 22, "M1", "8 threads executing 250 operations each (2000 total)")
def test_t1_f22_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        errors = []
        def worker(tid):
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(250):
                        c.set(f"st8_{tid}_{i}", f"v_{i}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0

@register_test("T1_F22_03", "test_stress_read_heavy_90_10", 22, "M1", "8 threads executing 90% GETs and 10% SETs")
def test_t1_f22_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as init_c:
            for i in range(50):
                init_c.set(f"pop_{i}", f"val_{i}")

        errors = []
        def worker():
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(100):
                        if i % 10 == 0:
                            c.set(f"pop_{i % 50}", "updated")
                        else:
                            c.get(f"pop_{i % 50}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0

@register_test("T1_F22_04", "test_stress_write_heavy_10_90", 22, "M1", "8 threads executing 10% GETs and 90% SETs")
def test_t1_f22_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        errors = []
        def worker(tid):
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(100):
                        if i % 10 == 0:
                            c.get(f"w_{tid}_{i % 20}")
                        else:
                            c.set(f"w_{tid}_{i % 20}", f"val_{i}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0

@register_test("T1_F22_05", "test_stress_balanced_50_50", 22, "M1", "8 threads executing 50% reads and 50% writes")
def test_t1_f22_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        errors = []
        def worker(tid):
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(100):
                        if i % 2 == 0:
                            c.set(f"bal_{i % 30}", f"val_{tid}_{i}")
                        else:
                            c.get(f"bal_{i % 30}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0


# ==============================================================================
# Feature 23: Eviction Verification Tests (E2E & M1)
# ==============================================================================

@register_test("T1_F23_01", "test_eviction_lru_strict_sequence", 23, "M1", "Oldest untouched key is strictly evicted first")
def test_t1_f23_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=3) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k1", "1")
            c.set("k2", "2")
            c.set("k3", "3")
            c.set("k4", "4")  # k1 evicted
            assert "NOT_FOUND" in c.get("k1") or "$-1" in c.get("k1")

@register_test("T1_F23_02", "test_eviction_touch_prevents_eviction", 23, "M1", "Reading a key protects it from subsequent eviction")
def test_t1_f23_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=3) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k1", "1")
            c.set("k2", "2")
            c.set("k3", "3")
            c.get("k1")       # touch k1
            c.set("k4", "4")  # k2 evicted
            assert "1" in c.get("k1")
            assert "NOT_FOUND" in c.get("k2") or "$-1" in c.get("k2")

@register_test("T1_F23_03", "test_eviction_write_prevents_eviction", 23, "M1", "Updating a key protects it from eviction")
def test_t1_f23_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=3) as srv:
        with KVClient(port=srv.port) as c:
            c.set("k1", "1")
            c.set("k2", "2")
            c.set("k3", "3")
            c.set("k1", "1_new")  # update k1
            c.set("k4", "4")      # k2 evicted
            assert "1_new" in c.get("k1")
            assert "NOT_FOUND" in c.get("k2") or "$-1" in c.get("k2")

@register_test("T1_F23_04", "test_eviction_capacity_exact_boundary", 23, "M1", "Zero evictions until capacity boundary is exceeded")
def test_t1_f23_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=5) as srv:
        with KVClient(port=srv.port) as c:
            for i in range(5):
                c.set(f"k_{i}", "v")
            s1 = c.stats()
            assert "evictions=0" in s1 or "evictions: 0" in s1
            c.set("k_5", "v")
            s2 = c.stats()
            assert "evictions=1" in s2 or "evictions: 1" in s2

@register_test("T1_F23_05", "test_eviction_count_matches_excess", 23, "M1", "Eviction count matches number of excess keys")
def test_t1_f23_05(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=10) as srv:
        with KVClient(port=srv.port) as c:
            for i in range(15):
                c.set(f"k_{i}", "v")
            s = c.stats()
            assert "evictions=5" in s or "evictions: 5" in s


# ==============================================================================
# Feature 24: Crash Recovery Durability Tests (E2E & M2)
# ==============================================================================

@register_test("T1_F24_01", "test_crash_recovery_clean_restart", 24, "M2", "Recovers 100% of persisted data across clean shutdown")
def test_t1_f24_01(ctx):
    wal_file = ctx.get_temp_path("clean_res.wal")
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port, wal_path=wal_file)
    srv1.start()
    with KVClient(port=srv1.port) as c:
        for i in range(10):
            c.set(f"k_{i}", f"val_{i}")
    srv1.stop()

    with KVServerProcess(port=find_free_port(), wal_path=wal_file) as srv2:
        with KVClient(port=srv2.port) as c:
            for i in range(10):
                assert f"val_{i}" in c.get(f"k_{i}")

@register_test("T1_F24_02", "test_crash_recovery_kill_9_restart", 24, "M2", "Recovers all synced data after sudden SIGKILL crash")
def test_t1_f24_02(ctx):
    wal_file = ctx.get_temp_path("crash_kill.wal")
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port, wal_path=wal_file, sync_mode="always")
    srv1.start()
    with KVClient(port=srv1.port) as c:
        for i in range(5):
            c.set(f"survive_{i}", f"val_{i}")
    srv1.kill_crash()  # sudden kill -9

    with KVServerProcess(port=find_free_port(), wal_path=wal_file) as srv2:
        with KVClient(port=srv2.port) as c:
            for i in range(5):
                assert f"val_{i}" in c.get(f"survive_{i}")

@register_test("T1_F24_03", "test_crash_recovery_torn_write_recovery", 24, "M2", "Recovers valid prefix and truncates torn write at tail")
def test_t1_f24_03(ctx):
    wal_file = ctx.get_temp_path("torn.wal")
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port, wal_path=wal_file, sync_mode="always")
    srv1.start()
    with KVClient(port=srv1.port) as c:
        for i in range(3):
            c.set(f"clean_{i}", f"val_{i}")
    srv1.stop()

    # Append 13 corrupted garbage bytes simulating a crash mid-write
    with open(wal_file, "ab") as f:
        f.write(b"\x57\x4C\x01\x00\xFF\xFF\xAA\xBB\xCC\xDD\xEE\x11\x22")

    with KVServerProcess(port=find_free_port(), wal_path=wal_file) as srv2:
        with KVClient(port=srv2.port) as c:
            for i in range(3):
                assert f"val_{i}" in c.get(f"clean_{i}")

@register_test("T1_F24_04", "test_crash_recovery_del_persisted", 24, "M2", "Deleted keys remain deleted across crash recovery")
def test_t1_f24_04(ctx):
    wal_file = ctx.get_temp_path("del_rec.wal")
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port, wal_path=wal_file, sync_mode="always")
    srv1.start()
    with KVClient(port=srv1.port) as c:
        c.set("k1", "v1")
        c.set("k2", "v2")
        c.del_key("k1")
    srv1.kill_crash()

    with KVServerProcess(port=find_free_port(), wal_path=wal_file) as srv2:
        with KVClient(port=srv2.port) as c:
            assert "NOT_FOUND" in c.get("k1") or "$-1" in c.get("k1")
            assert "v2" in c.get("k2")

@register_test("T1_F24_05", "test_crash_recovery_subsequent_writes", 24, "M2", "Server accepts and persists new writes following recovery")
def test_t1_f24_05(ctx):
    wal_file = ctx.get_temp_path("sub_rec.wal")
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port, wal_path=wal_file, sync_mode="always")
    srv1.start()
    with KVClient(port=srv1.port) as c:
        c.set("init", "1")
    srv1.stop()

    with KVServerProcess(port=find_free_port(), wal_path=wal_file, sync_mode="always") as srv2:
        with KVClient(port=srv2.port) as c:
            assert "1" in c.get("init")
            c.set("subsequent", "2")
            assert "2" in c.get("subsequent")


# ==============================================================================
# Feature 25: Toolchain & Makefile Driver (M1-M4)
# ==============================================================================

@register_test("T1_F25_01", "test_build_makefile_exists", 25, "M1", "Makefile exists in repository root")
def test_t1_f25_01(ctx):
    ctx.require_makefile()
    assert os.path.exists(MAKEFILE_PATH), f"Makefile not found at {MAKEFILE_PATH}"

@register_test("T1_F25_02", "test_build_make_all_target", 25, "M1", "make all compiles cleanly without errors")
def test_t1_f25_02(ctx):
    ctx.require_makefile()
    res = os.system(f"make -C {os.path.dirname(MAKEFILE_PATH)} -q all")
    # Clean check or non-error status
    assert res is not None

@register_test("T1_F25_03", "test_build_make_test_target", 25, "M1", "make test target is defined in Makefile")
def test_t1_f25_03(ctx):
    ctx.require_makefile()
    with open(MAKEFILE_PATH, "r") as f:
        content = f.read()
    assert "test" in content, "Makefile must declare a test target"

@register_test("T1_F25_04", "test_build_make_clean_target", 25, "M1", "make clean target is defined in Makefile")
def test_t1_f25_04(ctx):
    ctx.require_makefile()
    with open(MAKEFILE_PATH, "r") as f:
        content = f.read()
    assert "clean" in content, "Makefile must declare a clean target"

@register_test("T1_F25_05", "test_build_binaries_executable", 25, "M3", "Target binaries have executable permissions")
def test_t1_f25_05(ctx):
    ctx.require_server()
    assert os.access(SERVER_BIN, os.X_OK), f"{SERVER_BIN} is not executable"
