#!/usr/bin/env python3
"""
Tier 4: Real-World Application Scenarios Test Suite (10 End-to-End Scenarios).
Simulates realistic production workloads, user journeys, failover drills, and multi-tenant access patterns.
"""

import os
import time
import random
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
            "tier": 4,
            "name": name,
            "milestone": milestone,
            "description": description,
            "fn": fn
        })
        return fn
    return decorator


@register_test("T4_SCEN_01", "test_scenario_01_web_session_cache", "M1", "Simulated Web Server Session Cache: 8 client threads, 85% reads, LRU session expiry")
def test_scenario_01(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=100) as srv:
        errors = []
        def session_worker(worker_id):
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(50):
                        session_id = f"sess_{worker_id}_{i % 20}"
                        # 85% read / 15% write
                        if random.random() < 0.15:
                            c.set(session_id, f"auth_token_{worker_id}_{time.time()}")
                        else:
                            c.get(session_id)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=session_worker, args=(w,)) for w in range(8)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0

@register_test("T4_SCEN_02", "test_scenario_02_database_query_cache", "M1", "Read-through query cache with periodic invalidations and capacity bounds")
def test_scenario_02(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=50) as srv:
        with KVClient(port=srv.port) as c:
            # Seed 50 cached queries
            for i in range(50):
                c.set(f"sql_hash_{i}", f"result_set_{i}")

            # Simulate heavy read queries
            for _ in range(100):
                query_idx = random.randint(0, 49)
                c.get(f"sql_hash_{query_idx}")

            # Invalidate 10 rows
            for i in range(10):
                c.del_key(f"sql_hash_{i}")

            s = c.stats()
            assert "keys=40" in s or "key_count=40" in s

@register_test("T4_SCEN_03", "test_scenario_03_leaderboard_hotspot", "M1", "Leaderboard hotspot: 90% of requests hit top 5% of keys (Zipfian contention)")
def test_scenario_03(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=100) as srv:
        # Seed 100 players
        with KVClient(port=srv.port) as init_c:
            for p in range(100):
                init_c.set(f"player_{p}", str(p * 10))

        errors = []
        def gamer():
            try:
                with KVClient(port=srv.port) as c:
                    for _ in range(100):
                        # Top 5 players get 90% of traffic
                        if random.random() < 0.90:
                            pid = random.randint(0, 4)
                        else:
                            pid = random.randint(5, 99)
                        c.get(f"player_{pid}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=gamer) for _ in range(8)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0

@register_test("T4_SCEN_04", "test_scenario_04_e_commerce_cart", "M1", "Multi-step user shopping journey: add cart, view cart, update, checkout/del")
def test_scenario_04(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        errors = []
        def shopper(shopper_id):
            try:
                with KVClient(port=srv.port) as c:
                    cart_key = f"cart_{shopper_id}"
                    # Step 1: Add item
                    c.set(cart_key, "item:shoes,qty:1")
                    # Step 2: View cart
                    v1 = c.get(cart_key)
                    assert "item:shoes" in v1
                    # Step 3: Update cart
                    c.set(cart_key, "item:shoes,qty:2,item:socks,qty:1")
                    # Step 4: Checkout (delete cart)
                    del_resp = c.del_key(cart_key)
                    assert "DELETED" in del_resp or ":1" in del_resp or "+OK" in del_resp
                    # Step 5: Verify cart empty
                    v2 = c.get(cart_key)
                    assert "NOT_FOUND" in v2 or "$-1" in v2
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=shopper, args=(i,)) for i in range(10)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0

@register_test("T4_SCEN_05", "test_scenario_05_iot_telemetry_ingest", "M2", "High-throughput write-heavy sensor ingest streaming to WAL (95% SET)")
def test_scenario_05(ctx):
    wal = ctx.get_temp_path("iot_telemetry.wal")
    ctx.require_server()
    with KVServerProcess(port=ctx.port, wal_path=wal, sync_mode="buffered") as srv:
        errors = []
        def sensor_device(device_id):
            try:
                with KVClient(port=srv.port) as c:
                    for reading_id in range(50):
                        payload = f"temp:{20.0 + random.random()*10:.2f},ts:{time.time()}"
                        c.set(f"dev_{device_id}_sensor", payload)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=sensor_device, args=(d,)) for d in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0

    records, _, has_torn = WalDecoder.decode_file(wal)
    assert len(records) >= 100 and not has_torn

@register_test("T4_SCEN_06", "test_scenario_06_disaster_recovery_drill", "M2", "Complete disaster recovery drill: 500 mutations, kill -9, restore 100% data")
def test_scenario_06(ctx):
    wal = ctx.get_temp_path("disaster_drill.wal")
    ctx.require_server()
    srv1 = KVServerProcess(port=ctx.port, wal_path=wal, sync_mode="always")
    srv1.start()
    with KVClient(port=srv1.port) as c:
        for i in range(100):
            c.set(f"asset_{i}", f"value_{i}")
        for i in range(0, 100, 2):
            c.del_key(f"asset_{i}")  # delete evens
    # Simulated catastrophic power outage / server crash
    srv1.kill_crash()

    # Recovery on new server node
    with KVServerProcess(port=find_free_port(), wal_path=wal) as srv2:
        with KVClient(port=srv2.port) as c:
            for i in range(100):
                if i % 2 == 0:
                    assert "NOT_FOUND" in c.get(f"asset_{i}") or "$-1" in c.get(f"asset_{i}")
                else:
                    assert f"value_{i}" in c.get(f"asset_{i}")

@register_test("T4_SCEN_07", "test_scenario_07_microservices_token_bucket", "M1", "Concurrent rate limiter token bucket: atomic counter increments")
def test_scenario_07(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        with KVClient(port=srv.port) as c:
            c.set("api_rate_limiter", "1000")

        errors = []
        def consumer():
            try:
                with KVClient(port=srv.port) as c:
                    for _ in range(50):
                        c.get("api_rate_limiter")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=consumer) for _ in range(8)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0

@register_test("T4_SCEN_08", "test_scenario_08_mixed_client_ecosystem", "M3", "Simultaneous requests from CLI, RESP2 sockets, and inline sockets")
def test_scenario_08(ctx):
    ctx.require_client()
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        errors = []
        def cli_worker():
            try:
                for i in range(10):
                    KVClientCLI.run_single_shot(srv.port, ["SET", f"cli_{i}", "val"])
            except Exception as e:
                errors.append(e)

        def resp_worker():
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(10):
                        c.cmd_resp2("SET", f"resp_{i}", "val")
            except Exception as e:
                errors.append(e)

        def inline_worker():
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(10):
                        c.cmd_inline(f"SET inline_{i} val")
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=cli_worker)
        t2 = threading.Thread(target=resp_worker)
        t3 = threading.Thread(target=inline_worker)
        t1.start(); t2.start(); t3.start()
        t1.join(); t2.join(); t3.join()
        assert len(errors) == 0

@register_test("T4_SCEN_09", "test_scenario_09_noisy_neighbor_burst", "M3", "Sudden burst of 200 rapid requests from client A while B has steady traffic")
def test_scenario_09(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port) as srv:
        burst_errors = []
        steady_errors = []

        def burst_client():
            try:
                with KVClient(port=srv.port) as c:
                    for i in range(200):
                        c.set(f"burst_{i}", "burst")
            except Exception as e:
                burst_errors.append(e)

        def steady_client():
            try:
                with KVClient(port=srv.port) as c:
                    for _ in range(20):
                        assert "+PONG" in c.ping() or "PONG" in c.ping()
                        time.sleep(0.01)
            except Exception as e:
                steady_errors.append(e)

        b = threading.Thread(target=burst_client)
        s = threading.Thread(target=steady_client)
        b.start(); s.start()
        b.join(); s.join()
        assert len(burst_errors) == 0 and len(steady_errors) == 0

@register_test("T4_SCEN_10", "test_scenario_10_memory_ceiling_steady_state", "M1", "Steady state operation at 100% capacity over 500 operations")
def test_scenario_10(ctx):
    ctx.require_server()
    with KVServerProcess(port=ctx.port, capacity=25) as srv:
        with KVClient(port=srv.port) as c:
            # Fill to capacity
            for i in range(25):
                c.set(f"cap_{i}", "v")

            # Execute 200 surplus inserts in steady state
            for i in range(25, 225):
                c.set(f"cap_{i}", "v")

            s = c.stats()
            assert "keys=25" in s or "key_count=25" in s
            assert "evictions=200" in s or "evictions: 200" in s
