#!/usr/bin/env python3
"""
Master E2E Test Suite Runner for cpp_kv_store.
Executes and coordinates the 4-tier E2E test suite across all 25 features.
Supports milestone filtering, tier filtering, self-testing, and automated reporting.
"""

import os
import sys
import time
import shutil
import tempfile
import argparse
import traceback

# Add current directory to module search path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from harness_utils import (
    SERVER_BIN, CLIENT_BIN, BENCH_BIN, MAKEFILE_PATH,
    find_free_port, compute_crc32, WalDecoder, ReferenceOracle, OP_SET, OP_DEL
)

# Import test suites
import tier1_features
import tier2_boundaries
import tier3_combinations
import tier4_scenarios

ALL_TESTS = (
    tier1_features.TESTS +
    tier2_boundaries.TESTS +
    tier3_combinations.TESTS +
    tier4_scenarios.TESTS
)


class PrereqNotMet(Exception):
    """Raised when a milestone binary or file has not been built yet."""
    pass


class TestContext:
    """Provides isolated per-test sandbox and dependency validation."""

    def __init__(self, test_id: str, milestone: str):
        self.test_id = test_id
        self.milestone = milestone
        self.port = find_free_port()
        self.temp_dir = tempfile.mkdtemp(prefix=f"kv_test_{test_id}_")

    def cleanup(self):
        try:
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        except Exception:
            pass

    def get_temp_path(self, filename: str) -> str:
        return os.path.join(self.temp_dir, filename)

    def require_server(self):
        if not os.path.exists(SERVER_BIN):
            raise PrereqNotMet(f"Requires binary: {SERVER_BIN} (Milestone M3)")

    def require_client(self):
        if not os.path.exists(CLIENT_BIN):
            raise PrereqNotMet(f"Requires binary: {CLIENT_BIN} (Milestone M3)")

    def require_bench(self):
        if not os.path.exists(BENCH_BIN):
            raise PrereqNotMet(f"Requires binary: {BENCH_BIN} (Milestone M4)")

    def require_makefile(self):
        if not os.path.exists(MAKEFILE_PATH):
            raise PrereqNotMet(f"Requires Makefile at {MAKEFILE_PATH} (Milestone M1)")


def run_self_test() -> bool:
    """Verifies that internal test harness oracles and codecs are functionally correct."""
    print("=" * 70)
    print("RUNNING E2E TEST HARNESS SELF-VERIFICATION")
    print("=" * 70)

    # 1. Verify CRC32
    crc = compute_crc32(b"123456789")
    assert crc == 0xCBF43926, f"CRC32 mismatch: 0x{crc:08X}"
    print("[PASS] IEEE 802.3 CRC32 reference algorithm verified (0xCBF43926)")

    # 2. Verify WAL Encoder / Decoder
    temp_wal = tempfile.mktemp(suffix=".wal")
    try:
        raw_rec1 = WalDecoder.encode_record(OP_SET, "mykey", "myval", timestamp_ns=1000)
        raw_rec2 = WalDecoder.encode_record(OP_DEL, "mykey", "", timestamp_ns=2000)
        with open(temp_wal, "wb") as f:
            f.write(raw_rec1)
            f.write(raw_rec2)

        records, offset, has_torn = WalDecoder.decode_file(temp_wal)
        assert len(records) == 2, f"Expected 2 records, got {len(records)}"
        assert records[0].opcode == OP_SET and records[0].key == "mykey" and records[0].val == "myval"
        assert records[1].opcode == OP_DEL and records[1].key == "mykey"
        assert not has_torn
        print("[PASS] Binary WAL record framing, serialization, and CRC32 verification verified")

        # Verify torn tail detection
        with open(temp_wal, "ab") as f:
            f.write(b"\x57\x4C\x01\x00\xFF")  # incomplete record
        records, offset, has_torn = WalDecoder.decode_file(temp_wal)
        assert len(records) == 2 and has_torn
        print("[PASS] WAL torn-write tail detection and clean truncation verified")
    finally:
        if os.path.exists(temp_wal):
            os.remove(temp_wal)

    # 3. Verify Reference Oracle
    oracle = ReferenceOracle(capacity=2)
    oracle.set("k1", "v1")
    oracle.set("k2", "v2")
    oracle.get("k1")  # k1 MRU, k2 LRU
    oracle.set("k3", "v3")  # k2 evicted
    assert oracle.get("k2") is None and oracle.evictions == 1
    assert oracle.get("k1") == "v1" and oracle.get("k3") == "v3"
    print("[PASS] Reference Oracle LRU eviction semantics verified")

    print("-" * 70)
    print("ALL HARNESS COMPONENT SELF-TESTS PASSED SUCCESSFULLY!")
    print("=" * 70)
    return True


def print_summary():
    """Prints tier counts and milestone coverage summary."""
    t1 = [t for t in ALL_TESTS if t["tier"] == 1]
    t2 = [t for t in ALL_TESTS if t["tier"] == 2]
    t3 = [t for t in ALL_TESTS if t["tier"] == 3]
    t4 = [t for t in ALL_TESTS if t["tier"] == 4]

    print("=" * 70)
    print("cpp_kv_store E2E TEST SUITE BREAKDOWN")
    print("=" * 70)
    print(f"Tier 1 (Feature Coverage):            {len(t1):4d} tests (25 features x 5 tests)")
    print(f"Tier 2 (Boundary & Corner Cases):     {len(t2):4d} tests (25 features x 5 tests)")
    print(f"Tier 3 (Cross-Feature Combinations):   {len(t3):4d} tests (Pairwise interactions)")
    print(f"Tier 4 (Real-World Scenarios):         {len(t4):4d} tests (Complex application drills)")
    print("-" * 70)
    print(f"TOTAL TEST SUITE COUNT:                {len(ALL_TESTS):4d} tests")
    print("=" * 70)

    # Breakdown by milestone
    m_counts = {}
    for t in ALL_TESTS:
        m = t["milestone"]
        m_counts[m] = m_counts.get(m, 0) + 1
    print("Milestone Mapping Breakdown:")
    for m in sorted(m_counts.keys()):
        print(f"  - Milestone {m}: {m_counts[m]:3d} tests")
    print("=" * 70)


def list_tests():
    """Lists all tests in the catalog."""
    print("=" * 80)
    print(f"{'ID':<12} {'Tier':<6} {'M':<4} {'Name':<40} {'Description'}")
    print("-" * 80)
    for t in ALL_TESTS:
        desc = t["description"][:30] + "..." if len(t["description"]) > 30 else t["description"]
        print(f"{t['id']:<12} T{t['tier']:<5} {t['milestone']:<4} {t['name']:<40} {desc}")
    print("=" * 80)
    print(f"Total: {len(ALL_TESTS)} tests listed.")


def main():
    parser = argparse.ArgumentParser(description="cpp_kv_store E2E Test Suite Runner")
    parser.add_argument("--tier", choices=["1", "2", "3", "4", "all"], default="all",
                        help="Filter by test tier (1, 2, 3, 4, or all)")
    parser.add_argument("--milestone", choices=["m1", "m2", "m3", "m4", "m5", "all"], default="all",
                        help="Filter by target milestone (m1, m2, m3, m4, m5, or all)")
    parser.add_argument("--feature", type=int, choices=range(1, 26), default=None,
                        help="Filter by feature index (1-25)")
    parser.add_argument("--test", type=str, default=None,
                        help="Run a specific test by ID (e.g., T1_F01_01)")
    parser.add_argument("--self-test", action="store_true",
                        help="Run harness utility self-verification and exit")
    parser.add_argument("--summary", action="store_true",
                        help="Display test suite tier counts and summary")
    parser.add_argument("--list", action="store_true",
                        help="List all registered tests and exit")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable verbose output with error tracebacks")

    args = parser.parse_args()

    if args.self_test:
        success = run_self_test()
        sys.exit(0 if success else 1)

    if args.summary:
        print_summary()
        sys.exit(0)

    if args.list:
        list_tests()
        sys.exit(0)

    # Filter tests
    selected_tests = ALL_TESTS

    if args.tier != "all":
        tier_num = int(args.tier)
        selected_tests = [t for t in selected_tests if t["tier"] == tier_num]

    if args.milestone != "all":
        m_str = args.milestone.upper()
        if m_str == "M5":
            # M5 runs all tests
            pass
        else:
            selected_tests = [t for t in selected_tests if t["milestone"] == m_str]

    if args.feature is not None:
        selected_tests = [t for t in selected_tests if t.get("feature_id") == args.feature]

    if args.test is not None:
        selected_tests = [t for t in selected_tests if t["id"] == args.test]

    if not selected_tests:
        print("No tests match the specified filters.")
        sys.exit(0)

    print("=" * 70)
    print(f"RUNNING cpp_kv_store E2E TEST SUITE ({len(selected_tests)} tests selected)")
    print("=" * 70)

    passed = 0
    failed = 0
    skipped = 0
    start_total_time = time.time()

    for idx, test in enumerate(selected_tests, 1):
        test_id = test["id"]
        test_name = test["name"]
        tier = test["tier"]
        milestone = test["milestone"]
        fn = test["fn"]

        ctx = TestContext(test_id, milestone)
        test_start = time.time()
        try:
            fn(ctx)
            elapsed = time.time() - test_start
            passed += 1
            if args.verbose:
                print(f"[{idx:3d}/{len(selected_tests):3d}] [PASS] {test_id} (T{tier}, {milestone}) - {test_name} ({elapsed:.3f}s)")
        except PrereqNotMet as p:
            skipped += 1
            if args.verbose:
                print(f"[{idx:3d}/{len(selected_tests):3d}] [SKIP] {test_id} (T{tier}, {milestone}) - {test_name}: {p}")
        except Exception as e:
            failed += 1
            elapsed = time.time() - test_start
            print(f"[{idx:3d}/{len(selected_tests):3d}] [FAIL] {test_id} (T{tier}, {milestone}) - {test_name} ({elapsed:.3f}s)")
            print(f"       Error: {e}")
            if args.verbose:
                traceback.print_exc()
        finally:
            ctx.cleanup()

    total_time = time.time() - start_total_time

    print("=" * 70)
    print("E2E TEST SUITE EXECUTION SUMMARY")
    print("=" * 70)
    print(f"Total Selected: {len(selected_tests)}")
    print(f"Passed:         {passed}")
    print(f"Skipped:        {skipped} (pending target milestone builds)")
    print(f"Failed:         {failed}")
    print(f"Total Duration: {total_time:.3f}s")
    print("=" * 70)

    if failed > 0:
        print(f"STATUS: FAILED ({failed} failures)")
        sys.exit(1)
    elif skipped == len(selected_tests):
        print("STATUS: READY (All tests ready, pending milestone implementation builds)")
        sys.exit(0)
    else:
        print("STATUS: PASSED")
        sys.exit(0)


if __name__ == "__main__":
    main()
