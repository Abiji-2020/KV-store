#!/usr/bin/env python3
"""
E2E Test Harness Utilities for cpp_kv_store.
Provides process control, TCP socket client, RESP2/inline protocol codecs,
binary WAL decoders, IEEE 802.3 CRC32 validation, and isolated test fixtures.
"""

import os
import sys
import time
import socket
import struct
import signal
import shutil
import tempfile
import subprocess
import threading
from typing import Optional, Tuple, List, Dict, Any

# Root directories
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BIN_DIR = os.path.join(REPO_ROOT, "bin")
SERVER_BIN = os.path.join(BIN_DIR, "kv_server")
CLIENT_BIN = os.path.join(BIN_DIR, "kv_client")
BENCH_BIN = os.path.join(BIN_DIR, "kv_bench")
MAKEFILE_PATH = os.path.join(REPO_ROOT, "Makefile")

# WAL Framing Constants (Per PROJECT.md § Architecture)
WAL_MAGIC = b"\x57\x4C"  # 'W', 'L'
OP_SET = 1
OP_DEL = 2
OP_CLEAR = 3
WAL_HEADER_SIZE = 20  # 2 magic + 8 ts + 1 op + 4 key_len + 4 val_len + 1 padding/reserved or 24 total with crc
# IEEE 802.3 CRC32 polynomial: 0xEDB88320 (standard zlib/binascii.crc32)
import binascii

def compute_crc32(data: bytes) -> int:
    """Computes IEEE 802.3 CRC32 checksum as unsigned 32-bit integer."""
    return binascii.crc32(data) & 0xFFFFFFFF


def find_free_port() -> int:
    """Allocates an ephemeral free TCP port for test isolation."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


class KVClient:
    """Low-level TCP socket client supporting both inline text and RESP2 wire protocols."""

    def __init__(self, host: str = "127.0.0.1", port: int = 6379, timeout: float = 3.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None

    def connect(self, retries: int = 5, retry_delay: float = 0.1) -> bool:
        """Connects to server with retries."""
        for _ in range(retries):
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.settimeout(self.timeout)
                self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.sock.connect((self.host, self.port))
                return True
            except (ConnectionRefusedError, socket.timeout, OSError):
                if self.sock:
                    self.sock.close()
                    self.sock = None
                time.sleep(retry_delay)
        return False

    def close(self):
        """Closes TCP connection."""
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def send_raw(self, data: bytes):
        """Sends raw bytes over the TCP socket."""
        if not self.sock:
            raise ConnectionError("Socket not connected")
        self.sock.sendall(data)

    def read_line(self) -> str:
        """Reads a CRLF-terminated line from the server."""
        if not self.sock:
            raise ConnectionError("Socket not connected")
        buf = bytearray()
        while True:
            chunk = self.sock.recv(1)
            if not chunk:
                break
            buf.extend(chunk)
            if buf.endswith(b"\r\n"):
                break
        return buf.decode("utf-8", errors="replace")

    def read_exact(self, num_bytes: int) -> bytes:
        """Reads exactly num_bytes from socket."""
        if not self.sock:
            raise ConnectionError("Socket not connected")
        buf = bytearray()
        while len(buf) < num_bytes:
            chunk = self.sock.recv(num_bytes - len(buf))
            if not chunk:
                break
            buf.extend(chunk)
        return bytes(buf)

    # Inline Text Protocol Commands
    def cmd_inline(self, line: str) -> str:
        """Sends an inline text command and returns the single-line or multi-line response."""
        if not line.endswith("\r\n"):
            line += "\r\n"
        self.send_raw(line.encode("utf-8"))
        return self.read_line()

    def set(self, key: str, val: str) -> str:
        return self.cmd_inline(f"SET {key} {val}")

    def get(self, key: str) -> str:
        return self.cmd_inline(f"GET {key}")

    def del_key(self, key: str) -> str:
        return self.cmd_inline(f"DEL {key}")

    def ping(self, msg: Optional[str] = None) -> str:
        if msg:
            return self.cmd_inline(f"PING {msg}")
        return self.cmd_inline("PING")

    def stats(self) -> str:
        return self.cmd_inline("STATS")

    def quit(self) -> str:
        return self.cmd_inline("QUIT")

    # RESP2 Wire Protocol Commands
    def cmd_resp2(self, *args: str) -> str:
        """Encodes command as RESP2 array of bulk strings, sends, and parses response."""
        payload = f"*{len(args)}\r\n"
        for arg in args:
            payload += f"${len(arg.encode('utf-8'))}\r\n{arg}\r\n"
        self.send_raw(payload.encode("utf-8"))
        
        # Parse standard RESP2 or inline reply
        first_line = self.read_line()
        if not first_line:
            return ""
        if first_line.startswith("$"):
            # Bulk string: $<len>\r\n<data>\r\n
            try:
                length = int(first_line[1:].strip())
                if length == -1:
                    return "$-1\r\n"
                data = self.read_exact(length)
                crlf = self.read_exact(2)
                return data.decode("utf-8", errors="replace")
            except ValueError:
                return first_line
        return first_line

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class KVServerProcess:
    """Manages spawning, monitoring, and terminating the standalone kv_server process."""

    def __init__(self, port: Optional[int] = None, wal_path: Optional[str] = None,
                 capacity: int = 0, sync_mode: str = "always", extra_args: Optional[List[str]] = None):
        self.port = port if port is not None else find_free_port()
        self.wal_path = wal_path
        self.capacity = capacity
        self.sync_mode = sync_mode
        self.extra_args = extra_args or []
        self.process: Optional[subprocess.Popen] = None
        self.stdout_log: Optional[str] = None

    def start(self, timeout: float = 3.0) -> bool:
        """Starts kv_server and verifies it accepts connections."""
        if not os.path.exists(SERVER_BIN):
            return False

        cmd = [
            SERVER_BIN,
            "--port", str(self.port),
            "--capacity", str(self.capacity),
            "--sync", self.sync_mode,
        ]
        if self.wal_path:
            cmd.extend(["--wal", self.wal_path])
        cmd.extend(self.extra_args)

        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            preexec_fn=os.setsid if hasattr(os, "setsid") else None
        )

        # Wait for port readiness
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.process.poll() is not None:
                # Process exited prematurely
                return False
            client = KVClient(port=self.port, timeout=0.2)
            if client.connect():
                client.close()
                time.sleep(0.15)
                if self.process.poll() is not None:
                    return False
                return True
            time.sleep(0.05)
        return False

    def stop(self, timeout: float = 2.0):
        """Gracefully stops server with SIGTERM."""
        if self.process:
            try:
                self.process.terminate()
                self.process.wait(timeout=timeout)
            except (subprocess.TimeoutExpired, ProcessLookupError):
                try:
                    self.process.kill()
                    self.process.wait(timeout=timeout)
                except Exception:
                    pass
            self.process = None

    def kill_crash(self):
        """Simulates an abrupt crash via SIGKILL (kill -9) without clean shutdown."""
        if self.process:
            try:
                self.process.kill()
                self.process.wait(timeout=1.0)
            except Exception:
                pass
            self.process = None

    def is_alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


class WalRecord:
    """Represents a decoded binary WAL record."""
    def __init__(self, opcode: int, timestamp_ns: int, key: str, val: str, crc32: int):
        self.opcode = opcode
        self.timestamp_ns = timestamp_ns
        self.key = key
        self.val = val
        self.crc32 = crc32

    def __repr__(self):
        return f"WalRecord(op={self.opcode}, key='{self.key}', val='{self.val}', crc=0x{self.crc32:08X})"


class WalDecoder:
    """Binary parser and manipulator for Write-Ahead Log files."""

    @staticmethod
    def encode_record(opcode: int, key: str, val: str, timestamp_ns: Optional[int] = None) -> bytes:
        """Encodes a mutation into the 24-byte framed WAL binary format."""
        if timestamp_ns is None:
            timestamp_ns = int(time.time() * 1e9)
        k_bytes = key.encode("utf-8")
        v_bytes = val.encode("utf-8")
        k_len = len(k_bytes)
        v_len = len(v_bytes)

        # Header: magic (2B), opcode (1B), reserved (1B), timestamp (8B), k_len (4B), v_len (4B)
        header_data = struct.pack(">2sBBQII", WAL_MAGIC, opcode, 0, timestamp_ns, k_len, v_len)
        record_content = header_data + k_bytes + v_bytes
        crc = compute_crc32(record_content)
        return record_content + struct.pack(">I", crc)

    @staticmethod
    def decode_file(wal_path: str) -> Tuple[List[WalRecord], int, bool]:
        """
        Decodes all valid records from a WAL file.
        Returns: (records, valid_byte_offset, has_torn_tail)
        """
        records: List[WalRecord] = []
        if not os.path.exists(wal_path):
            return records, 0, False

        with open(wal_path, "rb") as f:
            data = f.read()

        offset = 0
        total_len = len(data)
        has_torn_tail = False

        while offset + 24 <= total_len:
            # Check magic bytes
            if data[offset:offset+2] != WAL_MAGIC:
                has_torn_tail = True
                break

            magic, opcode, _, ts, k_len, v_len = struct.unpack_from(">2sBBQII", data, offset)
            record_len = 20 + k_len + v_len + 4  # 20 header + payloads + 4 CRC
            if offset + record_len > total_len:
                # Incomplete record at file tail
                has_torn_tail = True
                break

            record_bytes = data[offset : offset + record_len - 4]
            stored_crc, = struct.unpack_from(">I", data, offset + record_len - 4)
            computed_crc = compute_crc32(record_bytes)

            if stored_crc != computed_crc:
                # CRC mismatch: torn or corrupted record
                has_torn_tail = True
                break

            k_start = offset + 20
            k_end = k_start + k_len
            v_end = k_end + v_len
            key = data[k_start:k_end].decode("utf-8", errors="replace")
            val = data[k_end:v_end].decode("utf-8", errors="replace")

            records.append(WalRecord(opcode, ts, key, val, stored_crc))
            offset += record_len

        if offset < total_len and not has_torn_tail:
            has_torn_tail = True

        return records, offset, has_torn_tail


class KVClientCLI:
    """Subprocess runner for bin/kv_client."""

    @staticmethod
    def run_single_shot(port: int, args: List[str], timeout: float = 3.0) -> Tuple[int, str, str]:
        cmd = [CLIENT_BIN, "--host", "127.0.0.1", "--port", str(port)] + args
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)
        return proc.returncode, proc.stdout, proc.stderr

    @staticmethod
    def run_repl(port: int, inputs: List[str], timeout: float = 3.0) -> Tuple[int, str, str]:
        cmd = [CLIENT_BIN, "--host", "127.0.0.1", "--port", str(port)]
        input_data = "\n".join(inputs) + "\n"
        proc = subprocess.run(cmd, input=input_data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)
        return proc.returncode, proc.stdout, proc.stderr


class KVBenchCLI:
    """Subprocess runner for bin/kv_bench."""

    @staticmethod
    def run_bench(port: int, threads: int = 4, requests: int = 1000,
                  ratio: float = 0.8, keys: int = 100, val_size: int = 64,
                  timeout: float = 10.0) -> Tuple[int, str, str]:
        cmd = [
            BENCH_BIN,
            "--host", "127.0.0.1",
            "--port", str(port),
            "-c", str(threads),
            "-n", str(requests),
            "-r", str(ratio),
            "-k", str(keys),
            "-s", str(val_size)
        ]
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)
        return proc.returncode, proc.stdout, proc.stderr


class ReferenceOracle:
    """Authoritative reference implementation of KVStore & LRU eviction logic."""

    def __init__(self, capacity: int = 0):
        self.capacity = capacity
        self.store: Dict[str, str] = {}
        self.lru: List[str] = []
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def set(self, key: str, val: str) -> str:
        if key in self.store:
            self.store[key] = val
            self.lru.remove(key)
            self.lru.append(key)
            return "OK"
        if self.capacity > 0 and len(self.store) >= self.capacity:
            evicted = self.lru.pop(0)
            del self.store[evicted]
            self.evictions += 1
        self.store[key] = val
        self.lru.append(key)
        return "OK"

    def get(self, key: str) -> Optional[str]:
        if key in self.store:
            self.hits += 1
            self.lru.remove(key)
            self.lru.append(key)
            return self.store[key]
        self.misses += 1
        return None

    def del_key(self, key: str) -> bool:
        if key in self.store:
            del self.store[key]
            self.lru.remove(key)
            return True
        return False
