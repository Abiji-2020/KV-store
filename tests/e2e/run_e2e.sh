#!/usr/bin/env bash
# ==============================================================================
# run_e2e.sh - Autonomous 4-Tier E2E Test Suite Runner for cpp_kv_store
# ==============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_EXEC="python3"

if ! command -v "$PYTHON_EXEC" &> /dev/null; then
    echo "Error: python3 is required to run the E2E test suite."
    exit 1
fi

exec "$PYTHON_EXEC" "$SCRIPT_DIR/test_runner.py" "$@"
