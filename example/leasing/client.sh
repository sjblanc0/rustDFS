#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# rustDFS Leasing Example
#
# Test 1: A locked file rejects a concurrent write.
# Test 2: A completed version is readable while a new write
#         to the same file is in progress.
# ─────────────────────────────────────────────────────────────
set -o pipefail

NAMENODE="namenode:5000"
CLIENT="/usr/local/bin/rustDFS-client"
PASS=0
FAIL=0

pass() { 
    echo "  ✓ PASS: $1"; PASS=$((PASS + 1)); 
}

fail() { 
    echo "  ✗ FAIL: $1"; FAIL=$((FAIL + 1)); 
}

# Helpers to throttle / restore bandwidth on the default interface.
# tbf rate-caps bytes/s regardless of TCP windowing, so a 12 MB file
# at 2 Mbit/s takes ~48 s — long enough for concurrent ops to overlap.
# Pick the first non-loopback up interface (eth0, eth1, ens*, etc.)
TC_IFACE=$(ip -o link show up | awk -F': ' '$2 != "lo" {gsub(/@.*/, "", $2); print $2; exit}')
if [ -z "$TC_IFACE" ]; then
    echo "ERROR: Could not detect a non-loopback network interface"
    exit 1
fi
echo "   Network interface: $TC_IFACE"

add_throttle() {
    echo "   Adding bandwidth cap: 2mbit on $TC_IFACE"
    tc qdisc add dev "$TC_IFACE" root tbf rate 2mbit burst 32kbit latency 300ms || {
        echo "ERROR: tc failed — is NET_ADMIN capability set?"
        exit 1
    }
}

drop_throttle() {
    echo "   Removing bandwidth cap"
    tc qdisc del dev "$TC_IFACE" root 2>/dev/null || true
}

echo "============================================"
echo " rustDFS Leasing Tests"
echo "============================================"
echo

# ──────────────────────────────────────────────────────────────
# Test 1 — Lease conflict: second writer is blocked
# ──────────────────────────────────────────────────────────────
echo "── Test 1: Lease conflict ──"
echo "   Client A begins a long write to lease-test.txt ..."

# Slow down the network so the large write holds the lease long enough
add_throttle

# Client A: start a slow write (large file) in the background
$CLIENT write $NAMENODE /root/files/large.txt lease-test.txt -v info &
PID_A=$!

# Give Client A time to acquire the lease and begin streaming
sleep 3

echo "   Client B attempts to write the same file ..."

# Client B: try to write the same file — should fail
if OUTPUT_B=$($CLIENT write $NAMENODE /root/files/small.txt lease-test.txt -v error 2>&1); then
    fail "Client B was NOT blocked (expected 'File is currently locked for write')"
else
    if echo "$OUTPUT_B" | grep -q "locked for write"; then
        pass "Client B was blocked — file is locked for write"
    else
        fail "Client B failed but with an unexpected error: $OUTPUT_B"
    fi
fi

# Let Client A finish
echo "   Waiting for Client A to finish ..."
wait $PID_A 2>/dev/null || true
drop_throttle

echo

# ──────────────────────────────────────────────────────────────
# Test 2 — Read during concurrent overwrite
# ──────────────────────────────────────────────────────────────
echo "── Test 2: Read completed version during in-progress overwrite ──"

# Step A: write small.txt as version-test.txt (small, completes quickly)
echo "   Client A writes small.txt as version-test.txt ..."
$CLIENT write $NAMENODE /root/files/small.txt version-test.txt -v info
sleep 1

# Step B: start an overwrite with large.txt (slow, lease held) in background
echo "   Client B begins overwriting version-test.txt with a large file ..."
add_throttle
$CLIENT write $NAMENODE /root/files/large.txt version-test.txt -v info &
PID_B=$!

# Give Client B time to acquire the lease and begin streaming
sleep 3

# Step C: read version-test.txt — should get Client A's small content
echo "   Client C reads version-test.txt while overwrite is in progress ..."
$CLIENT read $NAMENODE version-test.txt /root/read/version-test.txt -v info

# Verify the read content matches small.txt (Client A's write)
if diff /root/files/small.txt /root/read/version-test.txt > /dev/null 2>&1; then
    pass "Read returned Client A's completed version"
else
    fail "Read did NOT return the expected content"
fi

# Let Client B finish
echo "   Waiting for Client B to finish ..."
wait $PID_B 2>/dev/null || true
drop_throttle

echo

# ──────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────
TOTAL=$((PASS + FAIL))
echo "============================================"
echo " Results: $PASS / $TOTAL passed"
echo "============================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
