#!/usr/bin/env bash
set -e

NAMENODE="namenode:5000"
CLIENT="/usr/local/bin/rustDFS-client"

echo "============================================"
echo " rustDFS Demo"
echo "============================================"
echo

# 1. write small.txt to the name node
echo "── Step 1: Writing files/small.txt to DFS ──"
$CLIENT write $NAMENODE /root/files/small.txt small.txt -v info
echo
sleep 5

# 2. read small.txt from the name node
echo "── Step 2: Reading small.txt from DFS ──"
$CLIENT read $NAMENODE small.txt /root/read/small.txt -v info
echo
sleep 5

# 3. write large.txt to the name node
echo "── Step 3: Writing files/large.txt to DFS ──"
$CLIENT write $NAMENODE /root/files/large.txt large.txt -v info
echo
sleep 5

# 4. read large.txt from the name node
echo "── Step 4: Reading large.txt from DFS ──"
$CLIENT read $NAMENODE large.txt /root/read/large.txt -v info
echo
sleep 5

# 5. diff small.txt
echo "── Step 5: Comparing small.txt ──"
if diff /root/files/small.txt /root/read/small.txt; then
    echo "OK: small.txt files are identical"
else
    echo "FAIL: small.txt files differ"
    exit 1
fi
echo
sleep 5

# 6. diff large.txt
echo "── Step 6: Comparing large.txt ──"
if diff /root/files/large.txt /root/read/large.txt; then
    echo "OK: large.txt files are identical"
else
    echo "FAIL: large.txt files differ"
    exit 1
fi
echo

echo "============================================"
echo " All checks passed!"
echo "============================================"
