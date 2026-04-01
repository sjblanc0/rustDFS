#!/usr/bin/env bash
set -e

NAMENODE="namenode:5000"
CLIENT="/usr/local/bin/rustDFS-client"

# MODE is set by docker-compose: "write" or "verify"
MODE="${MODE:-write}"

if [ "$MODE" = "write" ]; then
    echo "============================================"
    echo " Block Report Demo — Phase 1: Write"
    echo "============================================"
    echo

    echo "── Writing files/small.txt to DFS ──"
    $CLIENT write $NAMENODE /root/files/small.txt small.txt -v info
    echo
    sleep 2

    echo "── Writing files/medium.txt to DFS ──"
    $CLIENT write $NAMENODE /root/files/medium.txt medium.txt -v info
    echo
    sleep 2

    echo "── Writing files/large.txt to DFS ──"
    $CLIENT write $NAMENODE /root/files/large.txt large.txt -v info
    echo

    echo "============================================"
    echo " Phase 1 complete — files written"
    echo "============================================"

elif [ "$MODE" = "verify" ]; then
    echo "============================================"
    echo " Block Report Demo — Phase 2: Verify"
    echo "============================================"
    echo

    PASSED=0
    FAILED=0

    for FILE in small.txt medium.txt large.txt; do
        echo "── Reading $FILE from DFS ──"
        $CLIENT read $NAMENODE $FILE /root/read/$FILE -v info
        echo

        if diff -q /root/files/$FILE /root/read/$FILE > /dev/null 2>&1; then
            echo "  ✓ $FILE matches original"
            PASSED=$((PASSED + 1))
        else
            echo "  ✗ $FILE DOES NOT match original"
            FAILED=$((FAILED + 1))
        fi
        echo
    done

    echo "============================================"
    if [ "$FAILED" -eq 0 ]; then
        echo " All checks passed! ($PASSED / $PASSED)"
    else
        echo " Results: $PASSED / $((PASSED + FAILED)) passed"
    fi
    echo "============================================"

    [ "$FAILED" -eq 0 ]
fi
