#!/usr/bin/env bash
#
# Block Report Demo — Full Cluster Restart
#
# 1. Starts the cluster, writes files via the client.
# 2. Stops the ENTIRE cluster (name node starts fresh, data nodes keep volumes).
# 3. Restarts the cluster — data nodes send block reports to reconstruct the namespace.
# 4. Reads back all files and verifies they match the originals.
#
set -e

cd "$(dirname "$0")"

echo "═══════════════════════════════════════════"
echo " Phase 1 — Build & write files"
echo "═══════════════════════════════════════════"

# build images once
docker compose build

# start cluster and run client in write mode
docker compose up -d namenode
docker compose up -d datanode0 datanode1 datanode2
echo "Waiting for cluster to become healthy..."
sleep 15

docker compose run --rm -e MODE=write client

echo
echo "═══════════════════════════════════════════"
echo " Phase 2 — Full cluster stop"
echo "═══════════════════════════════════════════"

docker compose stop namenode datanode0 datanode1 datanode2
echo "All services stopped."
sleep 2

echo
echo "═══════════════════════════════════════════"
echo " Phase 3 — Restart cluster (fresh name node)"
echo "═══════════════════════════════════════════"

# remove name node container so it starts with empty namespace
docker compose rm -f namenode
docker compose up -d namenode
docker compose up -d datanode0 datanode1 datanode2
echo "Waiting for cluster to become healthy and block reports to arrive..."
sleep 20

echo
echo "═══════════════════════════════════════════"
echo " Phase 4 — Verify reads after reconstruction"
echo "═══════════════════════════════════════════"

docker compose run --rm -e MODE=verify client
RC=$?

echo
echo "═══════════════════════════════════════════"
echo " Cleanup"
echo "═══════════════════════════════════════════"

docker compose down -v
exit $RC
