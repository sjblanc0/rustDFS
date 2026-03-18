# rustDFS Example

A Docker Compose setup that spins up a full rustDFS cluster and runs a read / write verification test.

## Cluster Layout

```
                ┌───────────────────┐
                │  namenode  (nn0)  │  :5000
                └─────────┬─────────┘
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ datanode0 (dn0) │ │ datanode1 (dn1) │ │ datanode2 (dn2) │
│     :5001       │ │     :5002       │ │     :5003       │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

The cluster is configured with a replica count of 2. This means that for any block written, there will be 1 primary node and 2 replicas. All nodes communicate over a shared `rustdfs` bridge network.

## Running

From the `example/basic/` directory:

```bash
docker compose up --build
```

## Demo

The client container executes `client.sh`, which performs the following steps:

1. **Write** `small.txt` into the cluster
2. **Read** `small.txt` back from the cluster
3. **Write** `medium.txt` into the cluster
4. **Read** `medium.txt` back from the cluster
5. **Write** `large.txt` into the cluster
6. **Read** `large.txt` back from the cluster
7. **Diff** the original and retrieved `small.txt` and verifies they are identical
8. **Diff** the original and retrieved `medium.txt` and verifies they are identical
9. **Diff** the original and retrieved `large.txt` and verifies they are identical

If all diffs pass, you'll see:

```
============================================
 All checks passed!
============================================
```

Name / data node console output is also visible from the Docker Compose orchestrator.

## Configuration

The cluster configuration lives in `rdfsconf.toml`. All data nodes share the same `[data-node]` config block; each resolves its own hostname automatically and receives its port via the `--port` CLI flag:

```toml
replica-count = 2
message-size = "16KB"
block-size = "4MB"

[name-node]
host = "namenode"
port = 5000
log-file = "/var/log/rustdfs/namenode.log"

[data-node]
data-dir = "/var/lib/rustdfs/data"
log-file = "/var/log/rustdfs/datanode.log"
```

| Key | Description |
|---|---|
| `replica-count` | Number of replicas per block (in addition to the primary) |
| `message-size` | Max gRPC streaming chunk size (e.g. `"16KB"`, `"64KB"`) |
| `block-size` | Max bytes per data block (e.g. `"4MB"`, `"32MB"`) |

To modify the cluster (e.g., change the replication factor, block size, or add / remove data nodes), edit `rdfsconf.toml` and update `docker-compose.yml` accordingly.

## Cleanup

```bash
docker compose down -v
```

This stops all containers and removes any created volumes.