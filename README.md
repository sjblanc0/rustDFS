# rustDFS

A distributed file system written in Rust, inspired by [Apache HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).

Files are split into configurable fixed-size blocks (default 4 MB), distributed across multiple data nodes with configurable replication, and tracked by a central name node.

## Modules

| Crate | Binary | Description |
|---|---|---|
| `client` | `rustDFS-client` | CLI tool that reads / writes files to the cluster via gRPC streaming |
| `name_node` | `rustDFS-namenode` | Metadata server — manages the file namespace, allocates blocks, assigns data nodes, issues write leases, and tracks data node liveness via heartbeats |
| `data_node` | `rustDFS-datanode` | Storage server — persists blocks on local disk, chains replication to peer data nodes, and sends periodic heartbeats to the name node |
| `proto` | *(library)* | Protocol Buffer definitions and generated gRPC code (`name_node.proto`, `data_node.proto`, `name_node_persist.proto`) |
| `shared` | *(library)* | Common code — config parsing, connection helpers, logging, error types, host resolution |

## How It Works

### Write Path

1. The client sends a `WriteStartRequest` to the name node with the file name, file size, and a unique operation ID.
2. The name node journals the operation, allocates blocks with UUID-named IDs, and randomly selects data nodes from the registered pool. It returns the block assignments and a time-limited write lease.
3. For each block, the client opens a **streaming gRPC connection directly to the primary data node** and sends `WriteRequest` messages containing the data, alongside an ordered list of replica nodes.
4. The primary data node writes each chunk to local disk and **forwards it to the next replica**. Each replica repeats this until the chain is exhausted.
5. Per-chunk acknowledgements flow back through the replication chain to the client.
6. After all blocks are written, the client calls `WriteEnd` to finalize the file, release the lease, and commit the journal entry.

### Read Path

1. The client sends a `ReadRequest` to the name node with the file name.
2. The name node returns the latest completed file descriptor: block IDs, sizes, and a shuffled list of replica nodes per block.
3. For each block, the client connects **directly to a data node** and streams `ReadResponse` messages containing data chunks.
4. If a data node fails mid-stream the client retries on the next replica, resuming from the byte offset where it left off.
5. Received chunks are written sequentially to a local file.

### Name Node Persistence

The name node durably persists the file namespace using an HDFS-inspired journal + checkpoint scheme:

- **Journal** — an append-only edit log of every namespace mutation (write start, write complete). Fast and crash-safe.
- **Checkpoint** — a full protobuf snapshot of the file namespace, written periodically or when the journal exceeds a configurable transaction threshold.
- **Recovery** — on startup, loads the latest checkpoint and replays journal entries to reconstruct the in-memory state. In-progress writes that were never completed are rolled back.

Data node locations are **not** persisted — they are recovered from data node registrations and heartbeats after restart.

### Heartbeats & Liveness

Data nodes send periodic heartbeat RPCs to the name node (default: every 3 seconds). The name node runs a background reaper that marks nodes as dead after a configurable timeout (default: 30 seconds of missed heartbeats). If a heartbeat fails, the data node re-enters its registration flow with exponential backoff retry.

## Developing

The project is built with the following tools:

- **Rust** (edition 2024)
- **protoc** (Protocol Buffers compiler) → required by `tonic-build` / `prost-build`

The `.devcontainer` directory provides tools for building / running rustDFS locally. This includes a Dockerfile with the required dependencies and additional dev tools.

The container can be built / run using the provided `docker.sh`.

```bash
bash .devcontainer/docker.sh build
bash .devcontainer/docker.sh run
```

## Building

```bash
cargo build --release
```

Binaries are placed in `target/release/`:
- `rustDFS-client`
- `rustDFS-namenode`
- `rustDFS-datanode`

## Configuration

All nodes read a shared TOML configuration file (default: `/etc/rustdfs/rdfsconf.toml`). See [example/basic/rdfsconf.toml](example/basic/rdfsconf.toml) for a full example.

### Global

| Key | Default | Description |
|---|---|---|
| `replica-count` | `0` | Number of replica copies per block (in addition to the primary) |
| `lease-duration` | `120` | Write lease duration in seconds |
| `message-size` | `"64KB"` | Max gRPC streaming chunk size (supports B, KB, MB, GB suffixes) |
| `block-size` | `"32MB"` | Max size of a single data block (supports B, KB, MB, GB suffixes) |

### Name Node (`[name-node]`)

| Key | Default | Description |
|---|---|---|
| `host` | — | Hostname or IP address |
| `port` | — | gRPC port |
| `name-dir` | `"/var/lib/rustdfs/names"` | Directory for checkpoint and journal files |
| `log-file` | `"/var/log/rustdfs"` | Log file path |
| `checkpoint-transactions` | `1000` | Max journal entries before forcing a checkpoint |
| `checkpoint-period` | `3600` | Max seconds between checkpoints |
| `heartbeat-timeout` | `30` | Seconds of missed heartbeats before declaring a data node dead |
| `heartbeat-recheck-interval` | `5` | Seconds between reaper sweeps for stale nodes |

### Data Node (`[data-node]`)

| Key | Default | Description |
|---|---|---|
| `data-dir` | `"/var/lib/rustdfs/data"` | Directory for storing data blocks |
| `log-file` | `"/var/log/rustdfs"` | Log file path |
| `heartbeat-interval` | `3` | Seconds between heartbeat RPCs to the name node |
| `replica-connection-ttl` | `300` | Seconds before an idle replication connection is evicted |

### Full Example

```toml
replica-count = 2
message-size = "16KB"
block-size = "4MB"

[name-node]
host = "namenode"
port = 5000
name-dir = "/var/lib/rustdfs/names"
log-file = "/var/log/rustdfs/namenode.log"
heartbeat-timeout = 30
heartbeat-recheck-interval = 5

[data-node]
data-dir = "/var/lib/rustdfs/data"
log-file = "/var/log/rustdfs/datanode.log"
heartbeat-interval = 3
replica-connection-ttl = 300
```

## Usage

### Name Node

```bash
rustDFS-namenode [--silent] [--log-level <error|info|debug>]
```

### Data Node

```bash
rustDFS-datanode --port <PORT> [--silent] [--log-level <error|info|debug>]
```

The data node resolves its own hostname automatically and registers itself with the name node on startup. If the connection drops, it retries with exponential backoff indefinitely.

### Client

```bash
# Write a file into the cluster
rustDFS-client write <namenode_host:port> <local_source> <remote_dest> [-v <error|info|debug>]

# Read a file from the cluster
rustDFS-client read <namenode_host:port> <remote_source> <local_dest> [-v <error|info|debug>]
```

## gRPC API

### Name Node

| RPC | Request | Response | Description |
|---|---|---|---|
| `WriteStart` | `WriteStartRequest` | `WriteStartResponse` | Allocate blocks, assign data nodes, acquire write lease |
| `WriteEnd` | `WriteEndRequest` | `Empty` | Finalize or abort a write, release lease |
| `RenewLease` | `RenewLeaseRequest` | `RenewLeaseResponse` | Extend an active write lease |
| `Read` | `ReadRequest` | `ReadResponse` | Get block locations for a file |
| `Register` | `RegisterRequest` | `Empty` | Register a data node with the cluster |
| `Heartbeat` | `HeartbeatRequest` | `HeartbeatResponse` | Data node liveness signal |

### Data Node

| RPC | Request | Response | Description |
|---|---|---|---|
| `Write` | `stream WriteRequest` | `stream Empty` | Stream block data with chained replication |
| `Read` | `ReadRequest` | `stream ReadResponse` | Stream block data from a given offset |

## CI

GitHub Actions runs four checks on every change to `main`:

| Job | Command |
|---|---|
| `build` | `cargo build --all-features` |
| `clippy` | `cargo clippy --all-targets --all-features -- -D warnings` |
| `machete` | `machete check` |
| `rustfmt` | `cargo fmt --all -- --check` |

## Running with Docker Compose

The `example/` directory contains two ready-to-run demos:

| Example | Description |
|---|---|
| [`example/basic`](example/basic/) | Write + read verification (small, medium, and large files) |
| [`example/leasing`](example/leasing/) | Write-lease conflict and read-during-overwrite tests |

Each has its own `docker-compose.yml` that spins up 3 data nodes, 1 name node, and a client container:

```bash
cd example/basic
docker compose up --build
```

### Test Files

| File | Size | Purpose |
|---|---|---|
| `small.txt` | 16 KB | Single-message transfer (1 × message size) |
| `medium.txt` | 64 KB | Multi-message, single-block transfer |
| `large.txt` | 12 MB | Multi-block transfer (3 blocks at 4 MB) |

## Project Layout

```
rustDFS/
├── client/          # CLI client crate
├── data_node/       # Data node server crate
├── name_node/       # Name node server crate
├── shared/          # Shared library crate (config, connections, logging, errors)
├── proto/           # Protobuf / gRPC crate
│   └── proto/
│       ├── name_node.proto
│       ├── name_node_persist.proto
│       └── data_node.proto
└── example/         # Docker Compose demos
    ├── basic/       # Read / write verification
    │   ├── docker-compose.yml
    │   ├── rdfsconf.toml
    │   └── client.sh
    └── leasing/     # Lease conflict & concurrent read tests
        ├── docker-compose.yml
        ├── rdfsconf.toml
        └── client.sh
```
