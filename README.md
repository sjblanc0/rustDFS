# rustDFS

A distributed file system written in Rust, inspired by [Apache HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html). 

Files are split into configurable fixed-size blocks (default 4 MB), distributed across multiple data nodes with configurable replication, and tracked by a central name node.

## Modules

| Crate | Binary | Description |
|---|---|---|
| `client` | `rustDFS-client` | CLI tool that reads / writes files to the cluster via gRPC streaming |
| `name_node` | `rustDFS-namenode` | Metadata server — manages the file namespace, allocates blocks, assigns data nodes, and issues write leases |
| `data_node` | `rustDFS-datanode` | Storage server — persists blocks on local disk and chains replication to peer data nodes |
| `proto` | *(library)* | Protocol Buffer definitions and generated gRPC code (`name_node.proto`, `data_node.proto`) |
| `shared` | *(library)* | Common code — config parsing, connection management, logging, error types, host resolution |

## How It Works

### Write Path

1. The client sends a `WriteStartRequest` to the name node with the file name, file size, and a unique operation ID.
2. The name node allocates corresponding blocks, assigns a UUID to each, and randomly selects data nodes from the registered pool. It returns the block assignments and a time-limited write lease.
3. For each block, the client opens a **streaming gRPC connection directly to the primary data node** and sends `WriteRequest` messages containing the data, alongside an ordered list of replica nodes.
4. The primary data node writes each chunk to local disk and **forwards it to the next replica**. Each replica repeats this until the chain is exhausted.
5. Per-chunk acknowledgements flow back through the replication chain to the client.
6. After all blocks are written, the client calls `WriteEnd` to finalize the file and release the lease.

### Read Path

1. The client sends a `ReadRequest` to the name node with the file name.
2. The name node returns the latest completed file descriptor: block IDs, sizes, and a shuffled list of replica nodes per block.
3. For each block, the client connects **directly to a data node** and streams `ReadResponse` messages containing data chunks.
4. If a data node fails mid-stream the client retries on the next replica, resuming from the byte offset where it left off.
5. Received chunks are written sequentially to a local file.

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

All nodes read a shared TOML configuration file (default: `/etc/rustdfs/rdfsconf.toml`). See [example/rdfsconf.toml](example/rdfsconf.toml) for a full example.

| Key | Default | Description |
|---|---|---|
| `replica-count` | `0` | Number of replica copies per block (in addition to the primary) |
| `lease-duration` | `120` | Write lease duration in seconds |
| `message-size` | `"64KB"` | Max gRPC streaming chunk size (supports B, KB, MB, GB suffixes) |
| `block-size` | `"32MB"` | Max size of a single data block (supports B, KB, MB, GB suffixes) |

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

## Usage

### Name Node

```bash
rustDFS-namenode [--silent] [--log-level <error|info|debug>]
```

### Data Node

```bash
rustDFS-datanode --port <PORT> [--silent] [--log-level <error|info|debug>]
```

The data node resolves its own hostname automatically and registers itself with the name node on startup.

### Client

```bash
# Write a file into the cluster
rustDFS-client write <namenode_host:port> <local_source> <remote_dest>

# Read a file from the cluster
rustDFS-client read <namenode_host:port> <remote_source> <local_dest>
```

## CI

GitHub Actions runs three checks on every change to `main`:

| Job | Command |
|---|---|
| `build` | `cargo build --all-features` |
| `clippy` | `cargo clippy --all-targets --all-features -- -D warnings` |
| `rustfmt` | `cargo fmt --all -- --check` |

## Running with Docker Compose

The `example/` directory contains a ready-to-run demo with Docker Compose that spins up 3 data nodes, 1 name node, and a client container.

```bash
cd example
docker compose up --build
```

The demo writes `small.txt` and `large.txt` into the cluster, reads them back, and diffs the results to verify data integrity.

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
│       └── data_node.proto
└── example/         # Docker Compose demo
    ├── docker-compose.yml
    ├── rdfsconf.toml
    ├── client.sh
    └── files/       # Sample test files (small.txt, large.txt)
```
