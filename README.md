# rustDFS

A distributed file system written in Rust, inspired by [Apache HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html). 

Files are split into 2 MB blocks, distributed across multiple data nodes with configurable replication, and tracked by a central name node.

## Modules

| Crate | Binary | Description |
|---|---|---|
| `client` | `rustDFS-client` | CLI tool that reads / writes files to the cluster via gRPC streaming |
| `name_node` | `rustDFS-namenode` | Metadata server — maps filenames to block IDs and data node locations, coordinates replication |
| `data_node` | `rustDFS-datanode` | Storage server — persists blocks on local disk and replicates to peer data nodes |
| `shared` | *(library)* | Common code — config parsing, proto definitions, connection management, logging, error types |

## How It Works

### Write Path

1. The client streams 2 MB chunks to the name node.
2. The name node assigns a UUID to each block and randomly selects a primary data node plus replica targets.
3. The primary data node writes the block to disk and forwards it to the replica nodes.

### Read Path

1. The client requests a file from the name node.
2. The name node looks up block descriptors (block ID → data node locations).
3. Blocks are read from data nodes with failover across replicas (randomized order).
4. Data is streamed back to the client.

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

```toml
replica-count = 2

[name-node.nn0]
host = "nn0"
port = 5000
log-file = "/var/log/rustdfs/namenode.log"

[data-node.dn0]
host = "dn0"
port = 5001
data-dir = "/var/lib/rustdfs/data"
log-file = "/var/log/rustdfs/datanode.log"

[ ... ]
```

## Usage

### Name Node

```bash
rustDFS-namenode --id nn0 [--silent] [--log-level <error|info|debug>]
```

### Data Node

```bash
rustDFS-datanode --id dn0 [--silent] [--log-level <error|info|debug>]
```

### Client

```bash
# Write a file into the cluster
rustDFS-client write <namenode_host:port> <local_source> <remote_dest>

# Read a file from the cluster
rustDFS-client read <namenode_host:port> <remote_source> <local_dest>
```

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
├── shared/          # Shared library crate
├── proto/           # Protocol Buffer definitions
│   ├── name_node.proto
│   └── data_node.proto
└── example/         # Docker Compose demo
    ├── docker-compose.yml
    ├── rdfsconf.toml
    ├── client.sh
    └── files/       # Sample files for testing
```
