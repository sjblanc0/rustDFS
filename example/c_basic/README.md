# rustDFS C FFI Example

A Docker Compose setup that spins up a full rustDFS cluster and runs a C client that uses the `librustdfs_client.so` shared library through the C FFI.

## Cluster Layout

Same as `example/basic/` — 1 name node, 3 data nodes, 1 client. The client container compiles and runs a C program instead of calling the `rustDFS-client` CLI.

## Running

From the `example/c_basic/` directory:

```bash
docker compose up --build
```

The C client will write `small.txt`, `medium.txt`, and `large.txt` to the cluster, read them back, and byte-compare the results.

## Cleanup

```bash
docker compose down -v
```
