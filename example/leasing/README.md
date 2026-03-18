# rustDFS Leasing Example

A Docker Compose setup that tests write-lease semantics in a rustDFS cluster.

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

## Running

From the `example/leasing/` directory:

```bash
docker compose up --build
```

## Tests

The client container runs `client.sh`, which exercises two lease-related scenarios.

### Test 1 — Lease Conflict

Verifies that a second writer is rejected while a write lease is held.

1. **Client A** starts a long-running write of `large.txt` (12 MB, 3 blocks) to `lease-test.txt`. This acquires a write lease and holds it for the duration of the upload.
2. **Client B** immediately attempts to write `small.txt` to the same `lease-test.txt`.
3. The name node rejects Client B with a **File is currently locked for write** error because the file is locked by Client A's active lease.

### Test 2 — Read During Concurrent Overwrite

Verifies that the last completed version remains readable while a new write to the same file is in progress.

1. **Client A** writes `small.txt` to `version-test.txt` and completes (lease released).
2. **Client B** starts overwriting `version-test.txt` with `large.txt` (slow, lease held).
3. While Client B is still writing, **Client C** reads `version-test.txt`.
4. Client C receives Client A's original content — the in-progress write does not affect the completed version.

### Expected Output

```
============================================
 Results: 2 / 2 passed
============================================
```

## Configuration

The cluster uses a tuned configuration (`rdfsconf.toml`) to make lease behavior observable:

| Key | Value | Why |
|---|---|---|
| `lease-duration` | `120` | Short enough to keep tests fast, long enough to span the write |
| `message-size` | `"16KB"` | Same as the basic example |
| `block-size` | `"4MB"` | Standard block size |
| `replica-count` | `2` | Same as the basic example |

## Cleanup

```bash
docker compose down -v
```
