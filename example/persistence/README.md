# Persistence Demo — Full Cluster Restart

Demonstrates data recovery after a **complete cluster restart**. The name node persists file metadata (checkpoint + journal) to disk. Data nodes persist raw block data. On restart, the name node restores the file map from its checkpoint/journal, and data nodes send block reports so the name node can repopulate block-to-node mappings.

## Cluster Layout

```
                ┌───────────────────┐
                │  namenode  (nn0)  │  :5000
                │  [nn-names vol]   │
                └─────────┬─────────┘
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ datanode0 (dn0) │ │ datanode1 (dn1) │ │ datanode2 (dn2) │
│     :5001       │ │     :5002       │ │     :5003       │
│  [dn0-data vol] │ │  [dn1-data vol] │ │  [dn2-data vol] │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

All nodes use **Docker volumes** to persist data across restarts. The name node stores checkpoint and journal files in `/var/lib/rustdfs/names`. Data nodes store raw block files in `/var/lib/rustdfs/data`.

## How to Run

```bash
cd example/persistence
bash run.sh
```

## What Happens

| Phase | Action |
|---|---|
| **1 — Write** | Cluster starts, client writes `small.txt`, `medium.txt`, and `large.txt` |
| **2 — Stop** | Entire cluster is stopped. |
| **3 — Restart** | Cluster restarts. Name node restores the file map from checkpoint/journal. Data nodes re-register and send `BlockReport` RPCs to repopulate block-to-node mappings. |
| **4 — Verify** | Client reads all three files back and diffs them against the originals. |

## Expected Output

```
═══════════════════════════════════════════
 Phase 4 — Verify reads after reconstruction
═══════════════════════════════════════════

── Reading small.txt from DFS ──
  ✓ small.txt matches original

── Reading medium.txt from DFS ──
  ✓ medium.txt matches original

── Reading large.txt from DFS ──
  ✓ large.txt matches original

============================================
 All checks passed! (3 / 3)
============================================
```

## Configuration

Uses the same config as the basic example (`rdfsconf.toml`):
- `replica-count = 2` — each block has 3 copies (1 primary + 2 replicas)
- `block-size = "4MB"` — `large.txt` (12 MB) splits into 3 blocks
- `heartbeat-interval = 3` — block reports are sent after registration completes
