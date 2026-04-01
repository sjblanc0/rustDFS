# Block Report Demo — Full Cluster Restart

Demonstrates data recovery after a **complete cluster restart**. After the name node and all data nodes are stopped, the name node starts with an empty namespace. Data nodes re-register and send block reports containing file metadata, allowing the name node to reconstruct the file map and serve reads.

## Cluster Layout

```
                ┌───────────────────┐
                │  namenode  (nn0)  │  :5000   (no persistent volume)
                └─────────┬─────────┘
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ datanode0 (dn0) │ │ datanode1 (dn1) │ │ datanode2 (dn2) │
│     :5001       │ │     :5002       │ │     :5003       │
│  [dn0-data vol] │ │  [dn1-data vol] │ │  [dn2-data vol] │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

Data nodes use **Docker volumes** to persist block data + metadata across restarts. The name node has no volume — it starts fresh each time to prove reconstruction works.

## How to Run

```bash
cd example/blockreport
bash run.sh
```

## What Happens

| Phase | Action |
|---|---|
| **1 — Write** | Cluster starts, client writes `small.txt`, `medium.txt`, and `large.txt` |
| **2 — Stop** | Entire cluster is stopped. Name node container is removed (empty namespace on restart). |
| **3 — Restart** | Cluster restarts. Data nodes re-register and send `BlockReport` RPCs with file metadata. Name node reconstructs the file map. |
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
