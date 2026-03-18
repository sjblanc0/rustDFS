# rustDFS Examples

This directory contains example setups that demonstrate
rustDFS behavior.

Shared test files

- The examples use a shared set of sample files stored in `example/files`.
- Primary shared files:
  - `small.txt` — 16 KB (1 × message size); tests a single-message transfer.
  - `medium.txt` — 64 KB (4 × message size, 1 block); tests multi-message single-block transfers.
  - `large.txt` — 12 MB (3 blocks); tests multi-block transfers and, in the leasing example, holds a write lease during throttled uploads.

Per-example notes

- `basic/` — writes and reads `small.txt`, `medium.txt`, and `large.txt`, then diffs each.
- `leasing/` — lease semantics tests that use `small.txt` and `large.txt`.

To run an example, `cd` into the appropriate subdir and run:

```bash
docker compose up --build
```

Cleanup:

```bash
docker compose down -v
```
