# Implementation verification log

These are local work items, not GitHub issue records. No remote issues have been created or closed yet because GitHub access is not connected.

## EW-01 — Atomic page/checkpoint persistence
Implemented in checkpoint-store/eventweave_store.py. Verified rollback after a Python exception and after a child process exits inside the transaction. The reopened SQLite journal contains neither partial events nor an advanced cursor.

## EW-02 — Retry and concurrent-collector consistency
Implemented event-ID deduplication, conflicting-payload rejection and cursor compare-and-swap. Verified repeated delivery, full-page rollback on conflict and rejection of stale collectors.

## EW-03 — Explicit bounded coverage
Implemented fixed ledger ranges, retention-window checks, bounded pagination, cursor resume without ledger parameters and incomplete manifests when the page limit is reached. Verified future-range, retention-gap and out-of-order response failures.

## EW-04 — Deterministic replay and read-only console
Implemented canonical ordered journal hashing, contract-count projection and a local manifest viewer. Verified identical hashes after different insertion orders; HTTP tests reject unknown paths, directory traversal and POST. Visual browser verification remains pending because browser control is unavailable.

Run from the parent workspace:

```sh
python3 -m unittest discover -s fault-scenarios -v
```

After publishing, link these verified changes and test results to their actual GitHub issues before closing them. Do not imply these local IDs are public issue numbers.
