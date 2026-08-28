# Customer Events v2 — design spec

A customer-event pipeline built on ClickHouse: **Amazon MSK → ClickHouse Kafka engine
table → per-batch AES-256-GCM decryption via a 5-minute rotating vault key → live queries
over nested JSON paths and array junctions**, targeting 20,000 events/s with sub-second
end-to-end latency and provable zero record loss.

## 📄 [Read the full spec →](SPEC.md)

## 💬 [Comment on it → Pull Request #1](../../pull/1)

Review happens in the pull request. Open the **Files changed** tab and click any line to
leave a comment — that anchors your remark to the exact text. Use
[Issues](../../issues) for concerns that outlive this draft, and
[Discussions](../../discussions) for open-ended questions.

## What's measured vs. proposed

Measured on a live ClickHouse service (§14.1, §16):

| | |
|---|---|
| Commit-lag p99 | **259 ms** at 19,131 events/s |
| Throughput ceiling reached | **~108,000 events/s** |
| Decrypt calls | **200/s serving 20,000 events/s** — 100× amortisation |
| gzip ratio on realistic payloads | **7.8×** |

One question is deliberately still open — see **A1** in §15: whether the Kafka engine commits
offsets only *after* materialized-view target writes succeed. It underpins the zero-loss
design and needs a broker reachable from ClickHouse Cloud to settle.

Cost is out of scope for this document.
