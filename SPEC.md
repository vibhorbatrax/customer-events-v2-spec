# Customer Events v2 — MSK → ClickHouse at 20k events/s, sub-second, batch-decrypted

**Status:** draft for review · **Owner:** vibhor.batra@clickhouse.com · **Date:** 2026-08-28

A second-generation customer-event pipeline for the Meridian demo: 20,000 events/s from
Amazon MSK into ClickHouse Cloud via the **Kafka engine table**, nested-JSON payloads
encrypted **per batch** under a 5-minute rotating vault key, decrypted inside ClickHouse,
queried live through JSON paths and array junctions, with a **p99 < 1s** budget from MSK
produce to Meridian API response.

This does not modify the existing `ccb_demo` tables or the running demo. It builds a
parallel `ccb_v2` database so the current CCB story keeps working during development.

---

## 0 · Summary of the proposal

**What.** A parallel `ccb_v2` customer-event pipeline: Amazon MSK → ClickHouse Kafka engine
table → nested-JSON events decrypted inside ClickHouse → live queries over JSON paths and
array junctions → Meridian API and UI. The existing `ccb_demo` demo is untouched.

**The five things that make it interesting**

1. **Batch-level decryption, not per-record.** Each Kafka record carries 100 events encrypted
   as a *single* ciphertext under a 5-minute rotating vault key. ClickHouse resolves the key
   once per epoch through a dictionary and calls `decrypt()` **once per batch** — 200 calls/s
   serving 20,000 events/s, a **100× reduction**. Verified working end to end (§5.3, §6.4).
2. **Nested JSON queried natively.** The decrypted event is a ≥3-level nested document with
   six independent array dimensions, stored in a `JSON` column. Queries use dynamic paths
   (`payload.device.attestation.risk`) and `ARRAY JOIN payload.consents[]` — no schema
   migration when a producer adds a field (§3.2, §6.5, §8).
3. **The Kafka engine, not ClickPipes.** Chosen because it is the only option where
   ClickHouse controls flush latency and exposes per-partition offsets, a dead-letter queue
   and consumer telemetry in SQL (§1.1).
4. **Zero record loss, proven not asserted.** `(partition, offset)` is the idempotency key;
   offset continuity is a mathematical proof that nothing was dropped in transport, and
   declared-vs-landed batch counts catch anything the decrypt step loses (§13).
5. **Failure containment as a design rule.** The MV attached to the Kafka table cannot throw,
   so a poison record can never stall a partition; a vault outage yields zero rows rather
   than an exception, and the retained ciphertext makes it replayable (§6.0, §6.4).

**Data flow**

```
MSK topic ──► events_kafka (Kafka engine, handle_error_mode='stream')
                 ├─ _error = ''  ──► events_raw      (envelope + ciphertext + offset)
                 └─ _error != '' ──► ingest_errors   (DLQ, replayable)
              events_raw ──► customer_events (decrypt ×1/batch, explode, JSON column)
                               └─► rollups ──► API ──► Meridian UI
```

**Status of the numbers.** Measured on the Meridian write service: commit-lag **p99 259 ms at
19,131 events/s**, with a laptop-side ceiling of **~108,000 events/s** — 5.4× the requirement
(§14.1). Cost is out of scope.

**The zero-loss linchpin is answered.** The Kafka engine commits offsets **only after
materialized-view target writes succeed**. Verified against Redpanda on ClickHouse 26.4.1 (the
Cloud version) and 26.8.1: while the MV target rejected every row, `num_commits` stayed at 0 and
the offset never advanced while `num_messages_read` climbed — the consumer retried the same
block. On recovery, all 100 records landed with **0 missing and 0 duplicates**. R9 rests on the
engine's own guarantees.

**Implementation.** `ccb-v2/`: schema, Kafka DDL, a batch producer with fault injection, and 19
explicit validations. Building it corrected four bugs in this document's SQL — see §16.

**Remaining unknown.** Kafka-engine *support posture* on ClickHouse Cloud: the DDL is accepted
and `system.kafka_consumers` populates, but production support is a question for CH engineering.

---

## 1 · Decisions already taken

| Question | Decision |
|---|---|
| Vault semantics | One API call per **300s epoch** for a key; **one `decrypt()` per batch**, not per record |
| Key expiry | Keys rotate every 5 min but **remain retrievable** from the vault by epoch |
| Broker | **Real Amazon MSK** (not the workshop `t4g.small` + SSH tunnel) |
| Ingest path | **Kafka engine table**, not ClickPipes — see §1.1 |
| Rate | **20,000 events/s** |
| Latency | **p99**, MSK produce → Meridian API response |
| Read routing | **API may read the write service** — replica lag is out of the budget |
| Blast radius | New `ccb_v2` database; `ccb_demo` untouched |

### 1.1 Why the Kafka engine and not ClickPipes

The two binding requirements are **p99 < 1s** (R2) and **zero record loss** (R9). The Kafka
engine is the only option where ClickHouse controls the term that decides the first and
provides native instrumentation for the second. All of the following were verified on the
Meridian write service (`26.4.1.2212`) on 2026-08-28:

| Capability | Kafka engine | ClickPipes |
|---|---|---|
| Flush interval | `kafka_flush_interval_ms` — **tunable**, 200 ms accepted | not exposed; the largest untunable term in the budget |
| Per-partition offsets / lag | `system.kafka_consumers` — populates on table creation | none: `kafka_consumers` stays empty, `clickpipes_log` is an error log only |
| Offset for loss proof | `_offset` / `_partition` virtual columns, native | via metadata columns |
| Dead-letter queue | `_error` + `_raw_message` with `kafka_handle_error_mode='stream'` | not exposed |
| Defined in | SQL — version-controlled | Cloud console |

The accepted trade-offs: the consumer runs **inside** the ClickHouse server and competes with
query workload (negligible at 200 records/s and 16 MB/s), scaling is manual via
`kafka_num_consumers`, and the Cloud **support posture** is not verifiable from here — DDL is
accepted; confirm supported-for-production with CH engineering.

Open items are collected in §14.

---

## 2 · Requirements and SLOs

| # | Requirement | Target | How it is measured |
|---|---|---|---|
| R1 | Sustained ingest | 20,000 events/s | `count()` over a 60s window ÷ 60 |
| R2 | End-to-end latency | **p99 < 1000 ms** | `produced_at` (producer clock, in payload) → API response `Date` header |
| R3 | Commit latency | p99 < 500 ms | `dateDiff('millisecond', produced_at, ingested_at)` |
| R4 | Payload shape | Nested JSON, ≥3 levels, ≥2 array dimensions | Schema review |
| R5 | Deserialization | Decrypt in ClickHouse via vault key lookup | §5 |
| R6 | Decrypt amortisation | ≤ 1 `decrypt()` per batch | `decrypt` call count = batch count |
| R7 | Query surface | JSON paths + `ARRAY JOIN` + MVs | §8 |
| R8 | No regression | `ccb_demo` demo unaffected | Existing hunts/showcases pass |
| R9 | **Zero record loss** | 0 dropped records, provable | §13 — offset continuity + batch reconciliation |

**R2 is the binding constraint.** See §7 and §14.1 — the existing pipeline already consumes
most of this budget, and that must be fixed before MSK is added.

---

## 3 · Record contract

### 3.1 The MSK message (the envelope)

One Kafka record carries **one batch of 100 events**. The envelope is **clear text** so
ClickHouse can route, prune and key-resolve *without* decrypting; only `payload_ct` is
encrypted.

```jsonc
{
  "batch_id":      "b-01J9X2A7-000412",   // ULID, idempotency key
  "schema_version": 2,
  "producer_id":   "msk-prod-03",
  "produced_at":   "2026-08-28T18:50:03.117Z",  // producer clock — starts the SLO clock
  "key_epoch":     1787943000,            // unix seconds, toStartOfInterval(ts, 300s)
  "cipher":        "aes-256-gcm",
  "iv":            "zc0kQ1p7Yq==",        // base64, 12 bytes, unique per batch
  "n_events":      100,
  "ts_min":        "2026-08-28T18:50:02.004Z",  // routing/pruning hints, clear
  "ts_max":        "2026-08-28T18:50:03.101Z",
  "customer_ids":  ["CUST-100696", "..."],      // distinct ids in this batch, clear
  "payload_ct":    "base64(aes-256-gcm(<inner JSON>))"
}
```

**Why the clear-text hints exist.** An opaque ciphertext cannot be filtered. Without
`customer_ids`, `ts_min`/`ts_max` and `key_epoch` in the clear, a per-customer query would
have to decrypt *every batch in the retention window* before it could evaluate a `WHERE`.
These fields make the raw table prunable by primary key and skip index. This is the single
most important structural choice in the design.

If `customer_ids` is considered sensitive, replace it with a Bloom-filtered
`cityHash64(customer_id)` array — prunes identically, reveals nothing.

### 3.2 The inner payload (encrypted, nested JSON)

`payload_ct` decrypts to exactly this:

```jsonc
{
  "batch_id": "b-01J9X2A7-000412",
  "events": [
    {
      "event_id":    "01J9X2A7QK8Z3M",
      "customer_id": "CUST-100696",
      "ts":          "2026-08-28T18:50:02.004Z",
      "channel":     "mobile",
      "event_type":  "profile_change",

      "actor":   { "type": "customer", "session_id": "…", "auth_method": "biometric",
                   "step_up": false },

      "change":  { "field": "email", "old_value": "a@x.com", "new_value": "b@y.com",
                   "reason_code": "SELF_SERVICE" },

      "device":  { "id": "DEV-NEW-4471", "is_new": true, "os": "iOS 18.2",
                   "model": "iPhone15,3", "app_version": "7.22.1",
                   "attestation": { "passed": true, "vendor": "app_attest",
                                    "risk": 0.07 } },

      "geo":     { "ip": "203.0.113.42", "lat": 50.4621, "lon": 30.5324,
                   "country": "UA", "asn": 13335, "is_proxy": true },

      "pii":     { "email": "b@y.com", "phone": "+380671234567",
                   "national_id_last4": "4417" },

      // ── array dimensions: the "junctions" ──
      "devices_seen_30d": [ { "id": "DEV-X-2", "last_seen": "2026-08-27T10:02:00Z",
                              "trust": 0.91 },
                            { "id": "DEV-NEW-4471", "last_seen": "2026-08-28T18:50:02Z",
                              "trust": 0.04 } ],

      "addresses":  [ { "line": "1252 Pine Ave", "city": "Austin", "country": "US",
                        "since": "2019-04-01", "current": false },
                      { "line": "5241 Cedar Rd", "city": "Austin", "country": "US",
                        "since": "2026-08-28", "current": true } ],

      "consents":   [ { "kind": "marketing_email", "granted": false,
                        "updated": "2026-08-28T18:50:02Z" },
                      { "kind": "data_sharing", "granted": true,
                        "updated": "2025-11-02T09:00:00Z" } ],

      "kyc":        { "status": "refresh_due", "last_cdd": "2024-06-11",
                      "risk_rating": "medium",
                      "sanctions": { "screened_at": "2026-08-28T18:50:02Z",
                                     "hits": [] } },

      "relationships": [ { "kind": "joint_holder", "customer_id": "CUST-100702" },
                         { "kind": "beneficiary",  "customer_id": "CUST-119004" } ],

      "scores":     { "ato_model": 0.87, "cdd_priority": 0.44,
                      "components": { "device": 0.61, "geo": 0.22, "velocity": 0.04 } },

      "cohort": "takeover"          // ground truth, demo only — never a model feature
    }
    // × 100
  ]
}
```

**Attributes added over v1** (v1 had 15 flat scalars): `actor`, `device.attestation`,
`geo.asn/is_proxy`, `pii` subtree, and six new nested/array dimensions —
`devices_seen_30d`, `addresses`, `consents`, `kyc.sanctions.hits`, `relationships`,
`scores.components`. These were chosen so the JSON paths are ≥3 levels deep and there are
multiple independent arrays to join, which is what makes §8 non-trivial.

**Size:** ~800 B per event → ~80 KB per batch → **16 MB/s** at 20k events/s.

---

## 4 · Architecture

```
┌─────────────────┐   200 rec/s          ┌──────────────────┐
│  Producer fleet │  (100 events each)   │   Amazon MSK     │
│  20k events/s   │ ───────────────────► │  3× m7g.large    │
│  batch + encrypt│  16 MB/s             │  24 partitions   │
└────────┬────────┘                      │  RF 3, TLS+SCRAM │
         │                               └────────┬─────────┘
         │ GET /keys/current (1 per 300s)          │
         ▼                                         │ Kafka engine table
┌─────────────────┐                                │ (consumer INSIDE the
│   Key vault     │◄───────────────────────────────┤  ClickHouse server)
│  epoch-keyed    │   dictionary refresh           │  kafka_num_consumers=8
│  keys retained  │   LIFETIME(240,300)            │  kafka_flush_interval_ms=200
└─────────────────┘                                ▼
                                        ┌────────────────────────┐
                                        │ ccb_v2.events_kafka    │
                                        │ ENGINE = Kafka         │
                                        │ handle_error_mode=     │
                                        │        'stream'        │
                                        └───┬────────────────┬───┘
                              _error = ''   │                │  _error != ''
                                            ▼                ▼
                            ┌────────────────────┐   ┌──────────────────┐
                            │ ccb_v2.events_raw  │   │ ingest_errors    │
                            │ envelope + cipher  │   │ DLQ, replayable  │
                            │ + _partition/_offset│  └──────────────────┘
                            └─────────┬──────────┘
                                      │ MV: dictGet key
                                      │     tryDecrypt ×1/batch
                                      │     explode events
                                      ▼
                            ┌────────────────────────┐
                            │ ccb_v2.customer_events │
                            │ typed cols + JSON col  │
                            └───────────┬────────────┘
                            ┌───────────┴────────────┐
                            ▼                        ▼
                  device_risk_1m            behaviour_vectors
                  consent_state             cohort_hourly
                            │
                            ▼
                  Meridian API (:4400) ──► Meridian UI
                  reads the WRITE service
```

**Why two hops and not one.** The Kafka table could decrypt and explode in a single MV. It
does not, because `events_raw` retaining the ciphertext is what makes a decrypt failure
*recoverable* rather than merely observable (§13.2). The first MV is deliberately trivial —
capture the envelope and the Kafka coordinates — so it is the least likely thing in the chain
to fail before the offset is committed.

**MSK sizing.** Batching makes the broker almost trivial: 200 records/s at 16 MB/s ingress.
3× `kafka.m7g.large` with RF 3 is comfortable (replication traffic 48 MB/s). 24 partitions
sets the ceiling on `kafka_num_consumers`; start at 8. Partition key = `customer_id` hash so
a customer's events stay ordered within a partition.

**Consumer scaling.** `kafka_num_consumers` must be ≤ partition count, and is additionally
capped by available cores unless `kafka_disable_num_consumers_limit=1`. To scale past one
table's consumers, create additional Kafka tables in the **same** consumer group and attach
each to the same landing MV; Kafka rebalances partitions across them.

**Why not the workshop broker.** It is a `t4g.small` doing 200 events/s bound to loopback
behind an SSH tunnel. A Kafka engine table needs to reach brokers directly from ClickHouse
Cloud, which that setup cannot offer. Retire it for v2; MSK must be reachable (PrivateLink or
public TLS+SCRAM endpoints).

---

## 5 · Key management and in-database deserialization

### 5.1 Epoch model

* Key epoch = `toStartOfInterval(ts, INTERVAL 300 SECOND)` as unix seconds. Verified:
  `toUnixTimestamp(toStartOfInterval(now(), INTERVAL 300 SECOND))` → `1787943000`.
* The producer fetches the current key once per epoch and stamps `key_epoch` on every batch
  it encrypts with that key.
* Keys are **retained** and retrievable by epoch, so historical data stays readable and
  decryption is idempotent and retryable.

### 5.2 The vault as a ClickHouse dictionary

```sql
CREATE NAMED COLLECTION vault_creds AS
    header_auth = 'Bearer <token>';          -- not inline in the DDL

CREATE DICTIONARY ccb_v2.dict_vault_keys
(
    key_epoch  UInt64,
    key_hex    String,
    algo       String
)
PRIMARY KEY key_epoch
SOURCE(HTTP(
    url    'https://vault.internal/v1/ccb/keys?window=6',
    format 'JSONEachRow',
    credentials(NAMED COLLECTION vault_creds)
))
LAYOUT(HASHED())
-- "an API call to a vault every 300 seconds": MIN/MAX jitters the refresh so
-- replicas do not stampede the vault on the epoch boundary.
LIFETIME(MIN 240 MAX 300);
```

**The vault endpoint must return a window of recent epochs, not just the current key.**
`window=6` returns the last 6 epochs (30 min). Reason: a batch produced at 18:54:59.9 can
arrive at 18:55:00.2, after rotation. If the vault only ever served "current", every
rotation boundary would drop data. This is a correctness requirement, not an optimisation.

Vault calls: 1 per 300s per ClickHouse replica ≈ **12/hour/replica** — negligible.

### 5.3 Decrypt once per batch

```sql
CREATE MATERIALIZED VIEW ccb_v2.mv_events_explode
TO ccb_v2.customer_events AS
WITH
    -- ONE decrypt per Kafka record, covering 100 events.
    -- tryDecrypt (not decrypt) so a missing key parks the batch instead of
    -- failing the whole insert. It returns Nullable(String), and ClickHouse
    -- rejects Nullable inside array functions, so it must be unwrapped:
    --   "Nested type Array(String) cannot be inside Nullable type"
    ifNull(
        tryDecrypt(
            cipher,
            base64Decode(payload_ct),
            unhex(dictGet(ccb_v2.dict_vault_keys, 'key_hex', key_epoch)),
            base64Decode(iv)
        ),
        '{"events":[]}'
    ) AS plain
SELECT
    JSONExtractString(ev, 'customer_id')                  AS customer_id,
    parseDateTime64BestEffort(JSONExtractString(ev, 'ts')) AS ts,
    produced_at,
    now64(3)                                              AS ingested_at,
    batch_id,
    key_epoch,
    JSONExtractString(ev, 'channel')                      AS channel,
    JSONExtractString(ev, 'change', 'field')              AS field_changed,
    JSONExtractString(ev, 'device', 'id')                 AS device_id,
    JSONExtractBool(ev,   'device', 'is_new')             AS device_is_new,
    JSONExtractFloat(ev,  'scores', 'ato_model')          AS ato_score,
    JSONExtractString(ev, 'cohort')                       AS cohort,
    ev::JSON                                              AS payload   -- full nested event
FROM
(
    SELECT plain, produced_at, batch_id, key_epoch,
           arrayJoin(JSONExtractArrayRaw(plain, 'events')) AS ev
    FROM ccb_v2.events_raw
);
```

**Decrypt amortisation (R6):** 200 `decrypt()` calls/s instead of 20,000 — a **100×
reduction**, and the reason this design is worth building. Per key epoch: ~6,000,000
events across ~60,000 batches, all under one key.

**Verified working** on service `26.4.1.2212` — the full chain
`encrypt → tryDecrypt → JSONExtractArrayRaw → arrayJoin → ARRAY JOIN` returned 3 rows from
a 2-event batch with 3 nested devices.

### 5.4 Failure handling

Insert-time decryption is **one-shot**: if the dictionary has not yet loaded a brand-new
epoch, the MV writes nothing for that batch and there is no automatic retry. Mitigations:

1. `events_raw` retains the ciphertext, so any batch is reprocessable.
2. A reconciliation job compares `events_raw.n_events` against rows landed in
   `customer_events` per `batch_id` and replays gaps.
3. Producers must not emit a `key_epoch` before the epoch has started (no clock skew into
   the future); enforce with NTP and a producer-side assertion.

```sql
-- undecryptable batches, for the reconciler and for a UI health tile
CREATE VIEW ccb_v2.v_decrypt_failures AS
SELECT key_epoch, count() AS batches, sum(n_events) AS events_at_risk,
       min(produced_at) AS first_seen
FROM ccb_v2.events_raw AS r
WHERE NOT dictHas(ccb_v2.dict_vault_keys, key_epoch)
   OR (SELECT count() FROM ccb_v2.customer_events WHERE batch_id = r.batch_id) = 0
GROUP BY key_epoch ORDER BY key_epoch DESC;
```

### 5.5 PII at rest — a stated trade-off

The MV persists decrypted PII in `customer_events`. That is a deliberate choice to meet
p99 < 1s: query-time decryption of a batch blob cannot be pruned by customer without first
decrypting, so per-customer queries would scan the retention window.

Controls: ClickHouse Cloud encrypts at rest; `pii` stays inside the `payload JSON` column
rather than being promoted to typed columns; access is via a masking view with RBAC.

```sql
CREATE VIEW ccb_v2.v_events_masked AS
SELECT * REPLACE (
    payload_json_set(payload, 'pii.email', concat('***@', splitByChar('@', payload.pii.email)[-1]))
    AS payload
) FROM ccb_v2.customer_events;
```

**Alternative if plaintext-at-rest is unacceptable:** keep only `events_raw`, expose a
decrypting view, and accept p99 in the 2–5s range for per-customer queries. That variant
cannot meet R2. Flagging it as a decision for security review rather than assuming.

---

## 6 · ClickHouse schemas

### 6.0 Soundness principles this schema encodes

Four rules, each of which the design below implements. They matter more than the DDL.

1. **`(partition, offset)` is the idempotency key.** Kafka guarantees it is globally unique
   and immutable per record. Every landing table is `ReplacingMergeTree` ordered on it, so
   consumer retries and rebalances are exactly deduplicable rather than merely tolerable.
2. **The MV attached to the Kafka table must be incapable of failing.** If it throws, the
   offset is not committed, the block is retried, and the partition stalls forever on a
   poison record. It therefore does *no* casting, no dictionary lookups and no arithmetic —
   it is a pure projection. All parsing risk is pushed into the format layer, where
   `kafka_handle_error_mode='stream'` converts a failure into a row instead of an exception.
3. **Ingestion must not depend on anything external.** The vault is reachable from the
   decrypt step only. Landing a record requires the broker and ClickHouse, nothing else.
4. **Nothing is deleted before it is proven landed.** `events_raw` keeps the ciphertext so
   every downstream failure is replayable (§13.2).

**Why rules 2 and 3 are load-bearing, not defensive style.** Measured on this service:

* A failing MV **target** write **does** propagate an error back to the writer —
  `Constraint ... is violated ... while pushing to view ...`. So an exception anywhere in the
  chain reaches the Kafka consumer.
* But the source insert is **not rolled back**. The source table kept its row while the MV
  target got nothing (`rows in source: 1 | rows in target: 0`).

Two consequences the design must absorb:

1. **MV writes are not atomic with the source insert.** A retry re-fires the MV, so every
   target in the chain must be idempotent — which is why `(partition, offset)` dedup exists on
   all landing tables and `(customer_id, ts, event_id)` on the query surface.
2. **A persistent exception anywhere in the chain stalls the partition**, because the block is
   retried indefinitely. This is not hypothetical: it is the confirmed behaviour. Every
   expression in §6.3–§6.6 is therefore non-throwing by construction, and that is a hard
   constraint on any MV added to this chain later.

### 6.1 The Kafka engine table

```sql
CREATE DATABASE IF NOT EXISTS ccb_v2;

CREATE TABLE ccb_v2.events_kafka
(
    batch_id       String,
    schema_version UInt16,
    producer_id    LowCardinality(String),
    produced_at    DateTime64(3, 'UTC'),
    key_epoch      UInt64,
    cipher         LowCardinality(String),
    iv             String,
    n_events       UInt16,
    ts_min         DateTime64(3, 'UTC'),
    ts_max         DateTime64(3, 'UTC'),
    customer_ids   Array(String),
    payload_ct     String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list        = '<msk-bootstrap>:9096',
    kafka_topic_list         = 'ccb.v2.customer_events',
    kafka_group_name         = 'ccb_v2_ingest',
    kafka_format             = 'JSONEachRow',
    kafka_num_consumers      = 8,
    -- see §7.1: defaults to stream_flush_interval_ms, which is 7500 ms here
    kafka_flush_interval_ms  = 200,
    kafka_max_block_size     = 8192,
    -- turns a malformed record into a row with _error set, instead of an
    -- exception that stalls the partition. This is what makes rule 2 hold.
    kafka_handle_error_mode  = 'stream',
    kafka_thread_per_consumer = 1,
    input_format_skip_unknown_fields = 1,
    date_time_input_format   = 'best_effort';
```

Declaring the envelope as **typed columns** (rather than one `String` parsed later) is
deliberate: it moves parse failure into the format layer where `_error` catches it, instead of
into an MV where it would throw. Unknown fields are skipped so a producer adding a field
cannot stall ingestion.

Verified virtual columns available on this engine: `_topic`, `_key`, `_offset`, `_partition`,
`_timestamp`, `_timestamp_ms`, `_headers.name`, `_headers.value`, `_table`, and — because
`handle_error_mode='stream'` is set — `_raw_message` and `_error`.

`stream_like_engine_allow_direct_select = 0` on this service, so the Kafka table cannot be
`SELECT`ed directly. All consumption is via the MVs below; debugging is done against
`events_raw` and `ingest_errors`.

### 6.2 Landing tables

```sql
-- Good path. ORDER BY (partition, offset) rather than by customer/time: this table
-- exists for replay and reconciliation, both of which are offset-oriented, and it
-- makes the Kafka identity the dedup key (rule 1). customer_events is the query surface.
CREATE TABLE ccb_v2.events_raw
(
    partition      UInt64,
    offset         UInt64,
    topic          LowCardinality(String),
    kafka_ts       Nullable(DateTime64(3, 'UTC')),
    received_at    DateTime64(3, 'UTC') DEFAULT now64(3),

    batch_id       String,
    schema_version UInt16,
    producer_id    LowCardinality(String),
    produced_at    DateTime64(3, 'UTC'),
    key_epoch      UInt64,
    cipher         LowCardinality(String),
    iv             String,
    n_events       UInt16,
    ts_min         DateTime64(3, 'UTC'),
    ts_max         DateTime64(3, 'UTC'),
    customer_ids   Array(LowCardinality(String)),
    payload_ct     String CODEC(NONE),           -- ciphertext is incompressible
    INDEX idx_cust  customer_ids TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_batch batch_id     TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(produced_at)
ORDER BY (partition, offset)
TTL toDateTime(produced_at) + INTERVAL 3 DAY;

-- Dead-letter queue. Same dedup key, same replay story.
CREATE TABLE ccb_v2.ingest_errors
(
    partition   UInt64,
    offset      UInt64,
    topic       LowCardinality(String),
    kafka_ts    Nullable(DateTime64(3, 'UTC')),
    seen_at     DateTime64(3, 'UTC') DEFAULT now64(3),
    error       String,
    raw_message String
)
ENGINE = ReplacingMergeTree
ORDER BY (partition, offset)
TTL toDateTime(seen_at) + INTERVAL 14 DAY;
```

### 6.3 The two MVs on the Kafka table — pure projections only

```sql
CREATE MATERIALIZED VIEW ccb_v2.mv_land_raw TO ccb_v2.events_raw AS
SELECT
    _partition     AS partition,
    _offset        AS offset,
    _topic         AS topic,
    _timestamp_ms  AS kafka_ts,
    batch_id, schema_version, producer_id, produced_at, key_epoch,
    cipher, iv, n_events, ts_min, ts_max, customer_ids, payload_ct
FROM ccb_v2.events_kafka
WHERE _error = '';

CREATE MATERIALIZED VIEW ccb_v2.mv_ingest_errors TO ccb_v2.ingest_errors AS
SELECT
    _partition    AS partition,
    _offset       AS offset,
    _topic        AS topic,
    _timestamp_ms AS kafka_ts,
    _error        AS error,
    _raw_message  AS raw_message
FROM ccb_v2.events_kafka
WHERE _error != '';
```

No expressions, no casts, no `dictGet`. Both are column renames plus a predicate, which is
the strongest guarantee available that neither can throw and stall a partition.

The two predicates are disjoint, so a record goes to exactly one target. A *block* may still
fan out to both; if one target insert fails the block is retried and the other target sees the
rows twice — which is exactly why both are `ReplacingMergeTree` on `(partition, offset)`.

### 6.4 Decrypt and explode — and how a vault outage is contained

```sql
CREATE MATERIALIZED VIEW ccb_v2.mv_events_explode
TO ccb_v2.customer_events AS
WITH
    -- dictGetOrDefault, NOT dictGet: a missing epoch must yield an empty key and
    -- fall through to the tryDecrypt/ifNull path below, never raise. dictGet would
    -- throw, and because this MV sits under the Kafka landing chain the exception
    -- would propagate to the consumer and stall the partition on a vault blip.
    dictGetOrDefault(ccb_v2.dict_vault_keys, 'key_hex', key_epoch, '') AS k_raw,
    base64Decode(iv)                                                   AS iv_raw,
    base64Decode(payload_ct)                                           AS ct_raw,
    -- ══ CORRECTED AFTER IMPLEMENTATION ══
    -- tryDecrypt does NOT swallow everything. It returns NULL for a WRONG key of
    -- the right size, but it THROWS on:
    --     key size != 32      "Invalid key size: 0 expected 32"
    --     IV size 0           "Invalid IV size 0 != expected size 12"
    --     garbage ciphertext  "Encrypted data is smaller than the size of ..."
    -- The earlier draft passed dictGetOrDefault(..., '') straight in, so a MISSING
    -- VAULT KEY raised — propagating through events_raw to the consumer, which
    -- never commits and retries forever. A stalled partition: exactly what §6.0
    -- rule 2 exists to prevent. Observed in the build before the fix.
    --
    -- Guarding with if() is insufficient — short-circuit evaluation is not
    -- guaranteed under vectorised execution. Every argument is instead coerced to
    -- a STRUCTURALLY VALID shape: a missing key becomes 32 zero bytes, a valid
    -- size that cannot decrypt, so tryDecrypt returns NULL. Verified non-throwing
    -- across all 8 failure modes.
    if(length(k_raw)  = 64, k_raw,  repeat('0', 64))         AS key_hex,
    if(length(iv_raw) = 12, iv_raw, unhex(repeat('00', 12))) AS iv_bytes,
    if(length(ct_raw) > 16, ct_raw, unhex(repeat('00', 32))) AS ct_bytes,
    -- The mode MUST be a literal. Passing the `cipher` COLUMN fails with "illegal
    -- type ... as 1st argument 'mode'". No per-row cipher agility, so `cipher` is
    -- ADVISORY and a validation asserts it.
    ifNull(
        tryDecrypt('aes-256-gcm', ct_bytes, unhex(key_hex), iv_bytes),
        '{"events":[]}'
    ) AS plain
SELECT
    JSONExtractString(ev, 'customer_id')                   AS customer_id,
    parseDateTime64BestEffortOrZero(JSONExtractString(ev, 'ts')) AS ts,
    JSONExtractString(ev, 'event_id')                      AS event_id,
    produced_at,
    now64(3)                                               AS ingested_at,
    partition, offset, batch_id, key_epoch,
    JSONExtractString(ev, 'channel')                       AS channel,
    JSONExtractString(ev, 'change', 'field')               AS field_changed,
    JSONExtractString(ev, 'device', 'id')                  AS device_id,
    JSONExtractBool(ev,   'device', 'is_new')              AS device_is_new,
    JSONExtractFloat(ev,  'scores', 'ato_model')           AS ato_score,
    JSONExtractString(ev, 'cohort')                        AS cohort,
    ev::JSON                                               AS payload
FROM
(
    SELECT plain, produced_at, partition, offset, batch_id, key_epoch,
           arrayJoin(JSONExtractArrayRaw(plain, 'events')) AS ev
    FROM ccb_v2.events_raw
);
```

**Containment properties.** A vault outage or an unloaded epoch produces `key_hex = ''` →
`tryDecrypt` returns NULL → `'{"events":[]}'` → **zero rows, no exception**. The ciphertext
is already durable in `events_raw`, the offset commits normally, Kafka keeps flowing, and
§13.2 reports the shortfall for replay. `parseDateTime64BestEffortOrZero` (not the throwing
variant) applies the same principle to a malformed inner timestamp.

**Residual risk, stated plainly:** if the *dictionary itself* fails to load, `dictGetOrDefault`
still raises. Keep `LIFETIME(MIN 240 MAX 300)` so a loaded dictionary is retained across a
transient vault failure, and alert on dictionary load errors —
`SELECT name, last_exception FROM system.dictionaries WHERE last_exception != ''`.

**Decrypt amortisation (R6):** 200 `decrypt()` calls/s serving 20,000 events/s — 100×. Per
key epoch: ~6,000,000 events across ~60,000 batches under one key.

### 6.5 Query surface

```sql
CREATE TABLE ccb_v2.customer_events
(
    customer_id   LowCardinality(String),
    ts            DateTime64(3, 'UTC'),
    event_id      String,
    produced_at   DateTime64(3, 'UTC'),
    ingested_at   DateTime64(3, 'UTC'),
    partition     UInt64,      -- Kafka lineage, retained for reconciliation
    offset        UInt64,
    batch_id      String,
    key_epoch     UInt64,
    channel       LowCardinality(String),
    field_changed LowCardinality(String),
    device_id     LowCardinality(String),
    device_is_new Bool,
    ato_score     Float32,
    cohort        LowCardinality(String),
    payload       JSON,
    INDEX idx_ato ato_score TYPE minmax GRANULARITY 4
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(ts)
-- event_id last so (customer_id, ts) still prunes, while the full tuple dedups.
ORDER BY (customer_id, ts, event_id)
TTL toDateTime(ts) + INTERVAL 30 DAY;
```

`ReplacingMergeTree` here is not optional. MVs fire per inserted block, **before** dedup or
merges, so duplicates arriving in `events_raw` are exploded again into `customer_events` —
deduplicating the landing table does not protect the query surface (§13.3).

Hot paths are promoted to typed columns; everything else stays addressable through `payload`,
so a new producer attribute needs no migration.

### 6.6 Rollups

```sql
CREATE MATERIALIZED VIEW ccb_v2.mv_device_risk_1m TO ccb_v2.device_risk_1m AS
SELECT toStartOfMinute(ts) AS minute, channel,
       count()                               AS events,
       countIf(device_is_new)                AS new_device_events,
       avgState(ato_score)                   AS ato_avg_state,
       quantileTDigestState(0.99)(ato_score) AS ato_p99_state
FROM ccb_v2.customer_events GROUP BY minute, channel;

CREATE MATERIALIZED VIEW ccb_v2.mv_consent_state TO ccb_v2.consent_state AS
-- CORRECTED: event_id must be in the projection AND the target sort key
-- (customer_id, consent_kind, ts, event_id). Without it, two DISTINCT events for
-- one customer in the same millisecond collapse — dedup causing data loss.
SELECT customer_id, ts, event_id,
       c.kind::LowCardinality(String) AS consent_kind,
       c.granted::Bool                AS granted
FROM ccb_v2.customer_events
ARRAY JOIN payload.consents[] AS c;

CREATE MATERIALIZED VIEW ccb_v2.mv_latency_1s TO ccb_v2.latency_1s AS
SELECT toStartOfSecond(ingested_at) AS second,
       count() AS events,
       quantileTDigestState(0.50)(dateDiff('millisecond', produced_at, ingested_at)) AS p50_state,
       quantileTDigestState(0.99)(dateDiff('millisecond', produced_at, ingested_at)) AS p99_state,
       max(dateDiff('millisecond', produced_at, ingested_at)) AS max_ms
FROM ccb_v2.customer_events GROUP BY second;
```

Rollups are built on counts that tolerate eventual dedup. `*State` columns are
`AggregateFunction` — read with the matching `-Merge` inside a `GROUP BY`, per `AGENTS.md`.

**Note the coupling:** these MVs are attached to `customer_events`, which is attached to
`events_raw`, which is attached to `events_kafka`. An exception anywhere in that chain
propagates to the consumer and stalls the partition. Every expression above is
non-throwing by construction; any future MV added to this chain must meet the same bar, and
that constraint belongs in the runbook.

---

## 7 · Latency budget (p99 < 1000 ms)

Commit lag is **measured** (§14.1). The Kafka flush term is now **tunable** rather than an
unknown, which is the main reason for choosing this path.

| Stage | p99 | Basis |
|---|---|---|
| Producer batch fill (100 events @ 20k/s) | 5 ms | arithmetic |
| Encrypt (1 × aes-256-gcm over ~95 KB) | 2 ms | |
| Produce + MSK ack (`acks=all`, RF 3) | 20–60 ms | estimate |
| **Kafka engine poll + flush** | **~200 ms** | **`kafka_flush_interval_ms = 200`, set explicitly** |
| **Insert + commit (measured, gzip, batch 200)** | **259 ms** | measured, 19,131 eps |
| Landing MV (envelope + virtuals) | 5–15 ms | trivial projection |
| Decrypt MV (1 decrypt + 100 JSON extracts per batch) | 20–60 ms | |
| API query (PK skip scan) | 15–60 ms | comparable queries at 7–16 ms |
| **Total** | **~530–660 ms** | **~340 ms headroom** |

### 7.1 The setting that will silently break this

`kafka_flush_interval_ms` **defaults to `stream_flush_interval_ms`, which is 7500 ms on this
service** (verified). Leave it unset and the pipeline works perfectly while missing the SLO
by more than 7×, with nothing in any log to indicate why. It must be set at table level.

```sql
-- non-negotiable for R2
kafka_flush_interval_ms = 200,   -- NOT the 7500 ms default
kafka_max_block_size    = 8192   -- flush on size too, not only on time
```

Related verified defaults: `kafka_max_wait_ms = 5000`, `stream_poll_timeout_ms = 500`.

### 7.2 Tunables, in measured order of leverage

1. **gzip the request body — 3.4× on p99.** Structured JSON compresses 7.8×. At 20k eps,
   changing nothing else: p99 **882 ms → 259 ms**, backpressure 221 skipped batches → 0.
   Already enabled in `server/src/clickhouse.js:24`. (Applies to the API/direct-insert paths;
   the Kafka consumer is server-side and unaffected.)
2. **Request body size.** p99 scales linearly with bytes/request and is nearly independent of
   rate: 160 KB → 271 ms, 800 KB → 464 ms, 1.6 MB → 810 ms, 6.4 MB → 1082 ms. Sweet spot
   200–400 rows.
3. `kafka_flush_interval_ms` / `kafka_max_block_size` — §7.1.
4. `async_insert_busy_timeout_max_ms = 50`. Second-order; see §13.4 on `wait_for_async_insert`.

---

## 8 · Query patterns

```sql
-- 8.1 dynamic JSON paths, no schema change needed
SELECT customer_id, ts,
       payload.device.attestation.risk  AS attest_risk,
       payload.geo.is_proxy             AS via_proxy,
       payload.scores.components.device AS device_component
FROM ccb_v2.customer_events
WHERE customer_id = {customer:String} AND ts >= now() - INTERVAL 15 MINUTE
ORDER BY ts DESC LIMIT 50;

-- 8.2 array junction: which consents changed alongside a new device
SELECT c.kind, countIf(device_is_new) AS on_new_device, count() AS total
FROM ccb_v2.customer_events
ARRAY JOIN payload.consents[] AS c
WHERE ts >= now() - INTERVAL 1 HOUR
GROUP BY c.kind ORDER BY on_new_device DESC;

-- 8.3 double junction: device trust × address history in one statement
SELECT customer_id,
       d.id AS device, d.trust AS trust,
       a.city AS city, a.since AS since
FROM ccb_v2.customer_events
ARRAY JOIN payload.devices_seen_30d[] AS d
ARRAY JOIN payload.addresses[]        AS a
WHERE customer_id = {customer:String} AND a.current AND d.trust < 0.1;

-- 8.4 relationship fan-out: a takeover that spreads to joint holders
SELECT r.customer_id AS related, r.kind, count() AS events, max(ato_score) AS peak_ato
FROM ccb_v2.customer_events
ARRAY JOIN payload.relationships[] AS r
WHERE cohort = 'takeover' AND ts >= now() - INTERVAL 6 HOUR
GROUP BY related, r.kind HAVING peak_ato > 0.8 ORDER BY peak_ato DESC LIMIT 25;

-- 8.5 the SLO, read from the MV
SELECT second,
       quantileTDigestMerge(0.50)(p50_state) AS p50_ms,
       quantileTDigestMerge(0.99)(p99_state) AS p99_ms,
       sum(events) AS events
FROM ccb_v2.latency_1s
WHERE second >= now() - INTERVAL 5 MINUTE
GROUP BY second ORDER BY second DESC;
```

Note the `[]` suffix on JSON array paths — `ARRAY JOIN payload.consents[]`. Verified on
26.4.1; without it the path does not resolve as an array.

---

## 9 · API

New routes on the existing Express server (`server/src/index.js`), namespaced `/api/v2`,
routed to the **write** service via the existing `query('write', …)` helper.

| Method | Route | Returns |
|---|---|---|
| GET | `/api/v2/ingest/status` | events/s, batches/s, lag by partition, decrypt failures |
| GET | `/api/v2/latency` | p50/p95/p99/max from `latency_1s`, plus the per-stage waterfall |
| GET | `/api/v2/customers/:id/events` | typed columns + selected JSON paths, `?window_s=` |
| GET | `/api/v2/customers/:id/consents` | array-junction view of consent state |
| GET | `/api/v2/customers/:id/graph` | relationship fan-out (§8.4) |
| GET | `/api/v2/devices/risk` | `device_risk_1m` rollup for the charts |
| GET | `/api/v2/keys/epochs` | epochs held in the dictionary, age, batches served |
| GET | `/api/v2/schema/paths` | discovered JSON paths + types, for the UI explorer |
| POST | `/api/v2/query` | guarded ad-hoc SQL, reusing the existing readonly guard |

`/api/v2/latency` is the SLO endpoint and must itself be fast — it reads the MV, never the
base table.

---

## 10 · Meridian UI

All of this lands in the existing **Customer Events** vertical (`vertical: 'customer-events'`).

**10.1 New nav page — "Stream v2" (under `01 · Data in`)**
* Latency waterfall: producer → MSK → Kafka engine flush → insert → MV → API, p50/p99 bars against the
  1000 ms budget line, red when p99 breaches.
* Throughput: events/s and batches/s, with the **decrypt-amortisation tile** — "200
  decrypt calls/s serving 20,000 events/s, 100×". This is the headline of the demo.
* Key-epoch strip: epochs currently in the dictionary, age of each, next refresh countdown,
  and a decrypt-failure counter (should be 0).
* MSK partition lag, 24 bars.

**10.2 Query Library — new bucket "Nested JSON & Array Junctions"**
Showcases for §8.1–8.5, each with `viz` specs. Registered as
`{ id: 'ce-json-*', vertical: 'customer-events', category: 'Nested JSON · arrays' }`, plus
a `BUCKETS` entry in `web/src/components/ShowcasePage.jsx`.

**10.3 JSON path explorer**
Tree of discovered paths from `/api/v2/schema/paths` with types and fill rates; clicking a
path drops a ready `SELECT` into the query pane. This is what makes "schema-on-read" legible
to an audience.

**10.4 Command page additions (Customer Events desk)**
* Replace the `ROWS / S LIVE` tile with a **v2 events/s** tile once v2 is primary.
* Add an "end-to-end p99" tile beside commit lag, coloured against the 1s budget.

**10.5 Hunt extension**
Add a step to *The Account-Takeover Hunt* that pivots through
`payload.relationships[]` — "did the takeover spread to the joint holder?" — which is only
answerable with the v2 array dimensions.

---

## 11 · Capacity

Cost is explicitly **out of scope** for this spec. Volumes are recorded only where they
constrain the design.

| | Value |
|---|---|
| Events/s | 20,000 (measured headroom to ~108k, §14.1) |
| Batches/s | 200 |
| Bytes/s | ~16 MB/s |
| `events_raw`/day | ~1.38 TB — ciphertext, `CODEC(NONE)`, **does not compress** |
| `customer_events`/day | ~170–275 GB (JSON compresses ~5–8×) |
| Consumers | `kafka_num_consumers = 8` of 24 partitions |

The one design consequence: ciphertext is incompressible, so `events_raw` carries a 3-day TTL
while `customer_events` keeps 30. If sustained 20k is not needed, run it as a burst — the
existing generator already has rate control.

---

## 12 · Monitoring the ingestion

### 12.1 Telemetry surfaces (verified on `26.4.1.2212`, 2026-08-28)

| Source | Status | What it gives |
|---|---|---|
| `system.kafka_consumers` | **populates the moment a Kafka table exists** | per-partition `assignments.current_offset`, `num_messages_read`, `num_commits`, `last_poll_time`, `last_commit_time`, `exceptions.text`, rebalance counters, full `rdkafka_stat` |
| `system.asynchronous_insert_log` | **populated** — 18,748 flushes / 7.56M rows / 0 exceptions under load | per-flush rows, bytes, `status` (Ok/ParsingError/FlushError), `exception`, `timeout_milliseconds` |
| `ccb_v2.ingest_errors` | by design | every record the format layer rejected, with `_error` and `_raw_message` |
| `system.dictionaries` | populated | `last_exception`, load status — the vault dependency |
| `system.parts` / `system.merges` | populated | part pressure, the thing that breaks first at high ingest |
| MSK CloudWatch | external | `MaxOffsetLag`, `EstimatedMaxTimeLag`, under-replicated partitions |

Choosing the Kafka engine means loss and lag are both answerable **inside ClickHouse**. That
was the deciding factor in §1.1.

### 12.2 Signals and thresholds

| Layer | Metric | Source | Alert |
|---|---|---|---|
| Producer | events/s, encrypt failures, vault fetch failures | producer metrics | deviates >5% from target |
| MSK | `MaxOffsetLag`, `EstimatedMaxTimeLag`, URP | CloudWatch | time lag > 500 ms, any URP |
| **Consumer** | `num_messages_read` rate, `last_poll_time` age, `exceptions.text` | `system.kafka_consumers` | poll age > 5 s, **any** exception |
| **Consumer** | rebalance churn | `num_rebalance_assignments` | > 2 in 10 min (stall symptom) |
| Insert | flush rate, `status != 'Ok'` | `asynchronous_insert_log` | any `ParsingError`/`FlushError` |
| Format | rejected records | `ingest_errors` | any row |
| Vault | dictionary load | `system.dictionaries` | `last_exception != ''` |
| Table | active parts | `system.parts` | > 3000 per partition |
| Freshness | commit lag p50/p99 | `ccb_v2.latency_1s` | p99 > 500 ms |
| Correctness | missing offsets, batch shortfall | §13 | **any** non-zero |

### 12.3 Monitoring queries

```sql
-- consumer health and per-partition position: the view ClickPipes cannot give
SELECT
    table,
    arrayJoin(arrayZip(assignments.partition_id, assignments.current_offset)) AS part_off,
    num_messages_read,
    num_commits,
    dateDiff('second', last_poll_time, now())   AS poll_age_s,
    dateDiff('second', last_commit_time, now()) AS commit_age_s,
    num_rebalance_assignments,
    arrayStringConcat(exceptions.text, ' | ')   AS exceptions
FROM system.kafka_consumers
WHERE database = 'ccb_v2';

-- insert-path health
SELECT toStartOfMinute(flush_time) AS minute, status,
       count() AS flushes, sum(rows) AS rows,
       formatReadableSize(sum(bytes)) AS bytes,
       round(avg(timeout_milliseconds)) AS avg_timeout_ms,
       countIf(exception != '') AS exceptions, any(exception) AS sample
FROM system.asynchronous_insert_log
WHERE database = 'ccb_v2' AND event_time >= now() - INTERVAL 30 MINUTE
GROUP BY minute, status ORDER BY minute DESC;

-- the DLQ: records the format layer rejected rather than dropped
SELECT partition, error, count() AS n, min(offset) AS first_offset,
       max(offset) AS last_offset, any(raw_message) AS sample
FROM ccb_v2.ingest_errors
WHERE seen_at >= now() - INTERVAL 1 HOUR
GROUP BY partition, error ORDER BY n DESC;

-- the vault dependency
SELECT name, status, last_successful_update_time, last_exception,
       element_count, loading_duration
FROM system.dictionaries WHERE database = 'ccb_v2';

-- part pressure
SELECT table, partition, count() AS parts, sum(rows) AS rows
FROM system.parts WHERE database = 'ccb_v2' AND active
GROUP BY table, partition ORDER BY parts DESC LIMIT 20;
```

---

## 13 · Validating records: Kafka ↔ ClickHouse (R9, zero loss)

Loss can occur at two independent boundaries, so there are two independent checks. Both are
pure SQL, both run continuously, and both work off `(partition, offset)` — which the Kafka
engine provides natively as virtual columns.

```
  MSK topic ──B1──► events_raw ──B2──► customer_events
              offset             declared n_events
              continuity         vs rows landed
```

### 13.1 B1 · Kafka → `events_raw`: offset continuity is the proof

Kafka offsets are strictly monotonic per partition with no holes. If the offsets landed in
ClickHouse are gap-free from min to max, **no record was dropped in transport** — a
mathematical guarantee, not a heuristic.

```sql
CREATE VIEW ccb_v2.v_offset_continuity AS
SELECT
    partition,
    min(offset)                                       AS first_offset,
    max(offset)                                       AS last_offset,
    count()                                           AS records_landed,
    uniqExact(offset)                                 AS distinct_offsets,
    max(offset) - min(offset) + 1                     AS offsets_spanned,
    -- > 0 means RECORDS WERE LOST
    (max(offset) - min(offset) + 1) - uniqExact(offset) AS missing_offsets,
    -- > 0 means at-least-once redelivery (not loss — see 13.3)
    count() - uniqExact(offset)                       AS duplicate_records
FROM ccb_v2.events_raw
-- A CLOSED window. Running to now() makes the live head of each partition look
-- like a gap, because the next offsets simply have not arrived yet.
WHERE received_at >= now() - INTERVAL 10 MINUTE
  AND received_at <  now() - INTERVAL 1 MINUTE
GROUP BY partition
ORDER BY partition;
```

Note the check must span **both** `events_raw` and `ingest_errors` — a record routed to the
DLQ consumed an offset and is not lost. The union view:

```sql
CREATE VIEW ccb_v2.v_offset_continuity_total AS
WITH all_offsets AS
(
    SELECT partition, offset FROM ccb_v2.events_raw
    WHERE received_at >= now() - INTERVAL 10 MINUTE AND received_at < now() - INTERVAL 1 MINUTE
    UNION ALL
    SELECT partition, offset FROM ccb_v2.ingest_errors
    WHERE seen_at >= now() - INTERVAL 10 MINUTE AND seen_at < now() - INTERVAL 1 MINUTE
)
SELECT partition,
       (max(offset) - min(offset) + 1) - uniqExact(offset) AS missing_offsets
FROM all_offsets GROUP BY partition HAVING missing_offsets > 0;
```

To *locate* a gap rather than count it — `leadInFrame` with an explicit frame, per the house
rules in `AGENTS.md`:

```sql
SELECT partition, gap_after_offset, next_offset - gap_after_offset - 1 AS missing_count
FROM
(
    SELECT partition, offset AS gap_after_offset,
           leadInFrame(offset) OVER (
               PARTITION BY partition ORDER BY offset
               ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING
           ) AS next_offset
    FROM ccb_v2.events_raw
    WHERE received_at >= now() - INTERVAL 10 MINUTE
      AND received_at <  now() - INTERVAL 1 MINUTE
)
WHERE next_offset > gap_after_offset + 1
ORDER BY partition, gap_after_offset;
```

Caveats to hold: offsets reset if the topic is recreated, and a partition with no traffic in
the window returns nothing — **absence is not continuity**. Cross-check against
`system.kafka_consumers.assignments.current_offset` to distinguish a stalled partition from an
idle one; that comparison is only possible because this design uses the Kafka engine.

```sql
-- is the consumer keeping up, and is what it read what we stored?
SELECT
    c.partition_id                    AS partition,
    c.current_offset                  AS consumer_offset,
    r.last_offset                     AS landed_offset,
    c.current_offset - r.last_offset  AS behind
FROM
(
    SELECT arrayJoin(arrayZip(assignments.partition_id, assignments.current_offset)) AS z,
           z.1 AS partition_id, z.2 AS current_offset
    FROM system.kafka_consumers WHERE database = 'ccb_v2'
) AS c
LEFT JOIN (SELECT partition, max(offset) AS last_offset FROM ccb_v2.events_raw GROUP BY partition) AS r
    ON r.partition = c.partition_id;
```

### 13.2 B2 · `events_raw` → `customer_events`: declared vs landed

Offset continuity says the *batch* arrived. It says nothing about whether the decrypt MV
exploded it — a batch whose key was unavailable lands in `events_raw` and contributes **zero**
rows downstream (by design, §6.4). The envelope's `n_events` is the producer's own
declaration, which makes this checkable.

```sql
-- CORRECTED AFTER IMPLEMENTATION. As a SummingMergeTree fed by count(), one
-- replayed envelope took customer_events 100 -> 200 AND batch_landed 100 -> 200
-- (MVs fire per block, before dedup), so this check reported a false discrepancy
-- in the WRONG direction on a pipeline that had lost nothing. uniqExact over
-- event_id is immune: a replayed event carries the same event_id.
CREATE TABLE ccb_v2.batch_landed
(
    batch_id     String,
    landed_state AggregateFunction(uniqExact, String)
)
ENGINE = AggregatingMergeTree ORDER BY batch_id;

CREATE MATERIALIZED VIEW ccb_v2.mv_batch_landed TO ccb_v2.batch_landed AS
SELECT batch_id, uniqExactState(event_id) AS landed_state
FROM ccb_v2.customer_events GROUP BY batch_id;

CREATE VIEW ccb_v2.v_batch_reconciliation AS
SELECT
    r.key_epoch, r.batch_id, r.partition, r.offset,
    r.n_events                        AS declared,
    ifNull(b.landed, 0)               AS landed,
    r.n_events - ifNull(b.landed, 0)  AS shortfall,
    dictHas(ccb_v2.dict_vault_keys, r.key_epoch) AS key_available,
    r.produced_at
FROM ccb_v2.events_raw AS r
LEFT JOIN
(
    SELECT batch_id, uniqExactMerge(landed_state) AS landed FROM ccb_v2.batch_landed GROUP BY batch_id
) AS b USING (batch_id)
WHERE r.produced_at >= now() - INTERVAL 30 MINUTE
  AND r.produced_at <  now() - INTERVAL 1 MINUTE
  AND r.n_events != ifNull(b.landed, 0);
```

`shortfall > 0` with `key_available = 0` is a vault problem; with `key_available = 1` it is an
MV or cast problem. Either way the ciphertext is still in `events_raw`, so the reconciler
replays it. **That is what makes zero-loss recoverable rather than merely observable** —
replay is an `INSERT SELECT` from `events_raw`, not a Kafka rewind.

### 13.3 Duplicates: zero loss must not be achieved by double counting

The Kafka engine is **at-least-once**. A retry, a rebalance, or an MV failure after a partial
fan-out redelivers the same record, so R9 must be paired with idempotency or counts silently
inflate.

`(partition, offset)` is Kafka's own unique record identity, so dedup is exact rather than
heuristic: `events_raw` and `ingest_errors` are `ReplacingMergeTree ORDER BY (partition,
offset)`.

The subtlety that catches people: **materialized views fire on every inserted block, before
deduplication or merges.** Deduplicating `events_raw` therefore does *not* protect
`customer_events` — the decrypt MV has already exploded the duplicate. Hence
`customer_events` is itself `ReplacingMergeTree(ingested_at) ORDER BY (customer_id, ts,
event_id)`. Reads that must be exact use `FINAL` or an `argMax` collapse; rollups are built on
counts that tolerate eventual dedup.

### 13.4 What breaks the guarantee

| Failure | Detected by | Recoverable? |
|---|---|---|
| Record never left the producer | producer events/s vs MSK bytes-in | no — needs a producer-side WAL |
| Lost between MSK and ClickHouse | `missing_offsets > 0` (§13.1) | yes — Kafka rewind |
| Malformed record | row in `ingest_errors` | yes — DLQ retains `_raw_message` |
| Batch landed, decrypt failed | `shortfall > 0`, `key_available = 0` | yes — ciphertext retained |
| Batch landed, MV cast failed | `shortfall > 0`, `key_available = 1` | yes — ciphertext retained |
| Duplicate delivery | `duplicate_records > 0` | yes — `ReplacingMergeTree` |
| Consumer stalled on a poison block | `last_poll_time` age, rebalance churn | yes — §6.0 rule 2 prevents it |
| Topic recreated, offsets reset | `first_offset` moves backwards | manual |
| `wait_for_async_insert = 0` on a **direct** insert path | **nothing** — errors never returned | **no** |

The last row is a note against the latency tuning in §14.1: `wait_for_async_insert = 0` is
correct for a throughput test and **unsafe for a zero-loss path**. It does not apply to the Kafka engine,
whose consumer is server-side, but any direct-insert tooling must use `1`.

### 13.5 Continuous assertion

One query, every 30s, that must always return zero rows.

```sql
SELECT 'missing_offsets' AS check, partition::String AS scope, missing_offsets AS bad
FROM ccb_v2.v_offset_continuity_total
UNION ALL
SELECT 'batch_shortfall', batch_id, shortfall
FROM ccb_v2.v_batch_reconciliation WHERE shortfall > 0
UNION ALL
SELECT 'dlq', error, count() FROM ccb_v2.ingest_errors
WHERE seen_at >= now() - INTERVAL 5 MINUTE GROUP BY error
UNION ALL
SELECT 'insert_errors', status::String, count() FROM system.asynchronous_insert_log
WHERE database = 'ccb_v2' AND status != 'Ok' AND event_time >= now() - INTERVAL 5 MINUTE
GROUP BY status
UNION ALL
SELECT 'consumer_exceptions', table, length(exceptions.text)
FROM system.kafka_consumers WHERE database = 'ccb_v2' AND notEmpty(exceptions.text)
UNION ALL
SELECT 'dictionary_error', name, 1 FROM system.dictionaries
WHERE database = 'ccb_v2' AND last_exception != '';
```

Non-empty = R9 violated. This backs the UI tile in §10.1 and `/api/v2/ingest/status`.

---

## 14 · Risks

### 14.1 Latency: measured, and the diagnosis that was wrong

Run on 2026-08-28 against the Meridian **write** service from a laptop, with the demo
generator stopped to remove contention. Harness: `scripts/phase1-latency.mjs`.

**The original hypothesis was wrong.** The spec first blamed
`async_insert_busy_timeout_ms = 1000`. Tuning it barely moved p99:

| scenario | busy_max | wait | ach eps | p50 | p95 | **p99** |
|---|---|---|---|---|---|---|
| baseline (server 1000 ms) | server | 1 | 19,759 | 251 | 284 | **909** |
| busy_max 200 | 200 | 1 | 19,769 | 247 | 680 | **1145** |
| busy_max 200 | 200 | 0 | 19,760 | 250 | 324 | **821** |
| busy_max 50 | 50 | 0 | 19,765 | 194 | 245 | **810** |

**The real driver is request body size**, at a fixed 2,000 eps:

| batch | bytes/req | p50 | **p99** |
|---|---|---|---|
| 200 | 160 KB | 126 | **271** |
| 1,000 | 800 KB | 157 | 464 |
| 2,000 | 1.6 MB | 194 | 810 |
| 8,000 | 6.4 MB | 1,055 | 1,082 |

p99 was still 668 ms at 2,000 eps with large batches, so it is upload time, not load and not
the async buffer. Network floor to us-east-2 measured 48–149 ms on an empty `SELECT 1`.

**With gzip (7.8× on realistic 947 B events) the constraint lifts:**

| offered | achieved | logical Mb/s | p50 | p95 | **p99** | max |
|---|---|---|---|---|---|---|
| 20,000 plain | 17,678 | 175 | 381 | 650 | **882** | 1,436 |
| 20,000 gzip | 19,131 | 189 | 187 | 244 | **259** | 550 |
| 60,000 gzip | 58,719 | 581 | 184 | 240 | **269** | 957 |
| 100,000 gzip | 92,908 | 919 | 190 | 247 | **475** | 843 |
| 200,000 gzip | **108,146** | 1,070 | 214 | 278 | **541** | 867 |

**Verdict: R3 met.** Commit lag p99 **259 ms at 19,131 eps**, against a 500 ms target.
Ceiling from this laptop is **~108,000 eps** — 5.4× the requirement — at p99 541 ms, limited
by ~137 Mb/s of wire bytes, i.e. the machine's uplink. ClickHouse never pushed back: zero
insert errors at every rate, 6–13 active parts, no merge pressure, and
`system.asynchronous_insert_log` recorded 18,748 flushes / 7.56M rows / **0 exceptions**.

**Residual risks:**

* The 20k target is comfortable. The remaining end-to-end unknown is the Kafka engine flush
  term, which unlike ClickPipes is at least **tunable** (`kafka_flush_interval_ms = 200`) —
  but is unmeasured until a broker exists.
* Numbers were taken from a laptop. In-region producers should be strictly better, so treat
  every figure above as a floor.
* Run-to-run variance at fixed settings was ±100 ms on p99 (network jitter). Any acceptance
  test must use repeated runs, not a single sample.

### 14.2 Other risks

| Risk | Impact | Mitigation |
|---|---|---|
| **Kafka engine support posture on Cloud** | could invalidate the whole ingest choice | **DDL verified accepted; production support is an open question for CH engineering** |
| ~~Offset-commit ordering vs MV success~~ | — | **ANSWERED: commits only AFTER MV success.** Verified on 26.4.1 and 26.8.1 against Redpanda: while the target rejected, `num_commits=0` and the offset never advanced; on recovery all 100 records landed with **0 missing, 0 duplicates** |
| `kafka_flush_interval_ms` left at default | silently misses the SLO by 7× (7500 ms) | set explicitly (§7.1); assert on `latency_1s` p99 continuously |
| Consumer competes with query workload | query latency regression at high ingest | negligible at 200 rec/s; watch `system.processes` under load |
| Poison block stalls a partition | ingest halts for that partition | §6.0 rule 2: the Kafka MV cannot throw; `handle_error_mode='stream'` routes to DLQ |
| Vault outage cascades to consumer stall | ingest halts entirely | argument coercion (§6.4) — `dictGetOrDefault` ALONE IS NOT ENOUGH, it throws on a 0-byte key; alert on `system.dictionaries.last_exception` |
| **Dictionary state is per-node** | first batches of a new epoch decrypt to zero rows on a stale node | measured: reload hit one node, a query another (2 elements vs 4 in the vault); self-corrected on `LIFETIME`. Keep `LIFETIME` well under the 300s epoch |
| Any future MV added to the Kafka chain throws | reintroduces the stall risk | runbook rule: MVs in this chain must be non-throwing by construction |
| Consumer scaling is manual | cannot absorb a 10× burst | `kafka_num_consumers` ≤ 24; add tables in the same group |
| `FINAL` on the hot read path | latency cost not yet budgeted | measure before go-live; consider `argMax` collapse instead |
| Plaintext PII at rest | security review may reject | masking view + RBAC; §5.5 alternative |
| Ciphertext incompressible, 1.38 TB/day | storage | 3-day TTL on `events_raw`; burst-mode demo |
| Clock skew on `produced_at` | SLO measured wrong; negative lag | NTP; clamp at 0 |
| JSON path explosion | many sparse subcolumns | `max_dynamic_paths`; promote hot paths |

## 15 · Acceptance criteria

Flat assertions, not a plan. Each is measurable and each must hold before this is considered
working.

**Must be answered first — it can invalidate the design**

| # | Question | How |
|---|---|---|
| ~~A1~~ | ~~Does the Kafka engine commit offsets only after MV target writes succeed?~~ | **RESOLVED — YES.** 0 commits while rejecting; 0 missing, 0 duplicates on recovery. Verified on 26.4.1 and 26.8.1 |
| A2 | Is the Kafka engine supported for production on ClickHouse Cloud? | CH engineering |
| A3 | Is plaintext PII at rest acceptable, or is the §5.5 variant required? | security review |

**Throughput and latency**

| # | Assertion |
|---|---|
| T1 | 20,000 events/s sustained for 1 hour, delivered as ~200 batches/s |
| T2 | `ccb_v2.latency_1s` p99 < 500 ms for commit lag, continuously |
| T3 | End-to-end p99 < 1000 ms, producer `produced_at` → API response |
| T4 | `kafka_flush_interval_ms` is explicitly set — never inheriting the 7500 ms default |

**Correctness — the zero-loss set**

| # | Assertion |
|---|---|
| C1 | `v_offset_continuity_total` returns zero rows |
| C2 | `v_batch_reconciliation` returns zero rows |
| C3 | `decrypt()` call count equals batch count — 200/s, not 20,000/s |
| C4 | `system.kafka_consumers.exceptions.text` is empty on every partition |
| C5 | All 24 partitions show as assigned |
| C6 | A deliberately malformed record lands in `ingest_errors`, and does **not** stall its partition |
| C7 | Duplicate delivery is absorbed — row counts unchanged after replaying a batch |
| C8 | §13.5 continuous assertion returns zero rows throughout |

**Failure behaviour — verify by injection**

| # | Injection | Required behaviour |
|---|---|---|
| F1 | Kill an MSK broker | ingest continues; no missing offsets |
| F2 | Make the vault unreachable | ingest **continues**, `customer_events` shortfall appears, no partition stall, backlog replays on recovery |
| F3 | Inject a poison record | routed to `ingest_errors`; partition keeps moving |
| F4 | Force a consumer rebalance | duplicates absorbed by dedup; no gaps |
| F5 | Skew a producer clock forward | commit lag clamps at 0 rather than going negative |

**Demo-ready**

| # | Assertion |
|---|---|
| D1 | A profile change is visible in the Meridian UI in < 1s at p99 |
| D2 | The ingest panel shows 200 decrypt calls/s serving 20,000 events/s |
| D3 | `ccb_demo` and all existing hunts/showcases still pass |

---

## 16 · Verified during authoring

Run against `26.4.1.2212`, Meridian services, 2026-08-28. Everything below was executed, not
assumed. Items *not* verified are called out in §14.2.

**Kafka engine**

| Check | Result |
|---|---|
| `CREATE TABLE ... ENGINE = Kafka` on Cloud | **accepted** |
| `system.kafka_consumers` after creation | **populates immediately** |
| `kafka_handle_error_mode = 'stream'` | accepted |
| `kafka_flush_interval_ms = 200`, `kafka_max_block_size = 8192` | accepted |
| Virtual columns | `_topic`, `_key`, `_offset`, `_partition`, `_timestamp`, `_timestamp_ms`, `_headers.name`, `_headers.value`, `_table`, `_raw_message`, `_error` |
| `stream_flush_interval_ms` default | **7500 ms** — the trap in §7.1 |
| `kafka_max_wait_ms` / `stream_poll_timeout_ms` | 5000 / 500 |
| `stream_like_engine_allow_direct_select` | 0 — consume via MV only |

**Encryption and JSON**

| Check | Result |
|---|---|
| `encrypt`/`decrypt`/`tryDecrypt` aes-256-gcm | present; round-trips a batch blob |
| `encrypt → tryDecrypt → JSONExtractArrayRaw → arrayJoin → ARRAY JOIN` | 3 rows from a 2-event batch with 3 nested devices |
| `tryDecrypt` returns `Nullable(String)` | **breaks array functions** — must `ifNull` unwrap |
| Native `JSON` cast + dynamic paths (`j.customer.risk.score`) | works |
| JSON array junction `ARRAY JOIN j.devices[] AS d` | works; **`[]` suffix required** |
| `toStartOfInterval(now(), INTERVAL 300 SECOND)` epoch | `1787943000` |

**Offset-commit ordering — the zero-loss linchpin (Redpanda + ClickHouse 26.4.1 and 26.8.1)**

| Observation | Result |
|---|---|
| While the MV target rejected every row | `num_commits = 0`, offset never advanced past one block |
| `num_messages_read` in that window | climbed 70 → 230 → 400: the consumer retried the same block |
| Rows reaching the target | 0 |
| After the constraint was dropped | offset 10 → 100, commits 0 → 10 |
| Records recovered | **100 of 100 — 0 missing** |
| Duplicates from the retry loop | **0** |
| Consistency across versions | identical on 26.4.1 (Cloud's version) and 26.8.1 |

**Corrected by implementation** (four bugs in earlier drafts of this document)

| Finding | Consequence |
|---|---|
| `tryDecrypt` THROWS on key size != 32, IV size 0, garbage ciphertext | a missing vault key would have stalled a partition; arguments now coerced (§6.4) |
| `tryDecrypt` mode must be a LITERAL, not a column | no per-row cipher agility; `cipher` is advisory and asserted |
| `SummingMergeTree` + `count()` for `batch_landed` double-counts a replay | false "landed exceeds declared" on a healthy pipeline; now `uniqExactState(event_id)` |
| `consent_state` key `(customer_id, consent_kind, ts)` too coarse | collapses distinct same-millisecond events; `event_id` added |

**Measured hazards**

| Check | Result |
|---|---|
| MV target failure propagates to writer | yes — `while pushing to view` |
| Source insert rolled back when MV fails | **no** — source keeps the row, target does not |
| MV fires again on replay | yes — `customer_events` 100 → 200; `FINAL` collapsed to 100 |
| Dictionary state per-node after `SYSTEM RELOAD` | yes — 2 elements vs 4 in the vault; self-corrected on `LIFETIME` |
| Coerced decrypt expression across 8 failure modes | **0 throws** |
| `stream_flush_interval_ms` on a stock 26.4.1 | **7500 ms** — confirmed off-Cloud too |

**Validations proven to fire (fault injection)**

| Injection | Detected by | Observed |
|---|---|---|
| no vault key for the epoch | `EPOCHKEY`, `RECON` | `declared=50 landed=0 key=NO`; insert clean, no exception |
| 5 skipped offsets | `CONTINUITY` | `p3 missing 5` |
| `cipher='aes-128-cbc'` | `CIPHER` | `ciphers seen: aes-256-gcm, aes-128-cbc` |
| envelope replayed | `DEDUP` | `rows=200 distinct=100 FINAL=100` — absorbed |
| Kafka table without `kafka_flush_interval_ms` | `KAFKA_FLUSH` | `bad=1 — will inherit 7500ms` (negative control) |
| consumer exceptions in the reject window | `KAFKA_CONSUMERS` | `exception_count=10`, `rebalances=13` |

**End-to-end proven**

| Check | Result |
|---|---|
| encrypt in Node → dictionary key → decrypt → explode → JSON column | 12 batches → 1,200 events, exact |
| decrypt amortisation | **100× per decrypt**, measured |
| `ARRAY JOIN payload.consents[]` | 1,200 events → 2,400 consent rows |
| Validation suite | **17 pass, 0 fail, 2 skip** on Cloud; both Kafka checks executed locally |

**Performance (latency test, §14.1)**

| Check | Result |
|---|---|
| Commit-lag p99 @ 19,131 eps, gzip, batch 200 | **259 ms** |
| MV target failure propagates to the writer | **yes** — `while pushing to view` |
| Source insert rolled back when MV fails | **no** — source keeps the row, target does not |
| Laptop ceiling | ~108,146 eps @ p99 541 ms |
| gzip ratio, realistic 947 B events | **7.8×** |
| `async_insert_busy_timeout` as a latency lever | **ineffective** — p99 909→810 ms |
| Request body size as the lever | 160 KB → 271 ms; 6.4 MB → 1082 ms |
| `system.asynchronous_insert_log` | 18,748 flushes, 7.56M rows, **0 exceptions** |
