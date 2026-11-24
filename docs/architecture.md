# Production architecture and engineering decisions

## 1. Target flow

```text
Source member CSVs                         Partner redemption JSON
        |                                             |
        v                                             v
Country-aware ingestion adapter              JSON landing adapter
        |                                             |
        +-------------------+-------------------------+
                            v
                 Immutable raw / Bronze landing
              (payload, file, row, batch, hash, time)
                            |
                            v
              Distributed parse + standardization
              - source-specific date parsing
              - schema and length checks
              - identity crosswalk lookup
              - mandatory/domain validation
                            |
                 +----------+----------+
                 |                     |
                 v                     v
          DQ rejects/quarantine    Typed staging
                                       |
                                       v
                            Derived age / stale flag
                                       |
                                       v
                         Latest by GLOBAL_MEMBER_ID
                                       |
                                       v
                           MEMBER_CURRENT (canonical)
                                       |
                      +----------------+----------------+
                      |                |                |
                      v                v                v
                TABLE_AUSTRALIA   TABLE_INDIA       TABLE_USA

Partner JSON --> LATERAL FLATTEN --> REDEMPTION_FLAT
                                      |
                                      +--> join on GLOBAL_MEMBER_ID
```

## 2. Why raw, staging, and curated are separate

- **Raw/landing:** append-only and replayable. Keep source payload and technical metadata so a parser fix does not require asking the upstream team to resend files.
- **Staging:** typed, normalized, validated records. No country-specific business consumer should have to know the original date format or header names.
- **Curated:** current member profile, country delivery targets, and flattened redemptions. Apply the same contract every batch.
- **Rejects:** preserve invalid rows with file/row/batch identifiers and stable error codes. Separate row-level rejects from batch-fatal failures such as a missing file or corrupt manifest.

## 3. Identity and country movement

Use an immutable `GLOBAL_MEMBER_ID` as the business key. The source files repeat local IDs across countries and do not carry a global ID, so the sample uses `COUNTRY:LOCAL_ID` to prevent collisions. In production, maintain a governed crosswalk:

| Source country | Source local ID | Global member ID |
|---|---|---|
| AUS | A-102 | MEMBER-9001 |
| IND | 77 | MEMBER-9001 |

When a member moves, the new source key resolves to the same `GLOBAL_MEMBER_ID`. Deduplicate all records for that golden ID using `SOURCE_UPDATED_AT DESC`, then ingestion timestamp and deterministic source metadata as tie-breakers. Update the canonical row's `SOURCE_COUNTRY`, then remove the member from its former country target and upsert it into the new one in the same orchestration transaction.

Do not use member name as a key. The brief's layout flags `Member_Name` as a key, but this conflicts with the use of `Member_Id` in the narrative and with normal identity design. Treat name as a required attribute and validate its presence; enforce member uniqueness on the golden ID. Ask the source owner to resolve this contract discrepancy before production.

## 4. Latest-record-wins and temporal correctness

A profile's last flight date is a business attribute, not a profile-change timestamp. It cannot safely order changes to address, tier, or country. Require one of:

1. A source `updated_at` timestamp with a documented timezone and monotonic semantics, or
2. A source event sequence/version that is strictly increasing per member.

If neither is available, ingestion time gives latest-arrival-wins, not necessarily latest-source-state. The sample code uses ingestion time only as a fallback when no conflicting version exists; if two different versions tie on both timestamps, it fails closed rather than selecting a random country. For auditability, retain the raw events and optionally maintain a type-2 history table with `VALID_FROM`, `VALID_TO`, `IS_CURRENT`, and source batch ID.

## 5. Scale design for billions of rows per day

- Land files into object storage using immutable, partitioned paths such as `source=member/country=IND/ingest_date=YYYY-MM-DD/batch_id=.../`.
- Require a manifest with file name, source country, schema version, expected row count, checksum, event-time range, and delivery sequence. Detect duplicate files by checksum/batch ID.
- Use distributed Spark/Databricks processing for high-volume CSV normalization, JSON flattening, validation, and deduplication. Keep transformations distributed; avoid `collect()` and Python row-at-a-time UDFs.
- Use columnar Parquet/Delta for internal high-volume stages where appropriate. Compact small files and tune partitioning based on measured read/write patterns; avoid high-cardinality partitions such as member ID.
- Bulk-load staged data to Snowflake using its supported cloud-storage stages and `COPY INTO`, or choose Snowpipe Streaming only when low-latency event delivery justifies it.
- Cluster the canonical table only when query profile shows meaningful micro-partition pruning gains. `SOURCE_COUNTRY` is a candidate for country filters; validate with `SYSTEM$CLUSTERING_INFORMATION` and query profiles rather than clustering reflexively.
- Process each batch idempotently. Use `PIPELINE_BATCH_AUDIT` with `STARTED`, `LANDED`, `VALIDATED`, `MERGED`, `PUBLISHED`, and `FAILED` states. Re-running a completed batch must not duplicate members or transactions.
- Deduplicate partner transactions by `TXN_ID`; define whether a repeated ID is a replay, an update, or a source defect. If updates are allowed, retain transaction version/source timestamp and merge accordingly.
- Use bounded micro-batches and warehouse sizing/concurrency controls. Separate profile and redemption ingestion so a delayed partner feed does not block member profile publication.

## 6. Validation strategy

**File/batch:** expected file pattern, checksum, non-empty file, schema version, header/column count, encoding, delimiter, delivery sequence, row-count reconciliation.

**Member row:** required member ID/name/enrollment date, valid date formats, no future enrollment date, DOB not in the future, field lengths, numeric postal code, accepted tier/active/member-type values, country assigned from trusted metadata, duplicate `(source_country, source_member_id)` detection.

**Golden/current state:** unique current `GLOBAL_MEMBER_ID`, one current country per member, valid crosswalk mapping, deterministic winner timestamp, no stale duplicates in old country tables, record-hash no-op detection.

**Redemption:** valid JSON schema, non-empty transaction ID, transaction/feed dates parse, non-negative integer miles, allowed status values, unique transaction ID, member join status and unmatched-rate threshold.

**Reconciliation:** source rows = accepted + rejected (with duplicates and intentionally filtered rows accounted for); flattened redemptions = matched + unmatched/ambiguous + invalid; country target counts sum to canonical current rows.

## 7. Age and stale-member rules

Age is calculated as of a parameterized batch date and subtracts one when the birthday has not occurred yet in that year. Stale means strictly more than 90 elapsed days since a valid last-flight date. A null DOB or flight date remains null/unknown; it is not silently converted into zero or stale.

## 8. Operational controls

Publish structured logs with `batch_id`, source file, country, row counts, rejected counts, duration, warehouse/job ID, and code version. Alert on missing deliveries, sudden volume drift, invalid date spikes, unmatched redemption rate, duplicate keys, and reconciliation failures. Store secrets in a secret manager, use least-privilege roles, encrypt data in transit/at rest, and mask DOB/member identifiers in lower environments. Define retention and privacy policies for raw records.
