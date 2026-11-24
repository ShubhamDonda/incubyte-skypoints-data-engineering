# SkyPoints - Incubyte Data Engineer Assessment

A reference implementation of the Incubyte SkyPoints data-engineering exercise. It ingests three country-specific member extracts, standardizes their schemas and dates, validates rows, derives age/stale-member fields, applies latest-record-wins at a global member identity, routes current members to country targets, and flattens partner redemption JSON.

## What is included

- `src/sky_points/`: runnable Python reference ETL (Python standard library only).
- `tests/`: automated unit and end-to-end tests.
- `sql/01_ddl.sql`: Snowflake DDL for raw landing, staging, identity map, canonical current members, country targets, redemptions, and rejects.
- `sql/02_transform.sql`: Snowflake-oriented transformation/merge and DQ query patterns. The CSV-to-canonical-VARIANT adapter is deliberately shown as an ingestion boundary; date parsing is normalized before the SQL staging promotion.
- `data/input/`: the three supplied sample datasets plus a small JSON feed adapted to a sample identity key so the join can be demonstrated end to end.
- `data/config/`: illustrative golden-ID crosswalk configuration (not an input redemption feed).
- `data/output/`: generated country outputs, flattened redemptions, rejects, warnings, superseded records, and a machine-readable quality report.
- `docs/`: architecture, assumptions, and a presentation-ready interview walkthrough.

## Run it

Requires Python 3.10+; no runtime third-party package is required.

```bash
# From the repository root
PYTHONPATH=src python -m unittest discover -s tests -v
PYTHONPATH=src python run_pipeline.py \
  --input-dir data/input \
  --output-dir data/output \
  --as-of-date 2022-09-01 \
  --ingested-at 2022-09-01T00:00:00Z
```

To use a golden identity crosswalk, pass a JSON file mapping `COUNTRY:LOCAL_ID` to `GLOBAL_MEMBER_ID`:

```json
{
  "AUS:2": "MEM-0002",
  "IND:8": "MEM-0002"
}
```

```bash
PYTHONPATH=src python run_pipeline.py --identity-map data/config/identity_map.example.json
```

The example map is illustrative only; it intentionally shows how two source-system IDs can point to one enterprise member identity. Do not load that test map as real member data.

## Sample run result

The checked-in output was generated with `as_of_date=2022-09-01` and a fixed ingestion timestamp for reproducibility.

| Metric | Result |
|---|---:|
| Source member rows read | 9 |
| Valid member rows before latest-wins | 8 |
| Current member rows | 8 |
| Rejected member rows | 1 |
| Rejected reason | Invalid mandatory Australia enrollment date |
| Missing DOBs (allowed because DOB is optional) | 4 |
| Duplicate member-name values (diagnostic only; names are not keys) | 1 |
| Duplicate global IDs in current output | 0 |
| Stale members (>90 days since last flight) | 5 |
| Flattened redemption transactions | 2 |
| Redemption rows joined to a member | 2 |

The Australia row for Jonnathan is retained in raw input but rejected from the curated member set because `2021-13-13` is not a valid date and enrollment date is mandatory. Mike's missing DOB is retained as a nullable attribute because DOB is optional. The sample run has one joinable JSON member key (`IND:1`) so the flattened join can be demonstrated.

## Key design decisions and explicit assumptions

1. **Global identity:** the PDF narrative uses `Member_Id` for member-level processing, but its field matrix flags `Member Name` as the key. The implementation does not use names as keys: names can change and are not guaranteed unique. It uses `GLOBAL_MEMBER_ID`, resolved through `MEMBER_IDENTITY_MAP`. In the provided country files IDs `1`, `2`, and `3` repeat, so the sample defaults to `AUS:1`, `IND:1`, and `USA:1` rather than silently colliding. Real cross-country moves require a source-provided global identifier or a governed crosswalk.
2. **Latest record wins:** the sample files do not include `updated_at`, source sequence, or effective timestamp. The Python sample uses ingestion time as a fallback when there is no competing version. If two different records for the same global ID have identical source and ingestion timestamps, the pipeline fails closed rather than choosing an arbitrary country. In production, the source must provide `SOURCE_UPDATED_AT` or a monotonic event sequence. Do not use `Last_Flight_Date` as a proxy for when a profile changed.
3. **Date formats:** Australia is parsed as `YYYY-MM-DD`; India as `M/D/YYYY`; USA compact values are interpreted as `MDDYYYY` (7 digits) or `MMDDYYYY` (8 digits). This makes `1052022` January 5, 2022. The owner of the USA feed must confirm the contract because seven-digit dates can be ambiguous without a format specification.
4. **Country:** the supplied files do not carry a country column. Country is derived from the controlled source-to-country configuration/file route, not guessed from state or member name.
5. **Optional fields:** DOB, last flight, tier, agent, state, postal code, active flag, and member type are nullable. Malformed mandatory enrollment dates are rejected; malformed optional dates become null with a warning in a production-quality extension.
6. **Staleness:** a member is stale only when a valid last-flight date exists and `as_of_date - last_flight_date > 90`. A null flight date is not automatically called stale; it is a separate unknown-data condition.
7. **Country tables:** `MEMBER_CURRENT` is the canonical one-row-per-global-member state. Country target tables are synchronized from it. This prevents a member from remaining active in two country targets after a move.
8. **JSON identity:** partner `member_id` is expected to resolve to the global ID. A source-local ID is joined only when it resolves unambiguously. Unmatched and ambiguous transactions should be retained and monitored, not dropped.

## Test coverage

Eight tests cover source-specific date parsing, malformed dates, birthday-aware age, latest-record-wins on a country move, failure on unorderable conflicting versions, sample-file row counts/rejects, and JSON flattening/join. Run the test command above before making a submission.

## Scale and operational notes

The local Python implementation is an executable correctness reference for the supplied nine-row files, not a claim that one Python process should handle billions of rows. `docs/architecture.md` describes the production pattern: immutable landing, distributed parsing/validation, idempotent batch manifests, Snowflake `MERGE`, clustering based on measured pruning, controlled country delivery, observability, and replay/backfill. For billions of daily records, use a distributed engine (e.g. Spark/Databricks) and Snowflake's bulk/streaming ingestion patterns based on latency needs; do not collect the full dataset into driver memory.

## What has and has not been executed

- The Python ETL and its seven tests were executed locally against the supplied sample files.
- Snowflake SQL is a Snowflake-oriented reference script and has not been run against a live Snowflake account in this environment. The CSV-to-raw-VARIANT adapter, warehouse roles, stages, file formats, and operational scheduling must be configured in the target account.
- No public GitHub/GitLab repository has been created or pushed. The local repository bundle is ready for review and upload; before publishing, replace any example identity mappings with approved identifiers and review the solution yourself.
