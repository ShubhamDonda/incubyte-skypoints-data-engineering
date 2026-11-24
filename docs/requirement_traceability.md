# Assessment requirement traceability

| Incubyte deliverable | Implemented in | Evidence / result |
|---|---|---|
| Raw/landing, staging, country target DDL | `sql/01_ddl.sql` | Raw VARIANT landing, typed staging, canonical member table, Australia/India/USA delivery tables, identity map, flattened redemptions, rejects, batch audit |
| Derive age and stale member | `src/sky_points/pipeline.py`, `sql/02_transform.sql` | Birthday-aware age; stale when elapsed days since flight is strictly greater than 90 |
| Country split and latest record wins | `src/sky_points/pipeline.py`, `sql/02_transform.sql` | Global-ID crosswalk; deduplicate by source update time; move out of former country target and upsert current one |
| Flatten JSON and join to members | `flatten_redemptions`, `sql/02_transform.sql` | One row per redemption item; join status explicitly distinguishes matched/unmatched/ambiguous cases |
| Data quality checks | Python validation, output reject/warning files, DQ queries in `sql/02_transform.sql` | Mandatory fields, row uniqueness, mapping quality, optional-date handling, field lengths, domain checks, transaction uniqueness, reconciliation |
| Live demonstration | `docs/interview_walkthrough.md` | Tested commands and an ordered walkthrough |

## Important source-specification gaps

- PDF marks `Member_Name` as key although the story uses member IDs and the sample has a duplicate name (`Mike`) across countries. This implementation uses an immutable global member ID and documents the discrepancy.
- Country member CSVs use local IDs which repeat across countries. A golden ID crosswalk is required to recognize the same member after a country move.
- No source update timestamp exists in the sample CSVs. The local code uses ingestion time only as a fallback and fails closed on tied, conflicting versions. Production latest-wins requires an update timestamp or event sequence.
- The partner JSON example uses member ID `223457`, which is not present in the attached country CSVs. The original example is retained as `docs/assessment_json_example.json`; `data/input/sample_redemptions.json` is an end-to-end joinable fixture using `IND:1`.
- The USA date file has no declared date format. The implementation assumes 7 digits = `MDDYYYY` and 8 digits = `MMDDYYYY` pending source-owner confirmation.
