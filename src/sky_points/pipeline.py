from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable


COUNTRY_CONFIG = {
    "AUS": {"name": "Australia", "file": "australia_members.csv", "date_format": "ISO"},
    "IND": {"name": "India", "file": "india_members.csv", "date_format": "MDY_SLASH"},
    "USA": {"name": "USA", "file": "usa_members.csv", "date_format": "MDY_COMPACT"},
}

OUTPUT_COLUMNS = [
    "global_member_id", "source_country", "source_member_id", "member_name",
    "dob", "enrollment_date", "last_flight_date", "tier_code", "agent_name",
    "state", "postal_code", "is_active", "member_type", "age",
    "stale_member", "source_updated_at", "ingested_at", "source_file",
    "source_row_number", "record_hash",
]
REDEMPTION_COLUMNS = [
    "txn_id", "member_id", "global_member_id", "feed_date", "txn_date",
    "partner", "miles_redeemed", "status", "member_join_status", "source_file",
]


class RowValidationError(ValueError):
    """A row cannot be promoted from raw to the validated member layer."""


def _key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.strip().lower())


def _normalised_row(row: dict[str, Any]) -> dict[str, str]:
    return {_key(str(k)): ("" if v is None else str(v).strip()) for k, v in row.items()}


def _first(row: dict[str, str], *aliases: str) -> str:
    for alias in aliases:
        value = row.get(_key(alias), "")
        if value != "":
            return value
    return ""


def parse_date(value: str | None, source_country: str) -> date | None:
    """Parse the source-specific formats documented by the sample files.

    USA compact dates use MDDYYYY when seven digits long and MMDDYYYY when
    eight digits long. This is an explicit source-contract assumption because
    seven-digit values can otherwise be ambiguous (e.g. 1052022).
    """
    if value is None or value.strip() == "" or value.strip().upper() in {"NULL", "N/A", "NONE"}:
        return None
    raw = value.strip()
    if source_country == "AUS":
        return datetime.strptime(raw, "%Y-%m-%d").date()
    if source_country == "IND":
        return datetime.strptime(raw, "%m/%d/%Y").date()
    if source_country == "USA":
        digits = re.sub(r"\D", "", raw)
        if len(digits) == 7:  # MDDYYYY
            month, day_part, year = int(digits[0]), int(digits[1:3]), int(digits[3:])
        elif len(digits) == 8:  # MMDDYYYY
            month, day_part, year = int(digits[:2]), int(digits[2:4]), int(digits[4:])
        else:
            raise ValueError(f"expected 7- or 8-digit MDDYYYY/MMDDYYYY date, got {raw!r}")
        return date(year, month, day_part)
    raise ValueError(f"Unsupported source country: {source_country}")


def calculate_age(dob: date | None, as_of: date) -> int | None:
    if dob is None:
        return None
    return as_of.year - dob.year - ((as_of.month, as_of.day) < (dob.month, dob.day))


def _parse_optional_date(raw: str, source_country: str, field_name: str,
                         warnings: list[str]) -> date | None:
    try:
        return parse_date(raw, source_country)
    except (ValueError, TypeError):
        warnings.append(f"invalid_optional_{field_name}")
        return None


def _record_hash(record: dict[str, Any]) -> str:
    stable = {k: record.get(k) for k in OUTPUT_COLUMNS if k not in {
        "ingested_at", "source_file", "source_row_number", "record_hash"
    }}
    payload = json.dumps(stable, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _resolve_global_id(country: str, local_id: str,
                       identity_map: dict[str, str]) -> str:
    # The country prefix prevents ID=1 in three source systems from colliding.
    # Production identity-map entries can map different local IDs to one golden ID.
    return identity_map.get(f"{country}:{local_id}", f"{country}:{local_id}")


def _extract_member(row: dict[str, Any], country: str, file_name: str,
                    row_number: int, ingested_at: datetime,
                    identity_map: dict[str, str]) -> tuple[dict[str, Any] | None, list[str], list[str]]:
    raw = _normalised_row(row)
    errors: list[str] = []
    warnings: list[str] = []
    local_id = _first(raw, "Unique ID", "ID", "Member ID", "Mem_Id")
    name = _first(raw, "Member Name", "Name", "Name")
    if not local_id:
        errors.append("missing_member_id")
    if not name:
        errors.append("missing_member_name")
    if len(local_id) > 18:
        errors.append("member_id_exceeds_18_characters")
    if len(name) > 255:
        errors.append("member_name_exceeds_255_characters")
    if errors:
        return None, errors, warnings

    tier = _first(raw, "Tier Type", "TierCode", "Tier Code") or None
    if tier and len(tier) > 5:
        errors.append("tier_code_exceeds_5_characters")
    if errors:
        return None, errors, warnings

    enrollment_raw = _first(raw, "Date of Enrollment", "EnrollmentDate", "Enrollment Date")
    if not enrollment_raw or enrollment_raw.upper() in {"NULL", "N/A", "NONE"}:
        errors.append("missing_enrollment_date")
        return None, errors, warnings
    try:
        enrollment = parse_date(enrollment_raw, country)
    except (ValueError, TypeError):
        errors.append("invalid_enrollment_date")
        return None, errors, warnings
    if enrollment is None:
        errors.append("missing_enrollment_date")
        return None, errors, warnings
    if enrollment > ingested_at.date():
        errors.append("future_enrollment_date")
        return None, errors, warnings

    dob_raw = _first(raw, "Date of Birth", "DOB", "DateOfBirth")
    flight_raw = _first(raw, "Date of Flight", "Flight Date", "FlightDate", "Last Flight Date")
    dob = _parse_optional_date(dob_raw, country, "dob", warnings)
    flight = _parse_optional_date(flight_raw, country, "flight_date", warnings)
    if dob and dob > ingested_at.date():
        warnings.append("future_dob_treated_as_null")
        dob = None
    if flight and flight > ingested_at.date():
        warnings.append("future_flight_date_treated_as_null")
        flight = None
    if dob and dob > enrollment:
        warnings.append("dob_after_enrollment_date")
    if flight and flight < enrollment:
        warnings.append("flight_before_enrollment_date")
    updated_raw = _first(raw, "Source Updated At", "Updated At", "Record Updated At")
    try:
        source_updated_at = datetime.fromisoformat(updated_raw.replace("Z", "+00:00")) if updated_raw else ingested_at
        if source_updated_at.tzinfo is None:
            source_updated_at = source_updated_at.replace(tzinfo=timezone.utc)
    except ValueError:
        warnings.append("invalid_source_updated_at_fallback_to_ingestion_time")
        source_updated_at = ingested_at

    postal = _first(raw, "Post Code", "Postal Code", "Zip Code") or None
    if postal is not None and (not postal.isdigit() or len(postal) > 5):
        warnings.append("invalid_postal_code")
        postal = None
    member_type = _first(raw, "Individual or Corporate", "Member Type") or None
    if member_type and member_type not in {"I", "C"}:
        warnings.append("unexpected_member_type")
    active = _first(raw, "Is Active", "Active Member", "FLAG") or None
    if active and active not in {"A", "I", "Y", "N"}:
        warnings.append("unexpected_active_flag")

    record = {
        "global_member_id": _resolve_global_id(country, local_id, identity_map),
        "source_country": country,
        "source_member_id": local_id,
        "member_name": name,
        "dob": dob.isoformat() if dob else None,
        "enrollment_date": enrollment.isoformat(),
        "last_flight_date": flight.isoformat() if flight else None,
        "tier_code": tier,
        "agent_name": _first(raw, "Agent Name", "Agent_Name") or None,
        "state": _first(raw, "State") or None,
        "postal_code": postal,
        "is_active": active,
        "member_type": member_type,
        "source_updated_at": source_updated_at.astimezone(timezone.utc).isoformat(),
        "ingested_at": ingested_at.astimezone(timezone.utc).isoformat(),
        "source_file": file_name,
        "source_row_number": row_number,
    }
    record["record_hash"] = _record_hash(record)
    return record, errors, warnings


def latest_record_wins(records: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Keep the newest source state per golden ID; return winners and superseded rows."""
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[record["global_member_id"]].append(record)
    winners, superseded = [], []
    for group in grouped.values():
        # A deterministic alphabetic tie-breaker is not evidence of chronology.
        # Fail closed when different records have identical source and ingestion
        # timestamps; publishing either country would be an arbitrary decision.
        temporal_keys = {(r["source_updated_at"], r["ingested_at"]) for r in group}
        content_hashes = {r.get("record_hash") for r in group}
        if len(group) > 1 and len(temporal_keys) == 1 and len(content_hashes) > 1:
            member_id = group[0]["global_member_id"]
            raise ValueError(
                f"Cannot determine latest record for {member_id}: conflicting "
                "records have identical source_updated_at and ingested_at; "
                "supply a source event timestamp or sequence."
            )
        ordered = sorted(group, key=lambda r: (
            r["source_updated_at"], r["ingested_at"], r["source_country"],
            r["source_file"], int(r["source_row_number"])
        ))
        winners.append(ordered[-1])
        superseded.extend(ordered[:-1])
    winners.sort(key=lambda r: (r["source_country"], r["global_member_id"]))
    return winners, superseded


def flatten_redemptions(json_paths: Iterable[Path], member_records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Flatten one-object-per-file or JSON-array partner feeds; retain orphan rows."""
    by_global_id = {r["global_member_id"]: r for r in member_records}
    by_local_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in member_records:
        by_local_id[r["source_member_id"]].append(r)

    flattened, errors = [], []
    seen_txn: set[str] = set()
    for path in json_paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            envelopes = payload if isinstance(payload, list) else [payload]
            for envelope in envelopes:
                member_id = str(envelope.get("member_id", "")).strip()
                feed_date = envelope.get("feed_date")
                items = envelope.get("redemptions", [])
                if not isinstance(items, list):
                    errors.append({"source_file": path.name, "error": "redemptions_not_array"})
                    continue
                for txn in items:
                    txn_id = str(txn.get("txn_id", "")).strip()
                    if not txn_id:
                        errors.append({"source_file": path.name, "error": "missing_txn_id", "raw": txn})
                        continue
                    if txn_id in seen_txn:
                        errors.append({"source_file": path.name, "error": "duplicate_txn_id", "txn_id": txn_id})
                        continue
                    seen_txn.add(txn_id)
                    try:
                        txn_date = datetime.strptime(str(txn.get("txn_date", "")), "%Y%m%d").date().isoformat()
                        parsed_feed_date = datetime.strptime(str(feed_date), "%Y%m%d").date().isoformat()
                        miles = int(txn.get("miles_redeemed"))
                        if miles < 0:
                            raise ValueError("negative miles")
                    except (TypeError, ValueError):
                        errors.append({"source_file": path.name, "error": "invalid_redemption_fields", "txn_id": txn_id})
                        continue

                    matched = by_global_id.get(member_id)
                    if matched is None and len(by_local_id.get(member_id, [])) == 1:
                        matched = by_local_id[member_id][0]
                    if matched is not None:
                        join_status, global_id = "MATCHED", matched["global_member_id"]
                    elif len(by_local_id.get(member_id, [])) > 1:
                        join_status, global_id = "AMBIGUOUS_MEMBER_ID", None
                    else:
                        join_status, global_id = "UNMATCHED_MEMBER_ID", None
                    flattened.append({
                        "txn_id": txn_id, "member_id": member_id,
                        "global_member_id": global_id, "feed_date": parsed_feed_date,
                        "txn_date": txn_date, "partner": txn.get("partner"),
                        "miles_redeemed": miles, "status": str(txn.get("status", "")).upper(),
                        "member_join_status": join_status, "source_file": path.name,
                    })
        except (OSError, json.JSONDecodeError, AttributeError) as exc:
            errors.append({"source_file": path.name, "error": f"invalid_json_feed: {exc}"})
    return flattened, errors


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def run_pipeline(input_dir: Path, output_dir: Path, as_of_date: date,
                 ingested_at: datetime, identity_map_path: Path | None = None) -> dict[str, Any]:
    identity_map: dict[str, str] = {}
    if identity_map_path and identity_map_path.exists():
        identity_map = json.loads(identity_map_path.read_text(encoding="utf-8"))

    raw_count = 0
    parsed: list[dict[str, Any]] = []
    rejects: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    source_keys: Counter[tuple[str, str]] = Counter()
    staged: list[tuple[dict[str, Any], str, int, list[str]]] = []

    for country, cfg in COUNTRY_CONFIG.items():
        path = input_dir / cfg["file"]
        if not path.exists():
            raise FileNotFoundError(f"Expected source file not found: {path}")
        with path.open(encoding="utf-8-sig", newline="") as fh:
            for row_number, row in enumerate(csv.DictReader(fh), start=2):
                raw_count += 1
                record, errors, row_warnings = _extract_member(
                    row, country, path.name, row_number, ingested_at, identity_map
                )
                if record is None:
                    rejects.append({"source_file": path.name, "source_row_number": row_number,
                                    "raw_record": json.dumps(row, ensure_ascii=False),
                                    "errors": ";".join(errors)})
                    continue
                source_keys[(country, record["source_member_id"])] += 1
                staged.append((record, path.name, row_number, row_warnings))

    duplicate_source_keys = {k for k, count in source_keys.items() if count > 1}
    for record, file_name, row_number, row_warnings in staged:
        key = (record["source_country"], record["source_member_id"])
        if key in duplicate_source_keys:
            rejects.append({"source_file": file_name, "source_row_number": row_number,
                            "raw_record": "", "errors": "duplicate_source_member_id_within_batch"})
            continue
        record["age"] = calculate_age(date.fromisoformat(record["dob"]) if record["dob"] else None, as_of_date)
        flight = date.fromisoformat(record["last_flight_date"]) if record["last_flight_date"] else None
        record["stale_member"] = bool(flight and (as_of_date - flight).days > 90)
        parsed.append(record)
        for issue in row_warnings:
            warnings.append({"source_file": file_name, "source_row_number": row_number,
                             "global_member_id": record["global_member_id"], "warning": issue})

    winners, superseded = latest_record_wins(parsed)
    for country in COUNTRY_CONFIG:
        country_rows = [r for r in winners if r["source_country"] == country]
        _write_csv(output_dir / f"table_{COUNTRY_CONFIG[country]['name'].lower()}.csv", country_rows, OUTPUT_COLUMNS)
    _write_csv(output_dir / "members_current.csv", winners, OUTPUT_COLUMNS)
    _write_csv(output_dir / "rejected_members.csv", rejects,
               ["source_file", "source_row_number", "raw_record", "errors"])
    _write_csv(output_dir / "superseded_member_records.csv", superseded, OUTPUT_COLUMNS)
    _write_csv(output_dir / "member_warnings.csv", warnings,
               ["source_file", "source_row_number", "global_member_id", "warning"])

    redemption_paths = sorted(input_dir.glob("*.json"))
    redemptions, redemption_errors = flatten_redemptions(redemption_paths, winners)
    _write_csv(output_dir / "redemptions_flat.csv", redemptions, REDEMPTION_COLUMNS)
    _write_csv(output_dir / "redemption_errors.csv", redemption_errors,
               ["source_file", "error", "txn_id", "raw"])

    quality = {
        "as_of_date": as_of_date.isoformat(),
        "ingested_at": ingested_at.astimezone(timezone.utc).isoformat(),
        "source_rows_read": raw_count,
        "member_rows_accepted_before_latest_wins": len(parsed),
        "current_member_rows": len(winners),
        "member_rows_rejected": len(rejects),
        "member_rows_superseded_by_latest_record": len(superseded),
        "member_warning_count": len(warnings),
        "missing_dob_count": sum(1 for r in winners if not r["dob"]),
        "stale_member_count": sum(1 for r in winners if r["stale_member"]),
        "duplicate_member_name_value_count": sum(1 for n, c in Counter(r["member_name"].casefold() for r in winners).items() if c > 1),
        "duplicate_global_member_id_count_in_current": len(winners) - len({r["global_member_id"] for r in winners}),
        "redemption_rows_flattened": len(redemptions),
        "redemption_error_count": len(redemption_errors),
        "redemption_join_status_counts": dict(Counter(r["member_join_status"] for r in redemptions)),
        "rows_by_country": dict(Counter(r["source_country"] for r in winners)),
        "reject_reasons": dict(Counter(reason for r in rejects for reason in r["errors"].split(";") if reason)),
        "notes": [
            "Sample source IDs are only unique within each country file; namespaced IDs are used unless a golden identity map is supplied.",
            "Sample sources do not provide a source update timestamp; ingestion time is used as a documented fallback.",
            "The seven-digit USA date contract is interpreted as MDDYYYY; the source owner must confirm this format.",
        ],
    }
    (output_dir / "data_quality_report.json").write_text(json.dumps(quality, indent=2), encoding="utf-8")
    return quality
