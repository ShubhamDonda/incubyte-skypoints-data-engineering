import csv
import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from sky_points.pipeline import (
    calculate_age, flatten_redemptions, latest_record_wins, parse_date, run_pipeline,
)

ROOT = Path(__file__).resolve().parents[1]


class DateAndDerivationTests(unittest.TestCase):
    def test_india_mdy_slash_date(self):
        self.assertEqual(parse_date("12/1/1998", "IND"), date(1998, 12, 1))

    def test_usa_compact_date_with_one_digit_month(self):
        self.assertEqual(parse_date("1052022", "USA"), date(2022, 1, 5))
        self.assertEqual(parse_date("6152022", "USA"), date(2022, 6, 15))

    def test_invalid_australia_date_raises(self):
        with self.assertRaises(ValueError):
            parse_date("2021-13-13", "AUS")

    def test_age_respects_whether_birthday_has_occurred(self):
        self.assertEqual(calculate_age(date(1998, 12, 1), date(2022, 9, 1)), 23)
        self.assertEqual(calculate_age(date(1982, 8, 13), date(2022, 9, 1)), 40)
        self.assertIsNone(calculate_age(None, date(2022, 9, 1)))


class LatestWinsTests(unittest.TestCase):
    def test_latest_source_record_moves_member_between_countries(self):
        base = {
            "global_member_id": "MEM-42", "source_member_id": "7",
            "source_updated_at": "2024-01-01T00:00:00+00:00",
            "ingested_at": "2024-01-01T00:01:00+00:00",
            "source_file": "old.csv", "source_row_number": 2,
        }
        old = {**base, "source_country": "AUS", "member_name": "Taylor"}
        new = {**base, "source_country": "IND", "member_name": "Taylor",
              "source_updated_at": "2024-02-01T00:00:00+00:00",
              "ingested_at": "2024-02-01T00:01:00+00:00", "source_file": "new.csv"}
        winners, superseded = latest_record_wins([old, new])
        self.assertEqual(len(winners), 1)
        self.assertEqual(winners[0]["source_country"], "IND")
        self.assertEqual(len(superseded), 1)
        self.assertEqual(superseded[0]["source_country"], "AUS")

    def test_conflicting_records_without_ordering_metadata_fail_closed(self):
        base = {
            "global_member_id": "MEM-99", "source_member_id": "9",
            "source_updated_at": "2024-01-01T00:00:00+00:00",
            "ingested_at": "2024-01-01T00:01:00+00:00",
            "source_file": "feed.csv", "source_row_number": 2,
        }
        records = [
            {**base, "source_country": "AUS", "member_name": "Alex", "record_hash": "hash-a"},
            {**base, "source_country": "IND", "member_name": "Alex", "record_hash": "hash-b"},
        ]
        with self.assertRaisesRegex(ValueError, "supply a source event timestamp or sequence"):
            latest_record_wins(records)


class EndToEndTests(unittest.TestCase):
    def test_supplied_sample_files_produce_expected_outputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "out"
            report = run_pipeline(
                ROOT / "data" / "input", output, date(2022, 9, 1),
                datetime(2022, 9, 1, tzinfo=timezone.utc),
            )
            self.assertEqual(report["source_rows_read"], 9)
            self.assertEqual(report["member_rows_accepted_before_latest_wins"], 8)
            self.assertEqual(report["member_rows_rejected"], 1)
            self.assertEqual(report["current_member_rows"], 8)
            self.assertEqual(report["rows_by_country"], {"AUS": 2, "IND": 3, "USA": 3})
            self.assertTrue((output / "table_australia.csv").exists())
            self.assertTrue((output / "table_india.csv").exists())
            self.assertTrue((output / "table_usa.csv").exists())
            with (output / "rejected_members.csv").open(encoding="utf-8", newline="") as fh:
                rejected = list(csv.DictReader(fh))
            self.assertEqual(rejected[0]["errors"], "invalid_enrollment_date")
            with (output / "members_current.csv").open(encoding="utf-8", newline="") as fh:
                members = list(csv.DictReader(fh))
            mike = next(r for r in members if r["global_member_id"] == "AUS:1")
            # Same display name exists in USA; names are not global keys.
            usa_mike = next(r for r in members if r["global_member_id"] == "USA:3")
            self.assertEqual(mike["member_name"], usa_mike["member_name"])
            self.assertNotEqual(mike["global_member_id"], usa_mike["global_member_id"])
            self.assertEqual(mike["dob"], "")
            self.assertEqual(mike["age"], "")
            self.assertFalse(mike["stale_member"] == "True")

    def test_redemption_flattening_and_member_join(self):
        member = {"global_member_id": "IND:1", "source_member_id": "1"}
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "feed.json"
            p.write_text(json.dumps({
                "member_id": "IND:1", "feed_date": "20240115",
                "redemptions": [{"txn_id": "T1", "txn_date": "20240110",
                                 "partner": "AeroLink", "miles_redeemed": 1000,
                                 "status": "COMPLETED"}]
            }), encoding="utf-8")
            rows, errors = flatten_redemptions([p], [member])
            self.assertEqual(errors, [])
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["member_join_status"], "MATCHED")
            self.assertEqual(rows[0]["global_member_id"], "IND:1")
            self.assertEqual(rows[0]["miles_redeemed"], 1000)


if __name__ == "__main__":
    unittest.main()
