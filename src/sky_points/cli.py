from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from pathlib import Path
import json

from .pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the SkyPoints sample member and redemption ETL.")
    parser.add_argument("--input-dir", type=Path, default=Path("data/input"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/output"))
    parser.add_argument("--as-of-date", type=date.fromisoformat, default=date.today(),
                        help="Date used to calculate age and stale-member status (YYYY-MM-DD).")
    parser.add_argument("--ingested-at", default=None,
                        help="ISO-8601 ingestion timestamp; defaults to current UTC time.")
    parser.add_argument("--identity-map", type=Path, default=None,
                        help="Optional JSON mapping such as {'AUS:2':'MEM-0002','IND:8':'MEM-0002'}.")
    args = parser.parse_args()
    ingested_at = (datetime.fromisoformat(args.ingested_at.replace("Z", "+00:00"))
                   if args.ingested_at else datetime.now(timezone.utc))
    report = run_pipeline(args.input_dir, args.output_dir, args.as_of_date, ingested_at, args.identity_map)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
