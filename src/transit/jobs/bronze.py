"""Spark job: load one GTFS table for one day from landing into bronze."""
import argparse
import os
import sys
from datetime import date, datetime
from zoneinfo import ZoneInfo

import boto3

from transit.bronze import TABLES, build_bronze_table
from transit.spark import get_spark


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", required=True, choices=sorted(TABLES))
    parser.add_argument("--feed-date", required=True, type=date.fromisoformat)
    args = parser.parse_args(argv)

    today = datetime.now(ZoneInfo("Asia/Jerusalem")).date()
    rows = build_bronze_table(
        get_spark(f"bronze_{args.table}"),
        boto3.client("s3"),
        os.environ["GTFS_BUCKET"],
        args.table,
        args.feed_date,
        today,
    )
    print(f"bronze {args.table} {args.feed_date}: {rows} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
