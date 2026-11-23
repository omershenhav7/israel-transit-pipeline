"""Spark job: build one silver table for one day from bronze."""
import argparse
import sys
from datetime import date, datetime
from zoneinfo import ZoneInfo

from transit.silver import SILVER_TABLES, build_silver_table
from transit.spark import get_spark, lakehouse_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", required=True, choices=sorted(SILVER_TABLES))
    parser.add_argument("--feed-date", required=True, type=date.fromisoformat)
    args = parser.parse_args(argv)

    now = datetime.now(ZoneInfo("Asia/Jerusalem"))
    counts = build_silver_table(
        get_spark(f"silver_{args.table}"), lakehouse_path(), args.table, args.feed_date, now.date(), now,
    )
    print(
        f"silver {args.table} {args.feed_date}: kept {counts.kept}, "
        f"rejected {counts.rejected}, non-bus {counts.non_bus}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
