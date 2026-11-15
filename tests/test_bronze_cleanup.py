from datetime import date, datetime, timedelta

from conftest import feed_text

from transit.bronze import TABLES, cleanup, load_table

TODAY = date(2026, 11, 15)
FIRST = date(2026, 11, 1)


def key(d: date) -> str:
    return f"landing/gtfs/feed_date={d.isoformat()}/gtfs.zip"


def seed(spark, tmp_path, days: list[date]) -> str:
    """Load one stops row per day through load_table; return the table path."""
    path = str(tmp_path / "stops")
    for d in days:
        row = dict(zip(TABLES["stops"], ["1", "c", "n", "d", "32.1", "34.8", "0", "", "5"]))
        csv = tmp_path / f"stops-{d}.txt"
        csv.write_bytes(feed_text("stops", [[row[c] for c in TABLES["stops"]]]).encode("utf-8"))
        load_table(spark, csv, "stops", d, key(d), path, datetime(d.year, d.month, d.day, 6))
    return path


def remaining(spark, path) -> list[date]:
    df = spark.read.format("delta").load(path).select("feed_date").distinct()
    return sorted(r["feed_date"] for r in df.collect())


def days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def test_cleanup_removes_days_older_than_seven(spark, tmp_path):
    path = seed(spark, tmp_path, days(FIRST, TODAY))
    deleted = cleanup(spark, path, TODAY, TODAY)
    assert deleted == days(date(2026, 11, 1), date(2026, 11, 7))
    assert remaining(spark, path) == days(date(2026, 11, 8), TODAY)


def test_cleanup_keeps_old_day_just_loaded(spark, tmp_path):
    path = seed(spark, tmp_path, days(FIRST, TODAY))
    keep = date(2026, 11, 2)
    deleted = cleanup(spark, path, TODAY, keep)
    assert keep not in deleted
    assert remaining(spark, path) == [keep] + days(date(2026, 11, 8), TODAY)


def test_cleanup_on_new_table_does_nothing(spark, tmp_path):
    path = seed(spark, tmp_path, [TODAY])
    assert cleanup(spark, path, TODAY, TODAY) == []
    assert remaining(spark, path) == [TODAY]
