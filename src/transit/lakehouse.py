"""Lakehouse helpers shared by the bronze and silver layers."""

from datetime import date, timedelta

RETENTION_DAYS = 7


def table_path(root: str, layer: str, table: str) -> str:
    """The Delta table location for a layer's table under the lakehouse root."""
    return f"{root}/{layer}/{table}"


def cleanup(spark, table_path: str, today: date, keep_date: date) -> list[date]:
    """Delete partitions older than the retention window, then VACUUM.

    Removes feed_date < today - RETENTION_DAYS, except keep_date (the day
    just loaded). Returns the deleted dates, oldest first.
    """
    from delta.tables import DeltaTable

    cutoff = (today - timedelta(days=RETENTION_DAYS)).isoformat()
    condition = f"feed_date < '{cutoff}' AND feed_date != '{keep_date.isoformat()}'"
    table = DeltaTable.forPath(spark, table_path)
    rows = table.toDF().filter(condition).select("feed_date").distinct().collect()
    deleted = sorted(r["feed_date"] for r in rows)
    if deleted:
        table.delete(condition)
    table.vacuum()
    return deleted
