"""Bronze layer: raw, all-text Delta tables, one per GTFS feed file."""

import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path

TABLES: dict[str, tuple[str, ...]] = {
    "agency": (
        "agency_id", "agency_name", "agency_url", "agency_timezone",
        "agency_lang", "agency_phone", "agency_fare_url",
    ),
    "calendar": (
        "service_id", "sunday", "monday", "tuesday", "wednesday",
        "thursday", "friday", "saturday", "start_date", "end_date",
    ),
    "routes": (
        "route_id", "agency_id", "route_short_name", "route_long_name",
        "route_desc", "route_type", "route_color",
    ),
    "stops": (
        "stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat",
        "stop_lon", "location_type", "parent_station", "zone_id",
    ),
    "stop_times": (
        "trip_id", "arrival_time", "departure_time", "stop_id",
        "stop_sequence", "pickup_type", "drop_off_type", "shape_dist_traveled",
    ),
    "trips": (
        "route_id", "service_id", "trip_id", "trip_headsign",
        "direction_id", "shape_id", "wheelchair_accessible",
    ),
}

RETENTION_DAYS = 7


class BronzeLoadError(Exception):
    """A bronze load cannot proceed: bad zip contents, header or counts."""


def extract_table(zip_path: Path, table: str, dest_dir: Path) -> Path:
    """Unpack only `<table>.txt` from the zip into dest_dir."""
    name = f"{table}.txt"
    with zipfile.ZipFile(zip_path) as zf:
        try:
            zf.getinfo(name)
        except KeyError:
            raise BronzeLoadError(f"{name} is not in {zip_path}") from None
        zf.extract(name, dest_dir)
    return dest_dir / name


def check_header(csv_path: Path, table: str) -> None:
    """Fail if the file's columns differ from the expected ones."""
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        first = f.readline()
    actual = first.rstrip("\r\n").split(",")
    expected = list(TABLES[table])
    missing = [c for c in expected if c not in actual]
    extra = [c for c in actual if c not in expected]
    if missing or extra:
        raise BronzeLoadError(
            f"{table}.txt header differs: missing {missing}, extra {extra}"
        )
    if actual != expected:
        raise BronzeLoadError(
            f"{table}.txt columns are reordered or duplicated: "
            f"got {actual}, expected {expected}"
        )


def count_rows(csv_path: Path) -> int:
    """Non-empty lines minus the header."""
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        lines = sum(1 for line in f if line.strip("\r\n"))
    return max(lines - 1, 0)


def load_table(
    spark,
    csv_path: Path,
    table: str,
    feed_date: date,
    source_key: str,
    table_path: str,
    loaded_at: datetime,
) -> int:
    """Write one feed file into its bronze Delta table as raw text.

    Replaces only this feed_date's partition. Returns the rows written.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([StructField(c, StringType(), True) for c in TABLES[table]])
    df = (
        spark.read.schema(schema)
        .option("header", "true")
        .option("enforceSchema", "true")
        .option("quote", "\u0000")
        .option("mode", "FAILFAST")
        .csv(str(csv_path))
        .withColumn("feed_date", F.lit(feed_date.isoformat()).cast("date"))
        .withColumn("_source_file", F.lit(source_key))
        .withColumn("_loaded_at", F.lit(loaded_at).cast("timestamp"))
    )
    iso = feed_date.isoformat()
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("feed_date")
        .option("replaceWhere", f"feed_date = '{iso}'")
        .save(table_path)
    )
    written = (
        spark.read.format("delta").load(table_path).filter(f"feed_date = '{iso}'").count()
    )
    expected = count_rows(csv_path)
    if written != expected:
        raise BronzeLoadError(
            f"{table}: wrote {written} rows for {iso} but the file has {expected}"
        )
    return written


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
