"""Silver layer: the bus filter and the rules that type bronze's all-text GTFS columns.

Spark 4 runs with ANSI mode on, so every cast here is guarded by a regex match or is a try_ cast.
"""

from dataclasses import dataclass
from datetime import date, datetime

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from transit.bronze import TABLES
from transit.lakehouse import SOURCES, cleanup, table_path

_TIME = r"^(\d{1,2}):([0-5]\d):([0-5]\d)$"
_ROUTE_DESC = r"^(\d+)-([123])-(.+)$"
_STOP_DESC = r"^(?s)רחוב:(.*)עיר:(.*)רציף:(.*)קומה:(.*)$"


def _blank_to_null(c: Column) -> Column:
    """Trim, and turn the empty string into null."""
    t = F.trim(c)
    return F.when(t == "", F.lit(None)).otherwise(t)


def gtfs_seconds(c: Column) -> Column:
    """Seconds since midnight from H:MM:SS (hours may pass 24), null if malformed."""
    secs = (
        F.regexp_extract(c, _TIME, 1).cast("int") * 3600
        + F.regexp_extract(c, _TIME, 2).cast("int") * 60
        + F.regexp_extract(c, _TIME, 3).cast("int")
    )
    return F.when(c.rlike(_TIME), secs)


def route_desc_parts(c: Column) -> Column:
    """Split route_desc '<line>-<direction>-<variant>' into a struct, null if it doesn't match."""
    parts = F.struct(
        F.regexp_extract(c, _ROUTE_DESC, 1).alias("line_id"),
        F.regexp_extract(c, _ROUTE_DESC, 2).alias("route_direction"),
        F.regexp_extract(c, _ROUTE_DESC, 3).alias("route_variant"),
    )
    return F.when(c.rlike(_ROUTE_DESC), parts)


def _name_city(side: Column) -> tuple[Column, Column]:
    """Name and city of one side: the city is the text after the last '-'."""
    has_dash = side.contains("-")
    name = F.when(has_dash, F.regexp_extract(side, r"^(?s)(.*)-([^-]*)$", 1)).otherwise(side)
    city = F.when(has_dash, F.regexp_extract(side, r"^(?s)(.*)-([^-]*)$", 2))
    return _blank_to_null(name), _blank_to_null(city)


def long_name_parts(long_name: Column, direction: Column, variant: Column) -> Column:
    """Split 'origin-city<->destination-city-<direction><variant>' into a struct.

    Null unless there is exactly one '<->' and the right side ends with the suffix.
    """
    sides = F.split(long_name, "<->", -1)
    left = F.try_element_at(sides, F.lit(1))
    right = F.try_element_at(sides, F.lit(2))
    suffix = F.concat(F.lit("-"), direction, variant)
    right = F.substring(right, 1, F.length(right) - F.length(suffix))
    origin_name, origin_city = _name_city(left)
    dest_name, dest_city = _name_city(right)
    parts = F.struct(
        origin_name.alias("origin_name"),
        origin_city.alias("origin_city"),
        dest_name.alias("destination_name"),
        dest_city.alias("destination_city"),
    )
    ok = (F.size(sides) == 2) & F.try_element_at(sides, F.lit(2)).endswith(suffix)
    return F.when(ok, parts)


def stop_desc_parts(c: Column) -> Column:
    """Split stop_desc 'רחוב: .. עיר: .. רציף: .. קומה: ..' into a struct; fields null if no match."""
    ok = c.rlike(_STOP_DESC)

    def field(i: int, name: str) -> Column:
        return F.when(ok, _blank_to_null(F.regexp_extract(c, _STOP_DESC, i))).alias(name)

    return F.struct(
        field(1, "street"), field(2, "city"), field(3, "platform"), field(4, "floor")
    )


# ---------------------------------------------------------------- tables

SILVER_TABLES: dict[str, tuple[str, ...]] = {
    "routes": (
        "route_id", "operator_id", "line_public_number", "line_id", "route_direction",
        "route_variant", "origin_name", "origin_city", "destination_name",
        "destination_city", "route_color",
    ),
    "trips": (
        "trip_id", "route_id", "service_id", "trip_headsign", "direction_id",
        "shape_id", "wheelchair_accessible",
    ),
    "stop_times": (
        "trip_id", "stop_sequence", "stop_id", "arrival_seconds", "departure_seconds",
        "boarding_allowed", "alighting_allowed", "dist_from_trip_start_m",
    ),
    "stops": (
        "stop_id", "stop_public_code", "stop_name", "street", "city", "platform",
        "floor", "lat", "lon", "location_type", "station_id",
    ),
    "operators": ("operator_id", "operator_name", "operator_url", "operator_phone"),
    "calendar": (
        "service_id", "sunday", "monday", "tuesday", "wednesday", "thursday",
        "friday", "saturday", "start_date", "end_date",
    ),
}

KEYS: dict[str, tuple[str, ...]] = {
    "routes": ("route_id",),
    "trips": ("trip_id",),
    "stop_times": ("trip_id", "stop_sequence"),
    "stops": ("stop_id",),
    "operators": ("operator_id",),
    "calendar": ("service_id",),
}

_DAYS = ("sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday")
_NUMBER = r"^-?\d+(\.\d+)?$"


# ---------------------------------------------------------------- bus filter

def bus_rows(table: str, bronze: dict[str, DataFrame]) -> DataFrame:
    """The bus rows of bronze[SOURCES[table][0]], decided from bronze only, columns unchanged."""

    def routes() -> DataFrame:
        return bronze["routes"].filter(F.trim(F.col("route_type")) == "3")

    def trips() -> DataFrame:
        return bronze["trips"].join(routes().select("route_id"), "route_id", "left_semi")

    def stop_times() -> DataFrame:
        return bronze["stop_times"].join(trips().select("trip_id"), "trip_id", "left_semi")

    if table == "routes":
        return routes()
    if table == "trips":
        return trips()
    if table == "stop_times":
        return stop_times()
    if table == "stops":
        stops = bronze["stops"]
        visited = stop_times().select("stop_id")
        stations = (
            stops.join(visited, "stop_id", "left_semi")
            .select(F.col("parent_station").alias("stop_id"))
            .filter(~_blank(F.col("stop_id")))
        )
        return stops.join(visited.union(stations), "stop_id", "left_semi")
    if table == "operators":
        return bronze["agency"].join(routes().select("agency_id"), "agency_id", "left_semi")
    if table == "calendar":
        return bronze["calendar"].join(trips().select("service_id"), "service_id", "left_semi")
    raise ValueError(f"unknown silver table {table!r}")


# ---------------------------------------------------------------- column rules
#
# A rule is (names, values, reason): the silver columns it produces, their values, and the
# reason its bronze column gives (null when fine; None for an optional column). Rules are
# listed in SILVER_TABLES order, so the first non-null reason is the first failing column.

def _blank(c: Column) -> Column:
    return c.isNull() | (F.trim(c) == "")


def _matched(src: str, pattern: str, value: Column) -> Column:
    """`value` when the trimmed bronze column matches `pattern`, else null."""
    return F.when(F.trim(F.col(src)).rlike(pattern), value)


def _required(src: str, names: tuple[str, ...], values: tuple[Column, ...], ok: Column):
    """A required column: missing_<src> when empty, bad_<src> when not `ok`."""
    reason = F.when(_blank(F.col(src)), F.lit(f"missing_{src}")).when(~ok, F.lit(f"bad_{src}"))
    return names, values, reason


def _req_text(src: str, name: str):
    c = F.col(src)
    return _required(src, (name,), (F.when(~_blank(c), c),), F.lit(True))


def _req_value(src: str, name: str, value: Column):
    """A required single typed column; a null `value` means malformed."""
    return _required(src, (name,), (value,), value.isNotNull())


def _optional(name: str, value: Column):
    return (name,), (value,), None


def _opt_text(src: str, name: str):
    c = F.col(src)
    return _optional(name, F.when(~_blank(c), c))


def _int_of(src: str, pattern: str) -> Column:
    return _matched(src, pattern, F.trim(F.col(src)).try_cast("int"))


def _double_in(src: str, low: float, high: float) -> Column:
    d = _matched(src, _NUMBER, F.trim(F.col(src)).try_cast("double"))
    return F.when(d.between(low, high), d)


def _flag(src: str) -> Column:
    """'0' / '1' as a boolean, else null."""
    return _matched(src, r"^[01]$", F.trim(F.col(src)) == "1")


def _allowed(src: str) -> Column:
    """pickup_type / drop_off_type: '0' or empty -> true, '1'-'3' -> false, else null."""
    t = F.trim(F.col(src))
    return F.when(_blank(F.col(src)) | (t == "0"), F.lit(True)).when(
        t.rlike(r"^[123]$"), F.lit(False)
    )


def _allowed_rule(src: str, name: str):
    """Empty is valid here (it means 0), so the only reason is bad_<src>."""
    value = _allowed(src)
    return (name,), (value,), F.when(value.isNull(), F.lit(f"bad_{src}"))


def _date_of(src: str) -> Column:
    ts = F.try_to_timestamp(F.trim(F.col(src)), F.lit("yyyyMMdd"))
    return _matched(src, r"^\d{8}$", ts.cast("date"))


def _rules(table: str) -> list:
    if table == "routes":
        desc = route_desc_parts(F.col("route_desc"))
        names = long_name_parts(
            F.col("route_long_name"), desc["route_direction"], desc["route_variant"]
        )
        desc_cols = ("line_id", "route_direction", "route_variant")
        long_cols = ("origin_name", "origin_city", "destination_name", "destination_city")
        return [
            _req_text("route_id", "route_id"),
            _req_text("agency_id", "operator_id"),
            _opt_text("route_short_name", "line_public_number"),
            _required("route_desc", desc_cols, tuple(desc[n] for n in desc_cols), desc.isNotNull()),
            _required(
                "route_long_name", long_cols, tuple(names[n] for n in long_cols),
                names.isNotNull(),
            ),
            _opt_text("route_color", "route_color"),
        ]
    if table == "trips":
        return [
            _req_text("trip_id", "trip_id"),
            _req_text("route_id", "route_id"),
            _req_text("service_id", "service_id"),
            _optional("trip_headsign", F.col("trip_headsign")),
            _optional("direction_id", _int_of("direction_id", r"^[01]$")),
            _opt_text("shape_id", "shape_id"),
            _optional("wheelchair_accessible", _int_of("wheelchair_accessible", r"^[0-2]$")),
        ]
    if table == "stop_times":
        meters = F.round(F.trim(F.col("shape_dist_traveled")).try_cast("double"), 0)
        return [
            _req_text("trip_id", "trip_id"),
            _req_value("stop_sequence", "stop_sequence", _int_of("stop_sequence", r"^\d+$")),
            _req_text("stop_id", "stop_id"),
            _req_value("arrival_time", "arrival_seconds", gtfs_seconds(F.trim(F.col("arrival_time")))),
            _req_value(
                "departure_time", "departure_seconds", gtfs_seconds(F.trim(F.col("departure_time")))
            ),
            _allowed_rule("pickup_type", "boarding_allowed"),
            _allowed_rule("drop_off_type", "alighting_allowed"),
            _optional(
                "dist_from_trip_start_m",
                _matched("shape_dist_traveled", _NUMBER, meters.try_cast("int")),
            ),
        ]
    if table == "stops":
        desc = stop_desc_parts(F.col("stop_desc"))
        parts = ("street", "city", "platform", "floor")
        return [
            _req_text("stop_id", "stop_id"),
            _opt_text("stop_code", "stop_public_code"),
            _opt_text("stop_name", "stop_name"),
            (parts, tuple(desc[p] for p in parts), None),
            _req_value("stop_lat", "lat", _double_in("stop_lat", 29.0, 34.0)),
            _req_value("stop_lon", "lon", _double_in("stop_lon", 34.0, 36.0)),
            _req_value("location_type", "location_type", _int_of("location_type", r"^[01]$")),
            _opt_text("parent_station", "station_id"),
        ]
    if table == "operators":
        return [
            _req_text("agency_id", "operator_id"),
            _req_text("agency_name", "operator_name"),
            _opt_text("agency_url", "operator_url"),
            _opt_text("agency_phone", "operator_phone"),
        ]
    if table == "calendar":
        return [
            _req_text("service_id", "service_id"),
            *(_req_value(d, d, _flag(d)) for d in _DAYS),
            _req_value("start_date", "start_date", _date_of("start_date")),
            _req_value("end_date", "end_date", _date_of("end_date")),
        ]
    raise ValueError(f"unknown silver table {table!r}")


def _cross_checks(table: str) -> list[Column]:
    """Checks between typed silver columns, run after the per-column ones."""
    if table == "stop_times":
        early = F.col("departure_seconds") < F.col("arrival_seconds")
        return [F.when(early, F.lit("departure_before_arrival"))]
    if table == "calendar":
        return [F.when(F.col("start_date") > F.col("end_date"), F.lit("start_after_end"))]
    return []


def clean(table: str, df: DataFrame) -> DataFrame:
    """Type and check bus rows: one row per input, the silver columns, then _reason and _raw.

    _reason is null for a kept row, else the first failing check: the columns in
    SILVER_TABLES order, then the cross-column checks, then duplicate_key (the key appears
    more than once among all the given rows). _raw is the bronze data columns as JSON.
    """
    rules = _rules(table)
    produced = [n for names, _, _ in rules for n in names]
    if produced != list(SILVER_TABLES[table]):
        raise AssertionError(f"{table} rules give {produced}, not SILVER_TABLES")
    columns = [v.alias(n) for names, values, _ in rules for n, v in zip(names, values)]
    none = F.lit(None).cast("string")
    column_reason = F.coalesce(*[r for _, _, r in rules if r is not None], none)
    raw = F.to_json(F.struct(*TABLES[SOURCES[table][0]]), {"ignoreNullFields": "false"})

    typed = df.select(*columns, column_reason.alias("_reason"), raw.alias("_raw"))
    reason = F.coalesce(F.col("_reason"), *_cross_checks(table), none)
    copies = F.count(F.lit(1)).over(Window.partitionBy(*KEYS[table]))
    reason = F.coalesce(reason, F.when(copies > 1, F.lit("duplicate_key")))
    return typed.withColumn("_reason", reason).select(*SILVER_TABLES[table], "_reason", "_raw")


# ---------------------------------------------------------------- building one table

REJECT_LIMIT = 0.001  # rejected / bus rows above this fails the build
REJECTS = "_rejects"


class SilverBuildError(Exception):
    """A silver build cannot proceed: missing bronze, too many rejects, or a count mismatch."""


@dataclass(frozen=True)
class SilverCounts:
    kept: int
    rejected: int
    non_bus: int


def _read_bronze(spark, root: str, feed_date: date, name: str) -> DataFrame:
    """The bronze table's partition for the day; fails if the table or the day is missing."""
    from delta.tables import DeltaTable

    path = table_path(root, "bronze", name)
    iso = feed_date.isoformat()
    if not DeltaTable.isDeltaTable(spark, path):
        raise SilverBuildError(f"bronze {name} does not exist at {path}, needed for {iso}")
    df = spark.read.format("delta").load(path).filter(F.col("feed_date") == F.lit(feed_date))
    if df.isEmpty():
        raise SilverBuildError(f"bronze {name} has no rows for {iso}")
    return df


def _write_day(df: DataFrame, path: str, replace_where: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("feed_date")
        .option("replaceWhere", replace_where)
        .save(path)
    )


def build_silver_table(
    spark, root: str, table: str, feed_date: date, today: date, loaded_at: datetime
) -> SilverCounts:
    """Build one silver table for one day from bronze, with its rejects, then apply retention.

    Raises SilverBuildError when a bronze partition is missing (before writing anything), when
    rejects exceed REJECT_LIMIT of the bus rows (after writing the rejects and deleting the
    day's silver partition), or when the rows written differ from the rows kept.
    """
    from delta.tables import DeltaTable

    iso = feed_date.isoformat()
    bronze = {name: _read_bronze(spark, root, feed_date, name) for name in SOURCES[table]}
    stamp = [
        F.lit(feed_date).alias("feed_date"),
        F.lit(loaded_at).cast("timestamp").alias("_loaded_at"),
    ]
    silver_path = table_path(root, "silver", table)
    rejects_path = table_path(root, "silver", REJECTS)

    rows = clean(table, bus_rows(table, bronze)).cache()
    try:
        rejected_rows = rows.filter(F.col("_reason").isNotNull())
        kept = rows.filter(F.col("_reason").isNull()).count()
        rejected = rejected_rows.count()
        non_bus = bronze[SOURCES[table][0]].count() - kept - rejected

        rejects = rejected_rows.select(
            F.lit(table).alias("table"), F.col("_reason").alias("reason"),
            F.col("_raw").alias("row"), *stamp,
        )
        _write_day(rejects, rejects_path, f"feed_date = '{iso}' AND `table` = '{table}'")

        bus = kept + rejected
        if bus and rejected / bus > REJECT_LIMIT:
            if DeltaTable.isDeltaTable(spark, silver_path):
                DeltaTable.forPath(spark, silver_path).delete(f"feed_date = '{iso}'")
            top = (
                rejected_rows.groupBy("_reason").count()
                .orderBy(F.desc("count"), "_reason").limit(3).collect()
            )
            reasons = ", ".join(f"{r['_reason']} ({r['count']})" for r in top)
            raise SilverBuildError(
                f"silver {table} {iso}: {rejected} of {bus} bus rows rejected, over the "
                f"{REJECT_LIMIT:.1%} limit (kept {kept}); most common reasons: {reasons}"
            )

        kept_rows = rows.filter(F.col("_reason").isNull()).select(*SILVER_TABLES[table], *stamp)
        _write_day(kept_rows, silver_path, f"feed_date = '{iso}'")
    finally:
        rows.unpersist()

    day = F.col("feed_date") == F.lit(feed_date)
    written = spark.read.format("delta").load(silver_path).filter(day).count()
    if written != kept:
        raise SilverBuildError(f"silver {table}: wrote {written} rows for {iso} but kept {kept}")

    cleanup(spark, silver_path, today, feed_date)
    cleanup(spark, rejects_path, today, feed_date)
    return SilverCounts(kept=kept, rejected=rejected, non_bus=non_bus)
