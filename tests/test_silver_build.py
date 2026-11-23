import itertools
import json
from datetime import date, datetime, timedelta

import pytest
from conftest import feed_text
from pyspark.sql import functions as F

from transit import silver
from transit.bronze import TABLES, load_table
from transit.lakehouse import table_path
from transit.silver import SILVER_TABLES

DAY = date(2026, 11, 15)
DAY2 = date(2026, 11, 16)
LOADED = datetime(2026, 11, 15, 6, 0, 0)
LONG_NAME = "ת. רכבת יבנה מערב-יבנה<->ת. רכבת יבנה מזרח-יבנה-1#"
_csv_names = itertools.count()


# ---------------------------------------------------------------- bronze row builders

def route(**kw):
    base = {
        "route_id": "1", "agency_id": "5", "route_short_name": "10",
        "route_long_name": LONG_NAME, "route_desc": "11337-1-#",
        "route_type": "3", "route_color": "FF0000",
    }
    return {**base, **kw}


def trip(**kw):
    base = {
        "route_id": "1", "service_id": "S1", "trip_id": "T1", "trip_headsign": "יבנה",
        "direction_id": "0", "shape_id": "SH1", "wheelchair_accessible": "1",
    }
    return {**base, **kw}


def stop_time(**kw):
    base = {
        "trip_id": "T1", "arrival_time": "07:00:00", "departure_time": "07:00:00",
        "stop_id": "100", "stop_sequence": "1", "pickup_type": "0",
        "drop_off_type": "0", "shape_dist_traveled": "0",
    }
    return {**base, **kw}


def stop(**kw):
    base = {
        "stop_id": "100", "stop_code": "21", "stop_name": "תחנה",
        "stop_desc": "רחוב: x עיר: y רציף: 15 קומה: 3", "stop_lat": "31.9",
        "stop_lon": "34.8", "location_type": "0", "parent_station": None, "zone_id": "1",
    }
    return {**base, **kw}


def stop_times(n, bad=()):
    """n stop times of trip T1, sequences 1..n; the sequences in `bad` get a malformed arrival."""
    return [
        stop_time(stop_sequence=str(i), arrival_time="ab:30:00" if i in bad else "07:00:00")
        for i in range(1, n + 1)
    ]


# ---------------------------------------------------------------- lakehouse helpers

def put(spark, root, table, rows, day=DAY):
    """Load rows into the bronze table's partition for `day` through load_table."""
    csv = root / "csv" / f"{table}-{day}-{next(_csv_names)}.txt"
    csv.parent.mkdir(exist_ok=True)
    data = [[r.get(c) or "" for c in TABLES[table]] for r in rows]
    csv.write_bytes(feed_text(table, data).encode("utf-8"))
    path = table_path(str(root), "bronze", table)
    load_table(spark, csv, table, day, "landing/gtfs/x.zip", path, LOADED)


def put_bus_trip(spark, root, day=DAY):
    put(spark, root, "routes", [route()], day)
    put(spark, root, "trips", [trip()], day)


def build(spark, root, table, day=DAY, today=None):
    return silver.build_silver_table(spark, str(root), table, day, today or day, LOADED)


def read(spark, root, table, day=None):
    """The silver table's rows (for one day if given); empty if the table doesn't exist."""
    from delta.tables import DeltaTable

    path = table_path(str(root), "silver", table)
    if not DeltaTable.isDeltaTable(spark, path):
        return []
    df = spark.read.format("delta").load(path)
    if day is not None:
        df = df.filter(F.col("feed_date") == F.lit(day))
    return df.collect()


def rejects(spark, root, table=None, day=DAY):
    rows = read(spark, root, "_rejects", day)
    return [r for r in rows if table is None or r["table"] == table]


def days_in(spark, root, table):
    return sorted({r["feed_date"] for r in read(spark, root, table)})


def plain(rows):
    return sorted(tuple(r.asDict().items()) for r in rows)


# ---------------------------------------------------------------- tests

def test_build_writes_typed_partition(spark, tmp_path):
    put(spark, tmp_path, "routes", [route(route_id="R1"), route(route_id="R2"),
                                    route(route_id="R3", route_type="2")])
    counts = build(spark, tmp_path, "routes")
    assert counts == silver.SilverCounts(kept=2, rejected=0, non_bus=1)

    df = spark.read.format("delta").load(table_path(str(tmp_path), "silver", "routes"))
    expected = [(c, "string") for c in SILVER_TABLES["routes"]] + [
        ("feed_date", "date"), ("_loaded_at", "timestamp"),
    ]
    assert [(f.name, f.dataType.simpleString()) for f in df.schema.fields] == expected
    assert sorted(r["route_id"] for r in df.collect()) == ["R1", "R2"]
    assert {r["feed_date"] for r in df.collect()} == {DAY}
    assert rejects(spark, tmp_path) == []


def test_rejects_are_written(spark, tmp_path):
    put_bus_trip(spark, tmp_path)
    put(spark, tmp_path, "stop_times", stop_times(1000, bad={500}))
    counts = build(spark, tmp_path, "stop_times")
    assert counts == silver.SilverCounts(kept=999, rejected=1, non_bus=0)
    assert len(read(spark, tmp_path, "stop_times", DAY)) == 999

    df = spark.read.format("delta").load(table_path(str(tmp_path), "silver", "_rejects"))
    assert [(f.name, f.dataType.simpleString()) for f in df.schema.fields] == [
        ("table", "string"), ("reason", "string"), ("row", "string"),
        ("feed_date", "date"), ("_loaded_at", "timestamp"),
    ]
    (bad,) = rejects(spark, tmp_path)
    assert (bad["table"], bad["reason"]) == ("stop_times", "bad_arrival_time")
    assert json.loads(bad["row"])["arrival_time"] == "ab:30:00"


def test_over_limit_fails_writes_rejects_and_no_partition(spark, tmp_path):
    put_bus_trip(spark, tmp_path)
    put(spark, tmp_path, "stop_times", stop_times(100, bad={7}))
    with pytest.raises(silver.SilverBuildError, match="stop_times") as err:
        build(spark, tmp_path, "stop_times")
    assert "bad_arrival_time" in str(err.value)
    assert [r["reason"] for r in rejects(spark, tmp_path, "stop_times")] == ["bad_arrival_time"]
    assert read(spark, tmp_path, "stop_times", DAY) == []


def test_over_limit_deletes_existing_partition(spark, tmp_path):
    for d in (DAY, DAY2):
        put_bus_trip(spark, tmp_path, d)
        put(spark, tmp_path, "stop_times", stop_times(100), d)
        build(spark, tmp_path, "stop_times", d, DAY2)
    other_day = plain(read(spark, tmp_path, "stop_times", DAY2))
    assert len(read(spark, tmp_path, "stop_times", DAY)) == 100

    put(spark, tmp_path, "stop_times", stop_times(100, bad={1, 2}), DAY)
    with pytest.raises(silver.SilverBuildError, match="stop_times"):
        build(spark, tmp_path, "stop_times", DAY, DAY2)
    assert read(spark, tmp_path, "stop_times", DAY) == []
    assert plain(read(spark, tmp_path, "stop_times", DAY2)) == other_day


def test_rebuild_clears_old_rejects(spark, tmp_path):
    # routes: one bad route of two is over the limit, which still leaves its reject
    put(spark, tmp_path, "routes", [route(), route(route_id="9", route_desc="x")])
    with pytest.raises(silver.SilverBuildError, match="routes"):
        build(spark, tmp_path, "routes")
    assert len(rejects(spark, tmp_path, "routes")) == 1

    put(spark, tmp_path, "trips", [trip()])
    put(spark, tmp_path, "stop_times", stop_times(1000, bad={3}))
    build(spark, tmp_path, "stop_times")
    assert len(rejects(spark, tmp_path, "stop_times")) == 1

    put(spark, tmp_path, "stop_times", stop_times(1000))
    counts = build(spark, tmp_path, "stop_times")
    assert counts == silver.SilverCounts(kept=1000, rejected=0, non_bus=0)
    assert rejects(spark, tmp_path, "stop_times") == []
    assert [r["reason"] for r in rejects(spark, tmp_path, "routes")] == ["bad_route_desc"]


def test_same_day_twice_and_second_day(spark, tmp_path):
    put(spark, tmp_path, "routes", [route(route_id="R1"), route(route_id="R2")])
    build(spark, tmp_path, "routes")
    first = plain(read(spark, tmp_path, "routes", DAY))
    build(spark, tmp_path, "routes")
    assert plain(read(spark, tmp_path, "routes", DAY)) == first
    assert len(first) == 2

    put(spark, tmp_path, "routes", [route(route_id="R3")], DAY2)
    build(spark, tmp_path, "routes", DAY2)
    assert plain(read(spark, tmp_path, "routes", DAY)) == first
    assert [r["route_id"] for r in read(spark, tmp_path, "routes", DAY2)] == ["R3"]


def test_missing_bronze_fails_and_writes_nothing(spark, tmp_path):
    put(spark, tmp_path, "trips", [trip()])
    with pytest.raises(silver.SilverBuildError, match="routes"):
        build(spark, tmp_path, "trips")
    # bronze routes exists, but has no partition for the day
    put(spark, tmp_path, "routes", [route()], DAY2)
    with pytest.raises(silver.SilverBuildError, match="routes.*2026-11-15"):
        build(spark, tmp_path, "trips")
    assert not (tmp_path / "silver" / "trips").exists()
    assert not (tmp_path / "silver" / "_rejects").exists()


def test_retention_on_table_and_rejects(spark, tmp_path, monkeypatch):
    # A loose limit lets each day's build pass with a reject, so _rejects has a row every day.
    monkeypatch.setattr(silver, "REJECT_LIMIT", 0.5)
    first = date(2026, 11, 6)
    days = [first + timedelta(days=i) for i in range(10)]
    for d in days:
        put(spark, tmp_path, "routes", [route(), route(route_id="2"),
                                        route(route_id="9", route_desc="x")], d)
        assert build(spark, tmp_path, "routes", d, days[-1]) == silver.SilverCounts(2, 1, 0)
    assert days_in(spark, tmp_path, "routes") == days[2:]
    assert days_in(spark, tmp_path, "_rejects") == days[2:]


def test_bad_stop_keeps_its_stop_times(spark, tmp_path, monkeypatch):
    # One bad stop of two is over the real limit; a loose one lets the build finish.
    monkeypatch.setattr(silver, "REJECT_LIMIT", 0.5)
    put_bus_trip(spark, tmp_path)
    put(spark, tmp_path, "stop_times", [stop_time(stop_sequence="1", stop_id="100"),
                                        stop_time(stop_sequence="2", stop_id="200")])
    put(spark, tmp_path, "stops", [stop(stop_id="100", stop_lat="40.0"), stop(stop_id="200")])

    assert build(spark, tmp_path, "stops") == silver.SilverCounts(kept=1, rejected=1, non_bus=0)
    assert [r["stop_id"] for r in read(spark, tmp_path, "stops", DAY)] == ["200"]
    assert [r["reason"] for r in rejects(spark, tmp_path, "stops")] == ["bad_stop_lat"]

    counts = build(spark, tmp_path, "stop_times")
    assert counts == silver.SilverCounts(kept=2, rejected=0, non_bus=0)
    assert sorted(r["stop_id"] for r in read(spark, tmp_path, "stop_times", DAY)) == ["100", "200"]
