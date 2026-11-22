from datetime import date, datetime
from pathlib import Path

import pytest
from conftest import feed_text

from transit.bronze import TABLES, load_table

DAY1 = date(2026, 11, 15)
DAY2 = date(2026, 11, 16)
LOADED1 = datetime(2026, 11, 15, 6, 0, 0)
LOADED2 = datetime(2026, 11, 16, 6, 0, 0)


def key(d: date) -> str:
    return f"landing/gtfs/feed_date={d.isoformat()}/gtfs.zip"


def stop(i: str, **over: str) -> list[str]:
    row = dict(zip(TABLES["stops"], [i, "c" + i, "name" + i, "desc", "32.1", "34.8", "0", "", "5"]))
    row.update(over)
    return [row[c] for c in TABLES["stops"]]


def write_csv(tmp_path: Path, table: str, rows: list[list[str]], name: str = "in.txt", **kw) -> Path:
    p = tmp_path / name
    p.write_bytes(feed_text(table, rows, **kw).encode("utf-8"))
    return p


def load(spark, tmp_path, table, rows, day=DAY1, loaded_at=LOADED1, **kw):
    csv = write_csv(tmp_path, table, rows, name=f"{table}-{day}.txt", **kw)
    return load_table(spark, csv, table, day, key(day), str(tmp_path / table), loaded_at)


def files_written(tmp_path, table):
    """Every file under the table path: empty if the failed load wrote nothing."""
    root = tmp_path / table
    return [str(p) for p in root.rglob("*") if p.is_file()] if root.exists() else []


def read(spark, tmp_path, table):
    return spark.read.format("delta").load(str(tmp_path / table))


def test_columns_are_text_with_original_names(spark, tmp_path):
    n = load(spark, tmp_path, "stops", [stop("1"), stop("2")])
    assert n == 2
    schema = read(spark, tmp_path, "stops").schema
    expected = [(c, "string") for c in TABLES["stops"]] + [
        ("feed_date", "date"), ("_source_file", "string"), ("_loaded_at", "timestamp"),
    ]
    assert [(f.name, f.dataType.simpleString()) for f in schema.fields] == expected
    assert schema.fields[0].name == "stop_id"


def test_bare_quote_is_kept(spark, tmp_path):
    load(spark, tmp_path, "stops", [stop("1", stop_name='קק"ל')])
    assert read(spark, tmp_path, "stops").first()["stop_name"] == 'קק"ל'


def test_time_past_midnight_is_kept(spark, tmp_path):
    row = ["t1", "25:30:00", "25:30:00", "s1", "1", "0", "0", ""]
    load(spark, tmp_path, "stop_times", [row])
    assert read(spark, tmp_path, "stop_times").first()["arrival_time"] == "25:30:00"


def test_no_trailing_carriage_return(spark, tmp_path):
    load(spark, tmp_path, "stops", [stop("1", zone_id="5")], crlf=True)
    assert read(spark, tmp_path, "stops").first()["zone_id"] == "5"


def test_empty_field_is_null(spark, tmp_path):
    load(spark, tmp_path, "stops", [stop("1", stop_desc="")])
    assert read(spark, tmp_path, "stops").first()["stop_desc"] is None


def test_same_day_twice_keeps_one_copy(spark, tmp_path):
    rows = [stop("1"), stop("2")]
    load(spark, tmp_path, "stops", rows)
    load(spark, tmp_path, "stops", rows)
    assert read(spark, tmp_path, "stops").count() == 2


def test_second_day_leaves_first_unchanged(spark, tmp_path):
    load(spark, tmp_path, "stops", [stop("1"), stop("2")], DAY1, LOADED1)
    load(spark, tmp_path, "stops", [stop("1"), stop("2"), stop("3")], DAY2, LOADED2)
    df = read(spark, tmp_path, "stops")
    day1 = df.filter(f"feed_date = '{DAY1}'").collect()
    assert sorted(r["stop_id"] for r in day1) == ["1", "2"]
    assert {r["_loaded_at"] for r in day1} == {LOADED1}
    assert df.filter(f"feed_date = '{DAY2}'").count() == 3


def test_long_row_fails(spark, tmp_path):
    bad = stop("1") + ["extra"]
    with pytest.raises(Exception, match="MALFORMED"):
        load(spark, tmp_path, "stops", [stop("0"), bad])
    assert files_written(tmp_path, "stops") == []


def test_short_row_fails(spark, tmp_path):
    bad = stop("1")[:-1]
    with pytest.raises(Exception, match="MALFORMED"):
        load(spark, tmp_path, "stops", [stop("0"), bad])
    assert files_written(tmp_path, "stops") == []


def test_failed_reload_keeps_existing_rows(spark, tmp_path):
    load(spark, tmp_path, "stops", [stop("1"), stop("2")], DAY1, LOADED1)
    bad = stop("3")[:-1]
    with pytest.raises(Exception, match="MALFORMED"):
        load(spark, tmp_path, "stops", [stop("1"), bad], DAY1, LOADED2)
    day = read(spark, tmp_path, "stops").filter(f"feed_date = '{DAY1}'").collect()
    assert sorted(r["stop_id"] for r in day) == ["1", "2"]
    assert {r["_loaded_at"] for r in day} == {LOADED1}

