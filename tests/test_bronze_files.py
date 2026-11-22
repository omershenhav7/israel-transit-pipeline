import pytest

from conftest import feed_text, make_zip
from transit.bronze import BronzeLoadError, check_header, count_rows, extract_table

STOP_ROW = ["1", "100", "Central", "", "32.0", "34.8", "0", "", "5"]


def test_extract_only_the_requested_file(tmp_path):
    zp = make_zip(
        tmp_path / "g.zip",
        {"stops.txt": feed_text("stops", [STOP_ROW]), "trips.txt": "x\n"},
    )
    dest = tmp_path / "out"
    dest.mkdir()
    path = extract_table(zp, "stops", dest)
    assert path == dest / "stops.txt"
    assert [p.name for p in dest.iterdir()] == ["stops.txt"]


def test_extract_missing_file_fails(tmp_path):
    zp = make_zip(tmp_path / "g.zip", {"trips.txt": "x\n"})
    with pytest.raises(BronzeLoadError, match="stops.txt"):
        extract_table(zp, "stops", tmp_path)


def test_header_with_bom_passes(tmp_path):
    p = tmp_path / "stops.txt"
    p.write_bytes(feed_text("stops", [STOP_ROW], bom=True).encode("utf-8"))
    check_header(p, "stops")


def test_missing_column_fails(tmp_path):
    p = tmp_path / "stops.txt"
    text = feed_text("stops", [STOP_ROW]).replace(",zone_id", "", 1)
    p.write_bytes(text.encode("utf-8"))
    with pytest.raises(BronzeLoadError, match="zone_id"):
        check_header(p, "stops")


def test_extra_column_fails(tmp_path):
    p = tmp_path / "stops.txt"
    text = feed_text("stops", [STOP_ROW]).replace("zone_id", "zone_id,platform_code", 1)
    p.write_bytes(text.encode("utf-8"))
    with pytest.raises(BronzeLoadError, match="platform_code"):
        check_header(p, "stops")


def test_count_rows_ignores_header_and_blank_lines(tmp_path):
    p = tmp_path / "stops.txt"
    text = feed_text("stops", [STOP_ROW, STOP_ROW, STOP_ROW]) + "\r\n"
    p.write_bytes(text.encode("utf-8"))
    assert count_rows(p) == 3


def test_reordered_columns_fail(tmp_path):
    p = tmp_path / "stops.txt"
    text = feed_text("stops", [STOP_ROW]).replace("stop_lat,stop_lon", "stop_lon,stop_lat", 1)
    p.write_bytes(text.encode("utf-8"))
    with pytest.raises(BronzeLoadError, match="reordered"):
        check_header(p, "stops")


def test_duplicate_column_fails(tmp_path):
    p = tmp_path / "stops.txt"
    text = feed_text("stops", [STOP_ROW]).replace("zone_id", "zone_id,zone_id", 1)
    p.write_bytes(text.encode("utf-8"))
    with pytest.raises(BronzeLoadError, match="duplicated"):
        check_header(p, "stops")
