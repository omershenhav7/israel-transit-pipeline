import datetime as dt
import json

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from transit.bronze import TABLES
from transit.silver import KEYS, SILVER_TABLES, SOURCES, bus_rows, clean

LONG_NAME = "ת. רכבת יבנה מערב-יבנה<->ת. רכבת יבנה מזרח-יבנה-1#"


def bronze(spark, table, rows):
    """A bronze-shaped DataFrame: the table's text columns (missing keys are null), plus feed_date etc."""
    schema = StructType([StructField(c, StringType(), True) for c in TABLES[table]])
    data = [tuple(r.get(c) for c in TABLES[table]) for r in rows]
    return (
        spark.createDataFrame(data, schema)
        .withColumn("feed_date", F.lit("2026-11-15").cast("date"))
        .withColumn("_source_file", F.lit("landing/gtfs/2026-11-15.zip"))
        .withColumn("_loaded_at", F.current_timestamp())
    )


def cleaned(spark, table, rows):
    """clean() over the given bronze rows, as a list of Row."""
    return clean(table, bronze(spark, SOURCES[table][0], rows)).collect()


def reasons(spark, table, rows):
    return [r._reason for r in cleaned(spark, table, rows)]


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


def agency(**kw):
    base = {
        "agency_id": "5", "agency_name": "אגד", "agency_url": "http://x",
        "agency_timezone": "Asia/Jerusalem", "agency_lang": "he",
        "agency_phone": "*2800", "agency_fare_url": None,
    }
    return {**base, **kw}


def calendar(**kw):
    base = {
        "service_id": "S1", "sunday": "1", "monday": "1", "tuesday": "1",
        "wednesday": "1", "thursday": "1", "friday": "0", "saturday": "0",
        "start_date": "20261115", "end_date": "20261130",
    }
    return {**base, **kw}


def test_interfaces():
    assert set(SILVER_TABLES) == set(KEYS) == set(SOURCES)
    assert KEYS["stop_times"] == ("trip_id", "stop_sequence")
    assert SOURCES["stops"] == ("stops", "stop_times", "trips", "routes")
    assert SOURCES["operators"] == ("agency", "routes")
    for table, key in KEYS.items():
        assert set(key) <= set(SILVER_TABLES[table])


def test_bus_rows(spark):
    b = {
        "routes": bronze(spark, "routes", [route(route_id="R3", route_type="3", agency_id="A1"),
                                           route(route_id="R2", route_type="2", agency_id="A2")]),
        "trips": bronze(spark, "trips", [trip(trip_id="TB", route_id="R3", service_id="SB"),
                                         trip(trip_id="TR", route_id="R2", service_id="SR"),
                                         trip(trip_id="TX", route_id="404", service_id="SX")]),
        "stop_times": bronze(spark, "stop_times", [stop_time(trip_id="TB", stop_id="P1"),
                                                   stop_time(trip_id="TR", stop_id="P2"),
                                                   stop_time(trip_id="TX", stop_id="P3")]),
        "stops": bronze(spark, "stops", [stop(stop_id="P1", parent_station="ST"),
                                         stop(stop_id="P2"),
                                         stop(stop_id="P3"),
                                         stop(stop_id="ST", location_type="1"),
                                         stop(stop_id="UNUSED")]),
        "agency": bronze(spark, "agency", [agency(agency_id="A1"), agency(agency_id="A2")]),
        "calendar": bronze(spark, "calendar", [calendar(service_id="SB"), calendar(service_id="SR"),
                                               calendar(service_id="SX")]),
    }

    def ids(table, col):
        return sorted(r[col] for r in bus_rows(table, b).collect())

    assert ids("routes", "route_id") == ["R3"]
    assert ids("trips", "trip_id") == ["TB"]
    assert ids("stop_times", "trip_id") == ["TB"]
    assert ids("stops", "stop_id") == ["P1", "ST"]
    assert ids("operators", "agency_id") == ["A1"]
    assert ids("calendar", "service_id") == ["SB"]
    # bronze columns unchanged
    assert bus_rows("stops", b).columns == b["stops"].columns


def test_routes_clean(spark):
    rows = cleaned(spark, "routes", [route()])
    assert list(rows[0].asDict()) == list(SILVER_TABLES["routes"]) + ["_reason", "_raw"]
    r = rows[0]
    assert r._reason is None
    assert (r.route_id, r.operator_id, r.line_public_number, r.route_color) == ("1", "5", "10", "FF0000")
    assert (r.line_id, r.route_direction, r.route_variant) == ("11337", "1", "#")
    assert (r.origin_name, r.origin_city, r.destination_name, r.destination_city) == (
        "ת. רכבת יבנה מערב", "יבנה", "ת. רכבת יבנה מזרח", "יבנה"
    )

    assert reasons(spark, "routes", [route(route_id="")]) == ["missing_route_id"]
    assert reasons(spark, "routes", [route(route_id=None)]) == ["missing_route_id"]
    assert reasons(spark, "routes", [route(route_desc="11337-9-0")]) == ["bad_route_desc"]
    assert reasons(spark, "routes", [route(route_long_name="a-b<->c-d<->e-f-1#")]) == ["bad_route_long_name"]

    (r,) = cleaned(spark, "routes", [route(route_short_name="")])
    assert r._reason is None and r.line_public_number is None


def test_trips_clean(spark):
    (r,) = cleaned(spark, "trips", [trip()])
    assert r._reason is None
    assert (r.trip_id, r.route_id, r.service_id, r.shape_id) == ("T1", "1", "S1", "SH1")
    assert (r.direction_id, r.wheelchair_accessible) == (0, 1)

    (r,) = cleaned(spark, "trips", [trip(direction_id="7")])
    assert r._reason is None and r.direction_id is None
    assert reasons(spark, "trips", [trip(service_id="")]) == ["missing_service_id"]
    (r,) = cleaned(spark, "trips", [trip(trip_headsign='קק"ל')])
    assert r._reason is None and r.trip_headsign == 'קק"ל'


def test_stop_times_clean(spark):
    (r,) = cleaned(spark, "stop_times", [stop_time(arrival_time="25:30:00", departure_time="25:31:00")])
    assert r._reason is None
    assert (r.arrival_seconds, r.departure_seconds, r.stop_sequence) == (91800, 91860, 1)

    (r,) = cleaned(spark, "stop_times", [stop_time(pickup_type="", drop_off_type="1")])
    assert r._reason is None and r.boarding_allowed is True and r.alighting_allowed is False
    assert reasons(spark, "stop_times", [stop_time(pickup_type="9")]) == ["bad_pickup_type"]

    (r,) = cleaned(spark, "stop_times", [stop_time(shape_dist_traveled="1234.6")])
    assert r._reason is None and r.dist_from_trip_start_m == 1235
    (r,) = cleaned(spark, "stop_times", [stop_time(shape_dist_traveled="x")])
    assert r._reason is None and r.dist_from_trip_start_m is None

    assert reasons(
        spark, "stop_times", [stop_time(arrival_time="07:05:00", departure_time="07:00:00")]
    ) == ["departure_before_arrival"]
    assert reasons(spark, "stop_times", [stop_time(arrival_time="")]) == ["missing_arrival_time"]
    assert reasons(spark, "stop_times", [stop_time(arrival_time="12:61:00")]) == ["bad_arrival_time"]


def test_stops_clean(spark):
    (r,) = cleaned(spark, "stops", [stop(parent_station="ST")])
    assert r._reason is None
    assert (r.stop_id, r.stop_public_code, r.stop_name) == ("100", "21", "תחנה")
    assert (r.street, r.city, r.platform, r.floor) == ("x", "y", "15", "3")
    assert (r.lat, r.lon, r.location_type, r.station_id) == (31.9, 34.8, 0, "ST")

    assert reasons(spark, "stops", [stop(stop_lat="40.0")]) == ["bad_stop_lat"]
    (r,) = cleaned(spark, "stops", [stop(stop_desc="")])
    assert r._reason is None and (r.street, r.city, r.platform, r.floor) == (None,) * 4
    assert reasons(spark, "stops", [stop(location_type="2")]) == ["bad_location_type"]


def test_operators_and_calendar_clean(spark):
    (r,) = cleaned(spark, "operators", [agency()])
    assert r._reason is None
    assert (r.operator_id, r.operator_name, r.operator_url, r.operator_phone) == ("5", "אגד", "http://x", "*2800")
    assert reasons(spark, "operators", [agency(agency_name="")]) == ["missing_agency_name"]

    assert reasons(spark, "calendar", [calendar(monday="2")]) == ["bad_monday"]
    assert reasons(spark, "calendar", [calendar(start_date="20261340")]) == ["bad_start_date"]
    assert reasons(spark, "calendar", [calendar(start_date="20261201")]) == ["start_after_end"]
    (r,) = cleaned(spark, "calendar", [calendar()])
    assert r._reason is None
    assert (r.sunday, r.friday, r.saturday) == (True, False, False)
    assert (r.start_date, r.end_date) == (dt.date(2026, 11, 15), dt.date(2026, 11, 30))


def test_malformed_values_reject_not_crash(spark):
    assert spark.conf.get("spark.sql.ansi.enabled") == "true"
    assert reasons(spark, "stops", [stop(stop_lat="abc")]) == ["bad_stop_lat"]
    assert reasons(spark, "stop_times", [stop_time(stop_sequence="1.5")]) == ["bad_stop_sequence"]
    assert reasons(spark, "stop_times", [stop_time(stop_sequence="99999999999")]) == ["bad_stop_sequence"]
    assert reasons(spark, "calendar", [calendar(start_date="2026-11-15")]) == ["bad_start_date"]


def test_one_reason_per_row(spark):
    rows = cleaned(
        spark, "stops",
        [stop(stop_id="1", stop_name="a"), stop(stop_id="1", stop_name="b", stop_lat=""),
         stop(stop_id="1", stop_name="c"), stop(stop_id="2")],
    )
    got = {r.stop_name: r._reason for r in rows if r.stop_id == "1"}
    assert got == {"a": "duplicate_key", "b": "missing_stop_lat", "c": "duplicate_key"}
    assert [r._reason for r in rows if r.stop_id == "2"] == [None]


def test_raw_keeps_the_bronze_row(spark):
    row = stop_time(pickup_type=None, shape_dist_traveled="")
    (r,) = cleaned(spark, "stop_times", [row])
    assert json.loads(r._raw) == row
