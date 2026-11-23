from pyspark.sql import functions as F

from transit.silver import gtfs_seconds, long_name_parts, route_desc_parts, stop_desc_parts


def one_col(spark, values):
    return spark.createDataFrame([(v,) for v in values], "v string")


def test_gtfs_seconds(spark):
    cases = {"07:52:00": 28320, "7:05:09": 25509, "25:30:00": 91800}
    bad = ["ab:30:00", "12:61:00", "12:30", "", None]
    df = one_col(spark, list(cases) + bad)
    got = {r.v: r.s for r in df.select("v", gtfs_seconds(F.col("v")).alias("s")).collect()}
    for text, secs in cases.items():
        assert got[text] == secs
    for text in bad:
        assert got[text] is None


def test_route_desc_parts(spark):
    good = {
        "11337-1-0": ("11337", "1", "0"),
        "35003-1-ב": ("35003", "1", "ב"),
        "10-3-#": ("10", "3", "#"),
    }
    bad = ["11337-4-0", "11337-1", "x-1-0", None]
    df = one_col(spark, list(good) + bad)
    rows = df.select("v", route_desc_parts(F.col("v")).alias("p")).collect()
    got = {r.v: r.p for r in rows}
    for text, parts in good.items():
        p = got[text]
        assert (p.line_id, p.route_direction, p.route_variant) == parts
    for text in bad:
        assert got[text] is None


def long_parts(spark, text, direction, variant):
    df = spark.createDataFrame([(text, direction, variant)], "t string, d string, v string")
    p = df.select(long_name_parts(F.col("t"), F.col("d"), F.col("v")).alias("p")).collect()[0].p
    return None if p is None else (p.origin_name, p.origin_city, p.destination_name, p.destination_city)


def test_long_name_cities(spark):
    assert long_parts(
        spark, "ת. רכבת יבנה מערב-יבנה<->ת. רכבת יבנה מזרח-יבנה-1#", "1", "#"
    ) == ("ת. רכבת יבנה מערב", "יבנה", "ת. רכבת יבנה מזרח", "יבנה")

    text = "ת.מרכזית עפולה/רציפים-עפולה<->מח.נעורה-עמק יזרעאל-10"
    got = long_parts(spark, text, "1", "0")
    assert got[2:] == ("מח.נעורה", "עמק יזרעאל")

    got = long_parts(spark, "תחנה<->מסוף-חיפה-10", "1", "0")
    assert got[:2] == ("תחנה", None)

    assert long_parts(spark, text, "2", "0") is None
    assert long_parts(spark, "a-b<->c-d<->e-f-10", "1", "0") is None
    assert long_parts(spark, "abc", "1", "0") is None
    assert long_parts(spark, None, "1", "0") is None


def test_stop_desc_parts(spark):
    df = one_col(
        spark,
        [
            "רחוב: בן יהודה 74 עיר: כפר סבא רציף:  קומה: ",
            "רחוב: x עיר: y רציף: 15 קומה: 3",
            "",
            None,
        ],
    )
    rows = df.select("v", stop_desc_parts(F.col("v")).alias("p")).collect()
    got = {r.v: (r.p.street, r.p.city, r.p.platform, r.p.floor) for r in rows}
    assert got["רחוב: בן יהודה 74 עיר: כפר סבא רציף:  קומה: "] == ("בן יהודה 74", "כפר סבא", None, None)
    assert got["רחוב: x עיר: y רציף: 15 קומה: 3"] == ("x", "y", "15", "3")
    assert got[""] == (None, None, None, None)
    assert got[None] == (None, None, None, None)
