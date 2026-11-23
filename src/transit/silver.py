"""Silver layer: parsers that type bronze's all-text GTFS columns.

Spark 4 runs with ANSI mode on, so every cast here is guarded by a regex match.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

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
    left = sides.getItem(0)
    right = sides.getItem(1)
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
    ok = (F.size(sides) == 2) & sides.getItem(1).endswith(suffix)
    return F.when(ok, parts)


def stop_desc_parts(c: Column) -> Column:
    """Split stop_desc 'רחוב: .. עיר: .. רציף: .. קומה: ..' into a struct; fields null if no match."""
    ok = c.rlike(_STOP_DESC)

    def field(i: int, name: str) -> Column:
        return F.when(ok, _blank_to_null(F.regexp_extract(c, _STOP_DESC, i))).alias(name)

    return F.struct(
        field(1, "street"), field(2, "city"), field(3, "platform"), field(4, "floor")
    )
