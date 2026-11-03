-- One row per trip scheduled on a service date. The feed only covers today onward,
-- so this table is the history the real-time data gets compared against.

CREATE SCHEMA IF NOT EXISTS schedule;

CREATE TABLE IF NOT EXISTS schedule.planned_trips (
    service_date      date        NOT NULL,
    trip_id           text        NOT NULL,
    route_id          text        NOT NULL,
    agency_id         text,
    agency_name       text,
    route_short_name  text,
    route_long_name   text,
    route_desc        text,       -- "<line license (makat)>-<direction>-<alternative>"
    route_type        smallint,
    direction_id      smallint,
    trip_headsign     text,
    first_stop_id     text,
    last_stop_id      text,
    n_stops           integer,
    planned_start     timestamptz NOT NULL,
    planned_end       timestamptz NOT NULL,
    loaded_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (service_date, trip_id)
);

CREATE INDEX IF NOT EXISTS planned_trips_route_idx ON schedule.planned_trips (service_date, route_id);

-- Idempotent: rerunning a day replaces it
DELETE FROM schedule.planned_trips WHERE service_date = %(service_date)s;

-- The MOT feed has no calendar_dates.txt, holidays are expressed through calendar.txt ranges.
-- (Keep semicolons out of comments here: the loader splits statements on them.)
WITH active_services AS (
    SELECT service_id
    FROM raw_gtfs.calendar
    WHERE %(service_date)s::date BETWEEN to_date(start_date, 'YYYYMMDD') AND to_date(end_date, 'YYYYMMDD')
      AND CASE extract(dow FROM %(service_date)s::date)
              WHEN 0 THEN sunday
              WHEN 1 THEN monday
              WHEN 2 THEN tuesday
              WHEN 3 THEN wednesday
              WHEN 4 THEN thursday
              WHEN 5 THEN friday
              WHEN 6 THEN saturday
          END = '1'
),

day_trips AS (
    SELECT t.*
    FROM raw_gtfs.trips t
    JOIN active_services USING (service_id)
),

day_stop_times AS (
    SELECT
        st.trip_id,
        st.stop_id,
        st.arrival_time,
        st.departure_time,
        row_number() OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence::int)      AS seq_from_start,
        row_number() OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence::int DESC) AS seq_from_end,
        count(*)     OVER (PARTITION BY st.trip_id)                                     AS n_stops
    FROM raw_gtfs.stop_times st
    JOIN day_trips USING (trip_id)
)

INSERT INTO schedule.planned_trips (
    service_date, trip_id, route_id, agency_id, agency_name, route_short_name, route_long_name,
    route_desc, route_type, direction_id, trip_headsign, first_stop_id, last_stop_id, n_stops,
    planned_start, planned_end
)
SELECT
    %(service_date)s::date,
    t.trip_id,
    t.route_id,
    r.agency_id,
    a.agency_name,
    r.route_short_name,
    r.route_long_name,
    r.route_desc,
    r.route_type::smallint,
    t.direction_id::smallint,
    t.trip_headsign,
    first_stop.stop_id,
    last_stop.stop_id,
    first_stop.n_stops,
    -- GTFS times are offsets from the service day and can exceed 24:00:00 for after-midnight trips.
    -- (Strictly they count from noon minus 12h, which differs by an hour on DST-change days.)
    (%(service_date)s::date + first_stop.departure_time::interval) AT TIME ZONE 'Asia/Jerusalem',
    (%(service_date)s::date + last_stop.arrival_time::interval)    AT TIME ZONE 'Asia/Jerusalem'
FROM day_trips t
JOIN day_stop_times first_stop ON first_stop.trip_id = t.trip_id AND first_stop.seq_from_start = 1
JOIN day_stop_times last_stop  ON last_stop.trip_id  = t.trip_id AND last_stop.seq_from_end   = 1
JOIN raw_gtfs.routes r ON r.route_id = t.route_id
LEFT JOIN raw_gtfs.agency a ON a.agency_id = r.agency_id;
