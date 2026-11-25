# Stage 3: silver

**Goal:** turn each day's bronze tables into clean, typed, **bus-only** tables, so gold can build the star schema without parsing text or guessing at bad values.

Silver is the second part of `gtfs_build`, after bronze. Gold, the quality gate and alerts are later stages.

## What silver is

- **One silver table per bronze table.** Six tables: `operators`, `calendar`, `routes`, `stops`, `trips`, `stop_times`. There are no joins into wider tables. Joining into the star schema is gold's job.
- **Typing and converting only.** Text becomes dates, numbers and booleans. Columns get the project's names from [`schema.md`](schema.md). Packed text fields (`route_desc`, `route_long_name`, `stop_desc`) are split into their parts.
- **Each row is judged on its own.** Silver doesn't know how the tables relate. It never rejects a row because of another table: a stop time whose stop was rejected, a trip whose calendar row is missing, or a route whose operator is missing all stay. Checking that every reference points at a real row is the gold quality gate's job.
- **Buses only, in every table: the one exception.** Only `routes` says whether something is a bus (`route_type` 3). To keep the other tables bus-only, silver follows the relations in **bronze** to decide "is this a bus?". It uses them for nothing else. Dropping non-bus rows is filtering, not rejecting: those rows don't count as rejects.
- **Unusable rows are removed, recorded, and limited.** A row with a bad key, time or coordinate is dropped from silver and written to a rejects table with the reason. If more than 0.1% of a table's bus rows are rejected, the day fails.
- **A rebuildable 7-day cache, like bronze.** Partitioned by `feed_date`, and kept 7 days. Any day can be rebuilt from its own bronze partition, and bronze from its own zip in landing, which is kept forever. A rebuild never reads another day's feed.

### Why this replaces "silver = latest feed"

`architecture.md` described silver as "replaced by the newest feed, always the current schedule". That conflicts with the 2026-11-15 decision that any day can be rebuilt from its own zip. If silver held only the latest feed, rebuilding an old day would build it from today's data. Keeping silver per `feed_date`, like bronze, removes the conflict. `architecture.md` is updated to match.

## Approaches considered

How silver is split into tables:

| Approach | Trade-off |
|---|---|
| **A. One silver table per bronze table (chosen)** | Same pattern as bronze: each table is visible and retried on its own, and silver stays "clean data", not half of gold. Costs six more Spark start-ups, and about 20 more minutes a day for silver in all. |
| B. Three tables shaped for gold (`routes` + operator, `stops` + station, `stop_visits` = stop times + trips + calendar) | Fewer tasks and a thinner gold, but silver does gold's modelling, and the join of about 12.5M bus stop times can't be retried in parts. |
| C. One job builds all of silver | Fastest, but one failure redoes everything and Airflow shows a single box. Already rejected for bronze. |

What to do with an unusable value:

| Option | Trade-off |
|---|---|
| Null the bad value, keep the row | Nothing lost, but gold must cope with nulls in keys and times, where a null row is useless anyway. |
| Drop the row into a rejects table | The day still builds, and what was dropped is visible. A format change could silently shrink a day. |
| **Drop into rejects, and fail the day above 0.1% (chosen)** | Same as above, but a broken feed or a parsing bug stops the build instead of quietly losing part of a day of history. The day is rebuilt from its zip once the code is fixed. |

References between tables:

| Option | Trade-off |
|---|---|
| **Silver judges each row on its own (chosen)** | Silver stays typing and cleaning. Broken references are found once, by the gold quality gate, which stops the day from being published. |
| Silver rejects rows that point at a missing or rejected row | Gold could trust every key, but one bad stop would reject every visit to it, which for a busy stop can fail the day on its own. |

How bus-only is applied:

| Option | Trade-off |
|---|---|
| **All six tables bus-only, decided from bronze relations (chosen)** | Silver is bus-only, as wanted. Relations are used for this filter only. |
| Only `routes` filtered; gold drops the rest by joining | Silver would know nothing about relations, but would keep about 0.1M non-bus stop times a day (113,788 on 2026-11-25) and stop being bus-only. |

## Layout

```
s3a://<bucket>/silver/<table>/feed_date=YYYY-MM-DD/
s3a://<bucket>/silver/_rejects/feed_date=YYYY-MM-DD/
```

Paths come from `lakehouse_path("silver", name)`. Every silver table ends with `feed_date` (`date`, the partition column) and `_loaded_at` (`timestamp`).

## Which rows are buses

Decided from the **bronze** partition of the same `feed_date`, never from silver, so a rejected row elsewhere never changes what counts as a bus:

| Table | A row is a bus row if… |
|---|---|
| `routes` | its `route_type` is `3` |
| `trips` | its `route_id` is a bus route |
| `stop_times` | its `trip_id` is a bus trip |
| `stops` | a bus stop time visits it, or it is the station (`parent_station`) of such a stop |
| `operators` | it runs at least one bus route |
| `calendar` | at least one bus trip uses its `service_id` |

A row that matches nothing (for example a trip whose `route_id` isn't in bronze `routes` at all) isn't a bus row, so it's dropped as non-bus, not rejected. The gold quality gate is what notices missing references.

Measured on the 2026-11-03 feed: 6,589 of 7,841 routes are buses, as are 29 of 36 operators, 301,634 of 310,599 trips, and 26,476 of 31,866 calendar rows.

## The six tables

"Required" means a bad or empty value rejects the row. "Optional" means a bad or empty value becomes null and the row stays. Rows with the same key are all rejected (reason `duplicate_key`), because silver can't tell which copy is right.

### `routes` (from bronze `routes`)

| Column | Type | From | Rule |
|---|---|---|---|
| `route_id` | string | `route_id` | required, key |
| `operator_id` | string | `agency_id` | required |
| `line_public_number` | string | `route_short_name` | optional |
| `line_id` | string | `route_desc`, part 1 | required: `route_desc` must match `^(\d+)-([123])-(.+)$` |
| `route_direction` | string | `route_desc`, part 2 | `1`, `2` or `3` |
| `route_variant` | string | `route_desc`, part 3 | kept as text: digits, `#` or Hebrew letters (`א`, `ב`, …) |
| `origin_name`, `origin_city` | string | `route_long_name`, left of `<->` | required: exactly one `<->` |
| `destination_name`, `destination_city` | string | `route_long_name`, right of `<->` | after removing the trailing `-<direction><variant>` |
| `route_color` | string | `route_color` | optional |

All 6,589 bus routes on Nov 3 match the `route_desc` pattern and have exactly one `<->`. Each side of `<->` is `name-city`. Names can contain `-` themselves (`מח.נעורה-עמק יזרעאל-10`), so the city is the text after the **last** `-` of each side, once the destination's direction-and-variant suffix is removed. The suffix is removed by matching the route's own direction and variant from `route_desc`, not by guessing. A side with no `-` keeps the whole text as the name and a null city (*my call*: the city isn't worth failing a day over).

### `trips` (from bronze `trips`)

| Column | Type | From | Rule |
|---|---|---|---|
| `trip_id` | string | `trip_id` | required, key |
| `route_id` | string | `route_id` | required |
| `service_id` | string | `service_id` | required |
| `trip_headsign` | string | `trip_headsign` | optional, kept as is (bare quotes included) |
| `direction_id` | int | `direction_id` | optional, `0` or `1` |
| `shape_id` | string | `shape_id` | optional |
| `wheelchair_accessible` | int | `wheelchair_accessible` | optional, `0`–`2` |

### `stop_times` (from bronze `stop_times`)

| Column | Type | From | Rule |
|---|---|---|---|
| `trip_id` | string | `trip_id` | required, key part |
| `stop_sequence` | int | `stop_sequence` | required, key part, ≥ 0 |
| `stop_id` | string | `stop_id` | required |
| `arrival_seconds` | int | `arrival_time` | required. `H:MM:SS` or `HH:MM:SS`, seconds since the start of the service day. Hours can pass 24: `25:30:00` → 91,800. |
| `departure_seconds` | int | `departure_time` | required, same format, not earlier than arrival |
| `boarding_allowed` | boolean | `pickup_type` | `0` or empty → true, `1`–`3` → false, anything else rejects |
| `alighting_allowed` | boolean | `drop_off_type` | same rule |
| `dist_from_trip_start_m` | int | `shape_dist_traveled` | optional, rounded to whole meters |

Times stay offsets in silver. They become real timestamps in gold, where the service date is known.

### `stops` (from bronze `stops`)

| Column | Type | From | Rule |
|---|---|---|---|
| `stop_id` | string | `stop_id` | required, key |
| `stop_public_code` | string | `stop_code` | optional |
| `stop_name` | string | `stop_name` | optional |
| `street`, `city`, `platform`, `floor` | string | `stop_desc` | optional, split on `רחוב:` `עיר:` `רציף:` `קומה:`, trimmed, empty → null |
| `lat`, `lon` | double | `stop_lat`, `stop_lon` | required, inside a box around Israel (lat 29–34, lon 34–36) |
| `location_type` | int | `location_type` | required, `0` (stop) or `1` (station) |
| `station_id` | string | `parent_station` | optional |

On Nov 3, 76 stops have an empty `stop_desc`. They keep null street, city, platform and floor; that isn't a reject.

### `operators` (from bronze `agency`)

| Column | Type | From | Rule |
|---|---|---|---|
| `operator_id` | string | `agency_id` | required, key |
| `operator_name` | string | `agency_name` | required |
| `operator_url`, `operator_phone` | string | `agency_url`, `agency_phone` | optional |

`agency_timezone`, `agency_lang` and `agency_fare_url` are the same for every operator, or empty, and aren't kept.

### `calendar` (from bronze `calendar`)

| Column | Type | From | Rule |
|---|---|---|---|
| `service_id` | string | `service_id` | required, key |
| `sunday` … `saturday` | boolean | same names | required, `0` or `1` |
| `start_date`, `end_date` | date | same names | required, `YYYYMMDD`, start not after end |

## Rejects

One table, `silver/_rejects`, partitioned by `feed_date`:

| Column | Meaning |
|---|---|
| `table` | which silver table rejected the row |
| `reason` | a short code, e.g. `bad_arrival_time`, `bad_route_desc`, `bad_stop_lat`, `duplicate_key` |
| `row` | the bronze row as JSON, unchanged |
| `feed_date`, `_loaded_at` | as in every silver table |

Each silver job replaces its own rejects with `replaceWhere feed_date = '<date>' AND table = '<table>'`, so rebuilding one table doesn't touch the others' rejects.

**The 0.1% limit:** for each table, rejected rows ÷ bus rows must be at most 0.1%, where bus rows are as defined above (kept + rejected). Above that the job writes the rejects, so the reason can be read, then **deletes that day's partition of the silver table** and fails (*my call*: on a rebuild, leaving the earlier run's rows would let gold build from data that no longer matches the rejects). In `operators` (29 rows) a single reject is over the limit. That's intended: in a table that small, any bad row is worth stopping for. A table with no bus rows at all fails the same way: it means the bus filter stopped matching, not that there are no buses.

## Order of the jobs

No silver job reads another silver table. Each one reads only bronze partitions of the same `feed_date`: its own table, plus the ones it needs to tell buses apart.

| Silver table | Reads bronze |
|---|---|
| `routes` | `routes` |
| `trips` | `trips`, `routes` |
| `stop_times` | `stop_times`, `trips`, `routes` |
| `stops` | `stops`, `stop_times`, `trips`, `routes` |
| `operators` | `agency`, `routes` |
| `calendar` | `calendar`, `trips`, `routes` |

If any of these partitions is missing, the job fails without writing.

## How one table is built

```
spark-submit src/transit/jobs/silver.py --table stop_times --feed-date 2026-11-15
```

1. Read the bronze partitions listed above for the `feed_date`.
2. Keep only bus rows.
3. Type, split and check each row on its own. Mark each one as kept or rejected with a reason.
4. Write the rejects, replacing this table's rejects for the day.
5. Check that there are bus rows and that the 0.1% limit holds. If not, delete the day's silver partition and fail.
6. Write the kept rows with `replaceWhere feed_date = '<date>'`.
7. Check the count: the rows written for the day equal the kept rows.
8. Clean up: the same 7-day retention and `VACUUM` as bronze, on this table and on `_rejects`. `cleanup()` moves from `bronze.py` to a shared module.

The job prints the kept, rejected and non-bus counts for the table.

## Airflow

In `gtfs_build`:

- **Six tasks** `silver_operators`, `silver_calendar`, `silver_routes`, `silver_stops`, `silver_trips`, `silver_stop_times`, each a `@task.bash` running the command above, in the `spark` pool, with the same retries and timeout as bronze.
- **Dependencies:** each silver task waits for the bronze tasks of the tables it reads (table above). There are no silver-to-silver dependencies.
- **Rebuilding a day** works as it does for bronze: trigger `gtfs_build` with a `feed_date`, or clear the silver tasks. A day older than 7 days needs its bronze rebuilt first, which a full `gtfs_build` run does anyway.

Total build time grows from 16–18 minutes a day to 33–35, measured on 2026-11-25.

## Tests (no network or AWS needed)

Against small local Delta tables, like the bronze tests:

- Each table: a valid row is typed and renamed as above.
- Non-bus routes, their trips, stop times, unused stops, operators and calendar rows are dropped and don't appear in rejects.
- A trip whose route isn't in bronze `routes` is dropped, not rejected.
- `route_desc` with Hebrew-letter and `#` variants splits correctly. A name containing `-` gives the right city. A side with no `-` gives a null city.
- `25:30:00` → 91,800. `ab:30:00`, `12:61:00` and an empty time each reject the row.
- A departure before its arrival rejects the row.
- A coordinate outside Israel rejects the stop, and the stop times visiting it stay in silver.
- An empty `stop_desc` gives null street and city, not a reject.
- Duplicate keys reject every copy.
- Rejects above 0.1% fail the job, write the rejects, and leave no partition for that day in the silver table.
- No bus rows fails the job and leaves no partition for that day in the silver table.
- Building the same day twice gives the same rows. Building a second day doesn't change the first.
- A missing bronze partition fails without writing.
- Retention removes partitions older than 7 days, in the table and in `_rejects`.

**Done when:** the tests pass on the VM, and `gtfs_build` for a real day builds all six silver tables with zero rejects (or a few, explained), kept + rejected equal to the bronze bus rows for each table, and the same counts on a second run.

**Status: done** on 2026-11-25, deployed on the VM from branch `stage-3-silver`, not merged to `main` yet:

- 77 tests pass in the image on the VM, in about 8.5 minutes.
- `gtfs_build` for 2026-11-25 succeeded twice, with no retries and no two Spark tasks overlapping. Both runs logged the same counts. Kept + rejected equals the bronze bus rows, counted separately for each table with SQL written from "Which rows are buses":

  | Table | Kept | Rejected | Non-bus | Bronze bus rows |
  |---|---|---|---|---|
  | `routes` | 6,603 | 0 | 1,584 | 6,603 |
  | `trips` | 326,709 | 0 | 8,313 | 326,709 |
  | `stop_times` | 12,491,639 | 0 | 113,788 | 12,491,639 |
  | `stops` | 30,649 | 0 | 4,746 | 30,649 |
  | `operators` | 29 | 0 | 7 | 29 |
  | `calendar` | 31,780 | 0 | 5,507 | 31,780 |

- After the second run, each silver table holds exactly the kept rows for the day, all with one `_loaded_at` inside that run, so it replaced the first run's rows. There are no duplicate keys and no nulls in required columns. `_rejects` is empty.
- Run times: 33 minutes (17:35–18:08 UTC) and 35 minutes (18:08–18:43 UTC). In each run the bronze tasks took about 13 minutes and the silver tasks about 20 and 22. `silver_stop_times` was the longest, at 6.2 and 6.6 minutes. The three bronze-only runs before this took 16–18 minutes.
- Memory, sampled every 3 seconds: the lowest available on the VM was 4.6 GB (4,625 MB) in the first run and 4.7 GB (4,726 MB) in the second, both during `silver_stop_times`. A shorter dip between samples wouldn't show.
- Bronze `stop_times` was much larger than on 2026-11-15 (12,605,427 rows against 8,005,802). It isn't duplicated: the row count equals the lines in that day's zip, no (`trip_id`, `stop_sequence`) repeats, and the feed had 335,022 trips against 214,625.
- Not checked: a run started by the daily capture (the two runs were triggered by hand), and the reject and failure paths on real data, since nothing was rejected. Those are covered by tests only.

## Not in this stage

Gold, the quality gate (including the reference checks silver leaves out), alerts, the service-date expansion (which trips run on a given day, from `calendar`), and turning time offsets into timestamps. Those are gold's.

## How the open details were settled

- **Where the shared code lives:** `src/transit/lakehouse.py` has `cleanup`, the table paths and which bronze tables each silver table reads. `src/transit/silver.py` has the bus filter, the table rules and the build, and `src/transit/jobs/silver.py` is the job.
- **Reason codes:** `missing_<column>` and `bad_<column>`, named after the bronze column (`bad_arrival_time`), plus `departure_before_arrival`, `start_after_end` and `duplicate_key`. A row gets the first one that applies, in that order: its columns, then the cross-column check, then the duplicate key.
- **The bus filter:** left semi joins against bronze inside each job, not lists collected to the driver.
