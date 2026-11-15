# Stage 2: bronze

**Goal:** when a feed zip lands, load its six schedule files into Delta tables on S3, unchanged, so silver has something to build from.

Bronze is the first part of `gtfs_build`. Silver, gold, the quality gate and alerts are later stages.

## What bronze is

- **A rebuildable 7-day cache.** Any day still in landing can be loaded again from its own zip, for example after a bug. Landing keeps every zip forever, so dropping bronze after 7 days loses nothing.
- **Raw.** Every column is text and keeps its original GTFS name. Bronze does no typing, renaming, filtering or joining. All of that is silver's job. A bad value then fails in silver, where it can be fixed and the day rebuilt.
- **Six files only:** `agency`, `calendar`, `routes`, `stops`, `stop_times`, `trips`. `shapes` (225 MB), `fare_rules`, `fare_attributes` and `translations` aren't used by gold, so they're left out. They can be added later from the zips.

## Approaches considered

How `build_bronze` runs inside `gtfs_build`:

| Approach | Trade-off |
|---|---|
| A. One task, one `spark-submit`, loads all six tables | One Spark start-up and one download, but a failure redoes all six and Airflow shows only one task |
| **B. One task per table, each its own `spark-submit` (chosen)** | Each table is visible and retried on its own. Costs six Spark start-ups (2–3 minutes a day) and six downloads of the zip. |
| C. A Python task downloads and unpacks, then one Spark task loads | Separates the download, but the unpacked folder has to survive between tasks on the VM |

B was chosen to see and retry each table on its own in Airflow. Its memory risk, two 4 GB drivers at once on a 12 GB VM, is removed with a pool (below).

How tasks are kept from running at the same time:

| Option | Trade-off |
|---|---|
| **A `spark` pool with 1 slot (chosen)** | One Spark job at a time across every DAG, including manual rebuilds. Tasks stay independent, so one failure doesn't block the rest. |
| Chain the six tasks | Also one at a time, but a failure in the middle blocks every table after it |

Why the zip is unpacked on the VM: Spark reads gzip directly but not zip, because a zip is an archive with its table of contents at the end of the file, and Hadoop has no zip codec. Reading the zip as one binary blob in Spark would put the whole file into a single task. Spark runs in local mode, so the VM's disk is the cluster's disk, and unpacking locally costs seconds.

## Layout

One Delta table per file, partitioned by `feed_date`:

```
s3a://<bucket>/bronze/<file>/feed_date=YYYY-MM-DD/
```

For example `bronze/stop_times/`. Paths come from `lakehouse_path("bronze", name)` in `src/transit/spark.py`.

Columns: the file's own columns, all `string`, in file order, then:

| Column | Value |
|---|---|
| `feed_date` | the feed's date, a `date`, the partition column |
| `_source_file` | the landing key of the zip, e.g. `landing/gtfs/feed_date=2026-11-15/gtfs.zip` |
| `_loaded_at` | when the load ran, a `timestamp` |

## How one table is loaded

A job script, `src/transit/jobs/bronze.py`, runs as:

```
spark-submit src/transit/jobs/bronze.py --table stop_times --feed-date 2026-11-15
```

It can also be run by hand, without Airflow, to rebuild or debug a day.

1. **Download** `landing/gtfs/feed_date=<date>/gtfs.zip` from S3 to a new temp folder. If it isn't there, fail: there is nothing to load.
2. **Unpack only this table's file** (`<table>.txt`) with `zipfile`.
3. **Check the header** against the expected columns for that file, after stripping the BOM. A missing column fails the load. An extra column fails the load too, so a feed change is noticed instead of silently dropped (*my call*: since bronze can be rebuilt, failing costs a day's build until the list is updated, not data). The expected columns, from the Oct 9 feed, live in code:

   | File | Columns |
   |---|---|
   | `agency` | `agency_id, agency_name, agency_url, agency_timezone, agency_lang, agency_phone, agency_fare_url` |
   | `calendar` | `service_id, sunday, monday, tuesday, wednesday, thursday, friday, saturday, start_date, end_date` |
   | `routes` | `route_id, agency_id, route_short_name, route_long_name, route_desc, route_type, route_color` |
   | `stops` | `stop_id, stop_code, stop_name, stop_desc, stop_lat, stop_lon, location_type, parent_station, zone_id` |
   | `stop_times` | `trip_id, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type, shape_dist_traveled` |
   | `trips` | `route_id, service_id, trip_id, trip_headsign, direction_id, shape_id, wheelchair_accessible` |

   Check these against a current feed before relying on them. Changing `TABLES` later means deleting that bronze table and rebuilding it from landing, because the existing Delta table keeps the old columns.
4. **Read with Spark** from the local file:
   - `header=true`, and every column read as string (no schema inference)
   - quote handling off (`quote` set to `\u0000`), so values with bare quotes like `קק"ל` stay as they are. Checked on the Oct 9 feed: no field in the six files starts with a quote. The only quotes are inside values, in 11,133 `trips` headsigns.
   - `mode=FAILFAST`, so a row with the wrong number of fields fails the load
   - the BOM removed from the first column name
   - lines end in CRLF in every file, and Spark's default line handling copes with that. A test checks that the last column has no trailing `\r`.
5. **Add** `feed_date`, `_source_file` and `_loaded_at`.
6. **Write** to the Delta table with `replaceWhere feed_date = '<date>'`, so a rebuild replaces only that day and running it twice gives the same result. The first write creates the table.
7. **Check the row count:** the rows written for that `feed_date` must equal the file's non-empty lines minus the header. A mismatch fails the task.
8. **Clean up** (*my call* on the details):
   - Delete partitions with `feed_date` more than 7 days before **today in Israel**, except the day just loaded. Counting from today, not from the loaded day, means rebuilding an old day never removes newer ones. A hand-rebuilt old day then stays until the next daily load removes it.
   - Run `VACUUM` with Delta's default 7-day retention. Files of a deleted partition are physically removed about 7 days after the delete, so bronze holds up to about two weeks of files on S3. Going lower means turning off Delta's retention safety check, which isn't worth it for a few hundred MB.
   - Old object versions then stay until the bucket's lifecycle rules expire them, as in stage 0.
9. **Delete the temp folder**, also when the load fails.

`stop_times` is the largest file (550 MB, 10.4M rows). The driver keeps its 4 GB default, and the pool makes sure it's the only Spark job on the VM.

## Airflow

New DAG `gtfs_build`, triggered by the `gtfs_landing` Asset event that `gtfs_capture` emits with `{"feed_date": ...}`.

- **Six tasks**, `bronze_agency`, `bronze_calendar`, `bronze_routes`, `bronze_stops`, `bronze_stop_times`, `bronze_trips`. Each is a `@task.bash` that runs the `spark-submit` command above with the run's `feed_date`.
- **No order between them.** All six are in the `spark` pool (1 slot), so only one runs at a time.
- **`build_silver`** (a later stage) will wait for all six.
- **The `spark` pool** is created with 1 slot. It's set up in code or at stack start-up, so a rebuilt VM gets it too.
- **Rebuilding a day by hand:** trigger `gtfs_build` with a `feed_date`, or clear its tasks. A day older than 7 days can be loaded again the same way.

## Tests (no network or AWS needed)

Run against small zips and local Delta tables, like the stage 1 tests:

- A valid file loads with all columns as text, original names, the BOM stripped and the three extra columns.
- Loading the same day twice leaves one copy of the rows.
- Loading a second day doesn't change the first.
- A missing column, an extra column, and a row with too many fields each fail the load.
- A value with a bare quote, such as `קק"ל`, is stored unchanged.
- A time past 24:00, such as `25:30:00`, is stored unchanged.
- The last column has no trailing `\r`.
- Partitions older than 7 days are removed, and newer ones stay.
- A day with no zip in landing fails without writing anything.

**Done when:** the tests pass and, on the VM, `gtfs_build` loads all six tables for a real day, with row counts matching the files, and a second run leaves the counts unchanged.

**Status: done** on 2026-11-15, on the VM:

- 51 tests pass in the image, in about 5 minutes. They run on the VM, since the development machine has no Java.
- The six expected column lists matched the 2026-11-15 feed exactly.
- `gtfs_build` for 2026-11-15 succeeded. The six tasks ran one at a time, in about 15 minutes in all, at 2–3 minutes per table. Rows written, each equal to the file's data lines counted separately from the zip:

  | Table | Rows |
  |---|---|
  | `agency` | 36 |
  | `calendar` | 26,761 |
  | `routes` | 7,669 |
  | `stops` | 35,402 |
  | `stop_times` | 8,005,802 |
  | `trips` | 214,625 |

- A second run for the same day also succeeded, with the same six counts. Each task re-counts the day's rows after writing, so a duplicated day would have failed it.
- Memory: at least 5.5 GB stayed available on the VM throughout, with the lowest point during `stop_times`.
- Logs: about 120 lines (52 KB) per task. Before shuffle partitions were lowered to 4 and Spark logging set to WARN, a 36-row test load wrote about 70,000 lines.
- Most of each task's time is fixed overhead: starting Spark, downloading the whole zip, and the retention step with `VACUUM`. If the run time ever matters, running `VACUUM` weekly instead of daily is the easiest saving.
- Empty fields are stored as null, which is Spark's default. GTFS doesn't distinguish an empty field from a missing one.

## Not in this stage

Silver, gold, the quality gate, alerts, and the other four files in the zip.

## How the open details were settled

- **`feed_date`:** a first task, `feed_date`, reads it from `triggering_asset_events[GTFS_LANDING]` (each event's `extra`), or from the run's `feed_date` param, which wins. Several events naming different days fail the run. A manual rebuild is `airflow dags trigger gtfs_build --conf '{"feed_date": "YYYY-MM-DD"}'`.
- **The `spark-submit` command:** `spark_submit_command` in `src/transit/spark.py` builds it. JVM settings made in Python are ignored under `spark-submit`, so it passes driver memory, the Delta and `hadoop-aws` packages, the Ivy cache and the AWS SDK bundle (on `--driver-class-path`, excluded as a package) on the command line. SQL settings still come from `get_spark`. The job script is `/opt/airflow/src/transit/jobs/bronze.py`.
- **The `spark` pool:** `airflow-init` runs `airflow pools set spark 1 ...` and fails if it can't, so the stack never starts without the pool.
- **Timeouts and retries:** each bronze task has two retries 10 minutes apart and a 1-hour timeout. The `feed_date` task isn't retried, since a bad date won't fix itself.
