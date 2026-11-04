# Stages 0 and 1: prove the stack, capture the feed

Compute runs locally. Only storage is online, in S3. This stage uses no Glue, Athena, EMR or EC2.

## Stage 0: prove the stack

**Goal:** local PySpark writes a small Delta table to S3 and reads it back.

### Versions

| Part | Version | Status |
|---|---|---|
| PySpark | `4.2.0`, pinned | Tested locally. An unpinned install would now get 4.3.0, which `delta-spark` 4.4.1 does not allow. |
| Delta | `io.delta:delta-spark_4.2_2.13:4.4.1` | Tested locally: partitioned write, `replaceWhere` on one partition, read back |
| Java / Python | 17 / 3.13 | Tested locally, from the current Airflow image |
| S3 connector | `org.apache.hadoop:hadoop-aws:3.5.0` | Published and matches Spark's Hadoop 3.5.0. **Not tested against S3 yet.** |

### Setup (done by hand in AWS)

1. Create a budget alert.
2. Create a bucket in `il-central-1`, with public access blocked and versioning on.
3. Create an access key that can only touch this bucket. It needs `s3:ListBucket` as well as read, write and delete on objects. Without `ListBucket`, S3 answers 403 instead of 404 for a missing zip, and capture would fail.
4. Add `GTFS_BUCKET`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to `.env`, which git ignores.

### Steps

1. Pin `pyspark==4.2.0` and add `delta-spark==4.4.1`.
2. Add a Spark session helper in `src/transit/` with the Delta and S3 settings.
3. Write a small test table partitioned by `service_date` to `s3a://<bucket>/spike/`, overwrite one partition, and read it back.
4. Delete the test data.

**Done when:** the read returns the expected rows from S3, including after the single-partition overwrite.

## Stage 1: landing

**Goal:** store each day's zip in S3 once, and only if it really is that day's feed.

**Location:** `s3://<bucket>/landing/gtfs/feed_date=YYYY-MM-DD/gtfs.zip`

### Rules, in order

1. **Not captured yet:** if the zip already exists, stop successfully without downloading. This comes first so that re-running an old day that was captured never marks it as missed.
2. **Right day:** the service date must be today in Israel. Otherwise fail without retrying and record the day as missed.
3. **Download** to a temporary local file.
4. **Valid zip:** the file is a zip and its contents pass a CRC check.
5. **Complete:** `agency`, `calendar`, `routes`, `stops`, `stop_times` and `trips` are all present.
6. **Today's feed:** the earliest `start_date` in `calendar.txt` equals the service date. This held for both feeds checked so far, so confirm it on more feeds before relying on it.
7. **Write once:** upload with an S3 conditional write (`If-None-Match: *`), so an existing zip can never be replaced.

If a check fails, retry until the end of the day, then record the day as missed.

### Tests (no network or AWS needed)

- A valid feed is stored.
- A second capture for the same date stores nothing and leaves the zip unchanged.
- An HTML page, a corrupt zip, a missing file, and a stale feed are each rejected.
- A run for a past date with no zip fails without downloading and records the day as missed.
- A run for a past date with a zip does nothing and records nothing.

The real zips in `data/` can be used to confirm the checks by hand. They are too large to commit, so tests should build small zips.

**Done when:** the tests pass and one real feed is in S3 with a second capture refused.

## Not in these stages

Bronze, silver and gold, the quality gate, alerts, and Athena.

## Feed quirks for bronze

Found while loading the feed in an earlier prototype:

- The files start with a BOM, so read them as `utf-8-sig` or strip it from the first column name.
- Unquoted fields contain bare quotes, such as `קק"ל`. Strict CSV parsers treat these as the start of a quoted value, so check Spark's CSV options against the real files.
- GTFS times can go past 24:00, such as `25:30:00`, which counts from the start of the service date.
