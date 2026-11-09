# Stages 0 and 1: prove the stack, capture the feed

Compute runs on one Oracle Cloud Always Free VM. Storage is in S3. This stage uses no Glue, Athena, EMR or EC2.

## Stage 0: prove the stack

**Goal:** PySpark on the VM writes a small Delta table to S3 and reads it back.

### Versions

| Part | Version | Status |
|---|---|---|
| PySpark | `4.2.0`, pinned | Tested on the VM. An unpinned install would now get 4.3.0, which `delta-spark` 4.4.1 does not allow. |
| Delta | `io.delta:delta-spark_4.2_2.13:4.4.1` | Tested on the VM against S3: partitioned write, `replaceWhere` on one partition, read back |
| Java / Python | 17 / 3.13 | Tested on the VM's Arm processor, from the current Airflow image |
| S3 connector | `org.apache.hadoop:hadoop-aws:3.5.0` | Tested on the VM against the bucket |
| pandas | below 3, pinned | PySpark 4.2 warns that it does not fully support pandas 3, which `geopandas` would otherwise install |

**Status: done** on 2026-11-09. `scripts/stage0_spike.py` passed on the VM.

Spark's libraries (Delta, `hadoop-aws` and the AWS SDK, about 760 MB) are downloaded once while building the image. Without that, every new container downloaded them again on its first Spark session.

- Delta and `hadoop-aws` go into the Ivy cache at `/opt/spark-ivy` and still load as Spark packages.
- The AWS SDK bundle (`software.amazon.awssdk:bundle:2.35.4`, 686 MB) is downloaded separately, checked against Maven's SHA-1, and put on `spark.driver.extraClassPath`. Loaded as a Spark package, it made the executor heartbeat fail every 10 seconds with `NullPointerException ... idWithoutTopologyInfo is null`. That was narrowed down on the VM: plain Spark, Delta alone, and `hadoop-aws` without the bundle were all clean, and each run with the bundle as a package was not.
- Only the bundle goes on the driver classpath. That classpath comes before Spark's own jars, and the bundle is fully shaded, so it can't override them. Delta's dependencies include older Jackson, Parquet and Hadoop 3.4.2 jars that would.

All services share one image, `transit-airflow:latest`, so `docker compose build` also rebuilds `airflow-cli`, which is in the `debug` profile and was otherwise skipped.

### Setup (done by hand in Oracle Cloud)

1. Check the home region and the free limits under Limits, Quotas and Usage. Free compute must be in the home region, which can't be changed.
2. Create an Arm VM (`VM.Standard.A1.Flex`, 2 OCPUs, 12 GB RAM). If it fails with "out of host capacity", try another availability domain or retry later.
3. Install Docker and clone the repo.
4. From the VM, download the Ministry's feed once. Some Israeli government sites may block foreign IPs, so **if this fails, the VM can't do the capture**.

### Setup (done by hand in AWS)

1. Create a budget alert.
2. Create a bucket in `eu-north-1`, with public access blocked and versioning on. Add a lifecycle rule that deletes incomplete multipart uploads after 7 days.
3. Create an access key that can only touch this bucket. It needs `s3:ListBucket` as well as read, write and delete on objects. Without `ListBucket`, S3 answers 403 instead of 404 for a missing zip, and capture would fail.
4. Add `GTFS_BUCKET`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to `.env` on the VM, which git ignores.

### Steps

1. Pin `pyspark==4.2.0` and add `delta-spark==4.4.1`.
2. Switch `docker-compose.yaml` from CeleryExecutor to LocalExecutor, dropping Redis and the worker, to leave memory for Spark.
3. Add a Spark session helper in `src/transit/` with the Delta and S3 settings.
4. Build the image on the VM and start the stack.
5. Write a small test table partitioned by `service_date` to `s3a://<bucket>/spike/`, overwrite one partition, and read it back.
6. Delete the test data. Versioning keeps the old versions, and the access key can't remove them, so they stay until a lifecycle rule expires them.

**Done when:** on the VM, the read returns the expected rows from S3, including after the single-partition overwrite.

### Staying on the free tier

Oracle may reclaim an Always Free instance if CPU, network and memory all stay below 20% over 7 days (95th percentile for CPU). A daily batch will sit below that on CPU and network, so check that memory stays above about 2.4 GB once the stack is running.

## Stage 1: landing

**Goal:** store each day's zip in S3 once, and only if it really is that day's feed.

**Location:** `s3://<bucket>/landing/gtfs/feed_date=YYYY-MM-DD/gtfs.zip`

### Rules, in order

1. **Not captured yet:** if the zip already exists, stop successfully without downloading. This comes first so that re-running an old day that was captured never marks it as missed.
2. **Right day:** the service date must be today in Israel. Otherwise fail without retrying and record the day as missed.
3. **Download** to a temporary local file.
4. **Valid zip:** the file is a zip and its contents pass a CRC check.
5. **Complete:** `agency`, `calendar`, `routes`, `stops`, `stop_times` and `trips` are all present.
6. **Today's feed:** the earliest `start_date` in `calendar.txt` equals the service date. This held for all three feeds checked so far (Oct 9, Nov 3, Nov 7), so confirm it on more feeds before relying on it.
7. **Write once:** upload with an S3 conditional write (`If-None-Match: *`), so an existing zip can never be replaced.

If a check fails, retry until the end of the day, then record the day as missed.

### Tests (no network or AWS needed)

- A valid feed is stored.
- A second capture for the same date stores nothing and leaves the zip unchanged.
- An HTML page, a corrupt zip, a missing file, and a stale feed are each rejected.
- A run for a past date with no zip fails without downloading and records the day as missed.
- A run for a past date with a zip does nothing and records nothing.

The real zips in `data/` can be used to confirm the checks by hand. They are too large to commit, so tests should build small zips.

**Done when:** the tests pass and, running on the VM, one real feed is in S3 with a second capture refused.

## Not in these stages

Bronze, silver and gold, the quality gate, alerts, and Athena.

## Feed quirks for bronze

Found while loading the feed in an earlier prototype:

- The files start with a BOM, so read them as `utf-8-sig` or strip it from the first column name.
- Unquoted fields contain bare quotes, such as `קק"ל`. Strict CSV parsers treat these as the start of a quoted value, so check Spark's CSV options against the real files.
- GTFS times can go past 24:00, such as `25:30:00`, which counts from the start of the service date.
