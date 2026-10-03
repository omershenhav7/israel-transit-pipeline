# Israel Transit Pipeline — Build Plan

**The question this project answers:** _Do Israeli buses actually show up?_

First you build a pipeline over the **planned** national timetable (GTFS). Then you add **actual** bus movements (SIRI) and compare the two: punctuality, ghost trips, and headway gaps per route, operator and city.

The plan goes from easy to hard. Phases 0–6 are the foundation and are presentable on their own. Phases 7–12 are the hard part, so only start them once the basics feel comfortable.

## Working with Claude on this project

I build everything myself. Claude is here only to answer questions — think of it as a senior engineer I can ask, not someone who does the work.

**Claude should:**

- Answer my questions and explain concepts, tools and trade-offs
- Point me to the relevant docs
- Give hints and point me in the right direction when I'm stuck, rather than the full solution
- Review code I paste: explain what's wrong and why, and let me fix it
- Ask me questions back when it helps me understand something better
- When I make a design decision or finish a phase, remind me to update the decisions log and numbers table in this file

**Claude should not:**

- Write files, components, DAGs, models or full functions for me
- Rewrite my code; it describes the fix and I make it
- Jump ahead to later phases unless I ask

**Small exceptions:** a one- or two-line snippet is fine when the question is about syntax or a command (e.g. a docker compose flag, a SQL function signature). If I explicitly say "show me the solution", it can.

---

How to read the bullets:

- `[ ]` a task to do
- **Learn:** something you probably need to study before or during that task
- 📖 a reference worth opening

---

## Timeline at a glance

| Week  | Dates     | Phases    | Checkpoint                                         |
| ----- | --------- | --------- | -------------------------------------------------- |
| 1     | Oct 4–10  | 0, 1      | Everything runs locally, and you know the data     |
| 2     | Oct 11–17 | 2, 3      | Raw GTFS loads daily, first dbt models             |
| 3     | Oct 18–24 | 4, 5, 6   | Spark + geospatial + tests.**Presentable v1**      |
| 4     | Oct 25–31 | 7, 8, (9) | SIRI explored, lake set up, first month backfilled |
| After | November+ | 9–12      | Planned vs. actual, SCD2, tuning                   |

If week 4 slips, that's fine. v1 already goes on the CV, and the hard part becomes "in progress" on the README.

---

## Target architecture (where you're heading)

```
MoT GTFS zip (daily)                 Hasadna SIRI archive (every ~minute)
        │                                        │
        ▼                                        ▼
  Airflow: download → version check        Airflow: backfill by date
        │                                        │
        ▼                                        ▼
  PostgreSQL raw schema              MinIO lake: bronze → silver → gold
        │                                        │
        ├── PySpark: calendar expansion          ├── PySpark: GPS pings → arrival events
        ├── GeoPandas: stop → city               └── PySpark: actual ⋈ planned
        ▼                                        ▼
               dbt: staging → marts (star schema) → tests
                               ▼
     fact_departures · fact_stop_arrival · dim_stop · dim_route · dim_city · dim_date
```

Phases 0–6 build only the left side.

---

## Phase 0 — Project setup (easy)

**Goal:** an empty project that starts with one command.

- [ ] Create a public GitHub repo called `israel-transit-pipeline`, with a Python `.gitignore` and a short README stub.
- [ ] Set up the folder layout:
  ```
  dags/          Airflow DAGs
  src/           Python code (ingestion, spark jobs, geo)
  dbt/           dbt project
  tests/         pytest tests
  notebooks/     exploration only, never imported by the pipeline
  data/          local downloads (gitignored)
  docker-compose.yaml
  ```
- [ ] Start from the official Airflow `docker-compose.yaml` and get the Airflow UI running at `localhost:8080`.
  - **Learn:** what the Airflow components are (scheduler, webserver, metadata DB, executor) and why the compose file has so many services.
  - 📖 [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [ ] Add a **separate** PostgreSQL service to the compose file to act as your warehouse. Don't reuse Airflow's metadata database.
  - **Learn:** why mixing orchestrator metadata and warehouse data in one database is a bad idea. One sentence of reasoning is enough, and it goes in the README.
- [ ] Extend the Airflow image with your own `Dockerfile` and `requirements.txt` (pyspark, geopandas, dbt-postgres). Run Spark in **local mode** inside the Airflow container; you don't need a cluster yet.
  - 📖 [Adding packages to the Airflow image](https://airflow.apache.org/docs/docker-stack/build.html)

**Done when:** `docker compose up` starts Airflow and Postgres, and you can connect to the warehouse from DBeaver or psql.

---

## Phase 1 — Explore the GTFS data (easy)

**Goal:** understand the data before writing any pipeline code. This is a notebook, not production code.

- [ ] Download the feed manually: `israel-public-transportation.zip` from the Ministry of Transport.
  - 📖 [MoT GTFS files](https://gtfs.mot.gov.il/gtfsfiles/)
- [ ] Load each file in pandas. For each one, record the **row count, primary key, and file size**. Keep these numbers; they go in the README.
  - Hint: `stop_times.txt` is huge. Try `pd.read_csv(..., nrows=1_000_000)` first, or read it in chunks.
- [ ] Draw how the files join: `routes → trips → stop_times ← stops`, and `trips.service_id → calendar`.
  - **Learn:** the GTFS spec, especially `trips`, `stop_times` and `calendar`. Read the field definitions, not just the file list.
  - 📖 [GTFS Schedule reference](https://gtfs.org/documentation/schedule/reference/)
- [ ] Check for these and write down what you find:
  - Times greater than `24:00:00` in `stop_times` (trips after midnight belong to the previous service day)
  - Whether `feed_info.txt` exists and contains a version
  - Whether `calendar_dates.txt` exists (holiday exceptions)
  - How many days ahead the feed covers (`calendar.start_date` / `end_date`)
- [ ] Plot all stops on a map with GeoPandas. It's a quick sanity check and makes a nice README image.

**Done when:** you can explain, without notes, how to get "all departures from stop X on date Y."

---

## Phase 2 — Ingestion DAG: raw data into Postgres (easy → medium)

**Goal:** a daily DAG that lands GTFS in Postgres, untouched.

- [ ] Build a DAG with small, separate tasks: `download → check_version → extract → load_raw`.
  - **Learn:** DAGs, tasks, operators vs. the TaskFlow API (`@task`), schedules, retries, and how tasks pass data (XCom, and why you pass file paths rather than DataFrames).
  - 📖 [Airflow tutorial: fundamentals](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html) · [TaskFlow tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [ ] Load into a `raw` schema: one table per GTFS file, all columns as text, values exactly as they come, plus `feed_version` and `loaded_at` columns. **No cleaning here.**
  - Use Postgres `COPY` for `stop_times`. Row-by-row inserts will be painfully slow.
  - 📖 [psycopg COPY](https://www.psycopg.org/psycopg3/docs/basic/copy.html)
- [ ] **Incremental:** hash the zip (or read `feed_info.txt`). If that version is already loaded, skip the rest of the run. Airflow's `ShortCircuitOperator` or `@task.short_circuit` does this cleanly.
- [ ] **Idempotent:** for a given version, `DELETE` its rows and then `INSERT`, inside one transaction. Re-running the same day must give the same result.
  - **Learn:** idempotency vs. incremental loading. They're different things and interviewers love asking about them. Also: why backfills require idempotent tasks.
  - 📖 [Airflow best practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [ ] Keep old versions; don't overwrite them. You'll need the history for SCD2 in Phase 11.
- [ ] Log row counts per table per run. These are the "rows processed per run" numbers for your CV.

**Done when:** the DAG runs twice in a row, the second run skips, and a forced re-run doesn't create duplicates.

---

## Phase 3 — dbt: staging and a first mart (medium)

**Goal:** clean, typed tables and a first star schema, all in SQL.

- [ ] Create a dbt project connected to the warehouse Postgres.
  - **Learn:** what dbt does (ELT inside the warehouse), models, `ref()`, `source()`, materializations (view vs. table vs. incremental).
  - 📖 [dbt Postgres setup](https://docs.getdbt.com/docs/core/connect-data-platform/postgres-setup) · [dbt Fundamentals course (free)](https://learn.getdbt.com/courses/dbt-fundamentals)
- [ ] **Staging models**, one per raw table: cast types, rename columns, filter to the latest feed version. No joins here.
- [ ] **Dimensions:** `dim_route` (joined with agency), `dim_stop`, `dim_date`.
  - `dim_date` can be generated with the `dbt_utils.date_spine` macro.
  - 📖 [dbt_utils package](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/)
- [ ] Write each model's description, including the **grain** of every fact table: "one row = …".
  - **Learn:** facts vs. dimensions, grain, star vs. snowflake schema.
  - 📖 [Kimball dimensional modeling techniques](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
- [ ] Add a dbt task to the Airflow DAG after the load (`BashOperator` running `dbt build` is fine to start).

**Done when:** `dbt build` succeeds and `dbt docs generate` shows the lineage graph. Screenshot it for the README.

---

## Phase 4 — PySpark: expand the calendar into real departures (medium)

**Goal:** turn "trip X runs on Sundays" into "trip X departs stop Y on 2026-10-05 at 08:14." This is the step that justifies Spark.

- [ ] Learn PySpark basics on one GTFS file first.
  - **Learn:** DataFrames, lazy evaluation (transformations vs. actions), partitions, what a shuffle is.
  - 📖 [PySpark DataFrame quickstart](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html) · [DE Zoomcamp, module 5: Batch processing with Spark](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- [ ] Expand `calendar` (+ `calendar_dates` exceptions) into one row per `(service_id, date)` for the next 7 days.
- [ ] Join `stop_times ⋈ trips ⋈ expanded calendar ⋈ routes`. Convert GTFS times (which can exceed 24:00) into real timestamps on the right date.
- [ ] Broadcast the small tables (`routes`, expanded calendar) so only `stop_times` gets shuffled.
  - **Learn:** broadcast joins and why they avoid a shuffle. Read the Spark UI (`localhost:4040`) and compare the plan before and after.
  - 📖 [Spark SQL performance tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [ ] Write the output to Postgres (JDBC) as `int_departures`. dbt then builds `fact_departures` from it.
  - 📖 [Spark JDBC data source](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)
- [ ] Record the output row count and the runtime. That's an interview story: "N trips became M departure rows."

**Done when:** `fact_departures` exists with grain _one scheduled departure of one trip from one stop on one date_, for 7 days.

---

## Phase 5 — Geospatial: stops to cities (easy for you)

**Goal:** add `dim_city` so everything can be grouped by municipality. This is your home turf.

- [ ] Find a public municipal boundaries file (search data.gov.il for "גבולות שיפוט" / municipal boundaries). Note its CRS.
  - 📖 [data.gov.il](https://data.gov.il/)
- [ ] Spatially join stops (points) to municipalities (polygons) with `gpd.sjoin`. Handle stops that fall outside any boundary rather than silently dropping them.
  - 📖 [GeoPandas spatial joins](https://geopandas.org/en/stable/docs/user_guide/mergingdata.html)
- [ ] Optional: add population per city from the Central Bureau of Statistics, to compute departures per resident.
  - 📖 [CBS](https://www.cbs.gov.il/)
- [ ] Load `stop_city` into Postgres and build `dim_city` in dbt.

**Done when:** you can query "scheduled departures per city, weekday vs. Saturday." Look at the Shabbat numbers; that's your first README finding.

---

## Phase 6 — Data quality, tests, CI, README v1 (medium)

**Goal:** make the project trustworthy and presentable. **After this phase, v1 goes on the CV.**

- [ ] dbt tests on every key: `unique`, `not_null`, `relationships`. Add `accepted_values` for things like `route_type`.
  - **Learn:** the data quality dimensions (completeness, uniqueness, validity, freshness), and whether a failure should stop the pipeline or only alert.
  - 📖 [dbt data tests](https://docs.getdbt.com/docs/build/data-tests)
- [ ] Add a row-count check (loaded vs. source file) and make the DAG fail when it doesn't match.
- [ ] pytest: 3–5 tests on pure functions. Start with the GTFS time parser (`"25:30:00"` should become the next day at 01:30).
  - 📖 [pytest: get started](https://docs.pytest.org/en/stable/getting-started.html)
- [ ] GitHub Actions: run pytest on every push.
  - 📖 [Building and testing Python with GitHub Actions](https://docs.github.com/en/actions/automating-builds-and-tests/building-and-testing-python)
- [ ] README v1:
  - The question, the data source, the numbers (rows, runtime)
  - An architecture diagram in Mermaid (GitHub renders it)
  - Run it in 3 steps: clone, `docker compose up`, trigger the DAG
  - **Design decisions**, one short paragraph each: why incremental, why delete-then-insert, why Spark for this step, why this grain
  - One or two findings, with a chart
  - 📖 [Mermaid diagrams on GitHub](https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/creating-diagrams)
- [ ] Pin the repo on your GitHub profile with topics: `airflow`, `spark`, `dbt`, `data-engineering`, `gtfs`.

**Done when:** a stranger can clone the repo, run it, and understand what it does from the README alone.

---

# ⬇️ The hard part — start only after Phase 6

---

## Phase 7 — Explore the real-time data (medium)

**Goal:** understand SIRI before building on it, exactly like Phase 1.

- [ ] Read how the Open Bus project (Hasadna) collects and stores the data.
  - **Learn:** what SIRI-SM is (stop monitoring), what a snapshot contains, and how SIRI rides and vehicle locations relate to GTFS trips.
  - 📖 [Open Bus Stride overview](https://github.com/hasadna/open-bus-pipelines/blob/main/STRIDE.md) · [SIRI data docs](<https://github.com/hasadna/open-bus/wiki/Bus-Real-Time-(SIRI)-Data-Documentation>)
- [ ] Try both access paths and pick one:
  - The REST API / Python client (`pip install open-bus-stride-client`): easier, but rate-limited and paginated
  - Raw snapshot files: more work, but this is the real-volume path
  - 📖 [Stride API docs](https://open-bus-stride-api.hasadna.org.il/docs) · [Stride Python client](https://github.com/hasadna/open-bus-stride-client) · [Raw snapshots](https://open-bus-siri-requester.hasadna.org.il/)
- [ ] Download **one day**. Measure: number of snapshots, rows, file size, and how often vehicles report. Multiply by 30 and by 365 to get the real volume. These numbers decide the design of Phase 8.
- [ ] Find the gaps: missing minutes, duplicate pings, GPS jumps, vehicles with no trip match. Write them down; each becomes a data quality rule.
- [ ] Ask in Hasadna's `#open-bus` Slack channel if something is unclear. They're friendly, and it's a good networking move too.

**Done when:** you know the volume of one day and the join key from a SIRI record to a GTFS trip.

---

## Phase 8 — Data lake setup (medium → hard)

**Goal:** storage that handles hundreds of millions of rows. Postgres keeps only the final marts.

- [ ] Add MinIO (a local S3-compatible store) to Docker Compose.
  - 📖 [MinIO container quickstart](https://min.io/docs/minio/container/index.html)
- [ ] Use a bronze / silver / gold layout:
  - **bronze:** raw snapshots exactly as downloaded
  - **silver:** parsed, typed, deduplicated Parquet, partitioned by date
  - **gold:** business tables ready for the marts
  - **Learn:** the medallion architecture, Parquet (columnar, compressed), partitioning and partition pruning, and the difference between a data lake, a warehouse and a lakehouse.
  - 📖 [Databricks: medallion architecture](https://www.databricks.com/glossary/medallion-architecture)
- [ ] Use Delta Lake (or Iceberg) for silver and gold tables, so you get MERGE, ACID writes and time travel on top of Parquet.
  - **Learn:** what a table format adds over plain Parquet files.
  - 📖 [Delta Lake quickstart](https://docs.delta.io/latest/quick-start.html)
- [ ] Configure Spark to read from and write to MinIO via `s3a://`.
  - **Learn:** the `hadoop-aws` package and the S3A config keys. Expect some friction here; it's a normal rite of passage.
- [ ] Decide where the marts live. Option A: keep Postgres. Option B: DuckDB reading the gold Parquet directly, with `dbt-duckdb`. Write down why in the README.
  - 📖 [dbt-duckdb](https://github.com/duckdb/dbt-duckdb)

**Done when:** Spark writes a partitioned Delta table to MinIO, and you can query a single day without scanning the others.

---

## Phase 9 — SIRI backfill DAG (hard)

**Goal:** load one month of real-time history, safely and re-runnably.

- [ ] Build a DAG parametrized by date: `download_day → land_bronze → parse_to_silver`.
- [ ] Use Airflow's backfill to run it for 30 past dates. Limit concurrency so you don't overload Hasadna's servers or your laptop.
  - **Learn:** `logical_date`, `catchup`, backfills, `max_active_runs`, and why every task must be idempotent for a backfill to be safe.
  - 📖 [Airflow: DAG runs, catchup and backfill](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html)
- [ ] Write silver per date partition with overwrite-by-partition (or Delta `replaceWhere`), so re-running a day replaces only that day.
- [ ] Deduplicate pings: same vehicle and same timestamp count once.
- [ ] Track completeness per day (snapshots received vs. expected) in a small quality table.

**Done when:** 30 days are in silver, and re-running any single day gives identical results.

---

## Phase 10 — From GPS pings to arrival events (hard, and the core of the project)

**Goal:** infer "trip X arrived at stop Y at 08:14" from raw vehicle positions. This is the most interesting engineering in the project.

- [ ] For each ride, find the pings near each planned stop (for example within 50–100 m), and take the closest one as the arrival time. Start simple; refine later.
  - **Learn:** distance calculations at scale. Project to a metric CRS (Israel TM Grid, EPSG:2039) rather than computing in lat/lon degrees.
  - 📖 [EPSG:2039](https://epsg.io/2039)
- [ ] Do it in Spark, not GeoPandas. Approach: join pings to the trip's planned stops by trip ID, compute the distance per pair, keep the minimum with a window function.
  - **Learn:** Spark window functions (`row_number` over a partition), and how to keep a "pings × stops" join from exploding (filter early, join only within the same trip).
  - 📖 [PySpark Window functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)
- [ ] Handle the messy cases explicitly, and write down each choice:
  - Vehicle never came near a stop: mark it as missing, don't guess
  - GPS jumps: drop pings implying an impossible speed
  - Loops, where a route passes the same spot twice: use stop sequence order
- [ ] Validate on a few rides by eye: plot pings and stops on a map for one trip.

**Done when:** `fact_stop_arrival` exists with grain _one actual arrival of one trip at one stop_, and spot checks look right.

---

## Phase 11 — Planned vs. actual, plus schedule history (hard)

**Goal:** answer the project's question.

- [ ] Join actual arrivals to planned departures (`fact_departures`) on trip, stop and date.
- [ ] Build the marts:
  - **Punctuality:** delay distribution per route, operator, city, hour. Use percentiles (median, p90), not just averages.
  - **Ghost trips:** planned trips with no actual ride at all.
  - **Headway reliability:** for frequent lines, the actual gap between consecutive buses vs. the planned gap.
  - **Learn:** SQL window functions (`LAG`, `LEAD`, percentile functions). This doubles as interview practice.
  - 📖 [DataLemur SQL tutorial](https://datalemur.com/sql-tutorial)
- [ ] Turn `dim_route` and `dim_stop` into **SCD Type 2** using dbt snapshots over the daily GTFS versions you've been keeping since Phase 2. A delay in March must be matched to the route as it was defined in March.
  - **Learn:** slowly changing dimensions (types 1, 2, 3), and how dbt snapshots implement type 2 with `valid_from` / `valid_to`.
  - 📖 [dbt snapshots](https://docs.getdbt.com/docs/build/snapshots) · [Kimball on SCDs](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/)
- [ ] dbt tests for the new marts, including a freshness check on the SIRI source.
  - 📖 [dbt source freshness](https://docs.getdbt.com/docs/deploy/source-freshness)

**Done when:** you can answer "which operator in which city has the most ghost trips?" with one SQL query.

---

## Phase 12 — Scale, tune, and tell the story (hard)

**Goal:** prove the pipeline handles real volume, and make the README v2 tell the story.

- [ ] Extend the backfill from one month to several months, or a year if the archive allows.
- [ ] Profile the heaviest Spark job in the Spark UI. Look for skew: Tel Aviv and Jerusalem partitions will be far bigger than small towns.
  - **Learn:** data skew, adaptive query execution (AQE), salting, and how many partitions to use.
  - 📖 [Spark SQL performance tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html) (sections on AQE and skew joins)
- [ ] Record before/after numbers for at least one optimization (runtime, shuffle size). This is a CV bullet.
- [ ] README v2:
  - Updated architecture diagram with the lake
  - Volume numbers: rows per day, total rows, runtime
  - The findings, with 2–3 charts (the ghost-trip numbers are the headline)
  - New design decisions: why a lake for SIRI, why Delta, how arrivals are inferred and what the known limitations are
- [ ] Update the CV Projects section and LinkedIn.

---

## Design decisions log

Write down every non-obvious choice the moment you make it: one line, the date, what you chose and why. It takes 30 seconds and turns into the README's best section, and into your interview answers.

| Date       | Decision                         | Why                                                                                                                        |
| ---------- | -------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| 2026-10-03 | speperate postgres for warehouse | Different services for different purposes: I want to be able to replace one of them in the future while keeping the other. |

## Numbers to collect along the way

| Metric                          | Value |
| ------------------------------- | ----- |
| GTFS zip size / unzipped size   |       |
| `stop_times` rows               |       |
| Departures generated for 7 days |       |
| Spark job runtime (Phase 4)     |       |
| SIRI rows per day               |       |
| SIRI rows total                 |       |
| Arrival inference runtime       |       |
| Optimization before / after     |       |
