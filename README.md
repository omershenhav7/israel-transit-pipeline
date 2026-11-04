# Israel Transit Pipeline

A daily data pipeline that keeps a history of the Israeli bus schedule, to answer two questions per day:

1. Which bus lines most often **never show up**?
2. Which bus lines are **late the most**?

The Ministry of Transport's GTFS feed only covers today onward, so the pipeline captures each day's schedule into a growing star schema: Delta Lake tables on Amazon S3, built and queried locally with PySpark.

- [Architecture](docs/architecture.md): how the schedule history is built and protected
- [Schema](docs/schema.md): vocabulary (line, route, trip, stop, station) and the star schema

**Status:** in development, not in operation yet. The first DAG, `gtfs_capture`, is written. See [Stages 0 and 1](docs/stage-0-1.md).
