# Israel Transit Pipeline

A daily data pipeline that keeps a history of the Israeli bus schedule, to answer two questions per day:

1. Which bus lines most often **never show up**?
2. Which bus lines are **late the most**?

The Ministry of Transport's GTFS feed only covers today onward, so the pipeline captures each day's schedule into a growing star schema: Delta Lake tables on Amazon S3, built and queried with PySpark on a free Oracle Cloud VM.

- [Architecture](docs/architecture.md): how the schedule history is built and protected
- [Schema](docs/schema.md): vocabulary (line, route, trip, stop, station) and the star schema

**Status:** in development. The daily capture has run on the VM since 2026-11-15, and each captured feed is built into bronze and silver Delta tables. Gold and the quality gate are not built yet. See [stages 0 and 1](docs/stage-0-1.md), [bronze](docs/stage-2-bronze.md) and [silver](docs/stage-3-silver.md).
