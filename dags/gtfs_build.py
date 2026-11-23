"""Build the bronze, then the silver, tables from a landed GTFS zip: one spark-submit job per table, one at a time."""
import shlex
from datetime import timedelta

import pendulum
from airflow.sdk import Param, dag, task

from transit.assets import GTFS_LANDING
from transit.bronze import TABLES, resolve_feed_date
from transit.lakehouse import SOURCES  # not transit.silver: that imports pyspark

LOCAL_TZ = "Asia/Jerusalem"
BRONZE_JOB = "/opt/airflow/src/transit/jobs/bronze.py"
SILVER_JOB = "/opt/airflow/src/transit/jobs/silver.py"


def _event_extras(triggering_asset_events) -> list[dict]:
    """The `extra` dicts of the events of GTFS_LANDING that triggered this run (none for a manual run)."""
    try:
        events = triggering_asset_events[GTFS_LANDING]
    except KeyError:
        return []
    return [dict(e.extra or {}) for e in events]


@dag(
    schedule=[GTFS_LANDING],
    start_date=pendulum.datetime(2026, 11, 15, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    params={
        "feed_date": Param(
            None, type=["null", "string"],
            description="YYYY-MM-DD. Set on a manual run to rebuild that day; wins over the triggering events.",
        )
    },
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["gtfs", "bronze", "silver"],
)
def gtfs_build():
    @task(retries=0)
    def feed_date(*, params, triggering_asset_events) -> str:
        return resolve_feed_date(_event_extras(triggering_asset_events), params.get("feed_date"))

    day = feed_date()

    bronze = {}
    for table in TABLES:

        @task.bash(
            task_id=f"bronze_{table}", pool="spark", retries=2, retry_delay=timedelta(minutes=10),
            execution_timeout=timedelta(hours=1),
        )
        def build_table(day: str, table: str = table) -> str:
            from transit.spark import spark_submit_command  # not at parse time: it imports pyspark

            return shlex.join(spark_submit_command(BRONZE_JOB, "--table", table, "--feed-date", day))

        bronze[table] = build_table(day)

    for table, sources in SOURCES.items():

        @task.bash(
            task_id=f"silver_{table}", pool="spark", retries=2, retry_delay=timedelta(minutes=10),
            execution_timeout=timedelta(hours=1),
        )
        def build_silver(day: str, table: str = table) -> str:
            from transit.spark import spark_submit_command  # not at parse time: it imports pyspark

            return shlex.join(spark_submit_command(SILVER_JOB, "--table", table, "--feed-date", day))

        silver = build_silver(day)
        for source in sources:
            bronze[source] >> silver


gtfs_build()
