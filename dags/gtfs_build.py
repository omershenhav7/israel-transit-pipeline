"""Build the bronze tables from a landed GTFS zip: one spark-submit job per table, one at a time."""
import shlex
from datetime import timedelta

import pendulum
from airflow.sdk import Param, dag, task

from transit.assets import GTFS_LANDING
from transit.bronze import TABLES, resolve_feed_date
from transit.spark import spark_submit_command

LOCAL_TZ = "Asia/Jerusalem"
JOB = "/opt/airflow/src/transit/jobs/bronze.py"


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
    tags=["gtfs", "bronze"],
)
def gtfs_build():
    @task
    def feed_date(*, params, triggering_asset_events) -> str:
        return resolve_feed_date(_event_extras(triggering_asset_events), params.get("feed_date"))

    day = feed_date()

    for table in TABLES:

        @task.bash(task_id=f"bronze_{table}", pool="spark", retries=2, retry_delay=timedelta(minutes=10))
        def build_table(day: str, table: str = table) -> str:
            return shlex.join(spark_submit_command(JOB, "--table", table, "--feed-date", day))

        build_table(day)


gtfs_build()
