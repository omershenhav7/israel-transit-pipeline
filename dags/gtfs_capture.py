"""Daily: store the MOT GTFS feed in S3 landing, and record the day as missed if that never succeeds."""
import os
from datetime import date, timedelta

import boto3
import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

from transit import landing
from transit.assets import GTFS_LANDING

LOCAL_TZ = "Asia/Jerusalem"


def _service_date() -> date:
    """The Israel-local calendar day this run is for."""
    context = get_current_context()
    run_time = context.get("logical_date") or context["dag_run"].run_after
    return pendulum.instance(run_time).in_timezone(LOCAL_TZ).date()


@dag(
    schedule="0 4 * * *",
    # Set to the launch day. Catchup records every day since then that has no zip as missed.
    start_date=pendulum.datetime(2027, 1, 1, tz=LOCAL_TZ),
    catchup=True,  # never backfills: a run for a past day fails without downloading and records the gap
    max_active_runs=1,
    # Hourly attempts from 04:00 to 23:00, the last chance to capture the day
    default_args={"retries": 19, "retry_delay": timedelta(hours=1), "execution_timeout": timedelta(minutes=30)},
    tags=["gtfs", "landing"],
)
def gtfs_capture():
    @task(outlets=[GTFS_LANDING])
    def capture_feed(*, outlet_events) -> None:
        service_date = _service_date()
        try:
            stored = landing.capture_feed(
                boto3.client("s3"), os.environ["GTFS_BUCKET"], service_date, pendulum.now(LOCAL_TZ).date()
            )
        except landing.MissedDayError as e:
            raise AirflowFailException(str(e)) from e
        if not stored:
            # No new zip, so no Asset event and no new build
            raise AirflowSkipException(f"{service_date} is already captured")
        outlet_events[GTFS_LANDING].extra = {"feed_date": service_date.isoformat()}

    @task(trigger_rule="all_failed", retries=3, retry_delay=timedelta(minutes=5))
    def record_missed() -> None:
        landing.record_missed(boto3.client("s3"), os.environ["GTFS_BUCKET"], _service_date())

    capture_feed() >> record_missed()


gtfs_capture()
