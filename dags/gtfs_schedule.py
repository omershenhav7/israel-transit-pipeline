"""Daily: download the MOT GTFS feed, load it raw into the warehouse, and snapshot today's planned trips."""
import shutil
from contextlib import closing
from datetime import date, timedelta
from pathlib import Path

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, get_current_context, task

from transit import gtfs

DATA_DIR = Path("/opt/airflow/data/gtfs")
WAREHOUSE_CONN_ID = "warehouse"
LOCAL_TZ = "Asia/Jerusalem"


def _service_date() -> date:
    """The Israel-local calendar day this run is for."""
    context = get_current_context()
    run_time = context.get("logical_date") or context["dag_run"].run_after
    return pendulum.instance(run_time).in_timezone(LOCAL_TZ).date()


@dag(
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz=LOCAL_TZ),
    catchup=False,  # the feed only covers today onward, so past days cannot be backfilled
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["gtfs", "schedule"],
)
def gtfs_schedule():
    @task
    def download() -> str:
        zip_path = gtfs.download_feed(DATA_DIR / _service_date().isoformat())
        return str(gtfs.extract_feed(zip_path))

    @task
    def load_raw(feed_dir: str) -> dict[str, int]:
        with closing(PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).get_conn()) as conn:
            return gtfs.load_raw(conn, Path(feed_dir))

    @task
    def build_planned_trips() -> int:
        with closing(PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).get_conn()) as conn:
            return gtfs.build_planned_trips(conn, _service_date())

    @task
    def cleanup(feed_dir: str) -> None:
        # Keep the zip for reprocessing, drop the ~800 MB of extracted text files
        shutil.rmtree(feed_dir)

    feed_dir = download()
    load_raw(feed_dir) >> build_planned_trips() >> cleanup(feed_dir)


gtfs_schedule()
