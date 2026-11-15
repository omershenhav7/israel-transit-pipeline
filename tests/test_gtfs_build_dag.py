from datetime import timedelta

import pytest

from transit.bronze import TABLES, resolve_feed_date


def test_resolve_feed_date_from_event():
    assert resolve_feed_date([{"feed_date": "2026-11-15"}], None) == "2026-11-15"


def test_resolve_feed_date_param_wins():
    assert resolve_feed_date([{"feed_date": "2026-11-15"}], "2026-11-10") == "2026-11-10"


def test_resolve_feed_date_rejects_two_days():
    events = [{"feed_date": "2026-11-14"}, {"feed_date": "2026-11-15"}]
    with pytest.raises(ValueError, match="2026-11-14") as exc:
        resolve_feed_date(events, None)
    assert "2026-11-15" in str(exc.value)


def test_resolve_feed_date_same_day_twice_is_fine():
    events = [{"feed_date": "2026-11-15"}, {"feed_date": "2026-11-15"}]
    assert resolve_feed_date(events, None) == "2026-11-15"


def test_resolve_feed_date_needs_one():
    with pytest.raises(ValueError):
        resolve_feed_date([], None)


def test_resolve_feed_date_rejects_a_bad_date():
    with pytest.raises(ValueError):
        resolve_feed_date([{"feed_date": "15/11/2026"}], None)
    with pytest.raises(ValueError):
        resolve_feed_date([], "yesterday")


def test_dag_shape(tmp_path, monkeypatch):
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path))
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder="/opt/airflow/dags")
    assert bag.import_errors == {}
    dag = bag.dags["gtfs_build"]
    assert dag.max_active_runs == 1
    assert dag.catchup is False
    assert {"gtfs", "bronze"} <= set(dag.tags)
    bronze = {t.task_id: t for t in dag.tasks if t.task_id.startswith("bronze_")}
    assert set(bronze) == {f"bronze_{t}" for t in TABLES}
    assert dag.get_task("feed_date").retries == 0
    for task in bronze.values():
        assert task.pool == "spark"
        assert task.retries == 2
        assert task.execution_timeout == timedelta(hours=1)
        assert task.upstream_task_ids == {"feed_date"}
        assert task.downstream_task_ids == set()
