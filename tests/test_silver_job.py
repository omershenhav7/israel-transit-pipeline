from test_silver_build import put, route

from transit.jobs import silver as job


def test_job_builds_one_table(spark, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("GTFS_BUCKET", "test-bucket")
    monkeypatch.setattr(job, "lakehouse_path", lambda *parts: str(tmp_path))
    monkeypatch.setattr(job, "get_spark", lambda app_name="transit": spark)
    put(spark, tmp_path, "routes", [route(route_id="R1"), route(route_id="R2"),
                                    route(route_id="R3", route_type="2")])

    assert job.main(["--table", "routes", "--feed-date", "2026-11-15"]) == 0

    assert "silver routes 2026-11-15: kept 2, rejected 0, non-bus 1" in capsys.readouterr().out
