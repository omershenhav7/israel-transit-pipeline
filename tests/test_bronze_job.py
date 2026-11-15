import shutil
import tempfile
from datetime import date
from pathlib import Path

import pytest
from botocore.exceptions import ClientError
from conftest import feed_text, make_zip

from transit.bronze import BronzeLoadError, build_bronze_table
from transit.jobs.bronze import main
from transit.spark import DELTA_SPARK, HADOOP_AWS, spark_submit_command

DAY = date(2026, 11, 15)
BUCKET = "test-bucket"


def agency_rows() -> list[list[str]]:
    return [
        ["1", "A", "http://a", "Asia/Jerusalem", "he", "", ""],
        ["2", "B", "http://b", "Asia/Jerusalem", "he", "", ""],
    ]


class FakeS3:
    """download_file copies a local zip, or fails like S3 does."""

    def __init__(self, zip_path: Path | None = None, error: ClientError | None = None):
        self.zip_path = zip_path
        self.error = error
        self.calls: list[tuple[str, str]] = []

    def download_file(self, bucket, key, local_path):
        self.calls.append((bucket, key))
        if self.error:
            raise self.error
        shutil.copy(self.zip_path, local_path)


def test_build_loads_from_landing_zip(spark, tmp_path):
    zip_path = make_zip(tmp_path / "gtfs.zip", {"agency.txt": feed_text("agency", agency_rows())})
    s3 = FakeS3(zip_path)
    table_path = str(tmp_path / "agency")

    n = build_bronze_table(spark, s3, BUCKET, "agency", DAY, DAY, table_path=table_path)

    assert n == 2
    key = "landing/gtfs/feed_date=2026-11-15/gtfs.zip"
    assert s3.calls == [(BUCKET, key)]
    sources = spark.read.format("delta").load(table_path).select("_source_file").distinct().collect()
    assert [r["_source_file"] for r in sources] == [key]


def test_build_without_zip_fails_and_writes_nothing(spark, tmp_path):
    s3 = FakeS3(error=ClientError({"Error": {"Code": "404"}}, "HeadObject"))
    table_path = tmp_path / "agency"

    with pytest.raises(BronzeLoadError, match="no zip"):
        build_bronze_table(spark, s3, BUCKET, "agency", DAY, DAY, table_path=str(table_path))

    assert not table_path.exists()


def test_build_names_the_table_on_a_bad_file(spark, tmp_path):
    zip_path = make_zip(tmp_path / "gtfs.zip", {"agency.txt": "wrong,header\r\n1,2\r\n"})
    with pytest.raises(BronzeLoadError, match="bronze agency 2026-11-15"):
        build_bronze_table(spark, FakeS3(zip_path), BUCKET, "agency", DAY, DAY,
                           table_path=str(tmp_path / "agency"))


def test_other_client_errors_are_not_hidden(spark, tmp_path):
    s3 = FakeS3(error=ClientError({"Error": {"Code": "403"}}, "HeadObject"))
    with pytest.raises(ClientError):
        build_bronze_table(spark, s3, BUCKET, "agency", DAY, DAY, table_path=str(tmp_path / "agency"))


def test_build_removes_its_temp_folder(spark, tmp_path, monkeypatch):
    temp = tmp_path / "t"
    temp.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temp))
    zip_path = make_zip(tmp_path / "gtfs.zip", {"agency.txt": feed_text("agency", agency_rows())})

    build_bronze_table(spark, FakeS3(zip_path), BUCKET, "agency", DAY, DAY,
                       table_path=str(tmp_path / "agency"))
    assert list(temp.iterdir()) == []

    missing = FakeS3(error=ClientError({"Error": {"Code": "404"}}, "HeadObject"))
    with pytest.raises(BronzeLoadError):
        build_bronze_table(spark, missing, BUCKET, "agency", DAY, DAY,
                           table_path=str(tmp_path / "agency2"))
    assert list(temp.iterdir()) == []


def test_spark_submit_command_carries_jvm_settings(monkeypatch):
    monkeypatch.setenv("SPARK_IVY_DIR", "/ivy")
    monkeypatch.setenv("AWS_SDK_BUNDLE", "/jars/bundle.jar")
    monkeypatch.setenv("SPARK_DRIVER_MEMORY", "3g")

    cmd = spark_submit_command("/x/job.py", "--table", "agency")

    assert cmd[:3] == ["spark-submit", "--master", "local[*]"]
    assert cmd[cmd.index("--driver-memory") + 1] == "3g"
    assert cmd[cmd.index("--driver-class-path") + 1] == "/jars/bundle.jar"
    assert cmd[cmd.index("--exclude-packages") + 1] == "software.amazon.awssdk:bundle"
    packages = cmd[cmd.index("--packages") + 1]
    assert DELTA_SPARK in packages and HADOOP_AWS in packages
    assert "spark.log.level=WARN" in cmd
    assert "spark.jars.ivy=/ivy" in cmd
    assert cmd[-3:] == ["/x/job.py", "--table", "agency"]


def test_spark_submit_command_without_optional_settings(monkeypatch):
    for v in ("SPARK_IVY_DIR", "AWS_SDK_BUNDLE", "SPARK_DRIVER_MEMORY"):
        monkeypatch.delenv(v, raising=False)
    cmd = spark_submit_command("/x/job.py")
    assert cmd[cmd.index("--driver-memory") + 1] == "4g"
    assert "--driver-class-path" not in cmd and "--exclude-packages" not in cmd
    assert not any("spark.jars.ivy" in c for c in cmd)


def test_delta_spark_matches_installed_delta():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = configure_spark_with_delta_pip(SparkSession.builder)
    assert DELTA_SPARK in builder._options["spark.jars.packages"]


def test_job_rejects_unknown_table():
    with pytest.raises(SystemExit) as e:
        main(["--table", "shapes", "--feed-date", "2026-11-15"])
    assert e.value.code != 0
