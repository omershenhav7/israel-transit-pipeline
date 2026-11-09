"""Stage 0: prove PySpark can write a partitioned Delta table to S3, overwrite one partition, and read it back.

Run inside the Airflow image on the VM:
    docker compose run --rm airflow-cli python /opt/airflow/scripts/stage0_spike.py
Deletes its test data at the end, whether it passed or not.
"""
import os
import sys
from datetime import date

import boto3

from transit.spark import get_spark, lakehouse_path

PREFIX = "spike/"
D1, D2 = date(2026, 1, 1), date(2026, 1, 2)


def rows(df):
    return sorted((r.service_date, r.trip_id, r.n) for r in df.collect())


def main() -> int:
    bucket = os.environ["GTFS_BUCKET"]
    path = lakehouse_path("spike", "trips")
    spark = get_spark("stage0_spike")
    try:
        first = spark.createDataFrame(
            [(D1, "a", 1), (D1, "b", 2), (D2, "c", 3)], "service_date date, trip_id string, n int"
        )
        first.write.format("delta").mode("overwrite").partitionBy("service_date").save(path)

        # Replace only D1. D2 must survive untouched.
        replacement = spark.createDataFrame([(D1, "z", 9)], "service_date date, trip_id string, n int")
        (
            replacement.write.format("delta").mode("overwrite")
            .option("replaceWhere", f"service_date = '{D1.isoformat()}'")
            .save(path)
        )

        got = rows(spark.read.format("delta").load(path))
        expected = [(D1, "z", 9), (D2, "c", 3)]
        versions = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").count()
        print(f"read back: {got}")
        print(f"delta versions: {versions}")
        if got != expected or versions != 2:
            print(f"FAIL: expected {expected} and 2 versions")
            return 1
        print("PASS")
        return 0
    finally:
        spark.stop()
        # Plain delete leaves delete markers. The key can't remove old versions, so they expire by lifecycle or by hand.
        b = boto3.resource("s3").Bucket(bucket)
        b.objects.filter(Prefix=PREFIX).delete()
        print(f"deleted s3://{bucket}/{PREFIX}")


if __name__ == "__main__":
    sys.exit(main())
