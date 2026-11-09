"""Spark session for the lakehouse: Delta Lake tables on S3, read and written through s3a."""
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Must match the Hadoop version bundled with pyspark 4.2.0
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.5.0"
AWS_SDK_BUNDLE = "software.amazon.awssdk:bundle"


def get_spark(app_name: str = "transit") -> SparkSession:
    """Local Spark with Delta and S3. Credentials come from the AWS_* environment variables."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        # Read only when the JVM starts, so it has to be set here, not after
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_DEFAULT_REGION", "eu-north-1"))
        .config("spark.sql.session.timeZone", "Asia/Jerusalem")
    )
    if os.environ.get("SPARK_IVY_DIR"):
        # Libraries baked into the image at build time (see Dockerfile)
        builder = builder.config("spark.jars.ivy", os.environ["SPARK_IVY_DIR"])
    if os.environ.get("AWS_SDK_BUNDLE"):
        # Loaded as a Spark package, the 686 MB SDK bundle makes the executor heartbeat fail with
        # "idWithoutTopologyInfo is null" every 10 seconds. On the driver classpath it doesn't.
        # Only the bundle goes there: it is fully shaded, so it can't shadow Spark's own libraries.
        builder = builder.config("spark.jars.excludes", AWS_SDK_BUNDLE).config(
            "spark.driver.extraClassPath", os.environ["AWS_SDK_BUNDLE"]
        )
    return configure_spark_with_delta_pip(builder, extra_packages=[HADOOP_AWS]).getOrCreate()


def lakehouse_path(*parts: str, bucket: str | None = None) -> str:
    """s3a:// path inside the project bucket, e.g. lakehouse_path("silver", "trips")."""
    bucket = bucket or os.environ["GTFS_BUCKET"]
    return "/".join([f"s3a://{bucket}", *parts])
