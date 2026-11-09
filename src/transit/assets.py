"""Airflow Assets shared between DAGs."""
from airflow.sdk import Asset

# A new feed zip reached S3 landing. Events carry {"feed_date": "YYYY-MM-DD"}.
GTFS_LANDING = Asset("gtfs_landing")
