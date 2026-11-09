"""Capture the Israel MOT GTFS feed into S3 landing: once per day, and only on its own day."""
import csv
import io
import json
import tempfile
import zipfile
from datetime import date
from pathlib import Path

import requests
from botocore.exceptions import ClientError

GTFS_URL = "https://gtfs.mot.gov.il/gtfsfiles/israel-public-transportation.zip"
REQUIRED_FILES = {"agency.txt", "calendar.txt", "routes.txt", "stops.txt", "stop_times.txt", "trips.txt"}


class MissedDayError(Exception):
    """The day has passed with no zip stored. The feed no longer contains it, so it cannot be captured."""


class InvalidFeedError(ValueError):
    """The downloaded file is not a complete feed for the service date."""


def zip_key(feed_date: date) -> str:
    return f"landing/gtfs/feed_date={feed_date.isoformat()}/gtfs.zip"


def missed_key(feed_date: date) -> str:
    return f"landing/gtfs/feed_date={feed_date.isoformat()}/_MISSED.json"


def capture_feed(s3, bucket: str, service_date: date, today: date, download=None) -> bool:
    """Store the service date's feed. Returns True if stored, False if it was already there."""
    key = zip_key(service_date)
    # Checked before the date, so re-running an old day that was captured never marks it as missed
    if _exists(s3, bucket, key):
        return False
    if service_date != today:
        raise MissedDayError(f"{service_date} has passed (today is {today}) and no feed was captured for it")

    with tempfile.TemporaryDirectory() as tmp:
        zip_path = Path(tmp) / "gtfs.zip"
        (download or download_feed)(zip_path)
        validate_feed(zip_path, service_date)
        with open(zip_path, "rb") as f:
            return _put_once(s3, bucket, key, f)


def record_missed(s3, bucket: str, feed_date: date) -> bool:
    """Mark a day as missed, so the gap is never read as "no buses ran"."""
    body = json.dumps({"feed_date": feed_date.isoformat(), "reason": "no valid feed was captured on this day"})
    return _put_once(s3, bucket, missed_key(feed_date), body.encode("utf-8"))


def download_feed(dest: Path, url: str = GTFS_URL) -> None:
    with requests.get(url, stream=True, timeout=(30, 600)) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)


def validate_feed(zip_path: Path, service_date: date) -> None:
    """Raise InvalidFeedError unless zip_path is an intact, complete feed that starts on service_date."""
    # The server answers some clients with an HTML page and status 200
    if not zipfile.is_zipfile(zip_path):
        raise InvalidFeedError("the download is not a zip file")

    with zipfile.ZipFile(zip_path) as zf:
        corrupt = zf.testzip()
        if corrupt:
            raise InvalidFeedError(f"{corrupt} failed its CRC check")

        missing = REQUIRED_FILES - set(zf.namelist())
        if missing:
            raise InvalidFeedError(f"missing files: {sorted(missing)}")

        first_date = _first_calendar_date(zf)
        if first_date != service_date:
            raise InvalidFeedError(f"feed starts on {first_date}, not {service_date}")


def _first_calendar_date(zf: zipfile.ZipFile) -> date | None:
    with zf.open("calendar.txt") as f:
        # utf-8-sig strips the BOM the MOT files start with
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig", newline=""))
        try:
            first = min((row["start_date"] for row in reader), default=None)
        except KeyError as e:
            raise InvalidFeedError("calendar.txt has no start_date column") from e
    if first is None:
        return None
    try:
        return date(int(first[:4]), int(first[4:6]), int(first[6:8]))
    except ValueError as e:
        raise InvalidFeedError(f"calendar.txt has a bad start_date: {first!r}") from e


def _exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise
    return True


def _put_once(s3, bucket: str, key: str, body) -> bool:
    """Write an object only if the key is free. Returns False if something is already there."""
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*")
    except ClientError as e:
        if e.response["Error"]["Code"] == "PreconditionFailed":
            return False
        raise
    return True
