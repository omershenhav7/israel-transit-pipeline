import io
import json
import zipfile
from datetime import date

import botocore.session
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from transit import landing

BUCKET = "test-bucket"
DAY = date(2026, 11, 3)
KEY = landing.zip_key(DAY)


class FakeS3:
    """The two S3 calls landing uses, with S3's conditional write behaviour."""

    def __init__(self):
        self.objects = {}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")

    def put_object(self, Bucket, Key, Body, IfNoneMatch):
        assert IfNoneMatch == "*"
        if Key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.read()


def make_feed(path, start_dates=("20261103", "20261110"), files=None, bom=True):
    calendar = "start_date,end_date\n" + "".join(f"{d},20261203\n" for d in start_dates)
    contents = {name: "x\n" for name in landing.REQUIRED_FILES} | {"calendar.txt": calendar}
    if bom:
        contents["calendar.txt"] = "﻿" + calendar
    if files is not None:
        contents = {name: contents[name] for name in files}
    with zipfile.ZipFile(path, "w") as zf:
        for name, text in contents.items():
            zf.writestr(name, text)


def downloads(**feed_kwargs):
    def download(dest):
        make_feed(dest, **feed_kwargs)

    return download


def must_not_download(dest):
    pytest.fail("should not download")


@pytest.fixture
def s3():
    return FakeS3()


def test_stores_a_valid_feed(s3):
    assert landing.capture_feed(s3, BUCKET, DAY, DAY, downloads()) is True
    assert zipfile.is_zipfile(io.BytesIO(s3.objects[KEY]))


def test_second_capture_stores_nothing(s3):
    landing.capture_feed(s3, BUCKET, DAY, DAY, downloads())
    original = s3.objects[KEY]
    assert landing.capture_feed(s3, BUCKET, DAY, DAY, must_not_download) is False
    assert s3.objects[KEY] == original


def test_write_race_does_not_replace_the_zip(s3):
    def download_while_another_run_stores(dest):
        make_feed(dest)
        s3.objects[KEY] = b"stored by the other run"

    assert landing.capture_feed(s3, BUCKET, DAY, DAY, download_while_another_run_stores) is False
    assert s3.objects[KEY] == b"stored by the other run"


def test_past_day_without_zip_is_missed_without_downloading(s3):
    with pytest.raises(landing.MissedDayError):
        landing.capture_feed(s3, BUCKET, DAY, date(2026, 11, 4), must_not_download)
    assert s3.objects == {}


def test_past_day_with_zip_does_nothing(s3):
    landing.capture_feed(s3, BUCKET, DAY, DAY, downloads())
    assert landing.capture_feed(s3, BUCKET, DAY, date(2026, 11, 9), must_not_download) is False
    assert landing.missed_key(DAY) not in s3.objects


@pytest.mark.parametrize(
    "download, message",
    [
        (lambda dest: dest.write_text("<html>blocked</html>"), "not a zip"),
        (downloads(files=landing.REQUIRED_FILES - {"stop_times.txt"}), "stop_times.txt"),
        (downloads(start_dates=("20261102",)), "feed starts on 2026-11-02"),
        (downloads(start_dates=()), "feed starts on None"),
    ],
    ids=["html_page", "missing_file", "stale_feed", "empty_calendar"],
)
def test_bad_downloads_are_rejected_and_not_stored(s3, download, message):
    with pytest.raises(landing.InvalidFeedError, match=message):
        landing.capture_feed(s3, BUCKET, DAY, DAY, download)
    assert s3.objects == {}


def test_corrupt_zip_is_rejected(tmp_path):
    path = tmp_path / "gtfs.zip"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_STORED) as zf:
        for name in landing.REQUIRED_FILES - {"calendar.txt"}:
            zf.writestr(name, "x\n")
        zf.writestr("calendar.txt", "start_date,end_date\n20261103,20261203\n")
    data = path.read_bytes()
    path.write_bytes(data.replace(b"20261103,20261203", b"20261103,20261204"))
    with pytest.raises(landing.InvalidFeedError, match="CRC"):
        landing.validate_feed(path, DAY)


def test_earliest_start_date_decides_regardless_of_row_order(tmp_path):
    path = tmp_path / "gtfs.zip"
    make_feed(path, start_dates=("20261110", "20261103"))
    landing.validate_feed(path, DAY)


def test_record_missed_writes_a_marker_once(s3):
    assert landing.record_missed(s3, BUCKET, DAY) is True
    assert json.loads(s3.objects[landing.missed_key(DAY)])["feed_date"] == "2026-11-03"
    assert landing.record_missed(s3, BUCKET, DAY) is False


def test_other_s3_errors_are_not_treated_as_missing():
    class Forbidden(FakeS3):
        def head_object(self, Bucket, Key):
            raise ClientError({"Error": {"Code": "403"}}, "HeadObject")

    with pytest.raises(ClientError):
        landing.capture_feed(Forbidden(), BUCKET, DAY, DAY, must_not_download)


def test_real_s3_client_accepts_the_conditional_write():
    client = botocore.session.get_session().create_client(
        "s3", region_name="il-central-1", aws_access_key_id="test", aws_secret_access_key="test"
    )
    with Stubber(client) as stub:
        stub.add_response(
            "put_object", {}, {"Bucket": BUCKET, "Key": KEY, "Body": b"zip", "IfNoneMatch": "*"}
        )
        assert landing._put_once(client, BUCKET, KEY, b"zip") is True
        stub.assert_no_pending_responses()
