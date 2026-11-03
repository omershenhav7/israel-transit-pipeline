"""Download the Israel MOT GTFS feed, load it into the warehouse, and snapshot the daily schedule."""
import csv
import io
import zipfile
from datetime import date
from pathlib import Path

import requests

GTFS_URL = "https://gtfs.mot.gov.il/gtfsfiles/israel-public-transportation.zip"
RAW_SCHEMA = "raw_gtfs"
SQL_DIR = Path(__file__).parent / "sql"


def download_feed(dest_dir: Path, url: str = GTFS_URL) -> Path:
    """Stream the feed zip into dest_dir and return its path."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    zip_path = dest_dir / "gtfs.zip"
    part_path = zip_path.with_suffix(".part")

    with requests.get(url, stream=True, timeout=(30, 600)) as resp:
        resp.raise_for_status()
        with open(part_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)

    # The server answers some clients with an HTML page and status 200, so check we really got a zip
    if not zipfile.is_zipfile(part_path):
        part_path.unlink()
        raise ValueError(f"{url} did not return a zip file (content-type: {resp.headers.get('content-type')})")

    part_path.replace(zip_path)
    return zip_path


def extract_feed(zip_path: Path) -> Path:
    feed_dir = zip_path.parent / "gtfs"
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(feed_dir)
    return feed_dir


def load_raw(conn, feed_dir: Path) -> dict[str, int]:
    """Replace every raw_gtfs.<file> table with the contents of feed_dir, all columns as text.

    Runs in a single transaction, so readers never see a half-loaded feed.
    """
    counts = {}
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
        for path in sorted(feed_dir.glob("*.txt")):
            table = f"{RAW_SCHEMA}.{path.stem}"
            # utf-8-sig strips the BOM the MOT files start with
            with open(path, encoding="utf-8-sig", newline="") as f:
                reader = csv.reader(f)
                columns_sql = ", ".join(f'"{c.strip()}" text' for c in next(reader))
                cur.execute(f"DROP TABLE IF EXISTS {table}")
                cur.execute(f"CREATE TABLE {table} ({columns_sql})")
                with cur.copy(f"COPY {table} FROM STDIN WITH (FORMAT csv)") as copy:
                    _copy_rows(reader, copy)
            counts[path.stem] = cur.rowcount
        cur.execute(f"CREATE INDEX ON {RAW_SCHEMA}.stop_times (trip_id)")
    conn.commit()
    return counts


def _copy_rows(reader, copy, batch_size: int = 50_000) -> None:
    """Re-serialize parsed rows into a COPY.

    The MOT files contain bare quotes inside unquoted fields (e.g. קק"ל), which Python's csv
    reads as literal characters but Postgres' CSV parser treats as the start of a quoted value.
    """
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    for i, row in enumerate(reader, start=1):
        writer.writerow(row)
        if i % batch_size == 0:
            copy.write(buffer.getvalue())
            buffer.seek(0)
            buffer.truncate()
    copy.write(buffer.getvalue())


def build_planned_trips(conn, service_date: date) -> int:
    """(Re)build schedule.planned_trips for one service date from the raw tables. Returns rows written."""
    sql = (SQL_DIR / "build_planned_trips.sql").read_text(encoding="utf-8")
    # psycopg 3 binds parameters server-side, which allows one statement per execute
    statements = [s for s in sql.split(";") if s.strip()]
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement, {"service_date": service_date} if "%(service_date)s" in statement else None)
        n_trips = cur.rowcount  # rowcount of the final INSERT
    if n_trips == 0:
        conn.rollback()
        raise ValueError(f"No trips scheduled on {service_date}; is the loaded feed current?")
    conn.commit()
    return n_trips
