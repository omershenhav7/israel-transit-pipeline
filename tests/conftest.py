import zipfile
from pathlib import Path

import pytest

from transit.bronze import TABLES


def make_zip(path: Path, files: dict[str, str]) -> Path:
    """Write a zip holding the given {name: text} files."""
    with zipfile.ZipFile(path, "w") as zf:
        for name, text in files.items():
            zf.writestr(name, text.encode("utf-8"))
    return path


def feed_text(table: str, rows: list[list[str]], bom: bool = True, crlf: bool = True) -> str:
    """A feed file's text: expected header, then rows, no quoting."""
    eol = "\r\n" if crlf else "\n"
    lines = [",".join(TABLES[table])] + [",".join(r) for r in rows]
    return ("﻿" if bom else "") + eol.join(lines) + eol


@pytest.fixture(scope="session")
def spark():
    from transit.spark import get_spark

    session = get_spark("tests")
    yield session
    session.stop()
