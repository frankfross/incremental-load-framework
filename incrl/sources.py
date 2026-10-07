"""Data sources: anything that can yield rows newer than a watermark.

A Source is deliberately small: `fetch_since(bound)` yields dict rows whose
watermark value is strictly greater than `bound`. Filtering is the source's
job; the loader never sees old rows. Watermark values compare with
`watermark_key`, which handles ints, floats, ISO-8601 timestamps, and falls
back to plain string order.
"""

from __future__ import annotations

import csv
import os
from datetime import datetime
from typing import Any, Iterable, Mapping, Protocol


def _coerce(value: Any) -> tuple[int, Any]:
    """Return a sortable key for a watermark value.

    Ranks: 0=int, 1=float, 2=datetime, 3=str. Mixed-type watermarks are a
    config error, but ranking keeps comparisons total instead of crashing.
    """
    if value is None:
        return (3, "")
    text = str(value).strip()
    try:
        return (0, int(text))
    except ValueError:
        pass
    try:
        return (1, float(text))
    except ValueError:
        pass
    try:
        return (2, datetime.fromisoformat(text))
    except ValueError:
        pass
    return (3, text)


def watermark_key(value: Any) -> tuple[int, Any]:
    return _coerce(value)


class Source(Protocol):
    watermark_column: str

    def fetch_since(self, bound: Any) -> Iterable[Mapping[str, Any]]: ...


class CsvSource:
    """Read rows from a CSV file (or every CSV in a directory)."""

    def __init__(self, path: str, watermark_column: str):
        self.path = path
        self.watermark_column = watermark_column

    def _files(self) -> list[str]:
        if os.path.isdir(self.path):
            return sorted(
                os.path.join(self.path, f)
                for f in os.listdir(self.path)
                if f.endswith(".csv")
            )
        return [self.path]

    def fetch_since(self, bound: Any) -> Iterable[Mapping[str, Any]]:
        bound_key = watermark_key(bound)
        for file in self._files():
            if not os.path.exists(file):
                continue
            with open(file, newline="", encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    value = row.get(self.watermark_column)
                    if value is None or value == "":
                        continue  # rows without a watermark can't be ordered; skip, don't guess
                    if watermark_key(value) > bound_key:
                        yield dict(row)


class MemorySource:
    """In-memory append-only log. Useful for demos and tests: append events,
    then run the loader to see only the new ones get picked up."""

    def __init__(self, watermark_column: str, rows: Iterable[Mapping[str, Any]] = ()):
        self.watermark_column = watermark_column
        self._rows: list[dict[str, Any]] = [dict(r) for r in rows]

    def append(self, **row: Any) -> None:
        self._rows.append(dict(row))

    def fetch_since(self, bound: Any) -> Iterable[Mapping[str, Any]]:
        bound_key = watermark_key(bound)
        for row in self._rows:
            value = row.get(self.watermark_column)
            if value is None or value == "":
                continue
            if watermark_key(value) > bound_key:
                yield dict(row)
