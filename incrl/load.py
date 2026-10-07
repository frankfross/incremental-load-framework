"""The incremental loader: watermark -> fetch -> upsert -> advance."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Mapping

from .db import Database, upsert_rows
from .sources import Source, watermark_key
from .watermarks import WatermarkStore


@dataclass
class LoadReport:
    pipeline: str
    watermark_before: str | None
    watermark_after: str | None
    rows_seen: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_skipped_no_key: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "watermark_before": self.watermark_before,
            "watermark_after": self.watermark_after,
            "rows_seen": self.rows_seen,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_skipped_no_key": self.rows_skipped_no_key,
        }


_LOOKBACK_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([smh]?)\s*$", re.IGNORECASE)


def parse_lookback(spec: str) -> timedelta | float | int:
    """Parse a lookback like '24h', '30m', '60s' (durations) or '500' (units).

    Durations apply to datetime watermarks; plain numbers apply to numeric
    (int/float) watermarks.
    """
    m = _LOOKBACK_RE.match(spec or "")
    if not m:
        raise ValueError(f"bad lookback spec: {spec!r} (try '24h' or '500')")
    amount = float(m.group(1))
    unit = m.group(2).lower()
    if unit == "h":
        return timedelta(hours=amount)
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "s":
        return timedelta(seconds=amount)
    return int(amount) if amount.is_integer() else amount


def apply_lookback(watermark: Any, lookback: timedelta | float | int) -> Any:
    """Move a watermark back by the lookback so late rows are re-fetched."""
    key = watermark_key(watermark)
    if isinstance(lookback, timedelta) and key[0] == 2:
        return (key[1] - lookback).isoformat()
    if isinstance(lookback, (int, float)) and key[0] in (0, 1):
        return key[1] - lookback
    return watermark  # lookback doesn't apply to this watermark type; no overlap


class IncrementalLoader:
    def __init__(
        self,
        db: Database,
        store: WatermarkStore,
        source: Source,
        pipeline: str,
        destination_table: str,
        natural_key: str,
        initial_watermark: Any = "1970-01-01T00:00:00",
        lookback: str = "0",
    ):
        self.db = db
        self.store = store
        self.source = source
        self.pipeline = pipeline
        self.destination_table = destination_table
        self.natural_key = natural_key
        self.initial_watermark = initial_watermark
        self.lookback = parse_lookback(lookback)

    def run(self) -> LoadReport:
        before = self.store.get(self.pipeline)
        watermark = before if before is not None else self.initial_watermark
        bound = apply_lookback(watermark, self.lookback)

        rows: list[Mapping[str, Any]] = []
        skipped = 0
        for row in self.source.fetch_since(bound):
            if not row.get(self.natural_key):
                skipped += 1  # counted, never inserted with a NULL key
                continue
            rows.append(row)

        report = LoadReport(
            pipeline=self.pipeline,
            watermark_before=before,
            rows_seen=len(rows),
            rows_skipped_no_key=skipped,
            watermark_after=before,
        )
        if rows:
            inserted, updated = upsert_rows(self.db, self.destination_table, rows, self.natural_key)
            report.rows_inserted = inserted
            report.rows_updated = updated
            # Advance to the max watermark actually observed, not the bound.
            newest = max(rows, key=lambda r: watermark_key(r[self.source.watermark_column]))
            new_watermark = str(newest[self.source.watermark_column])
            self.store.set(self.pipeline, new_watermark)
            report.watermark_after = new_watermark
        return report
