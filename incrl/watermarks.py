"""Persistent high-watermark store: one row per pipeline."""

from __future__ import annotations

from datetime import datetime, timezone

from .db import Database


class WatermarkStore:
    TABLE = "pipeline_watermarks"

    def __init__(self, db: Database):
        self.db = db
        db.execute(
            f"CREATE TABLE IF NOT EXISTS {self.TABLE} ("
            "pipeline TEXT PRIMARY KEY, "
            "watermark TEXT NOT NULL, "
            "updated_at TEXT NOT NULL)"
        )
        db.commit()

    def get(self, pipeline: str) -> str | None:
        cur = self.db.execute(
            f"SELECT watermark FROM {self.TABLE} WHERE pipeline = {self.db.ph}", (pipeline,)
        )
        row = cur.fetchone()
        return row[0] if row else None

    def set(self, pipeline: str, watermark: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.db.execute(
            f"INSERT INTO {self.TABLE} (pipeline, watermark, updated_at) "
            f"VALUES ({self.db.ph}, {self.db.ph}, {self.db.ph}) "
            f"ON CONFLICT (pipeline) DO UPDATE SET "
            f"watermark = EXCLUDED.watermark, updated_at = EXCLUDED.updated_at",
            (pipeline, watermark, now),
        )
        self.db.commit()
