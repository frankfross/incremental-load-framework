"""incremental-load-framework: watermark-based incremental loads.

Instead of re-reading an entire source on every run, each pipeline remembers
its high-watermark (max `updated_at` / max `id` seen so far) and only fetches
newer rows. A configurable lookback window re-fetches a slice of overlap so
late-arriving rows are still picked up, and upserts on the natural key make
every run safe to retry.
"""

from .db import connect, upsert_rows, Database
from .watermarks import WatermarkStore
from .sources import CsvSource, MemorySource, Source
from .load import IncrementalLoader, LoadReport

__all__ = [
    "connect",
    "upsert_rows",
    "Database",
    "WatermarkStore",
    "CsvSource",
    "MemorySource",
    "Source",
    "IncrementalLoader",
    "LoadReport",
]
