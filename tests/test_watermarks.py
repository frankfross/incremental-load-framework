"""Tests for the watermark store."""

import pytest

from incrl.db import connect
from incrl.watermarks import WatermarkStore


@pytest.fixture
def store(tmp_path):
    db = connect(sqlite_path=str(tmp_path / "state.db"))
    yield WatermarkStore(db)
    db.close()


def test_get_missing_returns_none(store):
    assert store.get("nope") is None


def test_set_get_roundtrip(store):
    store.set("orders", "2026-10-05T18:31:00")
    assert store.get("orders") == "2026-10-05T18:31:00"


def test_set_overwrites(store):
    store.set("orders", "2026-10-05T18:31:00")
    store.set("orders", "2026-10-06T09:12:00")
    assert store.get("orders") == "2026-10-06T09:12:00"


def test_pipelines_are_independent(store):
    store.set("orders", "2026-10-05T18:31:00")
    store.set("customers", "999")
    assert store.get("orders") == "2026-10-05T18:31:00"
    assert store.get("customers") == "999"
