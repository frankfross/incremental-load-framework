"""End-to-end tests for watermark-based incremental loads."""

import pytest

from incrl.db import connect
from incrl.load import IncrementalLoader, parse_lookback, apply_lookback
from incrl.sources import CsvSource, MemorySource, watermark_key
from incrl.watermarks import WatermarkStore


def make_loader(tmp_path, source, lookback="0", initial="1970-01-01T00:00:00"):
    db = connect(sqlite_path=str(tmp_path / "state.db"))
    store = WatermarkStore(db)
    loader = IncrementalLoader(
        db=db,
        store=store,
        source=source,
        pipeline="orders",
        destination_table="orders",
        natural_key="order_id",
        initial_watermark=initial,
        lookback=lookback,
    )
    return db, store, loader


def rows(*recs):
    return list(recs)


def test_first_run_loads_everything_and_sets_watermark(tmp_path):
    src = MemorySource("updated_at", rows(
        {"order_id": "1", "updated_at": "2026-10-05T08:00:00"},
        {"order_id": "2", "updated_at": "2026-10-05T09:00:00"},
    ))
    db, store, loader = make_loader(tmp_path, src)
    report = loader.run().as_dict()

    assert report["rows_seen"] == 2
    assert report["rows_inserted"] == 2
    assert report["rows_updated"] == 0
    assert report["watermark_before"] is None
    assert report["watermark_after"] == "2026-10-05T09:00:00"
    assert store.get("orders") == "2026-10-05T09:00:00"
    db.close()


def test_second_run_with_no_new_rows_loads_nothing(tmp_path):
    src = MemorySource("updated_at", rows(
        {"order_id": "1", "updated_at": "2026-10-05T08:00:00"},
    ))
    db, store, loader = make_loader(tmp_path, src)
    loader.run()
    report = loader.run().as_dict()

    assert report["rows_seen"] == 0
    assert report["rows_inserted"] == 0
    assert report["watermark_after"] == report["watermark_before"]
    db.close()


def test_only_new_rows_are_fetched(tmp_path):
    src = MemorySource("updated_at", rows(
        {"order_id": "1", "updated_at": "2026-10-05T08:00:00"},
    ))
    db, store, loader = make_loader(tmp_path, src)
    loader.run()

    src.append(order_id="2", updated_at="2026-10-06T08:00:00")
    src.append(order_id="3", updated_at="2026-10-06T09:00:00")
    report = loader.run().as_dict()

    assert report["rows_seen"] == 2
    assert report["rows_inserted"] == 2
    assert report["rows_updated"] == 0
    assert report["watermark_after"] == "2026-10-06T09:00:00"
    db.close()


def test_late_arriving_row_within_lookback_is_picked_up(tmp_path):
    src = MemorySource("updated_at", rows(
        {"order_id": "1", "amount": "10", "updated_at": "2026-10-05T08:00:00"},
        {"order_id": "2", "amount": "20", "updated_at": "2026-10-05T18:00:00"},
    ))
    db, store, loader = make_loader(tmp_path, src, lookback="24h")
    loader.run()

    # A correction for order 1 arrives late, timestamped before the watermark.
    src.append(order_id="1", amount="12", updated_at="2026-10-05T07:30:00")
    report = loader.run().as_dict()

    assert report["rows_seen"] == 3          # 24h lookback re-fetches day-1 rows
    assert report["rows_inserted"] == 0      # nothing new...
    assert report["rows_updated"] == 3       # ...but the late correction lands
    cur = db.execute('SELECT amount FROM orders WHERE order_id = ?', ("1",))
    assert cur.fetchone()[0] == "12"
    db.close()


def test_rerun_is_idempotent(tmp_path):
    src = MemorySource("updated_at", rows(
        {"order_id": "1", "updated_at": "2026-10-05T08:00:00"},
    ))
    db, store, loader = make_loader(tmp_path, src, lookback="24h")
    first = loader.run().as_dict()
    second = loader.run().as_dict()

    assert first["rows_inserted"] == 1
    assert second["rows_inserted"] == 0
    assert second["rows_seen"] == 1  # lookback overlap, all upserts
    cur = db.execute("SELECT COUNT(*) FROM orders")
    assert cur.fetchone()[0] == 1  # no duplicates
    db.close()


def test_rows_without_natural_key_are_skipped_and_counted(tmp_path):
    src = MemorySource("updated_at", rows(
        {"order_id": "", "updated_at": "2026-10-05T08:00:00"},
        {"order_id": "1", "updated_at": "2026-10-05T09:00:00"},
    ))
    db, store, loader = make_loader(tmp_path, src)
    report = loader.run().as_dict()

    assert report["rows_skipped_no_key"] == 1
    assert report["rows_inserted"] == 1
    cur = db.execute("SELECT COUNT(*) FROM orders")
    assert cur.fetchone()[0] == 1
    db.close()


def test_numeric_id_watermark_with_unit_lookback(tmp_path):
    src = MemorySource("id", rows(
        {"order_id": "a", "id": 100},
        {"order_id": "b", "id": 200},
    ))
    db, store, loader = make_loader(tmp_path, src, lookback="50", initial=0)
    loader.run()
    src.append(order_id="c", id=250)
    report = loader.run().as_dict()

    # bound = 200 - 50 = 150: rows with id > 150 -> b (re-upsert) and c (new)
    assert report["rows_inserted"] == 1
    assert report["rows_updated"] == 1
    db.close()


def test_csv_source_filters_by_bound(tmp_path):
    csv_file = tmp_path / "events.csv"
    csv_file.write_text(
        "order_id,updated_at\n"
        "1,2026-10-05T08:00:00\n"
        "2,2026-10-05T09:00:00\n"
        "3,2026-10-06T09:00:00\n"
    )
    src = CsvSource(str(csv_file), "updated_at")
    fetched = list(src.fetch_since("2026-10-05T09:00:00"))
    assert [r["order_id"] for r in fetched] == ["3"]


def test_csv_source_missing_file_yields_nothing(tmp_path):
    src = CsvSource(str(tmp_path / "missing.csv"), "updated_at")
    assert list(src.fetch_since("1970-01-01T00:00:00")) == []


def test_parse_lookback():
    from datetime import timedelta

    assert parse_lookback("24h") == timedelta(hours=24)
    assert parse_lookback("30m") == timedelta(minutes=30)
    assert parse_lookback("60s") == timedelta(seconds=60)
    assert parse_lookback("500") == 500
    assert parse_lookback("0") == 0
    with pytest.raises(ValueError):
        parse_lookback("yesterday")


def test_apply_lookback_datetime_and_numeric():
    from datetime import timedelta

    assert apply_lookback("2026-10-05T18:00:00", timedelta(hours=24)) == "2026-10-04T18:00:00"
    assert apply_lookback(200, 50) == 150
    # lookback that doesn't match the watermark type is a no-op, not an error
    assert apply_lookback("2026-10-05T18:00:00", 50) == "2026-10-05T18:00:00"


def test_watermark_key_ordering():
    assert watermark_key(5) < watermark_key(10)
    assert watermark_key("2026-10-05T08:00:00") < watermark_key("2026-10-06T08:00:00")
