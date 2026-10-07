"""CLI: run one or all pipelines.

    python3 -m incrl.run --config config.yaml --pipeline orders
    python3 -m incrl.run --config config.yaml --all

Postgres instead of SQLite: set DATABASE_URL (needs `pip install psycopg`).
"""

from __future__ import annotations

import argparse
import json
import sys

import yaml

from .db import connect
from .load import IncrementalLoader
from .sources import CsvSource
from .watermarks import WatermarkStore


def build_loader(db, store, name: str, cfg: dict) -> IncrementalLoader:
    src_cfg = cfg["source"]
    if src_cfg["type"] == "csv":
        source = CsvSource(src_cfg["path"], src_cfg["watermark_column"])
    else:
        raise ValueError(f"unknown source type: {src_cfg['type']!r}")
    return IncrementalLoader(
        db=db,
        store=store,
        source=source,
        pipeline=name,
        destination_table=cfg.get("destination_table", name),
        natural_key=cfg["natural_key"],
        initial_watermark=cfg.get("initial_watermark", "1970-01-01T00:00:00"),
        lookback=cfg.get("lookback", "0"),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run incremental load pipelines.")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--pipeline", help="pipeline name from the config")
    parser.add_argument("--all", action="store_true", help="run every pipeline")
    args = parser.parse_args(argv)

    with open(args.config, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    pipelines = cfg.get("pipelines", {})
    if args.all:
        names = list(pipelines)
    elif args.pipeline:
        names = [args.pipeline]
    else:
        parser.error("pass --pipeline NAME or --all")
    unknown = [n for n in names if n not in pipelines]
    if unknown:
        parser.error(f"unknown pipeline(s): {', '.join(unknown)}")

    db_cfg = cfg.get("database", {})
    db = connect(database_url="", sqlite_path=db_cfg.get("sqlite_path", "state/incremental.db"))
    store = WatermarkStore(db)

    reports = []
    try:
        for name in names:
            loader = build_loader(db, store, name, pipelines[name])
            reports.append(loader.run().as_dict())
    finally:
        db.close()

    print(json.dumps(reports if args.all else reports[0], indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
