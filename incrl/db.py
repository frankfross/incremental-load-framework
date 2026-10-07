"""Database connection factory and upsert helper.

SQLite is the default (a single file holds the watermark state table and the
destination tables). Set DATABASE_URL to a Postgres DSN to use Postgres
instead — the SQL used here (ON CONFLICT upserts) works on both.
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence


@dataclass
class Database:
    conn: Any
    flavor: str  # "sqlite" | "postgres"

    @property
    def ph(self) -> str:
        return "?" if self.flavor == "sqlite" else "%s"

    def execute(self, sql: str, params: Sequence[Any] = ()) -> Any:
        cur = self.conn.cursor()
        cur.execute(sql, params)
        return cur

    def executemany(self, sql: str, params: Iterable[Sequence[Any]]) -> Any:
        cur = self.conn.cursor()
        cur.executemany(sql, params)
        return cur

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def connect(database_url: str = "", sqlite_path: str = "state/incremental.db") -> Database:
    """Open a Database. Postgres when DATABASE_URL (or database_url) is set."""
    url = database_url or os.environ.get("DATABASE_URL", "")
    if url:
        import psycopg  # type: ignore

        return Database(psycopg.connect(url), "postgres")
    os.makedirs(os.path.dirname(sqlite_path) or ".", exist_ok=True)
    conn = sqlite3.connect(sqlite_path)
    return Database(conn, "sqlite")


def _quote(ident: str) -> str:
    if not ident.replace("_", "").isalnum():
        raise ValueError(f"unsafe identifier: {ident!r}")
    return f'"{ident}"'


def ensure_table(db: Database, table: str, columns: Sequence[str], key: str) -> None:
    cols = ", ".join(f"{_quote(c)} TEXT" for c in columns)
    db.execute(
        f"CREATE TABLE IF NOT EXISTS {_quote(table)} ({cols}, PRIMARY KEY ({_quote(key)}))"
    )
    db.commit()


def upsert_rows(
    db: Database, table: str, rows: Sequence[Mapping[str, Any]], key: str
) -> tuple[int, int]:
    """Upsert rows keyed on `key`. Returns (inserted, updated).

    Rows missing the natural key are skipped by the caller, never inserted
    with a NULL key.
    """
    if not rows:
        return 0, 0
    columns = list(rows[0].keys())
    ensure_table(db, table, columns, key)

    keys = [str(r[key]) for r in rows]
    placeholders = ", ".join([db.ph] * len(keys))
    existing = {
        r[0]
        for r in db.execute(
            f"SELECT {_quote(key)} FROM {_quote(table)} WHERE {_quote(key)} IN ({placeholders})",
            keys,
        ).fetchall()
    }

    col_list = ", ".join(_quote(c) for c in columns)
    vals = ", ".join([db.ph] * len(columns))
    updates = ", ".join(f"{_quote(c)} = EXCLUDED.{_quote(c)}" for c in columns if c != key)
    sql = (
        f"INSERT INTO {_quote(table)} ({col_list}) VALUES ({vals}) "
        f"ON CONFLICT ({_quote(key)}) DO UPDATE SET {updates}"
    )
    db.executemany(
        sql, [[None if v is None else str(v) for v in (r[c] for c in columns)] for r in rows]
    )
    db.commit()

    inserted = sum(1 for k in keys if k not in existing)
    return inserted, len(rows) - inserted
