"""Load a prepared SQLite snapshot into the target database.

    python -m pipeline.seed [path/to/snapshot.db.gz]

Why this exists: parsing is the expensive half of a first load. Extracting
133k line items from 221 PDFs and spreadsheets takes ~26 minutes on a laptop
and several hours on a shared two-core CI runner, which is long enough to lose
races with job timeouts. But that work is deterministic -- the same documents
produce the same rows -- so re-deriving it in the cloud buys nothing except a
way to fail. Seeding ships the finished rows instead and lets the daily
incremental take over, which only ever parses the handful of item lists that
actually changed.

This is a *replace*, not a merge. The snapshot is treated as the whole truth:
every table is emptied first, so a half-finished crawl cannot leave orphan rows
behind to be mistaken for real data. It is therefore safe to re-run, and unsafe
to run while a crawl is writing -- the caller is responsible for that, and the
workflow's concurrency group enforces it.
"""
from __future__ import annotations

import gzip
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

from . import config as C
from . import db
from .run import _setup_logging, refresh_rollups

import logging

log = logging.getLogger("nupco.seed")

DEFAULT_SNAPSHOT = C.ROOT / "seed" / "nupco-seed.db.gz"

# Parents before children. tender_items and tender_history reference tenders;
# parse_failures references attachments. Emptying runs in reverse.
TABLES = ["run_log", "tenders", "attachments", "tender_items",
          "tender_history", "parse_failures"]

# Columns the snapshot carries but the destination must not inherit.
# local_path points at a filesystem that does not exist here: in cloud mode
# nothing is archived, and a path that resolves to nothing is worse than a
# NULL, because the API would offer a document link that 404s instead of
# falling back to nupco.com.
NULL_COLUMNS = {"attachments": ["local_path"], "parse_failures": ["local_path"]}

BATCH = 2000


def open_snapshot(path: Path) -> tuple[sqlite3.Connection, Path | None]:
    """Return a read-only connection to the snapshot, decompressing if needed."""
    if not path.exists():
        raise FileNotFoundError(f"snapshot not found: {path}")
    tmp = None
    if path.suffix == ".gz":
        tmp = Path(tempfile.mkdtemp()) / "snapshot.db"
        with gzip.open(path, "rb") as src, open(tmp, "wb") as dst:
            shutil.copyfileobj(src, dst, length=1 << 20)
        path = tmp
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn, tmp


def _columns(conn, table: str) -> list[str]:
    """Columns the DESTINATION has, so a snapshot from an older or newer schema
    still loads: extra columns are dropped, missing ones take their defaults."""
    if conn.is_pg:
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name=? AND is_generated='NEVER' ORDER BY ordinal_position",
            (table,)).fetchall()
        return [r["column_name"] for r in rows]
    return [r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def clear(conn) -> None:
    if conn.is_pg:
        # One statement, so the FK graph never has to be torn down in order.
        conn.execute("TRUNCATE " + ", ".join(TABLES) + " RESTART IDENTITY CASCADE")
    else:
        for t in reversed(TABLES):
            conn.execute(f"DELETE FROM {t}")
    conn.commit()


def copy_table(conn, snap: sqlite3.Connection, table: str) -> int:
    dest_cols = _columns(conn, table)
    src_cols = {r["name"] for r in snap.execute(f"PRAGMA table_info({table})")}
    cols = [c for c in dest_cols if c in src_cols]
    if not cols:
        log.warning("  %s: no columns in common, skipped", table)
        return 0
    if dropped := [c for c in dest_cols if c not in src_cols]:
        log.info("  %s: snapshot has no %s, using defaults", table, ", ".join(dropped))

    blanked = {c for c in NULL_COLUMNS.get(table, []) if c in cols}
    placeholders = ", ".join("?" for _ in cols)
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"

    total = 0
    cur = snap.execute(f"SELECT {', '.join(cols)} FROM {table}")
    while rows := cur.fetchmany(BATCH):
        payload = []
        for r in rows:
            values = [r[c] for c in cols]
            for c in blanked:
                values[cols.index(c)] = None
            payload.append(tuple(values))
        conn.executemany(sql, payload)
        total += len(payload)
        conn.commit()
        if total % (BATCH * 10) == 0:
            log.info("  %s: %d rows", table, total)
    return total


def resync_sequences(conn) -> None:
    """Postgres SERIAL columns keep their own counter, and it does not move when
    rows arrive with explicit ids. Left alone, the very next insert collides on
    the primary key -- so the first thing tomorrow's crawl does would be to
    crash. Fast-forward every sequence past the highest id we just loaded.
    """
    if not conn.is_pg:
        return
    for table in TABLES:
        # Only ask about columns the table actually has: pg_get_serial_sequence
        # raises UndefinedColumn for a missing one rather than returning NULL.
        present = set(_columns(conn, table))
        for col in ("id", "run_id", "attachment_id"):
            if col not in present:
                continue
            seq = conn.execute(
                "SELECT pg_get_serial_sequence(?, ?) AS s", (table, col)).fetchone()
            if not seq or not seq["s"]:
                continue
            conn.execute(
                f"SELECT setval(?, COALESCE((SELECT MAX({col}) FROM {table}), 0) + 1, false)",
                (seq["s"],))
            log.info("  sequence %s reset past MAX(%s.%s)", seq["s"], table, col)
    conn.commit()


def seed(snapshot: Path | None = None) -> dict:
    snapshot = Path(snapshot) if snapshot else DEFAULT_SNAPSHOT
    started = time.time()
    conn = db.connect()
    db.init(conn)

    snap, tmp = open_snapshot(snapshot)
    try:
        engine = "postgres" if conn.is_pg else "sqlite"
        log.info("seeding %s from %s (%.1f MB)", engine, snapshot,
                 snapshot.stat().st_size / 1048576)

        clear(conn)

        counts: dict[str, int] = {}
        for table in TABLES:
            counts[table] = copy_table(conn, snap, table)
            log.info("%-16s %8d rows", table, counts[table])

        resync_sequences(conn)
        refresh_rollups(conn)
        db.rebuild_fts(conn)

        # Recorded only now, after resync_sequences: run_log arrives from the
        # snapshot with explicit run_ids, so until the sequence has been moved
        # past them, allocating a new one collides on the primary key.
        run_id = db.start_run(conn, "seed")
        db.finish_run(conn, run_id, "ok",
                      tenders_seen=counts.get("tenders", 0),
                      tenders_new=counts.get("tenders", 0),
                      items_parsed=counts.get("tender_items", 0),
                      notes=f"seeded from {snapshot.name}")
        conn.commit()

        log.info("=== seed complete in %.0fs: %s ===", time.time() - started, counts)
        return counts
    finally:
        snap.close()
        conn.close()
        if tmp:
            shutil.rmtree(tmp.parent, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    _setup_logging()
    argv = argv if argv is not None else sys.argv[1:]
    path = Path(argv[0]) if argv else None
    try:
        seed(path)
    except FileNotFoundError as e:
        log.error("%s", e)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
