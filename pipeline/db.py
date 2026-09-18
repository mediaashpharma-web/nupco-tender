"""Schema and data access, for SQLite locally and Postgres in the cloud.

One codebase, two engines. Set ``DATABASE_URL`` and everything talks to
Postgres (Supabase); leave it unset and it uses the local SQLite file. The
pipeline, the API and the tests all go through this module, so nothing else
has to care which engine is underneath.

Design notes
------------
* ``post_id`` (the WordPress post id) is the primary key, not the tender id.
  Tender ids are *not* unique across the 229 live tender pages -- archived and
  duplicate pages reuse them. post_id is stable and unique.
* Nothing is ever hard-deleted. A tender that leaves the grid has almost
  certainly gone ``hidden``, not away: we flip ``is_listed`` and keep
  ``last_seen_at``.
* Every field-level change is appended to ``tender_history``. For tender
  intelligence the diff *is* the product -- overwriting destroys the signal.
* Attachments are versioned on content hash, keyed on (post_id, role) rather
  than URL, because NUPCO republishes revised files at new /uploads/ paths.

Why not an ORM: the whole point of this store is a handful of carefully
indexed queries and one full-text search per engine. An ORM would hide exactly
the part that needs to be explicit, and add a dependency to a pipeline whose
only other requirements are an HTTP client and two file parsers.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Iterable

from .config import DB_PATH

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
IS_POSTGRES = bool(DATABASE_URL)

# Serialises writes when the pipeline fans out across worker threads.
write_lock = threading.RLock()


# --- schema ------------------------------------------------------------------

_COMMON_TABLES = """
CREATE TABLE IF NOT EXISTS tenders (
    post_id             {pk_int},
    tender_id           TEXT,
    url                 TEXT,
    slug                TEXT,
    title_en            TEXT,
    title_ar            TEXT,
    status_label        TEXT,
    status_slugs        TEXT,
    opening_date        TEXT,
    submission_deadline TEXT,
    bid_opening         TEXT,
    booklet_price_sar   {real},
    buy_url             TEXT,
    tender_kind         TEXT,
    tender_seq          INTEGER,
    tender_year         INTEGER,
    opening_ts          TEXT,
    submission_ts       TEXT,
    bid_opening_ts      TEXT,
    days_to_deadline    INTEGER,
    category_guess      TEXT,
    item_count          INTEGER DEFAULT 0,
    total_qty           {real},
    attachment_count    INTEGER DEFAULT 0,
    lang                TEXT,
    sitemap_lastmod     TEXT,
    is_listed           INTEGER DEFAULT 1,
    is_terminal         INTEGER DEFAULT 0,
    detail_fetched_at   TEXT,
    discovered_via      TEXT,
    content_hash        TEXT,
    first_seen_at       TEXT,
    last_seen_at        TEXT,
    last_changed_at     TEXT
);

CREATE TABLE IF NOT EXISTS tender_history (
    id          {pk_serial},
    post_id     INTEGER,
    tender_id   TEXT,
    field       TEXT,
    old_value   TEXT,
    new_value   TEXT,
    change_kind TEXT,
    changed_at  TEXT,
    run_id      INTEGER
);

CREATE TABLE IF NOT EXISTS attachments (
    id                 {pk_serial},
    post_id            INTEGER,
    tender_id          TEXT,
    role               TEXT,
    url                TEXT,
    filename           TEXT,
    ext                TEXT,
    content_sha256     TEXT,
    size_bytes         INTEGER,
    etag               TEXT,
    http_last_modified TEXT,
    local_path         TEXT,
    version            INTEGER DEFAULT 1,
    is_current         INTEGER DEFAULT 1,
    parse_status       TEXT,
    parse_rows         INTEGER DEFAULT 0,
    first_seen_at      TEXT,
    last_seen_at       TEXT,
    last_changed_at    TEXT
);

CREATE TABLE IF NOT EXISTS tender_items (
    id             {pk_serial},
    attachment_id  INTEGER,
    post_id        INTEGER,
    tender_id      TEXT,
    sn             TEXT,
    item_no        TEXT,
    nupco_code     TEXT,
    description    TEXT,
    uom            TEXT,
    qty            {real},
    qty_raw        TEXT,
    item_group     TEXT,
    itemized       TEXT,
    category_guess TEXT,
    code_group     TEXT,
    is_accessory   INTEGER DEFAULT 0,
    source_page    INTEGER,
    row_index      INTEGER,
    raw_row        TEXT
);

CREATE TABLE IF NOT EXISTS parse_failures (
    id            {pk_serial},
    attachment_id INTEGER,
    tender_id     TEXT,
    role          TEXT,
    local_path    TEXT,
    reason        TEXT,
    occurred_at   TEXT,
    run_id        INTEGER
);

CREATE TABLE IF NOT EXISTS run_log (
    run_id          {pk_serial},
    mode            TEXT,
    started_at      TEXT,
    finished_at     TEXT,
    status          TEXT,
    tenders_seen    INTEGER DEFAULT 0,
    tenders_new     INTEGER DEFAULT 0,
    tenders_changed INTEGER DEFAULT 0,
    files_checked   INTEGER DEFAULT 0,
    files_changed   INTEGER DEFAULT 0,
    items_parsed    INTEGER DEFAULT 0,
    errors          INTEGER DEFAULT 0,
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_tenders_tid    ON tenders(tender_id);
CREATE INDEX IF NOT EXISTS idx_tenders_status ON tenders(status_label);
CREATE INDEX IF NOT EXISTS idx_tenders_sub    ON tenders(submission_ts);
CREATE INDEX IF NOT EXISTS idx_tenders_open   ON tenders(opening_ts);
CREATE INDEX IF NOT EXISTS idx_hist_post      ON tender_history(post_id);
CREATE INDEX IF NOT EXISTS idx_hist_at        ON tender_history(changed_at);
CREATE INDEX IF NOT EXISTS idx_att_post       ON attachments(post_id, role, is_current);
CREATE INDEX IF NOT EXISTS idx_att_hash       ON attachments(content_sha256);
CREATE INDEX IF NOT EXISTS idx_items_att      ON tender_items(attachment_id);
CREATE INDEX IF NOT EXISTS idx_items_code     ON tender_items(nupco_code);
CREATE INDEX IF NOT EXISTS idx_items_tid      ON tender_items(tender_id);
CREATE INDEX IF NOT EXISTS idx_items_cat      ON tender_items(category_guess);
"""

_VIEWS = """
CREATE VIEW IF NOT EXISTS v_current_items AS
SELECT i.*, t.status_label, t.submission_ts, t.opening_ts, t.days_to_deadline,
       t.title_en, t.url AS tender_url, t.is_listed,
       a.role, a.local_path, a.url AS file_url
FROM tender_items i
JOIN attachments a ON a.id = i.attachment_id AND a.is_current = 1
JOIN tenders     t ON t.post_id = i.post_id;

CREATE VIEW IF NOT EXISTS v_item_catalog AS
SELECT nupco_code,
       MIN(description)                  AS description,
       COUNT(DISTINCT tender_id)         AS tender_count,
       COUNT(*)                          AS line_count,
       SUM(COALESCE(qty, 0))             AS total_qty,
       MAX(category_guess)               AS category_guess,
       MAX(submission_ts)                AS latest_deadline,
       {agg}(DISTINCT tender_id)         AS tender_ids
FROM v_current_items
WHERE nupco_code IS NOT NULL AND nupco_code <> ''
GROUP BY nupco_code;

CREATE VIEW IF NOT EXISTS v_open_tenders AS
SELECT * FROM tenders
WHERE is_terminal = 0
  AND (days_to_deadline IS NULL OR days_to_deadline >= 0);

CREATE VIEW IF NOT EXISTS v_recent_changes AS
SELECT h.changed_at, h.change_kind, h.tender_id, h.post_id, h.field,
       h.old_value, h.new_value, t.title_en, t.status_label, t.url
FROM tender_history h
LEFT JOIN tenders t ON t.post_id = h.post_id;
"""

# SQLite full-text: FTS5 external-content tables kept in step by triggers.
_SQLITE_FTS = """
CREATE VIRTUAL TABLE IF NOT EXISTS items_fts USING fts5(
    description, nupco_code, item_no, tender_id,
    content='tender_items', content_rowid='id', tokenize='porter unicode61'
);
CREATE TRIGGER IF NOT EXISTS items_ai AFTER INSERT ON tender_items BEGIN
    INSERT INTO items_fts(rowid, description, nupco_code, item_no, tender_id)
    VALUES (new.id, new.description, new.nupco_code, new.item_no, new.tender_id);
END;
CREATE TRIGGER IF NOT EXISTS items_ad AFTER DELETE ON tender_items BEGIN
    INSERT INTO items_fts(items_fts, rowid, description, nupco_code, item_no, tender_id)
    VALUES ('delete', old.id, old.description, old.nupco_code, old.item_no, old.tender_id);
END;
CREATE VIRTUAL TABLE IF NOT EXISTS tenders_fts USING fts5(
    title_en, title_ar, tender_id,
    content='tenders', content_rowid='post_id', tokenize='porter unicode61'
);
"""

# Postgres full-text: generated tsvector columns, so they can never drift out
# of step with their rows the way a trigger-maintained index can.
_PG_FTS = """
ALTER TABLE tender_items ADD COLUMN IF NOT EXISTS search_vector tsvector
    GENERATED ALWAYS AS (to_tsvector('english',
        coalesce(description,'') || ' ' || coalesce(nupco_code,'') || ' '
        || coalesce(item_no,''))) STORED;
CREATE INDEX IF NOT EXISTS idx_items_search ON tender_items USING GIN(search_vector);

ALTER TABLE tenders ADD COLUMN IF NOT EXISTS search_vector tsvector
    GENERATED ALWAYS AS (to_tsvector('english',
        coalesce(title_en,'') || ' ' || coalesce(title_ar,'') || ' '
        || coalesce(tender_id,''))) STORED;
CREATE INDEX IF NOT EXISTS idx_tenders_search ON tenders USING GIN(search_vector);
"""


def schema_sql(is_pg: bool | None = None) -> list[str]:
    """The full schema for one engine, as individual statements.

    The dialect follows the *connection*, not the process: a test can open an
    in-memory SQLite database while DATABASE_URL is set, and must still get
    SQLite DDL.
    """
    if IS_POSTGRES if is_pg is None else is_pg:
        body = _COMMON_TABLES.format(
            pk_int="BIGINT PRIMARY KEY", pk_serial="BIGSERIAL PRIMARY KEY",
            real="DOUBLE PRECISION")
        views = _VIEWS.format(agg="string_agg_distinct")
        # Postgres has no CREATE VIEW IF NOT EXISTS before 9.x semantics we can
        # rely on across providers; OR REPLACE is the portable equivalent here.
        views = views.replace("CREATE VIEW IF NOT EXISTS", "CREATE OR REPLACE VIEW")
        # string_agg needs a delimiter; wrap it rather than special-casing the view.
        views = views.replace("string_agg_distinct(DISTINCT tender_id)",
                              "string_agg(DISTINCT tender_id, ',')")
        return _split(body) + _split(_PG_FTS) + _split(views)

    body = _COMMON_TABLES.format(
        pk_int="INTEGER PRIMARY KEY", pk_serial="INTEGER PRIMARY KEY AUTOINCREMENT",
        real="REAL")
    views = _VIEWS.format(agg="GROUP_CONCAT")
    return _split(body) + _split(_SQLITE_FTS) + _split(views)


def _split(script: str) -> list[str]:
    """Split a script into statements, keeping SQLite BEGIN...END trigger bodies."""
    out, buf, in_trigger = [], [], False
    for line in script.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        buf.append(line)
        upper = stripped.upper()
        if upper.startswith("CREATE TRIGGER"):
            in_trigger = True
        if stripped.endswith(";"):
            if in_trigger:
                if upper.startswith("END;"):
                    in_trigger = False
                    out.append("\n".join(buf)); buf = []
            else:
                out.append("\n".join(buf)); buf = []
    if buf:
        out.append("\n".join(buf))
    return out


TENDER_TRACKED_FIELDS = [
    "tender_id", "title_en", "title_ar", "status_label", "status_slugs",
    "opening_date", "submission_deadline", "bid_opening", "booklet_price_sar",
    "buy_url", "url", "item_count", "category_guess",
]


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# --- connection wrapper ------------------------------------------------------

class Cursor:
    """Just enough of the sqlite3 cursor API for the call sites we have."""

    def __init__(self, cur):
        self._cur = cur

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def __iter__(self):
        return iter(self._cur)

    @property
    def lastrowid(self):
        return getattr(self._cur, "lastrowid", None)


_PLACEHOLDER = re.compile(r"\?(?=(?:[^']*'[^']*')*[^']*$)")


class Connection:
    """A thin façade so the rest of the codebase writes SQLite-flavoured SQL.

    Queries are written once with ``?`` placeholders; on Postgres they are
    rewritten to ``%s``. Rows come back addressable by column name on both
    engines, which is what every call site expects.
    """

    def __init__(self, raw, is_pg: bool):
        self._raw = raw
        self.is_pg = is_pg

    def _sql(self, sql: str) -> str:
        """Rewrite ? placeholders for Postgres.

        Callers that need a literal % (a LIKE pattern, say) should pass it as a
        bound parameter rather than inlining it, which is better practice on
        both engines anyway.
        """
        return _PLACEHOLDER.sub("%s", sql) if self.is_pg else sql

    def execute(self, sql: str, args: Iterable = ()) -> Cursor:
        cur = self._raw.cursor()
        # psycopg2 only treats % as a placeholder when vars is not None, so a
        # parameterless query must pass None -- otherwise a literal LIKE '%x%'
        # blows up with "tuple index out of range".
        params = tuple(args)
        # psycopg2 only treats % as a placeholder when vars is not None, so a
        # parameterless query must pass None or a literal LIKE '%x%' blows up.
        # sqlite3 is the opposite: it rejects None and wants an empty sequence.
        cur.execute(self._sql(sql), (params if params else None) if self.is_pg else params)
        return Cursor(cur)

    def executemany(self, sql: str, rows) -> Cursor:
        rows = [tuple(r) for r in rows]
        cur = self._raw.cursor()
        if rows:
            cur.executemany(self._sql(sql), rows)
        return Cursor(cur)

    def insert_returning_id(self, sql: str, args: Iterable = (), pk: str = "id"):
        """INSERT that yields the new primary key on either engine.

        ``pk`` names the key column: most tables use ``id``, run_log uses
        ``run_id``. SQLite hands it back as lastrowid regardless.
        """
        if self.is_pg:
            cur = self._raw.cursor()
            params = tuple(args)
            cur.execute(self._sql(sql.rstrip().rstrip(";") + f" RETURNING {pk}"),
                        params if params else None)   # see execute(): pg wants None
            row = cur.fetchone()
            return row[pk] if isinstance(row, dict) else row[0]
        cur = self._raw.cursor()
        cur.execute(sql, tuple(args))
        return cur.lastrowid

    def commit(self):
        self._raw.commit()

    def rollback(self):
        try:
            self._raw.rollback()
        except Exception:                       # noqa: BLE001
            pass

    def close(self):
        try:
            self._raw.close()
        except Exception:                       # noqa: BLE001
            pass


def connect(path=None, threaded: bool = False) -> Connection:
    """Open the active database. ``path`` only applies to SQLite."""
    if IS_POSTGRES and path is None:
        import psycopg2
        import psycopg2.extras
        raw = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        raw.autocommit = False
        return Connection(raw, True)
    raw = sqlite3.connect(str(path or DB_PATH), timeout=60,
                          check_same_thread=not threaded)
    raw.row_factory = sqlite3.Row
    raw.execute("PRAGMA foreign_keys=ON")
    return Connection(raw, False)


# --- journal mode (SQLite only) ----------------------------------------------

# WAL is fastest and lets the UI read while the pipeline writes, but it needs a
# shared-memory sidecar the filesystem may not provide, and DELETE journalling
# needs to unlink its journal -- neither works on every mount (a synced folder,
# a network share, or a sandboxed bridge). TRUNCATE works anywhere.
JOURNAL_MODES = ("WAL", "TRUNCATE", "PERSIST", "MEMORY")


def set_journal_mode(conn: Connection, path=None) -> str:
    """Pick the fastest journal mode this filesystem genuinely supports.

    The probe checks writing AND that a *separate* connection can still read.
    That second half matters: on some mounts the write probe passes while every
    later reader dies with "disk I/O error", which would leave the search UI
    broken against a database the pipeline was happily filling.
    """
    if conn.is_pg:
        return "postgres"
    target = str(path or DB_PATH)
    for mode in JOURNAL_MODES:
        try:
            got = conn.execute(f"PRAGMA journal_mode={mode}").fetchone()[0]
            conn.execute("CREATE TABLE IF NOT EXISTS _probe(x)")
            conn.execute("INSERT INTO _probe VALUES (1)")
            conn.commit()
            if target and target != ":memory:":
                probe = sqlite3.connect(target, timeout=10)
                try:
                    probe.execute("SELECT COUNT(*) FROM _probe").fetchone()
                finally:
                    probe.close()
            conn.execute("DROP TABLE _probe")
            conn.commit()
            return got
        except sqlite3.Error:
            conn.rollback()
            try:
                conn.execute("DROP TABLE IF EXISTS _probe")
                conn.commit()
            except sqlite3.Error:
                pass
    return "unknown"


ADDED_COLUMNS = [("tender_items", "code_group", "TEXT")]


def schema_exists(conn: Connection) -> bool:
    try:
        conn.execute("SELECT 1 FROM tenders LIMIT 1").fetchone()
        return True
    except Exception:                           # noqa: BLE001
        conn.rollback()
        return False


def init(conn: Connection, path=None, force: bool = False) -> str:
    """Create the schema if it is not already there.

    Re-running the DDL on every start looks harmless but is not: ALTER TABLE
    takes an ACCESS EXCLUSIVE lock in Postgres, so a deploy could block behind
    a long-lived reader and hang the pipeline. If the schema is present we skip
    straight past it.
    """
    mode = set_journal_mode(conn, path)
    if not force and schema_exists(conn):
        return mode
    for statement in schema_sql(conn.is_pg):
        try:
            conn.execute(statement)
            conn.commit()
        except Exception as e:                  # noqa: BLE001
            conn.rollback()
            # IF NOT EXISTS covers most reruns; anything left is a benign
            # "already exists" race we would rather log than crash on.
            if "already exists" not in str(e).lower():
                raise
    _add_missing_columns(conn)
    _migrate_absolute_paths(conn)
    conn.commit()
    return mode


def table_columns(conn: Connection, table: str) -> set:
    if conn.is_pg:
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name=?",
            (table,)).fetchall()
        return {r["column_name"] for r in rows}
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _add_missing_columns(conn: Connection) -> None:
    for table, column, decl in ADDED_COLUMNS:
        try:
            existing = table_columns(conn, table)
            if existing and column not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
                conn.commit()
        except Exception:                       # noqa: BLE001
            conn.rollback()


def _migrate_absolute_paths(conn: Connection) -> int:
    """Rewrite absolute local_paths as relative so the database stays portable."""
    from .config import FILES_DIR

    root = str(FILES_DIR).replace("\\", "/").rstrip("/") + "/"
    changed = 0
    try:
        rows = conn.execute(
            "SELECT id, local_path FROM attachments WHERE local_path IS NOT NULL"
        ).fetchall()
    except Exception:                           # noqa: BLE001
        conn.rollback()
        return 0
    for row in rows:
        p = str(row["local_path"]).replace("\\", "/")
        if p.startswith(root):
            conn.execute("UPDATE attachments SET local_path=? WHERE id=?",
                         (p[len(root):], row["id"]))
            changed += 1
    return changed


def rebuild_fts(conn: Connection) -> None:
    """SQLite keeps FTS in external tables that need rebuilding after bulk edits.
    Postgres uses generated columns, which are always current -- nothing to do."""
    if conn.is_pg:
        return
    conn.execute("INSERT INTO items_fts(items_fts) VALUES('rebuild')")
    conn.execute("INSERT INTO tenders_fts(tenders_fts) VALUES('rebuild')")
    conn.commit()


# --- run bookkeeping ---------------------------------------------------------

def start_run(conn: Connection, mode: str) -> int:
    run_id = conn.insert_returning_id(
        "INSERT INTO run_log(mode, started_at, status) VALUES (?,?,'running')",
        (mode, now()), pk="run_id")
    conn.commit()
    return run_id


def finish_run(conn: Connection, run_id: int, status: str, **counts: Any) -> None:
    fields = ", ".join(f"{k}=?" for k in counts)
    args = list(counts.values()) + [now(), status, run_id]
    conn.execute(
        f"UPDATE run_log SET {fields + ',' if fields else ''} finished_at=?,"
        f" status=? WHERE run_id=?", args)
    conn.commit()


def log_change(conn, run_id, post_id, tender_id, field, old, new, kind="update") -> None:
    conn.execute(
        "INSERT INTO tender_history(post_id, tender_id, field, old_value, new_value,"
        " change_kind, changed_at, run_id) VALUES (?,?,?,?,?,?,?,?)",
        (post_id, tender_id, field,
         None if old is None else str(old), None if new is None else str(new),
         kind, now(), run_id))


# --- tender upsert -----------------------------------------------------------

_COLS_CACHE: dict[int, set] = {}


def _tender_columns(conn: Connection) -> set:
    key = id(conn)
    if key not in _COLS_CACHE:
        # search_vector is generated; it must never appear in an INSERT column list.
        _COLS_CACHE[key] = table_columns(conn, "tenders") - {"search_vector"}
    return _COLS_CACHE[key]


def upsert_tender(conn: Connection, run_id: int, rec: dict) -> str:
    """Insert or update one tender. Returns 'new', 'changed' or 'same'.

    Every differing tracked field is appended to tender_history.
    """
    rec = dict(rec)
    if isinstance(rec.get("status_slugs"), (list, tuple)):
        rec["status_slugs"] = json.dumps(sorted(rec["status_slugs"]))
    post_id = rec["post_id"]
    ts = now()
    cols = _tender_columns(conn)

    row = conn.execute("SELECT * FROM tenders WHERE post_id=?", (post_id,)).fetchone()
    if row is None:
        use = [c for c in rec if c in cols]
        conn.execute(
            f"INSERT INTO tenders({','.join(use)}, first_seen_at, last_seen_at,"
            f" last_changed_at) VALUES ({','.join('?' * len(use))},?,?,?)",
            [rec[c] for c in use] + [ts, ts, ts])
        log_change(conn, run_id, post_id, rec.get("tender_id"), "_record", None,
                   rec.get("tender_id"), kind="new")
        return "new"

    changed = []
    for f in TENDER_TRACKED_FIELDS:
        if f not in rec:
            continue
        if _norm(row[f]) != _norm(rec[f]):
            changed.append(f)
            log_change(conn, run_id, post_id, rec.get("tender_id"), f, row[f], rec[f])

    if "is_listed" in rec and row["is_listed"] != rec["is_listed"]:
        log_change(conn, run_id, post_id, rec.get("tender_id"), "is_listed",
                   row["is_listed"], rec["is_listed"],
                   kind="relist" if rec["is_listed"] else "delist")
        changed.append("is_listed")

    sets = {f: rec[f] for f in rec if f in cols and f != "post_id"}
    sets["last_seen_at"] = ts
    if changed:
        sets["last_changed_at"] = ts
    conn.execute(f"UPDATE tenders SET {','.join(f'{k}=?' for k in sets)} WHERE post_id=?",
                 list(sets.values()) + [post_id])
    return "changed" if changed else "same"


def _norm(v):
    """Compare values the way a human would: 1 == 1.0, '' == None."""
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v).strip()


def mark_unseen_as_delisted(conn: Connection, run_id: int,
                            seen_post_ids: Iterable[int]) -> int:
    """A tender that vanished from discovery is delisted, never deleted.

    Guarded by the caller: this must not run after a partial crawl.
    """
    seen = set(seen_post_ids)
    n = 0
    rows = conn.execute(
        "SELECT post_id, tender_id FROM tenders WHERE is_listed=1").fetchall()
    for row in rows:
        if row["post_id"] not in seen:
            conn.execute("UPDATE tenders SET is_listed=0, last_changed_at=? WHERE post_id=?",
                         (now(), row["post_id"]))
            log_change(conn, run_id, row["post_id"], row["tender_id"], "is_listed",
                       1, 0, kind="delist")
            n += 1
    return n
