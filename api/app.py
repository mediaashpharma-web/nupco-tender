"""Read-only JSON API over the NUPCO tender database.

Runs on Render as a normal long-lived process, and locally for development.
The only dependency beyond the Postgres driver is the standard library, so it
starts even if nothing else installed cleanly.

    python api/app.py              # http://127.0.0.1:8000

Engine comes from the pipeline's db module: DATABASE_URL set means Postgres
(Supabase), unset means the local SQLite file. The two differ in exactly one
interesting place -- full-text search -- which is isolated in `search_clause`.
"""
from __future__ import annotations

import json
import mimetypes
import os
import re
import sys
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline import config as C        # noqa: E402
from pipeline import db                 # noqa: E402
from pipeline import files as filemod   # noqa: E402

WEB_DIR = ROOT / "web"
HOST = os.environ.get("HOST", "0.0.0.0" if os.environ.get("PORT") else "127.0.0.1")
PORT = int(os.environ.get("PORT", os.environ.get("NUPCO_UI_PORT", "8000")))

# Which browser origins may call this API. Vercel preview deployments get a new
# hostname per commit, so an explicit list would go stale every deploy; the
# data is public procurement information and the API is strictly read-only, so
# "*" is the honest setting rather than a list that pretends to be a control.
ALLOWED_ORIGIN = os.environ.get("CORS_ALLOW_ORIGIN", "*")

_local = threading.local()


def conn():
    if getattr(_local, "db", None) is None:
        _local.db = db.connect()
        if not _local.db.is_pg:
            _local.db.execute("PRAGMA query_only=1")
    return _local.db


def rows(sql: str, args=()) -> list[dict]:
    return [dict(r) for r in conn().execute(sql, args).fetchall()]


def one(sql: str, args=()) -> dict | None:
    r = conn().execute(sql, args).fetchone()
    return dict(r) if r else None


_TOKEN = re.compile(r"[\w؀-ۿ]+", re.UNICODE)


def fts_query(text: str) -> str:
    """Turn free text into a safe full-text expression for the active engine.

    People type 'insulin pen' or a bare SAP code, not query syntax, so every
    token is quoted and prefix-matched. Unescaped input would otherwise be a
    syntax error at best on either engine.
    """
    tokens = _TOKEN.findall(text or "")
    if not tokens:
        return ""
    if db.IS_POSTGRES:
        return " & ".join(f"{t}:*" for t in tokens)
    return " AND ".join(f'"{t}"*' for t in tokens)


def search_clause(kind: str) -> tuple[str, str, str]:
    """(from_sql, where_sql, rank_sql) for a matched full-text search.

    kind is 'items' or 'tenders'. This is the only part of the API that cares
    which database is underneath.
    """
    if kind == "items":
        if db.IS_POSTGRES:
            return ("FROM tender_items i"
                    " JOIN attachments a ON a.id = i.attachment_id AND a.is_current = 1"
                    " JOIN tenders     t ON t.post_id = i.post_id"
                    " WHERE i.search_vector @@ to_tsquery('english', ?)",
                    "", "ts_rank(i.search_vector, to_tsquery('english', ?)) DESC")
        return ("FROM items_fts f"
                " JOIN tender_items i ON i.id = f.rowid"
                " JOIN attachments  a ON a.id = i.attachment_id AND a.is_current = 1"
                " JOIN tenders      t ON t.post_id = i.post_id"
                " WHERE items_fts MATCH ?",
                "", "bm25(items_fts)")
    if db.IS_POSTGRES:
        return ("FROM tenders t WHERE t.search_vector @@ to_tsquery('english', ?)",
                "", "ts_rank(t.search_vector, to_tsquery('english', ?)) DESC")
    return ("FROM tenders_fts f JOIN tenders t ON t.post_id = f.rowid"
            " WHERE tenders_fts MATCH ?", "", "bm25(tenders_fts)")


# Sort orders are a whitelist, never interpolated user input.
ITEM_SORTS = {
    "deadline_asc":  "t.submission_ts IS NULL, t.submission_ts ASC,  i.id",
    "deadline_desc": "t.submission_ts IS NULL, t.submission_ts DESC, i.id",
    "opening_desc":  "t.opening_ts IS NULL,    t.opening_ts DESC,    i.id",
    "opening_asc":   "t.opening_ts IS NULL,    t.opening_ts ASC,     i.id",
    "qty_desc":      "i.qty IS NULL, i.qty DESC, i.id",
}
TENDER_SORTS = {
    "deadline_asc":  "t.submission_ts IS NULL, t.submission_ts ASC",
    "deadline_desc": "t.submission_ts IS NULL, t.submission_ts DESC",
    "opening_desc":  "t.opening_ts IS NULL,    t.opening_ts DESC",
    "opening_asc":   "t.opening_ts IS NULL,    t.opening_ts ASC",
    "qty_desc":      "t.total_qty IS NULL, t.total_qty DESC",
}
CATALOG_SORTS = {
    "deadline_asc":  "latest_deadline IS NULL, latest_deadline ASC",
    "deadline_desc": "latest_deadline IS NULL, latest_deadline DESC",
    "opening_desc":  "latest_deadline IS NULL, latest_deadline DESC",
    "opening_asc":   "latest_deadline IS NULL, latest_deadline ASC",
    "qty_desc":      "total_qty IS NULL, total_qty DESC",
    "relevance":     "tender_count DESC, total_qty DESC",
}
# With no search term there is no relevance to rank by, so the most useful
# default is newest first rather than the longest-closed tenders.
DEFAULT_SORT = "opening_desc"


def pick_sort(requested: str, table: dict, has_match: bool) -> str | None:
    if requested in table:
        return table[requested]
    if requested == "relevance" and has_match:
        return None
    return table.get(DEFAULT_SORT)


# --- API ---------------------------------------------------------------------

def api_stats() -> dict:
    s = one("""SELECT
        (SELECT COUNT(*) FROM tenders)                          AS tenders,
        (SELECT COUNT(*) FROM tenders WHERE is_listed=1)        AS listed,
        (SELECT COUNT(*) FROM tenders WHERE is_terminal=0
            AND (days_to_deadline IS NULL OR days_to_deadline>=0)) AS open_tenders,
        (SELECT COUNT(*) FROM tender_items)                     AS items,
        (SELECT COUNT(DISTINCT nupco_code) FROM tender_items
            WHERE nupco_code IS NOT NULL)                       AS codes,
        (SELECT COUNT(*) FROM attachments WHERE is_current=1)   AS files,
        (SELECT COUNT(*) FROM attachments WHERE parse_status='failed') AS parse_failed,
        (SELECT COUNT(*) FROM tender_history)                   AS changes
    """) or {}
    s["last_run"] = one(
        "SELECT run_id, mode, started_at, finished_at, status, tenders_seen,"
        " tenders_new, tenders_changed, files_changed, items_parsed, errors"
        " FROM run_log ORDER BY run_id DESC LIMIT 1")
    s["by_status"] = rows(
        "SELECT COALESCE(status_label,'(none)') AS label, COUNT(*) AS n"
        " FROM tenders GROUP BY status_label ORDER BY n DESC")
    s["engine"] = "postgres" if db.IS_POSTGRES else "sqlite"
    return s


def api_search(p: dict) -> dict:
    q = (p.get("q", [""])[0] or "").strip()
    mode = p.get("mode", ["items"])[0]
    limit = min(int(p.get("limit", ["50"])[0] or 50), 500)
    offset = int(p.get("offset", ["0"])[0] or 0)
    sort = p.get("sort", ["relevance"])[0]

    where, args = [], []
    if p.get("category", [""])[0]:
        where.append("i.category_guess = ?")
        args.append(p["category"][0])
    if p.get("status", [""])[0]:
        where.append("t.status_label = ?")
        args.append(p["status"][0])
    if p.get("open_only", ["0"])[0] == "1":
        where.append("t.is_terminal = 0 AND (t.days_to_deadline IS NULL"
                     " OR t.days_to_deadline >= 0)")
    if p.get("hide_accessories", ["0"])[0] == "1":
        where.append("i.is_accessory = 0")

    if mode == "tenders":
        return _search_tenders(q, where, args, limit, offset, sort)

    match = fts_query(q)
    rank_args: list = []
    if match:
        base, _, rank = search_clause("items")
        args = [match] + args
        if db.IS_POSTGRES:
            rank_args = [match]          # ts_rank needs the query a second time
    else:
        base = ("FROM tender_items i"
                " JOIN attachments a ON a.id = i.attachment_id AND a.is_current = 1"
                " JOIN tenders     t ON t.post_id = i.post_id WHERE 1=1")
        rank = None

    chosen = pick_sort(sort, ITEM_SORTS, bool(match))
    if chosen is None:
        order = f" ORDER BY {rank}, " + ITEM_SORTS[DEFAULT_SORT]
    else:
        order, rank_args = " ORDER BY " + chosen, []
    clause = (" AND " + " AND ".join(where)) if where else ""

    total = (one(f"SELECT COUNT(*) AS n {base}{clause}", args) or {}).get("n", 0)
    data = rows(
        "SELECT i.id, i.tender_id, i.post_id, i.nupco_code, i.description, i.uom,"
        " i.qty, i.category_guess, i.is_accessory, i.item_no, i.item_group,"
        " t.title_en, t.status_label, t.submission_ts, t.opening_ts,"
        " t.days_to_deadline, t.url, t.booklet_price_sar,"
        " a.id AS attachment_id, a.filename, a.url AS file_url "
        + base + clause + order + " LIMIT ? OFFSET ?",
        args + rank_args + [limit, offset])
    return {"mode": "items", "total": total, "limit": limit, "offset": offset,
            "sort": sort, "results": data}


def _search_tenders(q, where, args, limit, offset, sort) -> dict:
    where = [w.replace("i.category_guess", "t.category_guess")
              .replace("i.is_accessory = 0", "1=1") for w in where]
    match = fts_query(q)
    rank_args: list = []
    if match:
        base, _, rank = search_clause("tenders")
        args = [match] + args
        if db.IS_POSTGRES:
            rank_args = [match]
    else:
        base, rank = "FROM tenders t WHERE 1=1", None

    chosen = pick_sort(sort, TENDER_SORTS, bool(match))
    if chosen is None:
        order = f" ORDER BY {rank}"
    else:
        order, rank_args = " ORDER BY " + chosen, []
    clause = (" AND " + " AND ".join(where)) if where else ""

    total = (one(f"SELECT COUNT(*) AS n {base}{clause}", args) or {}).get("n", 0)
    data = rows(
        "SELECT t.post_id, t.tender_id, t.title_en, t.title_ar, t.status_label,"
        " t.category_guess, t.opening_ts, t.submission_ts, t.days_to_deadline,"
        " t.booklet_price_sar, t.item_count, t.total_qty, t.attachment_count,"
        " t.url, t.is_listed, t.is_terminal "
        + base + clause + order + " LIMIT ? OFFSET ?",
        args + rank_args + [limit, offset])
    return {"mode": "tenders", "total": total, "limit": limit, "offset": offset,
            "sort": sort, "results": data}


def api_catalog(p: dict) -> dict:
    q = (p.get("q", [""])[0] or "").strip()
    limit = min(int(p.get("limit", ["50"])[0] or 50), 500)
    order = CATALOG_SORTS.get(p.get("sort", ["relevance"])[0], CATALOG_SORTS["relevance"])
    match = fts_query(q)
    if match:
        if db.IS_POSTGRES:
            codes = rows("SELECT DISTINCT nupco_code FROM tender_items"
                         " WHERE search_vector @@ to_tsquery('english', ?)"
                         " AND nupco_code IS NOT NULL LIMIT ?", (match, limit * 4))
        else:
            codes = rows("SELECT DISTINCT i.nupco_code FROM items_fts f"
                         " JOIN tender_items i ON i.id = f.rowid"
                         " WHERE items_fts MATCH ? AND i.nupco_code IS NOT NULL LIMIT ?",
                         (match, limit * 4))
        if not codes:
            return {"results": []}
        marks = ",".join("?" * len(codes))
        data = rows(f"SELECT * FROM v_item_catalog WHERE nupco_code IN ({marks})"
                    f" ORDER BY {order} LIMIT ?",
                    [c["nupco_code"] for c in codes] + [limit])
    else:
        data = rows(f"SELECT * FROM v_item_catalog ORDER BY {order} LIMIT ?", (limit,))
    return {"results": data}


def api_tender(post_id: int) -> dict:
    t = one("SELECT * FROM tenders WHERE post_id=?", (post_id,))
    if not t:
        return {"error": "not found"}
    t.pop("search_vector", None)
    t["attachments"] = rows(
        "SELECT id, role, filename, ext, size_bytes, version, url, parse_status,"
        " parse_rows, local_path, last_changed_at FROM attachments"
        " WHERE post_id=? AND is_current=1 ORDER BY role", (post_id,))
    t["item_total"] = (one(
        "SELECT COUNT(*) AS n FROM tender_items i JOIN attachments a"
        " ON a.id=i.attachment_id AND a.is_current=1 WHERE i.post_id=?",
        (post_id,)) or {}).get("n", 0)
    t["items"] = [
        {k: v for k, v in r.items() if k != "search_vector"} for r in rows(
            "SELECT i.* FROM tender_items i JOIN attachments a ON a.id=i.attachment_id"
            " AND a.is_current=1 WHERE i.post_id=?"
            " ORDER BY i.source_page, i.row_index, i.id LIMIT 1000", (post_id,))]
    t["history"] = rows(
        "SELECT field, old_value, new_value, change_kind, changed_at"
        " FROM tender_history WHERE post_id=? ORDER BY changed_at DESC LIMIT 100",
        (post_id,))
    return t


def api_code(code: str) -> dict:
    return {
        "code": code,
        "summary": one("SELECT * FROM v_item_catalog WHERE nupco_code=?", (code,)),
        "lines": rows(
            "SELECT i.tender_id, i.post_id, i.description, i.uom, i.qty,"
            " t.status_label, t.submission_ts, t.days_to_deadline, t.url"
            " FROM tender_items i"
            " JOIN attachments a ON a.id=i.attachment_id AND a.is_current=1"
            " JOIN tenders t ON t.post_id=i.post_id"
            " WHERE i.nupco_code=? ORDER BY t.submission_ts DESC", (code,)),
    }


def api_changes(p: dict) -> dict:
    limit = min(int(p.get("limit", ["100"])[0] or 100), 1000)
    kind = p.get("kind", [""])[0]
    clause, args = ("", [])
    if kind:
        clause, args = " WHERE h.change_kind = ?", [kind]
    return {"results": rows(
        "SELECT h.changed_at, h.change_kind, h.tender_id, h.post_id, h.field,"
        " h.old_value, h.new_value, t.title_en, t.status_label, t.url"
        " FROM tender_history h LEFT JOIN tenders t ON t.post_id = h.post_id"
        + clause + " ORDER BY h.id DESC LIMIT ?", args + [limit])}


def api_runs() -> dict:
    return {"results": rows("SELECT * FROM run_log ORDER BY run_id DESC LIMIT 50")}


# --- HTTP --------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    server_version = "NupcoAPI/2.0"

    def log_message(self, fmt, *a):
        if "--verbose" in sys.argv:
            super().log_message(fmt, *a)

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", ALLOWED_ORIGIN)
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")

    def _send(self, code: int, body: bytes, ctype: str, extra: dict | None = None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self._cors()
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, obj, code: int = 200):
        self._send(code, json.dumps(obj, ensure_ascii=False, default=str).encode(),
                   "application/json; charset=utf-8")

    def do_OPTIONS(self):                       # noqa: N802
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):                           # noqa: N802
        u = urlparse(self.path)
        path, p = u.path, parse_qs(u.query)
        try:
            if path in ("/healthz", "/api/health"):
                return self._json({"ok": True, "engine": "postgres" if db.IS_POSTGRES
                                   else "sqlite"})
            if path == "/api/stats":
                return self._json(api_stats())
            if path == "/api/search":
                return self._json(api_search(p))
            if path == "/api/catalog":
                return self._json(api_catalog(p))
            if path == "/api/changes":
                return self._json(api_changes(p))
            if path == "/api/runs":
                return self._json(api_runs())
            if path.startswith("/api/tender/"):
                return self._json(api_tender(int(path.rsplit("/", 1)[-1])))
            if path.startswith("/api/code/"):
                return self._json(api_code(unquote(path.rsplit("/", 1)[-1])))
            if path.startswith("/file/"):
                return self._document(int(path.rsplit("/", 1)[-1]))
            if path in ("/", "/index.html"):
                return self._file(WEB_DIR / "index.html", "text/html; charset=utf-8")
            target = (WEB_DIR / path.lstrip("/")).resolve()
            if WEB_DIR.resolve() in target.parents and target.exists():
                return self._file(target)
            self._json({"error": "not found", "path": path}, 404)
        except Exception as e:                  # noqa: BLE001
            try:
                conn().rollback()               # keep the connection usable
            except Exception:                   # noqa: BLE001
                _local.db = None
            self._json({"error": f"{type(e).__name__}: {e}"}, 500)

    do_HEAD = do_GET

    def _file(self, path: Path, ctype: str | None = None):
        if not path.exists():
            return self._json({"error": f"missing {path.name}"}, 404)
        ctype = ctype or (mimetypes.guess_type(path.name)[0] or "application/octet-stream")
        self._send(200, path.read_bytes(), ctype)

    def _document(self, attachment_id: int):
        """Serve a tender document.

        With a local archive we serve the stored copy. In the cloud we keep
        hashes, not bytes, so we redirect to the file on nupco.com -- the link
        works either way, which is what the front end cares about.
        """
        row = one("SELECT local_path, filename, url FROM attachments WHERE id=?",
                  (attachment_id,))
        if not row:
            return self._json({"error": "unknown attachment"}, 404)
        path = filemod.resolve(row["local_path"]) if row["local_path"] else None
        if path and path.exists():
            ctype = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            return self._send(200, path.read_bytes(), ctype,
                              {"Content-Disposition": f'inline; filename="{row["filename"]}"'})
        if row["url"]:
            self.send_response(302)
            self.send_header("Location", row["url"])
            self._cors()
            self.end_headers()
            return
        self._json({"error": "document not stored and no source url"}, 404)


def main() -> int:
    engine = "Postgres" if db.IS_POSTGRES else f"SQLite ({C.DB_PATH})"
    srv = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"\n  NUPCO tender API  ->  http://{HOST}:{PORT}")
    print(f"  database: {engine}")
    print("  press Ctrl+C to stop\n", flush=True)
    if "--open" in sys.argv:
        threading.Timer(0.8, lambda: webbrowser.open(f"http://127.0.0.1:{PORT}")).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n  stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
