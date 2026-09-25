"""Pipeline orchestrator.

    python -m nupco.run backfill        # first full load
    python -m nupco.run incremental     # daily refresh (default)
    python -m nupco.run reparse         # re-run parsers over files already on disk
    python -m nupco.run reclassify      # re-apply category rules, no re-parse
    python -m nupco.run fetch-missing   # re-download documents absent from disk
    python -m nupco.run fetch-missing --verify   # ...and repair corrupt ones

Refresh strategy
----------------
The list grid is always re-scanned in full: it is only 6 requests and it is the
only way to catch a tender *entering* a terminal state. Expensive work -- detail
pages and attachment downloads -- is skipped for tenders already frozen in
final-results/cancelled whose detail we have captured, unless a cheap signal
(sitemap lastmod, a changed card, a changed attachment URL) says otherwise.
"""
from __future__ import annotations

import argparse
import gc
import json
import logging
import sys
import traceback
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from . import config as C
from . import db, files, items, scrape

log = logging.getLogger("nupco")


def _setup_logging(verbose: bool = True) -> None:
    C.LOG_DIR.mkdir(parents=True, exist_ok=True)
    handlers = [logging.FileHandler(C.LOG_DIR / f"run-{date.today()}.log", encoding="utf-8")]
    if verbose:
        handlers.append(logging.StreamHandler(sys.stdout))
    logging.basicConfig(
        level=logging.INFO, handlers=handlers, force=True,
        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S",
    )


# --- discovery ---------------------------------------------------------------

def discover(conn) -> tuple[dict[int, dict], dict[str, str | None], list[str]]:
    """Returns (cards_by_post_id, sitemap_lastmod_by_url, errors)."""
    errors: list[str] = []
    cards: dict[int, dict] = {}
    sitemap: dict[str, str | None] = {}

    try:
        for card in scrape.discover_grid():
            cards[card["post_id"]] = card
        log.info("grid: %d tenders across the loop pages", len(cards))
    except Exception as e:                        # noqa: BLE001
        errors.append(f"grid discovery failed: {e}")
        log.error("grid discovery failed: %s", e)

    try:
        sitemap = scrape.discover_sitemap()
        log.info("sitemap: %d tender urls", len(sitemap))
    except Exception as e:                        # noqa: BLE001
        errors.append(f"sitemap discovery failed: {e}")
        log.error("sitemap discovery failed: %s", e)

    return cards, sitemap, errors


def _known(conn) -> dict:
    rows = conn.execute(
        "SELECT post_id, url, is_terminal, detail_fetched_at, sitemap_lastmod,"
        " status_slugs, submission_deadline FROM tenders"
        " WHERE COALESCE(source, 'nupco') = 'nupco'").fetchall()
    return {r["post_id"]: dict(r) for r in rows}


def _norm_url(url: str | None) -> str:
    """Grid and sitemap spell the same Arabic slug with different escaping."""
    return urllib.parse.unquote(url or "").rstrip("/").lower()


def _by_url(known: dict) -> dict:
    return {_norm_url(r["url"]): pid for pid, r in known.items()}


def needs_detail(prev: dict | None, card: dict | None, lastmod: str | None,
                 force: bool) -> bool:
    if force or prev is None or not prev.get("detail_fetched_at"):
        return True
    if lastmod and lastmod != prev.get("sitemap_lastmod"):
        return True
    if card:
        if card.get("submission_deadline") != prev.get("submission_deadline"):
            return True
        if json.dumps(sorted(card.get("status_slugs") or [])) != (prev.get("status_slugs") or ""):
            return True
    # Frozen: terminal state already captured and no signal says otherwise.
    return not prev.get("is_terminal")


# --- main run ----------------------------------------------------------------

def run(mode: str = "incremental", force: bool = False, limit: int | None = None,
        skip_files: bool = False) -> dict:
    conn = db.connect(threaded=True)
    journal = db.init(conn)
    run_id = db.start_run(conn, mode)
    log.info("database %s (journal_mode=%s)", C.DB_PATH, journal)
    counts = dict(tenders_seen=0, tenders_new=0, tenders_changed=0, files_checked=0,
                  files_changed=0, items_parsed=0, errors=0)
    log.info("=== run %s (%s) ===", run_id, mode)

    cards, sitemap, errors = discover(conn)
    counts["errors"] += len(errors)
    complete_crawl = not errors

    known = _known(conn)
    # Resolve sitemap urls against BOTH what we already store and what the grid
    # just returned. Without the second half, a first run treats every grid
    # tender twice and the sitemap pass (which carries no card) wrongly marks
    # a perfectly visible tender as unlisted.
    url_to_post = _by_url(known)
    for pid, card in cards.items():
        if card.get("url"):
            url_to_post[_norm_url(card["url"])] = pid

    # Union of both discovery sources, keyed by post_id where we know it.
    targets: dict[object, dict] = {}
    for pid, card in cards.items():
        targets[pid] = {"card": card, "url": card["url"], "lastmod": None,
                        "via": "grid", "post_id": pid}
    for url, lastmod in sitemap.items():
        key = url_to_post.get(_norm_url(url))
        if key is not None and key in targets:
            targets[key]["lastmod"] = lastmod
        elif key is not None:
            targets[key] = {"card": None, "url": url, "lastmod": lastmod,
                            "via": "sitemap", "post_id": key}
        else:
            targets[url] = {"card": None, "url": url, "lastmod": lastmod,
                            "via": "sitemap", "post_id": None}

    work = list(targets.values())
    if limit:
        work = work[:limit]
    log.info("%d tenders to reconcile", len(work))

    # --- phase 1: fetch detail pages (parallel network, serial DB) -----------
    def fetch(t: dict):
        prev = known.get(t["post_id"]) if t["post_id"] else None
        if not needs_detail(prev, t["card"], t["lastmod"], force):
            return t, None, None
        try:
            return t, scrape.fetch_detail(t["url"]), None
        except Exception as e:                    # noqa: BLE001
            return t, None, str(e)

    fetched = []
    with ThreadPoolExecutor(C.WORKERS) as pool:
        for n, (t, detail, err) in enumerate(pool.map(fetch, work), start=1):
            if err:
                counts["errors"] += 1
                log.warning("detail failed %s: %s", t["url"], err)
            fetched.append((t, detail))
            if n % 25 == 0:
                log.info("  fetched %d/%d detail pages", n, len(work))

    # --- phase 2: upsert tenders --------------------------------------------
    seen_post_ids = []
    pending_files = []
    for t, detail in fetched:
        # Detail first, card second: the card is the cleaner source for the
        # fields it carries (titles, status slugs, the two dates), while the
        # detail page is the only source of opening date, price and files.
        rec: dict = {}
        attachments = None
        if detail:
            attachments = detail.pop("attachments", {})
            rec.update({k: v for k, v in detail.items() if v is not None})
            rec["detail_fetched_at"] = db.now()
        if t["card"]:
            rec.update({k: v for k, v in t["card"].items()
                        if v not in (None, "", []) and k != "status_slugs"})
            if t["card"].get("status_slugs"):
                rec["status_slugs"] = t["card"]["status_slugs"]
        if not rec.get("status_slugs"):
            rec["status_slugs"] = scrape.status_slugs_from_label(rec.get("status_label"))

        rec.setdefault("url", t["url"])
        rec.setdefault("is_listed", 1 if t["card"] else 0)
        rec.setdefault("discovered_via", t["via"])
        if t["lastmod"]:
            rec["sitemap_lastmod"] = t["lastmod"]
        if not rec.get("post_id"):
            continue
        scrape.derive(rec)

        with db.write_lock:
            state = db.upsert_tender(conn, run_id, rec)
        counts["tenders_seen"] += 1
        counts["tenders_new"] += state == "new"
        counts["tenders_changed"] += state == "changed"
        seen_post_ids.append(rec["post_id"])

        if attachments and not skip_files:
            frozen = rec.get("is_terminal") and state == "same" and not force
            if not frozen:
                pending_files.append((dict(rec), attachments))
    conn.commit()
    log.info("tenders: %d seen, %d new, %d changed",
             counts["tenders_seen"], counts["tenders_new"], counts["tenders_changed"])

    # --- phase 3: attachments ------------------------------------------------
    changed_attachments: list[tuple[int, dict]] = []
    for n, (rec, attachments) in enumerate(pending_files, start=1):
        try:
            with db.write_lock:
                stats = files.sync_tender_attachments(conn, run_id, rec, attachments,
                                                      force=force)
            counts["files_checked"] += stats["checked"]
            counts["files_changed"] += stats["changed"] + stats["new"]
            counts["errors"] += stats["errors"]
            for aid in stats["ids"]:
                changed_attachments.append((aid, rec))
        except Exception as e:                    # noqa: BLE001
            counts["errors"] += 1
            log.warning("attachments failed for %s: %s", rec.get("tender_id"), e)
        if n % 25 == 0:
            conn.commit()
            log.info("  attachments %d/%d tenders (%d changed)",
                     n, len(pending_files), counts["files_changed"])
    conn.commit()

    # --- phase 4: parse item lists ------------------------------------------
    counts["items_parsed"] += parse_pending(conn, run_id)
    if not C.KEEP_FILES:
        discard_scratch_files(conn)

    # --- phase 5: rollups, delisting, indexes -------------------------------
    # Everything above is committed; a failed tidy-up must not throw it away
    # or leave run_log reading "running" -- it is an error, not a crash.
    try:
        refresh_rollups(conn)
    except Exception as e:                      # noqa: BLE001
        conn.rollback()
        counts["errors"] += 1
        errors.append(f"rollups: {type(e).__name__}: {e}")
        log.warning("rollups failed (crawl results are saved): %s", e)
    if complete_crawl and mode != "reparse" and not limit:
        n = db.mark_unseen_as_delisted(conn, run_id, seen_post_ids)
        if n:
            log.info("delisted %d tenders no longer discoverable", n)
    else:
        log.info("skipping delist sweep (partial crawl or limited run)")
    db.rebuild_fts(conn)

    status = "ok" if counts["errors"] == 0 else ("partial" if complete_crawl else "failed")
    db.finish_run(conn, run_id, status, notes=json.dumps(errors)[:2000], **counts)
    log.info("=== run %s %s: %s ===", run_id, status, counts)
    conn.close()
    return {"run_id": run_id, "status": status, **counts}


def discard_scratch_files(conn) -> None:
    """Cloud mode: the bytes were only needed to hash and parse them.

    Clearing local_path keeps the database honest -- it never claims to hold a
    file it does not -- and the API falls back to linking nupco.com for the
    source document.
    """
    import shutil

    conn.execute("UPDATE attachments SET local_path=NULL WHERE local_path IS NOT NULL")
    conn.commit()
    try:
        shutil.rmtree(C.FILES_DIR, ignore_errors=True)
        C.FILES_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:                           # noqa: BLE001
        pass


def parse_pending(conn, run_id: int, reparse_all: bool = False) -> int:
    """Parse every item-list attachment whose content we have not parsed yet."""
    where = ("role = ? AND is_current = 1" if reparse_all
             else "role = ? AND is_current = 1 AND (parse_status IS NULL OR parse_status IN ('pending','failed'))")
    if not C.KEEP_FILES:
        # Without an archive we can only parse what this run just downloaded.
        where += " AND local_path IS NOT NULL"
    rows = conn.execute(
        f"SELECT * FROM attachments WHERE {where}", (C.ITEM_LIST_ROLE,)).fetchall()
    total = 0
    for n, row in enumerate(rows, start=1):
        gc.collect()          # PDF parsing is the memory high-water mark
        path = files.resolve(row["local_path"])
        try:
            parsed, parser = items.parse_item_file(path)
            if not parsed:
                raise ValueError(
                    "no item rows recognised - the document may not be an item "
                    "list, or its layout is one the parsers do not yet handle")
            count = items.store_items(conn, row["id"], row["post_id"],
                                      row["tender_id"], parsed)
            conn.execute(
                "UPDATE attachments SET parse_status='parsed', parse_rows=? WHERE id=?",
                (count, row["id"]))
            total += count
        except Exception as e:                    # noqa: BLE001
            conn.execute("UPDATE attachments SET parse_status='failed' WHERE id=?",
                         (row["id"],))
            conn.execute(
                "INSERT INTO parse_failures(attachment_id, tender_id, role, local_path,"
                " reason, occurred_at, run_id) VALUES (?,?,?,?,?,?,?)",
                (row["id"], row["tender_id"], row["role"], str(path),
                 f"{type(e).__name__}: {e}"[:500], db.now(), run_id))
            log.warning("parse failed %s (%s): %s", row["tender_id"], row["filename"], e)
        if n % 5 == 0:
            # Committed every 5 files, not every 20: parsing is the slowest
            # phase and the likeliest place to be killed by a job timeout.
            # Whatever is committed survives, and the next run skips it.
            conn.commit()
        if n % 20 == 0:
            log.info("  parsed %d/%d item lists (%d rows)", n, len(rows), total)
    conn.commit()
    if rows:
        log.info("item lists: %d files -> %d rows", len(rows), total)
    return total


def fetch_missing(conn, max_seconds: float | None = None,
                  verify: bool = False) -> dict:
    """Download every document the database references but that isn't on disk.

    Skips discovery entirely, so it starts working immediately and can be run in
    short bursts -- useful after moving the database to another machine, or on a
    host where a long-running background job won't survive. Fully resumable:
    each call simply picks up whatever is still missing.

    With verify=True it also re-hashes every file already present and re-fetches
    any whose bytes no longer match what we recorded. That turns an interrupted
    copy, a half-written file or bit-rot into something the pipeline repairs by
    itself instead of something you discover months later in a parsed quantity.
    """
    import time as _time

    if not C.KEEP_FILES:
        log.info("document archive is disabled (cloud mode); nothing to fetch")
        return {"total": 0, "missing": 0, "corrupt": 0, "fetched": 0, "errors": 0,
                "remaining": 0, "done": True}
    started = _time.time()
    rows = conn.execute(
        "SELECT id, url, local_path, tender_id, filename, content_sha256, size_bytes"
        " FROM attachments WHERE is_current = 1 AND local_path IS NOT NULL ORDER BY id"
    ).fetchall()

    todo, corrupt = [], 0
    for r in rows:
        path = files.resolve(r["local_path"])
        if path is None or not path.exists():
            todo.append(r)
            continue
        if verify:
            if path.stat().st_size != (r["size_bytes"] or -1):
                todo.append(r); corrupt += 1
            elif r["content_sha256"] and files.sha256(path.read_bytes()) != r["content_sha256"]:
                todo.append(r); corrupt += 1

    stats = {"total": len(rows), "missing": len(todo) - corrupt, "corrupt": corrupt,
             "fetched": 0, "errors": 0, "remaining": len(todo), "done": False}
    if not todo:
        stats["done"] = True
        log.info("all %d documents present%s", len(rows),
                 " and hash-verified" if verify else "")
        return stats

    log.info("%d of %d documents to fetch (%d missing, %d corrupt)",
             len(todo), len(rows), stats["missing"], corrupt)
    for n, row in enumerate(todo, start=1):
        if max_seconds and (_time.time() - started) > max_seconds:
            log.info("time budget reached: %d fetched, %d still missing",
                     stats["fetched"], stats["remaining"])
            return stats
        try:
            data, _ = scrape.http_get(row["url"], binary=True)
            target = files.resolve(row["local_path"])
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            stats["fetched"] += 1
            stats["remaining"] -= 1
        except Exception as e:                    # noqa: BLE001
            stats["errors"] += 1
            log.warning("could not fetch %s (%s): %s", row["tender_id"], row["filename"], e)
        if n % 25 == 0:
            log.info("  fetched %d/%d (%d errors)", stats["fetched"], len(todo),
                     stats["errors"])
    stats["done"] = stats["remaining"] == 0
    log.info("documents: %d fetched, %d errors, %d still missing",
             stats["fetched"], stats["errors"], stats["remaining"])
    return stats


def reclassify(conn, batch: int = 20000) -> int:
    """Re-apply the classification rules to every stored line item.

    Classification is cheap and evolves as we learn the catalogue, so it is
    worth being able to improve it without re-parsing 130k rows out of 766
    documents. Touches only derived columns; nothing source-derived changes.
    """
    total, last_id = 0, 0
    while True:
        rows = conn.execute(
            "SELECT id, nupco_code, description FROM tender_items"
            " WHERE id > ? ORDER BY id LIMIT ?", (last_id, batch)).fetchall()
        if not rows:
            break
        conn.executemany(
            "UPDATE tender_items SET category_guess=?, code_group=?, is_accessory=?"
            " WHERE id=?",
            [(scrape.classify(r["nupco_code"], r["description"]),
              scrape.code_group(r["nupco_code"]),
              scrape.is_accessory(r["description"]), r["id"]) for r in rows])
        conn.commit()
        total += len(rows)
        last_id = rows[-1]["id"]
        log.info("  reclassified %d rows", total)
    # A tender's own category follows its items when it has any: the item mix is
    # a far better signal than the title, which is often just "framework
    # agreement for supplies".
    conn.execute("""
        UPDATE tenders SET category_guess = COALESCE((
            SELECT i.category_guess FROM tender_items i
            JOIN attachments a ON a.id = i.attachment_id AND a.is_current = 1
            WHERE i.post_id = tenders.post_id AND i.is_accessory = 0
            GROUP BY i.category_guess ORDER BY COUNT(*) DESC LIMIT 1
        ), category_guess)
    """)
    conn.commit()
    return total


def refresh_rollups(conn, post_ids=None) -> None:
    """Denormalise per-tender counts and refresh the daily countdown.

    With post_ids, only those tenders: a long crawl calls this after every
    batch so its tenders show their line counts while it is still running."""
    ids = list(post_ids or [])
    scope = f" AND post_id IN ({','.join('?' * len(ids))})" if ids else ""
    conn.execute(f"""
        UPDATE tenders SET
          item_count = COALESCE((SELECT COUNT(*) FROM tender_items i
                                 JOIN attachments a ON a.id = i.attachment_id
                                 WHERE i.post_id = tenders.post_id AND a.is_current = 1), 0),
          total_qty  = (SELECT SUM(i.qty) FROM tender_items i
                        JOIN attachments a ON a.id = i.attachment_id
                        WHERE i.post_id = tenders.post_id AND a.is_current = 1),
          attachment_count = COALESCE((SELECT COUNT(*) FROM attachments a
                                       WHERE a.post_id = tenders.post_id AND a.is_current = 1), 0)
        WHERE 1=1{scope}
    """, ids)
    # Date arithmetic is the one place the two engines genuinely diverge:
    # SQLite counts Julian days, Postgres subtracts dates directly.
    if conn.is_pg:
        conn.execute(f"""
            UPDATE tenders SET days_to_deadline = (submission_ts::date - CURRENT_DATE)
            WHERE submission_ts IS NOT NULL AND submission_ts <> ''{scope}
        """, ids)
    else:
        conn.execute(f"""
            UPDATE tenders SET days_to_deadline =
              CAST(julianday(submission_ts) - julianday('now','localtime') AS INTEGER)
            WHERE submission_ts IS NOT NULL{scope}
        """, ids)
    conn.commit()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="NUPCO tender pipeline")
    ap.add_argument("mode", nargs="?", default="incremental",
                    choices=["backfill", "incremental", "reparse", "reclassify",
                             "fetch-missing", "seed"])
    ap.add_argument("--force", action="store_true",
                    help="ignore all skip heuristics and re-fetch everything")
    ap.add_argument("--limit", type=int, help="only process the first N tenders")
    ap.add_argument("--skip-files", action="store_true",
                    help="tenders only, do not touch attachments")
    ap.add_argument("--max-seconds", type=float,
                    help="time budget for fetch-missing; it is resumable")
    ap.add_argument("--verify", action="store_true",
                    help="also re-hash files already on disk and repair mismatches")
    ap.add_argument("--snapshot", help="seed mode: path to the snapshot to load")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args(argv)
    _setup_logging(not a.quiet)

    try:
        if a.mode == "seed":
            # Imported here, not at module scope: seed imports from this module,
            # so a top-level import would be circular.
            from .seed import seed as load_seed
            load_seed(a.snapshot)
            return 0
        if a.mode == "fetch-missing":
            conn = db.connect()
            db.init(conn)
            stats = fetch_missing(conn, a.max_seconds, verify=a.verify)
            conn.close()
            print(json.dumps(stats))
            return 0 if stats["errors"] == 0 else 1
        if a.mode == "reclassify":
            conn = db.connect()
            db.init(conn)
            n = reclassify(conn)
            refresh_rollups(conn)
            db.rebuild_fts(conn)
            log.info("reclassified %d line items", n)
            conn.close()
            return 0
        if a.mode == "reparse":
            conn = db.connect()
            db.init(conn)
            run_id = db.start_run(conn, "reparse")
            n = parse_pending(conn, run_id, reparse_all=True)
            reclassify(conn)
            refresh_rollups(conn)
            db.rebuild_fts(conn)
            db.finish_run(conn, run_id, "ok", items_parsed=n)
            log.info("reparse complete: %d rows", n)
            conn.close()
            return 0
        result = run(a.mode, force=a.force or a.mode == "backfill",
                     limit=a.limit, skip_files=a.skip_files)
        return 0 if result["status"] in ("ok", "partial") else 1
    except Exception:                             # noqa: BLE001
        log.error("run crashed:\n%s", traceback.format_exc())
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
