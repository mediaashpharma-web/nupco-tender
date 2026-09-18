"""Attachment download, versioning and change detection.

Change-detection cascade, cheapest signal first:
  1. URL differs from what we stored  -> changed, no network needed.
     (NUPCO republishes revised documents at brand-new /uploads/YYYY/MM/ paths,
      so the URL itself is a reliable change event.)
  2. HTTP HEAD: ETag / Last-Modified / Content-Length all unchanged -> skip.
  3. Download and compare SHA-256. Only a hash change creates a new version.

Attachments are keyed on (post_id, role), never on URL.
"""
from __future__ import annotations

import hashlib
import re
import urllib.parse
from pathlib import Path

from . import config as C
from .db import log_change, now
from .scrape import clean, http_get, http_head

_ILLEGAL = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def safe_name(text: str, fallback: str = "file", maxlen: int = 120) -> str:
    """Windows-safe path component (NTFS handles Unicode, not these chars)."""
    name = _ILLEGAL.sub("-", urllib.parse.unquote(text or "")).strip(" .")
    name = re.sub(r"[-\s]+", "-", name)
    return (name[:maxlen] or fallback)


def filename_from_url(url: str) -> str:
    path = urllib.parse.urlsplit(url).path
    return safe_name(path.rsplit("/", 1)[-1] or "download")


def ext_from_url(url: str) -> str:
    name = urllib.parse.unquote(urllib.parse.urlsplit(url).path).rsplit("/", 1)[-1]
    return (name.rsplit(".", 1)[-1].lower() if "." in name else "")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def resolve(local_path: str | None) -> Path | None:
    """Turn a stored local_path into a real path on THIS machine.

    Paths are stored relative to files/ so the database stays portable -- it can
    be built in one place and used in another without every file link breaking.
    Absolute paths from older rows still resolve.
    """
    if not local_path:
        return None
    p = Path(local_path)
    return p if p.is_absolute() else (C.FILES_DIR / p)


def _current(conn, post_id: int, role: str):
    return conn.execute(
        "SELECT * FROM attachments WHERE post_id=? AND role=? AND is_current=1",
        (post_id, role),
    ).fetchone()


def _head_unchanged(row, headers: dict) -> bool:
    """True only when the server positively tells us nothing moved."""
    if not headers:
        return False
    etag = headers.get("etag")
    lastmod = headers.get("last-modified")
    length = headers.get("content-length")
    signals = 0
    if etag and row["etag"]:
        if etag != row["etag"]:
            return False
        signals += 1
    if lastmod and row["http_last_modified"]:
        if lastmod != row["http_last_modified"]:
            return False
        signals += 1
    if length and row["size_bytes"]:
        if int(length) != int(row["size_bytes"]):
            return False
        signals += 1
    return signals > 0


def sync_attachment(conn, run_id: int, tender: dict, role: str, url: str,
                    force: bool = False) -> dict:
    """Reconcile one (tender, role) attachment. Returns a small result dict.

    result['state'] is one of: 'new', 'changed', 'same', 'skipped', 'error'.
    """
    post_id = tender["post_id"]
    tender_id = tender.get("tender_id") or str(post_id)
    row = _current(conn, post_id, role)
    ts = now()

    # (1) URL moved -> content changed, by NUPCO's own publishing convention.
    url_moved = bool(row) and row["url"] != url

    # A row whose file is not on this disk must be fetched whatever the hash
    # says -- otherwise moving the database to another machine leaves every
    # document permanently missing.
    if C.KEEP_FILES:
        missing = (row is not None
                   and not (resolve(row["local_path"]) or Path("/x")).exists())
    else:
        # Cloud mode keeps no archive, and last run's bytes died with the
        # runner. Re-fetching all 800MB nightly would be absurd, so we re-fetch
        # exactly one class of document: an item list we have never managed to
        # parse. That is what makes an interrupted run resumable rather than
        # permanently stuck -- without it, the HEAD probe below says
        # "unchanged", the file is never downloaded again, and the rows that
        # run was killed before parsing can never be recovered.
        #
        # 'failed' is deliberately excluded: it already had its chance and its
        # reason is recorded, so retrying it nightly would burn bandwidth and
        # append an identical parse_failures row every single day.
        missing = (row is not None and role == C.ITEM_LIST_ROLE
                   and (row["parse_status"] or "pending") == "pending")

    # (2) Cheap HEAD probe when the URL is stable and we still hold the bytes.
    if row is not None and not url_moved and not force and not missing:
        if _head_unchanged(row, http_head(url)):
            conn.execute("UPDATE attachments SET last_seen_at=? WHERE id=?", (ts, row["id"]))
            return {"state": "same", "attachment_id": row["id"], "downloaded": False}

    # (3) Download and hash.
    try:
        data, headers = http_get(url, binary=True)
    except Exception as e:                       # noqa: BLE001
        return {"state": "error", "attachment_id": row["id"] if row else None,
                "error": str(e), "downloaded": False}

    # A new version is created only when the bytes genuinely differ. `force`
    # re-downloads and re-checks; it must never manufacture a phantom version.
    digest = sha256(data)
    if row is not None and digest == row["content_sha256"]:
        if missing:                          # same bytes, just re-materialise them
            target = resolve(row["local_path"])
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        conn.execute(
            "UPDATE attachments SET last_seen_at=?, url=?, etag=?, http_last_modified=?"
            " WHERE id=?",
            (ts, url, headers.get("ETag"), headers.get("Last-Modified"), row["id"]),
        )
        if url_moved:
            # Same bytes at a new URL: worth recording, not a content change.
            log_change(conn, run_id, post_id, tender_id, f"file_url:{role}",
                       row["url"], url)
        return {"state": "same", "attachment_id": row["id"], "downloaded": True}

    # New content -> write a new version.
    version = (row["version"] + 1) if row else 1
    rel_dir = safe_name(tender_id)
    folder = C.FILES_DIR / rel_dir
    folder.mkdir(parents=True, exist_ok=True)
    fname = filename_from_url(url)
    leaf = (f"v{version}_{safe_name(role, 'file', 40)}_{fname}" if version > 1
            else f"{safe_name(role, 'file', 40)}_{fname}")
    (folder / leaf).write_bytes(data)
    local = f"{rel_dir}/{leaf}"              # stored relative: keeps the DB portable

    if row is not None:
        conn.execute("UPDATE attachments SET is_current=0 WHERE id=?", (row["id"],))

    attachment_id = conn.insert_returning_id(
        "INSERT INTO attachments(post_id, tender_id, role, url, filename, ext,"
        " content_sha256, size_bytes, etag, http_last_modified, local_path, version,"
        " is_current, parse_status, first_seen_at, last_seen_at, last_changed_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,'pending',?,?,?)",
        (post_id, tender_id, role, url, fname, ext_from_url(url), digest, len(data),
         headers.get("ETag"), headers.get("Last-Modified"), str(local), version,
         row["first_seen_at"] if row else ts, ts, ts),
    )
    state = "changed" if row is not None else "new"
    log_change(conn, run_id, post_id, tender_id, f"file:{role}",
               row["content_sha256"][:12] if row else None, digest[:12])
    return {"state": state, "attachment_id": attachment_id, "downloaded": True,
            "local_path": str(local), "version": version}


def sync_tender_attachments(conn, run_id: int, tender: dict, attachments: dict,
                            force: bool = False) -> dict:
    """Reconcile every downloadable attachment on one tender."""
    stats = {"checked": 0, "changed": 0, "new": 0, "errors": 0, "ids": []}
    for role, url in (attachments or {}).items():
        if role not in C.FILE_ROLES:
            continue                       # 'Buy This Tender' is a SAP link, not a file
        stats["checked"] += 1
        res = sync_attachment(conn, run_id, tender, role, url, force=force)
        if res["state"] == "new":
            stats["new"] += 1
        elif res["state"] == "changed":
            stats["changed"] += 1
        elif res["state"] == "error":
            stats["errors"] += 1
        if res.get("attachment_id") and res["state"] in ("new", "changed"):
            stats["ids"].append(res["attachment_id"])
    return stats
