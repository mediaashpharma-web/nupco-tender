"""Render a short markdown digest of the most recent run.

    python -m pipeline.report

Written to the GitHub Actions step summary after every scheduled run, so the
daily answer to "did anything move?" is visible without opening the database.
Leads with what is new or closing soon, and says nothing at all when nothing
changed -- a report that pads itself stops being read.
"""
from __future__ import annotations

import json
import sys

from . import db


def _rows(conn, sql, args=()):
    return [dict(r) for r in conn.execute(sql, args).fetchall()]


def _table(rows: list[dict], columns: list[tuple[str, str]]) -> list[str]:
    if not rows:
        return []
    out = ["| " + " | ".join(label for _, label in columns) + " |",
           "|" + "|".join("---" for _ in columns) + "|"]
    for r in rows:
        cells = []
        for key, _ in columns:
            v = r.get(key)
            cells.append("—" if v in (None, "") else str(v).replace("|", "\\|")[:70])
        out.append("| " + " | ".join(cells) + " |")
    return out


def build(conn) -> str:
    run = conn.execute(
        "SELECT * FROM run_log ORDER BY run_id DESC LIMIT 1").fetchone()
    if run is None:
        return "\n".join(["## Tender daily refresh", "", "No runs recorded yet."])
    run = dict(run)
    # Each source reports on itself: the workflow summarises after each step,
    # and a Jordan digest listing Saudi deadlines would bury its own news.
    jordan = str(run.get("mode") or "").startswith("joneps")
    source = "joneps" if jordan else "nupco"
    lines: list[str] = ["## " + ("Jordan (JONEPS)" if jordan else "Saudi Arabia (NUPCO)")]

    status = run.get("status", "?")
    icon = {"ok": "✅", "partial": "⚠️", "failed": "❌"}.get(status, "•")
    lines += ["",
              f"{icon} **{status}** — {run.get('tenders_seen', 0)} tenders seen, "
              f"{run.get('tenders_new', 0)} new, {run.get('tenders_changed', 0)} changed, "
              f"{run.get('files_changed', 0)} documents changed, "
              f"{run.get('items_parsed', 0)} line items parsed"]
    if jordan:
        try:
            notes = json.loads(run.get("notes") or "{}")
        except ValueError:
            notes = {}
        if notes.get("award_lines"):
            lines.append(f"{notes['award_lines']} priced award line(s) captured or updated, "
                         f"{notes.get('requests', '?')} requests to the portal")
        if notes.get("stopped_early"):
            lines.append("> Stopped at its time budget. Run it again to continue; finished "
                         "tenders are skipped.")
    if run.get("errors"):
        lines.append(f"> {run['errors']} error(s) during the run — see the log above.")

    new = _rows(conn,
                "SELECT tender_id, status_label, submission_ts, days_to_deadline,"
                " item_count FROM tenders WHERE first_seen_at >= ?"
                " AND COALESCE(source, 'nupco') = ?"
                " ORDER BY submission_ts LIMIT 60",
                (run.get("started_at") or "", source))
    if new:
        lines += ["", f"### {len(new)} new tender(s)"]
        lines += _table(new, [("tender_id", "Tender"), ("status_label", "Status"),
                              ("submission_ts", "Deadline"),
                              ("days_to_deadline", "Days left"), ("item_count", "Items")])

    changes = _rows(conn,
                    "SELECT tender_id, field, old_value, new_value FROM tender_history"
                    " WHERE run_id = ? AND change_kind = 'update'"
                    " ORDER BY tender_id LIMIT 40", (run.get("run_id"),))
    docs = [c for c in changes if str(c["field"]).startswith("file:")]
    fields = [c for c in changes if not str(c["field"]).startswith("file:")]

    if docs:
        # An amended item list is the most commercially interesting event on
        # the site: the requirement itself moved.
        lines += ["", f"### {len(docs)} document(s) republished"]
        lines += _table(docs, [("tender_id", "Tender"), ("field", "Document")])
    if fields:
        lines += ["", f"### {len(fields)} field change(s)"]
        lines += _table(fields, [("tender_id", "Tender"), ("field", "Field"),
                                 ("old_value", "From"), ("new_value", "To")])

    closing = _rows(conn,
                    "SELECT tender_id, status_label, submission_ts, days_to_deadline,"
                    " item_count FROM tenders WHERE category_guess = 'pharma'"
                    " AND days_to_deadline BETWEEN 0 AND 14"
                    " AND COALESCE(source, 'nupco') = ?"
                    " ORDER BY days_to_deadline LIMIT 60", (source,))
    if closing:
        lines += ["", f"### {len(closing)} pharma tender(s) closing within 14 days"]
        lines += _table(closing, [("tender_id", "Tender"), ("status_label", "Status"),
                                  ("submission_ts", "Deadline"),
                                  ("days_to_deadline", "Days left"),
                                  ("item_count", "Items")])

    if not (new or changes):
        lines += ["", "Nothing changed on the portal since the last run."]

    totals = conn.execute(
        "SELECT (SELECT COUNT(*) FROM tenders WHERE COALESCE(source,'nupco') = ?) AS tenders,"
        " (SELECT COUNT(*) FROM tender_items i JOIN tenders t ON t.post_id = i.post_id"
        "   WHERE COALESCE(t.source,'nupco') = ?) AS items,"
        " (SELECT COUNT(*) FROM awards WHERE source = ?) AS awards",
        (source, source, source)).fetchone()
    if totals:
        t = dict(totals)
        tail = f", {t['awards']:,} priced award lines" if jordan else ""
        lines += ["", f"_{'Jordan' if jordan else 'Saudi Arabia'} now holds {t['tenders']:,} "
                      f"tenders and {t['items']:,} line items{tail}._"]
    return "\n".join(lines)


def main() -> int:
    try:
        conn = db.connect()
    except Exception as e:                      # noqa: BLE001
        print(f"## NUPCO daily refresh\n\nCould not open the database: `{e}`")
        return 0                                # never fail the workflow on the report
    try:
        print(build(conn))
    except Exception as e:                      # noqa: BLE001
        print(f"## NUPCO daily refresh\n\nCould not build the report: `{e}`")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
