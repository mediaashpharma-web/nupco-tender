"""Test suite for the NUPCO pipeline.

    python -m tests.test_pipeline          (from the Nupco folder)

Uses stdlib unittest only. Unit tests run against a throwaway database; the
data-sanity tests run against db/nupco.db and skip themselves if it is absent.
No network access is required.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline import db, files, items, scrape           # noqa: E402
from pipeline import config as C                        # noqa: E402


# --- helpers -----------------------------------------------------------------

def fresh_db() -> sqlite3.Connection:
    conn = db.connect(":memory:")
    db.init(conn)
    return conn


def tender(**over) -> dict:
    rec = {"post_id": 1, "tender_id": "NPT0001/26", "url": "https://x/tender/a/",
           "title_en": "OPEN FRAMEWORK AGREEMENT FOR INSULIN",
           "status_label": "Available / New", "status_slugs": ["available-new"],
           "opening_date": "Wed, 16/09/2026", "submission_deadline": "Sun, 20/09/2026",
           "bid_opening": "Sun, 20/09/2026", "booklet_price_sar": 1000.0}
    rec.update(over)
    return scrape.derive(rec)


# --- derived fields ----------------------------------------------------------

class TestDerive(unittest.TestCase):
    def test_date_parsing(self):
        self.assertEqual(scrape.to_iso("Sun, 20/09/2026"), "2026-09-20")
        self.assertEqual(scrape.to_iso("16/09/2026"), "2026-09-16")
        self.assertIsNone(scrape.to_iso("Always available"))
        self.assertIsNone(scrape.to_iso(None))
        self.assertIsNone(scrape.to_iso("32/13/2026"))       # invalid date

    def test_tender_id_decomposition(self):
        r = tender(tender_id="NDP0864/26")
        self.assertEqual(r["tender_kind"], "NDP")
        self.assertEqual(r["tender_seq"], 864)
        self.assertEqual(r["tender_year"], 2026)
        # NUPCO writes the id both ways; both must decompose identically.
        self.assertEqual(tender(tender_id="NDP0835-26")["tender_seq"], 835)

    def test_terminal_flag(self):
        self.assertEqual(tender(status_slugs=["final-results"])["is_terminal"], 1)
        self.assertEqual(tender(status_slugs=["cancelled"])["is_terminal"], 1)
        self.assertEqual(tender(status_slugs=["available-new"])["is_terminal"], 0)

    def test_category_guess(self):
        cases = {
            "AMOXICILLIN 500MG CAPSULE": "pharma",
            "K-FILE ENDODONTIC FLEX R SIZE 45": "dental",
            "KIT LEGIONELLA PCR AND DNA EXTRACTION": "lab",
            "STENT SET, BILIARY, PRELOADED": "device",
            "MAIN AREA DELIVERY COST RELATED TO SCALE": "service",
        }
        for text, expected in cases.items():
            self.assertEqual(scrape.guess_category(text), expected, text)

    def test_accessory_detection(self):
        self.assertEqual(scrape.is_accessory("2 ML ADAPTER RELATED TO CENTRIFUGE"), 1)
        self.assertEqual(scrape.is_accessory("3rd YEAR WARRANTY COST RELATED TO X"), 1)
        self.assertEqual(scrape.is_accessory("CENTRIFUGE FLOOR"), 0)

    def test_numbers(self):
        self.assertEqual(scrape.to_float("13,800"), 13800.0)
        self.assertEqual(scrape.to_float("1.00"), 1.0)
        self.assertIsNone(scrape.to_float("SAR"))


# --- change tracking ---------------------------------------------------------

class TestUpsertAndHistory(unittest.TestCase):
    def setUp(self):
        self.conn = fresh_db()

    def test_new_then_same_is_idempotent(self):
        self.assertEqual(db.upsert_tender(self.conn, 1, tender()), "new")
        self.assertEqual(db.upsert_tender(self.conn, 1, tender()), "same")
        self.assertEqual(db.upsert_tender(self.conn, 1, tender()), "same")
        n = self.conn.execute("SELECT COUNT(*) FROM tenders").fetchone()[0]
        self.assertEqual(n, 1, "re-running must not duplicate rows")

    def test_deadline_change_is_recorded(self):
        db.upsert_tender(self.conn, 1, tender())
        state = db.upsert_tender(self.conn, 2,
                                 tender(submission_deadline="Thu, 01/10/2026"))
        self.assertEqual(state, "changed")
        row = self.conn.execute(
            "SELECT * FROM tender_history WHERE field='submission_deadline'").fetchone()
        self.assertIsNotNone(row, "a moved deadline must appear in the change feed")
        self.assertEqual(row["new_value"], "Thu, 01/10/2026")

    def test_status_transition_into_terminal(self):
        db.upsert_tender(self.conn, 1, tender())
        db.upsert_tender(self.conn, 2, tender(status_label="Final Results",
                                              status_slugs=["final-results"]))
        row = self.conn.execute("SELECT is_terminal FROM tenders").fetchone()
        self.assertEqual(row["is_terminal"], 1)
        fields = {r["field"] for r in
                  self.conn.execute("SELECT field FROM tender_history")}
        self.assertIn("status_label", fields)

    def test_float_int_noise_is_not_a_change(self):
        db.upsert_tender(self.conn, 1, tender(booklet_price_sar=1000.0))
        state = db.upsert_tender(self.conn, 2, tender(booklet_price_sar=1000))
        self.assertEqual(state, "same", "1000 and 1000.0 must not look like a change")

    def test_delist_never_deletes(self):
        db.upsert_tender(self.conn, 1, tender())
        db.mark_unseen_as_delisted(self.conn, 2, seen_post_ids=[])
        row = self.conn.execute("SELECT is_listed FROM tenders").fetchone()
        self.assertEqual(row["is_listed"], 0)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM tenders").fetchone()[0], 1,
                         "a tender leaving the grid must never be deleted")
        kinds = {r["change_kind"] for r in
                 self.conn.execute("SELECT change_kind FROM tender_history")}
        self.assertIn("delist", kinds)


# --- attachment versioning ---------------------------------------------------

class TestAttachmentVersioning(unittest.TestCase):
    """Exercises the version logic directly, without touching the network."""

    def setUp(self):
        self.conn = fresh_db()
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def _insert(self, sha, version=1, url="https://x/a.pdf"):
        self.conn.execute(
            "INSERT INTO attachments(post_id, tender_id, role, url, filename, ext,"
            " content_sha256, size_bytes, local_path, version, is_current,"
            " parse_status, first_seen_at, last_seen_at, last_changed_at)"
            " VALUES (1,'T1','Tender Item List',?,'a.pdf','pdf',?,10,'/tmp/a.pdf',?,1,"
            "'parsed',?,?,?)",
            (url, sha, version, db.now(), db.now(), db.now()))
        self.conn.commit()

    def test_same_hash_is_not_a_new_version(self):
        self._insert("abc123")
        row = self.conn.execute("SELECT * FROM attachments WHERE is_current=1").fetchone()
        self.assertEqual(row["version"], 1)
        self.assertEqual(files.sha256(b"hello"),
                         "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")

    def test_head_unchanged_requires_a_positive_signal(self):
        self._insert("abc123")
        row = self.conn.execute("SELECT * FROM attachments").fetchone()
        self.assertFalse(files._head_unchanged(row, {}),
                         "no headers must never count as 'unchanged'")
        self.assertFalse(files._head_unchanged(row, {"content-length": "99"}))
        self.assertTrue(files._head_unchanged(row, {"content-length": "10"}))

    def test_windows_safe_filenames(self):
        self.assertNotIn("/", files.safe_name("NDP0864/26"))
        self.assertNotIn(":", files.safe_name("a:b*c?d"))
        # Arabic filenames must survive -- NTFS handles Unicode fine.
        self.assertIn("منافسة", files.safe_name("قالب-منافسة.pdf"))

    def test_ext_detection_ignores_query_strings(self):
        self.assertEqual(files.ext_from_url("https://x/y/NDP0864-26.xlsx"), "xlsx")
        self.assertEqual(files.ext_from_url("https://x/y/a.PDF"), "pdf")


# --- parsers -----------------------------------------------------------------

class TestParsers(unittest.TestCase):
    def test_header_mapping_across_all_published_shapes(self):
        shapes = [
            ["SN", "ITEM NO", "NUPCO CODE", "ITEM DESCRIPTION", "UOM", "QTY", "GROUPS"],
            ["ITEM NO", "SRM CODE", "ITEM DESCRIPTION", "GROUP"],
            ["SN", "NUPCO CODE", "DESCRIPTION", "UOM", "ITEMIZED", "INITIAL QTY"],
            ["SN", "Generic Code", "SAP Generic Name", "Requested Quantity", "INUPCO"],
        ]
        for cols in shapes:
            m = items.map_headers(cols)
            self.assertTrue(m, f"header row not recognised: {cols}")
            self.assertIn("description", m.values(), cols)
            self.assertIn("nupco_code", m.values(), cols)

    def test_borderless_row_split(self):
        row = items._split_row(
            "1 4220341207500 STENT SET, BILIARY, PRELOADED PLASTIC STENT, "
            "ASSORTED SIZES EACH ITEMIZED 11376")
        self.assertEqual(row["sn"], "1")
        self.assertEqual(row["nupco_code"], "4220341207500")
        self.assertEqual(row["uom"], "EACH")
        self.assertEqual(row["itemized"], "ITEMIZED")
        self.assertEqual(row["qty_raw"], "11376")
        self.assertTrue(row["description"].startswith("STENT SET"))
        self.assertNotIn("ITEMIZED", row["description"])
        self.assertNotIn("11376", row["description"])

    def test_row_without_a_code_is_not_a_row(self):
        self.assertIsNone(items._split_row("SIZES"))
        self.assertIsNone(items._split_row("Page 3 of 46"))

    def test_qty_only_stripped_from_the_end(self):
        row = items._split_row("5 4110391102400 5 - 6.5 ML ADAPTER RELATED TO X EACH 12")
        self.assertEqual(row["qty_raw"], "12")
        self.assertIn("ML ADAPTER", row["description"])

    def test_magic_byte_sniffing_beats_the_extension(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "mislabelled.xlsx"
            p.write_bytes(b"%PDF-1.7\nrest")
            self.assertEqual(items.sniff_kind(p), "pdf")
            p2 = Path(d) / "a.pdf"
            p2.write_bytes(b"PK\x03\x04rest")
            self.assertEqual(items.sniff_kind(p2), "xlsx")

    def test_store_items_replaces_rather_than_appends(self):
        conn = fresh_db()
        db.upsert_tender(conn, 1, tender())
        rows = [{"nupco_code": "4110000000001", "description": "INSULIN GLARGINE",
                 "qty": 10.0, "category_guess": "pharma"}]
        items.store_items(conn, 1, 1, "NPT0001/26", rows)
        items.store_items(conn, 1, 1, "NPT0001/26", rows)
        n = conn.execute("SELECT COUNT(*) FROM tender_items").fetchone()[0]
        self.assertEqual(n, 1, "re-parsing a file must not duplicate its items")


# --- search ------------------------------------------------------------------

class TestSearch(unittest.TestCase):
    def test_fts_query_is_injection_safe(self):
        """Hostile input must never reach the engine as syntax.

        Runs against whichever engine is active, because the two full-text
        dialects fail in completely different ways.
        """
        sys.path.insert(0, str(ROOT / "api"))
        import app as server

        if db.IS_POSTGRES:
            conn = db.connect()
            db.init(conn)
            probe_sql = ("SELECT id FROM tender_items"
                         " WHERE search_vector @@ to_tsquery('english', ?)")
        else:
            conn = fresh_db()
            db.upsert_tender(conn, 1, tender())
            items.store_items(conn, 1, 1, "NPT0001/26", [
                {"nupco_code": "4110000000001", "description": "INSULIN GLARGINE 100IU",
                 "qty": 5.0, "category_guess": "pharma"}])
            conn.commit()
            probe_sql = "SELECT rowid FROM items_fts WHERE items_fts MATCH ?"

        for probe in ['insulin', 'INSULIN GLAR', '"', 'a AND OR b', 'NEAR(', '*',
                      "'; DROP TABLE tenders;--", '4110000000001', '']:
            q = server.fts_query(probe)
            if not q:
                continue
            with self.subTest(probe=probe):
                try:
                    conn.execute(probe_sql, (q,)).fetchall()   # must not raise
                except Exception:
                    conn.rollback()
                    raise

        before = conn.execute("SELECT COUNT(*) AS n FROM tenders").fetchone()
        before = before["n"] if isinstance(before, dict) else before[0]
        conn.execute(probe_sql, (server.fts_query("insulin"),)).fetchall()
        after = conn.execute("SELECT COUNT(*) AS n FROM tenders").fetchone()
        after = after["n"] if isinstance(after, dict) else after[0]
        self.assertEqual(before, after, "search must never mutate data")


# --- real data sanity --------------------------------------------------------

class TestRealDatabase(unittest.TestCase):
    """Skipped automatically until the pipeline has produced a database."""

    @classmethod
    def setUpClass(cls):
        # A bare file is not a database: connecting to a missing path creates an
        # empty one, so check for actual tables rather than mere existence.
        if not db.IS_POSTGRES and not C.DB_PATH.exists():
            raise unittest.SkipTest(f"no database at {C.DB_PATH}")
        cls.conn = db.connect(None if db.IS_POSTGRES else C.DB_PATH)
        try:
            row = cls.conn.execute("SELECT COUNT(*) AS n FROM tenders").fetchone()
        except Exception as e:                  # noqa: BLE001
            raise unittest.SkipTest(f"database not populated yet ({e})")
        n = row["n"] if isinstance(row, dict) else row[0]
        if n < 50:
            # A handful of rows means a --limit run or a fresh cloud database.
            # These are whole-dataset sanity checks; against a stub they would
            # only ever produce noise.
            raise unittest.SkipTest(f"only {n} tenders: looks like a partial load")

    def q(self, sql, *a):
        """First column of the first row, on either engine.

        SQLite hands back a tuple-like Row; Postgres a dict. Tests should not
        have to care which.
        """
        row = self.conn.execute(sql, a).fetchone()
        if row is None:
            return None
        # sqlite3.Row also has .keys(), so that is not a safe discriminator;
        # only psycopg2's RealDictRow is an actual dict.
        return list(row.values())[0] if isinstance(row, dict) else row[0]

    def test_tenders_were_loaded(self):
        self.assertGreater(self.q("SELECT COUNT(*) FROM tenders"), 100)

    def test_every_tender_has_an_id_and_url(self):
        self.assertEqual(self.q(
            "SELECT COUNT(*) FROM tenders WHERE tender_id IS NULL OR url IS NULL"), 0)

    def test_post_ids_are_unique(self):
        self.assertEqual(
            self.q("SELECT COUNT(*) FROM tenders"),
            self.q("SELECT COUNT(DISTINCT post_id) FROM tenders"))

    def test_dates_are_iso_normalised(self):
        if db.IS_POSTGRES:
            bad = self.q("SELECT COUNT(*) FROM tenders WHERE submission_ts IS NOT NULL"
                         " AND submission_ts !~ ?", r"^\d{4}-\d{2}-\d{2}$")
        else:
            bad = self.q("SELECT COUNT(*) FROM tenders WHERE submission_ts IS NOT NULL"
                         " AND submission_ts NOT GLOB"
                         " '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'")
        self.assertEqual(bad, 0)

    def test_only_one_current_version_per_attachment_role(self):
        dupes = self.q(
            "SELECT COUNT(*) FROM (SELECT post_id, role FROM attachments"
            " WHERE is_current=1 GROUP BY post_id, role HAVING COUNT(*) > 1)")
        self.assertEqual(dupes, 0)

    def test_parsed_items_have_codes_and_descriptions(self):
        total = self.q("SELECT COUNT(*) FROM tender_items")
        if total == 0:
            self.skipTest("no items parsed yet")
        missing = self.q("SELECT COUNT(*) FROM tender_items"
                         " WHERE description IS NULL OR description = ''")
        self.assertEqual(missing, 0)
        coded = self.q("SELECT COUNT(*) FROM tender_items WHERE nupco_code IS NOT NULL")
        self.assertGreater(coded / total, 0.9,
                           "over 90% of line items should carry a SAP code")

    def test_no_item_leaks_a_header_row(self):
        leaked = self.q(
            "SELECT COUNT(*) FROM tender_items WHERE upper(description) IN"
            " ('DESCRIPTION','ITEM DESCRIPTION','SAP GENERIC NAME','ITEM NAME')")
        self.assertEqual(leaked, 0)

    def test_search_finds_a_known_term(self):
        """The engines index differently; both must still find a real drug."""
        if self.q("SELECT COUNT(*) FROM tender_items") == 0:
            self.skipTest("no items parsed yet")
        sys.path.insert(0, str(ROOT / "api"))
        import app as server
        term = server.fts_query("catheter")
        if db.IS_POSTGRES:
            n = self.q("SELECT COUNT(*) FROM tender_items"
                       " WHERE search_vector @@ to_tsquery('english', ?)", term)
        else:
            n = self.q("SELECT COUNT(*) FROM items_fts WHERE items_fts MATCH ?", term)
        self.assertGreater(n, 0, "full-text search returned nothing for 'catheter'")

    def test_quantities_are_sane(self):
        negatives = self.q("SELECT COUNT(*) FROM tender_items WHERE qty < 0")
        self.assertEqual(negatives, 0)

    def test_fts_is_in_sync_with_the_items_table(self):
        if db.IS_POSTGRES:
            # Postgres uses a generated tsvector column, which cannot drift out
            # of step with its row the way an external FTS table can.
            self.skipTest("generated column; drift is impossible by construction")
        if self.q("SELECT COUNT(*) FROM tender_items") == 0:
            self.skipTest("no items parsed yet")
        self.assertEqual(self.q("SELECT COUNT(*) FROM items_fts"),
                         self.q("SELECT COUNT(*) FROM tender_items"))

    def test_local_files_exist_on_disk(self):
        if not C.KEEP_FILES:
            self.skipTest("document archive disabled (cloud mode)")
        rows = self.conn.execute(
            "SELECT local_path FROM attachments WHERE is_current=1 LIMIT 40").fetchall()
        if not rows:
            self.skipTest("no attachments yet")
        missing = [r["local_path"] for r in rows
                   if not (files.resolve(r["local_path"]) or Path("/x")).exists()]
        self.assertFalse(missing, f"recorded but missing on disk: {missing[:3]}")

    def test_grid_and_sitemap_coverage(self):
        listed = self.q("SELECT COUNT(*) FROM tenders WHERE is_listed=1")
        total = self.q("SELECT COUNT(*) FROM tenders")
        self.assertGreater(listed, 50, "the grid should contribute ~133 tenders")
        self.assertGreater(total, listed,
                           "the sitemap should contribute tenders the grid hides")


if __name__ == "__main__":
    unittest.main(verbosity=2)
