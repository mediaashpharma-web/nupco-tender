"""Central configuration for the NUPCO tender pipeline."""
import os
import tempfile
from pathlib import Path

# --- Paths -------------------------------------------------------------------
# ROOT is the Nupco project folder. Everything the pipeline writes lives under it.
ROOT = Path(os.environ.get("NUPCO_ROOT", Path(__file__).resolve().parent.parent))
DB_DIR = ROOT / "db"
LOG_DIR = ROOT / "logs"
DB_PATH = Path(os.environ.get("NUPCO_DB", DB_DIR / "nupco.db"))

# In the cloud we parse every document but keep none of them: the hash and the
# extracted line items are what matter, and 800MB of PDFs would cost more than
# the intelligence in them. Locally the archive is kept as before.
KEEP_FILES = os.environ.get("NUPCO_KEEP_FILES",
                            "0" if os.environ.get("DATABASE_URL") else "1") != "0"
FILES_DIR = Path(os.environ.get(
    "NUPCO_FILES_DIR",
    ROOT / "files" if KEEP_FILES else Path(tempfile.gettempdir()) / "nupco-files"))

for _d in (DB_DIR, FILES_DIR, LOG_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# --- Site --------------------------------------------------------------------
SITE = "https://www.nupco.com"
LIST_URL = f"{SITE}/tenders/tenders-list/"
SITEMAP_URL = f"{SITE}/tender-sitemap.xml"

# Elementor widget ids. These are stable per loop template; if NUPCO rebuilds the
# page these are the first things to re-check (see docs/NOTES.md).
GRID_ID = "3085e1f"
FILTER_PARAM = f"e-filter-{GRID_ID}-tender-status"
PAGE_PARAM = f"e-page-{GRID_ID}"

# Field -> Elementor data-id, on the loop card.
CARD_FIELDS = {
    "tender_id": "3317f29",
    "status_label": "d1dd940",
    "title_ar": "0e2104d",
    "title_en": "e400466",
    "submission_deadline": "7e0a2fb",
    "bid_opening": "f40fb4d",
}

STATUS_SLUGS = [
    "available-new", "available-updated", "always-available", "direct-purchase",
    "under-studying", "initial-results", "final-results", "cancelled", "hidden",
]

# A tender in one of these states no longer changes -- but only once we have
# actually captured it in that state. See run.py: we always re-scan the cheap
# list pages so we can catch a tender *entering* a terminal state.
TERMINAL_STATUSES = {"final-results", "cancelled"}

# Attachment roles seen on detail pages. "Buy This Tender" is an external SAP
# deep link, not a file -- never downloaded.
FILE_ROLES = [
    "Tender Item List",
    "Terms & Conditions",
    "Tender Preliminary Result",
    "Tender Final Result",
    "Additional Attachments",
    "Objection to the Results",
]
LINK_ROLES = ["Buy This Tender"]
ITEM_LIST_ROLE = "Tender Item List"

# --- HTTP --------------------------------------------------------------------
USER_AGENT = "Mozilla/5.0 (compatible; NupcoTenderPipeline/1.0)"
HTTP_TIMEOUT = 90
THROTTLE_SECONDS = 0.35   # be a polite citizen; robots.txt allows everything
MAX_RETRIES = 3
WORKERS = 6

# --- UI ----------------------------------------------------------------------
UI_HOST = "127.0.0.1"
UI_PORT = int(os.environ.get("NUPCO_UI_PORT", 8000))
