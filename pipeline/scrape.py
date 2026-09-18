"""Discovery and extraction for nupco.com tenders.

There is no JSON API. The tenders list is WordPress + Elementor Pro and every
control (status filter, free-text search, date range, pagination) is a GET
parameter on the same URL, server-rendered. So everything here is plain HTTP +
HTML parsing.

Two discovery sources:
  * the grid  -- 24 cards/page, ~133 tenders, carries status + dates cheaply
  * the sitemap -- 229 live tender pages, including ~97 that are `hidden` or
    untagged and never appear in the grid but still carry prices and results.
"""
from __future__ import annotations

import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime
from typing import Iterator

from bs4 import BeautifulSoup

from . import config as C

_last_request = [0.0]
_throttle_lock = threading.Lock()


# --- HTTP --------------------------------------------------------------------

def _throttle() -> None:
    """Global rate limit, shared across worker threads. Be a polite citizen."""
    with _throttle_lock:
        delta = time.time() - _last_request[0]
        if delta < C.THROTTLE_SECONDS:
            time.sleep(C.THROTTLE_SECONDS - delta)
        _last_request[0] = time.time()


def _safe_url(url: str) -> str:
    """NUPCO serves attachments under Arabic filenames; percent-encode safely."""
    parts = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((
        parts.scheme, parts.netloc,
        urllib.parse.quote(parts.path, safe="/%"),
        urllib.parse.quote(parts.query, safe="=&%?:/"),
        parts.fragment,
    ))


def http_get(url: str, params: dict | None = None, binary: bool = False):
    if params:
        url = url + ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
    last = None
    for attempt in range(C.MAX_RETRIES):
        _throttle()
        try:
            req = urllib.request.Request(
                _safe_url(url),
                headers={"User-Agent": C.USER_AGENT, "X-Requested-With": "fetch"},
            )
            with urllib.request.urlopen(req, timeout=C.HTTP_TIMEOUT) as r:
                data = r.read()
                return (data, dict(r.headers)) if binary else data.decode("utf-8", "ignore")
        except Exception as e:                      # noqa: BLE001 - retry anything transient
            last = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"GET failed after {C.MAX_RETRIES} tries: {url} ({last})")


def http_head(url: str) -> dict:
    """Cheap change probe. Returns {} when the server refuses HEAD."""
    _throttle()
    try:
        req = urllib.request.Request(_safe_url(url), method="HEAD",
                                     headers={"User-Agent": C.USER_AGENT})
        with urllib.request.urlopen(req, timeout=C.HTTP_TIMEOUT) as r:
            return {k.lower(): v for k, v in r.headers.items()}
    except Exception:                               # noqa: BLE001
        return {}


# --- helpers -----------------------------------------------------------------

_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")


def to_iso(value: str | None) -> str | None:
    """'Sun, 20/09/2026' -> '2026-09-20'."""
    if not value:
        return None
    m = _DATE_RE.search(value)
    if not m:
        return None
    d, mo, y = (int(x) for x in m.groups())
    try:
        return date(y, mo, d).isoformat()
    except ValueError:
        return None


def to_float(value) -> float | None:
    if value is None:
        return None
    s = re.sub(r"[^\d.]", "", str(value))
    try:
        return float(s) if s else None
    except ValueError:
        return None


def clean(text: str | None) -> str | None:
    if text is None:
        return None
    t = re.sub(r"\s+", " ", text).strip()
    return t or None


# Keyword heuristics. NUPCO has no category taxonomy at all, so this is an
# honest first-pass label, not ground truth -- the `category_guess` name is
# deliberate. Downstream drug-name harmonisation should override it.
# Stems are prefix-matched (leading \b only) so ENDODONTIC matches 'endodont'
# and PROSTHESIS matches 'prosthe'. Order matters: first rule to hit wins.
_CATEGORY_RULES = [
    ("dental",  r"\b(dental|dentist|endodont|orthodont|periodont|prosthodont|"
                r"amalgam|composite resin|gutta.?percha|k.?file)|الأسنان|أسنان"),
    ("pharma",  r"\b(drug|pharmac|medicin|tablet|capsule|injection|vial|ampoule|"
                r"syrup|suspension|insulin|vaccine|antibiot|antisept|antifung|antivir|"
                r"anesthet|anaesthet|analges|oncolog|chemotherap|infusion|heparin|"
                r"saline|dextrose|sodium chloride|ointment|suppositor|inhaler|"
                r"eye drop|iv fluid)|دواء|أدوية|دوائية|لقاح|مستحضرات"),
    ("lab",     r"\b(laborator|reagent|assay|pcr|elisa|centrifuge|analyz|analys|"
                r"microbiolog|histopath|specimen|diagnostic|test kit|culture medium|"
                r"pipette|cuvette)|مختبر|مختبرات|تحاليل"),
    ("device",  r"\b(device|equipment|instrument|surgical|endoscop|stent|catheter|"
                r"implant|monitor|ventilator|imaging|radiolog|ultrasound|mri|ct scan|"
                r"disposable|consumable|supplies|prosthe|syringe|needle|glove|suture|"
                r"dressing|bandage)|أجهزة|جهاز|مستلزمات|جراح"),
    ("service", r"\b(service|maintenance|installation|fit.?out|construction|training|"
                r"consultanc|logistic|warehous|transport|delivery cost|warranty cost)"
                r"|خدمات|صيانة|تشغيل|توريد وتركيب"),
]


def guess_category(*texts: str | None) -> str:
    blob = " ".join(t for t in texts if t).lower()
    if not blob:
        return "other"
    for label, pattern in _CATEGORY_RULES:
        if re.search(pattern, blob, re.IGNORECASE):
            return label
    return "other"


# NUPCO's own material grouping, recovered from the data: the first two digits
# of the 13-digit SAP code partition the catalogue far more reliably than any
# keyword rule. 51 is pharmaceuticals, 41 diagnostics, 42 devices/dental, and
# so on. Counts at time of writing, across 130k parsed lines:
#   42 -> 68,665   41 -> 42,826   40 -> 9,850   51 -> 5,962   46 -> 1,002
CODE_GROUPS = {
    "51": "pharma",      # drugs, vaccines, IV solutions
    "41": "lab",         # reagents, assays, culture media, rapid tests
    "45": "lab",         # histopathology consumables
    "42": "device",      # medical + dental devices and consumables
    "40": "equipment",   # machines, anaesthesia, sterilisers, their accessories
    "46": "consumable",  # general consumables
    "43": "other", "44": "other", "47": "other",
    "48": "other", "49": "other", "50": "other", "53": "other",
}


def code_group(nupco_code: str | None) -> str | None:
    """The raw two-digit SAP material group. A fact, not a guess."""
    code = re.sub(r"\D", "", nupco_code or "")
    return code[:2] if len(code) >= 10 else None


def classify(nupco_code: str | None, description: str | None) -> str:
    """Blend the structural signal with the keyword one.

    The SAP group leads because it is NUPCO's own classification. Keywords only
    override it where they are strictly more specific than the group: dental
    sits inside the device group, and topical drugs inside it too. Delivery and
    warranty lines are service whatever they are coded as.
    """
    keyword = guess_category(description)
    if keyword == "service":
        return "service"
    group = CODE_GROUPS.get(code_group(nupco_code) or "")
    if group is None:
        return keyword
    if group == "device" and keyword in ("dental", "pharma"):
        return keyword
    if group == "lab" and keyword == "dental":
        return keyword
    return group


_ACCESSORY_RE = re.compile(
    r"related to|warranty cost|delivery cost|installation cost|training cost|spare part",
    re.IGNORECASE,
)


def is_accessory(description: str | None) -> int:
    return 1 if description and _ACCESSORY_RE.search(description) else 0


def derive(rec: dict) -> dict:
    """Add computed columns to a tender record."""
    tid = rec.get("tender_id") or ""
    m = re.match(r"([A-Z]{2,4})(\d+)[-/](\d{2,4})", tid.strip())
    if m:
        rec["tender_kind"] = m.group(1)
        rec["tender_seq"] = int(m.group(2))
        yr = int(m.group(3))
        rec["tender_year"] = yr if yr > 100 else 2000 + yr
    rec["opening_ts"] = to_iso(rec.get("opening_date"))
    rec["submission_ts"] = to_iso(rec.get("submission_deadline"))
    rec["bid_opening_ts"] = to_iso(rec.get("bid_opening"))
    if rec.get("submission_ts"):
        rec["days_to_deadline"] = (date.fromisoformat(rec["submission_ts"]) - date.today()).days
    rec["category_guess"] = guess_category(rec.get("title_en"), rec.get("title_ar"))
    slugs = rec.get("status_slugs") or []
    rec["is_terminal"] = 1 if set(slugs) & C.TERMINAL_STATUSES else 0
    if rec.get("url"):
        rec["slug"] = urllib.parse.unquote(rec["url"].rstrip("/").rsplit("/", 1)[-1])
    return rec


# --- grid --------------------------------------------------------------------

def parse_cards(html: str) -> list[dict]:
    grid = BeautifulSoup(html, "lxml").select_one("#tender-grid")
    out = []
    for card in (grid.select(".e-loop-item") if grid else []):
        classes = " ".join(card.get("class", []))
        rec = {}
        for field, wid in C.CARD_FIELDS.items():
            el = card.select_one(f'[data-id="{wid}"]')
            rec[field] = clean(el.get_text(" ", strip=True)) if el else None
        m = re.search(r"post-(\d+)", classes)
        rec["post_id"] = int(m.group(1)) if m else None
        rec["status_slugs"] = re.findall(r"tender-status-([a-z0-9\-]+)", classes)
        link = card.select_one("a[href]")
        rec["url"] = link["href"] if link else None
        rec["is_listed"] = 1
        rec["discovered_via"] = "grid"
        if rec["post_id"]:
            out.append(rec)
    return out


def discover_grid(max_pages: int = 60) -> list[dict]:
    """Walk the loop grid. 24 cards/page; an empty grid means we're past the end."""
    seen, out, page = set(), [], 1
    while page <= max_pages:
        params = {} if page == 1 else {C.PAGE_PARAM: page}
        cards = parse_cards(http_get(C.LIST_URL, params))
        if not cards:
            break
        fresh = [c for c in cards if c["post_id"] not in seen]
        if not fresh:                      # defensive: server repeated a page
            break
        for c in fresh:
            seen.add(c["post_id"])
        out.extend(fresh)
        page += 1
    return out


def discover_sitemap() -> dict[str, str | None]:
    """{tender_url: lastmod}. lastmod is a free change signal."""
    xml = http_get(C.SITEMAP_URL)
    out = {}
    for block in re.findall(r"<url>(.*?)</url>", xml, re.S):
        loc = re.search(r"<loc>([^<]+)</loc>", block)
        mod = re.search(r"<lastmod>([^<]+)</lastmod>", block)
        if loc and "/tender/" in loc.group(1):
            out[loc.group(1).strip()] = mod.group(1).strip() if mod else None
    return out


# --- detail page -------------------------------------------------------------

def parse_detail(html: str, url: str | None = None) -> dict:
    soup = BeautifulSoup(html, "lxml")
    rec: dict = {}

    body_classes = " ".join(soup.body.get("class", [])) if soup.body else ""
    m = re.search(r"postid-(\d+)", body_classes)
    if m:
        rec["post_id"] = int(m.group(1))
    rec["lang"] = (soup.html.get("lang") or "en")[:2] if soup.html else "en"

    headings = [clean(h.get_text(" ", strip=True)) or ""
                for h in soup.select('[data-widget_type="heading.default"]')]

    # Detail pages render "<Label>:" immediately followed by its value.
    labels = {
        "Opening Date": "opening_date",
        "Submission Deadline": "submission_deadline",
        "Bid Opening": "bid_opening",
        "Tender Booklet Price": "booklet_price_sar",
    }
    for i, h in enumerate(headings):
        key = labels.get(h.rstrip(":").strip())
        if key and i + 1 < len(headings):
            rec[key] = headings[i + 1]
    if "booklet_price_sar" in rec:
        rec["booklet_price_sar"] = to_float(rec["booklet_price_sar"])

    # Tender ID sits right after the literal heading "Tender ID"; the status
    # label is the heading after that.
    if "Tender ID" in headings:
        i = headings.index("Tender ID")
        rec["tender_id"] = headings[i + 1] if i + 1 < len(headings) else None
        if i + 2 < len(headings):
            rec["status_label"] = headings[i + 2]

    # Titles: the template renders both an en-dash and hyphen variant and hides
    # one per language. Take the two longest distinct headings after the status.
    titles = [h for h in headings if len(h) > 25]
    if titles:
        rec.setdefault("title_ar", titles[0])
        rec.setdefault("title_en", titles[1] if len(titles) > 1 else titles[0])

    attachments = {}
    for a in soup.select("a.elementor-button"):
        role = clean(a.get_text(" ", strip=True))
        href = a.get("href")
        if role and href:
            attachments[role] = href
    rec["attachments"] = attachments
    rec["buy_url"] = attachments.get("Buy This Tender")
    rec["attachment_count"] = sum(1 for r in attachments if r in C.FILE_ROLES)
    if url:
        rec["url"] = url
    return rec


def fetch_detail(url: str) -> dict:
    return parse_detail(http_get(url), url=url)


def status_slugs_from_label(label: str | None) -> list[str]:
    """Detail pages give a human label ('Final Results'); grid gives slugs."""
    if not label:
        return []
    return [re.sub(r"[^a-z0-9]+", "-", part.strip().lower()).strip("-")
            for part in label.split(",") if part.strip()]


def iter_chunks(seq, size: int) -> Iterator[list]:
    buf = []
    for item in seq:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf
