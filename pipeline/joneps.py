"""JONEPS -- Jordan's e-procurement portal -- medicine tenders and awards.

    python -m pipeline.joneps backfill  [--years 2023,2024] [--max-minutes 300]
    python -m pipeline.joneps incremental

What it collects, per medicine tender:
  * the tender itself: number, title, buyer, method, therapeutic sub-category,
    publication date, deadline, status;
  * every requested drug line, from the portal's own JSON API: INN name in
    English and Arabic, strength and form, Jordan RDL code, UNSPSC code,
    quantity, and the quantity each hospital asked for;
  * for final-awarded tenders, the award: winning supplier, brand, manufacturer,
    country of origin, pack, price, total value, free goods, and the reason.

How the site works (docs/SITE-NOTES-JONEPS.md has the detail):
  * a Java app. The tender list is a POSTed search form with a CSRF token and
    a JSESSIONID; medicines are searchTendTypeCd1=EP0016, and the server will
    return 100 rows a page although its UI offers at most 20.
  * drug lines come from GET /ep/tender/MDgoods.do, JSON, pageSize honoured.
  * awards are server-rendered HTML at /ep/evaw/selectDetailAwardDecsn.do,
    reached through the award tab. Initial awards and bid-opening results are
    not public (both 404); only final awards publish prices.

Design, mostly lessons from NUPCO:
  * Resumable from the start. Every tender commits on its own, and a backfill
    skips tenders already fetched, so a killed run loses one tender, not the
    run. --max-minutes stops cleanly inside a CI job's time limit.
  * No archive needed. Drug lines are JSON and awards are HTML parsed on the
    spot; each is hashed, and only a changed hash writes anything.
  * Polite. One request at a time, a fixed pause between them, backoff on
    errors. No robots.txt exists; that is not a licence to hammer a
    government portal.
"""
from __future__ import annotations

import argparse
import hashlib
import http.cookiejar
import json
import logging
import os
import re
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from datetime import date

from . import db
from .scrape import CODE_GROUPS

log = logging.getLogger("nupco.joneps")

BASE = "https://joneps.gov.jo"
SOURCE, COUNTRY = "joneps", "JO"
USER_AGENT = "Mozilla/5.0 (compatible; TenderResearchPipeline/1.0)"
THROTTLE = float(os.environ.get("JONEPS_THROTTLE", "1.0"))    # seconds between requests
FIRST_YEAR = 2018                                              # earliest year the portal offers

MEDICINES = "EP0016"          # searchTendTypeCd1: the Medicines tender type
MEDICINE_CATEG = "EP1313"     # tendCategCd a medicine tender carries
LIST_PAGE_SIZE = 100

# Tender keys live alongside NUPCO's WordPress post ids in one BIGINT column.
# NUPCO's are small; offsetting JONEPS by 10^12 keeps the two ranges apart and
# makes the key a pure function of (tendNo, tendSeq), so every run agrees.
ID_BASE = 10 ** 12

# searchTendStatusCd -> (label, terminal). "Opened" means the BIDS were opened
# -- every such tender's deadline has passed. A tender still taking bids carries
# no status at all; tender_record() labels those from the deadline.
STATUSES = {
    "Opened":          ("Bids opened", False),
    "Initial_Awarded": ("Initially awarded", False),
    "Final_Awarded":   ("Awarded", True),
}

# searchTendStatCd: a second, independent status axis -- the tender's
# lifecycle -- that carries what the award axis cannot, above all cancellation.
# Amended-invitation states are left out: an amended tender is still open.
LIFECYCLE = {
    "EP0034": ("Cancelled", True),
    "EP0035": ("Re-tendered", True),
    "EP0854": ("Frozen", False),
    "EP0561": ("Financial bids opened", False),
    "EP0560": ("Technical bids opened", False),
}

# When a tender carries several labels the most decisive one wins: an award
# outranks a cancellation of an earlier round, which outranks being open.
STATUS_PRECEDENCE = ["Final_Awarded", "EP0034", "EP0035", "Initial_Awarded",
                     "EP0854", "EP0561", "EP0560", "Opened"]

# searchTendTypeCd2 when the type is Medicines: the portal's therapeutic classes.
# Condensed from the portal's own English labels (switch it to English with
# /um/setLocale.do?lang=en), which run to eighty characters -- checked against
# them one by one, not translated from the Arabic.
SUBCATEGORIES = {
    "EP0401": "Cardiovascular",
    "EP0402": "Antibiotics",
    "EP0403": "Respiratory & ENT",
    "EP0404": "Gastrointestinal",
    "EP0405": "Endocrine, obstetric & genitourinary",
    "EP0406": "Nervous & musculoskeletal system",
    "EP0407": "Ophthalmic",
    "EP0408": "Immunology & oncology",
    "EP0409": "Blood & nutrition",
    "EP0410": "Vaccines, sera & blood products",
    "EP0411": "Dermatology",
    "EP0412": "Anaesthesia",
    "EP0413": "HIV",
    "EP0414": "Diagnostic agents",
    "EP0415": "Smoking cessation",
    "EP0416": "Family planning",
    "EP0417": "Infant formula",
}

# searchTendMthodCd. Taken from the portal's English option labels, not
# inferred: an earlier version guessed these and got three of four wrong --
# "EP0022" is pre-qualification, not a limited tender -- which would have shown
# a confidently wrong method on every one of those tenders.
METHODS = {
    "EP0021": "Open competitive bidding, one envelope",
    "EP0024": "Open competitive bidding, two envelopes",
    "EP0022": "Open tendering with pre-qualification",
    "EP0023": "Pre-qualification, two stages",
    "EP5021": "Open tendering with expression of interest",
    "EP0025": "Limited bidding, one envelope",
    "EP0026": "Limited bidding, two envelopes",
    "EP0027": "Single source (direct)",
    "EP1122": "Individual consultant services",
}


# --- keys and small parsers ----------------------------------------------------

def post_id_for(tend_no: str, tend_seq: str) -> int:
    return ID_BASE + int(tend_no) * 100 + int(tend_seq or 0)


def tender_id_for(tend_no: str, tend_seq: str) -> str:
    return f"{tend_no}-{tend_seq}"


def _iso(dmy: str | None) -> str | None:
    """'23/09/2026' or '23/09/2026 16:00' -> '2026-09-23'."""
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", dmy or "")
    return f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}" if m else None


def _num(text: str | None) -> float | None:
    m = re.search(r"-?[\d,]*\.?\d+", (text or "").replace("٫", "."))
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except ValueError:
        return None


def _bracket(text: str | None) -> str | None:
    """'1.775 [JOD]' -> 'JOD'; '108/1995 [مسجل]' -> 'مسجل'."""
    m = re.search(r"\[\s*([^\]]+?)\s*\]", text or "")
    return m.group(1) if m else None


def rdl(code: str | None) -> str | None:
    """Jordan's RDL code in one canonical form. The drug-line API writes
    '08-030500-025', the award page '08030500025'; left as published, the
    award for a drug could never be joined back to the drug itself."""
    c = re.sub(r"[\s\-]", "", code or "")
    return c or None


def _clean(html: str | None) -> str:
    text = re.sub(r"<br\s*/?>", " ", html or "", flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&").replace("&quot;", '"')
    return re.sub(r"\s+", " ", text).strip()


def _hash(obj) -> str:
    return hashlib.sha256(json.dumps(obj, ensure_ascii=False, sort_keys=True,
                                     default=str).encode()).hexdigest()


# --- HTTP -------------------------------------------------------------------------

class Client:
    """One polite session: cookie jar, CSRF token, throttle, retries."""

    def __init__(self, throttle: float = THROTTLE):
        self.throttle = throttle
        self.requests = 0
        self._last = 0.0
        self._token: str | None = None
        self._jar = http.cookiejar.CookieJar()
        self._op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self._jar))
        self._op.addheaders = [("User-Agent", USER_AGENT)]

    def _pace(self):
        wait = self.throttle - (time.monotonic() - self._last)
        if wait > 0:
            time.sleep(wait)
        self._last = time.monotonic()

    def _open(self, req, attempts: int = 4) -> tuple[int, bytes]:
        for attempt in range(attempts):
            self._pace()
            self.requests += 1
            try:
                r = self._op.open(req, timeout=90)
                return r.status, r.read()
            except urllib.error.HTTPError as e:
                # 404 is an answer, not a failure: tabs that are not public 404.
                if e.code == 404:
                    return 404, b""
                if e.code in (401, 403, 419) and attempt < attempts - 1:
                    self._token = None                # session expired; start over
                    self.token()
                    continue
                if e.code >= 500 and attempt < attempts - 1:
                    time.sleep(5 * (attempt + 1))
                    continue
                raise
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                if attempt == attempts - 1:
                    raise
                time.sleep(5 * (attempt + 1))
        raise RuntimeError("unreachable")

    def token(self) -> str:
        if not self._token:
            _, body = self._open(urllib.request.Request(BASE + "/ep/invt/selectListTendInvitAL.do"))
            m = re.search(r'name="_csrf"[^>]*value="([^"]+)"', body.decode("utf-8", "replace"))
            if not m:
                raise RuntimeError("JONEPS returned no CSRF token; the page layout may have changed")
            self._token = m.group(1)
        return self._token

    def list_page(self, page: int, **filters) -> str:
        data = {"_csrf": self.token(), "searchTendTypeCd1": MEDICINES,
                "currentPageNo": page, "recordCountPerPage": LIST_PAGE_SIZE}
        data.update({k: v for k, v in filters.items() if v not in (None, "")})
        req = urllib.request.Request(
            BASE + "/ep/invt/selectListTendInvitAL.do",
            data=urllib.parse.urlencode(data).encode(),
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        _, body = self._open(req)
        return body.decode("utf-8", "replace")

    def goods(self, t: dict) -> dict:
        q = dict(tendNo=t["tend_no"], tendSeq=t["tend_seq"], intrlLocalTypeCd=t["intrl"],
                 tendCategCd=t["categ"], isFrameworkAgreement="Y" if t["fa"] else "N",
                 page=1, pageSize=500)
        req = urllib.request.Request(BASE + "/ep/tender/MDgoods.do?" + urllib.parse.urlencode(q),
                                     headers={"Accept": "application/json",
                                              "X-Requested-With": "XMLHttpRequest"})
        status, body = self._open(req)
        if status == 404 or not body:
            return {"total": 0, "list": []}
        return json.loads(body.decode("utf-8", "replace"))

    def award_tab(self, t: dict) -> str:
        q = dict(tendNo=t["tend_no"], tendSeq=t["tend_seq"],
                 frameworkAgreementSeq=t["fa"], _csrf=self.token())
        req = urllib.request.Request(BASE + "/ep/invt/awrdRsultMedicine.do?" + urllib.parse.urlencode(q),
                                     headers={"X-Requested-With": "XMLHttpRequest"})
        status, body = self._open(req)
        return "" if status == 404 else body.decode("utf-8", "replace")

    def award(self, t: dict, no: str, seq: str) -> str:
        q = dict(tendCategCd=t["categ"], awrdDcsionNo=no, awrdDcsionSeq=seq,
                 tendTypeCd1=MEDICINES, tendNo=t["tend_no"], tendSeq=t["tend_seq"],
                 intrlLocalTypeCd=t["intrl"])
        req = urllib.request.Request(
            BASE + "/ep/evaw/selectDetailAwardDecsn.do?" + urllib.parse.urlencode(q),
            data=urllib.parse.urlencode({"_csrf": self.token()}).encode(),
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        status, body = self._open(req)
        return "" if status == 404 else body.decode("utf-8", "replace")


def detail_url(t: dict) -> str:
    """A plain GET works without a session, so this is a real link for people."""
    q = dict(tendNo=t["tend_no"], tendSeq=t["tend_seq"], frameworkAgreementSeq=t["fa"],
             tendCategCd=t["categ"], workType="", intrlLocalTypeCd=t["intrl"],
             tendMthodCd=t["method"], invtYn="Y", searchConditions="")
    return BASE + "/ep/invt/MedicineDetails.do?" + urllib.parse.urlencode(q)


# --- list parsing ------------------------------------------------------------------

_ROW = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S)
_GO = re.compile(r"fn_goDetail\(\s*'(\d+)'\s*,\s*'(\d*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,"
                 r"\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*\)")


def parse_total(html: str) -> int | None:
    # The thousands separator is a DOT: "1.045" is one thousand and forty-five.
    m = re.search(r'pointTxt2">\s*([\d.,\s]+?)\s*</span>\s*النتائج', html)
    return int(re.sub(r"[.,\s]", "", m.group(1))) if m else None


def parse_pages(html: str) -> int | None:
    m = re.search(r'الصفحة\s*<span[^>]*>\s*\d+\s*</span>\s*/\s*([\d.,]+)', html)
    return int(re.sub(r"[.,]", "", m.group(1))) if m else None


def parse_list(html: str) -> list[dict]:
    """One dict per result row. Columns: number, title, buyer, type, published, deadline."""
    out = []
    for tr in _ROW.findall(html):
        go = _GO.search(tr)
        if not go:
            continue
        cells = [_clean(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
        if len(cells) < 6:
            continue
        tend_no, tend_seq, fa, categ, work, intrl, method, typ = go.groups()
        out.append({
            "tend_no": tend_no, "tend_seq": tend_seq, "fa": fa, "categ": categ or MEDICINE_CATEG,
            "work": work, "intrl": intrl, "method": method, "type": typ,
            "title": cells[1], "buyer": cells[2],
            "published": _iso(cells[4]), "deadline": _iso(cells[5]),
            "published_raw": cells[4], "deadline_raw": cells[5],
        })
    return out


# --- discovery ---------------------------------------------------------------------

def _crawl_list(client: Client, **filters) -> list[dict]:
    """Every row a filtered search returns, across its pages.

    The server CLAMPS an out-of-range page to the last real one rather than
    returning nothing, so "keep going until a page comes back empty" never
    terminates on a result that is an exact multiple of the page size. Stop on
    the page count the page states, and independently on any page that adds no
    row we have not already seen.
    """
    rows, seen, page = [], set(), 1
    while True:
        html = client.list_page(page, **filters)
        got = [r for r in parse_list(html) if (r["tend_no"], r["tend_seq"]) not in seen]
        seen.update((r["tend_no"], r["tend_seq"]) for r in got)
        rows += got
        pages = parse_pages(html)
        if not got or len(got) < LIST_PAGE_SIZE or (pages is not None and page >= pages):
            return rows
        page += 1


def discover(client: Client, years: list[int]) -> tuple[dict[int, dict], list[str]]:
    """Every medicine tender for the given years, labelled with status and class.

    Three passes per year, all over the cheap list pages: everything; then each
    status, to learn which tenders are awarded; then each therapeutic class.
    The alternative -- opening every tender's information tab to read the same
    labels -- costs one request per tender instead of a few per year.
    """
    found: dict[int, dict] = {}
    errors: list[str] = []
    for year in years:
        try:
            for r in _crawl_list(client, searchFiscalYear=year):
                pid = post_id_for(r["tend_no"], r["tend_seq"])
                found.setdefault(pid, {**r, "year": year, "subcategory": None, "labels": set()})
            for code in STATUSES:
                for r in _crawl_list(client, searchFiscalYear=year, searchTendStatusCd=code):
                    pid = post_id_for(r["tend_no"], r["tend_seq"])
                    found.setdefault(pid, {**r, "year": year, "subcategory": None,
                                           "labels": set()})["labels"].add(code)
            for code in LIFECYCLE:
                for r in _crawl_list(client, searchFiscalYear=year, searchTendStatCd=code):
                    pid = post_id_for(r["tend_no"], r["tend_seq"])
                    found.setdefault(pid, {**r, "year": year, "subcategory": None,
                                           "labels": set()})["labels"].add(code)
            for code in SUBCATEGORIES:
                for r in _crawl_list(client, searchFiscalYear=year, searchTendTypeCd2=code):
                    pid = post_id_for(r["tend_no"], r["tend_seq"])
                    found.setdefault(pid, {**r, "year": year, "labels": set()})["subcategory"] = code
        except Exception as e:                          # noqa: BLE001
            errors.append(f"{year}: {type(e).__name__}: {e}")
            log.warning("discovery failed for %s: %s", year, e)
        log.info("  %s: %d tenders so far", year, len(found))
    for t in found.values():
        t["status"] = next((c for c in STATUS_PRECEDENCE if c in t["labels"]), None)
    return found, errors


def tender_record(pid: int, t: dict, today: date | None = None) -> dict:
    label, terminal = (STATUSES.get(t.get("status") or "")
                       or LIFECYCLE.get(t.get("status") or "") or (None, False))
    if label is None:
        # Unlabelled on the portal: open for bids until the deadline, then
        # waiting for bid opening. Re-derived every run, so it rolls over.
        deadline = t.get("deadline") or ""
        label = "Open" if deadline >= (today or date.today()).isoformat() else "Closed"
    sub = t.get("subcategory")
    return {
        "post_id": pid, "tender_id": tender_id_for(t["tend_no"], t["tend_seq"]),
        "url": detail_url(t), "title_ar": t["title"], "buyer": t["buyer"],
        "status_label": label, "status_slugs": sorted(t.get("labels") or []),
        "opening_date": t["published_raw"], "submission_deadline": t["deadline_raw"],
        "opening_ts": t["published"], "submission_ts": t["deadline"],
        "subcategory": SUBCATEGORIES.get(sub) if sub else None,
        "method": METHODS.get(t["method"], t["method"] or None),
        "fiscal_year": t["year"], "tender_year": t["year"],
        "source": SOURCE, "country": COUNTRY, "category_guess": "pharma",
        "is_terminal": 1 if terminal else 0, "is_listed": 1, "discovered_via": "joneps-list",
    }


# --- drug lines --------------------------------------------------------------------

_ITEM_FIELDS = ("lotSerno", "itemSerno", "clId", "clNmAr", "clNmEn", "rdlItemCd",
                "rdlGenerNm", "unitMesurNm", "whoutTaxQty", "whTaxQty", "totlQty")


def parse_goods(payload: dict) -> list[dict]:
    items = []
    for n, it in enumerate(payload.get("list") or [], start=1):
        unspsc = str(it.get("clId") or "").strip() or None
        group = unspsc[:2] if unspsc and len(unspsc) >= 2 else None
        demand = [{"entity": d.get("deNo"), "abbr": d.get("abbrNm"), "qty": d.get("totlQty")}
                  for d in (it.get("details") or []) if d.get("totlQty")]
        items.append({
            "sn": str(it.get("lotSerno") or ""), "item_no": str(it.get("itemSerno") or n),
            "description": it.get("rdlGenerNm") or it.get("clNmEn") or it.get("clNmAr"),
            "generic_name": it.get("clNmEn"), "generic_name_ar": it.get("clNmAr"),
            "rdl_code": rdl(it.get("rdlItemCd")), "unspsc": unspsc,
            "uom": it.get("unitMesurNm"), "qty": it.get("totlQty"),
            "qty_raw": it.get("totlQtyStr"),
            "code_group": group,
            # A medicine tender's lines are medicines unless UNSPSC says
            # otherwise: diagnostic agents and infant formula ride along too.
            "category_guess": CODE_GROUPS.get(group or "", "pharma"),
            "demand_json": json.dumps(demand, ensure_ascii=False) if demand else None,
            "raw_row": json.dumps({k: it.get(k) for k in _ITEM_FIELDS if it.get(k) not in (None, "")},
                                  ensure_ascii=False),
            "row_index": n,
        })
    return items


def _current_attachment(conn, pid: int, role: str):
    return conn.execute("SELECT * FROM attachments WHERE post_id=? AND role=? AND is_current=1",
                        (pid, role)).fetchone()


def _version(conn, run_id: int, pid: int, tid: str, role: str, url: str,
             digest: str, rows: int) -> tuple[str, int]:
    """Record one derived 'document' -- the goods JSON, or an award -- by hash.

    Mirrors how NUPCO versions its files, so both sources share change
    detection and the change log: an unchanged hash touches last_seen_at and
    nothing else, a changed one writes a new version and retires the old.
    """
    ts = db.now()
    cur = _current_attachment(conn, pid, role)
    if cur is not None and cur["content_sha256"] == digest:
        conn.execute("UPDATE attachments SET last_seen_at=? WHERE id=?", (ts, cur["id"]))
        return "same", cur["id"]
    version = (cur["version"] + 1) if cur is not None else 1
    if cur is not None:
        conn.execute("UPDATE attachments SET is_current=0 WHERE id=?", (cur["id"],))
    att = conn.insert_returning_id(
        "INSERT INTO attachments(post_id, tender_id, role, url, filename, ext, content_sha256,"
        " version, is_current, parse_status, parse_rows, first_seen_at, last_seen_at, last_changed_at)"
        " VALUES (?,?,?,?,?,?,?,?,1,'parsed',?,?,?,?)",
        (pid, tid, role, url, f"{role}.json", "json", digest, version, rows,
         cur["first_seen_at"] if cur is not None else ts, ts, ts))
    if cur is not None:
        db.log_change(conn, run_id, pid, tid, f"file:{role}",
                      cur["content_sha256"][:12], digest[:12])
    return ("changed" if cur is not None else "new"), att


# --- awards ------------------------------------------------------------------------

def award_refs(tab_html: str) -> list[tuple[str, str]]:
    refs = re.findall(r"fn_goDetailAwrdDcsion(?:Md)?\('(\d+)'\s*,\s*'(\d*)'\)", tab_html or "")
    return list(dict.fromkeys(refs))              # de-duplicated, order kept


def _cells(tr: str) -> list[tuple[str, str]]:
    """(tag, text) per cell, positions kept -- empty cells matter here."""
    return [(tag, _clean(inner))
            for tag, inner in re.findall(r"<(td|th)[^>]*>(.*?)</t[dh]>", tr, re.S)]


def _pairs(table: str) -> dict[str, str]:
    """Label/value tables: each <th> labels the <td> that follows it."""
    out = {}
    for tr in _ROW.findall(table):
        cells = _cells(tr)
        for i, (tag, text) in enumerate(cells):
            if tag == "th" and i + 1 < len(cells) and cells[i + 1][0] == "td":
                out.setdefault(text, cells[i + 1][1])
    return out


def _entity(text: str | None) -> tuple[str | None, str | None]:
    """'[270100001]  مديرية الخدمات الطبية الملكية' -> ('270100001', name)."""
    m = re.match(r"\s*\[(\d+)\]\s*(.*)", text or "")
    return (m.group(1), m.group(2).strip() or None) if m else (None, (text or "").strip() or None)


def parse_award(html: str) -> tuple[dict, list[dict]]:
    """An award decision page -> (header, one row per awarded line per block).

    The page repeats (supplier x beneficiary) blocks, each a label/value
    supplier table followed by an items table. Each item spans up to four rows:
    codes, pack, free goods, quantity, value and reason; then offer number,
    scientific name, registration and unit price; then manufacturer, origin,
    shelf life and brand; then any special conditions.

    Whether the price includes tax is stated in each items table's own header,
    and it differs between blocks of the same page -- military hospitals buy
    tax-exempt -- so it is read per table, never once per page.
    """
    tables = re.findall(r"<table.*?</table>", html or "", re.S)
    header: dict = {}
    rows: list[dict] = []
    supplier: dict = {}
    for t in tables:
        heads = [x for _, x in (c for tr in _ROW.findall(t) for c in _cells(tr)) if x]
        if "حالة الإحالة" in heads and "رقم قرار الإحالة" in heads and not header:
            p = _pairs(t)
            header = {"award_no": p.get("رقم قرار الإحالة"),
                      "award_status": p.get("حالة الإحالة"),
                      "published_at": _iso(p.get("تاريخ نشر قرار الاحالة")),
                      "buyer": _entity(p.get("الجهة المشترية"))[1]}
        elif "رقم المناقص" in heads:
            p = _pairs(t)
            code, name = _entity(p.get("الجهة المستفيدة"))
            supplier = {"supplier_no": p.get("رقم المناقص"), "supplier": p.get("اسم المناقص"),
                        "supplier_country": p.get("البلد"), "po_no": p.get("رقم العملية الشرائية"),
                        "beneficiary_code": code, "beneficiary": name}
        elif "رمز UNSPSC" in heads:
            price_head = next((h for h in heads if h.startswith("السعر النهائي")), "")
            incl = None if not price_head else (0 if "غير شامل" in price_head else 1)
            item: dict | None = None
            stage = 0
            for tr in _ROW.findall(t):
                cells = _cells(tr)
                tds = [x for tag, x in cells if tag == "td"]
                if not tds:
                    continue
                if len(tds) >= 10 and re.fullmatch(r"\d+", tds[0]):
                    if item:
                        rows.append(item)
                    total = _num(tds[8])
                    qty = _num(tds[7])
                    item = {**supplier, "price_incl_tax": incl,
                            "item_no": tds[0], "unspsc": tds[1] or None, "rdl_code": rdl(tds[2]),
                            "pack": tds[3] or None, "pack_size": _num(tds[3]),
                            "unit_size": tds[4] or None,
                            "free_qty_pct": _num(tds[5]), "discount_pct": _num(tds[6]),
                            "awarded_qty": qty, "total_value": total,
                            "currency": (re.search(r"[A-Z]{3}", tds[8]) or [None])[0],
                            "unit_cost": (total / qty) if total and qty else None,
                            "award_reason": tds[9] or None}
                    stage = 1
                elif item is not None and cells and cells[0][0] == "th" and "شروط" in cells[0][1]:
                    item["conditions"] = tds[0] if tds else None
                elif item is not None and len(tds) == 4 and stage == 1:
                    item.update({"offer_no": tds[0], "scientific_name": tds[1].strip("[] ") or None,
                                 "registration": tds[2] or None, "unit_price": _num(tds[3])})
                    item["currency"] = item["currency"] or _bracket(tds[3])
                    stage = 2
                elif item is not None and len(tds) == 4 and stage == 2:
                    item.update({"manufacturer": tds[0] or None, "origin_country": tds[1] or None,
                                 "shelf_life": tds[2] or None, "brand": tds[3] or None})
                    stage = 3
            if item:
                rows.append(item)
    return header, rows


_AWARD_COLS = ("award_no", "award_status", "published_at", "buyer", "beneficiary_code",
               "beneficiary", "supplier_no", "supplier", "supplier_country", "po_no",
               "item_no", "unspsc", "rdl_code", "scientific_name", "brand", "manufacturer",
               "origin_country", "registration", "shelf_life", "pack", "pack_size",
               "unit_size", "free_qty_pct", "discount_pct", "awarded_qty", "unit_price",
               "price_incl_tax", "total_value", "currency", "unit_cost", "award_reason",
               "conditions")


def store_award(conn, pid: int, tid: str, header: dict, rows: list[dict]) -> int:
    """Replace one decision's rows. A decision is re-parsed whole, never merged."""
    no = header.get("award_no")
    conn.execute("DELETE FROM awards WHERE post_id=? AND award_no=?", (pid, no))
    ts = db.now()
    payload = [tuple([SOURCE, pid, tid] + [({**header, **r}).get(c) for c in _AWARD_COLS] + [ts, ts])
               for r in rows]
    if payload:
        conn.executemany(
            f"INSERT INTO awards(source, post_id, tender_id, {', '.join(_AWARD_COLS)},"
            f" first_seen_at, last_seen_at) VALUES ({', '.join('?' * (len(_AWARD_COLS) + 5))})",
            payload)
    return len(payload)


# --- one tender ----------------------------------------------------------------------

def process_tender(conn, client: Client, run_id: int, pid: int, t: dict,
                   state: str, force: bool, counts: dict) -> None:
    tid = tender_id_for(t["tend_no"], t["tend_seq"])
    prev = conn.execute("SELECT detail_fetched_at FROM tenders WHERE post_id=?", (pid,)).fetchone()
    awarded = "Final_Awarded" in (t.get("labels") or ())
    have_award = _current_attachment(conn, pid, "award") is not None

    # A final award does not change what was requested, so once both halves
    # are captured the tender is frozen -- unless the list itself says it moved.
    if (not force and prev is not None and prev["detail_fetched_at"] and state == "same"
            and (not awarded or have_award)):
        return

    payload = client.goods(t)
    items = parse_goods(payload)
    digest = _hash([{k: v for k, v in i.items() if k != "row_index"} for i in items])
    goods_url = BASE + "/ep/tender/MDgoods.do?" + urllib.parse.urlencode(
        dict(tendNo=t["tend_no"], tendSeq=t["tend_seq"]))
    how, att = _version(conn, run_id, pid, tid, "item_list", goods_url, digest, len(items))
    if how != "same":
        from .items import store_items
        counts["items_parsed"] += store_items(conn, att, pid, tid, items)
        counts["files_changed"] += 1

    if awarded:
        for no, seq in award_refs(client.award_tab(t)):
            header, rows = parse_award(client.award(t, no, seq))
            if not header.get("award_no"):
                header["award_no"] = f"{no}-{seq}" if seq else no
            digest = _hash([header] + rows)
            url = f"{BASE}/ep/evaw/selectDetailAwardDecsn.do?awrdDcsionNo={no}&awrdDcsionSeq={seq}"
            how, _ = _version(conn, run_id, pid, tid, "award", url, digest, len(rows))
            if how != "same":
                counts["award_lines"] += store_award(conn, pid, tid, header, rows)
                counts["awards_changed"] += 1

    conn.execute("UPDATE tenders SET detail_fetched_at=? WHERE post_id=?", (db.now(), pid))


# --- the run ------------------------------------------------------------------------

BATCH = 100      # tenders between progress checkpoints


def _checkpoint(conn, run_id: int, batch: list[int], counts: dict, done: int, total: int,
                requests: int) -> None:
    """Make a finished batch fully visible while the crawl goes on.

    Every tender is already committed on its own, so nothing is lost between
    checkpoints; this fills in what is otherwise only computed at the end --
    the batch's line counts and countdowns -- and writes the running totals to
    run_log, where the site's "last run" and the step summary read them."""
    from .run import refresh_rollups
    refresh_rollups(conn, batch)
    fields = ("tenders_seen", "tenders_new", "tenders_changed", "files_changed",
              "items_parsed", "errors")
    conn.execute(f"UPDATE run_log SET {', '.join(f + '=?' for f in fields)}, notes=? "
                 "WHERE run_id=?",
                 [counts[f] for f in fields]
                 + [json.dumps({"progress": f"{done}/{total}", "requests": requests,
                                "award_lines": counts["award_lines"]}), run_id])
    conn.commit()
    log.info("  batch saved: %d/%d tenders, %d award lines, %d errors, %d requests",
             done, total, counts["award_lines"], counts["errors"], requests)


def run(mode: str = "incremental", years: list[int] | None = None, force: bool = False,
        max_minutes: float | None = None, limit: int | None = None,
        client: Client | None = None, batch_size: int = BATCH) -> dict:
    """Crawl JONEPS medicine tenders into the shared database.

    backfill    every year from 2018 (or --years), fetching each tender once;
                a re-run skips what is already done, so it resumes.
    incremental the current and previous fiscal years -- where anything still
                moves -- re-fetching only tenders the list shows have changed.
    """
    started = time.monotonic()
    this_year = date.today().year
    if not years:
        years = (list(range(FIRST_YEAR, this_year + 1)) if mode == "backfill"
                 else [this_year - 1, this_year])
    client = client or Client()
    conn = db.connect()
    db.init(conn)
    run_id = db.start_run(conn, f"joneps-{mode}")
    counts = dict(tenders_seen=0, tenders_new=0, tenders_changed=0, files_changed=0,
                  items_parsed=0, errors=0, award_lines=0, awards_changed=0)
    log.info("=== JONEPS %s, years %s ===", mode, ",".join(map(str, years)))

    found, errors = discover(client, years)
    counts["errors"] += len(errors)
    log.info("discovered %d medicine tenders in %d requests", len(found), client.requests)

    # Awarded tenders first, newest first: the award is where the prices are,
    # so if a run meets its time budget the most valuable data is already in.
    todo = sorted(found.items(), reverse=True,
                  key=lambda kv: ("Final_Awarded" in kv[1]["labels"], kv[1]["year"], kv[0]))
    if limit:
        todo = todo[:limit]
    stopped_early = False
    batch: list[int] = []
    for n, (pid, t) in enumerate(todo, start=1):
        if max_minutes and (time.monotonic() - started) / 60 > max_minutes:
            log.info("time budget of %s min reached after %d tenders; stopping cleanly",
                     max_minutes, n - 1)
            stopped_early = True
            break
        try:
            with db.write_lock:
                state = db.upsert_tender(conn, run_id, tender_record(pid, t))
            counts["tenders_seen"] += 1
            counts["tenders_new"] += state == "new"
            counts["tenders_changed"] += state == "changed"
            process_tender(conn, client, run_id, pid, t, state, force, counts)
            conn.commit()                       # one tender at a time: resumable
        except Exception as e:                  # noqa: BLE001
            conn.rollback()
            counts["errors"] += 1
            errors.append(f"{tender_id_for(t['tend_no'], t['tend_seq'])}: {type(e).__name__}: {e}")
            log.warning("tender %s failed: %s", tender_id_for(t["tend_no"], t["tend_seq"]), e)
        batch.append(pid)
        if len(batch) >= batch_size:
            _checkpoint(conn, run_id, batch, counts, n, len(todo), client.requests)
            batch = []

    from .run import refresh_rollups
    refresh_rollups(conn)
    db.rebuild_fts(conn)

    complete = not stopped_early and not errors and not limit
    if complete and mode == "backfill" and years == list(range(FIRST_YEAR, this_year + 1)):
        # Only a full, clean sweep of every year may conclude anything is gone.
        n = db.mark_unseen_as_delisted(conn, run_id, found.keys(), source=SOURCE)
        if n:
            log.info("delisted %d JONEPS tenders no longer on the portal", n)

    status = "ok" if not errors and not stopped_early else "partial"
    notes = {"errors": errors[:40], "requests": client.requests,
             "award_lines": counts["award_lines"], "stopped_early": stopped_early}
    db.finish_run(conn, run_id, status, notes=json.dumps(notes, ensure_ascii=False)[:2000],
                  **{k: v for k, v in counts.items() if k not in ("award_lines", "awards_changed")})
    conn.commit()
    conn.close()
    log.info("=== JONEPS %s %s: %s, %d requests ===", mode, status, counts, client.requests)
    return {"status": status, "run_id": run_id, **counts, "requests": client.requests}


def main(argv=None) -> int:
    from .run import _setup_logging
    ap = argparse.ArgumentParser(description="JONEPS medicine tenders and awards")
    ap.add_argument("mode", nargs="?", default="incremental", choices=["backfill", "incremental"])
    ap.add_argument("--years", help="comma-separated fiscal years, e.g. 2023,2024")
    ap.add_argument("--force", action="store_true", help="re-fetch tenders already captured")
    ap.add_argument("--max-minutes", type=float, help="stop cleanly after this long; resumable")
    ap.add_argument("--limit", type=int, help="only process the first N tenders")
    ap.add_argument("--batch", type=int, default=BATCH,
                    help=f"tenders between progress checkpoints (default {BATCH})")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args(argv)
    _setup_logging(not a.quiet)
    years = [int(y) for y in a.years.split(",")] if a.years else None
    try:
        result = run(a.mode, years=years, force=a.force, max_minutes=a.max_minutes, limit=a.limit,
                     batch_size=a.batch)
    except Exception:                           # noqa: BLE001
        log.error("JONEPS run crashed:\n%s", traceback.format_exc())
        return 2
    return 0 if result["status"] in ("ok", "partial") else 1


if __name__ == "__main__":
    sys.exit(main())
