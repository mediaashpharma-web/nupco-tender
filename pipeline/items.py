"""Parse Tender Item Lists into normalised line items.

The item list is the actual demand data -- SKU, quantity, unit -- and NUPCO
publishes it in at least four shapes:

  xlsx : SN | Generic Code  | SAP Generic Name  | Requested Quantity | INUPCO
  pdf A: SN | ITEM NO | NUPCO CODE | ITEM DESCRIPTION | UOM | QTY | GROUPS
  pdf B: ITEM NO | SRM CODE | ITEM DESCRIPTION | GROUP          (no quantity)
  pdf C: SN | NUPCO CODE | DESCRIPTION | UOM | ITEMIZED | INITIAL QTY

All of them are text-based (no OCR needed) and the 13-digit SAP material code
appears in every variant, which makes it the join key for item identity.

Anything we cannot parse is recorded in parse_failures rather than dropped.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from .scrape import classify, clean, code_group, guess_category, is_accessory, to_float

# --- header normalisation ----------------------------------------------------

CANON = {
    "sn":          ["sn", "s n", "s.n", "sr", "sr no", "serial", "no", "#", "seq"],
    "item_no":     ["item no", "item number", "itemno", "item code", "item"],
    "nupco_code":  ["nupco code", "srm code", "generic code", "sap code",
                    "material code", "material number", "code", "sap generic code"],
    "description": ["item description", "description", "sap generic name",
                    "generic name", "item name", "material description", "desc",
                    "generic material text", "material text", "item text",
                    "short text", "product description", "drug name", "trade name"],
    "uom":         ["uom", "unit", "unit of measure", "units"],
    "qty":         ["qty", "quantity", "requested quantity", "initial qty",
                    "total qty", "req qty", "annual qty", "estimated qty",
                    "sum of org open quantity", "org open quantity", "open quantity",
                    "required quantity", "total quantity"],
    "item_group":  ["group", "groups", "group no", "group number"],
    "itemized":    ["itemized", "itemised", "item type"],
}
_LOOKUP = {alias: canon for canon, aliases in CANON.items() for alias in aliases}

CODE_RE = re.compile(r"^\d{10,14}$")


def _norm_header(cell) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", str(cell or "").lower()).strip()


def map_headers(cells: list) -> dict[int, str]:
    """Map column index -> canonical field, for a candidate header row."""
    mapping = {}
    for i, cell in enumerate(cells):
        key = _norm_header(cell)
        if not key:
            continue
        if key in _LOOKUP:
            mapping[i] = _LOOKUP[key]
            continue
        for alias, canon in _LOOKUP.items():          # substring fallback
            if alias in key and len(alias) > 2:
                mapping[i] = canon
                break
    # A header row must at least identify a description or a code.
    return mapping if {"description", "nupco_code"} & set(mapping.values()) else {}


def _row_to_item(cells: list, mapping: dict[int, str]) -> dict | None:
    rec = {v: None for v in CANON}
    for idx, field in mapping.items():
        if idx < len(cells):
            rec[field] = clean(str(cells[idx])) if cells[idx] is not None else None

    # Recover a code that landed in the wrong column.
    if not rec["nupco_code"]:
        for c in cells:
            s = re.sub(r"\D", "", str(c or ""))
            if CODE_RE.match(s):
                rec["nupco_code"] = s
                break
    if rec["nupco_code"]:
        rec["nupco_code"] = re.sub(r"\D", "", rec["nupco_code"]) or None

    if not rec["description"] or len(rec["description"]) < 3:
        return None
    if _norm_header(rec["description"]) in _LOOKUP:    # repeated header row
        return None

    rec["qty_raw"] = rec["qty"]
    rec["qty"] = to_float(rec["qty"])
    rec["category_guess"] = classify(rec["nupco_code"], rec["description"])
    rec["code_group"] = code_group(rec["nupco_code"])
    rec["is_accessory"] = is_accessory(rec["description"])
    rec["raw_row"] = json.dumps([None if c is None else str(c) for c in cells],
                                ensure_ascii=False)[:4000]
    return rec


# --- spreadsheet -------------------------------------------------------------

def parse_spreadsheet(path: Path) -> tuple[list[dict], str]:
    import openpyxl

    if path.suffix.lower() in (".csv", ".txt"):
        return _parse_csv(path)

    # keep_links=False matters far more than it looks: one NUPCO item list is a
    # 44MB workbook whose sheets are 0.5MB and whose cached external-link XML is
    # 350MB. openpyxl parses those by default and the process dies.
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True, keep_links=False)
    items: list[dict] = []
    try:
        for ws in wb.worksheets:
            # Stream. Item lists run to 40MB+ and hundreds of thousands of rows;
            # materialising every row at once is what gets the pipeline killed.
            items.extend(_rows_to_items(ws.iter_rows(values_only=True)))
    finally:
        wb.close()
    return items, "openpyxl"


def _parse_csv(path: Path) -> tuple[list[dict], str]:
    import csv

    for enc in ("utf-8-sig", "utf-8", "cp1256", "latin-1"):
        try:
            text = path.read_text(encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        return [], "csv-undecodable"
    sample = text[:4000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel
    rows = [r for r in csv.reader(text.splitlines(), dialect)]
    return _rows_to_items(rows), "csv"


MAX_ROWS = 300_000          # guard against a pathological sheet
HEADER_SCAN_ROWS = 40
SNIFF_ROWS = 25             # data rows buffered to infer an unlabelled column


def infer_description_column(sample: list[list], mapping: dict[int, str]) -> int | None:
    """Pick the description column when the header name is one we don't know.

    NUPCO's headers are not standardised -- 'Generic Material Text' and
    'Sum of Org.Open Quantity' both turn up. Rather than drop a whole tender's
    item list over an unseen label, fall back to what the data looks like: the
    free-text column is the one with by far the longest average string.
    """
    taken = set(mapping)
    width = max((len(r) for r in sample), default=0)
    best, best_len = None, 0.0
    for col in range(width):
        if col in taken:
            continue
        vals = [str(r[col]) for r in sample
                if col < len(r) and r[col] not in (None, "")]
        if len(vals) < max(2, len(sample) // 3):
            continue
        if sum(v.replace(".", "").isdigit() for v in vals) > len(vals) * 0.6:
            continue                                   # a numeric column
        avg = sum(len(v) for v in vals) / len(vals)
        if avg > best_len:
            best, best_len = col, avg
    return best if best_len >= 8 else None


def _rows_to_items(rows) -> list[dict]:
    """Find the header row, then stream everything under it.

    Takes any row iterable, so a workbook can be read without ever holding the
    whole sheet in memory.
    """
    out: list[dict] = []
    mapping: dict[int, str] = {}
    buffer: list[tuple[int, list]] = []
    settled = False

    def emit(j: int, row: list) -> None:
        rec = _row_to_item(row, mapping)
        if rec:
            rec["row_index"] = j
            out.append(rec)

    for j, row in enumerate(rows):
        if j >= MAX_ROWS:
            break
        row = list(row)
        if not mapping:
            if j < HEADER_SCAN_ROWS:
                m = map_headers(row)
                if m:
                    mapping = m
                    continue
                continue
            break                              # no header in range: not a table
        if not any(c not in (None, "") for c in row):
            continue

        if not settled:
            buffer.append((j, row))
            if len(buffer) < SNIFF_ROWS:
                continue
            settled = True
            if "description" not in mapping.values():
                col = infer_description_column([r for _, r in buffer], mapping)
                if col is not None:
                    mapping[col] = "description"
            for bj, brow in buffer:
                emit(bj, brow)
            buffer.clear()
            continue
        emit(j, row)

    if not settled and buffer:                 # fewer rows than the sniff window
        if "description" not in mapping.values():
            col = infer_description_column([r for _, r in buffer], mapping)
            if col is not None:
                mapping[col] = "description"
        for bj, brow in buffer:
            emit(bj, brow)
    return out


# --- pdf ---------------------------------------------------------------------

class ScannedPdfError(ValueError):
    """A PDF with no text layer. Genuinely needs OCR; say so rather than
    reporting a vague parse failure."""


def parse_pdf(path: Path) -> tuple[list[dict], str]:
    """Table extraction first; fall back to line reconstruction."""
    import pdfplumber

    items: list[dict] = []
    mapping: dict[int, str] = {}
    with pdfplumber.open(str(path)) as pdf:
        for pno, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables() or []
            _release(page)                  # pdfplumber caches every page object
            for table in tables:
                rows = [[clean(c) if isinstance(c, str) else c for c in r] for r in table]
                local = dict(mapping)
                start = 0
                for i, row in enumerate(rows[:6]):
                    m = map_headers(row)
                    if m:
                        local, mapping, start = m, m, i + 1
                        break
                if not local:
                    continue
                for j, row in enumerate(rows[start:], start=start):
                    rec = _row_to_item(row, local)
                    if rec:
                        rec["source_page"] = pno
                        rec["row_index"] = j
                        items.append(rec)
    if items:
        return items, "pdfplumber-table"
    parsed, parser = _parse_pdf_text(path)
    if not parsed and _has_no_text_layer(path):
        raise ScannedPdfError(
            "PDF has no text layer (scanned image) - needs OCR to extract items")
    return parsed, parser


def _has_no_text_layer(path: Path, sample_pages: int = 3) -> bool:
    import pdfplumber

    try:
        with pdfplumber.open(str(path)) as pdf:
            chars = 0
            for page in pdf.pages[:sample_pages]:
                chars += len(page.extract_text() or "")
                _release(page)
            return chars < 50
    except Exception:                               # noqa: BLE001
        return False


def _lines(words: list[dict], tol: float = 3.0) -> list[list[dict]]:
    """Cluster words into visual lines by their vertical position."""
    out: list[list[dict]] = []
    for w in sorted(words, key=lambda w: (round(w["top"], 1), w["x0"])):
        if out and abs(w["top"] - out[-1][0]["top"]) <= tol:
            out[-1].append(w)
        else:
            out.append([w])
    for line in out:
        line.sort(key=lambda w: w["x0"])
    return out


def _header_columns(line: list[dict], gap: float = 9.0):
    """Turn a header line into [(canonical_field, x_start, x_end), ...].

    Adjacent words are glued into phrases ('NUPCO CODE', 'INITIAL QTY') before
    being matched against the alias table.
    """
    phrases, cur = [], [line[0]]
    for w in line[1:]:
        if w["x0"] - cur[-1]["x1"] <= gap:
            cur.append(w)
        else:
            phrases.append(cur)
            cur = [w]
    phrases.append(cur)

    cols = []
    for ph in phrases:
        label = _norm_header(" ".join(w["text"] for w in ph))
        field = _LOOKUP.get(label)
        if field is None:
            for alias, canon in _LOOKUP.items():
                if len(alias) > 2 and alias in label:
                    field = canon
                    break
        if field:
            cols.append([field, ph[0]["x0"], ph[-1]["x1"]])
    if not any(c[0] in ("description", "nupco_code") for c in cols):
        return []

    # Widen each column to the midpoint between neighbours so wrapped text and
    # right-aligned numbers still land in the right bucket.
    cols.sort(key=lambda c: c[1])
    bounds = []
    for i, (field, x0, x1) in enumerate(cols):
        left = 0.0 if i == 0 else (cols[i - 1][2] + x0) / 2
        right = 10_000.0 if i == len(cols) - 1 else (x1 + cols[i + 1][1]) / 2
        bounds.append((field, left, right))
    return bounds


def _release(page) -> None:
    """Drop a pdfplumber page's object cache.

    pdfplumber keeps every parsed character, line and rect of every page it has
    touched. Across a 46-page item list with ~1500 rows that runs to gigabytes,
    which is enough to get the pipeline OOM-killed part-way through parsing.
    """
    try:
        page.flush_cache()
    except Exception:                               # noqa: BLE001
        pass
    for attr in ("_objects", "_layout", "_textmap"):
        try:
            delattr(page, attr)
        except Exception:                           # noqa: BLE001
            pass


# Unit-of-measure vocabulary, used to peel the tail off a borderless row.
UOM_WORDS = {
    "each", "ea", "pac", "pack", "packet", "pkt", "box", "bx", "vial", "kit", "set",
    "pcs", "piece", "pieces", "btl", "bottle", "amp", "ampoule", "tab", "tablet",
    "cap", "capsule", "tube", "roll", "bag", "carton", "ctn", "case", "unit", "units",
    "mtr", "meter", "ltr", "liter", "litre", "gm", "kg", "mg", "ml", "dz", "dozen",
    "tin", "jar", "pouch", "strip", "sachet", "cyl", "pair", "-",
}
_ITEMIZED_RE = re.compile(r"\b(non[-\s]?itemi[sz]ed|itemi[sz]ed)\b", re.IGNORECASE)
_TRAIL_NUM_RE = re.compile(r"(?P<body>.*?)[\s|]+(?P<qty>[\d,]+(?:\.\d+)?)\s*$")
# The serial may be written "1", "1." or "1)" -- all three occur in NUPCO PDFs.
_ROW_RE = re.compile(
    r"^\s*(?:(?P<sn>\d{1,5})[.)]?\s+)?(?P<code>\d{10,14})\s+(?P<rest>.+)$")


def _peel_tail(rest: str) -> tuple[str, str | None, str | None, str | None]:
    """Strip the trailing quantity, ITEMIZED marker and UOM off a row body."""
    qty_raw = None
    tail = _TRAIL_NUM_RE.match(rest)
    if tail:
        rest, qty_raw = tail.group("body").strip(), tail.group("qty")

    itemized = None
    im = _ITEMIZED_RE.search(rest)
    if im:
        itemized = im.group(0).upper()
        rest = (rest[:im.start()] + " " + rest[im.end():]).strip()

    uom = None
    tokens = rest.split()
    while tokens and tokens[-1].strip(",.").lower() in UOM_WORDS:
        uom = tokens.pop().strip(",.")
    return " ".join(tokens).strip(" ,;|"), uom, itemized, qty_raw


def _split_row(text: str) -> dict | None:
    """Content-driven split of one borderless table row.

    Geometry is unreliable on these PDFs -- the DESCRIPTION header is narrow and
    centred while its data spans half the page, so x-midpoint bucketing shreds
    the description. Anchoring on what the values *look like* is far sturdier:
    a 10-14 digit SAP code, an optional leading serial, a trailing quantity, and
    an optional UOM / ITEMIZED marker between the description and the quantity.
    """
    m = _ROW_RE.match(text)
    if not m:
        return None
    sn, code, rest = m.group("sn"), m.group("code"), m.group("rest").strip()
    rest, uom, itemized, qty_raw = _peel_tail(rest)
    return {"sn": sn, "nupco_code": code, "description": clean(rest),
            "uom": uom, "qty_raw": qty_raw, "itemized": itemized}


def _parse_pdf_text(path: Path) -> tuple[list[dict], str]:
    """Borderless tables: rebuild rows from the text layer, content-first.

    NUPCO's item-list PDFs are frequently unruled, so extract_tables() collapses
    every row into one cell. We cluster words into visual lines, then split each
    line by value shape and stitch wrapped description lines back onto their row.
    """
    import pdfplumber

    items: list[dict] = []
    with pdfplumber.open(str(path)) as pdf:
        for pno, page in enumerate(pdf.pages, start=1):
            words = page.extract_words(use_text_flow=False, keep_blank_chars=False)
            _release(page)
            if not words:
                continue
            for j, line in enumerate(_lines(words)):
                text = " ".join(w["text"] for w in line).strip()
                if not text or _norm_header(text) in _LOOKUP:
                    continue
                row = _split_row(text)

                if row is None:
                    # Wrapped continuation: the description spilled onto the next
                    # visual line, often carrying that row's UOM/ITEMIZED/qty tail.
                    if (items and len(text) > 2 and not text.isdigit()
                            and not _header_columns(line)
                            and not re.search(r"page \d+ of \d+|www\.", text, re.I)):
                        body, uom, itemized, qty_raw = _peel_tail(text)
                        prev = items[-1]
                        if body:
                            prev["description"] = clean(f"{prev['description']} {body}")
                        prev["uom"] = prev.get("uom") or uom
                        prev["itemized"] = prev.get("itemized") or itemized
                        prev["qty_raw"] = prev.get("qty_raw") or qty_raw
                    continue
                if not row["description"] or len(row["description"]) < 3:
                    continue

                rec = {k: None for k in CANON}
                rec.update(row)
                rec["source_page"] = pno
                rec["row_index"] = j
                rec["raw_row"] = json.dumps(text, ensure_ascii=False)[:4000]
                items.append(rec)

    for rec in items:
        rec["qty"] = to_float(rec.get("qty_raw"))
        rec["category_guess"] = classify(rec.get("nupco_code"), rec["description"])
        rec["code_group"] = code_group(rec.get("nupco_code"))
        rec["is_accessory"] = is_accessory(rec["description"])
    return items, "pdfplumber-words"


# --- dispatch ----------------------------------------------------------------

def sniff_kind(path: Path) -> str:
    """Detect by magic bytes, not extension -- NUPCO mislabels occasionally."""
    with path.open("rb") as fh:
        head = fh.read(8)
    if head.startswith(b"%PDF"):
        return "pdf"
    if head.startswith(b"PK\x03\x04"):
        return "xlsx"
    if head.startswith(b"\xd0\xcf\x11\xe0"):
        return "xls"
    return {"csv": "csv", "txt": "csv"}.get(path.suffix.lower().lstrip("."), "unknown")


def parse_item_file(path: str | Path) -> tuple[list[dict], str]:
    """Returns (items, parser_name). Raises on unsupported input."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    kind = sniff_kind(path)
    if kind == "pdf":
        return parse_pdf(path)
    if kind in ("xlsx", "xls"):
        return parse_spreadsheet(path)
    if kind == "csv":
        return _parse_csv(path)
    raise ValueError(f"unsupported file type '{kind}' for {path.name}")


def store_items(conn, attachment_id: int, post_id: int, tender_id: str,
                items: list[dict]) -> int:
    """Replace the parsed items for one attachment version."""
    conn.execute("DELETE FROM tender_items WHERE attachment_id=?", (attachment_id,))
    rows = [(attachment_id, post_id, tender_id, it.get("sn"), it.get("item_no"),
             it.get("nupco_code"), it.get("description"), it.get("uom"),
             it.get("qty"), it.get("qty_raw"), it.get("item_group"),
             it.get("itemized"), it.get("category_guess"), it.get("code_group"),
             it.get("is_accessory", 0),
             it.get("source_page"), it.get("row_index"), it.get("raw_row"),
             # Structured drug identity: JONEPS publishes it as fields, NUPCO's
             # parsed PDFs do not, so these stay NULL for NUPCO rows.
             it.get("generic_name"), it.get("generic_name_ar"), it.get("rdl_code"),
             it.get("unspsc"), it.get("demand_json"))
            for it in items]
    conn.executemany(
        "INSERT INTO tender_items(attachment_id, post_id, tender_id, sn, item_no,"
        " nupco_code, description, uom, qty, qty_raw, item_group, itemized,"
        " category_guess, code_group, is_accessory, source_page, row_index, raw_row,"
        " generic_name, generic_name_ar, rdl_code, unspsc, demand_json)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    return len(rows)
