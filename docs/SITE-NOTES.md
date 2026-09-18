# NUPCO Tenders List — structure, data model, and access notes

Source: `https://www.nupco.com/tenders/tenders-list/` (verified 17 Sep 2026)

## 1. What the page actually is

WordPress + Elementor Pro. **There is no JSON API.**

| Piece | Value |
|---|---|
| Post type | `tender` → single URL `/tender/<slug>/` |
| Taxonomy | `tender-status` |
| Loop Grid widget | `data-id="3085e1f"`, container `#tender-grid`, loop template `272`, 24 items/page, `pagination_type: load_more_infinite_scroll` |
| Taxonomy Filter widget | `data-id="c882661"`, targets `3085e1f` |
| Search/date form | custom shortcode `form.tender-search-form[data-ajax="1"][data-target="#tender-grid"]` |
| i18n | WPML — Arabic mirror at `/ar/المنافسات/tenders-list/` (same widget ids, same params) |

`wp-json` is locked down (`rest_forbidden`, 401) for anonymous users — including
`/wp/v2/tender` and `/wp/v2/tender-status`. `admin-ajax.php` is present but not used
for the tender grid.

## 2. How the interactions work

All four controls are **server-rendered off query params on the same URL**. The inline
JS (`ajaxSwapGrid`) does `fetch(url, {headers:{'X-Requested-With':'fetch'}})`, parses the
returned HTML with `DOMParser`, and replaces `#tender-grid`'s `innerHTML`, then
`history.replaceState`. The Elementor status filter does the same thing natively.

So **any param combination can be fetched with a plain GET and parsed as HTML.**

| Param | Meaning |
|---|---|
| `e-page-3085e1f=<n>` | page number, 1-based, 24 cards/page. Empty grid = past the end. |
| `e-filter-3085e1f-tender-status=<slug>` | status filter. Comma-separated slugs = OR. |
| `q=<text>` | free-text over title + tender ID. JS fires at ≥3 chars (350 ms debounce); server accepts any length. |
| `date_from=dd/mm/yyyy` | **opening date** range start |
| `date_to=dd/mm/yyyy` | opening date range end |

Notes
- Param name is `e-filter-<GRID_ID>-<TAXONOMY>` — i.e. keyed on the **grid** id `3085e1f`, *not* the filter widget id. `data-filter` on the buttons carries just the slug.
- Changing the status filter clears `q` / `date_from` / `date_to` (`clearSearchIfFiltersChanged`).
- Filter + search + dates combine as AND.
- No CSRF/nonce and no rate limiting observed on the list URL.

## 3. Status taxonomy

Slugs come off the loop-item's CSS classes (`tender-status-<slug>`); a tender can carry more than one.

| Slug | Label (EN / AR) | Badge colour | Count (17 Sep 2026) |
|---|---|---|---|
| `available-new` | Available / New · متاحة / جديد | `#0097CA` | 10 |
| `available-updated` | Available / Updated · متاحة / محدثة | `#EC6A11` | 3 |
| `direct-purchase` | Direct Purchase · الشراء المباشر | `#6C63D9` | 35 |
| `under-studying` | Under Studying · تحت الدراسة | `#FFE23B` | 36 |
| `initial-results` | Initial Results · النتائج الأولية | `#902333` | 4 |
| `final-results` | Final Results · النتائج النهائية | `#98CB33` | 41 |
| `cancelled` | Cancelled · ملغاة | `#898989` | 5 |
| `always-available` | (modifier) renders dates as "Always available", sorts first (`order:-1`) | — | 0 |
| `hidden` | exists on tender pages but excluded from the grid | — | 0 in grid |

## 4. Card fields (loop template 272)

Elementor widget `data-id`s are stable per template — use them as selectors.

| Field | Selector inside `.e-loop-item` |
|---|---|
| Tender ID (e.g. `NDP0864/26`) | `[data-id="3317f29"]` |
| Status label | `[data-id="d1dd940"]` |
| Title (AR variant, shown in AR) | `[data-id="0e2104d"]` |
| Title (EN variant, shown in EN) | `[data-id="e400466"]` |
| Submission Deadline (`Sun, 20/09/2026`) | `[data-id="7e0a2fb"]` |
| Bid Opening | `[data-id="f40fb4d"]` |
| Detail URL | first `a[href]` (also the "Discover More" button `[data-id="66fe4bf"]`) |
| WP post id + status slugs | the item's `class` attr: `post-<id>`, `tender-status-<slug>` |

Titles are stored twice (one with `–`, one with `-`); CSS hides one per language. Most
titles are Arabic even on the EN site.

## 5. Detail page (`/tender/<slug>/`)

Adds fields the card doesn't carry:

- **Opening Date** (card only has submission deadline + bid opening)
- **Tender Booklet Price** + `SAR` (present on 94 of 133 listed tenders)
- Attachment buttons (`a.elementor-button`), any of:
  - `Buy This Tender` → `https://tenders.nupco.com/redirect/redirect.jsp?url=https://srm.nupco.com/sap/bc/webdynpro/sap/zsrm_sadad_supplier_app?tenderid=<ID>` — SAP SRM/SADAD supplier app, tender ID passed in the URL
  - `Tender Item List` → `.xlsx` line-item file under `/wp-content/uploads/<yyyy>/<mm>/`
  - `Terms & Conditions` → `.pdf`
  - `Tender Preliminary Result`, `Tender Final Result`, `Additional Attachments`

The item-list `.xlsx` is the richest structured artefact on the site — actual SKU-level
demand per tender.

## 6. Coverage gap worth knowing

- Grid today: **133** tenders (6 pages).
- `https://www.nupco.com/tender-sitemap.xml`: **229** tender URLs.
- 97 tender pages are live but not in the grid (status `hidden`, or no `tender-status`
  term). They still resolve, still carry prices and attachments. Spot-checked example:
  `/tender/39229-2/` = NPT0057/25, booklet price 13,800 SAR, full result PDFs.

For a complete historical crawl, **drive off the sitemap, not the grid.**

## 7. Files

- `nupco_tenders.py` — scraper. `python3 nupco_tenders.py --details` → JSON + CSV.
  Flags: `--status`, `--q`, `--date-from`, `--date-to`, `--details`, `--out`.
- `nupco_tenders.json` / `.csv` — 133 tenders with detail-page fields, pulled 17 Sep 2026.
