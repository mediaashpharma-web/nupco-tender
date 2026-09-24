# JONEPS site notes

How Jordan's e-procurement portal (<https://joneps.gov.jo>) exposes medicine
tenders, as reverse-engineered in September 2026. Read this first when the
crawler breaks: it records what each request needs and which assumptions the
parsers make, so a layout change can be traced to the step it broke.

## Shape of the site

A Java web application (`.do` URLs, `JSESSIONID` cookie, Spring-style CSRF
token). Server-rendered HTML for lists and award pages; a small Vue app, fed
by a JSON endpoint, for the drug lines of a tender. There is no `robots.txt`
(404). The OCDS pilot the portal mentions is not published anywhere public.

The portal has an English locale: `GET /um/setLocale.do?lang=en` switches the
session. Labels in `pipeline/joneps.py` were taken from it, not translated.

## 1. Tender list

`POST /ep/invt/selectListTendInvitAL.do`, form-encoded, needs `_csrf` from any
earlier page of the same session.

| Parameter | Meaning |
|---|---|
| `searchTendTypeCd1=EP0016` | Tender type **Medicines** (`EP0011` supplies, `EP0012` services, `EP0014` technical services, `EP0015` works) |
| `searchTendTypeCd2=EP04xx` | Therapeutic class, `EP0401`-`EP0417`. The page's JS copies `tendTypeCd2Medicine` into this before submitting; sending it directly works |
| `searchFiscalYear` | `2018`-current. Note tender *numbers* can carry the next year (a `2026…` tender in fiscal 2025) |
| `searchTendStatusCd` | Award axis: `Opened`, `Initial_Awarded`, `Final_Awarded` |
| `searchTendStatCd` | Lifecycle axis: `EP0034` cancelled, `EP0035` re-tendered, `EP0854` frozen, `EP0560`/`EP0561` technical/financial opening published, `EP0032`/`EP0190`/`EP0856` amended |
| `recordCountPerPage` | UI offers 5-20; **the server honours 100** |
| `currentPageNo` | 1-based |

Traps, both of which broke an early version:

- **The result count uses a dot as the thousands separator.** `1.045 النتائج`
  is 1,045 results, not 1.
- **An out-of-range page returns the LAST real page, not an empty one.** Loop
  "until a page is empty" never ends on a result that is an exact multiple of
  the page size. Stop on the stated page count (`(الصفحة 1/11)`) and on any
  page that adds no new row.

Each row's link is `fn_goDetail(tendNo, tendSeq, frameworkAgreementSeq,
tendCategCd, workType, intrlLocalTypeCd, tendMthodCd, tendTypeCd1)`. Columns:
number, title (Arabic), buying entity, type, publication date, deadline
(`dd/mm/yyyy`). The row carries neither status nor class: those are learned by
re-running the list with each filter, which costs a few requests per year
instead of one per tender.

List pages are slow, about 4 seconds each at 100 rows. Discovery for one year
is about 50 requests.

## 2. Tender page

`GET /ep/invt/MedicineDetails.do?tendNo=…&tendSeq=…&frameworkAgreementSeq=&tendCategCd=EP1313&workType=&intrlLocalTypeCd=…&tendMthodCd=…&invtYn=Y&searchConditions=`

Works as a plain GET with no session, so it is the link shown to people. The
page is a shell; its tabs load by AJAX via `fn_selectAllData(tendNo, tendSeq,
fa, url, …)`, a GET with `tendNo`, `tendSeq`, `frameworkAgreementSeq`, `_csrf`:

| Tab | Endpoint | Public |
|---|---|---|
| General / tender info | `/ep/invt/medicine/generalInformation.do`, `/tenderInformation.do` | yes |
| Fee, conditions, attachments | `/ep/invt/medicine/feeBidGuaranteeDetail.do`, `conditionDetail.do`, `attachmentDetail.do` | yes |
| Drug information | `/ep/invt/compMedicineDetails.do` - a Vue shell, see 3 | yes |
| Bid opening results | `/ep/invt/openRsultMedicine.do` | **no - 404** |
| Award results | `/ep/invt/awrdRsultMedicine.do` | final awards only |

The tabs work in a fresh session: the detail page need not be opened first.

## 3. Drug lines - JSON

`GET /ep/tender/MDgoods.do?tendNo=…&tendSeq=…&intrlLocalTypeCd=…&tendCategCd=EP1313&isFrameworkAgreement=N&page=1&pageSize=500`

`pageSize` is honoured, so one request returns every line. Response
`{total, list: [...]}`; the useful fields of each line:

| Field | Example |
|---|---|
| `clNmEn` / `clNmAr` | `Imatinib` / `إيماتينيب` - INN generic name |
| `rdlGenerNm` | `IMATINIB TABS/CAP 400 MG` - name, form, strength |
| `rdlItemCd` | `08-030500-025` - Jordan RDL code (stored without dashes) |
| `clId` | `51112005` - UNSPSC; segment 51 is pharmaceuticals, as in NUPCO's SAP groups |
| `totlQty`, `unitMesurNm` | `1080`, `TAB/CAP` |
| `details[]` | per-entity demand: `deNo`, `abbrNm` (`PHH`), `totlQty` |

## 4. Award

The award tab lists decisions as `fn_goDetailAwrdDcsion(awrdDcsionNo,
awrdDcsionSeq)`. Each opens:

`POST /ep/evaw/selectDetailAwardDecsn.do?tendCategCd=EP1313&awrdDcsionNo=…&awrdDcsionSeq=…&tendTypeCd1=EP0016&tendNo=…&tendSeq=…&intrlLocalTypeCd=…` with `_csrf`.

Server-rendered. One header table (decision number, status, publication date,
buyer, beneficiaries), then repeating **(supplier x beneficiary)** blocks: a
label/value supplier table, then an items table. Each awarded item spans up to
four rows:

1. item no, UNSPSC, RDL, pack `30 [TAB]`, unit size, free goods %, discount %,
   awarded quantity, total value `6840.000 JOD`, reason
2. offer no, `[scientific name]`, registration `108/1995 [مسجل]`, final price `190.000 [JOD]`
3. manufacturer, country of origin, shelf life, brand / description
4. special conditions (optional)

Traps:

- **Tax basis is per items table, not per page.** The price column header says
  `السعر النهائي شامل الضريبة` (incl. tax) or `… غير شامل الضريبة` (excl.), and
  on joint tenders it differs between blocks: Royal Medical Services buys
  tax-exempt, civilian hospitals do not.
- **Price is per pack; quantity is in base units; free goods are netted out of
  the total.** 19,000 minims, packs of 30, 18% free: 537 paid packs x 1.775 =
  953.175. The comparable figure is `total_value / awarded_qty`, stored as
  `unit_cost`. Comparing `unit_price` across tenders compares packs of 30 with
  packs of 1.

Initial awards are not public (the tab 404s until an award is final).

## 5. Keys

`post_id = 10^12 + tendNo * 100 + tendSeq`. Deterministic, so every run agrees,
and far above NUPCO's WordPress ids. It needs `BIGINT` everywhere - it is why
the child tables' `post_id` was widened from `INTEGER`.
