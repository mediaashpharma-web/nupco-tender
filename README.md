# NUPCO tender pipeline

Monitors Saudi NUPCO's public tender portal: crawls every tender, versions the
attached documents, parses the Tender Item Lists into searchable line items,
records every change over time, and serves it as a searchable web app.

Runs itself daily, for free.

```
GitHub Actions  ──▶  Supabase Postgres  ──▶  Render API  ──▶  Vercel frontend
  (daily crawl)         (the data)            (JSON)          (the search page)
```

---

## Why this shape

Each piece is where it is for a reason, not by preference:

* **The crawl is on GitHub Actions**, not a hosted cron. Render's cron jobs are
  not in the free tier at all, and Vercel's hobby functions time out long
  before a crawl that takes 10–30 minutes. Actions gives a real Python
  environment with no time pressure.
* **The data is on Supabase**, not Render Postgres. Render's free Postgres
  expires 30 days after creation; Supabase's free 500 MB tier does not.
* **Documents are parsed but not stored.** The hash and the extracted line
  items are what matter; 800 MB of PDFs would cost more than the intelligence
  in them. Change detection compares hashes, so nothing is lost, and the UI
  links to the file on nupco.com.

## Local development

```bash
pip install -r requirements.txt
python -m pipeline.run backfill     # first load into db/nupco.db
python api/app.py                   # http://127.0.0.1:8000
```

With no `DATABASE_URL` the whole stack runs on a local SQLite file and keeps a
full document archive. Set `DATABASE_URL` and the same code talks to Postgres
and stops storing files. One codebase, two engines — see `pipeline/db.py`.

## Commands

```
python -m pipeline.run backfill        full load
python -m pipeline.run incremental     daily refresh (the default)
python -m pipeline.run reparse         re-parse stored files, no downloads
python -m pipeline.run reclassify      re-apply category rules, no re-parse
python -m pipeline.run fetch-missing   re-download documents absent from disk
python -m pipeline.report              markdown digest of the last run
python -m tests.test_pipeline          34 tests, no network needed
```

## How the crawl works

**There is no API.** nupco.com is WordPress + Elementor Pro; the tender list,
its status filter, search and pagination are all GET parameters on one URL, and
`wp-json` is locked. `docs/SITE-NOTES.md` has the full parameter and selector
reference, including the Elementor widget ids to re-check if the page is ever
rebuilt.

**Two discovery sources.** The grid (6 requests, ~130 tenders, carries status
and dates cheaply) and the sitemap (~225 live tender pages, including ~95 that
are `hidden` and never appear in the grid but still carry prices, item lists
and final results). Both are keyed on the WordPress `post_id`, because tender
ids are reused across archived duplicate pages.

**What gets re-checked.** The grid is always re-scanned in full — it is the
only way to catch a tender *entering* `final-results` or `cancelled`, which is
exactly the transition that matters. Terminal tenders are frozen only against
the expensive work, and any cheap signal (changed sitemap `lastmod`, changed
card, changed document URL) unfreezes them.

**File change detection, cheapest signal first:** URL differs → changed, no
network needed (NUPCO republishes revisions at new `/uploads/` paths); then
HTTP `HEAD` on ETag/Last-Modified/size; then SHA-256. Only a hash change
creates a new version, and old versions are kept.

**Nothing is ever deleted.** A tender that leaves the grid has gone `hidden`,
not away: `is_listed` flips and `last_seen_at` stays. The delist sweep is
skipped entirely if discovery hit an error, so a half-failed run can never look
like a mass deletion.

## The item lists

The Tender Item List is the real prize — SKU-level demand. NUPCO publishes it
in at least four shapes, and roughly three in four are PDFs rather than
spreadsheets. All are text-based, so no OCR is needed, and all normalise onto
one schema. Borderless PDFs get a content-driven parser rather than a geometric
one: the DESCRIPTION header is narrow and centred while its data spans half the
page, so x-position bucketing shreds descriptions. Anchoring on what values
*look like* — a 10–14 digit code, a leading serial, a trailing quantity, a UOM
token — is far sturdier.

Anything that fails to parse lands in `parse_failures` with a reason rather
than being silently dropped.

### Classification uses NUPCO's own taxonomy
There is no category field on the portal, but the first two digits of the
13-digit SAP material code partition the catalogue properly:

| Group | Meaning |
|---|---|
| `51` | **pharmaceuticals** — drugs, vaccines, IV solutions |
| `41`, `45` | lab / diagnostics — reagents, assays, media |
| `42` | medical and dental devices and consumables |
| `40` | equipment — machines, anaesthesia, sterilisers |
| `46` | general consumables |

`code_group` stores the raw digits as fact; `category_guess` blends them with
keyword rules that only override where strictly more specific (dental sits
inside group 42; delivery and warranty lines are service whatever their code).

## Deployment

**Secrets.** `DATABASE_URL` is the Supabase connection string. It goes in two
places, both set by hand and never committed:

* GitHub → Settings → Secrets and variables → Actions → `DATABASE_URL`
* Render → the service's Environment tab → `DATABASE_URL`

**First load.** Run the `NUPCO daily refresh` workflow manually with mode
`backfill`. It takes 20–40 minutes and populates Supabase from scratch.

**Frontend.** `web/config.js` holds the Render API URL. Vercel serves `web/`
as a static site; the page falls back to same-origin, so the Render service can
also serve it directly for testing.

**Free-tier things to know.** Render free web services sleep after 15 minutes
idle and take about a minute to wake. Supabase free projects pause after a week
of inactivity — the daily run keeps it awake. GitHub disables scheduled
workflows after 60 days without repo activity, which the workflow's own
keepalive commit prevents.
