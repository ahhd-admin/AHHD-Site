# AHHD Production Synchronization Strategy & Repository Audit

**Project:** Accredited Home Healthcare Directory (AHHD)
**Document Version:** 4.0 (merged)
**Status:** Proposed Production Architecture + Engineering Audit, Open Findings
**Last Updated:** 2026-07-27
**Merged from:** `AHHD-Production-Synchronization-Strate.md` (v1.0), `repo-audit.md` (v1.0), `nightly-fixes.md`, and `issue-log.md`

---

# Purpose

This document merges four prior documents into a single reference:

1. **The target architecture** — the intended long-term synchronization design between the ACHC directory, the scraper, Google Sheets, and the production database.
2. **The repository audit** — a second-pass architectural review of the current implementation, identifying gaps between what exists today and the target architecture above.
3. **The nightly workflow & fixes** — a concrete, step-by-step nightly execution flow (including the validation gate and failure branch) plus a prioritized list of fixes needed to bring the current implementation in line with the target architecture.
4. **The engineering issue log** — a file-level engineering audit assigning formal severity IDs (P0/P1/P2) to each finding, used below as the canonical numbering for Part 2.

Part 2 below has been re-verified against the actual codebase as of 2026-07-27 (not just against the prior audit narratives) — several previously-"Open" items turned out to already be fixed in code, and two previously-**undocumented, pipeline-breaking bugs** were found and fixed in the process. See the "Verified This Session" callout at the top of Part 2.

The primary goals of the overall strategy are:

- Treat ACHC as the single source of truth.
- Preserve historical information without introducing stale or manually entered data.
- Avoid unnecessary writes to Google Sheets and Supabase.
- Detect meaningful provider changes over time.
- Handle accreditation removals safely.
- Create a fully auditable synchronization pipeline.

---

# Executive Summary (from Audit)

Overall the project has a solid foundation and demonstrates thoughtful separation between scraping, normalization, synchronization, and database loading.

However, there are several areas where the architecture still behaves more like a prototype than a long-term production synchronization engine.

The largest remaining theme is:

**The system currently favors preserving previous data rather than treating ACHC as the sole authoritative source.**

Future work should continue moving toward:

```
ACHC
↓
Snapshot
↓
Compare
↓
Update only true changes
```

rather than

```
Previous data
+
New scrape
↓
Merged result
```

---

# Part 1 — Target Architecture

## Source of Truth / Immutable Source Philosophy

### Production Rule

The authoritative source for provider information is the ACHC website.

No provider information should originate from:

- Sample datasets
- Manually entered spreadsheet rows
- Previous versions of the database
- Google Places
- Any third-party provider directory

Existing production data exists only for:

- Historical comparison
- Change detection
- Audit history
- Missing-record tracking

It must never be used to "fill in" missing ACHC information.

Future development should continue reinforcing one guiding principle:

```
ACHC
↓
Immutable Source
```

The scraper should never attempt to improve ACHC. Instead:

```
ACHC
↓
Normalize
↓
Compare
↓
Store
↓
History
```

Production records should always represent exactly what ACHC currently reports. Historical tables explain how that information has changed over time.

---

## Data Flow

```
ACHC Website
      │
      ▼
Complete National Scrape
      │
      ▼
Raw ACHC Snapshot
      │
      ▼
Normalization
      │
      ▼
Snapshot Comparison
      │
      ├──────────────┐
      │              │
      ▼              ▼
Changed?         Unchanged?
      │              │
      ▼              ▼
Update DB      Update metadata only
      │
      ▼
Historical Log
```

The comparison happens AFTER a complete scrape.

The production tables should never be modified while the scraper is still collecting data.

---

## Guiding Principles

1. ACHC is the single source of truth.
2. Every nightly run should scrape the complete ACHC directory — scraping everything ensures confidence that nothing was missed.
3. The database (and Google Sheets, and Supabase) should only update records that have actually changed — writing only changes minimizes unnecessary writes and history entries. Scraping completely and writing selectively are separate concerns.
4. Historical data should never overwrite current ACHC data.
5. Every production decision should be traceable to a specific scrape run.

---

## Nightly Synchronization Workflow

The full nightly run is a 9-step pipeline with an explicit validation gate between scraping and any write to production.

```
Nightly Trigger
      │
      ▼
Run Complete ACHC Scrape
      │
      ▼
Validate Scrape Completion
      │
      ├─────────────── NO ────────────────┐
      │                                   │
      ▼                                   ▼
YES                               Log Failure
      │                          Send Notification
      ▼                                   │
Normalize ACHC Data                End Run
      │
      ▼
Generate Record Hashes
      │
      ▼
Compare Against Production
      │
      ├───────────────┬───────────────┬───────────────┐
      │               │               │
      ▼               ▼               ▼
New            Changed        Unchanged
      │               │               │
 Insert         Update      Update Metadata Only
      │               │               │
      └───────────────┴───────────────┘
                      │
                      ▼
Compare Missing Records
                      │
                      ▼
Update Missing Counters
                      │
                      ▼
Generate Daily Report
                      │
                      ▼
Sync Production Database
                      │
                      ▼
Complete
```

### Step 1 — Complete Scrape

Run a complete ACHC scrape. Scrape every provider currently listed by ACHC.

Expected result: approximately 4,600 providers (subject to ACHC growth).

Do not overwrite production data during scraping. Instead, create an isolated snapshot.

Example:

```
scrape_run_id = 2026-07-27T02:00:00Z
```

### Step 2 — Validate Scrape Completion

Before continuing, verify:

- Browser completed successfully
- Every expected program searched
- No pagination failures
- No selector failures
- Provider count within expected range
- No critical scraper exceptions

(See Complete Scrape Requirement below for the full definition of an incomplete scrape.)

If validation fails:

```
Abort synchronization.
```

- Do not update production.
- Do not mark providers inactive.
- Do not overwrite Google Sheets.
- Log the failure and send a notification, then end the run.

### Step 3 — Normalize Data

Normalize only ACHC-provided information.

Allowed:

- whitespace cleanup
- capitalization normalization
- phone formatting
- ZIP formatting
- state abbreviation standardization
- service ordering

Do NOT:

- invent missing values
- merge sample data
- preserve stale information
- enrich from Google Places

### Step 4 — Generate Stable Record Hash

Generate a hash from authoritative ACHC fields.

Suggested fields to hash:

```
provider_name
street_address
city
state
zip
phone
services
accreditation_status
source_url
```

Do NOT hash:

```
timestamps
coordinates
last_seen
internal IDs
metadata
```

Normalize before hashing (sort services alphabetically, consistent capitalization, standardized phone formatting). If two hashes are identical, the ACHC record has not changed. Purpose: quickly determine whether a provider changed.

### Step 5 — Compare Against Production

Each provider falls into one of four categories:

**New** — identity not previously found. Action: insert provider.

**Changed** — identity exists, hash changed. Action: update provider, write history entry.

**Unchanged** — identity exists, hash identical. Action: do NOT rewrite provider fields. Update only `last_seen_at` / `last_scrape_run_id`. Nothing else changes.

**Missing** — previously existed, not found in current scrape. Action: increment missing counter. Do NOT deactivate immediately.

### Step 6 — Missing Provider Verification

See Missing Provider Strategy below for the full state machine (`ACTIVE → MISSING_ONCE → PENDING_INACTIVE → INACTIVE`, reset to `ACTIVE` if the provider reappears).

### Step 7 — Historical Logging

Write history only when meaningful changes occur, e.g.:

```
Phone changed
Address changed
Services changed
Accreditation changed
```

Avoid history entries for `last_seen` updates, timestamps, or routine metadata.

### Step 8 — Synchronize Production

Production synchronization should only happen after:

```
Scrape Complete + Validation Passed + Comparison Complete
```

Never synchronize after: a partial scrape, timeout, browser crash, or selector failure.

### Step 9 — Generate Daily Summary

Produce a report for the run (see Production Metrics below for the full field list). Example:

```
Run ID              2026-07-27
Providers Found     4618
New                 3
Updated             6
Unchanged           4609
Missing             2
Programs Failed     0
Duration            22 minutes
Status              SUCCESS
```

---

## Provider Identity

Current implementation uses:

```
provider_name + state
```

This is insufficient. Multiple locations may share a provider name and state, causing different offices to collapse into one record.

**Audit finding:** within a single scrape, services from duplicate providers are merged. This is only correct if both rows truly represent the same physical location. If provider identity is incorrect, different locations inherit each other's accreditation services. Provider identity must be strengthened before any merge occurs — never merge based solely on provider name + state.

Future identity priority:

1. ACHC unique ID (preferred)
2. ACHC provider detail URL
3. Stable ACHC record identifier
4. Provider name + full street address

Avoid provider name + state.

---

## Historical Tracking

Maintain two concepts:

**Current State** — the latest verified ACHC information.

**Historical State** — every meaningful change.

History should only be written when ACHC data changes.

Example history event:

```
Phone changed
Old: (312) 555-1111
New: (312) 555-4444
```

---

## Missing Provider Strategy

A provider disappearing from ACHC does NOT automatically mean accreditation expired, the provider closed, or the provider was removed.

Possible explanations: scraper failure, timeout, selector change, temporary ACHC issue, search indexing issue.

Therefore: never deactivate after one scrape.

Recommended state machine:

```
ACTIVE
  ↓
MISSING_ONCE
  ↓
PENDING_INACTIVE
  ↓
INACTIVE
```

Suggested policy:

- Run 1: `missing_count = 1`, `status = ACTIVE`
- Run 2: `missing_count = 2`, `status = PENDING_INACTIVE`
- Run 3: `missing_count = 3`, `status = INACTIVE`

If provider reappears: `missing_count = 0`, `status = ACTIVE`.

---

## Complete Scrape Requirement

Missing counts should ONLY increase if the scrape is considered complete.

A scrape is incomplete if: timeout occurred, scraper crashed, major selector failures, unexpectedly low provider count, ACHC unavailable, or required program searches failed.

Incomplete runs should never deactivate providers.

**Audit finding:** current logic determines scrape completeness primarily by counting failed program searches. Future production should validate: expected provider count, every required program, pagination completion, selector success, browser success, network success, and duplicate detection. Require all validation checks before marking providers inactive, updating accreditation status, or generating change reports.

---

## Database Tables

### `achc_locations`

Represents current production state. Suggested fields:

```
provider_id, provider_name, street_address, city, state, zip, phone,
services, source_url, record_hash, status, first_seen_at, last_seen_at,
last_changed_at, inactive_at, missing_count, last_scrape_run_id
```

### `achc_scrape_runs`

Tracks every scrape. Suggested fields:

```
run_id, started_at, completed_at, status, records_found, records_new,
records_changed, records_unchanged, records_missing, warnings, duration, notes
```

### `achc_location_history`

Stores only changes. Suggested fields:

```
history_id, provider_id, changed_at, old_values, new_values,
changed_fields, scrape_run_id
```

---

## Google Sheets

**Decision (2026-07-27): Google Sheets is a read-only backup/audit trail, not a synchronization layer.** Rather than evolving Sheets into a synchronization layer (the original recommendation below), the simpler and lower-risk path was chosen: Sheets is written to by the scraper (`doPost`/`replace_raw_only` → `Raw_ACHC`/`Normalized_ACHC` tabs) purely so a human (e.g. the founder) can open the spreadsheet and look up what was scraped. Nothing automated reads from Sheets back into Supabase anymore — the `sync-google-sheets` Edge Function and the `daily-scraper.yml` step that invoked it have been retired (see P0-020). Production writes happen exclusively through `write_to_supabase_direct()`. This removes the entire class of "stale Sheets data re-synced into production" risk (P0-004) and the unsecured-endpoint risk (P0-020) in one move, at the cost of Sheets no longer being usable as a synchronization mechanism if one is ever wanted again later.

Original recommendation (superseded by the above, kept for context): Google Sheets should act as a synchronization layer, not the permanent source of truth, with rows representing the latest verified ACHC snapshot and no accumulation of historical rows. The audit had found the prior role was partially storage, history, synchronization, and temporary workspace all at once:

```
Current snapshot
  ↓
temporary synchronization layer
```

Historical storage belongs elsewhere.

---

## Geocoding

Coordinates are derived data — they are NOT supplied by ACHC. Store separately:

```
latitude, longitude, geocoded_at, geocoder_version
```

Changing coordinates should not imply ACHC changed.

**Audit finding — coordinates may become stale:** current behavior retains existing coordinates if geocoding fails. This is useful for temporary failures but dangerous if the address changed, the provider moved, old coordinates originated from sample data, or the previous geocode was incorrect. Coordinates should only persist if the normalized address remains identical. If the address changes, latitude/longitude should be set to `pending` until successfully re-geocoded.

---

## Google Places

Current recommendation: **disable as a production data source.**

Reason: Google Places introduces information that did not originate from ACHC (websites, business hours, ratings, photos, phone corrections).

If used later, clearly distinguish `ACHC Data` vs `Derived Data`. Never merge Google Places into authoritative ACHC fields.

---

## Caching

Persistent caches should survive between runs. Candidate caches: `geocode_cache`, `places_cache`.

Do not recreate every night. Recommended storage: Supabase, dedicated database tables, or persistent object storage. Avoid GitHub runner local storage — GitHub runners are ephemeral.

---

## Status Field Design (from Audit)

Current implementation overloads `confidence_status`, which represents multiple concepts at once (`verified`, `changed`, `possibly_inactive`, `incomplete_scrape`). These are different concerns and should be separated into dedicated fields:

```
record_status:       ACTIVE | PENDING_INACTIVE | INACTIVE
verification_status:  VERIFIED | NEEDS_REVIEW
change_status:        NEW | UPDATED | UNCHANGED
scrape_status:         COMPLETE | FAILED | INCOMPLETE
```

This greatly simplifies downstream logic.

---

## Production Metrics

Every completed scrape should produce a report. Combined suggested fields (from both documents):

```
Run ID
Started At / Completed At / Duration
ACHC providers found
New providers (Providers Added)
Changed providers (Providers Updated)
Unchanged providers
Missing providers (Providers Removed)
Programs Searched / Programs Failed
Database inserts / updates / unchanged
Warnings / Errors
Status (SUCCESS / FAILED)
```

Example:

```
ACHC providers found     4618
New providers            2
Changed providers        5
Unchanged providers      4609
Missing providers        2
Database inserts         2
Database updates         5
Database unchanged       4609
Run duration              24 minutes
Status                    SUCCESS
```

These metrics become invaluable when debugging.

---

# Part 2 — Current Repository Issues & Recommended Fixes

Numbering below follows `issue-log.md`'s severity-ID scheme (P0 = Critical, P1 = High, P2 = Medium — see Severity Definitions), which is more rigorous than the two earlier audit passes and is now the canonical reference. Items are marked **Fixed** only where verified directly against the code during this merge (2026-07-27); everything else is carried over as **Open** exactly as found.

## Verified This Session (2026-07-27)

While confirming P0-001 against the actual code, two things came up that no prior audit document had caught:

- **P0-001 was already fixed** — `scrapers/geocode_helper.py` was fully rewritten (Nominatim → Google Maps Geocoding API) in the current uncommitted working tree and consistently uses `street_address` throughout. Verified via `git diff` and the existing test suite (`test_geocode_helper.py`, 15/15 passing).
- **A new, inverse field-mismatch bug** was found and fixed in `scrapers/achc_scraper.py`: `write_to_supabase_direct()` (the primary nightly write path — `ENABLE_DIRECT_SUPABASE` defaults to `true`) renamed `provider_name`→`organization` and `street_address`→`address` before POSTing to the `merge_google_sheets_data` RPC. But that RPC reads `row_data->>'provider_name'` and `row_data->>'street_address'` exclusively (verified against `supabase/migrations/20260101000000_merge_google_sheets_data.sql`) — it never reads `organization` or `address`. Every direct-to-Supabase write was silently landing `address_line_1` (and effectively `location_name`) as blank. **Fixed:** removed both remap lines; extracted the logic to a module-level `prep_row_for_supabase()` and added a regression test suite (`scrapers/test_prep_row_for_supabase.py`, 8 tests) asserting `provider_name`/`street_address` now pass through unchanged.
- **A separate, more severe, entirely undocumented bug** was found while running the test suite: `normalize_row()` in `scrapers/achc_scraper.py` referenced `dba_name` (`dba_name = _html.unescape(dba_name)`) **before** it was ever assigned (the assignment happened 11 lines later). This is a hard `UnboundLocalError` on every single row — meaning **the scraper could not complete a normalization pass at all**, in test mode or otherwise. This predates today's session (confirmed present in `HEAD`, not part of any uncommitted diff) and was not flagged in any of the three prior audit documents. **Fixed:** moved the `dba_name` assignment to the top of the function, before first use, and removed the now-redundant duplicate assignment. Confirmed by re-running the full test suite: `test_normalize.py` went from 26 failing / 23 passing to all passing.

This means the pipeline was not merely running in test mode — it could not have completed a real normalization pass in its pre-session state. Verify this fix by running a manual/`workflow_dispatch` scrape before making any decisions about flipping `LIMIT_LOCATIONS`/`TEST_MODE`.

## Severity Definitions

**P0 — Critical.** Will likely cause incorrect provider data, failed nightly runs, data corruption, production outage, or incorrect accreditation information. Must be addressed before production deployment.

**P1 — High.** System functions but risks stale data, incorrect synchronization, future bugs, or scalability problems. Should be completed before long-term production.

**P2 — Medium.** Improves maintainability, observability, reliability, documentation, or developer experience.

## Priority 1 — Critical (P0)

- **P0-001 — Geocoder expected incorrect field name** (`scrapers/geocode_helper.py`). Status: **Fixed** (verified this session — see above).
- **P0-001b — `prep_row_for_supabase` remapped `street_address`/`provider_name` away from what the RPC reads** (`scrapers/achc_scraper.py`). Status: **Fixed** (verified + fixed this session — see above).
- **P0-001c — `normalize_row()` referenced `dba_name` before assignment, crashing every normalization pass** (`scrapers/achc_scraper.py`). Status: **Fixed** (found + fixed this session — see above).
- **P0-002 — Provider identity is not unique** (`provider_name + state`, `scrapers/achc_scraper.py`). Organizations with multiple locations in one state collapse into a single record, risking corruption of addresses, phone numbers, services, geocoding, and accreditation data. Fix priority: ACHC record ID → ACHC detail URL → provider name + normalized street address. Never merge on name alone. Requires updates to normalization, the comparison engine, Google Sheets, Supabase, and historical tracking. (See Provider Identity section above.) Status: Open.
- **P0-003 — Nightly workflow still configured for testing** (`LIMIT_LOCATIONS=3`, `TEST_MODE=true` in `.github/workflows/daily-scraper.yml`) — confirmed still present as of 2026-07-27. Status: Open — hold until P0-001c fix is verified via a manual/`workflow_dispatch` run.
- **P0-004 — Production synchronization may execute after a test scrape.** Test scrape should never trigger production synchronization. Status: **Resolved 2026-07-27** — see P0-019/P0-020 below; the Sheets→Supabase sync path this issue depended on has been retired entirely, so a test scrape can no longer trigger production synchronization through that route. Production synchronization now happens exclusively via `write_to_supabase_direct()`, gated on a real completed scrape.
- **P0-005 — Google Sheets / RPC success is not actually verified.** HTTP 200 is treated as success even when Apps Script returns `{"ok": false}`. Verify HTTP status **and** `response.ok == true` before continuing; fail the run otherwise. (See fail-closed pipeline note below.) Status: Open.
- **P0-006 — `SCRAPER_SECRET` not consistently passed** to every production workflow (GitHub Actions / Apps Script) — missing secret can result in unauthorized writes. Pass it to every production workflow and fail closed if missing. Status: Open.
- **P0-007 — Services only grow, never shrink** (`google-sheets-script.js`) — union of old + new services means removed ACHC services remain forever. Replace services using the latest verified ACHC snapshot; write removed services into history. Status: Open.
- **P0-008 — Comparison logic ignores important fields.** Primarily checks name/address/city/state/ZIP; changes to phone, accreditation, or services may never trigger an update. Generate a normalized record hash over all authoritative ACHC fields instead. Status: Open.
- **P0-009 — Coordinates may become stale** when the address changes but geocoding fails — old coordinates are retained. When the normalized address changes: `latitude = null`, `longitude = null`, `status = pending_geocode` until re-geocoded. (See Geocoding section above.) Status: Open.
- **P0-010 — Historical data currently influences current records**, contrary to the core principle that ACHC alone is the source of truth. Current provider data should always reflect the latest verified ACHC snapshot; historical data should never populate missing ACHC fields. (See Source of Truth section above.) Status: Open.
- **P0-018 — Canonical schema mismatch across systems** — different components use different field names for the same concept (e.g. `organization` vs `provider_name`, `address` vs `street_address`). Define one canonical schema; every component maps to it. Note: this is the exact class of bug behind P0-001b above, so this item should be treated as materially more urgent than "documentation cleanup." Status: Open (partially mitigated by the P0-001b fix, but the underlying lack-of-single-schema risk remains).
- **P0-019 — Supabase sync may request the wrong endpoint / mishandle the response.** Checked this session: `supabase/functions/sync-google-sheets/index.ts:53` already correctly called `${googleSheetsUrl}?action=get_normalized` and checked `sheetsResponse.ok` before parsing — this specific concern was not reproducible. Status: **Moot as of 2026-07-27** — the function has been retired (see P0-020).
- **P0-020 — Supabase sync endpoint insufficiently protected.** The function ran with the service-role key internally but had no application-level secret check — the only gate was Supabase's platform JWT verification, satisfied by the **anon key** (a key that is, by design, safe to ship client-side). Anyone who extracted the anon key from the site's own client traffic and found the function URL could have triggered a full production sync merge at will. **Decision (2026-07-27): retire the endpoint rather than harden it.** Google Sheets is being repositioned as a read-only backup/audit trail — the scraper writes to it (`doPost`/`replace_raw_only`), but nothing reads from it back into Supabase anymore, so this endpoint served no purpose worth the residual risk. `supabase/functions/sync-google-sheets/index.ts` removed from the repo and the `daily-scraper.yml` "Trigger Supabase sync" step that invoked it removed. Status: **Code retired.** ⚠️ **Not yet fully closed** — deleting the source does not undeploy an already-live Edge Function. The function may still be reachable at its deployed URL until it is explicitly deleted via the Supabase Dashboard (Edge Functions → `sync-google-sheets` → Delete) or `supabase functions delete sync-google-sheets` (requires `supabase login`, which needs to be run interactively by a human). **Action item: confirm the function is actually undeployed on Supabase's side, not just removed from the repo.**

## Priority 2 — Reliability (P1)

- **P1-011 — Providers removed after insufficient evidence.** Single failed scrape should never deactivate providers — require the `ACTIVE → MISSING_ONCE → PENDING_INACTIVE → INACTIVE` state machine across multiple complete scrapes. (See Missing Provider Strategy above.) Status: Open.
- **P1-012 — Incomplete-scrape validation too weak** — currently only counts failed programs. Future validation should also check browser success, pagination, selectors, provider count, expected programs, and duplicate detection; synchronization should abort unless validation passes. (See Complete Scrape Requirement above.) Status: Open.
- **P1-013 — Hardcoded empty programs** assumed to return zero providers forever. Monitor expected-empty programs instead of hardcoding; alert if one starts returning results. Status: Open.
- **P1-014 — Geocoding every provider nightly is unnecessary.** Geocode only new providers and providers whose address changed. Status: Open.
- **P1-015 / P1-016 — Geocode/Places caches are not persistent** across GitHub Actions runs — lost every run. Persist in Supabase, a database table, or object storage. (See Caching section above.) Status: Open.
- **P1-017 — Workflow timeout risk** — a national scrape plus full geocoding may exceed the GitHub Actions job timeout. Separate scraping from enrichment. Status: Open.
- **P1-021 — Apps Script authentication should fail closed** on a missing secret rather than risk unintentionally allowing writes. Status: Open (related to P0-006).
- **P1-022 / P1-023 — Raw sheet is not historical; need immutable snapshots.** Separate `Raw_ACHC_Current` from historical snapshots (e.g. `snapshots/2026-07-27.json`). Status: Open.
- **P1-024 — Need field-level history** (field changed, old value, new value, timestamp, scrape run). (See Field-Level Change Detection, Part 3.) Status: Open.
- **P1-025 — `confidence_status` overloaded** with multiple concepts. (See Status Field Design above.) Status: Open.
- **P1-026 — Pipeline should fail atomically.** Desired flow: `Scrape → Validate → Normalize → Compare → Write → Sync → Complete`; failure anywhere should abort, not partially apply. Directly related to P0-005/P0-001c above — a normalization-stage crash should have aborted the run cleanly rather than (previously) crashing uncontrolled. Status: Open.
- **P1-027 — Need integration tests** across the full pipeline (Normalization → Geocoder → Google Sheets → Supabase), not just unit tests per stage. Status: Open — note the new unit-level regression tests added this session (`test_prep_row_for_supabase.py`) are a step toward this but do not replace a full pipeline integration test.

## Priority 3 — Observability (P2)

- **P2-028 — Daily metrics missing** (runtime, provider count, changes, removals, warnings, errors). (See Production Metrics above.) Status: Open.
- **P2-029 — Need operational alerts** (low provider count, failed programs, timeout, duplicate explosion, unusually high changes). (See Future Monitoring, Part 3.) Status: Open.
- **P2-030 — Need deployment documentation** (scraper, Apps Script, GitHub Actions, Supabase, env vars, synchronization lifecycle). Status: Open.

---

# Part 3 — Priority 3 (Observability) & Future Enhancements

## Snapshot Auditing

Every synchronization should be reproducible. Given a run ID (e.g. `Run 145`), the system should answer: exactly what ACHC returned, exactly what changed, exactly what was written, and exactly why records changed.

## Versioned Provider Snapshots

Maintain snapshots of provider records over time. Allows: accreditation timeline, phone history, address history, service history.

## Change Reports

Automatically generate a daily summary, e.g.:

```
Daily Summary
2 New Providers
5 Updated Providers
1 Removed Provider
0 Accreditation Changes
```

## Field-Level Change Detection

Instead of only recording "Record Changed," store which fields changed: Phone changed, Website changed, Address changed, Services changed. Useful for auditing.

## Confidence Levels

Each provider could receive a confidence level: Verified, Pending Verification, Possibly Removed, Inactive. This communicates certainty to administrators.

## Manual Review Queue

Some changes should require review: provider name changed, address moved to a different city, duplicate identity detected, unusually large service changes.

## Future Monitoring

Eventually introduce automated monitoring for: unusually low/high provider counts, duplicate providers/identities, geocoding failures, failed program searches, unusually large numbers of changes or removals, synchronization duration increases, API failures. Threshold-based alerts will help identify scraper regressions before incorrect data reaches production.

---

# Recommended Future Development Order

## Phase 1 — Critical

- ~~Fix geocoder field mismatch (P0-001).~~ **Done 2026-07-27.**
- ~~Fix Supabase RPC field remap bug (P0-001b).~~ **Done 2026-07-27.**
- ~~Fix `dba_name` UnboundLocalError crashing normalization (P0-001c).~~ **Done 2026-07-27.**
- Verify the above via a manual/`workflow_dispatch` scrape before touching `LIMIT_LOCATIONS`/`TEST_MODE`.
- Finish production synchronization architecture.
- Strengthen provider identity (P0-002).
- Replace service union with authoritative replacement (P0-007).
- Improve record hashing (P0-008).
- Improve complete scrape validation (P1-012).
- Remove test-mode workflow config once the above is verified (P0-003).
- Validate Google Sheets / RPC responses (P0-005).
- ~~Secure the sync endpoint (P0-020).~~ **Retired instead of hardened, 2026-07-27** — confirm actual undeployment on Supabase's side (see P0-020).

## Phase 2 — Reliability

- Persistent caches.
- Historical snapshot storage.
- Better error handling.
- Production metrics.
- Automated reporting.

## Phase 3 — Observability

- Change dashboards.
- Daily reports.
- Audit history.
- Historical provider timelines.
- Alerting.

## Phase 4 — Long-Term Enhancements

- Administrative review queue.
- Field-level change reports.
- Provider timeline visualization.
- Historical accreditation analytics.
- Public API endpoints.
- Internal health dashboard.

---

# Final Architectural Direction

The long-term goal should be to evolve AHHD from a nightly scraper into a production synchronization platform.

Instead of asking:

> "Can we scrape the website?"

the system should answer:

- What changed today?
- What stayed the same?
- What is new?
- What disappeared?
- How confident are we in those conclusions?

Every run should answer four questions:

1. What is new?
2. What changed?
3. What disappeared?
4. What stayed exactly the same?

Every production decision should be traceable back to a specific ACHC snapshot, while maintaining a complete audit trail of how the directory evolves over time.

ACHC remains the single source of truth. Historical data exists only to explain how the directory has evolved.
