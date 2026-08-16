// Read-only, pull-based sync: queries Supabase's `locations` table directly
// (the same published/public data the live site itself reads) and writes it
// into a "Providers" tab on its own schedule, completely decoupled from the
// nightly scraper's push (google-sheets-script.js's doPost handler, bound to
// a different spreadsheet). That decoupling is the whole point -- the push
// path timing out on a slow write used to fail the scraper's entire nightly
// job even after its actual (and more important) Supabase write had already
// succeeded; a separate script pulling on its own trigger can run as slow as
// it needs to without ever affecting the scraper's success/failure status.
//
// Meant for a founder/non-technical stakeholder to browse -- see
// setUpSlicers below for the interactive filter UI (called automatically at
// the end of every sync -- see the comment on that call in
// syncLocationsFromSupabase), and share the spreadsheet itself as
// Viewer-only (Google Sheets' own sharing dialog -- not something this
// script can set) so it stays read-only in practice, not just in name.
//
// One-time setup (do this in the Apps Script editor -- script.google.com,
// bound to a NEW spreadsheet, not the existing scraper one):
//   1. Project Settings -> Script Properties -> add:
//        SUPABASE_URL       = the project's Supabase URL (same value as the
//                              site's VITE_SUPABASE_URL)
//        SUPABASE_ANON_KEY  = the project's Supabase anon/publishable key
//                              (same value as VITE_SUPABASE_ANON_KEY -- this
//                              key is already public, shipped in the site's
//                              own JS bundle, and RLS already restricts it to
//                              exactly the published/public-display rows
//                              queried below, so no new Supabase role or key
//                              needs to be created for this).
//   2. Run syncLocationsFromSupabase() once manually (Run menu) and approve
//      the OAuth prompt (it needs permission to call an external URL) --
//      this populates the sheet AND adds the slicers in one run.
//   3. Run createDailyTrigger() once to schedule future syncs -- do this
//      LAST, since triggers can't be created from inside a simulated/preview
//      run, only from an actual manual execution.

const PROVIDERS_SHEET_NAME = 'Providers';

// PostgREST caps any single response at 1000 rows regardless of what's
// asked for (confirmed against this same Supabase project in
// SearchHero.tsx's own pagination) -- looping with the Range header is what
// gets the real full set instead of a silent 1000-row truncation.
const PAGE_SIZE = 1000;

// Mirrors the exact same "what does the live site actually show" filter
// SearchHero.tsx's queries use -- this sync is meant to mirror the live
// site, not the full underlying table (which also holds pending/rejected/
// unpublished rows, and service types outside the site's current MVP
// scope, that a founder browsing "our real listings" shouldn't see).
//
// MVP_SERVICE_SLUGS mirrors src/lib/serviceCategories.ts's own MVP scope
// list (Home Care, Home Health, Hospice) -- update both together if that
// scope ever changes.
const MVP_SERVICE_SLUGS = ['home-care', 'home-health-care', 'hospice'];

// !inner on both hops of the service-types join (not a plain embed) is
// required for the slug filter below to actually restrict rows rather
// than just restrict which nested service_types show up per row -- see
// the matching comment on this same join in SearchHero.tsx's loadLocations.
const SELECT_COLUMNS =
  'location_id,location_name,address_line_1,address_line_2,city,state,postal_code,' +
  'public_phone,public_email,website_url,latitude,longitude,listing_status,' +
  'last_verified_at,' +
  'organization:organizations(organization_name,website_url),' +
  'service_types:location_service_types!inner(service_type:service_types!inner(service_type_name)),' +
  'accreditation_records(accrediting_body,accreditation_status,is_current_record)';

const SHEET_HEADERS = [
  'Organization', 'Location Name', 'Address', 'City', 'State', 'Postal Code',
  'Phone', 'Email', 'Website', 'Service Types', 'Accrediting Body',
  'Accreditation Status', 'Last Verified', 'Latitude', 'Longitude',
];

// A 2-row summary block above the table (live count + last-sync time) so a
// founder glancing at the sheet doesn't have to scroll or count rows to
// answer "how many do we actually have right now, and is this current."
// Row 3 is left blank as a visual spacer before the real header row.
const HEADER_ROW = 4;
const DATA_START_ROW = HEADER_ROW + 1;

// Planned next: pull accreditation_records.expiration_date into
// SELECT_COLUMNS/flattenLocationRow_ and add a summary line here counting
// how many current accreditations expire within the next 3-6 months, so
// the founder can see who's coming up for renewal without cross-referencing
// ACHC's own records by hand. Not wired up yet -- today's sync only carries
// the current status/body (see flattenLocationRow_), not the actual dates.

function syncLocationsFromSupabase() {
  const props = PropertiesService.getScriptProperties();
  const supabaseUrl = props.getProperty('SUPABASE_URL');
  const anonKey = props.getProperty('SUPABASE_ANON_KEY');
  if (!supabaseUrl || !anonKey) {
    throw new Error(
      'Missing SUPABASE_URL / SUPABASE_ANON_KEY Script Properties -- see the ' +
      'setup comment at the top of this file.'
    );
  }

  const rows = fetchAllPublishedLocations_(supabaseUrl, anonKey);
  const values = rows.map(flattenLocationRow_);

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(PROVIDERS_SHEET_NAME);
  if (!sheet) sheet = ss.insertSheet(PROVIDERS_SHEET_NAME);

  // Full clear + rewrite, not an incremental diff -- this mirrors "what the
  // live site shows right now," so a location that was unpublished (or
  // whose accepts_public_display flag was flipped off) since the last sync
  // needs to actually disappear here too, not linger as a stale row.
  sheet.clear();

  const now = new Date();
  sheet.getRange(1, 1, 2, 2).setValues([
    ['Live Listings', values.length],
    ['Last Updated', Utilities.formatDate(now, ss.getSpreadsheetTimeZone(), "MMM d, yyyy h:mm a 'UTC'Z")],
  ]);
  sheet.getRange(1, 1, 2, 1).setFontWeight('bold');

  sheet.getRange(HEADER_ROW, 1, 1, SHEET_HEADERS.length).setValues([SHEET_HEADERS]).setFontWeight('bold');
  sheet.setFrozenRows(HEADER_ROW);
  if (values.length > 0) {
    sheet.getRange(DATA_START_ROW, 1, values.length, SHEET_HEADERS.length).setValues(values);
  }
  sheet.autoResizeColumns(1, SHEET_HEADERS.length);
  // autoResizeColumns fits exactly to content with zero margin -- text
  // reads as visually cramped against the cell edge/next column without a
  // little breathing room.
  for (let col = 1; col <= SHEET_HEADERS.length; col++) {
    sheet.setColumnWidth(col, sheet.getColumnWidth(col) + 16);
  }

  // sheet.clear() above wipes the range any existing slicers were bound
  // to, which knocks them loose from their column-17 anchor and collapses
  // them to the top-left, overlapping the summary block -- confirmed live
  // after a second sync run. Re-running this (idempotent -- see its own
  // comment) on every sync, not just once ever, is what keeps them correctly
  // positioned after every nightly refresh instead of just the first one.
  setUpSlicers();

  Logger.log('Synced %s published locations from Supabase at %s', values.length, now.toISOString());
}

function fetchAllPublishedLocations_(supabaseUrl, anonKey) {
  const all = [];
  let offset = 0;

  while (true) {
    const url =
      supabaseUrl.replace(/\/$/, '') +
      '/rest/v1/locations' +
      '?select=' + encodeURIComponent(SELECT_COLUMNS) +
      '&listing_status=eq.published' +
      '&accepts_public_display=eq.true' +
      '&location_service_types.service_types.service_type_slug=in.(' + MVP_SERVICE_SLUGS.join(',') + ')' +
      '&order=state.asc,city.asc';

    const response = UrlFetchApp.fetch(url, {
      method: 'get',
      headers: {
        apikey: anonKey,
        Authorization: 'Bearer ' + anonKey,
        Range: offset + '-' + (offset + PAGE_SIZE - 1),
      },
      muteHttpExceptions: true,
    });

    const status = response.getResponseCode();
    if (status !== 200 && status !== 206) {
      throw new Error('Supabase request failed (HTTP ' + status + '): ' + response.getContentText().slice(0, 500));
    }

    const page = JSON.parse(response.getContentText());
    all.push.apply(all, page);

    if (page.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }

  return all;
}

// Supabase returns last_verified_at as a full ISO timestamp with a UTC
// offset (e.g. "2026-05-10T07:31:41.557+00:00") -- ~29 characters that
// blew out the column width for a value nobody needs down to the second.
// A bare date is what's actually useful here (roughly when this was last
// confirmed), so this reformats to that rather than truncating the string.
function formatVerifiedDate_(isoString) {
  if (!isoString) return '';
  const parsed = new Date(isoString);
  if (isNaN(parsed.getTime())) return isoString;
  return Utilities.formatDate(parsed, 'UTC', 'MMM d, yyyy');
}

function flattenLocationRow_(loc) {
  const serviceTypeNames = (loc.service_types || [])
    .map(function (st) { return st.service_type && st.service_type.service_type_name; })
    .filter(Boolean)
    .join(', ');

  const currentAccreditations = (loc.accreditation_records || []).filter(function (rec) {
    return rec.is_current_record;
  });
  const accreditingBodies = currentAccreditations.map(function (rec) { return rec.accrediting_body; }).join(', ');
  const accreditationStatuses = currentAccreditations.map(function (rec) { return rec.accreditation_status; }).join(', ');

  const address = [loc.address_line_1, loc.address_line_2].filter(Boolean).join(', ');

  return [
    (loc.organization && loc.organization.organization_name) || '',
    loc.location_name || '',
    address,
    loc.city || '',
    loc.state || '',
    loc.postal_code || '',
    loc.public_phone || '',
    loc.public_email || '',
    loc.website_url || (loc.organization && loc.organization.website_url) || '',
    serviceTypeNames,
    accreditingBodies,
    accreditationStatuses,
    formatVerifiedDate_(loc.last_verified_at),
    loc.latitude != null ? loc.latitude : '',
    loc.longitude != null ? loc.longitude : '',
  ];
}

// Run once, manually, after the first successful sync has populated the
// Providers sheet. Idempotent -- removes any slicers it previously added
// before re-adding them, so re-running this after a header/column change
// doesn't pile up duplicates. Filter criteria are left unset (an empty
// slicer widget) -- the point is letting the founder pick values
// interactively from the sheet UI, not pre-deciding a filter for them.
function setUpSlicers() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(PROVIDERS_SHEET_NAME);
  if (!sheet || sheet.getLastRow() < DATA_START_ROW) {
    throw new Error('Run syncLocationsFromSupabase() first -- the sheet has no data yet.');
  }

  sheet.getSlicers().forEach(function (slicer) { slicer.remove(); });

  // Starts at HEADER_ROW, not row 1 -- the summary block above it (live
  // count/last-updated) isn't part of the filterable table.
  const dataRange = sheet.getRange(HEADER_ROW, 1, sheet.getLastRow() - HEADER_ROW + 1, SHEET_HEADERS.length);

  // Column positions are 1-indexed and match SHEET_HEADERS above.
  const slicerColumns = [
    { name: 'State', column: 5 },
    { name: 'Service Types', column: 10 },
    { name: 'Accreditation Status', column: 12 },
  ];

  slicerColumns.forEach(function (def, i) {
    // Stacked in the columns just past the data table, not overlapping it --
    // SHEET_HEADERS has 15 columns, so column 17 clears it with a gap.
    const anchorRow = HEADER_ROW + i * 8;
    const anchorColumn = 17;
    const slicer = sheet.insertSlicer(dataRange, anchorRow, anchorColumn);
    slicer.setColumnFilterCriteria(def.column, SpreadsheetApp.newFilterCriteria().build());
    slicer.setTitle(def.name);
  });

  Logger.log('Slicers added for: %s', slicerColumns.map(function (d) { return d.name; }).join(', '));
}

// Run once, manually, after the sync and slicers are both confirmed working.
function createDailyTrigger() {
  ScriptApp.getProjectTriggers()
    .filter(function (t) { return t.getHandlerFunction() === 'syncLocationsFromSupabase'; })
    .forEach(function (t) { ScriptApp.deleteTrigger(t); });

  // Offset from the scraper's own 6:00 AM UTC run (daily-scraper.yml) so
  // this always pulls data from a run that's already finished, not one
  // still in flight.
  ScriptApp.newTrigger('syncLocationsFromSupabase')
    .timeBased()
    .atHour(8)
    .everyDays(1)
    .inTimezone('Etc/UTC')
    .create();

  Logger.log('Daily trigger created: syncLocationsFromSupabase at 08:00 UTC');
}
