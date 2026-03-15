function doGet(e) {
  return ContentService
    .createTextOutput(JSON.stringify({
      ok: true,
      message: "ACHC Google Sheets web app is live"
    }))
    .setMimeType(ContentService.MimeType.JSON);
}

function doPost(e) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();

    if (!e || !e.postData || !e.postData.contents) {
      return jsonResponse_({
        ok: false,
        error: "No POST body received"
      });
    }

    const payload = JSON.parse(e.postData.contents);
    const action = payload.action || "replace_all";
    const testMode = payload.test_mode === true;

    const rawSheetName = testMode ? "Raw_ACHC_Test" : "Raw_ACHC";
    const mergedSheetName = testMode ? "Merged_Locations_Test" : "Merged_Locations";

    let rawSheet = ss.getSheetByName(rawSheetName);
    let mergedSheet = ss.getSheetByName(mergedSheetName);

    if (!rawSheet) rawSheet = ss.insertSheet(rawSheetName);
    if (!mergedSheet) mergedSheet = ss.insertSheet(mergedSheetName);

    const rawHeaders = [
      "location_key",
      "display_name",
      "legal_name",
      "dba_name",
      "searched_program_type",
      "accreditation_program",
      "services",
      "address_raw",
      "address_line_1",
      "address_line_2",
      "city",
      "state",
      "state_abbr",
      "zip",
      "phone",
      "website_url",
      "latitude",
      "longitude",
      "accreditation_dates",
      "source_url",
      "last_seen"
    ];

    const mergedHeaders = [
      "location_key",
      "display_name",
      "legal_name",
      "dba_names",
      "name_variants",
      "program_types",
      "accreditation_programs",
      "services",
      "address_raw_variants",
      "address_line_1",
      "address_line_2",
      "city",
      "state",
      "state_abbr",
      "zip",
      "phone",
      "website_url",
      "latitude",
      "longitude",
      "accreditation_dates",
      "source_urls",
      "source_count",
      "last_seen",
      "enhanced_listing"
    ];

    ensureHeaders_(rawSheet, rawHeaders);
    ensureHeaders_(mergedSheet, mergedHeaders);

    const rawRows = Array.isArray(payload.raw_rows) ? payload.raw_rows : [];
    const mergedRows = Array.isArray(payload.merged_rows) ? payload.merged_rows : [];

    if (action === "replace_all") {
      clearDataKeepHeaders_(rawSheet, rawHeaders);
      clearDataKeepHeaders_(mergedSheet, mergedHeaders);

      if (rawRows.length > 0) {
        const rawValues = rawRows.map(row => rawHeaders.map(h => normalizeValue_(row[h])));
        rawSheet.getRange(2, 1, rawValues.length, rawHeaders.length).setValues(rawValues);
      }

      if (mergedRows.length > 0) {
        const mergedValues = mergedRows.map(row => mergedHeaders.map(h => normalizeValue_(row[h])));
        mergedSheet.getRange(2, 1, mergedValues.length, mergedHeaders.length).setValues(mergedValues);
      }

      return jsonResponse_({
        ok: true,
        action: "replace_all",
        test_mode: testMode,
        raw_rows_received: rawRows.length,
        merged_rows_received: mergedRows.length,
        raw_sheet_name: rawSheetName,
        merged_sheet_name: mergedSheetName
      });
    }

    return jsonResponse_({
      ok: false,
      error: "Unknown action",
      action_received: action
    });

  } catch (err) {
    return jsonResponse_({
      ok: false,
      error: String(err),
      stack: err && err.stack ? String(err.stack) : ""
    });
  }
}

function ensureHeaders_(sheet, headers) {
  if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    return;
  }

  const existing = sheet.getRange(1, 1, 1, headers.length).getValues()[0];
  const mismatch = headers.some((h, i) => existing[i] !== h);

  if (mismatch) {
    sheet.clearContents();
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }
}

function clearDataKeepHeaders_(sheet, headers) {
  const maxRows = sheet.getMaxRows();
  const maxCols = Math.max(sheet.getMaxColumns(), headers.length);

  if (maxRows > 1) {
    sheet.getRange(2, 1, maxRows - 1, maxCols).clearContent();
  }

  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
}

function normalizeValue_(value) {
  return value === null || value === undefined ? "" : value;
}

function jsonResponse_(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
