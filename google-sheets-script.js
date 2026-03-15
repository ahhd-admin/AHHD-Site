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
    const action = payload.action || "replace_raw_only";
    const testMode = payload.test_mode === true;

    const rawSheetName = testMode ? "Raw_ACHC_Test" : "Raw_ACHC";
    let rawSheet = ss.getSheetByName(rawSheetName);

    if (!rawSheet) rawSheet = ss.insertSheet(rawSheetName);

    const rawHeaders = [
      "raw_index",
      "container_type",
      "searched_program_type",
      "searched_state",
      "raw_text",
      "source_url",
      "last_seen"
    ];

    ensureHeaders_(rawSheet, rawHeaders);

    const rawRows = Array.isArray(payload.raw_rows) ? payload.raw_rows : [];

    if (action === "replace_raw_only") {
      clearDataKeepHeaders_(rawSheet, rawHeaders);

      if (rawRows.length > 0) {
        const rawValues = rawRows.map(row => rawHeaders.map(h => normalizeValue_(row[h])));
        rawSheet.getRange(2, 1, rawValues.length, rawHeaders.length).setValues(rawValues);
      }

      return jsonResponse_({
        ok: true,
        action: "replace_raw_only",
        test_mode: testMode,
        raw_rows_received: rawRows.length,
        raw_sheet_name: rawSheetName
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
