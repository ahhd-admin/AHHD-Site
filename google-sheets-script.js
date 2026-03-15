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
    const sheetName = "Providers";
    let sheet = ss.getSheetByName(sheetName);

    if (!sheet) {
      sheet = ss.insertSheet(sheetName);
    }

    const headers = [
      "organization",
      "program",
      "address",
      "city",
      "state",
      "state_abbr",
      "zip",
      "phone",
      "latitude",
      "longitude",
      "source_url",
      "last_seen"
    ];

    ensureHeaders_(sheet, headers);

    if (!e || !e.postData || !e.postData.contents) {
      return jsonResponse_({ ok: false, error: "No POST body received" });
    }

    const payload = JSON.parse(e.postData.contents);
    const action = payload.action || "replace_all";
    const rows = Array.isArray(payload.rows) ? payload.rows : [];

    if (action === "replace_all") {
      clearDataKeepHeaders_(sheet, headers);

      if (rows.length > 0) {
        const values = rows.map(row => headers.map(h => normalizeValue_(row[h])));
        sheet.getRange(2, 1, values.length, headers.length).setValues(values);
      }

      return jsonResponse_({
        ok: true,
        action: "replace_all",
        rows_received: rows.length,
        sheet_name: sheetName
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
