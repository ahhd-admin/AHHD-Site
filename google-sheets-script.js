// Deploy: clasp push && clasp deploy --deploymentId AKfycbz9AC_PgqENlgWyuc_LmNDCCvPYXafwqeRGeBsLUdEzLQ1e_aK3r5DuOLbpIGKsHzhUYQ

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
      "search_trigger_state",
      "result_scope",
      "raw_name_line",
      "raw_address_block",
      "parsed_state_abbr",
      "matches_trigger_state",
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

      const metadata = payload.run_metadata || {};
      writeRunLog_(ss, metadata, "success", rawRows.length, testMode);
      sendCompletionEmail_(metadata, rawRows.length, testMode);

      return jsonResponse_({
        ok: true,
        action: "replace_raw_only",
        test_mode: testMode,
        raw_rows_received: rawRows.length,
        raw_sheet_name: rawSheetName
      });
    }

    if (action === "notify_failure") {
      const failureInfo = {
        run_id: payload.run_id || "unknown",
        github_run_id: payload.github_run_id || "",
        workflow: payload.workflow || "Daily Healthcare Provider Scraper",
        failed_at_utc: new Date().toISOString()
      };
      writeRunLog_(ss, failureInfo, "failed", 0, false);
      sendFailureEmail_(failureInfo);
      return jsonResponse_({ ok: true, action: "notify_failure" });
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

function writeRunLog_(ss, metadata, status, rowCount, testMode) {
  const logHeaders = [
    "run_id", "started_at_utc", "completed_at_utc", "duration_human",
    "duration_seconds", "total_rows_captured", "programs_scraped",
    "programs_with_zero_rows", "limit_locations", "no_state_filter",
    "trigger_state", "test_mode", "status"
  ];

  let logSheet = ss.getSheetByName("Run_Log");
  if (!logSheet) logSheet = ss.insertSheet("Run_Log");
  ensureHeaders_(logSheet, logHeaders);

  const programsScraped = Array.isArray(metadata.programs_scraped)
    ? metadata.programs_scraped.join(", ")
    : (metadata.programs_scraped || "");

  const programsWithZero = Array.isArray(metadata.programs_with_zero_rows)
    ? metadata.programs_with_zero_rows.join(", ")
    : (metadata.programs_with_zero_rows || "");

  const row = [
    metadata.run_id || "",
    metadata.started_at_utc || metadata.failed_at_utc || "",
    metadata.completed_at_utc || "",
    metadata.duration_human || "",
    metadata.duration_seconds || "",
    rowCount,
    programsScraped,
    programsWithZero,
    metadata.limit_locations !== undefined ? metadata.limit_locations : "",
    metadata.no_state_filter !== undefined ? metadata.no_state_filter : "",
    metadata.trigger_state || "",
    testMode,
    status
  ];

  logSheet.appendRow(row);
}

function sendCompletionEmail_(metadata, rowCount, testMode) {
  const recipient = "jay@jayfox.design";
  const runId = metadata.run_id || "unknown";
  const duration = metadata.duration_human || "unknown duration";
  const startedAt = metadata.started_at_utc || "";
  const completedAt = metadata.completed_at_utc || "";
  const programsWithZero = Array.isArray(metadata.programs_with_zero_rows) && metadata.programs_with_zero_rows.length > 0
    ? metadata.programs_with_zero_rows.join(", ")
    : "none";
  const modeLabel = testMode ? " [TEST MODE]" : "";

  const subject = `ACHC Scrape Complete${modeLabel} — ${rowCount} rows in ${duration}`;

  const body = [
    `Run ID: ${runId}`,
    `Started:   ${startedAt} UTC`,
    `Completed: ${completedAt} UTC`,
    `Duration:  ${duration}`,
    ``,
    `Programs scraped: ${Array.isArray(metadata.programs_scraped) ? metadata.programs_scraped.length : "unknown"}`,
    `Total rows captured: ${rowCount}`,
    `Programs with zero results: ${programsWithZero}`,
    ``,
    testMode ? "Note: This was a TEST MODE run — data written to Raw_ACHC_Test tab." : "Data written to Raw_ACHC tab.",
    ``,
    `View Google Sheets: https://docs.google.com/spreadsheets/d/${SpreadsheetApp.getActiveSpreadsheet().getId()}`
  ].join("\n");

  try {
    MailApp.sendEmail(recipient, subject, body);
  } catch (emailErr) {
    console.error("sendCompletionEmail_ failed: " + String(emailErr));
  }
}

function sendFailureEmail_(failureInfo) {
  const recipient = "jay@jayfox.design";
  const subject = "ACHC Scrape FAILED — Manual review needed";

  const body = [
    `The scheduled ACHC scraper failed.`,
    ``,
    `Run ID: ${failureInfo.run_id || "unknown"}`,
    `GitHub Run ID: ${failureInfo.github_run_id || "unknown"}`,
    `Workflow: ${failureInfo.workflow || "Daily Healthcare Provider Scraper"}`,
    `Failed at: ${failureInfo.failed_at_utc} UTC`,
    ``,
    `The last known-good data is preserved in Google Sheets.`,
    `Check the GitHub Actions logs for the full error.`,
    ``,
    `View Google Sheets: https://docs.google.com/spreadsheets/d/${SpreadsheetApp.getActiveSpreadsheet().getId()}`
  ].join("\n");

  try {
    MailApp.sendEmail(recipient, subject, body);
  } catch (emailErr) {
    console.error("sendFailureEmail_ failed: " + String(emailErr));
  }
}
