function doGet(e) {
  try {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Providers');
    if (!sheet) {
      return ContentService
        .createTextOutput(JSON.stringify({error: 'Sheet named "Providers" not found'}))
        .setMimeType(ContentService.MimeType.JSON);
    }

    const data = sheet.getDataRange().getValues();

    if (data.length === 0) {
      return ContentService
        .createTextOutput(JSON.stringify([]))
        .setMimeType(ContentService.MimeType.JSON);
    }

    const headers = data[0];
    const rows = data.slice(1).map(row => {
      const obj = {};
      headers.forEach((header, i) => {
        obj[header] = row[i];
      });
      return obj;
    });

    return ContentService
      .createTextOutput(JSON.stringify(rows))
      .setMimeType(ContentService.MimeType.JSON);

  } catch (error) {
    return ContentService
      .createTextOutput(JSON.stringify({error: error.toString()}))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

function doPost(e) {
  try {
    let sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Providers');

    if (!sheet) {
      sheet = SpreadsheetApp.getActiveSpreadsheet().insertSheet('Providers');
      sheet.appendRow([
        'organization', 'program', 'address', 'city', 'state',
        'zip', 'phone', 'latitude', 'longitude', 'source_url', 'last_seen'
      ]);
    }

    const payload = JSON.parse(e.postData.contents);

    if (Array.isArray(payload)) {
      payload.forEach(item => {
        sheet.appendRow([
          item.organization || '',
          item.program || '',
          item.address || '',
          item.city || '',
          item.state || '',
          item.zip || '',
          item.phone || '',
          item.latitude || '',
          item.longitude || '',
          item.source_url || '',
          item.last_seen || ''
        ]);
      });

      return ContentService
        .createTextOutput(JSON.stringify({success: true, count: payload.length}))
        .setMimeType(ContentService.MimeType.JSON);
    } else {
      sheet.appendRow([
        payload.organization || '',
        payload.program || '',
        payload.address || '',
        payload.city || '',
        payload.state || '',
        payload.zip || '',
        payload.phone || '',
        payload.latitude || '',
        payload.longitude || '',
        payload.source_url || '',
        payload.last_seen || ''
      ]);

      return ContentService
        .createTextOutput(JSON.stringify({success: true, count: 1}))
        .setMimeType(ContentService.MimeType.JSON);
    }

  } catch (error) {
    return ContentService
      .createTextOutput(JSON.stringify({error: error.toString()}))
      .setMimeType(ContentService.MimeType.JSON);
  }
}
