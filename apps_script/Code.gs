/**
 * Email-to-GCS pipeline trigger.
 *
 * Runs from the inbox owner's Google account — has native Gmail access,
 * no OAuth dance, no refresh tokens, no Secret Manager.
 *
 * Two time triggers (set up via setupTriggers() once):
 *   1. pollForEmail   — runs every 5 min between 09:00 and 10:00 IST
 *   2. checkAndAlert  — runs once at 10:00 IST, alerts admin if file didn't arrive
 *
 * Auth to GCS: this Apps Script project is bound to a GCP project (set via
 * the Google Cloud Platform menu in script editor → set GCP project).
 * The script then uses ScriptApp.getOAuthToken() to authenticate to GCS using
 * the inbox owner's identity (the inbox owner just needs storage.objectCreator
 * on the landing bucket). No service account, no key.
 */

// ===== Config — edit these =====
const CONFIG = {
  GCS_BUCKET:        'ecom-bi-landing-prod',     // landing bucket name
  SENDER:            'reports@source-system.com',
  SUBJECT_CONTAINS:  'Daily Orders Export',
  ATTACHMENT_REGEX:  /^orders_\d{8}\.csv$/i,
  ADMIN_EMAIL:       'data-team@yourcompany.com',
  // Used by checkAndAlert to know which date to look for
  TIMEZONE:          'Asia/Kolkata',
};

const PROCESSED_LABEL = 'orders-csv-processed';

/**
 * Poll trigger — runs every 5 min during the polling window.
 * Searches Gmail for today's email; if found, pushes attachment to GCS
 * and labels the thread to avoid reprocessing.
 */
function pollForEmail() {
  // Only poll between 09:00 and 09:59 local time (window ends; checkAndAlert fires at 10:00)
  const hour = parseInt(Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'H'), 10);
  if (hour !== 9) {
    return;
  }

  const today = todayString_();  // YYYY/MM/DD
  const query = `from:${CONFIG.SENDER} subject:"${CONFIG.SUBJECT_CONTAINS}" `
              + `has:attachment after:${today} -label:${PROCESSED_LABEL}`;

  const threads = GmailApp.search(query, 0, 5);
  if (threads.length === 0) {
    console.log('No matching email yet');
    return;
  }

  const label = getOrCreateLabel_(PROCESSED_LABEL);
  for (const thread of threads) {
    for (const msg of thread.getMessages()) {
      for (const att of msg.getAttachments()) {
        if (CONFIG.ATTACHMENT_REGEX.test(att.getName())) {
          uploadToGcs_(att);
          console.log(`Uploaded ${att.getName()} (${att.getBytes().length} bytes)`);
        }
      }
    }
    thread.addLabel(label);
  }
}

/**
 * Alert trigger — runs once at the end of the polling window.
 * If the processed label wasn't applied today, send admin alert.
 */
function checkAndAlert() {
  const today = todayString_();
  const query = `label:${PROCESSED_LABEL} after:${today}`;
  const threads = GmailApp.search(query, 0, 1);
  if (threads.length > 0) {
    console.log('File already processed today, no alert needed');
    return;
  }

  GmailApp.sendEmail(
    CONFIG.ADMIN_EMAIL,
    '[ALERT] Daily orders CSV missing',
    `The daily orders CSV did not arrive by 10:00 ${CONFIG.TIMEZONE}.\n\n`
    + `Filter: from=${CONFIG.SENDER}, subject contains "${CONFIG.SUBJECT_CONTAINS}"\n\n`
    + `If the file arrives later, drop it directly into:\n`
    + `  gs://${CONFIG.GCS_BUCKET}/incoming/\n\n`
    + `The pipeline will pick it up automatically via the GCS event trigger.`
  );
  console.log('Admin alerted');
}

/** Upload a Gmail attachment to GCS using the script owner's OAuth token. */
function uploadToGcs_(att) {
  const url = `https://storage.googleapis.com/upload/storage/v1/b/${CONFIG.GCS_BUCKET}`
            + `/o?uploadType=media&name=incoming/${encodeURIComponent(att.getName())}`;

  const response = UrlFetchApp.fetch(url, {
    method:      'post',
    contentType: 'text/csv',
    payload:     att.getBytes(),
    headers:     { Authorization: 'Bearer ' + ScriptApp.getOAuthToken() },
    muteHttpExceptions: true,
  });

  const code = response.getResponseCode();
  if (code !== 200) {
    throw new Error(`GCS upload failed (${code}): ${response.getContentText()}`);
  }
}

function getOrCreateLabel_(name) {
  return GmailApp.getUserLabelByName(name) || GmailApp.createLabel(name);
}

function todayString_() {
  return Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy/MM/dd');
}

/**
 * One-time setup: run this manually from the script editor to install triggers.
 * Polls every 5 min. The "9-10 AM only" window is enforced by pollForEmail
 * checking the hour itself — keeps trigger config simple (one trigger, one cadence).
 */
function setupTriggers() {
  // Wipe existing triggers for this script
  ScriptApp.getProjectTriggers().forEach(t => ScriptApp.deleteTrigger(t));

  // Poll every 5 minutes (Apps Script minimum is 1 min; 5 is plenty)
  // pollForEmail itself short-circuits outside the polling window.
  ScriptApp.newTrigger('pollForEmail')
    .timeBased().everyMinutes(5).create();

  // Daily alert check at 10:00 in script's timezone
  ScriptApp.newTrigger('checkAndAlert')
    .timeBased().atHour(10).everyDays(1).create();

  console.log('Triggers installed');
}
