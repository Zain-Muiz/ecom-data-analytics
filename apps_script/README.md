# Email-to-GCS via Apps Script

Replaces the Cloud Function poller. Runs inside the inbox owner's Google account
with native Gmail access — no OAuth client, no refresh tokens, no Secret Manager.

## One-time setup

1. **Create the Apps Script project**
   - Go to https://script.google.com (signed in as the inbox owner — the account that receives the daily CSV email).
   - Click **New project**.
   - Replace the default `Code.gs` with the contents of `Code.gs` from this folder.
   - Project Settings → check "Show appsscript.json" → replace its content with `appsscript.json`.

2. **Bind the script to your GCP project** (so it can write to GCS as the inbox owner)
   - In the script editor: Project Settings → Google Cloud Platform (GCP) Project → Change project → enter your GCP project number.
   - This lets the script's OAuth token authenticate to GCS APIs in *your* project.

3. **Grant the inbox owner write access to the landing bucket**
   ```bash
   gsutil iam ch user:data-ingest@yourcompany.com:objectCreator gs://ecom-bi-landing-prod
   ```
   (replace with the real inbox email and bucket)

4. **Edit `CONFIG` at the top of `Code.gs`**: bucket name, sender, subject, admin email.

5. **Install the triggers**
   - In the script editor, select function `setupTriggers` → Run.
   - First run will prompt to authorize the listed scopes — accept.
   - Verify in the Triggers view (left sidebar, clock icon): you should see
     `pollForEmail` (every 5 min) and `checkAndAlert` (daily at 10:00).

## How it works

- `pollForEmail` runs every 5 min. It short-circuits if the hour isn't 09:xx local.
  Inside the 09:00–10:00 window it searches Gmail for today's matching email
  (excluding any thread already labeled `orders-csv-processed`). On finding it,
  uploads the attachment to `gs://{bucket}/incoming/` and labels the thread.
- The GCS finalize event then triggers the loader Cloud Function — same as before.
- `checkAndAlert` runs at 10:00. If no thread carries today's label, it emails the admin.

## Why this is simpler

| Component before | Component now |
|---|---|
| `gmail_poller` Cloud Function (157 LOC Python) | `Code.gs` (~80 LOC) |
| OAuth client JSON file | None |
| `setup_gmail_oauth.py` bootstrap | None |
| Secret Manager secret + IAM | None |
| Cloud Scheduler job | Apps Script time triggers |
| `gmail-poller` service account | None |
| `secretmanager.googleapis.com` API | None |

## Caveats

- Apps Script time triggers are best-effort — Google doesn't guarantee exact firing time. ±30s typical, very occasionally a few minutes. Fine for daily polling.
- `UrlFetchApp` daily quota: 100k calls/day on consumer accounts, 100k on Workspace. We use 1.
- If the inbox owner leaves the company, the script needs to be transferred. (Same risk applies to the OAuth-token approach. Long-term mitigation: shared Workspace mailbox with multiple owners.)
