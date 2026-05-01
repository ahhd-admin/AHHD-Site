# Supabase Deployment

## Required GitHub Secrets
Add these in GitHub → Settings → Secrets and variables → Actions:

| Secret | Value |
|--------|-------|
| `SUPABASE_URL` | Project URL from Supabase dashboard (Settings → API) |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role key (Settings → API) |
| `SUPABASE_ANON_KEY` | Anon/public key (Settings → API) |
| `SUPABASE_SYNC_URL` | `https://<project-ref>.supabase.co/functions/v1/sync-google-sheets` |

## Required Supabase Edge Function Environment Variable
In Supabase dashboard → Edge Functions → sync-google-sheets → Secrets, add:

| Variable | Value |
|----------|-------|
| `GOOGLE_SHEETS_WEB_APP_URL` | The Apps Script web app URL (same as the GitHub secret) |

## Deploy SQL function
Run the migration against your Supabase project (pick one):

```bash
# Via CLI
supabase db push --project-ref <project-ref>

# Or paste supabase/migrations/20260101000000_merge_google_sheets_data.sql
# into the SQL Editor in the Supabase dashboard
```

## Deploy edge function

```bash
supabase functions deploy sync-google-sheets --project-ref <project-ref>
```

The function source is at `supabase/functions/sync-google-sheets/index.ts`.

## How the sync is triggered
After each successful scrape, GitHub Actions POSTs to `SUPABASE_SYNC_URL`. The edge
function fetches the `Normalized_ACHC` sheet via `get_normalized`, filters out any
`incomplete_scrape` rows, and calls `merge_google_sheets_data` to upsert into Supabase.

If `SUPABASE_SYNC_URL` is not set, the workflow step skips silently.
