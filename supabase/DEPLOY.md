# Supabase Deployment

## Required GitHub Secrets
Add these in GitHub → Settings → Secrets and variables → Actions:

| Secret | Value |
|--------|-------|
| `SUPABASE_URL` | Project URL from Supabase dashboard (Settings → API) |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role key (Settings → API) |

These are consumed directly by the scraper's `write_to_supabase_direct()` (`ENABLE_DIRECT_SUPABASE`), which is the only path that writes to Supabase.

`SUPABASE_ANON_KEY` and `SUPABASE_SYNC_URL` are no longer needed — they existed only to invoke the now-retired `sync-google-sheets` Edge Function (see below) and can be removed from the repo's GitHub secrets.

## Deploy SQL function
Run the migration against your Supabase project (pick one):

```bash
# Via CLI
supabase db push --project-ref <project-ref>

# Or paste supabase/migrations/20260101000000_merge_google_sheets_data.sql
# (plus the later migrations in supabase/migrations/) into the SQL Editor
# in the Supabase dashboard
```

## Retired: sync-google-sheets Edge Function

This function used to fetch `Normalized_ACHC` from Google Sheets (`?action=get_normalized`) and call `merge_google_sheets_data` to upsert into Supabase, triggered by a GitHub Actions step after each scrape.

It was retired on 2026-07-27: it ran with the service-role key internally but had no application-level secret check, only Supabase's platform JWT verification — satisfied by the anon key, which is meant to be public. Anyone who extracted the anon key from client traffic could have triggered a full production sync merge at will.

Google Sheets is now a read-only backup/audit trail only (the scraper still writes to it via `doPost`/`replace_raw_only`), and nothing reads from it back into Supabase. All production writes go through `write_to_supabase_direct()` directly from the scraper.

If this function (or any duplicate of its logic) is still deployed under any name in the Supabase dashboard, delete it there — removing the source from this repo does not undeploy it.
