/*
  # Fixes from a full RLS/security audit (2026-08-03)

  Found via `supabase db advisors --linked --type security`:

  1. merge_google_sheets_data and perform_keepalive_ping (both
     SECURITY DEFINER) were callable by anon/authenticated via
     PostgREST's /rest/v1/rpc/ endpoint -- Postgres grants EXECUTE to
     PUBLIC by default on function creation, and that grant was never
     revoked. merge_google_sheets_data auto-publishes a location the
     moment it resolves real coordinates (no review-queue gate in that
     path) -- a crafted anonymous call with fake-but-geocodable data
     could have published straight to the live public directory.
     Confirmed live (curl with the public anon key) that the call was
     rejected with 42501 permission denied after this fix, and that
     service_role/postgres retain their own separate explicit grants,
     so the real nightly pipeline (which authenticates with
     SUPABASE_SERVICE_ROLE_KEY, not these grants) is unaffected.

  2. achc_raw_http_test and achc_normalized_http_test had RLS disabled
     entirely -- anyone with the public anon key could read/write them
     with zero restriction. Both were confirmed empty (0 rows) before
     this fix, so nothing had actually leaked.

  3. Three functions had a mutable search_path (standard Postgres
     hardening advisory -- an unset search_path lets a malicious actor
     with schema-creation rights on some other schema shadow an
     unqualified table/function reference inside a SECURITY DEFINER
     function).

  Separately (not in this migration, since it's an Auth project
  setting, not a schema change): public sign-up was disabled via the
  Management API (disable_signup: true). There was no admin-role
  concept anywhere -- client or RLS -- so "authenticated" and "admin"
  were effectively the same thing, and sign-up was open with
  auto-confirm on and no CAPTCHA. Confirmed live that signup now
  returns signup_disabled. The broader fix (RLS policies scoped to a
  real admin role instead of blanket `authenticated`, across
  locations/organizations/accreditation_records/articles/etc, all
  flagged by the same audit as `rls_policy_always_true`) is real,
  larger follow-up work, deliberately not rushed into this pass.
*/

REVOKE EXECUTE ON FUNCTION public.merge_google_sheets_data(jsonb) FROM PUBLIC, anon, authenticated;
REVOKE EXECUTE ON FUNCTION public.perform_keepalive_ping() FROM PUBLIC, anon, authenticated;

ALTER FUNCTION public.set_updated_at_achc_normalized_http_test() SET search_path = public;
ALTER FUNCTION public.perform_keepalive_ping() SET search_path = public;
ALTER FUNCTION public.merge_google_sheets_data(jsonb) SET search_path = public;

ALTER TABLE public.achc_raw_http_test ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.achc_normalized_http_test ENABLE ROW LEVEL SECURITY;
