/*
  # Drop unused search_logs scaffold table

  Discovered during a security/RLS audit: search_logs already existed
  (part of the same earlier admin-pages scaffold as SearchAnalytics.tsx
  and its unwired /admin/search-logs route), but nothing in the codebase
  ever referenced it -- confirmed via grep, and confirmed empty (0 rows)
  live before dropping.

  Its schema conflicts with the privacy-by-design approach taken for the
  new search_events table (see 20260803000000_search_events_log.sql):
  it stored the raw typed query_text, un-rounded exact
  interpreted_latitude/longitude, and a session_identifier -- exactly
  what search_events deliberately avoids, and what the Privacy Policy
  now explicitly says isn't collected. Keeping both tables around risked
  a future feature getting wired into the more invasive one by mistake.
*/

DROP TABLE IF EXISTS public.search_logs;
