/*
  # Deactivate non-MVP service types

  service_types has 16 rows (Home Care, Home Health, Hospice, plus 13
  others -- Pharmacy, DMEPOS, Dentistry, Assisted Living, etc.) left
  over from Bolt's originally-planned full scope. The scraper has been
  scoped to the 3 MVP program types for a while (TEST_PROGRAMS in
  daily-scraper.yml), but a live check ahead of launch found the other
  13 were never actually deactivated in the data -- 9,323 published,
  publicly-displayable locations are still linked to "Pharmacy" alone
  (comparable to or exceeding the MVP categories' own counts), left
  over from a full-scope population run before the MVP decision.

  These aren't currently visible through the normal UI (the checkbox
  list in serviceCategories.ts only ever offers the 3 MVP slugs), but
  they were reachable through an unvalidated `?care=` URL query
  parameter (see the matching SearchHero.tsx fix in this same commit)
  and would resurface immediately if a future frontend change ever
  queried service_types without the frontend's own hardcoded allowlist.

  Setting is_active = false here is the actual data-layer switch --
  merge_google_sheets_data's service-sync step already only links
  is_active = true types going forward (WHERE ... AND is_active =
  true), so this also stops any future accidental re-linking, not just
  today's exposure. Flip back to true (and uncomment the matching
  frontend entries in serviceCategories.ts) when a category is
  deliberately brought into scope -- see MVP-Rollout-Roadmap.md's
  Phase 5.
*/

UPDATE service_types
   SET is_active = false
 WHERE service_type_name NOT IN ('Home Care', 'Home Health', 'Hospice');
