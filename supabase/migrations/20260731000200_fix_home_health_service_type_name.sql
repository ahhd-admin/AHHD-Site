/*
  # Fix Home Health service_type name mismatch

  service_types.service_type_name was "Home Health Care", but the scraper
  writes the service name as "Home Health" (ACHC's actual program name, and
  what the site's own filter UI already displays as the label). The RPC's
  service-sync step does an exact case-insensitive match
  (lower(service_type_name) = lower(v_service_name)), so every single
  Home Health location -- the largest category in the MVP dataset -- never
  matched and never got linked via location_service_types. Filtering the
  site by "Home Health" silently returned zero results.

  This only renames the existing row; it doesn't create a duplicate, so
  location_service_types rows already linked to it (there shouldn't be any,
  since the match never succeeded) are unaffected either way.
*/

UPDATE service_types
SET service_type_name = 'Home Health', updated_at = now()
WHERE service_type_slug = 'home-health-care';
