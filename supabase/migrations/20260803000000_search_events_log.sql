/*
  # Foundation for search-location tracking (coverage-gap heatmap, later)

  Jay wants visibility into what locations/areas visitors actually search
  for, to identify coverage gaps -- separate from GA4 (which will track
  page views/events generically, not this site-specific detail). This
  isn't the heatmap itself, just the data foundation: log every real
  search so the data exists to build a view against once GA4 is live and
  there's enough volume to make a heatmap meaningful.

  Privacy-by-design, deliberately minimal:
  - No user identifier, no IP address, no session/device fingerprint --
    nothing here can be tied back to a specific visitor.
  - search_lat/search_lng are rounded to 1 decimal place (~7 miles) by the
    client before insert -- coarse enough that a "My Location" (GPS)
    search can't reconstruct someone's actual address, while still being
    precise enough to see real regional search patterns.
  - The raw free-text search box contents are never stored. For a typed
    address search, and especially for "My Location" (which fills the
    search box with a reverse-geocoded formatted address that can be
    street-level), that string is exactly the kind of thing that
    shouldn't sit in a table indefinitely. state_code and scale capture
    what's actually useful for a coverage-gap view without it.
  - result_count and is_zero_results are the actual "coverage gap"
    signal Jay asked about -- a search that resolved to a region with
    zero real matches is precisely what the future heatmap needs to
    surface first.

  RLS: insert-only for the public (anon) role. No select policy for anon
  at all -- the anon key can write new rows but can never read this table
  back, so a future heatmap view has to be built with the service role
  or an authenticated admin, not queried directly from the client.
*/

CREATE TABLE IF NOT EXISTS search_events (
  search_event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now(),
  search_lat numeric(5,1),
  search_lng numeric(5,1),
  state_code text,
  scale text CHECK (scale IN ('state', 'city', 'zip', 'address')),
  service_type_slugs text[] NOT NULL DEFAULT '{}',
  radius_miles integer,
  confine_to_state boolean NOT NULL DEFAULT false,
  result_count integer NOT NULL,
  is_zero_results boolean NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS search_events_created_at_idx ON search_events (created_at);
CREATE INDEX IF NOT EXISTS search_events_state_code_idx ON search_events (state_code);
CREATE INDEX IF NOT EXISTS search_events_zero_results_idx ON search_events (is_zero_results) WHERE is_zero_results;

ALTER TABLE search_events ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Anyone can log a search event"
  ON search_events
  FOR INSERT
  TO anon, authenticated
  WITH CHECK (true);
