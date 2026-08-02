/*
  # Don't show a location live until it has real coordinates

  merge_google_sheets_data has always set accepts_public_display = true
  unconditionally on every insert/update, regardless of geocode_status --
  a location with no lat/lng yet (pending, or a real geocode failure)
  still went live immediately. Nothing on the map would show it (the
  search query's lat/lng bounding-box filters implicitly exclude nulls),
  but it WOULD still count toward "N accredited providers" in a
  whole-state search (which filters on the `state` column directly, not
  lat/lng -- see the AHHD-Site confine-to-state work) and would still
  render as a card in Grid view with no way to actually find it on a map.

  Fix: accepts_public_display is now computed from whether the location
  ends up with real coordinates after this merge (the same
  COALESCE(incoming, existing) resolution latitude/longitude already use),
  not from the incoming row alone -- self-correcting, so a location
  automatically goes live the moment a later run successfully geocodes
  it, with no separate "republish" step needed. geocode_status is given
  the same treatment: once a location has real coordinates, later runs
  can't regress it back to 'pending'/'failed' just because that specific
  run's incoming row happened to carry a stale label (e.g. a geocode
  cache miss) -- coordinates already on file are the source of truth for
  "is this geocoded," not whatever label arrived with today's row.

  Everything else in this function is unchanged from the previous
  version (20260731000100_skip_conflicting_duplicate_locations.sql) --
  same defensive exception-safe upsert logic, same review-queue
  flagging for confidence changes and geocode failures.
*/

CREATE OR REPLACE FUNCTION merge_google_sheets_data(sheet_data jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  row_data            jsonb;
  v_org_slug          text;
  v_org_id            uuid;
  v_location_id       uuid;
  v_lat               numeric;
  v_lon               numeric;
  v_listing_status    public.listing_status;
  v_achc_source_id    text;
  v_services_raw      text;
  v_service_name      text;
  v_service_type_id   uuid;
  v_confidence        text;
  v_geocode_status    text;
  v_existing_services text;
  v_skip_row          boolean;
  v_has_coords        boolean;

  c_orgs_inserted     int := 0;
  c_orgs_updated      int := 0;
  c_locs_inserted     int := 0;
  c_locs_updated      int := 0;
  c_locs_skipped      int := 0;
  c_flagged           int := 0;
  c_geocode_failed    int := 0;
BEGIN

  FOR row_data IN SELECT * FROM jsonb_array_elements(sheet_data)
  LOOP

    -- --------------------------------------------------------
    -- A. Upsert organisation (explicit lookup, no ON CONFLICT)
    -- --------------------------------------------------------

    v_org_slug := lower(
      regexp_replace(
        regexp_replace(
          COALESCE(row_data->>'provider_name', ''),
          '[^a-zA-Z0-9 ]', '', 'g'
        ),
        '\s+', '-', 'g'
      )
    );

    SELECT organization_id INTO v_org_id FROM organizations WHERE organization_slug = v_org_slug;

    IF v_org_id IS NOT NULL THEN
      UPDATE organizations SET
        organization_name   = COALESCE(NULLIF(row_data->>'provider_name', ''), organization_name),
        website_url         = COALESCE(NULLIF(row_data->>'website', ''), website_url),
        main_phone          = COALESCE(NULLIF(row_data->>'phone', ''), main_phone),
        parent_company_name = COALESCE(NULLIF(row_data->>'dba_name', ''), parent_company_name),
        updated_at          = now()
      WHERE organization_id = v_org_id;
      c_orgs_updated := c_orgs_updated + 1;
    ELSE
      INSERT INTO organizations (
        organization_name, organization_slug, website_url, main_phone,
        parent_company_name, is_active
      ) VALUES (
        row_data->>'provider_name', v_org_slug,
        NULLIF(row_data->>'website', ''), NULLIF(row_data->>'phone', ''),
        NULLIF(row_data->>'dba_name', ''), true
      )
      RETURNING organization_id INTO v_org_id;
      c_orgs_inserted := c_orgs_inserted + 1;
    END IF;

    -- --------------------------------------------------------
    -- B. Upsert location (explicit lookup, exception-safe)
    -- --------------------------------------------------------

    v_achc_source_id := NULLIF(row_data->>'achc_company_id', '');
    v_lat := NULLIF(row_data->>'latitude',  '')::numeric;
    v_lon := NULLIF(row_data->>'longitude', '')::numeric;
    v_confidence     := COALESCE(row_data->>'confidence_status', 'verified');
    v_geocode_status := COALESCE(NULLIF(row_data->>'geocode_status', ''), CASE WHEN v_lat IS NOT NULL THEN 'ok' ELSE 'pending' END);

    v_listing_status := CASE v_confidence
      WHEN 'verified' THEN 'published'::public.listing_status
      WHEN 'changed'  THEN 'needs_review'::public.listing_status
      ELSE NULL
    END;

    v_location_id := NULL;
    v_skip_row := false;

    IF v_achc_source_id IS NOT NULL THEN
      SELECT location_id INTO v_location_id FROM locations WHERE achc_source_id = v_achc_source_id;
    END IF;

    IF v_location_id IS NULL THEN
      SELECT location_id INTO v_location_id FROM locations
       WHERE organization_id = v_org_id
         AND address_line_1  = row_data->>'street_address'
         AND city            = row_data->>'city'
         AND state           = row_data->>'state'
         AND postal_code     = row_data->>'zip';
    END IF;

    BEGIN
      IF v_location_id IS NOT NULL THEN
        -- Whether this location HAS real coordinates after this merge --
        -- either just-incoming ones, or ones already on file that this
        -- row's (possibly null/stale-labeled) data doesn't overwrite.
        -- Source of truth for "is this geocoded," not the incoming
        -- geocode_status label alone (see migration header).
        SELECT (COALESCE(v_lat, locations.latitude) IS NOT NULL AND COALESCE(v_lon, locations.longitude) IS NOT NULL)
          INTO v_has_coords
          FROM locations WHERE location_id = v_location_id;

        UPDATE locations SET
          achc_source_id       = COALESCE(locations.achc_source_id, v_achc_source_id),
          organization_id      = v_org_id,
          location_name        = COALESCE(NULLIF(row_data->>'provider_name', ''), locations.location_name),
          address_line_1       = COALESCE(NULLIF(row_data->>'street_address', ''), locations.address_line_1),
          city                 = COALESCE(NULLIF(row_data->>'city', ''), locations.city),
          state                = COALESCE(NULLIF(row_data->>'state', ''), locations.state),
          postal_code          = COALESCE(NULLIF(row_data->>'zip', ''), locations.postal_code),
          latitude              = COALESCE(v_lat, locations.latitude),
          longitude             = COALESCE(v_lon, locations.longitude),
          geocode_status        = CASE WHEN v_has_coords THEN 'ok' ELSE v_geocode_status END,
          public_phone         = COALESCE(NULLIF(row_data->>'phone', ''), locations.public_phone),
          website_url          = COALESCE(NULLIF(row_data->>'website', ''), locations.website_url),
          source_url           = COALESCE(NULLIF(row_data->>'source_url', ''), locations.source_url),
          source_last_seen_at  = COALESCE(NULLIF(row_data->>'last_seen', '')::timestamptz, locations.source_last_seen_at),
          last_verified_at     = COALESCE(NULLIF(row_data->>'last_verified_at', '')::timestamptz, locations.last_verified_at),
          listing_status       = CASE WHEN v_confidence = 'possibly_inactive' THEN locations.listing_status ELSE COALESCE(v_listing_status, locations.listing_status) END,
          accepts_public_display = v_has_coords,
          source_system        = 'achc',
          updated_at           = now()
        WHERE location_id = v_location_id;
        c_locs_updated := c_locs_updated + 1;
      ELSE
        v_has_coords := (v_lat IS NOT NULL AND v_lon IS NOT NULL);
        INSERT INTO locations (
          organization_id, achc_source_id, location_name, address_line_1,
          city, state, postal_code, latitude, longitude, public_phone,
          website_url, source_url, source_last_seen_at, last_verified_at,
          listing_status, accepts_public_display, source_system, geocode_status
        ) VALUES (
          v_org_id, v_achc_source_id, row_data->>'provider_name',
          row_data->>'street_address', row_data->>'city', row_data->>'state',
          row_data->>'zip', v_lat, v_lon,
          NULLIF(row_data->>'phone', ''), NULLIF(row_data->>'website', ''),
          row_data->>'source_url',
          NULLIF(row_data->>'last_seen', '')::timestamptz,
          NULLIF(row_data->>'last_verified_at', '')::timestamptz,
          COALESCE(v_listing_status, 'published'::public.listing_status),
          (v_lat IS NOT NULL AND v_lon IS NOT NULL), 'achc', v_geocode_status
        )
        RETURNING location_id INTO v_location_id;
        c_locs_inserted := c_locs_inserted + 1;
      END IF;
    EXCEPTION WHEN unique_violation THEN
      -- Pre-existing duplicate data conflict (see 20260731000100's
      -- header). Skip this row rather than aborting the whole batch;
      -- it's visible in the returned locations_skipped count for
      -- follow-up cleanup.
      c_locs_skipped := c_locs_skipped + 1;
      v_skip_row := true;
    END;

    CONTINUE WHEN v_skip_row;

    -- --------------------------------------------------------
    -- C. Upsert accreditation_record (explicit lookup)
    -- --------------------------------------------------------

    UPDATE accreditation_records
       SET is_current_record = false, updated_at = now()
     WHERE location_id = v_location_id AND accrediting_body = row_data->>'accrediting_body' AND is_current_record = true;

    IF EXISTS (SELECT 1 FROM accreditation_records WHERE location_id = v_location_id AND accrediting_body = row_data->>'accrediting_body') THEN
      UPDATE accreditation_records SET
        accreditation_status = 'active'::public.accreditation_status,
        accreditation_scope  = COALESCE(NULLIF(row_data->>'result_scope', ''), accreditation_scope),
        source_url           = COALESCE(NULLIF(row_data->>'source_url', ''), source_url),
        imported_at          = now(), is_current_record = true, updated_at = now()
      WHERE location_id = v_location_id AND accrediting_body = row_data->>'accrediting_body';
    ELSE
      INSERT INTO accreditation_records (
        location_id, accrediting_body, accreditation_status, accreditation_scope,
        source_url, imported_at, is_current_record
      ) VALUES (
        v_location_id, row_data->>'accrediting_body', 'active'::public.accreditation_status,
        NULLIF(row_data->>'result_scope', ''), row_data->>'source_url', now(), true
      );
    END IF;

    -- --------------------------------------------------------
    -- D. Sync service_types
    -- --------------------------------------------------------

    v_services_raw := NULLIF(row_data->>'services', '');
    IF v_services_raw IS NOT NULL THEN
      FOR v_service_name IN SELECT trim(unnest(string_to_array(v_services_raw, ','))) LOOP
        IF v_service_name <> '' THEN
          SELECT service_type_id INTO v_service_type_id
            FROM service_types WHERE lower(service_type_name) = lower(v_service_name) AND is_active = true LIMIT 1;
          IF v_service_type_id IS NOT NULL THEN
            SELECT 1 INTO v_existing_services FROM location_service_types WHERE location_id = v_location_id AND service_type_id = v_service_type_id;
            IF v_existing_services IS NULL THEN
              INSERT INTO location_service_types (location_id, service_type_id) VALUES (v_location_id, v_service_type_id);
            END IF;
          END IF;
        END IF;
      END LOOP;
    END IF;

    -- --------------------------------------------------------
    -- E. Review queue — confidence changes
    -- --------------------------------------------------------

    IF v_confidence = 'changed' THEN
      IF NOT EXISTS (SELECT 1 FROM review_queue_items WHERE location_id = v_location_id AND review_type = 'other'::public.review_type AND review_status = 'open'::public.review_status AND issue_summary LIKE 'ACHC data change%') THEN
        INSERT INTO review_queue_items (location_id, review_type, severity, review_status, issue_summary, opened_at)
        VALUES (v_location_id, 'other'::public.review_type, 'low'::public.review_severity, 'open'::public.review_status, 'ACHC data change detected for provider: ' || COALESCE(row_data->>'provider_name', '[unknown]'), now());
        c_flagged := c_flagged + 1;
      END IF;
    ELSIF v_confidence = 'possibly_inactive' THEN
      IF NOT EXISTS (SELECT 1 FROM review_queue_items WHERE location_id = v_location_id AND review_type = 'other'::public.review_type AND review_status = 'open'::public.review_status AND issue_summary LIKE 'ACHC possibly inactive%') THEN
        INSERT INTO review_queue_items (location_id, review_type, severity, review_status, issue_summary, opened_at)
        VALUES (v_location_id, 'other'::public.review_type, 'medium'::public.review_severity, 'open'::public.review_status, 'ACHC possibly inactive -- not found in latest pull: ' || COALESCE(row_data->>'provider_name', '[unknown]'), now());
        c_flagged := c_flagged + 1;
      END IF;
    END IF;

    -- --------------------------------------------------------
    -- F. Review queue — geocoding failure
    -- --------------------------------------------------------

    IF NOT v_has_coords AND v_geocode_status = 'failed' THEN
      c_geocode_failed := c_geocode_failed + 1;
      IF NOT EXISTS (
        SELECT 1 FROM review_queue_items
         WHERE location_id   = v_location_id
           AND review_type   = 'other'::public.review_type
           AND review_status = 'open'::public.review_status
           AND issue_summary LIKE 'Geocoding failed%'
      ) THEN
        INSERT INTO review_queue_items (location_id, review_type, severity, review_status, issue_summary, opened_at)
        VALUES (
          v_location_id,
          'other'::public.review_type,
          'medium'::public.review_severity,
          'open'::public.review_status,
          'Geocoding failed -- location will not appear on map: ' || COALESCE(row_data->>'provider_name', '[unknown]') || ' (' || COALESCE(row_data->>'city', '') || ', ' || COALESCE(row_data->>'state', '') || ')',
          now()
        );
      END IF;
    ELSIF v_has_coords THEN
      UPDATE review_queue_items
         SET review_status = 'resolved'::public.review_status,
             updated_at    = now()
       WHERE location_id   = v_location_id
         AND review_type   = 'other'::public.review_type
         AND review_status = 'open'::public.review_status
         AND issue_summary LIKE 'Geocoding failed%';
    END IF;

  END LOOP;

  RETURN jsonb_build_object(
    'orgs_inserted',      c_orgs_inserted,
    'orgs_updated',       c_orgs_updated,
    'locations_inserted', c_locs_inserted,
    'locations_updated',  c_locs_updated,
    'locations_skipped',  c_locs_skipped,
    'flagged',            c_flagged,
    'geocode_failed',     c_geocode_failed,
    'total_processed',    c_locs_inserted + c_locs_updated
  );

END;
$$;
