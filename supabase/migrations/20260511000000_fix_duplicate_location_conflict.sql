/*
  # Fix duplicate key violation in merge_google_sheets_data

  When a location has a non-null achc_source_id (branch A), the INSERT uses
  ON CONFLICT (achc_source_id). But if the same org+address already exists
  under a *different* achc_source_id, Postgres raises a 23505 unique_violation
  on locations_org_address_idx — which the achc_source_id conflict target
  doesn't catch.

  Fix: wrap branch A's INSERT in a sub-block with an EXCEPTION WHEN
  unique_violation handler that falls back to UPDATE-by-address.
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

  c_orgs_inserted     int := 0;
  c_orgs_updated      int := 0;
  c_locs_inserted     int := 0;
  c_locs_updated      int := 0;
  c_flagged           int := 0;
  c_geocode_failed    int := 0;
BEGIN

  FOR row_data IN SELECT * FROM jsonb_array_elements(sheet_data)
  LOOP

    -- --------------------------------------------------------
    -- A. Upsert organisation
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

    INSERT INTO organizations (
      organization_name, organization_slug, website_url, main_phone,
      parent_company_name, is_active
    )
    VALUES (
      row_data->>'provider_name', v_org_slug,
      NULLIF(row_data->>'website', ''), NULLIF(row_data->>'phone', ''),
      NULLIF(row_data->>'dba_name', ''), true
    )
    ON CONFLICT (organization_slug) DO UPDATE SET
      organization_name   = CASE WHEN NULLIF(EXCLUDED.organization_name, '')  IS NOT NULL THEN EXCLUDED.organization_name  ELSE organizations.organization_name  END,
      website_url         = CASE WHEN NULLIF(EXCLUDED.website_url, '')         IS NOT NULL THEN EXCLUDED.website_url         ELSE organizations.website_url         END,
      main_phone          = CASE WHEN NULLIF(EXCLUDED.main_phone, '')          IS NOT NULL THEN EXCLUDED.main_phone          ELSE organizations.main_phone          END,
      parent_company_name = CASE WHEN NULLIF(EXCLUDED.parent_company_name, '') IS NOT NULL THEN EXCLUDED.parent_company_name ELSE organizations.parent_company_name END,
      updated_at          = now()
    RETURNING organization_id INTO v_org_id;

    IF v_org_id IS NULL THEN
      SELECT organization_id INTO v_org_id FROM organizations WHERE organization_slug = v_org_slug;
      c_orgs_updated := c_orgs_updated + 1;
    ELSE
      c_orgs_inserted := c_orgs_inserted + 1;
    END IF;

    -- --------------------------------------------------------
    -- B. Upsert location
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

    IF v_achc_source_id IS NOT NULL THEN

      -- Primary path: upsert by achc_source_id.
      -- If org+address already exists under a different achc_source_id the
      -- INSERT raises unique_violation on locations_org_address_idx; the
      -- EXCEPTION block catches it and falls back to update-by-address.
      BEGIN
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
          true, 'achc', v_geocode_status
        )
        ON CONFLICT (achc_source_id) WHERE achc_source_id IS NOT NULL
        DO UPDATE SET
          organization_id     = EXCLUDED.organization_id,
          location_name       = COALESCE(NULLIF(EXCLUDED.location_name, ''),       locations.location_name),
          address_line_1      = COALESCE(NULLIF(EXCLUDED.address_line_1, ''),      locations.address_line_1),
          city                = COALESCE(NULLIF(EXCLUDED.city, ''),                locations.city),
          state               = COALESCE(NULLIF(EXCLUDED.state, ''),               locations.state),
          postal_code         = COALESCE(NULLIF(EXCLUDED.postal_code, ''),         locations.postal_code),
          latitude            = COALESCE(EXCLUDED.latitude,  locations.latitude),
          longitude           = COALESCE(EXCLUDED.longitude, locations.longitude),
          geocode_status      = EXCLUDED.geocode_status,
          public_phone        = COALESCE(NULLIF(EXCLUDED.public_phone, ''),        locations.public_phone),
          website_url         = COALESCE(NULLIF(EXCLUDED.website_url, ''),         locations.website_url),
          source_url          = COALESCE(NULLIF(EXCLUDED.source_url, ''),          locations.source_url),
          source_last_seen_at = EXCLUDED.source_last_seen_at,
          last_verified_at    = COALESCE(EXCLUDED.last_verified_at, locations.last_verified_at),
          listing_status      = CASE WHEN v_confidence = 'possibly_inactive' THEN locations.listing_status ELSE COALESCE(EXCLUDED.listing_status, locations.listing_status) END,
          accepts_public_display = true,
          source_system       = 'achc',
          updated_at          = now()
        RETURNING location_id INTO v_location_id;

      EXCEPTION WHEN unique_violation THEN
        -- org+address already exists under a different achc_source_id;
        -- adopt the achc_source_id and update in place.
        UPDATE locations SET
          achc_source_id      = COALESCE(locations.achc_source_id, v_achc_source_id),
          organization_id     = v_org_id,
          location_name       = COALESCE(NULLIF(row_data->>'provider_name', ''),    locations.location_name),
          latitude            = COALESCE(v_lat,                                     locations.latitude),
          longitude           = COALESCE(v_lon,                                     locations.longitude),
          geocode_status      = v_geocode_status,
          public_phone        = COALESCE(NULLIF(row_data->>'phone', ''),            locations.public_phone),
          website_url         = COALESCE(NULLIF(row_data->>'website', ''),          locations.website_url),
          source_url          = COALESCE(NULLIF(row_data->>'source_url', ''),       locations.source_url),
          source_last_seen_at = NULLIF(row_data->>'last_seen', '')::timestamptz,
          last_verified_at    = COALESCE(NULLIF(row_data->>'last_verified_at', '')::timestamptz, locations.last_verified_at),
          listing_status      = CASE WHEN v_confidence = 'possibly_inactive' THEN locations.listing_status ELSE COALESCE(v_listing_status, locations.listing_status) END,
          accepts_public_display = true,
          source_system       = 'achc',
          updated_at          = now()
        WHERE organization_id = v_org_id
          AND address_line_1  = row_data->>'street_address'
          AND city            = row_data->>'city'
          AND state           = row_data->>'state'
          AND postal_code     = row_data->>'zip'
        RETURNING location_id INTO v_location_id;
      END;

    ELSE

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
        true, 'achc', v_geocode_status
      )
      ON CONFLICT (organization_id, address_line_1, city, state, postal_code)
      DO UPDATE SET
        achc_source_id      = COALESCE(locations.achc_source_id, EXCLUDED.achc_source_id),
        location_name       = COALESCE(NULLIF(EXCLUDED.location_name, ''),       locations.location_name),
        latitude            = COALESCE(EXCLUDED.latitude,  locations.latitude),
        longitude           = COALESCE(EXCLUDED.longitude, locations.longitude),
        geocode_status      = EXCLUDED.geocode_status,
        public_phone        = COALESCE(NULLIF(EXCLUDED.public_phone, ''),        locations.public_phone),
        website_url         = COALESCE(NULLIF(EXCLUDED.website_url, ''),         locations.website_url),
        source_url          = COALESCE(NULLIF(EXCLUDED.source_url, ''),          locations.source_url),
        source_last_seen_at = EXCLUDED.source_last_seen_at,
        last_verified_at    = COALESCE(EXCLUDED.last_verified_at, locations.last_verified_at),
        listing_status      = CASE WHEN v_confidence = 'possibly_inactive' THEN locations.listing_status ELSE COALESCE(EXCLUDED.listing_status, locations.listing_status) END,
        accepts_public_display = true,
        source_system       = 'achc',
        updated_at          = now()
      RETURNING location_id INTO v_location_id;

    END IF;

    IF v_location_id IS NULL THEN
      IF v_achc_source_id IS NOT NULL THEN
        SELECT location_id INTO v_location_id FROM locations WHERE achc_source_id = v_achc_source_id;
      ELSE
        SELECT location_id INTO v_location_id FROM locations
         WHERE organization_id = v_org_id AND address_line_1 = row_data->>'street_address'
           AND city = row_data->>'city' AND state = row_data->>'state' AND postal_code = row_data->>'zip';
      END IF;
      c_locs_updated := c_locs_updated + 1;
    ELSE
      c_locs_inserted := c_locs_inserted + 1;
    END IF;

    -- --------------------------------------------------------
    -- C. Upsert accreditation_record
    -- --------------------------------------------------------

    UPDATE accreditation_records
       SET is_current_record = false, updated_at = now()
     WHERE location_id = v_location_id AND accrediting_body = row_data->>'accrediting_body' AND is_current_record = true;

    INSERT INTO accreditation_records (
      location_id, accrediting_body, accreditation_status, accreditation_scope,
      source_url, imported_at, is_current_record
    ) VALUES (
      v_location_id, row_data->>'accrediting_body', 'active'::public.accreditation_status,
      NULLIF(row_data->>'result_scope', ''), row_data->>'source_url', now(), true
    )
    ON CONFLICT (location_id, accrediting_body) DO UPDATE SET
      accreditation_status = 'active'::public.accreditation_status,
      accreditation_scope  = COALESCE(NULLIF(EXCLUDED.accreditation_scope, ''), accreditation_records.accreditation_scope),
      source_url           = COALESCE(NULLIF(EXCLUDED.source_url, ''), accreditation_records.source_url),
      imported_at          = now(), is_current_record = true, updated_at = now();

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
            INSERT INTO location_service_types (location_id, service_type_id) VALUES (v_location_id, v_service_type_id) ON CONFLICT DO NOTHING;
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

    IF v_geocode_status = 'failed' THEN
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
    ELSIF v_geocode_status = 'ok' THEN
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
    'flagged',            c_flagged,
    'geocode_failed',     c_geocode_failed,
    'total_processed',    c_locs_inserted + c_locs_updated
  );

END;
$$;
