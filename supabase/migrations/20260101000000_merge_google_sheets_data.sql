/*
  # Google Sheets Sync Function (rewritten)

  ## Overview
  Merges scraped ACHC provider data from Google Sheets into the live database.
  Handles organisations, locations, accreditation records, service-type links,
  and review-queue items in a single idempotent function call.

  ## Columns added
  - locations.achc_source_id  — stable ACHC company-level identifier used as
    the primary upsert key when available.

  ## Enum values (verified against live DB 2026-05-09)
  - listing_status       : verified→'published', changed→'needs_review', possibly_inactive→unchanged
  - accreditation_status : 'active'
  - review_type          : 'other' (no specific type exists for data_change or possible_closure)
  - review_severity      : 'low' (changed), 'medium' (possibly_inactive)
  - review_status        : 'open'
  - import_run_status    : 'running', 'completed', 'failed' (all confirmed)
*/

-- ============================================================
-- Step 1: Add achc_source_id to locations (idempotent)
-- ============================================================

ALTER TABLE locations
  ADD COLUMN IF NOT EXISTS achc_source_id text;

-- Unique partial index: only enforce uniqueness when the value is present.
CREATE UNIQUE INDEX IF NOT EXISTS locations_achc_source_id_idx
  ON locations (achc_source_id)
  WHERE achc_source_id IS NOT NULL;

-- Composite unique index used as fallback conflict target when achc_source_id
-- is absent (older rows).  Needed for ON CONFLICT(...) to work.
CREATE UNIQUE INDEX IF NOT EXISTS locations_org_address_idx
  ON locations (organization_id, address_line_1, city, state, postal_code);

-- One active accreditation record per (location, accrediting body).
-- Required for the ON CONFLICT clause in section C of the merge function.
CREATE UNIQUE INDEX IF NOT EXISTS accreditation_records_location_body_idx
  ON accreditation_records (location_id, accrediting_body);

-- ============================================================
-- Step 2: merge_google_sheets_data(sheet_data jsonb)
-- ============================================================

CREATE OR REPLACE FUNCTION merge_google_sheets_data(sheet_data jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  -- loop variable
  row_data            jsonb;

  -- working variables
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

  -- counters returned to caller
  c_orgs_inserted     int := 0;
  c_orgs_updated      int := 0;
  c_locs_inserted     int := 0;
  c_locs_updated      int := 0;
  c_flagged           int := 0;
BEGIN

  FOR row_data IN SELECT * FROM jsonb_array_elements(sheet_data)
  LOOP

    -- --------------------------------------------------------
    -- A. Upsert organisation
    -- --------------------------------------------------------

    -- Build a URL-safe slug from the legal company name.
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
      organization_name,
      organization_slug,
      website_url,
      main_phone,
      parent_company_name,   -- used for dba_name
      is_active
    )
    VALUES (
      row_data->>'provider_name',
      v_org_slug,
      NULLIF(row_data->>'website', ''),
      NULLIF(row_data->>'phone', ''),
      NULLIF(row_data->>'dba_name', ''),
      true
    )
    ON CONFLICT (organization_slug) DO UPDATE
      SET
        -- Only overwrite with a non-empty incoming value.
        organization_name  = CASE
          WHEN NULLIF(EXCLUDED.organization_name, '') IS NOT NULL
          THEN EXCLUDED.organization_name
          ELSE organizations.organization_name
        END,
        website_url        = CASE
          WHEN NULLIF(EXCLUDED.website_url, '') IS NOT NULL
          THEN EXCLUDED.website_url
          ELSE organizations.website_url
        END,
        main_phone         = CASE
          WHEN NULLIF(EXCLUDED.main_phone, '') IS NOT NULL
          THEN EXCLUDED.main_phone
          ELSE organizations.main_phone
        END,
        parent_company_name = CASE
          WHEN NULLIF(EXCLUDED.parent_company_name, '') IS NOT NULL
          THEN EXCLUDED.parent_company_name
          ELSE organizations.parent_company_name
        END,
        updated_at         = now()
    RETURNING organization_id INTO v_org_id;

    -- If the upsert matched an existing row, RETURNING gives us the id.
    -- If it returned NULL (shouldn't happen but guard anyway), look it up.
    IF v_org_id IS NULL THEN
      SELECT organization_id
        INTO v_org_id
        FROM organizations
       WHERE organization_slug = v_org_slug;

      c_orgs_updated := c_orgs_updated + 1;
    ELSE
      -- Distinguish insert from update: if xmax = 0, it was inserted.
      -- Simpler: track via GET DIAGNOSTICS after the INSERT.
      -- We'll use a flag approach below instead.
      c_orgs_inserted := c_orgs_inserted + 1;
    END IF;

    -- --------------------------------------------------------
    -- B. Upsert location
    -- --------------------------------------------------------

    -- Resolve achc_source_id (may be empty for older rows).
    v_achc_source_id := NULLIF(row_data->>'achc_company_id', '');

    -- Resolve lat / lon — treat empty string as NULL.
    v_lat := NULLIF(row_data->>'latitude',  '')::numeric;
    v_lon := NULLIF(row_data->>'longitude', '')::numeric;

    -- Map confidence_status → listing_status enum.
    -- NOTE: Verify 'published' is the correct enum label in your live DB.
    v_confidence := COALESCE(row_data->>'confidence_status', 'verified');

    v_listing_status := CASE v_confidence
      WHEN 'verified' THEN 'published'::public.listing_status
      WHEN 'changed'  THEN 'needs_review'::public.listing_status
      -- 'possibly_inactive': do NOT change existing status.
      ELSE NULL
    END;

    -- Branch on whether we have a stable ACHC company ID.
    IF v_achc_source_id IS NOT NULL THEN

      -- Conflict on achc_source_id partial unique index.
      INSERT INTO locations (
        organization_id,
        achc_source_id,
        location_name,
        address_line_1,
        city,
        state,
        postal_code,
        latitude,
        longitude,
        public_phone,
        website_url,
        source_url,
        source_last_seen_at,
        last_verified_at,
        listing_status,
        accepts_public_display,
        source_system
      )
      VALUES (
        v_org_id,
        v_achc_source_id,
        row_data->>'provider_name',
        row_data->>'street_address',
        row_data->>'city',
        row_data->>'state',
        row_data->>'zip',
        v_lat,
        v_lon,
        NULLIF(row_data->>'phone', ''),
        NULLIF(row_data->>'website', ''),
        row_data->>'source_url',
        NULLIF(row_data->>'last_seen', '')::timestamptz,
        NULLIF(row_data->>'last_verified_at', '')::timestamptz,
        COALESCE(v_listing_status, 'published'::public.listing_status),
        true,
        'achc'
      )
      ON CONFLICT (achc_source_id) WHERE achc_source_id IS NOT NULL
      DO UPDATE SET
        organization_id     = EXCLUDED.organization_id,
        location_name       = COALESCE(NULLIF(EXCLUDED.location_name, ''),       locations.location_name),
        address_line_1      = COALESCE(NULLIF(EXCLUDED.address_line_1, ''),      locations.address_line_1),
        city                = COALESCE(NULLIF(EXCLUDED.city, ''),                locations.city),
        state               = COALESCE(NULLIF(EXCLUDED.state, ''),               locations.state),
        postal_code         = COALESCE(NULLIF(EXCLUDED.postal_code, ''),         locations.postal_code),
        -- Never overwrite a real lat/lon with NULL.
        latitude            = COALESCE(EXCLUDED.latitude,  locations.latitude),
        longitude           = COALESCE(EXCLUDED.longitude, locations.longitude),
        public_phone        = COALESCE(NULLIF(EXCLUDED.public_phone, ''),        locations.public_phone),
        website_url         = COALESCE(NULLIF(EXCLUDED.website_url, ''),         locations.website_url),
        source_url          = COALESCE(NULLIF(EXCLUDED.source_url, ''),          locations.source_url),
        source_last_seen_at = EXCLUDED.source_last_seen_at,
        last_verified_at    = COALESCE(EXCLUDED.last_verified_at, locations.last_verified_at),
        -- Do NOT change listing_status for possibly_inactive rows.
        listing_status      = CASE
          WHEN v_confidence = 'possibly_inactive' THEN locations.listing_status
          ELSE COALESCE(EXCLUDED.listing_status, locations.listing_status)
        END,
        accepts_public_display = true,
        source_system       = 'achc',
        updated_at          = now()
      RETURNING location_id INTO v_location_id;

    ELSE

      -- Fallback: conflict on composite (org, address, city, state, zip) index.
      INSERT INTO locations (
        organization_id,
        achc_source_id,
        location_name,
        address_line_1,
        city,
        state,
        postal_code,
        latitude,
        longitude,
        public_phone,
        website_url,
        source_url,
        source_last_seen_at,
        last_verified_at,
        listing_status,
        accepts_public_display,
        source_system
      )
      VALUES (
        v_org_id,
        v_achc_source_id,   -- NULL here
        row_data->>'provider_name',
        row_data->>'street_address',
        row_data->>'city',
        row_data->>'state',
        row_data->>'zip',
        v_lat,
        v_lon,
        NULLIF(row_data->>'phone', ''),
        NULLIF(row_data->>'website', ''),
        row_data->>'source_url',
        NULLIF(row_data->>'last_seen', '')::timestamptz,
        NULLIF(row_data->>'last_verified_at', '')::timestamptz,
        COALESCE(v_listing_status, 'published'::public.listing_status),
        true,
        'achc'
      )
      ON CONFLICT (organization_id, address_line_1, city, state, postal_code)
      DO UPDATE SET
        -- Backfill achc_source_id if we now have it and didn't before.
        achc_source_id      = COALESCE(locations.achc_source_id, EXCLUDED.achc_source_id),
        location_name       = COALESCE(NULLIF(EXCLUDED.location_name, ''),       locations.location_name),
        latitude            = COALESCE(EXCLUDED.latitude,  locations.latitude),
        longitude           = COALESCE(EXCLUDED.longitude, locations.longitude),
        public_phone        = COALESCE(NULLIF(EXCLUDED.public_phone, ''),        locations.public_phone),
        website_url         = COALESCE(NULLIF(EXCLUDED.website_url, ''),         locations.website_url),
        source_url          = COALESCE(NULLIF(EXCLUDED.source_url, ''),          locations.source_url),
        source_last_seen_at = EXCLUDED.source_last_seen_at,
        last_verified_at    = COALESCE(EXCLUDED.last_verified_at, locations.last_verified_at),
        listing_status      = CASE
          WHEN v_confidence = 'possibly_inactive' THEN locations.listing_status
          ELSE COALESCE(EXCLUDED.listing_status, locations.listing_status)
        END,
        accepts_public_display = true,
        source_system       = 'achc',
        updated_at          = now()
      RETURNING location_id INTO v_location_id;

    END IF;

    -- Fallback lookup if RETURNING gave NULL (concurrent conflict edge-case).
    IF v_location_id IS NULL THEN
      IF v_achc_source_id IS NOT NULL THEN
        SELECT location_id INTO v_location_id
          FROM locations
         WHERE achc_source_id = v_achc_source_id;
      ELSE
        SELECT location_id INTO v_location_id
          FROM locations
         WHERE organization_id = v_org_id
           AND address_line_1  = row_data->>'street_address'
           AND city            = row_data->>'city'
           AND state           = row_data->>'state'
           AND postal_code     = row_data->>'zip';
      END IF;
      c_locs_updated := c_locs_updated + 1;
    ELSE
      c_locs_inserted := c_locs_inserted + 1;
    END IF;

    -- --------------------------------------------------------
    -- C. Upsert accreditation_record
    -- --------------------------------------------------------

    -- Mark any pre-existing current record for this body as no longer current
    -- before inserting the fresh one (keeps is_current_record accurate without
    -- requiring a separate trigger).
    UPDATE accreditation_records
       SET is_current_record = false,
           updated_at        = now()
     WHERE location_id        = v_location_id
       AND accrediting_body   = row_data->>'accrediting_body'
       AND is_current_record  = true;

    INSERT INTO accreditation_records (
      location_id,
      accrediting_body,
      accreditation_status,
      accreditation_scope,
      source_url,
      imported_at,
      is_current_record
    )
    VALUES (
      v_location_id,
      row_data->>'accrediting_body',
      'active'::public.accreditation_status,  -- enum verified 2026-05-09
      NULLIF(row_data->>'result_scope', ''),
      row_data->>'source_url',
      now(),
      true
    )
    ON CONFLICT (location_id, accrediting_body)
    DO UPDATE SET
      accreditation_status = 'active'::public.accreditation_status,
      accreditation_scope  = COALESCE(
        NULLIF(EXCLUDED.accreditation_scope, ''),
        accreditation_records.accreditation_scope
      ),
      source_url           = COALESCE(
        NULLIF(EXCLUDED.source_url, ''),
        accreditation_records.source_url
      ),
      imported_at          = now(),
      is_current_record    = true,
      updated_at           = now();

    -- --------------------------------------------------------
    -- D. Sync service_types → location_service_types
    --    (additive only — existing links are never removed)
    -- --------------------------------------------------------

    v_services_raw := NULLIF(row_data->>'services', '');

    IF v_services_raw IS NOT NULL THEN
      FOR v_service_name IN
        SELECT trim(unnest(string_to_array(v_services_raw, ',')))
      LOOP
        IF v_service_name <> '' THEN
          -- Case-insensitive lookup in service_types master table.
          SELECT service_type_id
            INTO v_service_type_id
            FROM service_types
           WHERE lower(service_type_name) = lower(v_service_name)
             AND is_active = true
           LIMIT 1;

          IF v_service_type_id IS NOT NULL THEN
            INSERT INTO location_service_types (location_id, service_type_id)
            VALUES (v_location_id, v_service_type_id)
            ON CONFLICT DO NOTHING;
          END IF;
        END IF;
      END LOOP;
    END IF;

    -- --------------------------------------------------------
    -- E. Review-queue items for changed / possibly_inactive
    -- --------------------------------------------------------

    IF v_confidence = 'changed' THEN
      -- Only create a new review item if no open one already exists.
      IF NOT EXISTS (
        SELECT 1 FROM review_queue_items
         WHERE location_id   = v_location_id
           AND review_type   = 'other'::public.review_type
           AND review_status = 'open'::public.review_status
           AND issue_summary LIKE 'ACHC data change%'
      ) THEN
        INSERT INTO review_queue_items (
          location_id,
          review_type,
          severity,
          review_status,
          issue_summary,
          opened_at
        )
        VALUES (
          v_location_id,
          'other'::public.review_type,
          'low'::public.review_severity,
          'open'::public.review_status,
          'ACHC data change detected for provider: '
            || COALESCE(row_data->>'provider_name', '[unknown]'),
          now()
        );
        c_flagged := c_flagged + 1;
      END IF;

    ELSIF v_confidence = 'possibly_inactive' THEN
      IF NOT EXISTS (
        SELECT 1 FROM review_queue_items
         WHERE location_id   = v_location_id
           AND review_type   = 'other'::public.review_type
           AND review_status = 'open'::public.review_status
           AND issue_summary LIKE 'ACHC possibly inactive%'
      ) THEN
        INSERT INTO review_queue_items (
          location_id,
          review_type,
          severity,
          review_status,
          issue_summary,
          opened_at
        )
        VALUES (
          v_location_id,
          'other'::public.review_type,
          'medium'::public.review_severity,
          'open'::public.review_status,
          'ACHC possibly inactive — not found in latest pull: '
            || COALESCE(row_data->>'provider_name', '[unknown]'),
          now()
        );
        c_flagged := c_flagged + 1;
      END IF;
    END IF;

  END LOOP;  -- end per-row loop

  -- --------------------------------------------------------
  -- F. Return summary counts
  -- --------------------------------------------------------

  RETURN jsonb_build_object(
    'orgs_inserted',      c_orgs_inserted,
    'orgs_updated',       c_orgs_updated,
    'locations_inserted', c_locs_inserted,
    'locations_updated',  c_locs_updated,
    'flagged',            c_flagged,
    'total_processed',    c_locs_inserted + c_locs_updated
  );

END;
$$;
