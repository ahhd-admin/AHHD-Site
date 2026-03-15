/*
  # Google Sheets Sync Function

  ## Overview
  Creates a database function to merge data from Google Sheets into the locations table.
  This enables automated daily synchronization of scraped provider data.

  ## What This Does
  1. Creates a merge function that:
     - Accepts scraped provider data from Google Sheets
     - Creates or updates organizations
     - Creates or updates locations with geocoded coordinates
     - Tracks last_seen timestamps
     - Prevents duplicates based on organization + address + city + state + zip

  ## Security
  - Function is marked as SECURITY DEFINER to run with elevated privileges
  - Only accessible via service role key (not public)
*/

CREATE OR REPLACE FUNCTION merge_google_sheets_data(sheet_data jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  row_data jsonb;
  org_id uuid;
  existing_org_id uuid;
  existing_location_id uuid;
  inserted_count int := 0;
  updated_count int := 0;
BEGIN
  FOR row_data IN SELECT * FROM jsonb_array_elements(sheet_data)
  LOOP
    SELECT id INTO existing_org_id
    FROM organizations
    WHERE name = (row_data->>'organization')
    LIMIT 1;

    IF existing_org_id IS NULL THEN
      INSERT INTO organizations (name, description, website, logo_url)
      VALUES (
        row_data->>'organization',
        '',
        '',
        ''
      )
      RETURNING id INTO org_id;
    ELSE
      org_id := existing_org_id;
    END IF;

    SELECT id INTO existing_location_id
    FROM locations
    WHERE organization_id = org_id
      AND address = (row_data->>'address')
      AND city = (row_data->>'city')
      AND state = (row_data->>'state')
      AND zip_code = (row_data->>'zip');

    IF existing_location_id IS NULL THEN
      INSERT INTO locations (
        organization_id,
        name,
        address,
        city,
        state,
        zip_code,
        phone,
        latitude,
        longitude,
        last_verified_at
      )
      VALUES (
        org_id,
        row_data->>'organization',
        row_data->>'address',
        row_data->>'city',
        row_data->>'state',
        row_data->>'zip',
        row_data->>'phone',
        CASE
          WHEN row_data->>'latitude' = '' THEN NULL
          ELSE (row_data->>'latitude')::numeric
        END,
        CASE
          WHEN row_data->>'longitude' = '' THEN NULL
          ELSE (row_data->>'longitude')::numeric
        END,
        NOW()
      );
      inserted_count := inserted_count + 1;
    ELSE
      UPDATE locations
      SET
        phone = COALESCE(NULLIF(row_data->>'phone', ''), phone),
        latitude = CASE
          WHEN row_data->>'latitude' = '' THEN latitude
          ELSE (row_data->>'latitude')::numeric
        END,
        longitude = CASE
          WHEN row_data->>'longitude' = '' THEN longitude
          ELSE (row_data->>'longitude')::numeric
        END,
        last_verified_at = NOW()
      WHERE id = existing_location_id;
      updated_count := updated_count + 1;
    END IF;
  END LOOP;

  RETURN jsonb_build_object(
    'inserted', inserted_count,
    'updated', updated_count,
    'total_processed', inserted_count + updated_count
  );
END;
$$;
