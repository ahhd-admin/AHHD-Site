import { createClient } from 'npm:@supabase/supabase-js@2.57.4';

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Client-Info, Apikey",
};

interface SheetRow {
  provider_key: string;
  provider_name: string;
  dba_name: string;
  website: string;
  street_address: string;
  city: string;
  state: string;
  zip: string;
  phone: string;
  services: string;           // comma-separated
  accrediting_body: string;
  source_url: string;
  last_seen: string;
  last_verified_at: string;
  confidence_status: string;  // "verified" | "changed" | "possibly_inactive"
  searched_program_type: string;
  result_scope: string;
  latitude: string | number;
  longitude: string | number;
  achc_company_id: string;
  geocode_status: string;     // "ok" | "failed" | "pending"
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response(null, {
      status: 200,
      headers: corsHeaders,
    });
  }

  try {
    const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
    const supabaseKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
    const googleSheetsUrl = Deno.env.get('GOOGLE_SHEETS_WEB_APP_URL')!;

    if (!googleSheetsUrl) {
      throw new Error('GOOGLE_SHEETS_WEB_APP_URL not configured');
    }

    const supabase = createClient(supabaseUrl, supabaseKey);

    console.log('Fetching data from Google Sheets...');
    const sheetsResponse = await fetch(`${googleSheetsUrl}?action=get_normalized`);

    if (!sheetsResponse.ok) {
      throw new Error(`Failed to fetch from Google Sheets: ${sheetsResponse.statusText}`);
    }

    const rawData: SheetRow[] = await sheetsResponse.json();
    console.log(`Retrieved ${rawData.length} rows from Google Sheets`);

    const byCStatus = rawData.reduce((acc, r) => {
      acc[r.confidence_status] = (acc[r.confidence_status] || 0) + 1;
      return acc;
    }, {} as Record<string, number>);
    console.log('Rows by confidence_status:', JSON.stringify(byCStatus));

    const geocodeFailed = rawData.filter(r => r.geocode_status === 'failed');
    if (geocodeFailed.length > 0) {
      console.warn(`GEOCODE ALERT: ${geocodeFailed.length} locations have no coordinates and will not appear on the map. Check review_queue_items for details or enable billing at console.cloud.google.com/project/_/billing/enable`);
    }

    if (rawData.length === 0) {
      return new Response(
        JSON.stringify({ message: 'No data in Google Sheets', synced: 0 }),
        {
          headers: {
            ...corsHeaders,
            'Content-Type': 'application/json',
          },
        }
      );
    }

    // Create import_run record before calling the RPC
    let importRun: { import_run_id: string } | null = null;
    const { data: importRunData, error: importRunError } = await supabase
      .from('import_runs')
      .insert({
        import_source: 'achc_google_sheets',
        started_at: new Date().toISOString(),
        status: 'running',
        records_scraped_count: rawData.length,
      })
      .select('import_run_id')
      .single();

    if (importRunError) {
      console.error('Failed to create import_run record (continuing anyway):', importRunError);
    } else {
      importRun = importRunData;
      console.log(`Created import_run: ${importRun?.import_run_id}`);
    }

    // Process in batches to stay within PostgREST statement timeout (~8s).
    // Each batch of 500 rows takes ~2-3s; well within the limit.
    const BATCH_SIZE = 500;
    const accumulated = { orgs_inserted: 0, orgs_updated: 0, locations_inserted: 0, locations_updated: 0, flagged: 0, total_processed: 0 };

    for (let i = 0; i < rawData.length; i += BATCH_SIZE) {
      const batch = rawData.slice(i, i + BATCH_SIZE);
      console.log(`Processing batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(rawData.length / BATCH_SIZE)} (rows ${i + 1}-${Math.min(i + BATCH_SIZE, rawData.length)})`);

      const { data: batchData, error: batchError } = await supabase.rpc('merge_google_sheets_data', {
        sheet_data: batch
      });

      if (batchError) {
        console.error(`Error on batch starting at row ${i}:`, batchError);
        if (importRun) {
          await supabase
            .from('import_runs')
            .update({
              completed_at: new Date().toISOString(),
              status: 'failed',
              error_summary: batchError.message ?? String(batchError),
            })
            .eq('import_run_id', importRun.import_run_id);
        }
        throw batchError;
      }

      if (batchData) {
        accumulated.orgs_inserted      += batchData.orgs_inserted      ?? 0;
        accumulated.orgs_updated       += batchData.orgs_updated       ?? 0;
        accumulated.locations_inserted += batchData.locations_inserted ?? 0;
        accumulated.locations_updated  += batchData.locations_updated  ?? 0;
        accumulated.flagged            += batchData.flagged            ?? 0;
        accumulated.total_processed    += batchData.total_processed    ?? 0;
      }
    }

    // Update import_run with final counts
    if (importRun) {
      await supabase
        .from('import_runs')
        .update({
          completed_at: new Date().toISOString(),
          status: 'completed',
          records_created_count: accumulated.locations_inserted,
          records_updated_count: accumulated.locations_updated,
          records_flagged_count: accumulated.flagged,
        })
        .eq('import_run_id', importRun.import_run_id);
    }

    console.log(`Successfully synced ${rawData.length} rows in ${Math.ceil(rawData.length / BATCH_SIZE)} batches`);

    return new Response(
      JSON.stringify({
        success: true,
        import_run_id: importRun?.import_run_id,
        rows_received: rawData.length,
        rows_by_status: byCStatus,
        geocode_failed: geocodeFailed.length,
        geocode_alert: geocodeFailed.length > 0
          ? `${geocodeFailed.length} locations missing coordinates — review_queue_items created. Enable billing to resolve.`
          : null,
        synced: rawData.length,
        result: accumulated
      }),
      {
        headers: {
          ...corsHeaders,
          'Content-Type': 'application/json',
        },
      }
    );

  } catch (error) {
    console.error('Error in sync-google-sheets function:', error);

    const errorMessage = error instanceof Error
      ? error.message
      : (typeof error === 'object' && error !== null && 'message' in error
          ? String((error as Record<string, unknown>).message)
          : JSON.stringify(error));

    return new Response(
      JSON.stringify({
        error: errorMessage,
        success: false
      }),
      {
        status: 500,
        headers: {
          ...corsHeaders,
          'Content-Type': 'application/json',
        },
      }
    );
  }
});
