import { createClient } from 'npm:@supabase/supabase-js@2.57.4';

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Client-Info, Apikey",
};

interface SheetRow {
  provider_key: string;
  provider_name: string;
  street_address: string;
  city: string;
  state: string;
  zip: string;
  phone: string;
  services: string;
  accrediting_body: string;
  source_url: string;
  last_seen: string;
  last_verified_at: string;
  confidence_status: string;
  searched_program_type: string;
  result_scope: string;
  latitude?: string | number;
  longitude?: string | number;
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
    const sheetData: SheetRow[] = rawData.filter(row => row.confidence_status !== "incomplete_scrape");
    console.log(`Retrieved ${rawData.length} rows from Google Sheets, ${sheetData.length} eligible for sync (excluding incomplete_scrape)`);

    if (sheetData.length === 0) {
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

    const { data, error } = await supabase.rpc('merge_google_sheets_data', {
      sheet_data: sheetData
    });

    if (error) {
      console.error('Error calling merge function:', error);
      throw error;
    }

    console.log(`Successfully synced ${sheetData.length} rows`);

    return new Response(
      JSON.stringify({
        success: true,
        message: `Synced ${sheetData.length} providers from Google Sheets`,
        synced: sheetData.length,
        result: data
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

    const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';

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
