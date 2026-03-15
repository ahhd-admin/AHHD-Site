import { createClient } from 'npm:@supabase/supabase-js@2.57.4';

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Client-Info, Apikey",
};

interface SheetRow {
  organization: string;
  program: string;
  address: string;
  city: string;
  state: string;
  zip: string;
  phone: string;
  latitude: string | number;
  longitude: string | number;
  source_url: string;
  last_seen: string;
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
    const sheetsResponse = await fetch(googleSheetsUrl);

    if (!sheetsResponse.ok) {
      throw new Error(`Failed to fetch from Google Sheets: ${sheetsResponse.statusText}`);
    }

    const sheetData: SheetRow[] = await sheetsResponse.json();
    console.log(`Retrieved ${sheetData.length} rows from Google Sheets`);

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
