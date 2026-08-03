// Regenerates public/sitemap.xml from the current live, MVP-scope
// provider set. Run manually (`node scripts/generate_sitemap.mjs`) or as
// a step in daily-scraper.yml after the nightly merge -- reads
// SUPABASE_URL/SUPABASE_ANON_KEY from the environment either way, so no
// separate local-vs-CI branching needed.
//
// Deliberately scoped to the real MVP service types (home-care/
// home-health-care/hospice), not every `published` row -- the unscoped
// set also includes non-MVP categories (Pharmacy, DMEPOS, etc., see
// MVP-Rollout-Roadmap.md's "Trimmed from Bolt's original scope") that
// are excluded from search but would still render if crawled directly
// (ProviderDetailPage loads by achc_source_id with no service-type
// filter). Including those would tell Google to index thousands of
// pages nobody can actually reach through the site's own navigation.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const SUPA_URL = process.env.SUPABASE_URL || process.env.VITE_SUPABASE_URL;
const SUPA_KEY = process.env.SUPABASE_ANON_KEY || process.env.VITE_SUPABASE_ANON_KEY;

if (!SUPA_URL || !SUPA_KEY) {
  console.error('Missing SUPABASE_URL/SUPABASE_ANON_KEY (or VITE_-prefixed) in the environment.');
  process.exit(1);
}

// Matches src/lib/slug.ts's buildProviderSlug exactly -- the sitemap's
// URLs have to be byte-identical to what the app itself generates.
function buildProviderSlug(name, achcSourceId) {
  const namePart = (name || 'provider')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  return achcSourceId ? `${namePart}-${achcSourceId}` : namePart;
}

const SITE_URL = 'https://www.accreditedhomehealthcare.directory';

async function fetchAllProviders() {
  const pageSize = 1000;
  let offset = 0;
  const rows = [];
  while (true) {
    const url =
      `${SUPA_URL}/rest/v1/locations?select=achc_source_id,location_name,updated_at,organization:organizations(organization_name),` +
      `location_service_types!inner(service_types!inner(service_type_slug))` +
      `&listing_status=eq.published&accepts_public_display=eq.true` +
      `&location_service_types.service_types.service_type_slug=in.(home-care,home-health-care,hospice)` +
      `&offset=${offset}&limit=${pageSize}`;
    const res = await fetch(url, { headers: { apikey: SUPA_KEY, Authorization: `Bearer ${SUPA_KEY}` } });
    if (!res.ok) {
      throw new Error(`Supabase fetch failed: ${res.status} ${await res.text()}`);
    }
    const batch = await res.json();
    if (!Array.isArray(batch) || batch.length === 0) break;
    rows.push(...batch);
    if (batch.length < pageSize) break;
    offset += pageSize;
  }
  return rows;
}

console.log('Fetching all published, MVP-scope providers...');
const rows = await fetchAllProviders();
console.log(`Fetched ${rows.length} providers.`);

const urls = [
  { loc: `${SITE_URL}/`, changefreq: 'daily', priority: '1.0' },
  { loc: `${SITE_URL}/privacy`, changefreq: 'monthly', priority: '0.3' },
];

for (const row of rows) {
  const name = row.organization?.organization_name || row.location_name || 'provider';
  const slug = buildProviderSlug(name, row.achc_source_id);
  const lastmod = row.updated_at ? row.updated_at.slice(0, 10) : undefined;
  urls.push({
    loc: `${SITE_URL}/provider/${slug}`,
    lastmod,
    changefreq: 'weekly',
    priority: '0.7',
  });
}

const xml =
  `<?xml version="1.0" encoding="UTF-8"?>\n` +
  `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n` +
  urls
    .map(
      (u) =>
        `  <url>\n` +
        `    <loc>${u.loc.replace(/&/g, '&amp;')}</loc>\n` +
        (u.lastmod ? `    <lastmod>${u.lastmod}</lastmod>\n` : '') +
        `    <changefreq>${u.changefreq}</changefreq>\n` +
        `    <priority>${u.priority}</priority>\n` +
        `  </url>\n`
    )
    .join('') +
  `</urlset>\n`;

const OUT_PATH = path.join(__dirname, '..', 'public', 'sitemap.xml');
fs.writeFileSync(OUT_PATH, xml);
const sizeKb = (fs.statSync(OUT_PATH).size / 1024).toFixed(1);
console.log(`Wrote ${urls.length} URLs to sitemap.xml (${sizeKb} KB).`);
