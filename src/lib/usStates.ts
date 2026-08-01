// The `state` column in Supabase stores 2-letter codes ("AK"), but a
// visitor typing a location will often type the full name ("Alaska").
// Without translating that first, a naive `state.ilike.%alaska%` filter
// can never match a column that only ever contains "AK".

export const US_STATE_NAME_TO_CODE: Record<string, string> = {
  alabama: 'AL', alaska: 'AK', arizona: 'AZ', arkansas: 'AR', california: 'CA',
  colorado: 'CO', connecticut: 'CT', delaware: 'DE', florida: 'FL', georgia: 'GA',
  hawaii: 'HI', idaho: 'ID', illinois: 'IL', indiana: 'IN', iowa: 'IA',
  kansas: 'KS', kentucky: 'KY', louisiana: 'LA', maine: 'ME', maryland: 'MD',
  massachusetts: 'MA', michigan: 'MI', minnesota: 'MN', mississippi: 'MS', missouri: 'MO',
  montana: 'MT', nebraska: 'NE', nevada: 'NV', 'new hampshire': 'NH', 'new jersey': 'NJ',
  'new mexico': 'NM', 'new york': 'NY', 'north carolina': 'NC', 'north dakota': 'ND', ohio: 'OH',
  oklahoma: 'OK', oregon: 'OR', pennsylvania: 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
  'south dakota': 'SD', tennessee: 'TN', texas: 'TX', utah: 'UT', vermont: 'VT',
  virginia: 'VA', washington: 'WA', 'west virginia': 'WV', wisconsin: 'WI', wyoming: 'WY',
  'district of columbia': 'DC',
};

const VALID_STATE_CODES = new Set(Object.values(US_STATE_NAME_TO_CODE));

/** Resolves free-typed text to a state code if it's a recognizable state name or code, else null. */
export function resolveStateCode(text: string): string | null {
  const trimmed = text.trim();
  const upper = trimmed.toUpperCase();
  if (VALID_STATE_CODES.has(upper)) return upper;
  const lower = trimmed.toLowerCase();
  return US_STATE_NAME_TO_CODE[lower] ?? null;
}
