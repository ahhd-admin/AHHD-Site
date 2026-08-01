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

const CODE_TO_DISPLAY_NAME: Record<string, string> = Object.fromEntries(
  Object.entries(US_STATE_NAME_TO_CODE).map(([name, code]) => [
    code,
    name.replace(/\b\w/g, (ch) => ch.toUpperCase()),
  ])
);

/** Reverse of US_STATE_NAME_TO_CODE, title-cased -- "ME" -> "Maine". */
export function stateNameFromCode(code: string): string | null {
  return CODE_TO_DISPLAY_NAME[code.toUpperCase()] ?? null;
}

/** Resolves free-typed text to a state code if it's a recognizable state name or code, else null. */
export function resolveStateCode(text: string): string | null {
  const trimmed = text.trim();
  const upper = trimmed.toUpperCase();
  if (VALID_STATE_CODES.has(upper)) return upper;
  const lower = trimmed.toLowerCase();
  return US_STATE_NAME_TO_CODE[lower] ?? null;
}

/**
 * Finds the single state whose name the typed text is currently heading
 * toward -- either the text is a prefix of the state name ("New Y" while
 * typing "New York") or the state name is a prefix of the text ("New
 * York, USA", the text continuing past the state name). Returns null if
 * zero or more than one state matches, so a short, ambiguous prefix
 * ("N", which matches New York/New Jersey/Nevada/... ) doesn't surface a
 * misleading single suggestion. Used to offer a distinct "[State] (entire
 * state)" suggestion while typing, since Google's own Places Autocomplete
 * tends to bury or omit the bare state as an option in favor of cities
 * sharing part of its name.
 */
export function findUniqueStateMatch(text: string): { code: string; name: string } | null {
  const trimmed = text.trim().toLowerCase();
  if (trimmed.length < 2) return null;
  const matches = Object.entries(US_STATE_NAME_TO_CODE).filter(
    ([name]) => name.startsWith(trimmed) || trimmed.startsWith(name)
  );
  if (matches.length !== 1) return null;
  const [name, code] = matches[0];
  return { code, name: name.replace(/\b\w/g, (ch) => ch.toUpperCase()) };
}

/**
 * Pulls the exact 2-letter state code out of a Google geocode result's
 * address_components -- the `state` column in Supabase always stores
 * exactly this format, and it's the ground truth Google itself assigned
 * to the result (short_name on the administrative_area_level_1
 * component), not a guess parsed from free text. Used to filter results
 * to precisely the searched state (see "confine to state" in
 * SearchHero.tsx) -- a bounding-box query for an odd-shaped state like
 * Maine can include a sliver of New Hampshire near the box's corner even
 * with zero added buffer, since the box is a rectangle, not the state's
 * real shape; filtering on this column instead is exact regardless of
 * geometry.
 */
export function extractStateCode(
  addressComponents: Array<{ types: string[]; short_name: string }> | undefined
): string | null {
  const component = addressComponents?.find((c) => c.types.includes('administrative_area_level_1'));
  return component?.short_name ?? null;
}
