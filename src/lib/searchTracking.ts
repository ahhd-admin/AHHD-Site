import { supabase } from './supabase';

interface SearchEventInput {
  coords: { lat: number; lng: number } | null;
  scale: 'state' | 'city' | 'zip' | 'address' | null;
  stateCode: string | null;
  serviceSlugs: string[];
  radiusMiles: number;
  confineToState: boolean;
  resultCount: number;
}

// Rounded to 1 decimal place (~7 miles) so a "My Location" (GPS) search
// can't be reconstructed back into anyone's actual address -- see the
// search_events migration for the full privacy reasoning.
function roundCoord(n: number): number {
  return Math.round(n * 10) / 10;
}

// Fire-and-forget: logging a search must never slow down or break the
// search itself. Callers should not await this -- errors are swallowed
// here rather than surfaced, since a failed log write isn't something a
// visitor should ever see or be blocked by.
export function logSearchEvent(input: SearchEventInput): void {
  const scale = input.scale && ['state', 'city', 'zip', 'address'].includes(input.scale) ? input.scale : null;

  supabase
    .from('search_events')
    .insert({
      search_lat: input.coords ? roundCoord(input.coords.lat) : null,
      search_lng: input.coords ? roundCoord(input.coords.lng) : null,
      state_code: input.stateCode,
      scale,
      service_type_slugs: input.serviceSlugs,
      radius_miles: input.radiusMiles,
      confine_to_state: input.confineToState,
      result_count: input.resultCount,
      is_zero_results: input.resultCount === 0,
    })
    .then(({ error }) => {
      if (error && import.meta.env.DEV) {
        console.warn('[logSearchEvent] failed to log search event', error);
      }
    });
}
