import { describe, it, expect } from 'vitest';
import { SERVICE_CATEGORIES, ALL_SERVICES, getCategoryForService } from './serviceCategories';

// These guard the MVP-scope decision: only Home Care, Home Health, and
// Hospice have real, intentional data in Supabase right now. If someone
// re-enables one of the commented-out categories without first confirming
// real data exists for it, these tests should fail as a deliberate signal
// to double check that decision, not silently ship a filter with 0 results.

describe('service category scope (MVP: Home Care / Home Health / Hospice only)', () => {
  it('exposes exactly the three MVP-scope services', () => {
    expect(Object.keys(ALL_SERVICES).sort()).toEqual(
      ['home-care', 'home-health-care', 'hospice'].sort()
    );
  });

  it('labels home-health-care as "Home Health" (matches ACHC\'s own program name, not "Home Health Care")', () => {
    // This label happening to already read "Home Health" is not what fixed
    // the underlying bug -- the actual database service_type_name had to be
    // corrected to match (see migration 20260731000200) -- but the UI label
    // and the DB name should stay in agreement going forward.
    expect(ALL_SERVICES['home-health-care']).toBe('Home Health');
  });

  it('only defines the home-based category (facility-based/retail-pharmacy stay disabled)', () => {
    expect(Object.keys(SERVICE_CATEGORIES)).toEqual(['home-based']);
  });

  it('the home-based category contains only the three MVP services', () => {
    expect(SERVICE_CATEGORIES['home-based'].services.sort()).toEqual(
      ['home-care', 'home-health-care', 'hospice'].sort()
    );
  });

  it('getCategoryForService resolves each MVP service to the home-based tag', () => {
    expect(getCategoryForService('home-care')).toBe('Home-Based Services');
    expect(getCategoryForService('home-health-care')).toBe('Home-Based Services');
    expect(getCategoryForService('hospice')).toBe('Home-Based Services');
  });

  it('getCategoryForService returns null for a disabled/out-of-scope service', () => {
    expect(getCategoryForService('pharmacy')).toBeNull();
    expect(getCategoryForService('assisted-living')).toBeNull();
  });
});
