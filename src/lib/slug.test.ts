import { describe, it, expect } from 'vitest';
import { buildProviderSlug, parseAchcSourceIdFromSlug } from './slug';

describe('buildProviderSlug', () => {
  it('builds a lowercase, hyphenated slug with the achc_source_id appended', () => {
    expect(buildProviderSlug('1st Choice Home Health Care, LLC', '75550')).toBe(
      '1st-choice-home-health-care-llc-75550'
    );
  });

  it('collapses non-alphanumeric runs into single hyphens', () => {
    expect(buildProviderSlug('A & B  Home--Care!!', '123')).toBe('a-b-home-care-123');
  });

  it('trims leading/trailing hyphens produced by punctuation at the edges', () => {
    expect(buildProviderSlug('"Best" Care, Inc.', '42')).toBe('best-care-inc-42');
  });

  it('falls back to "provider" for an empty name', () => {
    expect(buildProviderSlug('', '99')).toBe('provider-99');
  });

  it('omits the id segment entirely when achc_source_id is missing', () => {
    expect(buildProviderSlug('Some Agency', null)).toBe('some-agency');
    expect(buildProviderSlug('Some Agency', undefined)).toBe('some-agency');
  });
});

describe('parseAchcSourceIdFromSlug', () => {
  it('extracts the trailing numeric id', () => {
    expect(parseAchcSourceIdFromSlug('1st-choice-home-health-care-llc-75550')).toBe('75550');
  });

  it('returns null when there is no trailing numeric id', () => {
    expect(parseAchcSourceIdFromSlug('some-agency')).toBeNull();
  });

  it('does not mistake a numeric-looking name segment followed by non-digits for an id', () => {
    expect(parseAchcSourceIdFromSlug('24-hour-home-care')).toBeNull();
  });

  it('round-trips with buildProviderSlug', () => {
    const slug = buildProviderSlug('Aging, Disability & Transit Services', '31248');
    expect(parseAchcSourceIdFromSlug(slug)).toBe('31248');
  });
});
