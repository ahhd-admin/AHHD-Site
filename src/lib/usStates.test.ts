import { describe, it, expect } from 'vitest';
import { resolveStateCode } from './usStates';

describe('resolveStateCode', () => {
  it('resolves a full state name to its 2-letter code', () => {
    expect(resolveStateCode('Alaska')).toBe('AK');
    expect(resolveStateCode('alaska')).toBe('AK');
    expect(resolveStateCode('New York')).toBe('NY');
  });

  it('passes through an already-valid 2-letter code', () => {
    expect(resolveStateCode('AK')).toBe('AK');
    expect(resolveStateCode('ak')).toBe('AK');
  });

  it('returns null for text that is not a recognizable state', () => {
    expect(resolveStateCode('Anchorage')).toBeNull();
    expect(resolveStateCode('99501')).toBeNull();
    expect(resolveStateCode('')).toBeNull();
  });
});
