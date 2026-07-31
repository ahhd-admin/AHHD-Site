import { describe, it, expect } from 'vitest';
import { calculateDistance, formatDistance } from './geoUtils';

describe('calculateDistance', () => {
  it('returns 0 for identical coordinates', () => {
    expect(calculateDistance(40.7128, -74.006, 40.7128, -74.006)).toBeCloseTo(0, 5);
  });

  it('returns a plausible distance between two known cities (NYC to Philadelphia, ~80 miles)', () => {
    const miles = calculateDistance(40.7128, -74.006, 39.9526, -75.1652);
    expect(miles).toBeGreaterThan(70);
    expect(miles).toBeLessThan(90);
  });

  it('is symmetric regardless of argument order', () => {
    const a = calculateDistance(40.7128, -74.006, 39.9526, -75.1652);
    const b = calculateDistance(39.9526, -75.1652, 40.7128, -74.006);
    expect(a).toBeCloseTo(b, 6);
  });
});

describe('formatDistance', () => {
  it('formats sub-mile distances in feet', () => {
    expect(formatDistance(0.5)).toBe('2640 ft');
  });

  it('formats mile-plus distances with one decimal', () => {
    expect(formatDistance(3.456)).toBe('3.5 mi');
  });

  it('handles exactly 1 mile as miles, not feet', () => {
    expect(formatDistance(1)).toBe('1.0 mi');
  });
});
