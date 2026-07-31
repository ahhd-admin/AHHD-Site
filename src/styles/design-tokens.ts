/**
 * AHHD Design System Tokens
 * Based on the light blue from the logo and deep navy for structure
 */

export const colors = {
  primary: {
    50: '#E8F6FC',
    100: '#C2E8F7',
    200: '#9AD9F2',
    300: '#72CAED',
    400: '#5BB4E5',
    500: '#3FA0D9',
    600: '#2B8AC7',
    700: '#1F6FA0',
    800: '#165579',
    900: '#0E3A52'
  },
  navy: {
    50: '#E8EEF3',
    100: '#C5D4E0',
    200: '#9FB8CC',
    300: '#799CB8',
    400: '#5380A4',
    500: '#2D6490',
    600: '#1F4D75',
    700: '#1A3B5C',
    800: '#142943',
    900: '#0F1F31'
  },
  neutral: {
    50: '#F7F9FC',
    100: '#E6EAF0',
    200: '#CBD2DC',
    300: '#A8B3C3',
    400: '#8491A5',
    500: '#6B7280',
    600: '#4B5563',
    700: '#374151',
    800: '#2F3747',
    900: '#1F2937'
  },
  success: {
    50: '#ECFDF5',
    100: '#D1FAE5',
    200: '#A7F3D0',
    300: '#6EE7B7',
    400: '#34D399',
    500: '#2F8F6B',
    600: '#059669',
    700: '#047857',
    800: '#065F46',
    900: '#064E3B'
  },
  warning: {
    50: '#FFFBEB',
    100: '#FEF3C7',
    200: '#FDE68A',
    300: '#FCD34D',
    400: '#FBBF24',
    500: '#F59E0B',
    600: '#D97706',
    700: '#B45309',
    800: '#92400E',
    900: '#78350F'
  },
  error: {
    50: '#FEF2F2',
    100: '#FEE2E2',
    200: '#FECACA',
    300: '#FCA5A5',
    400: '#F87171',
    500: '#EF4444',
    600: '#DC2626',
    700: '#B91C1C',
    800: '#991B1B',
    900: '#7F1D1D'
  },
  white: '#FFFFFF',
  black: '#000000'
};

export const typography = {
  fonts: {
    heading: '"Montserrat", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
    body: '"Open Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
    mono: '"SF Mono", "Monaco", "Inconsolata", "Fira Code", monospace'
  },
  sizes: {
    xs: '0.75rem',
    sm: '0.875rem',
    base: '1.125rem',
    lg: '1.25rem',
    xl: '1.5rem',
    '2xl': '1.875rem',
    '3xl': '2.25rem',
    '4xl': '3rem',
    '5xl': '3.75rem',
    '6xl': '4.5rem'
  },
  weights: {
    normal: 400,
    medium: 500,
    semibold: 600,
    bold: 700
  },
  lineHeights: {
    tight: 1.2,
    normal: 1.5,
    relaxed: 1.75
  }
};

export const spacing = {
  0: '0',
  1: '0.25rem',
  2: '0.5rem',
  3: '0.75rem',
  4: '1rem',
  5: '1.25rem',
  6: '1.5rem',
  8: '2rem',
  10: '2.5rem',
  12: '3rem',
  16: '4rem',
  20: '5rem',
  24: '6rem'
};

export const radius = {
  none: '0',
  sm: '0.25rem',
  base: '0.5rem',
  md: '0.75rem',
  lg: '1rem',
  xl: '1.5rem',
  full: '9999px'
};

export const shadows = {
  sm: '0 1px 2px 0 rgba(0, 0, 0, 0.05)',
  base: '0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06)',
  md: '0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)',
  lg: '0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05)',
  xl: '0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04)',
  none: 'none'
};

export const breakpoints = {
  sm: '640px',
  md: '768px',
  lg: '1024px',
  xl: '1280px',
  '2xl': '1536px'
};
