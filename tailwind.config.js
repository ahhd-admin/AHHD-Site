/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
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
        }
      },
      fontFamily: {
        heading: ['Montserrat', 'sans-serif'],
        body: ['Open Sans', 'sans-serif'],
      },
    },
  },
  plugins: [],
}
