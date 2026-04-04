/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        bg: {
          page: '#F2EDE4',
          surface: '#FAF7F2',
          elevated: '#E8E2D9',
          sidebar: '#3D2B1A',
          'sidebar-item': '#4E3522',
          usermsg: '#111827',
        },
        border: {
          subtle: '#E8E2D9',
          default: '#DDD7CC',
          strong: '#C8C0B4',
        },
        text: {
          primary: '#1C1612',
          secondary: '#3D3530',
          tertiary: '#6B6560',
          muted: '#9C9590',
          faint: '#C8C0B4',
          sidebar: '#F0EBE3',
          'sidebar-muted': '#6B6560',
        },
        accent: {
          DEFAULT: '#6366F1',
          dim: 'rgba(99,102,241,0.08)',
          hover: '#4F46E5',
        },
        status: {
          success: { DEFAULT: '#10B981', dim: 'rgba(16,185,129,0.08)' },
          warning: { DEFAULT: '#F59E0B', dim: 'rgba(245,158,11,0.08)' },
          error:   { DEFAULT: '#EF4444', dim: 'rgba(239,68,68,0.08)' },
          info:    { DEFAULT: '#3B82F6', dim: 'rgba(59,130,246,0.08)' },
        },
      },
      fontFamily: {
        sans: ['"Inter"', 'system-ui', 'sans-serif'],
        mono: ['"JetBrains Mono"', '"Fira Code"', 'monospace'],
      },
      spacing: {
        sidebar: '220px',
      },
      boxShadow: {
        xs:    '0 1px 2px rgba(0,0,0,0.04)',
        sm:    '0 1px 4px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04)',
        md:    '0 4px 12px rgba(0,0,0,0.06), 0 1px 3px rgba(0,0,0,0.04)',
        input: '0 1px 3px rgba(0,0,0,0.05), 0 1px 1px rgba(0,0,0,0.03)',
      },
      borderRadius: {
        '2xl': '1rem',
        '3xl': '1.25rem',
      },
      transitionTimingFunction: {
        spring: 'cubic-bezier(0.165, 0.84, 0.44, 1)',
      },
    },
  },
  plugins: [],
}
