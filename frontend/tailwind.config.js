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
          page: '#FFFFFF',
          surface: '#F8FAFC',
          elevated: '#F1F5F9',
          sidebar: '#FFFFFF',
          'sidebar-item': '#F1F5F9',
          usermsg: '#0F172A',
        },
        border: {
          subtle: '#E2E8F0',
          default: '#CBD5E1',
          strong: '#94A3B8',
        },
        text: {
          primary: '#0F172A',
          secondary: '#1E293B',
          tertiary: '#475569',
          muted: '#64748B',
          faint: '#94A3B8',
          sidebar: '#0F172A',
          'sidebar-muted': '#64748B',
        },
        accent: {
          DEFAULT: '#FB7185',
          dim: 'rgba(251,113,133,0.08)',
          hover: '#F43F5E',
          gradientFrom: '#FB7185',
          gradientTo: '#FB923C',
          navy: '#0F172A',
        },
        status: {
          success: { DEFAULT: '#10B981', dim: 'rgba(16,185,129,0.08)' },
          warning: { DEFAULT: '#F59E0B', dim: 'rgba(245,158,11,0.08)' },
          error:   { DEFAULT: '#EF4444', dim: 'rgba(239,68,68,0.08)' },
          info:    { DEFAULT: '#3B82F6', dim: 'rgba(59,130,246,0.08)' },
        },
      },
      backgroundImage: {
        'gradient-brand': 'linear-gradient(135deg, #FB7185 0%, #FB923C 100%)',
        'gradient-brand-hover': 'linear-gradient(135deg, #F43F5E 0%, #F97316 100%)',
      },
      fontFamily: {
        sans: ['"Plus Jakarta Sans"', '"Inter"', 'system-ui', 'sans-serif'],
        mono: ['"JetBrains Mono"', '"Fira Code"', 'monospace'],
      },
      spacing: {
        sidebar: '220px',
      },
      boxShadow: {
        xs:    '0 1px 2px rgba(15,23,42,0.04)',
        sm:    '0 1px 4px rgba(15,23,42,0.06), 0 1px 2px rgba(15,23,42,0.04)',
        md:    '0 4px 12px rgba(15,23,42,0.06), 0 1px 3px rgba(15,23,42,0.04)',
        lg:    '0 10px 28px rgba(15,23,42,0.08), 0 2px 6px rgba(15,23,42,0.04)',
        input: '0 1px 3px rgba(15,23,42,0.05), 0 1px 1px rgba(15,23,42,0.03)',
        brand: '0 8px 24px rgba(251,113,133,0.30)',
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
