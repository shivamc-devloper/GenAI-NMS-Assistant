import type { Config } from 'tailwindcss';

const config: Config = {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        background: '#f8f9fb',
        card: '#ffffff',
        primary: '#2563eb',
        success: '#16a34a',
        muted: '#6b7280',
        border: '#e2e8f0'
      },
      boxShadow: {
        soft: '0 10px 30px -20px rgba(15, 23, 42, 0.35)',
      },
      keyframes: {
        fadeInUp: {
          '0%': { opacity: '0', transform: 'translateY(12px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
        pulseGlow: {
          '0%, 100%': { boxShadow: '0 0 0 0 rgba(37, 99, 235, 0.35)' },
          '50%': { boxShadow: '0 0 0 8px rgba(37, 99, 235, 0)' },
        },
      },
      animation: {
        fadeInUp: 'fadeInUp 0.45s ease-out both',
        pulseGlow: 'pulseGlow 2.4s ease-in-out infinite',
      },
    },
  },
  plugins: [],
};

export default config;
