import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],

  server: {
    // In dev, proxy all /api requests to FastAPI so you don't need CORS
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },

  build: {
    // Built assets land in backend/frontend/ — FastAPI serves them as static files
    // exactly as before. No second process needed in production.
    outDir: '../backend/frontend',
    emptyOutDir: true,
  },
})
