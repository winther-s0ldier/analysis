import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/upload': 'http://127.0.0.1:8000',
      '/profile': 'http://127.0.0.1:8000',
      '/discover': 'http://127.0.0.1:8000',
      '/analyze': 'http://127.0.0.1:8000',
      '/results': 'http://127.0.0.1:8000',
      '/synthesis': 'http://127.0.0.1:8000',
      '/report': 'http://127.0.0.1:8000',
      '/chat': 'http://127.0.0.1:8000',
      '/stream': 'http://127.0.0.1:8000',
      '/status': 'http://127.0.0.1:8000',
      '/rerun-synthesis': 'http://127.0.0.1:8000',
      '/output': 'http://127.0.0.1:8000',
      '/clarify': 'http://127.0.0.1:8000',
      '/api': 'http://127.0.0.1:8000',
      '/validate-metric': 'http://127.0.0.1:8000',
      '/add-metric': 'http://127.0.0.1:8000',
      '/chart': 'http://127.0.0.1:8000',
      '/user-activity': 'http://127.0.0.1:8000',
      '/history': 'http://127.0.0.1:8000',
      '/ga': 'http://127.0.0.1:8000',
      '/bq': 'http://127.0.0.1:8000',
    }
  }
})
