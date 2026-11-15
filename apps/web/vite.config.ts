import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: { alias: { '@': new URL('./src', import.meta.url).pathname } },
  server: { proxy: { '/api': process.env.FLUXPLANE_WEB_API_TARGET || 'http://127.0.0.1:8040',
    '/health': process.env.FLUXPLANE_WEB_API_TARGET || 'http://127.0.0.1:8040' } },
})
