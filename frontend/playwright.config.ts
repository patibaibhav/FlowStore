import { defineConfig } from '@playwright/test'

// E2E expects the full stack to be running (see TESTING.md): backend on :8000 with a reachable bucket
// (real S3 or the local moto emulator) and `npm run dev` on :5173 — or set E2E_BASE_URL to the Nginx URL.
export default defineConfig({
  testDir: './e2e',
  timeout: 90_000,
  retries: 0,
  use: {
    baseURL: process.env.E2E_BASE_URL ?? 'http://127.0.0.1:5173',
    viewport: { width: 1440, height: 900 },
    // Use the locally installed Chrome so no browser download is required; set E2E_CHANNEL= to use bundled Chromium.
    channel: process.env.E2E_CHANNEL === undefined ? 'chrome' : process.env.E2E_CHANNEL || undefined,
    screenshot: 'only-on-failure',
  },
})
