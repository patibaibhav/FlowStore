import { expect, test } from '@playwright/test'

// Single-active-run backend: make sure a run left over from an earlier session does not block this test.
test.beforeEach(async ({ request }) => {
  const cur = await (await request.get('/api/runs/current')).json()
  if (['STARTING', 'RUNNING', 'PAUSING', 'PAUSED', 'STOPPING', 'FLUSHING'].includes(cur.state)) {
    await request.post(`/api/runs/${cur.run_id}/stop`, { data: { force: true } })
    await expect.poll(async () => (await (await request.get('/api/runs/current')).json()).state, { timeout: 30_000 }).toMatch(/COMPLETED|CANCELLED|FAILED/)
  }
})

test('complete workflow: configure → run → pause/resume → persisted S3 objects → inspect → history → compare', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText('S3 verified')).toBeVisible({ timeout: 15_000 })
  await expect(page.getByText('Live', { exact: true })).toBeVisible()

  // Configure a short run on the Experiment page
  await page.getByRole('button', { name: 'Experiment' }).click()
  await page.getByRole('button', { name: /^Baseline Ingestion/ }).click()
  await page.getByRole('spinbutton', { name: /Duration/ }).fill('40')
  await page.getByRole('spinbutton', { name: /Event rate/ }).fill('400')
  await page.getByRole('spinbutton', { name: /Chunk size/ }).fill('200')
  await expect(page.getByText('Configuration is valid.')).toBeVisible()
  await page.getByRole('button', { name: /Start & watch live/ }).click()

  // Live Lab: pipeline + state machine
  await expect(page.getByText(/Live pipeline · run-/)).toBeVisible({ timeout: 15_000 })
  for (const n of ['Generator', 'Queue', 'Processor', 'Buffers', 'Chunk Builder', 'Parquet Writer', 'Amazon S3']) {
    await expect(page.locator('.react-flow__node', { hasText: n })).toBeVisible()
  }
  const header = page.locator('header')
  await expect(header.getByText('RUNNING')).toBeVisible()
  await expect(page.getByRole('button', { name: /^Start/ })).toBeDisabled()

  await page.getByRole('button', { name: /Pause/ }).click()
  await expect(header.getByText('PAUSED')).toBeVisible({ timeout: 10_000 })
  await page.getByRole('button', { name: /Resume/ }).click()
  await expect(header.getByText('RUNNING')).toBeVisible()

  // node inspector
  await page.getByRole('button', { name: /^Queue node/ }).click()
  await expect(page.getByLabel('Queue inspector')).toContainText('bounded queue')
  await page.getByRole('button', { name: 'Close inspector' }).click()

  // graceful stop: drains queue, flushes buffers, finishes uploads, then COMPLETED
  await page.getByRole('button', { name: /^Stop/ }).click()
  await expect(header.getByText('COMPLETED')).toBeVisible({ timeout: 60_000 })

  // Storage Explorer shows real objects and inspects one
  await page.getByRole('button', { name: 'Storage Explorer' }).click()
  const rows = page.locator('tbody tr')
  await expect(rows.first()).toBeVisible()
  await rows.first().click()
  const inspector = page.getByLabel('File inspector')
  await expect(inspector).toContainText('event_id', { timeout: 15_000 })
  await expect(inspector).toContainText('SNAPPY')

  // Run history persisted
  await page.getByRole('button', { name: 'Run History' }).click()
  await expect(page.locator('tbody tr').first()).toContainText('COMPLETED')

  // Learning center
  await page.getByRole('button', { name: 'Learning Center' }).click()
  await expect(page.getByText(/Experiment A — Understanding Backpressure/)).toBeVisible()
})
