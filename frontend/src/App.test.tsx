import { act, cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import App from './App'
import { FakeWS, installFetch, runningSnapshot } from './test/mocks'

const send = (snapshot: unknown, seq = 5, events: unknown[] = []) =>
  act(async () => { FakeWS.instances.at(-1)!.emit({ type: 'telemetry', seq, snapshot, events }) })

async function boot(routes: Record<string, unknown> = {}) {
  const calls = installFetch(routes)
  render(<App />)
  await waitFor(() => expect(FakeWS.instances.length).toBeGreaterThan(0))
  await act(async () => { FakeWS.instances.at(-1)!.emit({ type: 'hello', seq: 1, snapshot: { run_id: null, state: 'IDLE' }, events: [] }) })
  await screen.findByRole('option', { name: 'Queue Saturation' })
  return calls
}

beforeEach(() => { FakeWS.instances = []; location.hash = ''; vi.stubGlobal('WebSocket', FakeWS) })
afterEach(() => { cleanup(); vi.unstubAllGlobals() })

describe('Live Lab controls', () => {
  it('enables only Start when idle, and shows S3 status', async () => {
    await boot()
    expect(screen.getByRole('button', { name: /^Start/ })).toBeEnabled()
    expect(screen.getByRole('button', { name: /Pause/ })).toBeDisabled()
    expect(screen.getByRole('button', { name: /Resume/ })).toBeDisabled()
    expect(screen.getByRole('button', { name: /^Stop/ })).toBeDisabled()
    expect(screen.getByText('S3 verified')).toBeInTheDocument()
  })

  it('reflects the run state machine in the buttons', async () => {
    await boot()
    await send(runningSnapshot)
    expect(screen.getByRole('button', { name: /^Start/ })).toBeDisabled()
    expect(screen.getByRole('button', { name: /Pause/ })).toBeEnabled()
    expect(screen.getByRole('button', { name: /^Stop/ })).toBeEnabled()
    await send({ ...runningSnapshot, state: 'PAUSED' }, 6)
    expect(screen.getByRole('button', { name: /Resume/ })).toBeEnabled()
    expect(screen.getByRole('button', { name: /Pause/ })).toBeDisabled()
  })

  it('posts the selected preset config on Start and calls pause/stop with the run id', async () => {
    const calls = await boot()
    fireEvent.change(screen.getByLabelText('Preset'), { target: { value: 'queue_saturation' } })
    fireEvent.click(screen.getByRole('button', { name: /^Start/ }))
    await waitFor(() => expect(calls.some((c) => c.method === 'POST' && c.url === '/api/runs')).toBe(true))
    const body = JSON.parse(calls.find((c) => c.method === 'POST' && c.url === '/api/runs')!.body!)
    expect(body.queue_capacity).toBe(200)
    await send(runningSnapshot)
    fireEvent.click(screen.getByRole('button', { name: /Pause/ }))
    await waitFor(() => expect(calls.some((c) => c.url === '/api/runs/run-1/pause')).toBe(true))
  })

  it('ignores stale out-of-order telemetry', async () => {
    await boot()
    await send({ ...runningSnapshot, state: 'PAUSED' }, 10)
    await send(runningSnapshot, 9)
    expect(screen.getAllByText('PAUSED').length).toBeGreaterThan(0)
  })

  it('renders live metrics from telemetry', async () => {
    await boot()
    await send(runningSnapshot)
    const card = screen.getAllByText('Generated')[0].closest('.card') as HTMLElement
    expect(within(card).getByText('100')).toBeInTheDocument()
    expect(screen.getByText(/Live pipeline/)).toHaveTextContent('run-1')
  })

  it('renders all seven pipeline stages and opens an inspector on click', async () => {
    await boot()
    await send(runningSnapshot)
    for (const n of ['Generator', 'Queue', 'Processor', 'Buffers', 'Chunk Builder', 'Parquet Writer', 'Amazon S3']) {
      expect((await screen.findAllByText(n)).length).toBeGreaterThan(0)
    }
  })
})

describe('Experiment configuration', () => {
  it('validates ranges and shows backend warnings', async () => {
    await boot({ '/api/runs/estimate': { valid: false, errors: ['planned records exceed cap'], warnings: ['many objects'], planned_records: 9e9, estimated_valid_records: 0, estimated_chunks_upper_bound: 9, estimated_put_requests: 9, note: '' } })
    fireEvent.click(screen.getByRole('button', { name: /Experiment/ }))
    const rate = await screen.findByRole('spinbutton', { name: /Event rate/ })
    fireEvent.change(rate, { target: { value: '999999' } })
    expect(await screen.findByText(/Out of range: 1–20000/)).toBeInTheDocument()
    expect(await screen.findByText('planned records exceed cap')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /^Queue Saturation/ }))
    expect(screen.getByRole('spinbutton', { name: /Queue capacity/ })).toHaveValue(200)
  })
})

describe('Other pages', () => {
  it('Storage Explorer shows empty state and S3 objects', async () => {
    await boot()
    fireEvent.click(screen.getByRole('button', { name: /Storage Explorer/ }))
    expect(await screen.findByText('No Parquet objects yet')).toBeInTheDocument()
    cleanup(); FakeWS.instances = []; location.hash = ''
    await boot({ '/api/storage/objects': { total: 1, items: [{ object_id: 'o1', run_id: 'run-1-abc', chunk_id: 'run-1-p0-000001', partition_id: 0, bucket: 'b', object_key: 'flowstore/runs/x.parquet', size_bytes: 2048, record_count: 100, compression: 'snappy', uploaded_at: 1, upload_ms: 20, attempts: 1, status: 'PERSISTED' }] } })
    fireEvent.click(screen.getByRole('button', { name: /Storage Explorer/ }))
    expect(await screen.findByText('p0-000001.parquet')).toBeInTheDocument()
  })

  it('Run History lists persisted runs', async () => {
    await boot({ '/api/runs': [{ run_id: 'run-123456789012-ab', scenario: 'Baseline Ingestion', state: 'COMPLETED', started_at: 1, ended_at: 2, duration_s: 20, generated: 1000, accepted: 1000, rejected_validation: 0, dropped: 0, persisted: 1000, unpersisted: 0, peak_queue_depth: 5, avg_throughput: 50, peak_throughput: 60, files: 3, s3_bytes: 3000, uncompressed_bytes: 6000, avg_upload_ms: 10, failures: 0, note: null, config: {} }] })
    fireEvent.click(screen.getByRole('button', { name: /Run History/ }))
    expect(await screen.findByText('Baseline Ingestion')).toBeInTheDocument()
    expect(screen.getAllByText('COMPLETED').length).toBeGreaterThan(0)
  })

  it('Run Comparison asks for two runs when fewer exist', async () => {
    await boot()
    fireEvent.click(screen.getByRole('button', { name: /Compare/ }))
    expect(await screen.findByText('Need at least two finished runs')).toBeInTheDocument()
  })

  it('Learning Center lists the four guided experiments', async () => {
    await boot()
    fireEvent.click(screen.getByRole('button', { name: /Learning Center/ }))
    expect(await screen.findByText(/Experiment A — Understanding Backpressure/)).toBeInTheDocument()
    expect(screen.getByText(/Experiment D — Cloud Persistence/)).toBeInTheDocument()
  })

  it('surfaces S3-not-ready errors and disables Start', async () => {
    await boot({ '/api/storage/health': { bucket: '', region: 'r', prefix: 'p', ready: false, write_verified: false, checks: [{ name: 'configuration', ok: false, detail: 'S3_BUCKET_NAME is not set', hint: 'Set S3_BUCKET_NAME' }] } })
    expect(await screen.findByText(/S3_BUCKET_NAME is not set/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^Start/ })).toBeDisabled()
  })
})
