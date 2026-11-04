import { vi } from 'vitest'
import type { Preset, RunConfig } from '../types/api'

export const baseConfig: RunConfig = {
  scenario: 'Baseline Ingestion', seed: 42, rate_per_second: 500, duration_seconds: 20, num_devices: 50, invalid_ratio: 0,
  burst_enabled: false, burst_multiplier: 4, burst_every_seconds: 10, burst_duration_seconds: 3, queue_capacity: 2000,
  overflow_policy: 'block', processor_rate_limit: 0, num_partitions: 4, chunk_size_records: 1000, chunk_max_bytes: 8000000,
  chunk_max_age_seconds: 5, compression: 'snappy', max_pending_uploads: 4, upload_concurrency: 2, injected_upload_delay_ms: 0,
}
export const presets: Preset[] = [
  { id: 'baseline', name: 'Baseline Ingestion', description: 'd', expected: 'stable', config: baseConfig },
  { id: 'queue_saturation', name: 'Queue Saturation', description: 'd', expected: 'pinned', config: { ...baseConfig, scenario: 'Queue Saturation', queue_capacity: 200 } },
]
export const health = { bucket: 'b', region: 'r', prefix: 'flowstore', ready: true, write_verified: true, checks: [{ name: 'bucket_access', ok: true, detail: 'ok' }] }

export class FakeWS {
  static instances: FakeWS[] = []
  onopen: (() => void) | null = null
  onmessage: ((m: { data: string }) => void) | null = null
  onclose: (() => void) | null = null
  onerror: (() => void) | null = null
  constructor() { FakeWS.instances.push(this); setTimeout(() => this.onopen?.(), 0) }
  close() { this.onclose?.() }
  send() {}
  emit(obj: unknown) { this.onmessage?.({ data: JSON.stringify(obj) }) }
}

export function installFetch(routes: Record<string, unknown> = {}) {
  const calls: { url: string; method: string; body?: string }[] = []
  const table: Record<string, unknown> = {
    '/api/presets': presets, '/api/storage/health': health, '/api/runs/estimate': { valid: true, errors: [], warnings: [], planned_records: 10000, estimated_valid_records: 10000, estimated_chunks_upper_bound: 14, estimated_put_requests: 14, note: '' },
    '/api/runs': [], '/api/usage': { runs: 0, generated_records: 0, s3_objects: 0, parquet_bytes: 0, estimated_put_requests: 0, estimated_storage_gb: 0, estimated_monthly_storage_cost_usd: 0, estimated_put_cost_usd: 0, max_storage_mb: 512, cap_used_fraction: 0, disclaimer: 'ESTIMATES ONLY' },
    ...routes,
  }
  vi.stubGlobal('fetch', vi.fn(async (url: string, init?: RequestInit) => {
    const path = String(url).split('?')[0]
    calls.push({ url: String(url), method: init?.method ?? 'GET', body: init?.body as string | undefined })
    if (init?.method === 'POST' && path === '/api/runs') return new Response(JSON.stringify({ run_id: 'run-1', state: 'STARTING' }), { status: 202 })
    if (path.startsWith('/api/storage/objects')) return new Response(JSON.stringify(table['/api/storage/objects'] ?? { items: [], total: 0 }), { status: 200 })
    const body = table[path]
    if (body === undefined) return new Response(JSON.stringify({ message: 'not found' }), { status: 404 })
    return new Response(JSON.stringify(body), { status: 200 })
  }))
  return calls
}

export const runningSnapshot = {
  run_id: 'run-1', state: 'RUNNING', scenario: 'Baseline Ingestion', ts: 1, elapsed_s: 2, planned_records: 10000,
  counters: { generated: 100, accepted: 100, dropped: 0, dropped_newest: 0, evicted_oldest: 0, rejected_validation: 3, persisted: 50, failed: 0, discarded: 0, abandoned: 0, persisted_bytes: 4096, files: 2 },
  rates: { generation: 480, enqueue: 480, processing: 470, persist: 100, upload_bytes: 2048, chunks: 1, rejected: 1, dropped: 0 },
  generator: { configured_rate: 500, actual_rate: 480, generated: 100, done: false, burst_enabled: false },
  queue: { depth: 10, capacity: 2000, utilization: 0.005, enqueued: 100, dequeued: 90, dropped_newest: 0, evicted_oldest: 0, blocked_seconds: 0, blocked_events: 0, max_depth: 12, blocked_now: false, policy: 'block' },
  processor: { processed: 90, rate: 470, rejected: 3, reject_ratio: 0.03, latency_ms: 4, rate_limit: 0, top_rejections: [['invalid device identifier', 3]] },
  buffers: { partitions: [{ partition: 0, records: 10, bytes: 100, age_s: 1 }, { partition: 1, records: 5, bytes: 50, age_s: 1 }], records: 15, bytes: 150, active_partitions: 2, capacity: 4000, max_age_s: 1, utilization: 0.004 },
  chunks: { created: 2, pending: 0, target_size: 1000, current_max_fill: 10, builder_blocked: false, completed: 2, blocked_ms: 0 },
  writer: { writing: 0, uploading: 1, pending_uploads: 1, max_pending: 4, succeeded: 2, failed: 0, retries: 0, avg_serialize_ms: 5, avg_upload_ms: 20, recent_upload_ms: 18, compression_ratio: 2, injected_delay_ms: 0 },
  s3: { bucket: 'b', objects: 2, bytes: 4096, last_upload_at: 1, last_error: null, upload_bytes_rate: 2048, retries: 0, failures: 0 },
  nodes: { generator: 'running', queue: 'running', processor: 'running', buffers: 'running', chunk_builder: 'running', parquet_writer: 'uploading', s3: 'uploading' },
  bottleneck: { stage: 'none', confidence: 'low', summary: 'No clear bottleneck detected.', evidence: ['Queue utilization 0%'], heuristic: true },
}
