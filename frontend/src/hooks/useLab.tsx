import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import { api, ApiRequestError } from '../services/api'
import type { Estimate, LogEvent, MetricSample, Preset, RunConfig, Snapshot, StorageHealth } from '../types/api'

export const ACTIVE = ['STARTING', 'RUNNING', 'PAUSING', 'PAUSED', 'STOPPING', 'FLUSHING']
const MAX_POINTS = 240
const MAX_EVENTS = 300

export interface Lab {
  connection: 'connecting' | 'open' | 'closed'
  snapshot: Snapshot
  history: MetricSample[]
  events: LogEvent[]
  presets: Preset[]
  config: RunConfig | null
  setConfig: (c: RunConfig) => void
  presetId: string
  selectPreset: (id: string) => void
  resetConfig: () => void
  estimate: Estimate | null
  health: StorageHealth | null
  refreshHealth: () => Promise<void>
  busy: boolean
  error: string | null
  clearError: () => void
  start: () => Promise<void>
  pause: () => Promise<void>
  resume: () => Promise<void>
  stop: (force?: boolean) => Promise<void>
  isActive: boolean
}

const Ctx = createContext<Lab | null>(null)

export function useLab(): Lab {
  const v = useContext(Ctx)
  if (!v) throw new Error('useLab outside provider')
  return v
}

function toSample(s: Snapshot): MetricSample | null {
  if (!s.counters || !s.queue || !s.rates || !s.writer || !s.buffers) return null
  return {
    t: s.ts ?? Date.now() / 1000, elapsed: s.elapsed_s ?? 0, gen_rate: s.rates.generation, proc_rate: s.rates.processing,
    persist_rate: s.rates.persist, queue_depth: s.queue.depth, queue_capacity: s.queue.capacity, buffered: s.buffers.records,
    chunks: s.counters.files, chunk_rate: s.rates.chunks, upload_ms: s.writer.recent_upload_ms, persisted: s.counters.persisted,
    rejected: s.counters.rejected_validation, dropped: s.counters.dropped, reject_rate: s.rates.rejected,
    drop_rate: s.rates.dropped, pending_uploads: s.writer.pending_uploads,
  }
}

export function LabProvider({ children }: { children: ReactNode }) {
  const [connection, setConnection] = useState<Lab['connection']>('connecting')
  const [snapshot, setSnapshot] = useState<Snapshot>({ run_id: null, state: 'IDLE' })
  const [history, setHistory] = useState<MetricSample[]>([])
  const [events, setEvents] = useState<LogEvent[]>([])
  const [presets, setPresets] = useState<Preset[]>([])
  const [config, setConfig] = useState<RunConfig | null>(null)
  const [presetId, setPresetId] = useState('baseline')
  const [estimate, setEstimate] = useState<Estimate | null>(null)
  const [health, setHealth] = useState<StorageHealth | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const lastSeq = useRef(0)
  const runRef = useRef<string | null>(null)

  const apply = useCallback((snap: Snapshot, evs: LogEvent[], reset = false) => {
    setSnapshot(snap)
    if (snap.run_id !== runRef.current || reset) {
      runRef.current = snap.run_id
      setHistory([])
    }
    const sample = toSample(snap)
    if (sample && snap.run_id) {
      setHistory((h) => {
        if (h.length && h[h.length - 1].t >= sample.t) return h
        const next = [...h, sample]
        return next.length > MAX_POINTS ? next.slice(next.length - MAX_POINTS) : next
      })
    }
    if (evs.length) {
      setEvents((e) => {
        const seen = new Set(e.map((x) => x.seq))
        const add = evs.filter((x) => !seen.has(x.seq))
        return add.length ? [...e, ...add].slice(-MAX_EVENTS) : e
      })
    }
  }, [])

  // WebSocket with exponential-backoff reconnect; monotonic seq guards against stale messages
  useEffect(() => {
    let ws: WebSocket | null = null
    let closed = false
    let attempt = 0
    let timer: ReturnType<typeof setTimeout>
    const connect = () => {
      setConnection('connecting')
      const proto = location.protocol === 'https:' ? 'wss' : 'ws'
      ws = new WebSocket(`${proto}://${location.host}/ws/telemetry`)
      ws.onopen = () => { attempt = 0; setConnection('open'); lastSeq.current = 0 }
      ws.onmessage = (m) => {
        const msg = JSON.parse(m.data)
        if (msg.type === 'hello') {
          lastSeq.current = msg.seq
          apply(msg.snapshot, msg.events ?? [], true)
          return
        }
        if (msg.seq <= lastSeq.current) return
        lastSeq.current = msg.seq
        apply(msg.snapshot, msg.events ?? [])
      }
      ws.onclose = () => {
        setConnection('closed')
        if (!closed) timer = setTimeout(connect, Math.min(8000, 500 * 2 ** attempt++))
      }
      ws.onerror = () => ws?.close()
    }
    connect()
    return () => { closed = true; clearTimeout(timer); ws?.close() }
  }, [apply])

  const refreshHealth = useCallback(async () => {
    try { setHealth(await api.get<StorageHealth>('/api/storage/health')) } catch (e) {
      setError(e instanceof Error ? e.message : 'Health check failed')
    }
  }, [])

  useEffect(() => {
    api.get<Preset[]>('/api/presets').then((p) => {
      setPresets(p)
      setConfig(p[0].config)
    }).catch((e) => setError(e.message))
    api.get<StorageHealth>('/api/storage/health').then(setHealth).catch(() => undefined)
  }, [])

  // debounced server-side validation + estimate
  useEffect(() => {
    if (!config) return
    const t = setTimeout(() => {
      api.post<Estimate>('/api/runs/estimate', config).then(setEstimate).catch((e) => {
        if (e instanceof ApiRequestError && e.status === 422) setEstimate({ valid: false, errors: [e.message], planned_records: 0, estimated_valid_records: 0, estimated_chunks_upper_bound: 0, estimated_put_requests: 0, warnings: [], note: '' })
      })
    }, 300)
    return () => clearTimeout(t)
  }, [config])

  const selectPreset = useCallback((id: string) => {
    const p = presets.find((x) => x.id === id)
    if (p) { setPresetId(id); setConfig(p.config) }
  }, [presets])

  const resetConfig = useCallback(() => selectPreset(presetId), [presetId, selectPreset])

  const wrap = useCallback(async (fn: () => Promise<unknown>) => {
    setBusy(true); setError(null)
    try { await fn() } catch (e) { setError(e instanceof Error ? e.message : 'Request failed') } finally { setBusy(false) }
  }, [])

  const rid = snapshot.run_id
  const start = useCallback(() => wrap(async () => { if (config) await api.post('/api/runs', config) }), [wrap, config])
  const pause = useCallback(() => wrap(() => api.post(`/api/runs/${rid}/pause`)), [wrap, rid])
  const resume = useCallback(() => wrap(() => api.post(`/api/runs/${rid}/resume`)), [wrap, rid])
  const stop = useCallback((force = false) => wrap(() => api.post(`/api/runs/${rid}/stop`, { force })), [wrap, rid])

  const value = useMemo<Lab>(() => ({
    connection, snapshot, history, events, presets, config, setConfig, presetId, selectPreset, resetConfig, estimate,
    health, refreshHealth, busy, error, clearError: () => setError(null), start, pause, resume, stop,
    isActive: ACTIVE.includes(snapshot.state),
  }), [connection, snapshot, history, events, presets, config, presetId, selectPreset, resetConfig, estimate, health, refreshHealth, busy, error, start, pause, resume, stop])

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>
}
