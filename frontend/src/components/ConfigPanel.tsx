import { AlertTriangle, CheckCircle2 } from 'lucide-react'
import { useLab } from '../hooks/useLab'
import { fmt } from '../services/api'
import type { RunConfig } from '../types/api'
import { InfoTip } from './ui'

type Field =
  | { key: keyof RunConfig; label: string; kind: 'number'; min: number; max: number; step?: number; unit?: string; help: string; tip?: string; when?: (c: RunConfig) => boolean }
  | { key: keyof RunConfig; label: string; kind: 'select'; options: [string, string][]; help: string; tip?: string }
  | { key: keyof RunConfig; label: string; kind: 'bool'; help: string }

const GROUPS: { title: string; fields: Field[] }[] = [
  { title: 'Generator', fields: [
    { key: 'rate_per_second', label: 'Event rate', kind: 'number', min: 1, max: 20000, unit: '/s', help: 'Target events per second (1–20,000).', tip: 'stream_ingestion' },
    { key: 'duration_seconds', label: 'Duration', kind: 'number', min: 1, max: 300, unit: 's', help: 'Run length in seconds (1–300).' },
    { key: 'num_devices', label: 'Devices', kind: 'number', min: 1, max: 10000, help: 'Simulated sensors (1–10,000). Devices hash into partitions.' },
    { key: 'invalid_ratio', label: 'Invalid ratio', kind: 'number', min: 0, max: 0.9, step: 0.05, help: 'Share of deliberately malformed events (0–0.9).' },
    { key: 'burst_enabled', label: 'Burst traffic', kind: 'bool', help: 'Periodic traffic spikes.' },
    { key: 'burst_multiplier', label: 'Burst multiplier', kind: 'number', min: 1, max: 20, step: 0.5, unit: '×', help: 'Rate multiplier during a burst.', when: (c) => c.burst_enabled },
    { key: 'burst_every_seconds', label: 'Burst every', kind: 'number', min: 2, max: 120, unit: 's', help: 'Burst period.', when: (c) => c.burst_enabled },
    { key: 'burst_duration_seconds', label: 'Burst length', kind: 'number', min: 1, max: 30, unit: 's', help: 'Must be shorter than the period.', when: (c) => c.burst_enabled },
    { key: 'seed', label: 'Random seed', kind: 'number', min: 0, max: 4294967295, help: 'Same seed → same event sequence.' },
  ] },
  { title: 'Queue & processor', fields: [
    { key: 'queue_capacity', label: 'Queue capacity', kind: 'number', min: 10, max: 100000, unit: 'rec', help: 'Hard bound on queued records (10–100,000).', tip: 'bounded_queue' },
    { key: 'overflow_policy', label: 'Overflow policy', kind: 'select', tip: 'backpressure', options: [['block', 'Block producer (backpressure)'], ['drop_newest', 'Drop newest (reject incoming)'], ['drop_oldest', 'Drop oldest (evict queued)']], help: 'What happens when the queue is full.' },
    { key: 'processor_rate_limit', label: 'Processor capacity', kind: 'number', min: 0, max: 100000, unit: '/s', help: 'Max records/s the processor may handle. 0 = unthrottled.', tip: 'consumer_throughput' },
  ] },
  { title: 'Buffers & chunks', fields: [
    { key: 'num_partitions', label: 'Partitions', kind: 'number', min: 1, max: 32, help: 'Buffer/partition count (1–32).', tip: 'partitioning' },
    { key: 'chunk_size_records', label: 'Chunk size', kind: 'number', min: 10, max: 100000, unit: 'rec', help: 'Records per chunk, per partition (10–100,000).', tip: 'buffering' },
    { key: 'chunk_max_bytes', label: 'Chunk max bytes', kind: 'number', min: 10000, max: 64000000, unit: 'B', help: 'Approximate in-memory size threshold.' },
    { key: 'chunk_max_age_seconds', label: 'Max chunk age', kind: 'number', min: 0.5, max: 120, step: 0.5, unit: 's', help: 'Flush partial buffers older than this.' },
  ] },
  { title: 'Parquet & S3', fields: [
    { key: 'compression', label: 'Compression', kind: 'select', tip: 'compression', options: [['snappy', 'Snappy'], ['zstd', 'ZSTD'], ['gzip', 'GZIP'], ['none', 'None']], help: 'Parquet column compression codec.' },
    { key: 'max_pending_uploads', label: 'Max pending uploads', kind: 'number', min: 1, max: 32, help: 'Bounded slots between chunk builder and uploaders.' },
    { key: 'upload_concurrency', label: 'Upload concurrency', kind: 'number', min: 1, max: 16, help: 'Parallel S3 uploads.' },
    { key: 'injected_upload_delay_ms', label: 'Injected upload delay', kind: 'number', min: 0, max: 5000, unit: 'ms', help: 'EXPERIMENT CONDITION: artificial delay, not real AWS latency.' },
  ] },
]

export function ConfigPanel({ compact = false }: { compact?: boolean }) {
  const { config, setConfig, isActive, estimate } = useLab()
  if (!config) return null
  const set = (k: keyof RunConfig, v: unknown) => setConfig({ ...config, [k]: v } as RunConfig)
  return (
    <div>
      {isActive && <div className="mb-3 text-[12px] text-warn bg-warn/10 border border-warn/30 rounded-lg px-3 py-2">A run is active. Its configuration is frozen; edits apply to the next run.</div>}
      <div className={`grid gap-4 ${compact ? 'grid-cols-1' : 'grid-cols-1 xl:grid-cols-2'}`}>
        {GROUPS.map((g) => (
          <fieldset key={g.title} className="card p-4">
            <legend className="label px-1">{g.title}</legend>
            <div className="grid grid-cols-2 gap-x-3 gap-y-3 mt-1">
              {g.fields.filter((f) => !('when' in f && f.when) || f.when!(config)).map((f) => {
                const val = config[f.key]
                const id = `cfg-${f.key}`
                const out = f.kind === 'number' && (Number(val) < f.min || Number(val) > f.max || Number.isNaN(Number(val)))
                return (
                  <div key={f.key} className={f.kind === 'bool' ? 'col-span-2' : ''}>
                    <label htmlFor={id} className="text-[12px] text-ink-dim flex items-center">{f.label}{'unit' in f && f.unit ? <span className="text-ink-faint ml-1">({f.unit})</span> : null}{'tip' in f && f.tip && <InfoTip term={f.tip} />}</label>
                    {f.kind === 'number' && <input id={id} className="input mt-1" type="number" min={f.min} max={f.max} step={f.step ?? 1} value={Number.isNaN(Number(val)) ? '' : String(val)}
                      onChange={(e) => set(f.key, e.target.value === '' ? NaN : Number(e.target.value))} aria-invalid={out} style={out ? { borderColor: '#f87171' } : undefined} />}
                    {f.kind === 'select' && <select id={id} className="input mt-1" value={String(val)} onChange={(e) => set(f.key, e.target.value)}>{f.options.map(([v, l]) => <option key={v} value={v}>{l}</option>)}</select>}
                    {f.kind === 'bool' && <label className="flex items-center gap-2 mt-1 cursor-pointer"><input id={id} type="checkbox" className="accent-sky-400" checked={Boolean(val)} onChange={(e) => set(f.key, e.target.checked)} /><span className="text-[12px] text-ink-faint">{f.help}</span></label>}
                    {f.kind !== 'bool' && <div className={`text-[11px] mt-0.5 ${out ? 'text-bad' : 'text-ink-faint'}`}>{out ? `Out of range: ${f.min}–${f.max}` : f.help}</div>}
                  </div>
                )
              })}
            </div>
          </fieldset>
        ))}
      </div>
      <div className="card p-4 mt-4" aria-label="Estimated workload">
        <div className="label mb-2">Estimated workload <InfoTip text="Heuristic planning numbers computed by the backend. Real values are measured during the run." /></div>
        {estimate ? (
          <>
            <div className="flex flex-wrap gap-x-8 gap-y-1 mono text-[13px]">
              <span>~{fmt.int(estimate.planned_records)} <span className="text-ink-faint">records planned</span></span>
              <span>~{fmt.int(estimate.estimated_valid_records)} <span className="text-ink-faint">valid</span></span>
              <span>≤{fmt.int(estimate.estimated_put_requests)} <span className="text-ink-faint">S3 PUT requests</span></span>
            </div>
            {estimate.errors.map((e) => <div key={e} className="flex gap-2 text-bad text-[12px] mt-2"><AlertTriangle size={14} className="shrink-0 mt-0.5" />{e}</div>)}
            {estimate.warnings.map((w) => <div key={w} className="flex gap-2 text-warn text-[12px] mt-2"><AlertTriangle size={14} className="shrink-0 mt-0.5" />{w}</div>)}
            {estimate.valid && !estimate.warnings.length && <div className="flex gap-2 text-ok text-[12px] mt-2"><CheckCircle2 size={14} />Configuration is valid.</div>}
          </>
        ) : <div className="text-ink-faint text-[12px]">Calculating…</div>}
      </div>
    </div>
  )
}
