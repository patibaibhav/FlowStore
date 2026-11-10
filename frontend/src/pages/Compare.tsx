import { useEffect, useState } from 'react'
import { GitCompare } from 'lucide-react'
import { MiniChart } from '../charts/Charts'
import { Empty, ErrorBanner, StateBadge } from '../components/ui'
import { api, fmt } from '../services/api'
import type { MetricSample, RunConfig, RunRow } from '../types/api'

interface M {
  run_id: string; scenario: string; state: string; generated: number; accepted: number; persisted: number
  rejected_validation: number; dropped: number; rejection_rate: number; peak_queue_utilization: number
  avg_throughput: number; peak_throughput: number; files: number; s3_bytes: number; avg_file_size: number
  compression_ratio: number; avg_upload_ms: number; duration_s: number; failures: number; config: RunConfig
  processor_mode?: string; processor_batch_ms?: number; processor_rtt_ms?: number; processor_failed?: number
}
interface Result { runs: M[]; series: Record<string, MetricSample[]>; observations?: { kind: string; text: string }[] }

const ROWS: [string, (m: M) => number, (v: number) => string, boolean?][] = [
  ['Generated records', (m) => m.generated, fmt.int], ['Accepted records', (m) => m.accepted, fmt.int],
  ['Persisted records', (m) => m.persisted, fmt.int], ['Rejection rate', (m) => m.rejection_rate, (v) => fmt.pct(v, 1)],
  ['Records dropped', (m) => m.dropped, fmt.int], ['Peak queue utilization', (m) => m.peak_queue_utilization, (v) => fmt.pct(v)],
  ['Avg processing throughput', (m) => m.avg_throughput, (v) => `${fmt.rate(v)}/s`], ['Parquet files', (m) => m.files, fmt.int],
  ['Average file size', (m) => m.avg_file_size, fmt.bytes], ['Compression ratio', (m) => m.compression_ratio, (v) => `${v.toFixed(2)}×`],
  ['Avg S3 upload latency', (m) => m.avg_upload_ms, fmt.ms], ['Total bytes persisted', (m) => m.s3_bytes, fmt.bytes], ['Run duration', (m) => m.duration_s, (v) => fmt.dur(v)],
  ['Processor time per batch', (m) => m.processor_batch_ms ?? 0, fmt.ms],
  ['Lambda round trip (avg)', (m) => m.processor_rtt_ms ?? 0, (v) => (v ? fmt.ms(v) : '—')],
  ['Records lost in processor', (m) => m.processor_failed ?? 0, fmt.int],
]
const CFG = ['rate_per_second', 'duration_seconds', 'num_partitions', 'queue_capacity', 'overflow_policy', 'processor_rate_limit', 'processor_mode', 'invalid_ratio', 'chunk_size_records', 'chunk_max_age_seconds', 'compression', 'max_pending_uploads', 'upload_concurrency', 'injected_upload_delay_ms'] as const

export function Compare() {
  const [runs, setRuns] = useState<RunRow[]>([])
  const [a, setA] = useState('')
  const [b, setB] = useState('')
  const [res, setRes] = useState<Result | null>(null)
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    api.get<RunRow[]>('/api/runs').then((r) => {
      const done = r.filter((x) => ['COMPLETED', 'CANCELLED', 'FAILED'].includes(x.state))
      setRuns(done)
      if (done.length >= 2) { setA(done[1].run_id); setB(done[0].run_id) }
    }).catch((e) => setErr(e.message))
  }, [])
  useEffect(() => {
    if (!a || !b || a === b) { setRes(null); return }
    api.post<Result>('/api/runs/compare', { run_ids: [a, b] }).then((r) => { setRes(r); setErr(null) }).catch((e) => setErr(e.message))
  }, [a, b])

  const opt = (r: RunRow) => <option key={r.run_id} value={r.run_id}>{r.scenario} · {r.run_id.slice(4, 17)} · {fmt.datetime(r.started_at)}</option>
  return (
    <div className="space-y-4">
      <div><h2 className="text-xl font-semibold">Run comparison</h2><p className="text-ink-dim text-[13px] mt-1">Compare two finished runs side by side. Measured differences are stated as facts; explanations are labelled as hypotheses, not proven causes.</p></div>
      {err && <ErrorBanner message={err} onClose={() => setErr(null)} />}
      {runs.length < 2 ? <div className="card"><Empty icon={<GitCompare size={24} />} title="Need at least two finished runs" hint="Complete two experiments (for example Small Chunks and Large Chunks), then compare them here." /></div> : (
        <>
          <div className="card p-3 flex flex-wrap gap-3 items-center">
            <label className="text-[12px] text-ink-dim">Run A <select aria-label="Run A" className="input !w-auto ml-1" value={a} onChange={(e) => setA(e.target.value)}>{runs.map(opt)}</select></label>
            <label className="text-[12px] text-ink-dim">Run B <select aria-label="Run B" className="input !w-auto ml-1" value={b} onChange={(e) => setB(e.target.value)}>{runs.map(opt)}</select></label>
            {a === b && <span className="text-warn text-[12px]">Select two different runs.</span>}
          </div>
          {res && (
            <>
              <div className="card overflow-x-auto">
                <table className="w-full text-[13px]">
                  <thead><tr className="label text-left border-b border-line"><th className="px-4 py-2">Metric</th>{res.runs.map((r, i) => <th key={r.run_id} className="px-4 py-2">Run {i ? 'B' : 'A'} · {r.scenario} <StateBadge state={r.state} /></th>)}<th className="px-4 py-2">Difference (B − A)</th></tr></thead>
                  <tbody>{ROWS.map(([label, get, f]) => {
                    const [x, y] = [get(res.runs[0]), get(res.runs[1])]
                    const d = y - x
                    return <tr key={label} className="border-b border-line/60"><td className="px-4 py-1.5 text-ink-dim">{label}</td><td className="px-4 mono">{f(x)}</td><td className="px-4 mono">{f(y)}</td><td className={`px-4 mono ${d === 0 ? 'text-ink-faint' : d > 0 ? 'text-teal' : 'text-warn'}`}>{d > 0 ? '+' : d < 0 ? '−' : ''}{f(Math.abs(d))}</td></tr>
                  })}</tbody>
                </table>
              </div>
              <div className="card p-4">
                <div className="label mb-2">Input configuration</div>
                <div className="grid grid-cols-[1.2fr_1fr_1fr] gap-y-1 text-[12.5px]">
                  {CFG.map((k) => { const [x, y] = [res.runs[0].config[k], res.runs[1].config[k]]; const diff = x !== y
                    return <div key={k} className="contents"><div className="mono text-ink-faint">{k}</div><div className={`mono ${diff ? 'text-accent' : ''}`}>{String(x)}</div><div className={`mono ${diff ? 'text-accent' : ''}`}>{String(y)}</div></div> })}
                </div>
              </div>
              <div className="grid gap-3 md:grid-cols-2">
                {(['gen_rate', 'queue_depth'] as const).map((k) => (
                  <MiniChart key={k} title={k === 'gen_rate' ? 'Generation rate: A vs B' : 'Queue depth: A vs B'} unit={k === 'gen_rate' ? 'records / s' : 'records'}
                    data={mergeSeries(res.series[res.runs[0].run_id], res.series[res.runs[1].run_id], k)}
                    series={[{ key: 'a', name: 'Run A', color: '#38bdf8' }, { key: 'b', name: 'Run B', color: '#fbbf24' }]} />
                ))}
              </div>
              <div className="card p-4" aria-label="Observations">
                <div className="label mb-2">Observations</div>
                <ul className="space-y-1.5 text-[13px]">
                  {res.observations?.map((o, i) => <li key={i} className="flex gap-2"><span className={`mono text-[10px] uppercase mt-0.5 px-1.5 rounded h-fit ${o.kind === 'measured' ? 'bg-teal/15 text-teal' : o.kind === 'hypothesis' ? 'bg-violet/15 text-violet' : 'bg-surface-3 text-ink-dim'}`}>{o.kind}</span><span className="text-ink-dim">{o.text}</span></li>)}
                </ul>
              </div>
            </>
          )}
        </>
      )}
    </div>
  )
}

function mergeSeries(a: MetricSample[] = [], b: MetricSample[] = [], key: 'gen_rate' | 'queue_depth'): MetricSample[] {
  const n = Math.max(a.length, b.length)
  return Array.from({ length: n }, (_, i) => ({ elapsed: a[i]?.elapsed ?? b[i]?.elapsed ?? i, a: a[i]?.[key], b: b[i]?.[key] }) as unknown as MetricSample)
}
