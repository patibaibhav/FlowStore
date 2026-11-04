import { useCallback, useEffect, useState } from 'react'
import { History as HistoryIcon, Trash2 } from 'lucide-react'
import { MiniChart } from '../charts/Charts'
import { ConfirmDialog, Empty, ErrorBanner, Skeleton, StateBadge } from '../components/ui'
import { api, fmt, qs } from '../services/api'
import type { LogEvent, MetricSample, RunRow } from '../types/api'

interface RunDetail extends RunRow { chunks: { chunk_id: string; status: string; record_count: number; partition_id: number; seal_reason: string }[] }
interface Rej { id: number; event_id: string | null; ts: number; reason: string; field: string | null }

const SORTS: [string, string][] = [['started_at', 'Start time'], ['duration_s', 'Duration'], ['generated', 'Generated'], ['persisted', 'Persisted'], ['files', 'Files'], ['s3_bytes', 'S3 bytes'], ['avg_throughput', 'Throughput']]

export function History({ openStorage }: { openStorage: (runId: string) => void }) {
  const [runs, setRuns] = useState<RunRow[] | null>(null)
  const [search, setSearch] = useState('')
  const [state, setState] = useState('')
  const [sort, setSort] = useState('started_at')
  const [desc, setDesc] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [sel, setSel] = useState<string | null>(null)
  const [detail, setDetail] = useState<RunDetail | null>(null)
  const [samples, setSamples] = useState<MetricSample[]>([])
  const [rej, setRej] = useState<Rej[]>([])
  const [events, setEvents] = useState<LogEvent[]>([])
  const [delRun, setDelRun] = useState<RunRow | null>(null)

  const load = useCallback(() => {
    api.get<RunRow[]>(`/api/runs${qs({ search, state, sort, desc })}`).then(setRuns).catch((e) => setError(e.message))
  }, [search, state, sort, desc])
  useEffect(() => { const t = setTimeout(load, 150); return () => clearTimeout(t) }, [load])

  useEffect(() => {
    if (!sel) return
    setDetail(null)
    api.get<RunDetail>(`/api/runs/${sel}`).then(setDetail).catch((e) => setError(e.message))
    api.get<{ samples: MetricSample[] }>(`/api/runs/${sel}/metrics`).then((r) => setSamples(r.samples)).catch(() => setSamples([]))
    api.get<Rej[]>(`/api/runs/${sel}/rejected`).then(setRej).catch(() => setRej([]))
    api.get<LogEvent[]>(`/api/runs/${sel}/events?limit=60`).then(setEvents).catch(() => setEvents([]))
  }, [sel])

  const removeObjects = async () => {
    if (!delRun) return
    try { await api.del(`/api/storage/runs/${delRun.run_id}?confirm=true`); setDelRun(null); load() } catch (e) { setError(e instanceof Error ? e.message : 'Delete failed'); setDelRun(null) }
  }

  return (
    <div className="space-y-4">
      <div><h2 className="text-xl font-semibold">Run history</h2><p className="text-ink-dim text-[13px] mt-1">Persisted in SQLite; survives backend restarts. Runs interrupted by a restart are marked INTERRUPTED, never COMPLETED.</p></div>
      {error && <ErrorBanner message={error} onClose={() => setError(null)} />}
      <div className="card p-3 flex flex-wrap gap-2">
        <input aria-label="Search runs" className="input !w-56" placeholder="Search run id or scenario" value={search} onChange={(e) => setSearch(e.target.value)} />
        <select aria-label="Filter state" className="input !w-auto" value={state} onChange={(e) => setState(e.target.value)}><option value="">All states</option>{['COMPLETED', 'FAILED', 'CANCELLED', 'INTERRUPTED'].map((s) => <option key={s}>{s}</option>)}</select>
        <select aria-label="Sort by" className="input !w-auto" value={sort} onChange={(e) => setSort(e.target.value)}>{SORTS.map(([v, l]) => <option key={v} value={v}>Sort: {l}</option>)}</select>
        <button className="btn" onClick={() => setDesc(!desc)}>{desc ? 'Descending' : 'Ascending'}</button>
      </div>
      <div className="card overflow-x-auto">
        {!runs ? <div className="p-4 space-y-2">{[...Array(5)].map((_, i) => <Skeleton key={i} />)}</div> : runs.length === 0 ? <Empty icon={<HistoryIcon size={24} />} title="No runs yet" hint="Start an experiment from the Live Lab." /> : (
          <table className="w-full text-[12.5px]">
            <thead><tr className="label text-left border-b border-line">{['Run', 'Scenario', 'Started', 'Duration', 'State', 'Generated', 'Persisted', 'Rejected', 'Dropped', 'Files', 'Bytes', 'Avg thr.'].map((h) => <th key={h} className="px-3 py-2">{h}</th>)}</tr></thead>
            <tbody>{runs.map((r) => (
              <tr key={r.run_id} onClick={() => setSel(r.run_id)} className={`cursor-pointer border-b border-line/60 hover:bg-surface-2 ${sel === r.run_id ? 'bg-surface-2' : ''}`}>
                <td className="px-3 py-1.5 mono text-accent">{r.run_id.slice(4, 17)}</td><td className="px-3">{r.scenario}</td>
                <td className="px-3 mono text-ink-dim">{fmt.datetime(r.started_at)}</td><td className="px-3 mono">{fmt.dur(r.duration_s)}</td>
                <td className="px-3"><StateBadge state={r.state} /></td>
                <td className="px-3 mono">{fmt.int(r.generated)}</td><td className="px-3 mono">{fmt.int(r.persisted)}</td>
                <td className="px-3 mono">{fmt.int(r.rejected_validation)}</td><td className="px-3 mono">{fmt.int(r.dropped)}</td>
                <td className="px-3 mono">{fmt.int(r.files)}</td><td className="px-3 mono">{fmt.bytes(r.s3_bytes)}</td><td className="px-3 mono">{fmt.rate(r.avg_throughput)}/s</td>
              </tr>))}</tbody>
          </table>
        )}
      </div>
      {sel && (
        <section className="card p-4 space-y-4" aria-label="Run details">
          {!detail ? <Skeleton className="h-32" /> : (
            <>
              <div className="flex flex-wrap items-center gap-3">
                <h3 className="text-base font-semibold">{detail.scenario} <span className="mono text-ink-faint text-[12px]">{detail.run_id}</span></h3><StateBadge state={detail.state} />
                <button className="btn ml-auto" onClick={() => openStorage(detail.run_id)}>Open S3 objects</button>
                <button className="btn btn-danger" disabled={!detail.files} onClick={() => setDelRun(detail)}><Trash2 size={13} /> Delete run objects</button>
              </div>
              {detail.note && <div className="text-[12px] text-warn">{detail.note}</div>}
              <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-6 gap-3 text-[12.5px]">
                {([['Peak queue depth', fmt.int(detail.peak_queue_depth)], ['Avg throughput', `${fmt.rate(detail.avg_throughput)}/s`], ['Peak throughput', `${fmt.rate(detail.peak_throughput)}/s`], ['Avg upload', fmt.ms(detail.avg_upload_ms)], ['Unpersisted', fmt.int(detail.unpersisted)], ['Failures', fmt.int(detail.failures)]] as [string, string][]).map(([k, v]) => <div key={k}><div className="label">{k}</div><div className="mono text-base">{v}</div></div>)}
              </div>
              <div className="grid gap-3 md:grid-cols-3">
                <MiniChart title="Throughput" unit="records / s" data={samples} series={[{ key: 'gen_rate', name: 'Generated', color: '#38bdf8' }, { key: 'proc_rate', name: 'Processed', color: '#34d399' }]} />
                <MiniChart title="Queue depth" unit="records" kind="area" capacityKey="queue_capacity" data={samples} series={[{ key: 'queue_depth', name: 'Depth', color: '#fbbf24' }]} />
                <MiniChart title="Cumulative persisted" unit="records" kind="area" data={samples} series={[{ key: 'persisted', name: 'Persisted', color: '#a78bfa' }]} />
              </div>
              <div className="grid gap-3 lg:grid-cols-2">
                <div><div className="label mb-1">Rejected records (sampled, bounded)</div>
                  <div className="card max-h-52 overflow-y-auto text-[12px]">{rej.length === 0 ? <Empty title="None" /> : rej.map((r) => <div key={r.id} className="flex gap-3 px-3 py-1 border-b border-line/60"><span className="mono text-ink-faint">{fmt.time(r.ts)}</span><span className="mono text-accent w-24 truncate">{r.event_id?.slice(0, 8) ?? '—'}</span><span className="flex-1 text-ink-dim">{r.reason}</span><span className="mono text-warn">{r.field}</span></div>)}</div></div>
                <div><div className="label mb-1">Chunks ({detail.chunks.length})</div>
                  <div className="card max-h-52 overflow-y-auto text-[12px]">{detail.chunks.map((c) => <div key={c.chunk_id} className="flex gap-3 px-3 py-1 border-b border-line/60"><span className="mono flex-1 truncate">{c.chunk_id.slice(-14)}</span><span className="mono">{fmt.int(c.record_count)} rec</span><span className="text-ink-faint">{c.seal_reason}</span><span className={c.status === 'PERSISTED' ? 'text-ok' : 'text-bad'}>{c.status}</span></div>)}</div></div>
              </div>
              <div><div className="label mb-1">Config</div><pre className="mono text-[11px] text-ink-dim bg-bg rounded-lg p-3 overflow-x-auto">{JSON.stringify(detail.config, null, 1)}</pre></div>
              <div><div className="label mb-1">Last events</div><div className="card max-h-44 overflow-y-auto text-[12px]">{events.map((e) => <div key={e.id} className="flex gap-3 px-3 py-1 border-b border-line/60"><span className="mono text-ink-faint">{fmt.time(e.ts)}</span><span className="text-accent w-24 truncate">{e.component}</span><span className="text-ink-dim">{e.message}</span></div>)}</div></div>
            </>
          )}
        </section>
      )}
      {delRun && <ConfirmDialog title="Delete this run's S3 objects?" confirmLabel="Delete objects" onCancel={() => setDelRun(null)} onConfirm={removeObjects} message={`All ${delRun.files} Parquet objects of ${delRun.run_id} will be permanently deleted from Amazon S3. History rows remain.`} />}
    </div>
  )
}
