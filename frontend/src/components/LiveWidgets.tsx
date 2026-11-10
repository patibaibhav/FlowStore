import { useMemo, useState } from 'react'
import { AlertCircle, CheckCircle2, Info, Search, TriangleAlert } from 'lucide-react'
import { fmt } from '../services/api'
import type { LogEvent, Snapshot } from '../types/api'
import { Empty, InfoTip, Stat } from './ui'

export function MetricsStrip({ s }: { s: Snapshot }) {
  const c = s.counters
  const util = s.queue?.utilization ?? 0
  return (
    <div className="grid gap-2.5 grid-cols-2 md:grid-cols-4 xl:grid-cols-8">
      <Stat label="Generated" value={fmt.int(c?.generated)} sub={`of ~${fmt.int(s.planned_records)} planned`} tip="stream_ingestion" />
      <Stat label="Accepted" value={fmt.int(c?.accepted)} sub="admitted to queue" />
      <Stat label="Rejected" value={fmt.int(c?.rejected_validation)} sub={`dropped ${fmt.int(c?.dropped)}`} tone={(c?.rejected_validation ?? 0) + (c?.dropped ?? 0) > 0 ? 'warn' : undefined} />
      <Stat label="Persisted" value={fmt.int(c?.persisted)} sub={`${fmt.int(c?.files)} files · ${fmt.bytes(c?.persisted_bytes)}`} tone="ok" />
      <Stat label="Ingest rate" value={`${fmt.rate(s.rates?.generation)}/s`} sub={`target ${fmt.int(s.generator?.configured_rate)}/s`} />
      <Stat label="Throughput" value={`${fmt.rate(s.rates?.processing)}/s`} sub="processed" tip="consumer_throughput" />
      <Stat label="Queue" value={fmt.pct(util)} sub={`${fmt.int(s.queue?.depth)} / ${fmt.int(s.queue?.capacity)}`} tip="bounded_queue" tone={util >= 0.9 ? 'bad' : util >= 0.7 ? 'warn' : undefined} />
      <Stat label="S3 upload latency" value={s.writer?.avg_upload_ms ? fmt.ms(s.writer.avg_upload_ms) : '—'} sub={`last ${fmt.ms(s.writer?.recent_upload_ms)}`} tip="persistence_latency" />
    </div>
  )
}

const SEV_ICON = { INFO: Info, WARNING: TriangleAlert, ERROR: AlertCircle, SUCCESS: CheckCircle2 }
const SEV_COLOR = { INFO: 'text-ink-faint', WARNING: 'text-warn', ERROR: 'text-bad', SUCCESS: 'text-ok' }
const COMPONENTS = ['Generator', 'Queue', 'Processor', 'Validation', 'Buffer', 'Chunk Builder', 'Parquet Writer', 'Amazon S3', 'Run Controller', 'System']

export function EventLog({ events, height = 260 }: { events: LogEvent[]; height?: number }) {
  const [comp, setComp] = useState('')
  const [sev, setSev] = useState('')
  const [q, setQ] = useState('')
  const rows = useMemo(() => events.filter((e) => (!comp || e.component === comp) && (!sev || e.severity === sev) && (!q || e.message.toLowerCase().includes(q.toLowerCase()))).slice(-120).reverse(), [events, comp, sev, q])
  return (
    <section className="card p-3 flex flex-col min-w-0" aria-label="Event log">
      <div className="flex flex-wrap items-center gap-2 mb-2">
        <h3 className="label mr-auto">Event log</h3>
        <div className="relative"><Search size={12} className="absolute left-2 top-2.5 text-ink-faint" /><input aria-label="Search events" className="input !py-1 !pl-6 !w-36 text-[12px]" placeholder="Search" value={q} onChange={(e) => setQ(e.target.value)} /></div>
        <select aria-label="Filter component" className="input !py-1 !w-auto text-[12px]" value={comp} onChange={(e) => setComp(e.target.value)}><option value="">All components</option>{COMPONENTS.map((c) => <option key={c}>{c}</option>)}</select>
        <select aria-label="Filter severity" className="input !py-1 !w-auto text-[12px]" value={sev} onChange={(e) => setSev(e.target.value)}><option value="">All severities</option>{['INFO', 'WARNING', 'ERROR', 'SUCCESS'].map((c) => <option key={c}>{c}</option>)}</select>
      </div>
      <div className="overflow-y-auto" style={{ height }}>
        {rows.length === 0 ? <Empty title="No events" hint="Events appear here as the pipeline runs." /> : rows.map((e) => {
          const Icon = SEV_ICON[e.severity]
          return (
            <div key={e.seq ?? e.id} className="flex items-start gap-2 py-1 border-b border-line/60 text-[12px]">
              <Icon size={13} className={`mt-0.5 shrink-0 ${SEV_COLOR[e.severity]}`} />
              <span className="mono text-ink-faint shrink-0">{fmt.time(e.ts)}</span>
              <span className="text-accent shrink-0 w-[96px] truncate">{e.component}</span>
              <span className="text-ink-dim break-words min-w-0">{e.message}</span>
            </div>
          )
        })}
      </div>
    </section>
  )
}

export function BottleneckCard({ s }: { s: Snapshot }) {
  const b = s.bottleneck
  const flow = s.accounting
  return (
    <section className="card p-3 min-w-0" aria-label="Bottleneck inspector">
      <h3 className="label mb-1">Bottleneck inspector<InfoTip text="A heuristic reading of measured queue, processing, buffer and upload metrics. It suggests the most likely limiting stage; it is not a proven root cause." /></h3>
      {!b || b.stage === 'none' ? (
        <div className="text-ink-dim text-[13px] mt-2">{b?.summary ?? 'No active run.'}</div>
      ) : (
        <>
          <div className="text-[15px] font-semibold mt-1">{b.stage} <span className="mono text-[10px] text-ink-faint uppercase ml-1">{b.confidence} confidence · heuristic</span></div>
          <ul className="list-disc pl-4 mt-1.5 text-[12px] text-ink-dim space-y-0.5">{b.evidence.map((e) => <li key={e}>{e}</li>)}</ul>
        </>
      )}
      {flow && (
        <div className="mt-3 pt-2 border-t border-line text-[11px] mono text-ink-faint">
          in flight → queue {fmt.int(flow.in_queue as number)} · processing {fmt.int(flow.processing as number)} · buffered {fmt.int(flow.buffered as number)} · chunks {fmt.int(flow.chunks_in_flight as number)}
          {Number(flow.processor_failed) > 0 && <span className="text-bad"> · processor failed {fmt.int(flow.processor_failed as number)}</span>}
          {flow.balanced === true && <span className="text-ok ml-2">✓ accounting balanced</span>}
          {flow.balanced === false && <span className="text-bad ml-2">✗ unaccounted: {String(flow.unaccounted)}</span>}
        </div>
      )}
    </section>
  )
}
