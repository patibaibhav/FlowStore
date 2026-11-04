import { useCallback, useEffect, useState } from 'react'
import { Cloud, Download, RefreshCw, Trash2, Eye, ShieldCheck, ShieldAlert } from 'lucide-react'
import { ConfirmDialog, Empty, ErrorBanner, InfoTip, Skeleton } from '../components/ui'
import { useLab } from '../hooks/useLab'
import { api, fmt, qs } from '../services/api'
import type { RunRow, S3Object } from '../types/api'

interface Inspection {
  num_rows: number; num_row_groups: number; columns: number; compression: string | null; created_by: string
  schema: { name: string; type: string; nullable: boolean }[]; row_groups: { index: number; rows: number; bytes: number }[]
  sample: Record<string, unknown>[]; bytes_read: number
}
interface Usage {
  runs: number; generated_records: number; s3_objects: number; parquet_bytes: number; estimated_put_requests: number
  estimated_storage_gb: number; estimated_monthly_storage_cost_usd: number; estimated_put_cost_usd: number
  max_storage_mb: number; cap_used_fraction: number; disclaimer: string
}

export function Storage({ initialRun }: { initialRun?: string }) {
  const { health, refreshHealth } = useLab()
  const [runs, setRuns] = useState<RunRow[]>([])
  const [runId, setRunId] = useState(initialRun ?? '')
  const [partition, setPartition] = useState('')
  const [search, setSearch] = useState('')
  const [offset, setOffset] = useState(0)
  const [data, setData] = useState<{ items: S3Object[]; total: number } | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [sel, setSel] = useState<S3Object | null>(null)
  const [insp, setInsp] = useState<Inspection | null>(null)
  const [inspErr, setInspErr] = useState<string | null>(null)
  const [del, setDel] = useState<S3Object | null>(null)
  const [usage, setUsage] = useState<Usage | null>(null)
  const [msg, setMsg] = useState<string | null>(null)
  const limit = 25

  const load = useCallback(async () => {
    setLoading(true); setError(null)
    try {
      setData(await api.get(`/api/storage/objects${qs({ run_id: runId, partition, search, limit, offset })}`))
      setUsage(await api.get('/api/usage'))
    } catch (e) { setError(e instanceof Error ? e.message : 'Failed to load objects') } finally { setLoading(false) }
  }, [runId, partition, search, offset])

  useEffect(() => { api.get<RunRow[]>('/api/runs').then(setRuns).catch(() => undefined) }, [])
  useEffect(() => { const t = setTimeout(load, 200); return () => clearTimeout(t) }, [load])

  const open = async (o: S3Object) => {
    setSel(o); setInsp(null); setInspErr(null)
    try { setInsp((await api.get<{ inspection: Inspection }>(`/api/storage/objects/${o.object_id}`)).inspection) }
    catch (e) { setInspErr(e instanceof Error ? e.message : 'Could not read object from S3') }
  }
  const reconcile = async () => {
    setMsg(null)
    try {
      const r = await api.post<{ s3_objects: number; metadata_objects: number; repaired: string[]; missing_in_s3: string[]; orphans: string[] }>(`/api/storage/reconcile${qs({ run_id: runId })}`)
      setMsg(`S3 lists ${r.s3_objects} objects, metadata lists ${r.metadata_objects}. Repaired ${r.repaired.length}, missing in S3 ${r.missing_in_s3.length}, orphans ${r.orphans.length}.`)
      load()
    } catch (e) { setError(e instanceof Error ? e.message : 'Reconcile failed') }
  }
  const doDelete = async () => {
    if (!del) return
    try { await api.del(`/api/storage/objects/${del.object_id}?confirm=true`); setSel(null); setDel(null); load() }
    catch (e) { setError(e instanceof Error ? e.message : 'Delete failed'); setDel(null) }
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-end gap-3">
        <div className="mr-auto">
          <h2 className="text-xl font-semibold">Storage Explorer</h2>
          <p className="text-ink-dim text-[13px] mt-1">Parquet objects in Amazon S3 <InfoTip term="object_storage" />. The list comes from metadata recorded after each confirmed upload; "Verify with S3" compares it against a live S3 listing.</p>
        </div>
        <div className={`flex items-center gap-2 text-[12px] ${health?.write_verified ? 'text-ok' : 'text-warn'}`}>
          {health?.write_verified ? <ShieldCheck size={15} /> : <ShieldAlert size={15} />}
          <span className="mono">{health?.bucket || 'no bucket'}</span>
          <button className="btn !py-1 !px-2 text-[11px]" onClick={refreshHealth}>Check</button>
        </div>
      </div>
      {usage && (
        <div className="card p-3 grid grid-cols-2 md:grid-cols-5 gap-3 text-[12px]" aria-label="Usage summary">
          <div><div className="label">S3 objects</div><div className="mono text-base">{fmt.int(usage.s3_objects)}</div></div>
          <div><div className="label">Parquet bytes</div><div className="mono text-base">{fmt.bytes(usage.parquet_bytes)}</div><div className="mono text-ink-faint">{(usage.cap_used_fraction * 100).toFixed(1)}% of {usage.max_storage_mb} MB cap</div></div>
          <div><div className="label">Generated records</div><div className="mono text-base">{fmt.int(usage.generated_records)}</div></div>
          <div><div className="label">Est. PUT requests</div><div className="mono text-base">{fmt.int(usage.estimated_put_requests)}</div></div>
          <div><div className="label">Est. cost (estimate)</div><div className="mono text-base">${(usage.estimated_put_cost_usd + usage.estimated_monthly_storage_cost_usd).toFixed(4)}</div></div>
          <div className="col-span-full text-ink-faint">{usage.disclaimer}</div>
        </div>
      )}
      {error && <ErrorBanner message={error} onClose={() => setError(null)} />}
      {msg && <div className="text-[12px] text-teal bg-teal/10 border border-teal/30 rounded-lg px-3 py-2">{msg}</div>}
      <div className="card p-3 flex flex-wrap gap-2 items-center">
        <select aria-label="Filter by run" className="input !w-auto" value={runId} onChange={(e) => { setRunId(e.target.value); setOffset(0) }}>
          <option value="">All runs</option>{runs.map((r) => <option key={r.run_id} value={r.run_id}>{r.run_id} · {r.scenario}</option>)}
        </select>
        <input aria-label="Filter by partition" className="input !w-28" type="number" min={0} placeholder="Partition" value={partition} onChange={(e) => { setPartition(e.target.value); setOffset(0) }} />
        <input aria-label="Search objects" className="input !w-52" placeholder="Search key or chunk id" value={search} onChange={(e) => { setSearch(e.target.value); setOffset(0) }} />
        <button className="btn" onClick={load}><RefreshCw size={14} /> Refresh</button>
        <button className="btn" onClick={reconcile} title="List the bucket prefix and repair or flag inconsistencies"><Cloud size={14} /> Verify with S3</button>
      </div>
      <div className="grid gap-3 xl:grid-cols-[1.4fr_1fr]">
        <div className="card overflow-x-auto">
          {loading && !data ? <div className="p-4 space-y-2">{[...Array(6)].map((_, i) => <Skeleton key={i} />)}</div>
            : !data || data.items.length === 0 ? <Empty icon={<Cloud size={24} />} title="No Parquet objects yet" hint="Run an experiment; every persisted chunk appears here after its S3 upload is confirmed." />
              : (
                <table className="w-full text-[12.5px]">
                  <thead><tr className="text-left label border-b border-line">{['File', 'Run', 'Part.', 'Records', 'Size', 'Codec', 'Uploaded', 'Upload', 'Status'].map((h) => <th key={h} className="px-3 py-2 font-semibold">{h}</th>)}</tr></thead>
                  <tbody>
                    {data.items.map((o) => (
                      <tr key={o.object_id} onClick={() => open(o)} className={`cursor-pointer border-b border-line/60 hover:bg-surface-2 ${sel?.object_id === o.object_id ? 'bg-surface-2' : ''}`}>
                        <td className="px-3 py-1.5 mono text-accent truncate max-w-[200px]" title={o.object_key}>{o.chunk_id.split('-').slice(-2).join('-')}.parquet</td>
                        <td className="px-3 mono text-ink-dim truncate max-w-[110px]" title={o.run_id}>{o.run_id.slice(4, 16)}</td>
                        <td className="px-3 mono">{o.partition_id}</td><td className="px-3 mono">{fmt.int(o.record_count)}</td>
                        <td className="px-3 mono">{fmt.bytes(o.size_bytes)}</td><td className="px-3">{o.compression}</td>
                        <td className="px-3 mono text-ink-dim">{fmt.time(o.uploaded_at)}</td><td className="px-3 mono">{fmt.ms(o.upload_ms)}</td>
                        <td className="px-3"><span className={`text-[11px] px-1.5 py-0.5 rounded ${o.status === 'PERSISTED' ? 'bg-ok/15 text-ok' : 'bg-bad/15 text-bad'}`}>{o.status.toLowerCase()}</span></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
          {data && data.total > limit && (
            <div className="flex items-center gap-2 p-2.5 text-[12px] text-ink-dim border-t border-line">
              <button className="btn !py-1" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - limit))}>Prev</button>
              <span className="mono">{offset + 1}–{Math.min(offset + limit, data.total)} of {fmt.int(data.total)}</span>
              <button className="btn !py-1" disabled={offset + limit >= data.total} onClick={() => setOffset(offset + limit)}>Next</button>
            </div>
          )}
        </div>
        <aside className="card p-4 min-h-[260px]" aria-label="File inspector">
          {!sel ? <Empty icon={<Eye size={22} />} title="Select an object" hint="Schema, row groups, compression and sample rows are read directly from S3 using ranged requests, never a full download." /> : (
            <div className="text-[12.5px] space-y-3">
              <div className="flex items-start gap-2">
                <div className="flex-1 min-w-0"><div className="label">Object key</div><div className="mono break-all text-accent">{sel.object_key}</div></div>
                <a className="btn !py-1" href={`/api/storage/objects/${sel.object_id}/download`}><Download size={13} /> Download</a>
                <button className="btn btn-danger !py-1" onClick={() => setDel(sel)}><Trash2 size={13} /> Delete</button>
              </div>
              <div className="grid grid-cols-2 gap-2">
                <KV k="Bucket" v={sel.bucket} /><KV k="Size" v={fmt.bytes(sel.size_bytes)} /><KV k="Records" v={fmt.int(sel.record_count)} />
                <KV k="Uploaded" v={fmt.datetime(sel.uploaded_at)} /><KV k="Upload time" v={`${fmt.ms(sel.upload_ms)} (${sel.attempts} attempt${sel.attempts > 1 ? 's' : ''})`} /><KV k="Partition" v={String(sel.partition_id)} />
              </div>
              {inspErr && <ErrorBanner message={inspErr} />}
              {!insp && !inspErr && <Skeleton className="h-24" />}
              {insp && (
                <>
                  <div className="grid grid-cols-2 gap-2"><KV k="Codec" v={insp.compression ?? '—'} /><KV k="Row groups" v={String(insp.num_row_groups)} /><KV k="Rows (footer)" v={fmt.int(insp.num_rows)} /><KV k="Bytes read from S3" v={fmt.bytes(insp.bytes_read)} /></div>
                  <div><div className="label mb-1">Schema</div><div className="card p-2 mono text-[11.5px]">{insp.schema.map((c) => <div key={c.name} className="flex justify-between"><span>{c.name}</span><span className="text-ink-faint">{c.type}{c.nullable ? '?' : ''}</span></div>)}</div></div>
                  <div><div className="label mb-1">Sample records</div>
                    <div className="overflow-x-auto"><table className="mono text-[11px] w-full"><thead><tr className="text-ink-faint text-left">{['device_id', 'sensor_type', 'sensor_value', 'quality_flag', 'timestamp'].map((h) => <th key={h} className="pr-3">{h}</th>)}</tr></thead>
                      <tbody>{insp.sample.map((r, i) => <tr key={i}><td className="pr-3">{String(r.device_id)}</td><td className="pr-3">{String(r.sensor_type)}</td><td className="pr-3">{String(r.sensor_value)}</td><td className="pr-3">{String(r.quality_flag)}</td><td>{String(r.timestamp).slice(11, 23)}</td></tr>)}</tbody></table></div></div>
                </>
              )}
            </div>
          )}
        </aside>
      </div>
      {del && <ConfirmDialog title="Delete S3 object?" confirmLabel="Delete from S3" onCancel={() => setDel(null)} onConfirm={doDelete}
        message={<>Permanently delete <span className="mono text-ink break-all">{del.object_key}</span> from Amazon S3? Run history keeps its historical counters.</>} />}
    </div>
  )
}
function KV({ k, v }: { k: string; v: string }) { return <div><div className="label">{k}</div><div className="mono break-all">{v}</div></div> }
