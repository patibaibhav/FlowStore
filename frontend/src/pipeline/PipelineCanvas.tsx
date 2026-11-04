import { memo, useCallback, useEffect, useMemo, useState } from 'react'
import { Background, Handle, MarkerType, Position, ReactFlow, ReactFlowProvider, useReactFlow, type Edge, type Node, type NodeProps } from '@xyflow/react'
import { Boxes, Cloud, Cpu, FileText, Layers, ListOrdered, Maximize2, X, Zap } from 'lucide-react'
import { NODE_INFO } from '../learning/content'
import { fmt } from '../services/api'
import type { NodeStatus, Snapshot, StorageHealth } from '../types/api'
import { NODE_COLOR, StatusDot } from '../components/ui'

type Key = 'generator' | 'queue' | 'processor' | 'buffers' | 'chunk_builder' | 'parquet_writer' | 's3'
const ORDER: Key[] = ['generator', 'queue', 'processor', 'buffers', 'chunk_builder', 'parquet_writer', 's3']
const ICONS: Record<Key, typeof Zap> = { generator: Zap, queue: ListOrdered, processor: Cpu, buffers: Layers, chunk_builder: Boxes, parquet_writer: FileText, s3: Cloud }
const TITLES: Record<Key, string> = { generator: 'Generator', queue: 'Queue', processor: 'Processor', buffers: 'Buffers', chunk_builder: 'Chunk Builder', parquet_writer: 'Parquet Writer', s3: 'Amazon S3' }

export function nodeRows(key: Key, s: Snapshot, health: StorageHealth | null): [string, string][] {
  const z = (v: number | undefined) => fmt.int(v ?? 0)
  switch (key) {
    case 'generator':
      return [['Configured', `${fmt.int(s.generator?.configured_rate)}/s`], ['Actual', `${fmt.rate(s.generator?.actual_rate)}/s`], ['Generated', z(s.generator?.generated)], ['Status', s.generator?.done ? 'finished' : s.nodes?.generator ?? 'idle']]
    case 'queue':
      return [['Depth', `${z(s.queue?.depth)} / ${z(s.queue?.capacity)}`], ['Utilization', fmt.pct(s.queue?.utilization, 1)], ['Dropped / evicted', `${z(s.queue?.dropped_newest)} / ${z(s.queue?.evicted_oldest)}`], ['Backpressure', s.queue?.blocked_now ? `ACTIVE · ${(s.queue?.blocked_seconds ?? 0).toFixed(1)}s` : `${(s.queue?.blocked_seconds ?? 0).toFixed(1)}s total`]]
    case 'processor':
      return [['Processed', z(s.processor?.processed)], ['Rate', `${fmt.rate(s.processor?.rate)}/s`], ['Rejected', z(s.processor?.rejected)], ['Latency', fmt.ms(s.processor?.latency_ms)]]
    case 'buffers':
      return [['Partitions active', `${s.buffers?.active_partitions ?? 0} / ${s.buffers?.partitions.length ?? 0}`], ['Buffered', z(s.buffers?.records)], ['Bytes', fmt.bytes(s.buffers?.bytes)], ['Utilization', fmt.pct(s.buffers?.utilization, 1)]]
    case 'chunk_builder':
      return [['Fullest / target', `${z(s.chunks?.current_max_fill)} / ${z(s.chunks?.target_size)}`], ['Completed', z(s.chunks?.completed)], ['Pending', z(s.chunks?.pending)], ['Stalled', fmt.dur((s.chunks?.blocked_ms ?? 0) / 1000)]]
    case 'parquet_writer':
      return [['Writing / uploading', `${s.writer?.writing ?? 0} / ${s.writer?.uploading ?? 0}`], ['Pending uploads', `${s.writer?.pending_uploads ?? 0} / ${s.writer?.max_pending ?? 0}`], ['OK / failed', `${z(s.writer?.succeeded)} / ${z(s.writer?.failed)}`], ['Avg upload', fmt.ms(s.writer?.avg_upload_ms)]]
    case 's3':
      return [['Connection', health ? (health.write_verified ? 'verified' : health.ready ? 'read-only' : 'not ready') : '…'], ['Bucket', s.s3?.bucket || health?.bucket || '—'], ['Objects', z(s.s3?.objects)], ['Bytes', fmt.bytes(s.s3?.bytes)], ['Last upload', fmt.time(s.s3?.last_upload_at)]]
  }
}

interface StageData extends Record<string, unknown> { k: Key; status: NodeStatus; rows: [string, string][]; selected: boolean; onSelect: (k: Key) => void }

const StageNode = memo(function StageNode({ data }: NodeProps<Node<StageData>>) {
  const Icon = ICONS[data.k]
  const color = NODE_COLOR[data.status]
  return (
    <div className="nopan rounded-xl border bg-surface-2 w-[172px] px-3 py-2.5 transition-colors cursor-pointer" role="button" tabIndex={0}
      aria-label={`${TITLES[data.k]} node, ${data.status}`}
      onClick={() => data.onSelect(data.k)} onKeyDown={(e) => (e.key === 'Enter' || e.key === ' ') && data.onSelect(data.k)}
      style={{ borderColor: data.selected ? '#38bdf8' : color + '55', boxShadow: `0 0 0 1px ${color}22, 0 10px 30px -14px ${color}66` }}>
      <Handle type="target" position={Position.Left} style={{ background: color, border: 0, width: 8, height: 8 }} />
      <div className="flex items-center gap-2 mb-2">
        <div className="w-7 h-7 rounded-lg grid place-items-center" style={{ background: color + '20', color }}><Icon size={15} /></div>
        <div className="font-semibold text-[13px] flex-1 truncate">{TITLES[data.k]}</div>
        <StatusDot status={data.status} />
      </div>
      <div className="mono text-[10px] uppercase tracking-wider mb-1.5" style={{ color }}>{data.status}</div>
      <div className="space-y-0.5">
        {data.rows.slice(0, 4).map(([l, v]) => (
          <div key={l} className="flex justify-between gap-2 text-[11px]">
            <span className="text-ink-faint truncate">{l}</span>
            <span className="mono text-ink truncate">{v}</span>
          </div>
        ))}
      </div>
      <Handle type="source" position={Position.Right} style={{ background: color, border: 0, width: 8, height: 8 }} />
    </div>
  )
})
const nodeTypes = { stage: StageNode }

function edgeRates(s: Snapshot): number[] {
  const r = s.rates
  if (!r) return Array(6).fill(0)
  return [r.enqueue, r.processing, r.processing, r.chunks * (s.chunks?.target_size ?? 0), r.chunks, r.persist ? r.upload_bytes : 0]
}

function Inner({ snapshot, health }: { snapshot: Snapshot; health: StorageHealth | null }) {
  const [selected, setSelected] = useState<Key | null>(null)
  const rf = useReactFlow()
  const nodeStatus = snapshot.nodes
  const toggle = useCallback((k: Key) => setSelected((cur) => (cur === k ? null : k)), [])
  const active = nodeStatus && snapshot.state !== 'IDLE'

  const nodes = useMemo<Node<StageData>[]>(() => ORDER.map((k, i) => ({
    id: k, type: 'stage', position: { x: i * 222, y: 40 }, draggable: false, selectable: false,
    data: { k, status: (nodeStatus?.[k] ?? 'idle') as NodeStatus, rows: nodeRows(k, snapshot, health), selected: selected === k, onSelect: toggle },
  })), [snapshot, nodeStatus, health, selected, toggle])

  const edges = useMemo<Edge[]>(() => {
    const rates = edgeRates(snapshot)
    const labels = ['events', 'events', 'valid rows', 'chunks', 'files', 'objects']
    return ORDER.slice(0, -1).map((k, i) => {
      const flowing = !!active && (i === 5 ? snapshot.writer?.uploading : rates[i] > 0.5) && snapshot.state !== 'PAUSED'
      const blocked = nodeStatus?.[ORDER[i + 1]] === 'backpressured' || nodeStatus?.[k] === 'backpressured'
      const color = blocked ? '#fbbf24' : flowing ? '#38bdf8' : '#2b3646'
      // animation speed grows with measured throughput (log scale, 0.4s..2.4s per cycle)
      const dur = Math.max(0.4, 2.4 - Math.log10(1 + rates[i]) * 0.55)
      const label = i === 0 ? `${fmt.rate(rates[0])}/s` : i === 1 ? `${fmt.rate(rates[1])}/s` : i === 2 ? `${fmt.rate(Math.max(0, (snapshot.rates?.processing ?? 0) - (snapshot.rates?.rejected ?? 0)))}/s` : i === 3 ? `${fmt.rate(snapshot.rates?.chunks)} chunk/s` : i === 4 ? `${snapshot.writer?.pending_uploads ?? 0} pending` : `${fmt.bytes(snapshot.rates?.upload_bytes)}/s`
      return {
        id: `${k}-${ORDER[i + 1]}`, source: k, target: ORDER[i + 1], type: 'default',
        className: flowing ? 'flow-edge-active' : '',
        style: { stroke: color, animationDuration: `${dur}s` },
        markerEnd: { type: MarkerType.ArrowClosed, color },
        label: active ? label : labels[i], labelStyle: { fill: '#9aa7b6', fontSize: 10, fontFamily: 'var(--font-mono)' },
        labelBgStyle: { fill: '#0d1117' }, labelBgPadding: [4, 2] as [number, number],
      }
    })
  }, [snapshot, active, nodeStatus])

  const fit = useCallback(() => rf.fitView({ padding: 0.06, duration: 300 }), [rf])
  useEffect(() => { const t = setTimeout(() => rf.fitView({ padding: 0.06 }), 60); return () => clearTimeout(t) }, [rf])
  useEffect(() => { window.addEventListener('resize', fit); return () => window.removeEventListener('resize', fit) }, [fit])

  const info = selected ? NODE_INFO[selected] : null
  return (
    <div className="relative h-full">
      <ReactFlow nodes={nodes} edges={edges} nodeTypes={nodeTypes} fitView fitViewOptions={{ padding: 0.06 }}
        minZoom={0.3} maxZoom={1.4} nodesDraggable={false} nodesConnectable={false} elementsSelectable={false}
        zoomOnScroll={false} panOnDrag preventScrolling={false} proOptions={{ hideAttribution: true }}>
        <Background color="#1a2230" gap={22} size={1} />
      </ReactFlow>
      <button className="btn absolute top-3 right-3 !py-1 !px-2.5 text-[12px]" onClick={fit}><Maximize2 size={13} /> Fit view</button>
      {selected && info && (
        <aside aria-label={`${info.title} inspector`} className="absolute top-0 right-0 h-full w-[340px] max-w-full overflow-y-auto border-l border-line-strong bg-surface/95 backdrop-blur p-4 text-[12.5px]">
          <div className="flex items-center gap-2 mb-2">
            <StatusDot status={(nodeStatus?.[selected] ?? 'idle') as NodeStatus} />
            <h3 className="font-semibold text-[15px] flex-1">{info.title}</h3>
            <button aria-label="Close inspector" onClick={() => setSelected(null)}><X size={16} /></button>
          </div>
          <p className="text-ink-dim leading-relaxed mb-3">{info.does}</p>
          <div className="label mb-1">Live metrics · {nodeStatus?.[selected] ?? 'idle'}</div>
          <div className="card p-2.5 mb-3 space-y-1">
            {nodeRows(selected, snapshot, health).map(([l, v]) => (
              <div key={l} className="flex justify-between"><span className="text-ink-faint">{l}</span><span className="mono">{v}</span></div>
            ))}
          </div>
          {selected === 'buffers' && snapshot.buffers && (
            <div className="mb-3">
              <div className="label mb-1">Per-partition buffers</div>
              {snapshot.buffers.partitions.map((p) => (
                <div key={p.partition} className="flex items-center gap-2 mb-1">
                  <span className="mono w-6 text-ink-faint">P{p.partition}</span>
                  <div className="flex-1 h-1.5 rounded bg-surface-3 overflow-hidden"><div className="h-full bg-accent" style={{ width: `${Math.min(100, (p.records / (snapshot.chunks?.target_size || 1)) * 100)}%` }} /></div>
                  <span className="mono w-14 text-right">{fmt.int(p.records)}</span>
                </div>
              ))}
            </div>
          )}
          {selected === 'processor' && (snapshot.processor?.top_rejections?.length ?? 0) > 0 && (
            <div className="mb-3">
              <div className="label mb-1">Top rejection reasons</div>
              {snapshot.processor!.top_rejections.map(([r, n]) => <div key={r} className="flex justify-between"><span className="text-ink-dim truncate pr-2">{r}</span><span className="mono">{fmt.int(n)}</span></div>)}
            </div>
          )}
          <Section title="Inputs → Outputs"><div className="text-ink-dim">{info.inputs}<br /><span className="text-accent">→</span> {info.outputs}</div></Section>
          <Section title="Configuration"><div className="flex flex-wrap gap-1">{info.config.map((c) => <span key={c} className="mono text-[11px] px-1.5 py-0.5 rounded bg-surface-3 text-ink-dim">{c}</span>)}</div></Section>
          <Section title="Common failure modes"><ul className="list-disc pl-4 text-ink-dim space-y-0.5">{info.failures.map((f) => <li key={f}>{f}</li>)}</ul></Section>
          <Section title="System design concepts"><div className="text-ink-dim">{info.concepts}</div></Section>
        </aside>
      )}
    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return <div className="mb-3"><div className="label mb-1">{title}</div>{children}</div>
}

export function PipelineCanvas(props: { snapshot: Snapshot; health: StorageHealth | null }) {
  return <ReactFlowProvider><Inner {...props} /></ReactFlowProvider>
}
