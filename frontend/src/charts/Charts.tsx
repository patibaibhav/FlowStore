import { memo, type ReactNode } from 'react'
import { Area, AreaChart, CartesianGrid, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { Empty, InfoTip } from '../components/ui'
import type { MetricSample } from '../types/api'
import { TrendingUp } from 'lucide-react'

interface Series { key: keyof MetricSample | string; name: string; color: string; area?: boolean; dash?: boolean }

const axis = { stroke: '#667384', fontSize: 11, tickLine: false, axisLine: { stroke: '#1f2937' } } as const

export const MiniChart = memo(function MiniChart({ title, tip, unit, data, series, capacityKey, height = 168, kind = 'line' }: {
  title: string; tip?: string; unit?: string; data: MetricSample[]; series: Series[]; capacityKey?: string; height?: number; kind?: 'line' | 'area'
}) {
  const rows = data.map((d) => ({ ...d, x: Math.round(d.elapsed * 10) / 10 }))
  const cap = capacityKey && data.length ? (data[data.length - 1] as unknown as Record<string, number>)[capacityKey] : undefined
  const Chart = kind === 'area' ? AreaChart : LineChart
  return (
    <section className="card p-3 min-w-0" aria-label={title}>
      <div className="flex items-baseline justify-between mb-1">
        <h3 className="label">{title}{tip && <InfoTip term={tip} />}</h3>
        {unit && <span className="mono text-[10px] text-ink-faint">{unit}</span>}
      </div>
      {rows.length < 2 ? (
        <div style={{ height }}><Empty icon={<TrendingUp size={20} />} title="No data yet" hint="Start a run to see live measurements." /></div>
      ) : (
        <div style={{ height }}>
          <ResponsiveContainer width="100%" height="100%">
            <Chart data={rows} margin={{ top: 6, right: 8, left: -14, bottom: 0 }}>
              <CartesianGrid stroke="#161e2a" vertical={false} />
              <XAxis dataKey="x" {...axis} tickFormatter={(v) => `${v}s`} minTickGap={28} />
              <YAxis {...axis} width={44} tickFormatter={(v) => (v >= 1000 ? `${(v / 1000).toFixed(v >= 10000 ? 0 : 1)}k` : String(Math.round(v * 10) / 10))} />
              <Tooltip contentStyle={{ background: '#121821', border: '1px solid #2b3646', borderRadius: 8, fontSize: 12 }} labelFormatter={(v) => `t = ${v}s`}
                formatter={(v) => (typeof v === 'number' ? (Math.round(v * 100) / 100).toLocaleString() : String(v ?? ''))} />
              <Legend iconType="plainline" wrapperStyle={{ fontSize: 11, paddingTop: 2 }} />
              {cap ? <ReferenceLine y={cap} stroke="#f87171" strokeDasharray="4 4" label={{ value: 'capacity', fill: '#f87171', fontSize: 10, position: 'insideTopRight' }} /> : null}
              {series.map((s) => kind === 'area' ? (
                <Area key={s.key} type="monotone" dataKey={s.key} name={s.name} stroke={s.color} fill={s.color} fillOpacity={0.14} strokeWidth={1.8} dot={false} isAnimationActive={false} />
              ) : (
                <Line key={s.key} type="monotone" dataKey={s.key} name={s.name} stroke={s.color} strokeWidth={1.8} dot={false} strokeDasharray={s.dash ? '4 3' : undefined} isAnimationActive={false} />
              ))}
            </Chart>
          </ResponsiveContainer>
        </div>
      )}
    </section>
  )
})

export function ChartGrid({ data, extra, height }: { data: MetricSample[]; extra?: ReactNode; height?: number }) {
  return (
    <div className="grid gap-3 grid-cols-1 md:grid-cols-2 2xl:grid-cols-4 xl:grid-cols-3">
      <MiniChart height={height} title="Ingestion vs processing" tip="consumer_throughput" unit="records / s" data={data} series={[
        { key: 'gen_rate', name: 'Generated', color: '#38bdf8' }, { key: 'proc_rate', name: 'Processed', color: '#34d399' }, { key: 'persist_rate', name: 'Persisted', color: '#a78bfa', dash: true }]} />
      <MiniChart height={height} title="Queue depth" tip="bounded_queue" unit="records" kind="area" capacityKey="queue_capacity" data={data} series={[{ key: 'queue_depth', name: 'Depth', color: '#fbbf24' }]} />
      <MiniChart height={height} title="Buffer occupancy" tip="buffering" unit="records" kind="area" data={data} series={[{ key: 'buffered', name: 'Buffered', color: '#2dd4bf' }]} />
      <MiniChart height={height} title="Chunk creation" unit="chunks / s · pending" data={data} series={[{ key: 'chunk_rate', name: 'Chunks/s', color: '#38bdf8' }, { key: 'pending_uploads', name: 'Pending uploads', color: '#f87171', dash: true }]} />
      <MiniChart height={height} title="S3 upload latency" tip="persistence_latency" unit="ms" data={data} series={[{ key: 'upload_ms', name: 'Last upload', color: '#a78bfa' }]} />
      <MiniChart height={height} title="Cumulative persisted" unit="records" kind="area" data={data} series={[{ key: 'persisted', name: 'Persisted', color: '#34d399' }]} />
      <MiniChart height={height} title="Rejections & drops" unit="records / s" data={data} series={[{ key: 'reject_rate', name: 'Rejected', color: '#f87171' }, { key: 'drop_rate', name: 'Dropped', color: '#fbbf24' }]} />
      {extra}
    </div>
  )
}
