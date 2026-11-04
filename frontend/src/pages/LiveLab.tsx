import { ChartGrid } from '../charts/Charts'
import { BottleneckCard, EventLog, MetricsStrip } from '../components/LiveWidgets'
import { RunControls } from '../components/RunControls'
import { ErrorBanner, Skeleton } from '../components/ui'
import { useLab } from '../hooks/useLab'
import { PipelineCanvas } from '../pipeline/PipelineCanvas'

export function LiveLab() {
  const { snapshot, history, events, health, error, clearError, config, presets, presetId } = useLab()
  const preset = presets.find((p) => p.id === presetId)
  const failing = health?.checks.find((c) => !c.ok)
  return (
    <div className="space-y-3">
      <div className="card p-3 flex flex-wrap items-center gap-3">
        <RunControls />
        <div className="ml-auto text-[12px] text-ink-faint max-w-xl hidden lg:block">{preset?.expected}</div>
      </div>
      {error && <ErrorBanner message={error} onClose={clearError} />}
      {failing && <ErrorBanner message={`Amazon S3 is not ready — ${failing.detail}${failing.hint ? ` (${failing.hint})` : ''}`} />}
      {snapshot.note && <div className="text-[12px] text-warn bg-warn/10 border border-warn/30 rounded-lg px-3 py-2">{snapshot.note}</div>}
      <MetricsStrip s={snapshot} />
      <section className="card overflow-hidden relative" style={{ height: 'clamp(250px, 34vh, 340px)' }} aria-label="Pipeline">
        <div className="absolute top-3 left-4 z-10 label">Live pipeline · {snapshot.run_id ?? 'no active run'}</div>
        {config ? <PipelineCanvas snapshot={snapshot} health={health} /> : <Skeleton className="h-full w-full" />}
      </section>
      <ChartGrid data={history} extra={<BottleneckCard s={snapshot} />} />
      <EventLog events={events} />
    </div>
  )
}
