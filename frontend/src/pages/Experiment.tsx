import { Play } from 'lucide-react'
import { ConfigPanel } from '../components/ConfigPanel'
import { RunControls } from '../components/RunControls'
import { ErrorBanner } from '../components/ui'
import { useLab } from '../hooks/useLab'

export function Experiment({ goLive }: { goLive: () => void }) {
  const { presets, presetId, selectPreset, isActive, start, estimate, busy, error, clearError, health } = useLab()
  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-xl font-semibold">Experiment configuration</h2>
        <p className="text-ink-dim text-[13px] mt-1">Pick a scenario, adjust parameters, then start. Presets configure real pipeline behaviour (queue size, processor capacity, chunking, upload settings), not chart appearance.</p>
      </div>
      {error && <ErrorBanner message={error} onClose={clearError} />}
      <div className="grid gap-3 grid-cols-1 md:grid-cols-2 xl:grid-cols-3" role="group" aria-label="Scenario presets">
        {presets.map((p) => (
          <button key={p.id} aria-pressed={p.id === presetId} disabled={isActive} onClick={() => selectPreset(p.id)}
            className={`card text-left p-3.5 transition-colors ${p.id === presetId ? '!border-accent shadow-[0_0_0_1px_#38bdf8]' : 'hover:border-line-strong'} disabled:opacity-60`}>
            <div className="font-semibold text-[14px]">{p.name}</div>
            <div className="text-[12px] text-ink-dim mt-1">{p.description}</div>
            <div className="text-[11px] text-accent mt-2">Expected: {p.expected}</div>
          </button>
        ))}
      </div>
      <ConfigPanel />
      <div className="card p-3 flex items-center gap-3 sticky bottom-3">
        <RunControls />
        <button className="btn btn-primary ml-auto" disabled={isActive || busy || estimate?.valid === false || health?.ready === false}
          onClick={async () => { await start(); goLive() }}><Play size={14} /> Start &amp; watch live</button>
      </div>
    </div>
  )
}
