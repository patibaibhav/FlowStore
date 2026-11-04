import { useState } from 'react'
import { Pause, Play, RotateCcw, Square } from 'lucide-react'
import { useLab } from '../hooks/useLab'
import { ConfirmDialog } from './ui'

export function RunControls() {
  const { snapshot, isActive, presets, presetId, selectPreset, resetConfig, start, pause, resume, stop, busy, estimate, health } = useLab()
  const [confirmForce, setConfirmForce] = useState(false)
  const st = snapshot.state
  const canStart = !isActive && !busy && estimate?.valid !== false && health?.ready !== false
  return (
    <div className="flex flex-wrap items-center gap-2">
      <label className="sr-only" htmlFor="preset-select">Preset</label>
      <select id="preset-select" className="input !w-auto min-w-[210px]" value={presetId} disabled={isActive} onChange={(e) => selectPreset(e.target.value)}>
        {presets.map((p) => <option key={p.id} value={p.id}>{p.name}</option>)}
      </select>
      <button className="btn btn-primary" disabled={!canStart} onClick={start} title={health?.ready === false ? 'Amazon S3 is not ready' : undefined}><Play size={14} /> Start</button>
      <button className="btn" disabled={st !== 'RUNNING' || busy} onClick={pause}><Pause size={14} /> Pause</button>
      <button className="btn" disabled={!['PAUSED', 'PAUSING'].includes(st) || busy} onClick={resume}><Play size={14} /> Resume</button>
      <button className="btn" disabled={!['RUNNING', 'PAUSING', 'PAUSED', 'STARTING'].includes(st) || busy} onClick={() => stop(false)} title="Graceful stop: drain queue, flush buffers, finish uploads"><Square size={14} /> Stop</button>
      <button className="btn btn-danger" disabled={!isActive || busy} onClick={() => setConfirmForce(true)} title="Discard unpersisted records and cancel immediately">Force cancel</button>
      <button className="btn" disabled={isActive} onClick={resetConfig}><RotateCcw size={14} /> Reset</button>
      {confirmForce && (
        <ConfirmDialog title="Force cancel run?" confirmLabel="Force cancel" onCancel={() => setConfirmForce(false)}
          onConfirm={() => { setConfirmForce(false); stop(true) }}
          message="Accepted records that have not yet been persisted to Amazon S3 will be discarded. The run is recorded as CANCELLED with the number of unpersisted records. Use Stop for a graceful drain instead." />
      )}
    </div>
  )
}
