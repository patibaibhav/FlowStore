import { useState } from 'react'
import { BookOpen, FlaskConical } from 'lucide-react'
import { EXPERIMENTS, GLOSSARY, METRICS, NODE_INFO } from '../learning/content'
import { useLab } from '../hooks/useLab'

const TABS = ['Pipeline', 'Guided experiments', 'Metrics', 'Glossary', 'Architecture'] as const

export function Learning({ goLive }: { goLive: () => void }) {
  const [tab, setTab] = useState<(typeof TABS)[number]>('Guided experiments')
  const { selectPreset, isActive } = useLab()
  return (
    <div className="space-y-4">
      <div><h2 className="text-xl font-semibold flex items-center gap-2"><BookOpen size={20} /> Learning center</h2><p className="text-ink-dim text-[13px] mt-1">Concepts, guided experiments and metric definitions for the time-series storage &amp; ingestion pipeline.</p></div>
      <div role="tablist" className="flex gap-1 border-b border-line">
        {TABS.map((t) => <button key={t} role="tab" aria-selected={tab === t} onClick={() => setTab(t)} className={`px-3 py-2 text-[13px] font-semibold border-b-2 -mb-px ${tab === t ? 'border-accent text-ink' : 'border-transparent text-ink-faint hover:text-ink-dim'}`}>{t}</button>)}
      </div>
      {tab === 'Pipeline' && (
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          {Object.entries(NODE_INFO).map(([k, n], i) => (
            <article key={k} className="card p-4 text-[13px]">
              <div className="mono text-[11px] text-accent">STAGE {i + 1}</div><h3 className="font-semibold text-base mb-1">{n.title}</h3>
              <p className="text-ink-dim">{n.does}</p>
              <p className="mt-2 text-ink-faint text-[12px]"><b className="text-ink-dim">Concepts:</b> {n.concepts}</p>
            </article>
          ))}
        </div>
      )}
      {tab === 'Guided experiments' && (
        <div className="space-y-4">
          {EXPERIMENTS.map((e) => (
            <article key={e.id} className="card p-5 text-[13px]">
              <div className="flex items-center gap-3 mb-2"><FlaskConical size={18} className="text-accent" /><h3 className="text-base font-semibold flex-1">Experiment {e.id} — {e.title}</h3>
                <button className="btn" disabled={isActive} onClick={() => { selectPreset(e.preset); goLive() }}>Load preset &amp; open Live Lab</button></div>
              <p className="text-ink-dim"><b className="text-ink">Objective.</b> {e.objective}</p>
              <div className="grid md:grid-cols-2 gap-4 mt-3">
                <div><div className="label mb-1">Steps</div><ol className="list-decimal pl-5 space-y-1 text-ink-dim">{e.steps.map((s) => <li key={s}>{s}</li>)}</ol></div>
                <div><div className="label mb-1">What to observe</div><ul className="list-disc pl-5 space-y-1 text-ink-dim">{e.observe.map((s) => <li key={s}>{s}</li>)}</ul></div>
              </div>
              <p className="mt-3 text-ink-dim"><b className="text-ink">Explanation.</b> {e.explanation}</p>
              <p className="mt-2 text-violet"><b>Reflect:</b> {e.reflection}</p>
            </article>
          ))}
        </div>
      )}
      {tab === 'Metrics' && (
        <div className="card divide-y divide-line">{METRICS.map((m) => <div key={m.name} className="px-4 py-3 grid md:grid-cols-[200px_1fr] gap-2 text-[13px]"><div className="font-semibold">{m.name}</div><div className="text-ink-dim">{m.definition}</div></div>)}</div>
      )}
      {tab === 'Glossary' && (
        <div className="grid gap-3 md:grid-cols-2">{Object.entries(GLOSSARY).map(([k, g]) => <article key={k} className="card p-4"><h3 className="font-semibold">{g.title}</h3><p className="text-ink-dim text-[13px] mt-1">{g.body}</p></article>)}</div>
      )}
      {tab === 'Architecture' && (
        <article className="card p-5 text-[13px] text-ink-dim space-y-3 leading-relaxed max-w-4xl">
          <p><b className="text-ink">Data flow.</b> Generator → bounded Queue → Processor (validation) → partition Buffers → Chunk Builder → Parquet Writer → Amazon S3. Each stage runs in its own thread; the only shared structures are the bounded queue and a bounded upload queue.</p>
          <p><b className="text-ink">Backpressure chain.</b> A slow S3 fills the upload slots, which stalls the chunk builder and then the processor, which lets the queue fill, which blocks (or drops for) the generator. Memory and temp disk therefore stay bounded.</p>
          <p><b className="text-ink">Durability rule.</b> A chunk becomes PERSISTED only after its S3 PUT succeeded and the object metadata was committed in SQLite in one transaction. Failed uploads count as failed, never as persisted. There is no silent fallback to local storage.</p>
          <p><b className="text-ink">Pause semantics.</b> Pause halts the generator and processor at batch boundaries. Chunks already sealed and uploads in flight continue to completion; buffers age-flush as usual.</p>
          <p><b className="text-ink">Stop semantics.</b> Stop drains the queue, flushes all buffers as partial chunks and finishes uploads (graceful). Force cancel discards unpersisted records and records exactly how many.</p>
        </article>
      )}
    </div>
  )
}
