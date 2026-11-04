import { useEffect, useState } from 'react'
import { Activity, BookOpen, Cloud, GitCompare, HelpCircle, History as HistoryIcon, SlidersHorizontal, Wifi, WifiOff } from 'lucide-react'
import { StateBadge } from './components/ui'
import { LabProvider, useLab } from './hooks/useLab'
import { Compare } from './pages/Compare'
import { Experiment } from './pages/Experiment'
import { History } from './pages/History'
import { Learning } from './pages/Learning'
import { LiveLab } from './pages/LiveLab'
import { Storage } from './pages/Storage'

const TABS = [
  { id: 'live', label: 'Live Lab', icon: Activity },
  { id: 'experiment', label: 'Experiment', icon: SlidersHorizontal },
  { id: 'storage', label: 'Storage Explorer', icon: Cloud },
  { id: 'history', label: 'Run History', icon: HistoryIcon },
  { id: 'compare', label: 'Compare', icon: GitCompare },
  { id: 'learn', label: 'Learning Center', icon: BookOpen },
] as const
type Tab = (typeof TABS)[number]['id']

function readHash(): Tab {
  const h = location.hash.replace('#', '') as Tab
  return TABS.some((t) => t.id === h) ? h : 'live'
}

function Shell() {
  const { snapshot, connection, health } = useLab()
  const [tab, setTab] = useState<Tab>(readHash)
  const [storageRun, setStorageRun] = useState('')
  useEffect(() => { const h = () => setTab(readHash()); window.addEventListener('hashchange', h); return () => window.removeEventListener('hashchange', h) }, [])
  const go = (t: Tab) => { location.hash = t; setTab(t) }
  const s3 = health ? (health.write_verified ? ['ok', 'S3 verified'] : health.ready ? ['warn', 'S3 read-only'] : ['bad', 'S3 not ready']) : ['dim', 'S3 …']
  const s3color = { ok: 'text-ok', warn: 'text-warn', bad: 'text-bad', dim: 'text-ink-faint' }[s3[0]]
  return (
    <div className="min-h-full flex flex-col">
      <header className="sticky top-0 z-40 border-b border-line bg-bg/85 backdrop-blur">
        <div className="max-w-[1900px] mx-auto px-4 h-12 flex items-center gap-4">
          <div className="font-bold tracking-wide text-[15px]">FLOW<span className="text-accent">STORE</span> <span className="mono text-[10px] text-ink-faint border border-line-strong rounded px-1 ml-1">2.0</span></div>
          <div className="hidden md:block text-[12px] text-ink-faint truncate">{snapshot.scenario ? `Experiment: ${snapshot.scenario}` : 'No experiment running'}</div>
          <div className="ml-auto flex items-center gap-4 text-[12px]">
            <StateBadge state={snapshot.state} />
            <span className={`flex items-center gap-1 ${s3color}`} title={health?.checks.map((c) => `${c.name}: ${c.detail}`).join('\n')}><Cloud size={13} />{s3[1]}</span>
            <span className={`flex items-center gap-1 ${connection === 'open' ? 'text-ok' : 'text-warn'}`}>{connection === 'open' ? <Wifi size={13} /> : <WifiOff size={13} />}{connection === 'open' ? 'Live' : connection === 'connecting' ? 'Connecting' : 'Reconnecting'}</span>
            <button className="text-ink-faint hover:text-ink" aria-label="Help" onClick={() => go('learn')}><HelpCircle size={16} /></button>
          </div>
        </div>
        <nav className="max-w-[1900px] mx-auto px-3 flex gap-1 overflow-x-auto" aria-label="Main">
          {TABS.map(({ id, label, icon: Icon }) => (
            <button key={id} onClick={() => go(id)} aria-current={tab === id ? 'page' : undefined}
              className={`flex items-center gap-1.5 px-3 py-2 text-[13px] font-semibold border-b-2 -mb-px whitespace-nowrap transition-colors ${tab === id ? 'border-accent text-ink' : 'border-transparent text-ink-faint hover:text-ink-dim'}`}>
              <Icon size={14} />{label}
            </button>
          ))}
        </nav>
      </header>
      <main className="flex-1 max-w-[1900px] w-full mx-auto px-4 py-4">
        {tab === 'live' && <LiveLab />}
        {tab === 'experiment' && <Experiment goLive={() => go('live')} />}
        {tab === 'storage' && <Storage key={storageRun} initialRun={storageRun} />}
        {tab === 'history' && <History openStorage={(id) => { setStorageRun(id); go('storage') }} />}
        {tab === 'compare' && <Compare />}
        {tab === 'learn' && <Learning goLive={() => go('live')} />}
      </main>
    </div>
  )
}

export default function App() {
  return <LabProvider><Shell /></LabProvider>
}
