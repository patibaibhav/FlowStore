import { useEffect, useRef, useState, type ReactNode } from 'react'
import { Info, X } from 'lucide-react'
import { GLOSSARY } from '../learning/content'
import type { NodeStatus, RunState } from '../types/api'

export function InfoTip({ term, text }: { term?: string; text?: string }) {
  const [open, setOpen] = useState(false)
  const g = term ? GLOSSARY[term] : undefined
  const body = text ?? g?.body
  if (!body) return null
  return (
    <span className="relative inline-flex align-middle" onMouseEnter={() => setOpen(true)} onMouseLeave={() => setOpen(false)}>
      <button type="button" aria-label={`About ${g?.title ?? 'this'}`} className="text-ink-faint hover:text-accent ml-1 cursor-help" onFocus={() => setOpen(true)} onBlur={() => setOpen(false)}>
        <Info size={13} />
      </button>
      {open && (
        <span role="tooltip" className="absolute z-50 left-1/2 -translate-x-1/2 top-full mt-1.5 w-64 p-3 rounded-lg border border-line-strong bg-surface-2 text-[12px] leading-relaxed text-ink-dim shadow-xl normal-case tracking-normal font-normal">
          {g && <span className="block text-ink font-semibold mb-1">{g.title}</span>}
          {body}
        </span>
      )}
    </span>
  )
}

const STATE_STYLE: Record<string, string> = {
  IDLE: 'bg-surface-3 text-ink-dim', STARTING: 'bg-accent/15 text-accent', RUNNING: 'bg-ok/15 text-ok',
  PAUSING: 'bg-warn/15 text-warn', PAUSED: 'bg-warn/15 text-warn', STOPPING: 'bg-warn/15 text-warn',
  FLUSHING: 'bg-violet/15 text-violet', COMPLETED: 'bg-teal/15 text-teal', FAILED: 'bg-bad/15 text-bad',
  CANCELLED: 'bg-bad/10 text-bad', INTERRUPTED: 'bg-warn/15 text-warn',
}
export function StateBadge({ state }: { state: RunState | string }) {
  return <span className={`mono text-[11px] font-semibold px-2 py-0.5 rounded-md ${STATE_STYLE[state] ?? STATE_STYLE.IDLE}`}>{state}</span>
}

export const NODE_COLOR: Record<NodeStatus, string> = {
  idle: '#667384', running: '#34d399', waiting: '#9aa7b6', backpressured: '#fbbf24', flushing: '#a78bfa',
  uploading: '#38bdf8', warning: '#fbbf24', error: '#f87171', completed: '#2dd4bf',
}
export function StatusDot({ status }: { status: NodeStatus }) {
  const c = NODE_COLOR[status]
  const live = ['running', 'uploading', 'flushing', 'backpressured'].includes(status)
  return <span className={`inline-block w-2 h-2 rounded-full ${live ? 'pulse' : ''}`} style={{ background: c, ['--ring' as string]: c + '66' }} />
}

export function Stat({ label, value, sub, tip, tone }: { label: string; value: ReactNode; sub?: ReactNode; tip?: string; tone?: 'warn' | 'bad' | 'ok' }) {
  const color = tone === 'bad' ? 'text-bad' : tone === 'warn' ? 'text-warn' : tone === 'ok' ? 'text-ok' : 'text-ink'
  return (
    <div className="card px-3.5 py-2.5 min-w-0">
      <div className="label truncate">{label}{tip && <InfoTip term={tip} />}</div>
      <div className={`mono text-[20px] font-semibold leading-tight mt-0.5 ${color}`}>{value}</div>
      {sub && <div className="mono text-[11px] text-ink-faint mt-0.5 truncate">{sub}</div>}
    </div>
  )
}

export function Skeleton({ className = 'h-8 w-full' }: { className?: string }) {
  return <div className={`skeleton ${className}`} />
}

export function Empty({ icon, title, hint }: { icon?: ReactNode; title: string; hint?: string }) {
  return (
    <div className="flex flex-col items-center justify-center text-center py-10 px-4 text-ink-faint">
      {icon}
      <div className="text-ink-dim font-semibold mt-2">{title}</div>
      {hint && <div className="text-[12px] mt-1 max-w-sm">{hint}</div>}
    </div>
  )
}

export function ErrorBanner({ message, onClose }: { message: string; onClose?: () => void }) {
  return (
    <div role="alert" className="flex items-start gap-3 rounded-lg border border-bad/40 bg-bad/10 text-[13px] text-red-200 px-3.5 py-2.5">
      <span className="flex-1">{message}</span>
      {onClose && <button aria-label="Dismiss" onClick={onClose}><X size={14} /></button>}
    </div>
  )
}

export function Modal({ title, children, onClose }: { title: string; children: ReactNode; onClose: () => void }) {
  const ref = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const h = (e: KeyboardEvent) => e.key === 'Escape' && onClose()
    window.addEventListener('keydown', h)
    return () => window.removeEventListener('keydown', h)
  }, [onClose])
  return (
    <div className="fixed inset-0 z-[100] bg-black/60 backdrop-blur-sm flex items-center justify-center p-4" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <div ref={ref} role="dialog" aria-label={title} className="card w-full max-w-lg p-5">
        <div className="flex items-center mb-3"><h3 className="font-semibold text-base flex-1">{title}</h3><button aria-label="Close" onClick={onClose}><X size={16} /></button></div>
        {children}
      </div>
    </div>
  )
}

export function ConfirmDialog({ title, message, confirmLabel, onConfirm, onCancel }: { title: string; message: ReactNode; confirmLabel: string; onConfirm: () => void; onCancel: () => void }) {
  return (
    <Modal title={title} onClose={onCancel}>
      <div className="text-ink-dim text-[13px] mb-4">{message}</div>
      <div className="flex justify-end gap-2">
        <button className="btn" onClick={onCancel}>Cancel</button>
        <button className="btn btn-danger" onClick={onConfirm}>{confirmLabel}</button>
      </div>
    </Modal>
  )
}
