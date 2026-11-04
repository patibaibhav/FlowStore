export class ApiRequestError extends Error {
  status: number
  code?: string
  body: unknown
  constructor(status: number, message: string, code?: string, body?: unknown) {
    super(message)
    this.status = status
    this.code = code
    this.body = body
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let res: Response
  try {
    res = await fetch(path, {
      ...init,
      headers: { 'Content-Type': 'application/json', ...(init?.headers ?? {}) },
    })
  } catch {
    throw new ApiRequestError(0, 'Cannot reach the FlowStore backend', 'NETWORK')
  }
  const text = await res.text()
  let data: unknown = null
  try { data = text ? JSON.parse(text) : null } catch { data = text }
  if (!res.ok) {
    const d = data as { message?: string; detail?: unknown; code?: string } | null
    let message = d?.message ?? ''
    if (!message && typeof d?.detail === 'string') message = d.detail
    if (!message && Array.isArray(d?.detail)) {
      message = (d.detail as { loc?: string[]; msg: string }[]).map((e) => `${e.loc?.slice(1).join('.')}: ${e.msg}`).join('; ')
    }
    if (!message && d?.detail && typeof d.detail === 'object') message = (d.detail as { message?: string }).message ?? ''
    throw new ApiRequestError(res.status, message || `Request failed (${res.status})`, d?.code, data)
  }
  return data as T
}

export const api = {
  get: <T>(p: string) => request<T>(p),
  post: <T>(p: string, body?: unknown) => request<T>(p, { method: 'POST', body: body === undefined ? undefined : JSON.stringify(body) }),
  del: <T>(p: string) => request<T>(p, { method: 'DELETE' }),
}

export function qs(params: Record<string, string | number | boolean | undefined | null>): string {
  const u = new URLSearchParams()
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== null && v !== '') u.set(k, String(v))
  const s = u.toString()
  return s ? `?${s}` : ''
}

export const fmt = {
  int: (n: number | undefined | null) => (n ?? 0).toLocaleString('en-US'),
  rate: (n: number | undefined | null) => ((n ?? 0) >= 100 ? Math.round(n ?? 0).toLocaleString('en-US') : (n ?? 0).toFixed(1)),
  pct: (x: number | undefined | null, d = 0) => `${((x ?? 0) * 100).toFixed(d)}%`,
  ms: (n: number | undefined | null) => `${(n ?? 0).toFixed((n ?? 0) < 10 ? 1 : 0)} ms`,
  bytes: (n: number | undefined | null) => {
    const v = n ?? 0
    if (v < 1024) return `${v} B`
    if (v < 1024 ** 2) return `${(v / 1024).toFixed(1)} KB`
    if (v < 1024 ** 3) return `${(v / 1024 ** 2).toFixed(2)} MB`
    return `${(v / 1024 ** 3).toFixed(2)} GB`
  },
  time: (ts?: number | null) => (ts ? new Date(ts * 1000).toLocaleTimeString('en-GB') : '—'),
  datetime: (ts?: number | null) => (ts ? new Date(ts * 1000).toLocaleString('en-GB') : '—'),
  dur: (s?: number | null) => (s == null ? '—' : s < 60 ? `${s.toFixed(1)}s` : `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`),
}
