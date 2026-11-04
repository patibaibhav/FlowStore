import '@testing-library/jest-dom/vitest'

class RO {
  observe() {}
  unobserve() {}
  disconnect() {}
}
// jsdom lacks these APIs used by React Flow / Recharts
;(globalThis as unknown as { ResizeObserver: typeof RO }).ResizeObserver = RO
;(globalThis as unknown as { DOMMatrixReadOnly: unknown }).DOMMatrixReadOnly = class {
  m22 = 1
  constructor() {}
}
