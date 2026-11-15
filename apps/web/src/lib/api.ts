import type {
  DatasetLayoutOut, DatasetOut, DatasetProfileOut, DatasetVersionOut,
  FeatureOut, FeatureSnapshotOut, IdentityOut, JobOut, MaterializationOut,
  MetricsOut, PoolOut, QueryRowsOut, ReleaseOut, ResourceMetricsOut,
  SourceIn, SourceOut, StorageSummaryOut, WorkflowOut, WorkloadLaneOut,
} from './generated/types.gen'

export type Source = SourceOut
export type SourceKind = SourceIn['kind']
export type Dataset = DatasetOut
export type Feature = FeatureOut
export type Job = JobOut
export type Workflow = WorkflowOut
export type WorkloadLane = WorkloadLaneOut
export type Pool = PoolOut
export type ResourceMetrics = ResourceMetricsOut
export type Metrics = MetricsOut
export type DatasetVersion = DatasetVersionOut
export type DatasetProfile = DatasetProfileOut
export type FeatureSnapshot = FeatureSnapshotOut
export type Release = ReleaseOut
export type StorageSummary = StorageSummaryOut
export type Materialization = MaterializationOut
export type DatasetLayout = DatasetLayoutOut
export type QueryResultPage = QueryRowsOut
export type Role = IdentityOut['role']
export type PlatformEvent = { id: number; kind: string; subject_id: string; data: Record<string, unknown> }

export async function api<T>(path: string, options: RequestInit = {}): Promise<T> {
  const key = sessionStorage.getItem('fluxplane-api-key')
  const response = await fetch(`/api/v1${path}`, {
    ...options,
    headers: { 'Content-Type': 'application/json', ...(key ? { 'X-API-Key': key } : {}), ...options.headers },
  })
  if (!response.ok) {
    const body = await response.json().catch(() => ({}))
    const detail = typeof body.detail === 'string' ? body.detail :
      Array.isArray(body.detail) ? body.detail.map((item: { msg?: string }) => item.msg ?? '参数不正确').join('；') : null
    throw new Error(detail ?? `请求失败 (${response.status})`)
  }
  return response.json() as Promise<T>
}

export function post<T>(path: string, value?: unknown): Promise<T> {
  return api<T>(path, { method: 'POST', body: JSON.stringify(value ?? {}) })
}

export async function watchEvents(
  after: number,
  signal: AbortSignal,
  onEvent: (event: PlatformEvent) => void,
  onConnected: () => void,
): Promise<number> {
  const key = sessionStorage.getItem('fluxplane-api-key')
  const transport = new AbortController()
  const abort = () => transport.abort()
  signal.addEventListener('abort', abort, { once: true })
  if (signal.aborted) abort()
  let idleTimer: ReturnType<typeof setTimeout> | undefined
  const keepAlive = () => {
    clearTimeout(idleTimer)
    idleTimer = setTimeout(abort, 12_000)
  }
  const decoder = new TextDecoder()
  let buffer = ''
  let cursor = after
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined
  keepAlive()
  try {
    const response = await fetch(`/api/v1/events?after=${after}`, {
      headers: { Accept: 'text/event-stream', ...(key ? { 'X-API-Key': key } : {}) },
      cache: 'no-store', signal: transport.signal,
    })
    if (!response.ok || !response.body) throw new Error(`事件流连接失败 (${response.status})`)
    onConnected()
    reader = response.body.getReader()
    while (true) {
      const { value, done } = await reader.read()
      if (done) break
      keepAlive()
      buffer = (buffer + decoder.decode(value, { stream: true })).replace(/\r\n/g, '\n')
      let boundary = buffer.indexOf('\n\n')
      while (boundary !== -1) {
        const frame = buffer.slice(0, boundary)
        buffer = buffer.slice(boundary + 2)
        const data = frame.split('\n').find(line => line.startsWith('data:'))?.slice(5).trim()
        if (data) {
          const event = JSON.parse(data) as PlatformEvent
          if (Number.isSafeInteger(event.id) && event.id > cursor) {
            cursor = event.id
            onEvent(event)
          }
        }
        boundary = buffer.indexOf('\n\n')
      }
    }
  } finally {
    clearTimeout(idleTimer)
    signal.removeEventListener('abort', abort)
    reader?.releaseLock()
  }
  return cursor
}

export function short(id: string | null | undefined): string {
  return id ? id.slice(0, 8) : '—'
}

export function moment(value: string | null | undefined): string {
  return value ? new Date(value).toLocaleString('zh-CN', { hour12: false }) : '—'
}
