export type RunState =
  | 'IDLE' | 'STARTING' | 'RUNNING' | 'PAUSING' | 'PAUSED' | 'STOPPING' | 'FLUSHING'
  | 'COMPLETED' | 'FAILED' | 'CANCELLED' | 'INTERRUPTED'

export type NodeStatus =
  | 'idle' | 'running' | 'waiting' | 'backpressured' | 'flushing' | 'uploading' | 'warning' | 'error' | 'completed'

export interface RunConfig {
  scenario: string
  seed: number
  rate_per_second: number
  duration_seconds: number
  num_devices: number
  invalid_ratio: number
  burst_enabled: boolean
  burst_multiplier: number
  burst_every_seconds: number
  burst_duration_seconds: number
  queue_capacity: number
  overflow_policy: 'block' | 'drop_newest' | 'drop_oldest'
  processor_rate_limit: number
  num_partitions: number
  chunk_size_records: number
  chunk_max_bytes: number
  chunk_max_age_seconds: number
  compression: 'snappy' | 'zstd' | 'gzip' | 'none'
  max_pending_uploads: number
  upload_concurrency: number
  injected_upload_delay_ms: number
}

export interface Preset {
  id: string
  name: string
  description: string
  expected: string
  config: RunConfig
}

export interface QueueMetrics {
  depth: number; capacity: number; utilization: number; enqueued: number; dequeued: number
  dropped_newest: number; evicted_oldest: number; blocked_seconds: number; blocked_events: number
  max_depth: number; blocked_now: boolean; policy: string
}

export interface PartitionInfo { partition: number; records: number; bytes: number; age_s: number }

export interface Snapshot {
  run_id: string | null
  state: RunState
  scenario?: string
  ts?: number
  elapsed_s?: number
  planned_records?: number
  note?: string | null
  stopped_early?: boolean
  counters?: {
    generated: number; accepted: number; dropped: number; dropped_newest: number; evicted_oldest: number
    rejected_validation: number; persisted: number; failed: number; discarded: number; abandoned: number
    persisted_bytes: number; files: number
  }
  rates?: Record<'generation' | 'enqueue' | 'processing' | 'persist' | 'upload_bytes' | 'chunks' | 'rejected' | 'dropped', number>
  generator?: { configured_rate: number; actual_rate: number; generated: number; done: boolean; burst_enabled: boolean }
  queue?: QueueMetrics
  processor?: {
    processed: number; rate: number; rejected: number; reject_ratio: number; latency_ms: number
    rate_limit: number; top_rejections: [string, number][]
  }
  buffers?: {
    partitions: PartitionInfo[]; records: number; bytes: number; active_partitions: number
    capacity: number; max_age_s: number; utilization: number
  }
  chunks?: {
    created: number; pending: number; target_size: number; current_max_fill: number
    builder_blocked: boolean; completed: number; blocked_ms: number
  }
  writer?: {
    writing: number; uploading: number; pending_uploads: number; max_pending: number; succeeded: number
    failed: number; retries: number; avg_serialize_ms: number; avg_upload_ms: number
    recent_upload_ms: number; compression_ratio: number; injected_delay_ms: number
  }
  s3?: {
    bucket: string; objects: number; bytes: number; last_upload_at: number | null
    last_error: string | null; upload_bytes_rate: number; retries: number; failures: number
  }
  accounting?: Record<string, number | boolean | null>
  nodes?: Record<'generator' | 'queue' | 'processor' | 'buffers' | 'chunk_builder' | 'parquet_writer' | 's3', NodeStatus>
  bottleneck?: { stage: string; confidence: string; summary: string; evidence: string[]; heuristic: boolean; note?: string }
}

export interface LogEvent {
  seq?: number; id?: number; ts: number; run_id: string | null; component: string
  severity: 'INFO' | 'WARNING' | 'ERROR' | 'SUCCESS'; message: string; details?: Record<string, unknown> | null
}

export interface RunRow {
  run_id: string; scenario: string; state: RunState; started_at: number; ended_at: number | null
  duration_s: number | null; generated: number; accepted: number; rejected_validation: number
  dropped: number; persisted: number; unpersisted: number; peak_queue_depth: number
  avg_throughput: number; peak_throughput: number; files: number; s3_bytes: number
  uncompressed_bytes: number; avg_upload_ms: number; failures: number; note: string | null
  config: RunConfig
}

export interface S3Object {
  object_id: string; run_id: string; chunk_id: string; partition_id: number; bucket: string
  object_key: string; size_bytes: number; record_count: number; compression: string
  uploaded_at: number; upload_ms: number; attempts: number; status: string
}

export interface Estimate {
  valid: boolean; errors: string[]; planned_records: number; estimated_valid_records: number
  estimated_chunks_upper_bound: number; estimated_put_requests: number; warnings: string[]; note: string
}

export interface HealthCheck { name: string; ok: boolean; detail: string; hint?: string | null }
export interface StorageHealth {
  bucket: string; region: string; prefix: string; checks: HealthCheck[]; ready: boolean; write_verified: boolean
}

export interface MetricSample {
  t: number; elapsed: number; gen_rate: number; proc_rate: number; persist_rate: number
  queue_depth: number; queue_capacity: number; buffered: number; chunks: number; chunk_rate: number
  upload_ms: number; persisted: number; rejected: number; dropped: number; reject_rate: number
  drop_rate: number; pending_uploads: number
}
