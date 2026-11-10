export const GLOSSARY: Record<string, { title: string; body: string }> = {
  stream_ingestion: { title: 'Stream ingestion', body: 'Continuously accepting events as they are produced instead of loading them in periodic batches. The pipeline must absorb bursts without losing data.' },
  bounded_queue: { title: 'Bounded queue', body: 'A buffer with a fixed capacity between a producer and a consumer. The bound caps memory use and forces an explicit decision when the queue is full.' },
  backpressure: { title: 'Backpressure', body: 'When a downstream stage is slower, the signal "slow down" travels upstream. Here, with the Block policy, the generator waits while the queue is full, so no data is lost but the producer rate falls.' },
  consumer_throughput: { title: 'Consumer throughput', body: 'Records per second the processor actually completes. If it is lower than the producer rate, the queue grows until it hits its bound.' },
  partitioning: { title: 'Partitioning', body: 'Splitting data by a key (here a hash of device_id). Each partition has its own buffer and its own Parquet files, so more partitions mean smaller chunks and more objects, but better data locality per device group.' },
  buffering: { title: 'Buffering', body: 'Accumulating records in memory until a chunk threshold (records, bytes or age) is reached. Writing many tiny objects is expensive; buffering amortizes request overhead.' },
  columnar: { title: 'Columnar storage', body: 'Values of one column are stored together. Similar values compress better and analytical queries read only the needed columns.' },
  parquet: { title: 'Apache Parquet', body: 'A columnar file format with row groups, per-column encoding and compression, and a footer containing the schema and statistics.' },
  compression: { title: 'Compression', body: 'Snappy is fast with a moderate ratio; ZSTD and GZIP compress harder at more CPU cost. The ratio shown is measured from real files: in-memory columnar bytes divided by file bytes.' },
  object_storage: { title: 'Object storage', body: 'Amazon S3 stores immutable objects addressed by key. There is no append, so data is written as complete chunks; every PUT is a billable request.' },
  persistence_latency: { title: 'Persistence latency', body: 'Time for a chunk to be serialized and uploaded to S3. Measured per upload with a monotonic clock.' },
  lambda_processor: { title: 'Processor on AWS Lambda (optional)', body: 'In this mode each batch of events is sent to an AWS Lambda function, validated there with the same rules, and sent back. The round trip (network + invoke overhead + time inside the function) is added latency caused by using Lambda, and it caps what one processor thread can sustain at about batch size ÷ round-trip time. The numbers are measured per call. In-process mode has none of this overhead.' },
  retry: { title: 'Retry behavior', body: 'Throttling and 5xx errors are retried with exponential backoff and jitter; AccessDenied or NoSuchBucket are not retried because retrying cannot fix them.' },
}

export const METRICS: { name: string; definition: string }[] = [
  { name: 'Generated', definition: 'Events actually produced by the generator.' },
  { name: 'Accepted', definition: 'Generated events admitted into the bounded queue. Events rejected at admission (drop-newest) are not accepted.' },
  { name: 'Dropped', definition: 'Queue admission drops (drop-newest) plus evictions (drop-oldest: queued records removed to make room).' },
  { name: 'Rejected', definition: 'Accepted events that failed processor validation. They never reach Parquet.' },
  { name: 'Persisted', definition: 'Valid records inside Parquet objects whose S3 upload succeeded AND whose metadata was durably recorded in SQLite.' },
  { name: 'Queue depth', definition: 'Records currently waiting in the bounded queue.' },
  { name: 'Processing throughput', definition: 'Records leaving the queue per second over a ~1 s sliding window.' },
  { name: 'Upload throughput', definition: 'Bytes confirmed uploaded to S3 per second over a ~1 s window.' },
  { name: 'Blocked producer time', definition: 'Cumulative seconds the generator waited because the queue was full (Block policy).' },
  { name: 'Accounting invariant', definition: 'After a run ends: generated = abandoned + dropped + evicted + discarded + rejected + persisted + failed + processor-failed (batches the Lambda processor could not handle). While running, records in flight (queue, processing, buffers, pending chunks) are shown separately, so generated ≠ persisted during a run.' },
]

export interface NodeInfo {
  title: string
  does: string
  inputs: string
  outputs: string
  config: string[]
  failures: string[]
  concepts: string
}

export const NODE_INFO: Record<string, NodeInfo> = {
  generator: {
    title: 'Generator', does: 'Produces seeded IoT sensor events at the configured rate (with optional bursts and a controlled share of malformed events). Timing is wall-clock based and never a busy loop.',
    inputs: 'Run configuration (rate, devices, seed, invalid ratio).', outputs: 'Event dictionaries offered to the bounded queue.',
    config: ['rate_per_second', 'duration_seconds', 'num_devices', 'invalid_ratio', 'burst_*', 'seed'],
    failures: ['Blocked by a full queue (backpressure)', 'Run duration or record cap reached'],
    concepts: 'Stream ingestion, load generation, reproducibility via seeds.',
  },
  queue: {
    title: 'Queue', does: 'A real bounded queue between producer and consumer. Applies the configured overflow policy when full: block, drop newest, or drop oldest.',
    inputs: 'Events from the generator.', outputs: 'Batches pulled by the processor.',
    config: ['queue_capacity', 'overflow_policy'],
    failures: ['Saturation (utilization near 100%)', 'Dropped events under drop policies', 'Producer blocked time rising'],
    concepts: 'Bounded queue, backpressure, overflow policy trade-offs (loss vs latency).',
  },
  processor: {
    title: 'Processor', does: 'Validates schema, device id, timestamp range, numeric value range and quality flag; extracts the partition key; lightly normalizes values. Rejections are counted and sampled.',
    inputs: 'Queued events.', outputs: 'Valid rows routed to partition buffers; rejection records.',
    config: ['processor_rate_limit', 'processor_mode (local | lambda)', 'invalid_ratio (input)'],
    failures: ['Throughput below producer rate', 'High validation rejection ratio', 'Lambda mode: round-trip latency, cold starts, throttling or an unreachable function'],
    concepts: 'Consumer throughput, schema validation, data quality, cost of a remote call (Lambda mode).',
  },
  buffers: {
    title: 'Buffers', does: 'Per-partition in-memory buffers (routed by CRC32 of device_id). Each tracks records, approximate bytes and age.',
    inputs: 'Valid rows.', outputs: 'Sealed chunks when a record, byte or age threshold is reached.',
    config: ['num_partitions', 'chunk_size_records', 'chunk_max_bytes', 'chunk_max_age_seconds'],
    failures: ['Stalled when downstream upload slots are full'],
    concepts: 'Partitioning, buffering, batching to amortize object-store requests.',
  },
  chunk_builder: {
    title: 'Chunk Builder', does: 'Seals a buffer into a chunk with a unique id (run-partition-sequence), tracks its lifecycle BUFFERING → READY → WRITING → UPLOADING → PERSISTED and hands it to a bounded upload queue.',
    inputs: 'Buffered rows.', outputs: 'Chunks queued for the Parquet writer.',
    config: ['chunk_size_records', 'chunk_max_age_seconds', 'max_pending_uploads'],
    failures: ['Blocked: pending upload slots full (storage backpressure)', 'End-of-run flush creates partial chunks'],
    concepts: 'Chunk lifecycle, trade-off between small/large chunks, backpressure from storage.',
  },
  parquet_writer: {
    title: 'Parquet Writer', does: 'Serializes a chunk with PyArrow using an explicit schema, writes to a temp file, validates the footer and row count, then hands the file to the S3 uploader pool.',
    inputs: 'Sealed chunks.', outputs: 'Valid .parquet files ready for upload.',
    config: ['compression', 'upload_concurrency', 'max_pending_uploads'],
    failures: ['Serialization error (WRITE_FAILED)', 'Temp disk full'],
    concepts: 'Columnar serialization, compression ratio, safe temp-file handling.',
  },
  s3: {
    title: 'Amazon S3', does: 'Final, exclusive persistent store for Parquet files. Uploads use bounded retries with exponential backoff and jitter. A chunk becomes PERSISTED only after the PUT succeeded and metadata is committed.',
    inputs: 'Parquet files.', outputs: 'Objects under flowstore/runs/{run}/partitions/{p}/chunks/{chunk}.parquet.',
    config: ['S3_BUCKET_NAME', 'AWS_REGION', 'S3_UPLOAD_CONCURRENCY', 'S3_MAX_STORAGE_MB'],
    failures: ['AccessDenied / NoSuchBucket (non-retryable)', 'Throttling / 5xx (retried)', 'Storage cap reached'],
    concepts: 'Object storage, persistence latency, retry behaviour, request-cost awareness.',
  },
}

export interface Experiment {
  id: string
  title: string
  objective: string
  preset: string
  steps: string[]
  observe: string[]
  explanation: string
  reflection: string
}

export const EXPERIMENTS: Experiment[] = [
  {
    id: 'A', title: 'Understanding Backpressure', preset: 'queue_saturation',
    objective: 'See how a slow consumer and a bounded queue force the producer to slow down.',
    steps: ['Load the "Queue Saturation" preset and start the run.', 'Watch queue utilization climb to 100%.', 'Compare Generation rate with Processing throughput in the throughput chart.', 'Open the Queue node inspector and read "producer blocked" time.'],
    observe: ['Queue pinned near capacity', 'Generator node turns "backpressured"', 'Generation rate converges to the processor rate (~800/s) although 3000/s was requested'],
    explanation: 'With the Block policy no data is lost: the generator waits whenever the queue is full, so end-to-end speed equals the slowest stage. The Bottleneck Inspector should point at the Processor (heuristic, not proof).',
    reflection: 'What would change if the policy were "drop newest"? What would you give up, and what would you gain?',
  },
  {
    id: 'B', title: 'Chunk Size Trade-offs', preset: 'small_chunks',
    objective: 'Quantify how chunk size changes object count, file size and upload overhead.',
    steps: ['Run "Small Chunks" to completion.', 'Run "Large Chunks" to completion.', 'Open Compare, select both runs.', 'Read file count, average file size, compression ratio and upload latency.'],
    observe: ['Small chunks → many small objects', 'Large chunks → few larger files with a better compression ratio', 'Per-object upload latency differs'],
    explanation: 'Every object costs one PUT request plus fixed overhead; tiny files also compress poorly because there is less repeated data per column. Large chunks reduce overhead but keep data in memory longer (higher flush latency, more loss on a crash).',
    reflection: 'What chunk size would you choose for a dashboard that must show data within 5 seconds?',
  },
  {
    id: 'C', title: 'Validation Quality', preset: 'invalid_records',
    objective: 'Understand how invalid input affects accepted and persisted data.',
    steps: ['Run "Baseline Ingestion" (0% invalid).', 'Run "Invalid Records" (30% invalid).', 'Open the Rejected-records viewer in Run History.', 'Compare persisted records between runs.'],
    observe: ['Rejected count and reasons (invalid device, bad timestamp, non-numeric value, ...)', 'Persisted = generated − rejected', 'Rejected events never appear in Parquet'],
    explanation: 'Validation is a gate before persistence. Cheap early rejection protects storage quality, but it consumes processor time and needs monitoring: a rising rejection ratio is a data-quality signal.',
    reflection: 'Should rejected events be dropped, or sent to a dead-letter location? What would that cost?',
  },
  {
    id: 'D', title: 'Cloud Persistence', preset: 'baseline',
    objective: 'Trace a record from the generator to a real Parquet object in Amazon S3.',
    steps: ['Start the "Baseline Ingestion" run.', 'Watch chunks being sealed and uploaded in the Event Log (Amazon S3 entries).', 'Open Storage Explorer and select an object.', 'Inspect schema, row groups, compression and sample rows; download the file.'],
    observe: ['Object keys under flowstore/runs/{run}/partitions/{p}/chunks/', 'Footer metadata read via ranged GETs (not a full download)', 'Record count equals the persisted counter'],
    explanation: 'Parquet gives columnar layout, compression and a self-describing footer; S3 gives cheap, durable, immutable objects. Together they form the classic data-lake pattern: write immutable chunks, query later.',
    reflection: 'Why must S3 objects be written whole rather than appended to?',
  },
  {
    id: 'E', title: 'In-process vs Lambda Processor', preset: 'baseline',
    objective: 'Measure what it costs to move the processing stage to AWS Lambda (needs Lambda enabled on the server).',
    steps: ['Run "Baseline Ingestion" with "Processor runs on" = In-process.', 'Run it again with "Processor runs on" = AWS Lambda.', 'Open the Processor node inspector during the Lambda run and read the round trip, handler time and network overhead.', 'Open Compare with both runs.'],
    observe: ['Processor time per batch rises from about a millisecond to tens of milliseconds', 'Lambda round trip = time inside the function + network and invoke overhead', 'Cold starts are counted separately (the run pre-warms the function first)', 'Persisted counts stay the same: the rules are identical'],
    explanation: 'Calling a remote function turns a local loop into a network call per batch. Latency rises and one processor thread can sustain fewer records per second, so heavy presets may start filling the queue. The benefit is that the stage no longer runs on the same machine. The numbers shown are measured per call, never estimated.',
    reflection: 'When is a per-batch remote call worth its latency? How would a bigger batch size change the trade-off?',
  },
]
