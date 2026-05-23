use crate::common::require_one_child;
use crate::worker::generated::worker as pb;
use crate::{
    BytesCounterMetric, BytesMetricExt, GaugeMetricExt, LatencyMetricExt, MaxGaugeMetric,
    MaxLatencyMetric, P50LatencyMetric,
};
use datafusion::arrow::array::Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_common::metrics::{Gauge, MetricValue, MetricsSet};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, Time};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use std::any::Any;
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::sync::oneshot;

/// How many [RecordBatch]s to allow the input stream to yield synchronously (without yielding back
/// to tokio) before short-circuiting buffering.
const READY_CHUNK_LIMIT: usize = 256;
/// Maximum read of bytes per second allowed to be emitted. Reads greater than this will be
/// truncated to this max value, as it's assumed that [READY_CHUNK_LIMIT] was hit and no useful
/// measurement can actually be emitted.
const MAX_BYTES_PER_SECOND: usize = 512 * 1024 * 1024;

#[derive(Debug)]
pub struct SamplerExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) metric_set: ExecutionPlanMetricsSet,
    pub(crate) partition_samplers: Vec<PartitionSampler>,
}

/// Metrics that quantify how long the sampler held data in memory before the consumer
/// (real execution) attached, plus the peak buffer size reached. All metrics are shared
/// across the partition samplers; the latency metrics aggregate per-partition observations.
#[derive(Debug, Clone)]
pub(crate) struct SamplerExecMetrics {
    /// Time since [SamplerExec::kick_off_first_sampler] was called until the first batch from
    /// the input arrived
    kick_off_to_fist_batch_p50: P50LatencyMetric,
    kick_off_to_fist_batch_max: MaxLatencyMetric,
    /// Time since [SamplerExec::kick_off_first_sampler] was called until the [pb::LoadInfo] message
    /// was sent.
    kick_off_to_load_info_sent_p50: P50LatencyMetric,
    kick_off_to_load_info_sent_max: MaxLatencyMetric,
    /// Time since [SamplerExec::kick_off_first_sampler] was called until the first batch from
    /// the input arrived
    kick_off_to_execution_p50: P50LatencyMetric,
    kick_off_to_execution_max: MaxLatencyMetric,
    /// Maximum number of record batches buffered by a sampler.
    max_batches_buffered: MaxGaugeMetric,
    /// Peak memory buffered by any partition sampler during the sampling phase.
    max_mem_used: Gauge,
    /// Bytes per second flowing through the sampler node.
    bytes_per_sec: BytesCounterMetric,
    /// Bytes ready at the moment of reporting load info.
    bytes_ready: BytesCounterMetric,
    /// Elapsed compute while sampling.
    elapsed_compute: Time,
}

impl SamplerExecMetrics {
    fn new(metric_set: &ExecutionPlanMetricsSet) -> Self {
        let bdr = || MetricBuilder::new(metric_set);
        Self {
            kick_off_to_fist_batch_p50: bdr().p50_latency("kick_off_to_first_batch_p50"),
            kick_off_to_fist_batch_max: bdr().max_latency("kick_off_to_first_batch_max"),
            kick_off_to_load_info_sent_p50: bdr().p50_latency("kick_off_to_load_info_sent_p50"),
            kick_off_to_load_info_sent_max: bdr().max_latency("kick_off_to_load_info_sent_max"),
            kick_off_to_execution_p50: bdr().p50_latency("kick_off_to_execution_p50"),
            kick_off_to_execution_max: bdr().max_latency("kick_off_to_execution_max"),
            max_batches_buffered: bdr().max_gauge("max_batches_buffered"),
            max_mem_used: bdr().global_gauge("max_mem_used"),
            bytes_per_sec: bdr().bytes_counter("bytes_per_sec"),
            bytes_ready: bdr().bytes_counter("bytes_ready"),
            elapsed_compute: {
                let time = Time::new();
                bdr().build(MetricValue::ElapsedCompute(time.clone()));
                time
            },
        }
    }
}

impl SamplerExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let metric_set = ExecutionPlanMetricsSet::new();
        let metric_set_clone = metric_set.clone();
        // Metrics need to be lazily initialized, otherwise the coordinator side will register
        // them when they are never relevant there, they are just relevant in workers.
        let metrics: Arc<LazyLock<_, Box<dyn FnOnce() -> SamplerExecMetrics + Send>>> =
            Arc::new(LazyLock::new(Box::new(move || {
                SamplerExecMetrics::new(&metric_set_clone)
            })));
        let partitions = input.properties().partitioning.partition_count();
        let mut samplers = Vec::with_capacity(partitions);
        for i in 0..partitions {
            samplers.push(PartitionSampler {
                partition_idx: i,
                input: Arc::clone(&input),
                stream: Mutex::new(None),
                metrics: Arc::clone(&metrics),
                kick_off_at: Arc::new(OnceLock::new()),
                first_batch_at: Arc::new(OnceLock::new()),
                load_info_sent_at: Arc::new(OnceLock::new()),
            });
        }
        Self {
            input,
            metric_set,
            partition_samplers: samplers,
        }
    }

    pub(crate) fn kick_off_first_sampler(
        plan: Arc<dyn ExecutionPlan>,
        ctx: Arc<TaskContext>,
    ) -> Result<Vec<oneshot::Receiver<pb::LoadInfo>>> {
        let mut receivers = vec![];
        plan.apply(|plan| {
            let Some(sampler) = plan.as_any().downcast_ref::<SamplerExec>() else {
                return Ok(TreeNodeRecursion::Continue);
            };
            receivers.reserve(sampler.partition_samplers.len());
            for partition_sampler in &sampler.partition_samplers {
                let rx = partition_sampler.kick_off(Arc::clone(&ctx))?;
                receivers.push(rx);
            }
            Ok(TreeNodeRecursion::Stop)
        })?;
        Ok(receivers)
    }
}

pub(crate) struct PartitionSampler {
    partition_idx: usize,
    input: Arc<dyn ExecutionPlan>,
    stream: Mutex<Option<SendableRecordBatchStream>>,

    // Metrics state.
    metrics: Arc<LazyLock<SamplerExecMetrics, Box<dyn FnOnce() -> SamplerExecMetrics + Send>>>,
    /// Set when `kick_off` is invoked. Used at `execute()` time to record how long the
    /// sampler buffered data before the consumer attached.
    kick_off_at: Arc<OnceLock<Instant>>,
    /// Set the first time the producer task emits a `LoadInfo`. Used at `execute()` time
    /// to record the gap between the first sample and the consumer starting.
    first_batch_at: Arc<OnceLock<Instant>>,
    /// Set immediately after `sampling_tx.send()` succeeds. Used to measure the full
    /// round-trip: LoadInfo sent → coordinator collects votes → downstream plan dispatched
    /// → consumer calls execute().
    load_info_sent_at: Arc<OnceLock<Instant>>,
}

impl Debug for PartitionSampler {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionSampler").finish()
    }
}

impl PartitionSampler {
    fn start_stream(&self) -> Option<SendableRecordBatchStream> {
        let Some(kick_off_at) = self.kick_off_at.get() else {
            return self.stream.lock().unwrap().take();
        };

        // Time since this sampler was kicked off until the first batch arrived.
        if let Some(t) = self.first_batch_at.get() {
            let delay = t.saturating_duration_since(*kick_off_at);
            self.metrics.kick_off_to_fist_batch_p50.add_duration(delay);
            self.metrics.kick_off_to_fist_batch_max.add_duration(delay);
        }

        // Time since the sampler was kicked off until the pb::LoadInfo message was sent.
        if let Some(t) = self.load_info_sent_at.get() {
            let delay = t.saturating_duration_since(*kick_off_at);
            self.metrics
                .kick_off_to_load_info_sent_p50
                .add_duration(delay);
            self.metrics
                .kick_off_to_load_info_sent_max
                .add_duration(delay);
        }

        // Time since the sampler was kicked off until it started executing.
        let delay = kick_off_at.elapsed();
        self.metrics.kick_off_to_execution_p50.add_duration(delay);
        self.metrics.kick_off_to_execution_max.add_duration(delay);

        self.stream.lock().unwrap().take()
    }

    fn kick_off(&self, ctx: Arc<TaskContext>) -> Result<oneshot::Receiver<pb::LoadInfo>> {
        let _ = self.kick_off_at.set(Instant::now());
        let (sampling_tx, sampling_rx) = oneshot::channel();

        let input = Arc::clone(&self.input);
        let partition_idx = self.partition_idx;
        let schema = input.schema();
        let elapsed_compute = self.metrics.elapsed_compute.clone();
        let first_batch_at = Arc::clone(&self.first_batch_at);

        let mut reporter = LoadInfoDropHandler {
            load_info: pb::LoadInfo {
                partition: partition_idx as u64,
                bytes_per_second: 0,
                bytes_ready: 0,
            },
            sampling_tx: Some(sampling_tx),
            bytes_per_second_metric: self.metrics.bytes_per_sec.clone(),
            load_info_sent_at: Arc::clone(&self.load_info_sent_at),
            bytes_ready_metric: self.metrics.bytes_ready.clone(),
        };

        let mut buffer = RecordBatchBuffer {
            buffer: VecDeque::new(),
            max_mem_used: self.metrics.max_mem_used.clone(),
            max_batches_buffered: self.metrics.max_batches_buffered.clone(),
            memory_reservation: Arc::new(
                MemoryConsumer::new(format!("PartitionSampler[{partition_idx}]"))
                    .register(ctx.memory_pool()),
            ),
            first_batch_at: Arc::clone(&self.first_batch_at),
        };

        // Execute the input synchronously so any setup error surfaces before we
        // spawn the producer task.
        let mut input_stream = input.execute(partition_idx, ctx)?.fuse();

        let task = SpawnedTask::spawn(async move {
            // First, read at once all the RecordBatches that are ready to be yielded synchronously.
            // Some downstream nodes will accumulate data in-memory, and will then yield several
            // RecordBatches at once synchronously (without Poll::Pending gaps in between).
            let mut chunked = (&mut input_stream).ready_chunks(READY_CHUNK_LIMIT);
            let Some(batches) = chunked.next().await else {
                // Not a single RecordBatch was produced, so let bytes_per_second=0 be sent as-is.
                return Ok(buffer.chain(input_stream).boxed());
            };
            let _timer = elapsed_compute.timer();
            for batch in batches {
                let _ = first_batch_at.set(Instant::now());
                buffer.push(batch?);
            }

            // The downstream node yielded too many RecordBatches synchronously, more than what we
            // are willing to buffer here, so assume that there's going to be a massive amount of
            // data flowing through here.
            if buffer.len() >= READY_CHUNK_LIMIT {
                reporter.set_bytes_ready(buffer.bytes_ready());
                reporter.set_bytes_per_second(MAX_BYTES_PER_SECOND);
                return Ok(buffer.chain(input_stream).boxed());
            }

            // The downstream node finished producing all RecordBatches, there are not more. This
            // means that it spent some time accumulating data, and then yielded N RecordBatches
            // where N < READY_CHUNK_LIMIT. In this case, no data velocity measurement is reported.
            if matches!(input_stream.next().now_or_never(), Some(None)) {
                reporter.set_bytes_ready(buffer.bytes_ready());
                return Ok(buffer.chain(input_stream).boxed());
            }

            drop(_timer);

            // Wait for an async gap in order to measure data velocity.
            let poll_start = Instant::now();
            let Some(batch) = input_stream.try_next().await? else {
                // The last message was somehow the last message in the stream, but the stream did
                // not end immediately. This is an unlikely scenario.
                reporter.set_bytes_ready(buffer.bytes_ready());
                return Ok(buffer.chain(input_stream).boxed());
            };
            let bytes_per_second =
                (record_batch_size(&batch) as f32 / poll_start.elapsed().as_secs_f32()) as usize;

            let _timer = elapsed_compute.timer();

            buffer.push(batch);

            // Some RecordBatches where buffered, but there's more to be yielded, so both
            // bytes_per_second and bytes_ready can be reported.
            reporter.set_bytes_ready(buffer.bytes_ready());
            reporter.set_bytes_per_second(bytes_per_second);

            Ok(buffer.chain(input_stream).boxed())
        });

        let stream = async move {
            task.await
                .map_err(|err| DataFusionError::Internal(err.to_string()))?
        }
        .try_flatten_stream();

        self.stream
            .lock()
            .expect("poisoned lock")
            .replace(Box::pin(RecordBatchStreamAdapter::new(schema, stream)));

        Ok(sampling_rx)
    }
}

struct LoadInfoDropHandler {
    load_info: pb::LoadInfo,
    bytes_ready_metric: BytesCounterMetric,
    bytes_per_second_metric: BytesCounterMetric,
    sampling_tx: Option<oneshot::Sender<pb::LoadInfo>>,
    load_info_sent_at: Arc<OnceLock<Instant>>,
}

impl LoadInfoDropHandler {
    fn set_bytes_ready(&mut self, bytes_ready: usize) {
        self.load_info.bytes_ready = bytes_ready as u64;
        self.bytes_ready_metric.add_bytes(bytes_ready);
    }

    fn set_bytes_per_second(&mut self, bytes_per_second: usize) {
        self.load_info.bytes_per_second = bytes_per_second as u64;
        self.bytes_per_second_metric.add_bytes(bytes_per_second);
    }
}

impl Drop for LoadInfoDropHandler {
    fn drop(&mut self) {
        if let Some(sampling_tx) = self.sampling_tx.take() {
            let _ = sampling_tx.send(self.load_info);
            let _ = self.load_info_sent_at.set(Instant::now());
        }
    }
}

struct RecordBatchBuffer {
    buffer: VecDeque<(RecordBatch, usize)>,
    max_batches_buffered: MaxGaugeMetric,
    max_mem_used: Gauge,
    memory_reservation: Arc<MemoryReservation>,
    first_batch_at: Arc<OnceLock<Instant>>,
}

impl RecordBatchBuffer {
    fn push(&mut self, batch: RecordBatch) {
        let batch_size = record_batch_size(&batch);
        if self.buffer.is_empty() {
            let _ = self.first_batch_at.set(Instant::now());
        }
        self.max_mem_used.add(batch_size);
        self.memory_reservation.grow(batch_size);
        self.buffer.push_back((batch, batch_size));
        self.max_batches_buffered.set_max(self.buffer.len());
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }

    fn bytes_ready(&self) -> usize {
        self.buffer.iter().map(|(_, size)| *size).sum()
    }
}

impl Stream for RecordBatchBuffer {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.as_mut().buffer.pop_front() {
            None => Poll::Ready(None),
            Some((batch, size)) => {
                self.memory_reservation.shrink(size);
                Poll::Ready(Some(Ok(batch)))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.buffer.len(), Some(self.buffer.len()))
    }
}

fn record_batch_size(batch: &RecordBatch) -> usize {
    let mut result = 0;
    for c in batch.columns() {
        result += c.to_data().get_slice_memory_size().unwrap_or(0)
    }
    result
}

impl DisplayAs for SamplerExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "SamplerExec: partitions={}",
            self.partition_samplers.len()
        )
    }
}

impl ExecutionPlan for SamplerExec {
    fn name(&self) -> &str {
        "SamplerExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(require_one_child(children)?)))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let Some(stream) = self.partition_samplers[partition].start_stream() else {
            return exec_err!("SamplerExec[{partition}] was not kicked off");
        };
        Ok(stream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric_set.clone_inner())
    }
}
