use crate::common::TreeNodeExt;
use crate::coordinator::MetricsStore;
use crate::coordinator::distributed::PreparedPlan;
use crate::coordinator::task_spawner::{
    CoordinatorToWorkerMetrics, CoordinatorToWorkerTaskSpawner,
};
use crate::distributed_planner::{
    NetworkBoundaryBuilderResult, inject_network_boundaries, network_boundary_inject_sampler,
    network_boundary_scale_input,
};
use crate::stage::{LocalStage, RemoteStage};
use crate::worker::generated::worker as pb;
use crate::{
    DistributedConfig, NetworkBoundary, NetworkBoundaryExt, NetworkCoalesceExec, Stage,
    TaskCountAnnotation, TaskEstimator, TaskRoutingContext, get_distributed_worker_resolver,
};
use dashmap::DashMap;
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err};
use datafusion::config::ConfigOptions;
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use futures::{Stream, StreamExt};
use rand::Rng;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Mutex};
use tokio_stream::wrappers::UnboundedReceiverStream;
use url::Url;

pub(super) async fn prepare_dynamic_plan(
    base_plan: &Arc<dyn ExecutionPlan>,
    metrics: &ExecutionPlanMetricsSet,
    task_metrics: &Option<Arc<MetricsStore>>,
    ctx: &Arc<TaskContext>,
) -> Result<PreparedPlan> {
    let metrics = CoordinatorToWorkerMetrics::new(metrics);

    let worker_idx = AtomicUsize::new(rand::rng().random_range(0..100)); // TODO
    let plans_for_viz = PlanReconstructor::default();
    let outer_join_set = Mutex::new(JoinSet::new());

    let head_stage = inject_network_boundaries(
        Arc::clone(base_plan),
        |nb: Arc<dyn NetworkBoundary>, cfg: &ConfigOptions| {
            let d_cfg = DistributedConfig::from_config_options(cfg)?;
            let worker_resolver = get_distributed_worker_resolver(ctx.session_config())?;

            let task_estimator = &d_cfg.__private_task_estimator;
            let mut join_set = JoinSet::new();
            let Stage::Local(input_stage) = nb.input_stage() else {
                return exec_err!("NetworkBoundary's input stage was in remote mode.");
            };
            let mut input_stage = input_stage.clone();
            input_stage.plan = network_boundary_inject_sampler(input_stage.plan)?;
            let mut spawner = CoordinatorToWorkerTaskSpawner::new(
                &input_stage,
                &metrics,
                task_metrics,
                ctx,
                &mut join_set,
            )?;

            let urls = worker_resolver.get_urls()?;
            let next_url = || urls[(worker_idx.fetch_add(1, SeqCst)) % urls.len()].clone();

            let routed_urls = match task_estimator.route_tasks(&TaskRoutingContext {
                task_ctx: Arc::clone(ctx),
                plan: &input_stage.plan,
                task_count: input_stage.tasks,
                available_urls: &urls,
            }) {
                Ok(Some(routed_urls)) => routed_urls,
                // If the user has not defined custom routing with a `route_tasks` implementation, we
                // default to round-robin task assignation from a randomized starting point.
                Ok(None) => (0..input_stage.tasks).map(|_| next_url()).collect(),
                Err(e) => return exec_err!("error routing tasks to workers: {e}"),
            };

            if routed_urls.len() != input_stage.tasks {
                return exec_err!(
                    "number of tasks ({}) was not equal to number of urls ({}) at execution time",
                    input_stage.tasks,
                    routed_urls.len()
                );
            }

            let mut workers = Vec::with_capacity(input_stage.tasks);
            let mut load_info_rxs = Vec::with_capacity(input_stage.tasks);

            let mut url = if input_stage.tasks == 1 {
                get_child_stages_urls(&input_stage.plan)?
                    .iter()
                    .find_map(|v| match v.len() == 1 {
                        true => Some(v.first().cloned()),
                        false => None,
                    })
                    .flatten()
                    .unwrap_or_else(next_url)
            } else {
                next_url()
            };

            for i in 0..input_stage.tasks {
                workers.push(url.clone());
                // Spawns the task that feeds this subplan to this worker. There will be as
                // many as this spawned tasks as workers.
                let (tx, worker_rx) = spawner.send_plan_task(Arc::clone(ctx), i, url)?;
                load_info_rxs.push({
                    let rx = spawner.load_info_and_metrics_collection_task(i, worker_rx);
                    // Tag each LoadInfoBatch with the producer task index so
                    // `calculate_task_count` can identify (task_idx, partition) slices
                    // independently — `select_all` would otherwise collapse them.
                    UnboundedReceiverStream::new(rx).map(move |batch| (i, batch))
                });
                spawner.work_unit_feed_task(Arc::clone(ctx), i, tx)?;
                url = next_url();
            }

            outer_join_set
                .lock()
                .expect("poisoned lock")
                .spawn(async move {
                    for result in join_set.join_all().await {
                        result?;
                    }
                    Ok(())
                });

            plans_for_viz.insert(input_stage.num, Arc::clone(&input_stage.plan));

            let nb = nb.with_input_stage(Stage::Remote(RemoteStage {
                query_id: input_stage.query_id,
                num: input_stage.num,
                workers,
            }))?;

            let load_info_stream = futures::stream::select_all(load_info_rxs);
            let partitions_per_task = nb.properties().partitioning.partition_count();
            let partitions_remaining = vec![partitions_per_task; input_stage.tasks];
            let bytes_per_partition_per_second = d_cfg.bytes_per_partition_per_second;

            Ok(async move {
                let task_count_above = if nb.as_any().is::<NetworkCoalesceExec>() {
                    TaskCountAnnotation::Maximum(1)
                } else {
                    let bps = total_bytes_per_second(load_info_stream, partitions_remaining).await;
                    let necessary_partitions = bps.div_ceil(bytes_per_partition_per_second);
                    let expected_tasks = necessary_partitions.div_ceil(partitions_per_task);
                    TaskCountAnnotation::Desired(expected_tasks)
                };
                Ok(NetworkBoundaryBuilderResult {
                    task_count_above,
                    network_boundary: nb,
                })
            })
        },
        ctx.session_config().options(),
    )
    .await?;
    Ok(PreparedPlan {
        final_plan: plans_for_viz.reconstruct(&head_stage)?,
        head_stage,
        join_set: std::mem::take(&mut outer_join_set.lock().unwrap()),
    })
}

#[derive(Default)]
struct PlanReconstructor {
    stage_map: DashMap<usize, Arc<dyn ExecutionPlan>>,
}

impl PlanReconstructor {
    fn insert(&self, stage: usize, plan: Arc<dyn ExecutionPlan>) {
        self.stage_map.insert(stage, plan);
    }

    fn reconstruct(&self, head_stage: &Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let head_stage = Arc::clone(head_stage);
        let reconstructed = head_stage.transform_down_with_task_count(1, |plan, tc| {
            let Some(nb) = plan.as_network_boundary() else {
                return Ok(Transformed::no(plan));
            };
            let input_stage = nb.input_stage();
            let Some(plan_for_viz) = self.stage_map.get(&input_stage.num()) else {
                return exec_err!(
                    "Failed to retrieve plan for stage {} for visualization purposes",
                    input_stage.num()
                );
            };

            let plan_for_viz = network_boundary_scale_input(
                Arc::clone(&plan_for_viz),
                nb.properties().partitioning.partition_count(),
                tc,
            )?;

            let nb = nb.with_input_stage(Stage::Local(LocalStage {
                query_id: input_stage.query_id(),
                num: input_stage.num(),
                plan: plan_for_viz,
                tasks: input_stage.task_count(),
            }))?;

            Ok(Transformed::yes(nb))
        })?;
        Ok(reconstructed.data)
    }
}

fn get_child_stages_urls(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Vec</* stage */ &Vec</* worker */ Url>>> {
    let mut result = vec![];
    plan.apply(|plan| {
        let Some(nb) = plan.as_network_boundary() else {
            return Ok(TreeNodeRecursion::Continue);
        };

        match nb.input_stage() {
            Stage::Local(_) => exec_err!("While gathering child stages URLs, one was in local mode. This is a bug in the dynamic task count execution logic, please report it.")?,
            Stage::Remote(remote) => result.push(&remote.workers)
        }

        Ok(TreeNodeRecursion::Jump)
    })?;

    Ok(result)
}

/// Estimates the bytes per second flowing through a stage by reading sample information.
async fn total_bytes_per_second(
    mut load_info_stream: impl Stream<Item = (usize, pb::LoadInfo)> + Unpin,
    mut partitions_remaining: Vec<usize>,
) -> usize {
    const ANY_SAMPLE_PERCENTAGE: f32 = 0.5;
    const BYTES_READY_SAMPLE_PERCENTAGE: f32 = 0.2;
    const BYTES_PER_SECOND_SAMPLE_PERCENTAGE: f32 = 0.2;

    fn apply_pct(value: usize, pct: f32) -> usize {
        (value as f32 * pct) as usize
    }

    let total_partitions = partitions_remaining.iter().sum::<usize>();
    let mut partitions_with_bytes_per_second_done = 0;
    let mut partitions_with_bytes_ready_done = 0;
    let mut partitions_done = 0;
    let mut bytes_ready = 0;
    let mut bytes_per_second = 0;

    while let Some((task_idx, load_info)) = load_info_stream.next().await {
        partitions_remaining[task_idx] -= 1;
        bytes_per_second += load_info.bytes_per_second as usize;
        bytes_ready += load_info.bytes_ready as usize;

        partitions_with_bytes_per_second_done += (load_info.bytes_per_second > 0) as usize;
        partitions_with_bytes_ready_done += (load_info.bytes_ready > 0) as usize;
        partitions_done += 1;

        // Short circuit if we collected enough bytes_ready measurements.
        if partitions_with_bytes_ready_done
            >= apply_pct(total_partitions, BYTES_READY_SAMPLE_PERCENTAGE)
        {
            break;
        }

        // Short circuit if we collected enough bytes_per_second measurements.
        if partitions_with_bytes_per_second_done
            >= apply_pct(total_partitions, BYTES_PER_SECOND_SAMPLE_PERCENTAGE)
        {
            break;
        }

        // Short circuit early if there's any total reads, regarding of whether they contained
        // a bytes read or not.
        if partitions_done >= apply_pct(total_partitions, ANY_SAMPLE_PERCENTAGE) {
            break;
        }

        // Short circuit if there are no further partitions remaining to sample from.
        if partitions_remaining.iter().all(|p| *p == 0) {
            break;
        }
    }

    if partitions_done == 0 {
        return 0;
    }

    bytes_ready *= total_partitions;
    bytes_ready /= partitions_done;

    bytes_per_second *= total_partitions;
    bytes_per_second /= partitions_done;

    // TODO: it's not really fair to sum this two, as they are different magnitudes
    bytes_per_second + bytes_ready
}
