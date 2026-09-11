//! A small, realistic timeout/retry application.
//!
//! A client accepts logical work, sends it to a service, and keeps the request
//! outstanding until a response arrives. A periodic timer re-sends outstanding
//! requests whose deadline has passed. The service performs finite-rate work
//! and returns the result. There is no simulation clock or synthetic capacity
//! input in the program.

use std::time::{Duration, Instant};

use hydro_lang::live_collections::stream::{NoOrder, TotalOrder};
use hydro_lang::location::external_process::{ExternalBincodeBidi, NotMany};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// Logical work submitted by an application.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    pub id: u64,
    pub value: String,
}

/// Successful completion of a logical request.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Response {
    pub id: u64,
    pub value: String,
}

/// Application-aware ground-truth events. These are observations of the real
/// program, not inputs that drive a model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetryEvent {
    LogicalRequest(u64),
    PhysicalAttempt(u64),
    ServiceCompletion(u64),
    LogicalCompletion(u64),
}

pub struct TimeoutRetryOutputs<'a> {
    pub completed: Stream<Response, Process<'a, Client>, Unbounded, NoOrder>,
    pub events: Stream<RetryEvent, Process<'a, Client>, Unbounded, NoOrder>,
}

/// What the client needs while waiting for a response.
#[derive(Clone, Debug)]
struct Outstanding {
    request: Request,
    retry_at: Instant,
}

pub struct Client;
pub struct Service;

/// Exposes the real application to one external workload driver. The driver can
/// submit ordinary requests and observe ground-truth events; it does not control
/// service capacity or time.
pub fn timeout_retry_external<'a>(
    external: &External<'a, ()>,
    client: &Process<'a, Client>,
    service: &Process<'a, Service>,
    timeout_millis: u64,
    service_interval_millis: u64,
) -> ExternalBincodeBidi<Request, RetryEvent, NotMany> {
    let (port, requests, event_sink) =
        client.bind_single_client_bincode::<_, Request, RetryEvent>(external);
    let outputs = timeout_retry(
        client,
        service,
        requests,
        timeout_millis,
        service_interval_millis,
    );
    event_sink.complete(outputs.events.assume_ordering::<TotalOrder>(nondet!(
        /** Event order is observability only; event kinds carry their semantics. */
    )));
    outputs
        .completed
        .assume_ordering::<TotalOrder>(
            nondet!(/** Completion order is not externally observed here. */),
        )
        .for_each(q!(|_| {}));
    port
}

/// Connects a retrying client to a text-normalization service.
///
/// `requests` is ordinary application input. The returned stream contains
/// completed responses. Requests cross a real Hydro network edge in both
/// directions. The service uppercases each payload synchronously; its single
/// operator therefore has finite measured throughput, especially for large
/// payloads.
pub fn timeout_retry<'a>(
    client: &Process<'a, Client>,
    service: &Process<'a, Service>,
    requests: Stream<Request, Process<'a, Client>, Unbounded>,
    timeout_millis: u64,
    service_interval_millis: u64,
) -> TimeoutRetryOutputs<'a> {
    let (send_attempts, attempts) =
        client.forward_ref::<Stream<Request, Process<'a, Client>, Unbounded, NoOrder>>();

    let physical_attempt_events = attempts
        .clone()
        .map(q!(|request| RetryEvent::PhysicalAttempt(request.id)));
    let logical_request_events = requests
        .clone()
        .map(q!(|request| RetryEvent::LogicalRequest(request.id)));

    let service_ticks = service.source_interval(q!(Duration::from_millis(service_interval_millis)));
    let requests_at_service = attempts.send(service, TCP.fail_stop().bincode().name("requests"));
    let serviced = sliced! {
        let arrivals = use::batch(requests_at_service, nondet!(
            /** Network arrivals may be assigned to any service tick. */
        ));
        let pulses = use::batch(service_ticks, nondet!(
            /** Wall-clock service pulses may be assigned to any service tick. */
        ));
        let mut queue = use::state(|location| {
            location.singleton(q!(std::collections::VecDeque::<Request>::new()))
        });
        let arrival_vec = arrivals.assume_ordering::<TotalOrder>(nondet!(
            /** Any network arrival order is a valid FIFO service order. */
        )).fold(q!(|| Vec::new()), q!(|out, request| out.push(request)));
        let pulse_count = pulses.count();
        let tick = arrival_vec.location().clone();
        let arrivals_ref = arrival_vec.by_ref();
        let pulses_ref = pulse_count.by_ref();
        let queue_ref = queue.by_mut();

        tick.singleton(q!(()))
            .into_stream()
            .flat_map_ordered(q!(move |_| {
                queue_ref.extend(arrivals_ref.iter().cloned());
                (0..*pulses_ref)
                    .filter_map(|_| queue_ref.pop_front())
                    .collect::<Vec<_>>()
            }))
    };

    let responses = serviced
        .map(q!(|request| Response {
            id: request.id,
            value: request.value.to_uppercase(),
        }))
        .send(client, TCP.fail_stop().bincode().name("responses"));

    let completed = responses.clone().unique();
    let service_completion_events = responses
        .clone()
        .map(q!(|response| RetryEvent::ServiceCompletion(response.id)));
    let logical_completion_events = completed
        .clone()
        .map(q!(|response| RetryEvent::LogicalCompletion(response.id)));

    let retry_ticks = client.source_interval(q!(Duration::from_millis(timeout_millis)));
    let attempts_next_tick = sliced! {
        let new_requests = use::batch(requests, nondet!(
            /** Application arrivals may fall in any client tick. */
        ));
        let received = use::batch(responses, nondet!(
            /** A response may race with a retry deadline. Either outcome is a
             * normal execution: the service deduplicates completion below. */
        ));
        let timer_batch = use::batch(retry_ticks, nondet!(
            /** Wall-clock timer interrupts may fall in any client tick. */
        ));
        let mut outstanding = use::state(|location| {
            location.singleton(q!(std::collections::BTreeMap::<u64, Outstanding>::new()))
        });

        let tick = new_requests.location().clone();
        let new_vec = new_requests.fold(q!(|| Vec::new()), q!(|out, request| out.push(request)));
        let response_ids = received
            .map(q!(|response| response.id))
            .fold(
                q!(|| std::collections::BTreeSet::new()),
                q!(|ids, id| { ids.insert(id); }, commutative = manual_proof!(/** set insertion commutes */)),
            );
        let timer_fired = timer_batch.count();
        let state_ref = outstanding.by_mut();
        let new_ref = new_vec.by_ref();
        let completed_ref = response_ids.by_ref();
        let timer_ref = timer_fired.by_ref();

        tick.singleton(q!(()))
            .into_stream()
            .flat_map_unordered(q!(move |_| {
                for id in completed_ref.iter() {
                    state_ref.remove(id);
                }

                let now = Instant::now();
                let mut to_send = Vec::new();
                for request in new_ref.iter().cloned() {
                    state_ref.insert(request.id, Outstanding {
                        request: request.clone(),
                        retry_at: now + Duration::from_millis(timeout_millis),
                    });
                    to_send.push(request);
                }

                if *timer_ref > 0 {
                    for pending in state_ref.values_mut() {
                        if now >= pending.retry_at {
                            to_send.push(pending.request.clone());
                            pending.retry_at = now + Duration::from_millis(timeout_millis);
                        }
                    }
                }
                to_send
            }))
    };

    send_attempts.complete(attempts_next_tick);

    let events = logical_request_events
        .merge_unordered(physical_attempt_events)
        .merge_unordered(service_completion_events)
        .merge_unordered(logical_completion_events);

    TimeoutRetryOutputs {
        completed: completed.into(),
        events,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::pin::Pin;

    use dfir_lang::graph::{GraphNode, GraphNodeId};
    use futures::{SinkExt, Stream, StreamExt};
    use hydro_deploy::Deployment;
    use hydro_lang::telemetry::emf::RecordMetricsSidecar;

    use crate::stage_telemetry::{StageWindow, parse_stage_windows};
    use super::*;

    /// Measured shape: a stateful stage that emits network work in a window
    /// where it drained no ordinary handoff items. This describes what was
    /// observed; it is not, on its own, proof of a metastable hazard (ordinary
    /// queue drainage can also produce this shape). The surrounding
    /// ground-truth deployment separately establishes that these emissions are
    /// physical retry attempts.
    fn retained_state_stage_emits_network_without_ordinary_input(window: &StageWindow) -> bool {
        window.run_count > 0
            && (window.retained_state_reads > 0 || window.retained_state_writes > 0)
            && window.input_items == 0
            && window.network_message_count > 0
            && window.network_byte_count > 0
    }

    fn reaches_interval(
        graph: &dfir_lang::graph::DfirGraph,
        node_id: GraphNodeId,
        seen: &mut HashSet<GraphNodeId>,
    ) -> bool {
        if !seen.insert(node_id) {
            return false;
        }
        graph
            .operator_tag(node_id)
            .is_some_and(|tag| tag.starts_with("interval__"))
            || graph
                .node_predecessor_nodes(node_id)
                .any(|predecessor| reaches_interval(graph, predecessor, seen))
            || graph
                .node_handoff_references(node_id)
                .iter()
                .filter_map(|reference| reference.node_id)
                .any(|reference| reaches_interval(graph, reference, seen))
    }

    fn stage_records(path: &std::path::Path) -> Vec<StageWindow> {
        parse_stage_windows(&std::fs::read_to_string(path).unwrap())
    }

    async fn observe_for(
        events: &mut Pin<Box<dyn Stream<Item = RetryEvent>>>,
        duration: Duration,
    ) -> Vec<RetryEvent> {
        let deadline = tokio::time::Instant::now() + duration;
        let mut observed = Vec::new();
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break,
                event = events.next() => match event {
                    Some(event) => observed.push(event),
                    None => break,
                },
            }
        }
        observed
    }

    fn count(events: &[RetryEvent]) -> (usize, usize, usize, usize) {
        let mut logical = 0;
        let mut attempts = 0;
        let mut service = 0;
        let mut completed = 0;
        for event in events {
            match event {
                RetryEvent::LogicalRequest(_) => logical += 1,
                RetryEvent::PhysicalAttempt(_) => attempts += 1,
                RetryEvent::ServiceCompletion(_) => service += 1,
                RetryEvent::LogicalCompletion(_) => completed += 1,
            }
        }
        (logical, attempts, service, completed)
    }

    fn logical_completion_ids(events: &[RetryEvent]) -> std::collections::BTreeSet<u64> {
        events
            .iter()
            .filter_map(|event| match event {
                RetryEvent::LogicalCompletion(id) => Some(*id),
                _ => None,
            })
            .collect()
    }

    /// Real-time ground truth for weak metastability: the same baseline that is
    /// healthy from a clean start cannot recover after a finite burst because
    /// timeout retries keep the finite-rate service saturated. Removing organic
    /// input eventually drains the finite request population.
    #[test]
    fn optimized_graph_exposes_interval_feedback_and_retained_state() {
        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let service = flow.process::<Service>();
        let requests = client.source_iter(q!([Request {
            id: 1,
            value: "work".to_owned(),
        }]));
        let outputs = timeout_retry(&client, &service, requests.into(), 80, 20);
        outputs
            .completed
            .assume_ordering::<TotalOrder>(nondet!(/** only drives IR inspection */))
            .for_each(q!(|_| {}));
        outputs
            .events
            .assume_ordering::<TotalOrder>(nondet!(/** only drives IR inspection */))
            .for_each(q!(|_| {}));

        let mut built = flow
            .with_default_optimize::<hydro_lang::compile::embedded::EmbeddedDeploy>();
        let preview = built.preview_compile();
        let client_graph = preview.dfir_for(&client).unwrap();
        assert!(client_graph
            .node_ids()
            .any(|node_id| client_graph.operator_tag(node_id).is_some_and(|tag| tag.starts_with("interval__"))));
        let interval_stateful_stages: Vec<_> = client_graph
            .node_ids()
            .filter(|node_id| {
                client_graph
                    .node_handoff_references(*node_id)
                    .iter()
                    .any(|reference| reference.is_mut)
                    && reaches_interval(client_graph, *node_id, &mut HashSet::new())
            })
            .collect();
        assert_eq!(interval_stateful_stages.len(), 1);
        assert!(client_graph
            .nodes()
            .any(|(_, node)| matches!(node, GraphNode::Operator(op) if op.name_string() == "dest_sink")));
    }

    #[tokio::test]
    async fn stage_trace_observes_interval_driven_retained_work() {
        let trace_path = std::env::current_dir()
            .unwrap()
            .join(format!("target/retry-stage-trace-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&trace_path);
        let mut deployment = Deployment::new();
        let mut flow = FlowBuilder::new();
        let external = flow.external::<()>();
        let client = flow.process::<Client>();
        let service = flow.process::<Service>();
        let port = timeout_retry_external(&external, &client, &service, 40, 20);
        let sidecar = RecordMetricsSidecar::builder()
            .file_path(trace_path.to_string_lossy().into_owned())
            .interval(Duration::from_millis(100))
            .build();
        let nodes = flow
            .with_default_optimize()
            .with_process(&client, deployment.Localhost())
            .with_process(&service, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .with_sidecar_all(&sidecar)
            .deploy(&mut deployment);
        deployment.deploy().await.unwrap();
        let (_events, mut requests) = nodes.connect_bincode(port).await;
        deployment.start().await.unwrap();

        for id in 0..20 {
            requests
                .send(Request {
                    id,
                    value: "work".repeat(16),
                })
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(450)).await;
        deployment.stop().await.unwrap();

        let records = stage_records(&trace_path);
        assert!(!records.is_empty(), "stage sidecar must emit activation windows");
        assert!(records.iter().any(|record| {
            record.has_interval_source && record.output_items > 0
        }));
        // Directly observe the measured shape: at least one retained-state
        // stage emitted physical network work in a window with no ordinary
        // handoff input. This is the raw evidence; the hazard interpretation is
        // argued in the design doc, not asserted by a reusable predicate.
        let retained_state_windows: Vec<_> = records
            .iter()
            .filter(|window| retained_state_stage_emits_network_without_ordinary_input(window))
            .collect();
        assert!(
            !retained_state_windows.is_empty(),
            "expected at least one retained-state stage emitting network work without ordinary input"
        );
        eprintln!("RETRY_RETAINED_STATE_WINDOWS {retained_state_windows:?}");
    }

    #[tokio::test]
    async fn finite_burst_enters_weakly_metastable_regime() {
        const TIMEOUT_MS: u64 = 80;
        const SERVICE_MS: u64 = 20;
        const BASELINE_SPACING_MS: u64 = 120;

        let mut deployment = Deployment::new();
        let mut flow = FlowBuilder::new();
        let external = flow.external::<()>();
        let client = flow.process::<Client>();
        let service = flow.process::<Service>();
        let port = timeout_retry_external(
            &external,
            &client,
            &service,
            TIMEOUT_MS,
            SERVICE_MS,
        );
        let nodes = flow
            .with_default_optimize()
            .with_process(&client, deployment.Localhost())
            .with_process(&service, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);
        deployment.deploy().await.unwrap();
        let (mut events, mut requests) = nodes.connect_bincode(port).await;
        deployment.start().await.unwrap();

        // Warm-up: roughly 8 requests/s into a 50 requests/s service.
        for id in 0..8 {
            requests
                .send(Request {
                    id,
                    value: "ground truth".repeat(16),
                })
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(BASELINE_SPACING_MS)).await;
        }
        let warm = observe_for(&mut events, Duration::from_millis(300)).await;
        let (warm_logical, warm_attempts, _, warm_completed) = count(&warm);
        assert_eq!(warm_logical, 8);
        assert_eq!(warm_completed, 8);
        assert_eq!(warm_attempts, 8, "clean baseline must not retry");

        // Finite trigger: an instantaneous burst larger than service capacity
        // over one timeout interval.
        for id in 8..38 {
            requests
                .send(Request {
                    id,
                    value: "trigger".repeat(16),
                })
                .await
                .unwrap();
        }
        let trigger = observe_for(&mut events, Duration::from_millis(300)).await;

        // Restore the exact warm-up arrival rate. In the bad regime physical
        // work must exceed fresh logical work and completions must lag arrivals.
        for id in 38..50 {
            requests
                .send(Request {
                    id,
                    value: "ground truth".repeat(16),
                })
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(BASELINE_SPACING_MS)).await;
        }
        let post = observe_for(&mut events, Duration::from_millis(300)).await;
        let (post_logical, post_attempts, _, _post_completed) = count(&post);
        assert_eq!(post_logical, 12);
        assert!(post_attempts > post_logical * 2, "retries must amplify work");
        let post_baseline_completions = logical_completion_ids(&post)
            .into_iter()
            .filter(|id| (38..50).contains(id))
            .count();
        assert!(
            post_baseline_completions < post_logical,
            "restored-baseline requests must lag their arrival rate"
        );

        // Stop organic input. Weak (rather than strong) metastability requires
        // eventual drain: after all 50 logical requests complete, a later quiet
        // window contains no newly generated attempts.
        let drain = observe_for(&mut events, Duration::from_secs(8)).await;
        let mut all_after_warm = logical_completion_ids(&trigger);
        all_after_warm.extend(logical_completion_ids(&post));
        all_after_warm.extend(logical_completion_ids(&drain));
        assert!(
            (8..50).all(|id| all_after_warm.contains(&id)),
            "every triggered and post-trigger request must eventually complete"
        );
        let quiet = observe_for(&mut events, Duration::from_millis(300)).await;
        let (_, quiet_attempts, _, _) = count(&quiet);
        assert_eq!(quiet_attempts, 0, "finite population must eventually drain");

        // Restore the original baseline after the zero-arrival anti-trigger.
        for id in 50..58 {
            requests
                .send(Request {
                    id,
                    value: "ground truth".repeat(16),
                })
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(BASELINE_SPACING_MS)).await;
        }
        let recovered = observe_for(&mut events, Duration::from_millis(300)).await;
        let (logical, attempts, service, completed) = count(&recovered);
        assert_eq!((logical, attempts, service, completed), (8, 8, 8, 8));
    }

    /// Paired control: the same baseline stays healthy without a preceding burst.
    #[tokio::test]
    async fn baseline_control_stays_healthy() {
        const TIMEOUT_MS: u64 = 80;
        const SERVICE_MS: u64 = 20;
        const BASELINE_SPACING_MS: u64 = 120;

        let mut deployment = Deployment::new();
        let mut flow = FlowBuilder::new();
        let external = flow.external::<()>();
        let client = flow.process::<Client>();
        let service = flow.process::<Service>();
        let port = timeout_retry_external(
            &external,
            &client,
            &service,
            TIMEOUT_MS,
            SERVICE_MS,
        );
        let nodes = flow
            .with_default_optimize()
            .with_process(&client, deployment.Localhost())
            .with_process(&service, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);
        deployment.deploy().await.unwrap();
        let (mut events, mut requests) = nodes.connect_bincode(port).await;
        deployment.start().await.unwrap();

        for id in 0..12 {
            requests
                .send(Request {
                    id,
                    value: "ground truth".repeat(16),
                })
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(BASELINE_SPACING_MS)).await;
        }
        let control = observe_for(&mut events, Duration::from_millis(300)).await;
        let (logical, attempts, service, completed) = count(&control);
        assert_eq!((logical, attempts, service, completed), (12, 12, 12, 12));
    }
}
