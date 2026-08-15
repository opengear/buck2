/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Recovery for an `execute_with_progress` stream severed by a proxy or backend restart.
//!
//! A rolling upgrade of an intermediate proxy or of the RE backend itself can sever the
//! long-lived HTTP/2 stream carrying `Execute` progress while the RE operation keeps running on
//! the backend. [`classify`] inspects a failed [`tonic::Status`] structurally, walking its
//! `source()` chain for an `h2` or `io` error, to separate that severance from a status the
//! server returned deliberately. [`ReattachState::recover`] reattaches to the surviving
//! operation with `WaitExecution`, or re-issues `Execute` when the operation name is unknown or
//! the backend has forgotten it, bounded by a wall-clock budget that fails a dead endpoint.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::stream::BoxStream;
use rand::random_range;
use re_grpc_proto::build::bazel::remote::execution::v2::ExecuteRequest as GExecuteRequest;
use re_grpc_proto::google::longrunning::Operation;
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::response::ExecuteReattachStats;

const EXECUTE_REATTACH_BACKOFF_BASE: Duration = Duration::from_millis(250);
const EXECUTE_REATTACH_BACKOFF_CAP: Duration = Duration::from_secs(10);

/// Which RPC produced the stream currently being read.
///
/// A `NOT_FOUND` status means different things depending on which call reported it: a
/// `WaitExecution` that returns `NOT_FOUND` means the backend lost the operation and a fresh
/// `Execute` recovers it; an `Execute` that returns `NOT_FOUND` is a genuine rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamOrigin {
    Execute,
    WaitExecution,
}

/// The classified reason a severed execute stream is recoverable by reattaching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryCause {
    /// The transport closed underneath an in-flight RPC: a TCP reset, a TLS `close_notify`, or
    /// an HTTP/2 connection torn down mid-shutdown.
    Io,
    /// The peer sent an HTTP/2 GOAWAY with reason `NO_ERROR`.
    GracefulGoAway,
    /// The peer refused a new stream on a connection it is shutting down.
    RefusedStream,
    /// The RE endpoint refused a new connection.
    ConnectionRefused,
    /// The stream ended cleanly before a terminal response arrived.
    CleanEof,
    /// `WaitExecution` reported that the operation no longer exists.
    OperationNotFound,
}

/// Inspects a `tonic::Status` for a structural signal that the RPC failed because the
/// connection was severed rather than because the server rejected the request.
///
/// Classification walks the error's `source()` chain rather than branching on `status.code()`:
/// tonic maps a severed TLS-passthrough connection to `Code::Unknown` and an HTTP/2 GOAWAY to
/// `Code::Internal`, codes a server can also return deliberately. `NOT_FOUND` is the one code
/// inspected directly, since REAPI defines its meaning for `WaitExecution`: the operation is
/// gone and the client should call `Execute`.
pub(crate) fn classify(status: &tonic::Status, origin: StreamOrigin) -> Option<RetryCause> {
    if status.code() == tonic::Code::NotFound {
        return match origin {
            StreamOrigin::WaitExecution => Some(RetryCause::OperationNotFound),
            StreamOrigin::Execute => None,
        };
    }

    let mut source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(status);
    while let Some(err) = source {
        // This downcast succeeds only when this crate's `h2` and tonic's `h2` resolve to the
        // same compiled type. A single `h2` version in `Cargo.lock` keeps them unified; if that
        // ever splits, this downcast silently returns `None` and severed connections stop being
        // retried.
        if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
            return if h2_err.is_io() {
                Some(RetryCause::Io)
            } else {
                match h2_err.reason() {
                    Some(h2::Reason::NO_ERROR) => Some(RetryCause::GracefulGoAway),
                    Some(h2::Reason::REFUSED_STREAM) => Some(RetryCause::RefusedStream),
                    _ => None,
                }
            };
        }

        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return match io_err.kind() {
                std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::NotConnected
                | std::io::ErrorKind::TimedOut => Some(RetryCause::Io),
                std::io::ErrorKind::ConnectionRefused => Some(RetryCause::ConnectionRefused),
                _ => None,
            };
        }

        source = err.source();
    }

    None
}

/// Reports whether a status is a `WaitExecution` call answered with `UNIMPLEMENTED`, which
/// describes the backend rather than a transient failure. REAPI does not guarantee a server
/// implements `WaitExecution`, and `ExecutionCapabilities` advertises nothing a client can probe
/// ahead of time, so the call itself is the only discovery mechanism.
fn is_wait_execution_unimplemented(status: &tonic::Status, origin: StreamOrigin) -> bool {
    origin == StreamOrigin::WaitExecution && status.code() == tonic::Code::Unimplemented
}

/// Upper bound of the full-jitter backoff window for a given attempt count:
/// `250ms * 2^(attempt-1)`, capped at 10 seconds. The delay slept is sampled uniformly from
/// `[0, bound)`.
fn jittered_backoff_bound(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(u32::BITS - 1);
    EXECUTE_REATTACH_BACKOFF_BASE
        .saturating_mul(1u32 << exponent)
        .min(EXECUTE_REATTACH_BACKOFF_CAP)
}

/// Samples a full-jitter backoff delay for the given attempt count. `attempt` resets on any
/// received message, so a long action severed once per replica during a sequential rolling
/// upgrade pays close to the base delay each time.
fn sample_jittered_backoff(attempt: u32) -> Duration {
    let bound = jittered_backoff_bound(attempt);
    if bound == Duration::ZERO {
        return Duration::ZERO;
    }
    random_range(Duration::ZERO..bound)
}

type ReattachStream = BoxStream<'static, Result<Operation, tonic::Status>>;

/// Drives recovery of a severed `execute_with_progress` stream.
///
/// Owns the current stream and everything needed to reattach to it: the operation name last
/// seen, the wall-clock budget and backoff state, the shared limiter bounding concurrent
/// reattach dials, the shared latch a `WaitExecution` `UNIMPLEMENTED` sets for the client's
/// remaining lifetime, and cumulative counters for observability.
pub(crate) struct ReattachState<F, Fut, WF, WFut>
where
    F: Fn(GExecuteRequest) -> Fut,
    Fut: Future<Output = anyhow::Result<ReattachStream>> + Send + 'static,
    WF: Fn(String) -> WFut,
    WFut: Future<Output = anyhow::Result<ReattachStream>> + Send + 'static,
{
    execute_f: F,
    wait_execution_f: WF,
    request: GExecuteRequest,
    pub(crate) stream: ReattachStream,
    pub(crate) origin: StreamOrigin,
    op_name: Option<String>,
    pub(crate) seen_done: bool,
    // `tokio::time::Instant` rather than `std::time::Instant`: the severance budget tracks the
    // same clock as the backoff sleep, so a test that pauses and advances tokio's clock
    // exercises the budget deterministically.
    last_progress: Instant,
    attempt: u32,
    // `None` disables reattach: every severance propagates its trigger unmodified.
    budget: Option<Duration>,
    limiter: Arc<Semaphore>,
    wait_execution_unimplemented: Arc<AtomicBool>,
    stats: ExecuteReattachStats,
    first_break: Option<anyhow::Error>,
}

impl<F, Fut, WF, WFut> ReattachState<F, Fut, WF, WFut>
where
    F: Fn(GExecuteRequest) -> Fut,
    Fut: Future<Output = anyhow::Result<ReattachStream>> + Send + 'static,
    WF: Fn(String) -> WFut,
    WFut: Future<Output = anyhow::Result<ReattachStream>> + Send + 'static,
{
    pub(crate) fn new(
        execute_f: F,
        wait_execution_f: WF,
        request: GExecuteRequest,
        stream: ReattachStream,
        budget: Option<Duration>,
        limiter: Arc<Semaphore>,
        wait_execution_unimplemented: Arc<AtomicBool>,
    ) -> Self {
        Self {
            execute_f,
            wait_execution_f,
            request,
            stream,
            origin: StreamOrigin::Execute,
            op_name: None,
            seen_done: false,
            last_progress: Instant::now(),
            attempt: 0,
            budget,
            limiter,
            wait_execution_unimplemented,
            stats: ExecuteReattachStats::default(),
            first_break: None,
        }
    }

    /// A snapshot of cumulative reattach counters, attached to every response yielded to the
    /// consumer.
    pub(crate) fn stats(&self) -> ExecuteReattachStats {
        self.stats
    }

    /// The budget in effect for the next reattach: `None` if reattach is disabled by
    /// configuration, or if a severance on this client already discovered the backend does not
    /// implement `WaitExecution`.
    fn effective_budget(&self) -> Option<Duration> {
        if self.wait_execution_unimplemented.load(Ordering::Relaxed) {
            return None;
        }
        self.budget
    }

    /// Reports whether reattach is disabled. While disabled, every severance propagates its
    /// trigger error unmodified.
    pub(crate) fn is_disabled(&self) -> bool {
        self.effective_budget().is_none()
    }

    /// Records progress from a message received on the current stream. Captures the operation
    /// name, which every message refreshes because a re-issued `Execute` yields a new
    /// operation, and resets the severance budget and the backoff counter.
    pub(crate) fn observe_message(&mut self, msg: &Operation) {
        if !msg.name.is_empty() {
            self.op_name = Some(msg.name.clone());
        }
        self.last_progress = Instant::now();
        self.attempt = 0;
        self.first_break = None;
        self.seen_done = msg.done;
    }

    /// Recovers a severed execute stream. Once the backend has named the operation, reattaches
    /// to it via `WaitExecution`; a severance that arrives first re-issues `Execute`. REAPI
    /// permits a server to execute an action more than once and does not require it to
    /// deduplicate in-flight actions by digest, so re-issuing `Execute` runs the action a second
    /// time against a server that deduplicates nothing.
    ///
    /// The budget clock starts when this severance begins, not at the stream's last progress:
    /// detecting a severance can take as long as a TCP keepalive cycle, and that detection
    /// latency is not charged against the budget.
    ///
    /// If the reattach call itself fails with a retryable cause, retries it in place, bounded
    /// by the same severance budget. Replaces `self.stream` and returns on success. Returns an
    /// error identifying the original cause once the budget is exhausted, or immediately when
    /// the reattach call fails with a non-retryable cause or reattach is disabled. A
    /// `WaitExecution` call answered with `UNIMPLEMENTED` latches reattach off for every later
    /// severance on this client.
    pub(crate) async fn recover(
        &mut self,
        mut cause: RetryCause,
        trigger: anyhow::Error,
    ) -> anyhow::Result<()> {
        let Some(budget) = self.effective_budget() else {
            return Err(trigger);
        };

        self.apply_cause(cause);
        if self.first_break.is_none() {
            self.first_break = Some(trigger);
            self.last_progress = Instant::now();
        }

        loop {
            let elapsed = self.last_progress.elapsed();
            if elapsed >= budget {
                return Err(self.budget_exceeded(budget, elapsed));
            }

            self.attempt += 1;
            self.bump_cause_stat(cause);

            let backoff = sample_jittered_backoff(self.attempt);

            tracing::debug!(
                action_digest_hash = self.action_digest_hash(),
                op_name = self.op_name.as_deref().unwrap_or(""),
                cause = ?cause,
                attempt = self.attempt,
                elapsed_secs = elapsed.as_secs_f64(),
                backoff_secs = backoff.as_secs_f64(),
                "Recovering severed RE execute stream",
            );

            tokio::time::sleep(backoff).await;

            // The permit is held only across this call: acquired after the backoff sleep so the
            // limiter does not serialize the jitter, and dropped at the end of this block so a
            // long-lived reattached stream does not hold a permit for its own lifetime.
            let (origin, outcome) = {
                let _permit = self
                    .limiter
                    .acquire()
                    .await
                    .expect("reattach limiter is never closed");
                match self.op_name.clone() {
                    Some(name) => (
                        StreamOrigin::WaitExecution,
                        (self.wait_execution_f)(name).await,
                    ),
                    None => (
                        StreamOrigin::Execute,
                        (self.execute_f)(self.request.clone()).await,
                    ),
                }
            };

            match outcome {
                Ok(stream) => {
                    self.stream = stream;
                    self.origin = origin;
                    match origin {
                        StreamOrigin::WaitExecution => self.stats.wait_execution_reattaches += 1,
                        StreamOrigin::Execute => self.stats.re_executes += 1,
                    }
                    return Ok(());
                }
                Err(err) => {
                    let dial_status = err.downcast_ref::<tonic::Status>();
                    let reclassified = dial_status.and_then(|status| classify(status, origin));
                    match reclassified {
                        Some(next_cause) => {
                            cause = next_cause;
                            self.apply_cause(cause);
                        }
                        None => {
                            if dial_status.is_some_and(|status| {
                                is_wait_execution_unimplemented(status, origin)
                            }) {
                                self.wait_execution_unimplemented
                                    .store(true, Ordering::Relaxed);
                            }

                            let rpc = match origin {
                                StreamOrigin::WaitExecution => "WaitExecution",
                                StreamOrigin::Execute => "Execute",
                            };
                            let dial_failure_context = format!("{err:#}");
                            let reattach_context = format!(
                                "Reattaching RE execute stream via {rpc} for {}",
                                self.resource_context()
                            );
                            return Err(match self.first_break.take() {
                                Some(severance) => severance
                                    .context(dial_failure_context)
                                    .context(reattach_context),
                                None => err.context(reattach_context),
                            });
                        }
                    }
                }
            }
        }
    }

    /// `NOT_FOUND` from `WaitExecution` means the backend has forgotten the operation, so the
    /// next reattach re-`Execute`s rather than waiting on a name the backend no longer
    /// resolves.
    fn apply_cause(&mut self, cause: RetryCause) {
        if cause == RetryCause::OperationNotFound {
            self.op_name = None;
        }
    }

    fn bump_cause_stat(&mut self, cause: RetryCause) {
        match cause {
            RetryCause::Io => self.stats.severed_io += 1,
            RetryCause::GracefulGoAway | RetryCause::RefusedStream => {
                self.stats.severed_goaway += 1
            }
            RetryCause::ConnectionRefused => self.stats.dial_failures += 1,
            RetryCause::CleanEof => self.stats.clean_eof += 1,
            RetryCause::OperationNotFound => self.stats.operation_not_found += 1,
        }
    }

    fn action_digest_hash(&self) -> &str {
        self.request
            .action_digest
            .as_ref()
            .map(|digest| digest.hash.as_str())
            .unwrap_or("")
    }

    fn resource_context(&self) -> String {
        format!(
            "action `{}`, operation `{}`",
            self.action_digest_hash(),
            self.op_name.as_deref().unwrap_or("<unknown>")
        )
    }

    fn budget_exceeded(&mut self, budget: Duration, elapsed: Duration) -> anyhow::Error {
        tracing::warn!(
            action_digest_hash = self.action_digest_hash(),
            op_name = self.op_name.as_deref().unwrap_or(""),
            attempts = self.attempt,
            elapsed_secs = elapsed.as_secs_f64(),
            budget_secs = budget.as_secs_f64(),
            "Exceeding RE execute reattach budget",
        );

        let context = format!(
            "Exceeding RE execute reattach budget of {:?} after {} attempt(s) and {:?} for {}",
            budget,
            self.attempt,
            elapsed,
            self.resource_context()
        );
        match self.first_break.take() {
            Some(err) => err.context(context),
            None => anyhow::anyhow!(context),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use super::*;

    fn h2_status(err: h2::Error) -> tonic::Status {
        tonic::Status::from_error(Box::new(err))
    }

    fn io_status(kind: std::io::ErrorKind) -> tonic::Status {
        tonic::Status::from_error(Box::new(std::io::Error::from(kind)))
    }

    // `h2::Error`'s IO-kind constructor is crate-private; `classifies_io_disconnect_kinds_as_retryable`
    // below covers the plain `std::io::Error` that hyper unwraps it into before tonic sees it.

    #[test]
    fn classifies_goaway_no_error_as_retryable() {
        let status = h2_status(h2::Error::from(h2::Reason::NO_ERROR));
        assert_eq!(
            classify(&status, StreamOrigin::Execute),
            Some(RetryCause::GracefulGoAway)
        );
    }

    #[test]
    fn classifies_refused_stream_as_retryable() {
        let status = h2_status(h2::Error::from(h2::Reason::REFUSED_STREAM));
        assert_eq!(
            classify(&status, StreamOrigin::Execute),
            Some(RetryCause::RefusedStream)
        );
    }

    #[test]
    fn classifies_enhance_your_calm_as_fatal() {
        let status = h2_status(h2::Error::from(h2::Reason::ENHANCE_YOUR_CALM));
        assert_eq!(classify(&status, StreamOrigin::Execute), None);
    }

    #[test]
    fn classifies_other_h2_reasons_as_fatal() {
        for reason in [
            h2::Reason::PROTOCOL_ERROR,
            h2::Reason::CANCEL,
            h2::Reason::INTERNAL_ERROR,
            h2::Reason::FLOW_CONTROL_ERROR,
        ] {
            let status = h2_status(h2::Error::from(reason));
            assert_eq!(
                classify(&status, StreamOrigin::Execute),
                None,
                "reason: {reason:?}"
            );
        }
    }

    #[test]
    fn classifies_io_disconnect_kinds_as_retryable() {
        for kind in [
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::ConnectionAborted,
            std::io::ErrorKind::BrokenPipe,
            std::io::ErrorKind::UnexpectedEof,
            std::io::ErrorKind::NotConnected,
            std::io::ErrorKind::TimedOut,
        ] {
            let status = io_status(kind);
            assert_eq!(
                classify(&status, StreamOrigin::Execute),
                Some(RetryCause::Io),
                "kind: {kind:?}"
            );
        }
    }

    #[test]
    fn classifies_connection_refused_as_retryable() {
        let status = io_status(std::io::ErrorKind::ConnectionRefused);
        assert_eq!(
            classify(&status, StreamOrigin::Execute),
            Some(RetryCause::ConnectionRefused)
        );
    }

    #[test]
    fn classifies_unrelated_io_kind_as_fatal() {
        let status = io_status(std::io::ErrorKind::InvalidData);
        assert_eq!(classify(&status, StreamOrigin::Execute), None);
    }

    #[test]
    fn classifies_plain_status_with_no_source_as_fatal() {
        let status = tonic::Status::unavailable("backend is down");
        assert_eq!(classify(&status, StreamOrigin::Execute), None);
        let status = tonic::Status::internal("internal error");
        assert_eq!(classify(&status, StreamOrigin::Execute), None);
        let status = tonic::Status::unknown("unknown error");
        assert_eq!(classify(&status, StreamOrigin::Execute), None);
    }

    #[test]
    fn classifies_not_found_by_origin() {
        let status = tonic::Status::not_found("operation gone");
        assert_eq!(
            classify(&status, StreamOrigin::WaitExecution),
            Some(RetryCause::OperationNotFound)
        );
        assert_eq!(classify(&status, StreamOrigin::Execute), None);
    }

    #[test]
    fn jittered_backoff_bound_grows_and_caps() {
        assert_eq!(jittered_backoff_bound(1), Duration::from_millis(250));
        assert_eq!(jittered_backoff_bound(2), Duration::from_millis(500));
        assert_eq!(jittered_backoff_bound(3), Duration::from_millis(1000));
        assert_eq!(jittered_backoff_bound(6), Duration::from_secs(8));
        assert_eq!(jittered_backoff_bound(7), EXECUTE_REATTACH_BACKOFF_CAP);
        assert_eq!(jittered_backoff_bound(100), EXECUTE_REATTACH_BACKOFF_CAP);
    }

    #[test]
    fn sampled_backoff_stays_within_bound() {
        for attempt in 1..20 {
            let bound: Range<Duration> = Duration::ZERO..jittered_backoff_bound(attempt);
            for _ in 0..50 {
                let sample = sample_jittered_backoff(attempt);
                assert!(
                    bound.contains(&sample),
                    "attempt {attempt} sampled {sample:?} outside {bound:?}"
                );
            }
        }
    }
}
