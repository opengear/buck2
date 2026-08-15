/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use crate::daemon::client::BuckdClientConnector;
use crate::daemon::client::connect::DaemonConstraintsRequest;
use crate::events_ctx::EventsCtx;
use crate::subscribers::observer::ErrorObserver;

/// Monitor the state of our execution and decide whether we should restart the command we just
/// attempted to execute.
///
/// Two independent reasons can trigger a restart, and `apply_to_constraints` treats them
/// differently. `reject_daemon` and `reject_materializer_state` ask the next connection to reject
/// the daemon that just failed and start a fresh one, discarding all of its in-memory and
/// materializer state. `command_retries_used` tracks a same-daemon retry instead: the daemon
/// itself already addressed the cause of the failure, so the next connection is left free to
/// reuse the daemon that just ran, and only the command re-executes.
#[derive(Default)]
pub struct Restarter {
    pub reject_daemon: Option<String>,
    pub reject_materializer_state: Option<String>,
    pub enable_restarter: bool,
    /// True for the invocation immediately following an observed failure that the daemon
    /// reported as recoverable by retry, while this invocation's retry budget still has room.
    /// `observe` recomputes this on every call, so it reflects only the most recently observed
    /// command, never a stale command from further back in the restart chain.
    retry_same_daemon: bool,
    /// Same-daemon retries already spent across every restart of this invocation chain, charged
    /// the moment a retry is armed rather than when it runs.
    command_retries_used: u32,
}

impl Restarter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Observe our BuckdClientConnector after execution to decide whether we should be
    /// restarting.
    pub fn observe(&mut self, client: &BuckdClientConnector, events_ctx: &mut EventsCtx) {
        for obs in events_ctx.error_observers() {
            if obs.daemon_in_memory_state_is_corrupted() {
                self.reject_daemon = Some(client.daemon_constraints().daemon_id.clone());
            }

            if obs.daemon_materializer_state_is_corrupted() {
                self.reject_materializer_state = client
                    .daemon_constraints()
                    .extra
                    .as_ref()
                    .and_then(|e| e.materializer_state_identity.clone());
            }

            if obs.restarter_is_enabled() {
                self.enable_restarter = true;
            }

            self.observe_command_retry(obs);
        }

        if self.should_restart() {
            events_ctx.handle_should_restart();
        }
    }

    /// Decides whether the command just observed warrants a same-daemon retry, independent of
    /// the daemon-rejecting reasons `observe` also checks.
    ///
    /// A retry is armed only when the daemon reported the failure as recoverable by retry, the
    /// daemon has opted into automatic retries for that reason, and this invocation's budget for
    /// the reason has not already been spent. The budget is charged as soon as a retry is armed,
    /// so a budget of one permits exactly one restart for this reason across the whole
    /// invocation chain, however many times `observe` runs.
    fn observe_command_retry(&mut self, obs: &dyn ErrorObserver) {
        self.retry_same_daemon = obs.command_retry_on_recoverable_failure_is_enabled()
            && obs.recoverable_by_command_retry()
            && self.command_retries_used < obs.max_command_retries_on_recoverable_failure();

        if self.retry_same_daemon {
            self.command_retries_used += 1;
        }
    }

    /// True if a fresh daemon is warranted: `enable_restarter` is on and either reject reason is
    /// set. The caller gives this reason exactly one restart per invocation chain: a fresh daemon
    /// is expected to resolve its own corruption in that one attempt.
    pub fn daemon_rejecting_restart_wanted(&self) -> bool {
        self.enable_restarter
            && (self.reject_daemon.is_some() || self.reject_materializer_state.is_some())
    }

    /// True if the command just observed armed a same-daemon retry that is still within budget.
    /// See `observe_command_retry` for how the budget is spent.
    pub fn command_retry_wanted(&self) -> bool {
        self.retry_same_daemon
    }

    pub fn should_restart(&self) -> bool {
        self.daemon_rejecting_restart_wanted() || self.command_retry_wanted()
    }

    /// Fills in the constraints for the connection the retry will use.
    ///
    /// A same-daemon retry leaves both fields unset: `reject_daemon` and
    /// `reject_materializer_state` are set only by the daemon-rejecting reasons above, and a
    /// same-daemon retry never sets them, so the next connection accepts the daemon that just
    /// ran, keeping its in-memory state intact.
    pub fn apply_to_constraints(&self, req: &mut DaemonConstraintsRequest) {
        req.reject_daemon = self.reject_daemon.clone();
        req.reject_materializer_state = self.reject_materializer_state.clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct FakeErrorObserver {
        recoverable_by_command_retry: bool,
        command_retry_enabled: bool,
        max_command_retries: u32,
    }

    impl ErrorObserver for FakeErrorObserver {
        fn recoverable_by_command_retry(&self) -> bool {
            self.recoverable_by_command_retry
        }

        fn command_retry_on_recoverable_failure_is_enabled(&self) -> bool {
            self.command_retry_enabled
        }

        fn max_command_retries_on_recoverable_failure(&self) -> u32 {
            self.max_command_retries
        }
    }

    fn recoverable_failure(max_command_retries: u32) -> FakeErrorObserver {
        FakeErrorObserver {
            recoverable_by_command_retry: true,
            command_retry_enabled: true,
            max_command_retries,
        }
    }

    #[test]
    fn recoverable_failure_with_budget_arms_a_retry() {
        let mut restarter = Restarter::new();
        restarter.observe_command_retry(&recoverable_failure(1));
        assert!(restarter.should_restart());
    }

    #[test]
    fn recoverable_failure_leaves_reject_fields_unset() {
        let mut restarter = Restarter::new();
        restarter.observe_command_retry(&recoverable_failure(1));

        // apply_to_constraints copies these two fields verbatim, so asserting on them here is
        // exactly what a caller of apply_to_constraints would observe: the next connection
        // accepts the daemon that just ran instead of rejecting it.
        assert_eq!(restarter.reject_daemon, None);
        assert_eq!(restarter.reject_materializer_state, None);
    }

    #[test]
    fn unrecoverable_failure_does_not_arm_a_retry() {
        let mut restarter = Restarter::new();
        restarter.observe_command_retry(&FakeErrorObserver {
            recoverable_by_command_retry: false,
            command_retry_enabled: true,
            max_command_retries: 1,
        });
        assert!(!restarter.should_restart());
    }

    #[test]
    fn recoverable_failure_with_retry_disabled_does_not_arm_a_retry() {
        let mut restarter = Restarter::new();
        restarter.observe_command_retry(&FakeErrorObserver {
            recoverable_by_command_retry: true,
            command_retry_enabled: false,
            max_command_retries: 1,
        });
        assert!(!restarter.should_restart());
    }

    #[test]
    fn budget_of_zero_never_arms_a_retry() {
        let mut restarter = Restarter::new();
        restarter.observe_command_retry(&recoverable_failure(0));
        assert!(!restarter.should_restart());
    }

    #[test]
    fn exhausting_the_budget_stops_arming_further_retries() {
        let mut restarter = Restarter::new();

        restarter.observe_command_retry(&recoverable_failure(1));
        assert!(
            restarter.should_restart(),
            "first retry stays within budget"
        );

        restarter.observe_command_retry(&recoverable_failure(1));
        assert!(
            !restarter.should_restart(),
            "budget of one is already spent by the first retry"
        );
    }

    #[test]
    fn a_later_success_disarms_an_armed_retry() {
        let mut restarter = Restarter::new();
        restarter.observe_command_retry(&recoverable_failure(2));
        assert!(restarter.should_restart());

        // The retried command succeeded: the next observed command reports nothing recoverable,
        // and the stale arming from the previous command must not persist.
        restarter.observe_command_retry(&FakeErrorObserver::default());
        assert!(!restarter.should_restart());
    }

    #[test]
    fn command_retry_reason_composes_with_daemon_rejection() {
        let mut restarter = Restarter::new();
        restarter.reject_daemon = Some("daemon-id".to_owned());
        restarter.enable_restarter = true;

        // A same-daemon retry is independent of the daemon-rejecting reason: observing a command
        // with nothing recoverable must not clear a daemon rejection another observation set.
        restarter.observe_command_retry(&FakeErrorObserver::default());
        assert!(restarter.should_restart());
        assert_eq!(restarter.reject_daemon, Some("daemon-id".to_owned()));
    }
}
