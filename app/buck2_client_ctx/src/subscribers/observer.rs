/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

/// A trait for such event subscribers that are watching a specific set of
/// errors and keeping the record of them for later use.
pub trait ErrorObserver {
    /// Whether this observer thinks that the daemon needs killing to work again.
    fn daemon_in_memory_state_is_corrupted(&self) -> bool {
        false
    }

    /// Whether this observer thinks that the daemon needs to dump its materializer state to work
    /// again.
    fn daemon_materializer_state_is_corrupted(&self) -> bool {
        false
    }

    fn restarter_is_enabled(&self) -> bool {
        false
    }

    /// Whether the daemon reported this command's failure as recoverable by retrying the same
    /// command against the same daemon.
    fn recoverable_by_command_retry(&self) -> bool {
        false
    }

    /// Whether the daemon has opted into automatically retrying a command whose failure it
    /// reports as recoverable by retry.
    fn command_retry_on_recoverable_failure_is_enabled(&self) -> bool {
        false
    }

    /// The number of automatic same-daemon retries the client may perform for one invocation
    /// before treating a recoverable failure as final.
    fn max_command_retries_on_recoverable_failure(&self) -> u32 {
        0
    }
}
