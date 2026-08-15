/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::fmt::Write;

use buck2_core::category::CategoryRef;
use buck2_core::deferred::base_deferred_key::BaseDeferredKey;
use buck2_core::fs::buck_out_path::BuckOutScratchPath;
use buck2_data::ToProtoMessage;
use buck2_execute::execute::target::CommandExecutionTarget;
use derivative::Derivative;
use dupe::Dupe;

use crate::actions::RegisteredAction;
use crate::actions::cas_missing_recovery::CasRecoveryBatch;

/// Indicates why we are executing a given command.
#[derive(Clone, Dupe, Derivative)]
#[derivative(Debug)]
pub struct ActionExecutionTarget<'a> {
    action: &'a RegisteredAction,
    #[derivative(Debug = "ignore")]
    cas_recovery_batch: CasRecoveryBatch,
}

impl<'a> ActionExecutionTarget<'a> {
    pub(crate) fn new(action: &'a RegisteredAction, cas_recovery_batch: CasRecoveryBatch) -> Self {
        ActionExecutionTarget {
            action,
            cas_recovery_batch,
        }
    }

    pub fn owner(&self) -> &'a BaseDeferredKey {
        self.action.owner()
    }

    pub fn category(&self) -> CategoryRef<'a> {
        self.action.category()
    }

    pub fn identifier(&self) -> Option<&'a str> {
        self.action.identifier()
    }

    pub fn scratch_path(&self) -> BuckOutScratchPath {
        BuckOutScratchPath::new(
            self.action.owner().dupe(),
            self.action.category(),
            self.action.identifier(),
            self.action.action_key(),
            self.action.all_outputs_are_content_based(),
        )
        .unwrap()
    }

    /// Whether this action's execution must bypass every cache lookup — the action cache, the RE
    /// cache, and the remote and local dep-file caches — and execute unconditionally.
    ///
    /// CAS-missing recovery invalidated this action for repair: any cached result would hand
    /// back the digest that was reported missing, making a cache-served re-run a no-op instead
    /// of a repair.
    pub fn should_skip_cache_lookup(&self) -> bool {
        self.cas_recovery_batch.contains(self.action.key())
    }
}

impl CommandExecutionTarget for ActionExecutionTarget<'_> {
    fn re_action_key(&self) -> String {
        let mut key = String::new();
        write!(
            &mut key,
            "{} {}",
            self.action.owner(),
            self.action.category()
        )
        .unwrap();
        if let Some(ident) = self.action.identifier().as_ref() {
            write!(&mut key, " {ident}").unwrap();
        }
        key
    }

    fn re_affinity_key(&self) -> String {
        self.action.owner().to_string()
    }

    fn as_proto_action_key(&self) -> buck2_data::ActionKey {
        self.action.key().as_proto()
    }

    fn as_proto_action_name(&self) -> buck2_data::ActionName {
        buck2_data::ActionName {
            category: self.action.category().as_str().to_owned(),
            identifier: self.action.identifier().unwrap_or("").to_owned(),
        }
    }
}
