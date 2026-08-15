/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Tracks actions whose declared CAS output was reported missing during execution, and the set
//! of actions selected for re-execution in the current DICE transaction.
//!
//! [`CasMissingRecoveryRegistry`] lives for the daemon's lifetime and is written to from the
//! build layer whenever an action fails because an input it depends on has disappeared from the
//! RE CAS. [`CasRecoveryBatch`] is a per-transaction snapshot of the keys the daemon invalidated
//! for recovery, consulted by the executor layer to route those actions around cached results
//! that would hand back the digest that was reported missing, and to charge the registry once one
//! of them actually re-executes.

use std::collections::HashSet;
use std::sync::Arc;

use allocative::Allocative;
use buck2_artifact::actions::key::ActionKey;
use buck2_hash::BuckDashMap;
use dice::UserComputationData;
use dupe::Dupe;

/// One action's CAS-missing recovery state: how many repair executions have already run for it,
/// and whether a failure has armed it for another attempt.
#[derive(Clone, Copy, Dupe, Debug, Default, PartialEq, Eq, Allocative)]
struct RecoveryState {
    attempts: u32,
    armed: bool,
}

/// Tracks actions whose declared output was reported missing from the RE CAS during execution,
/// so the daemon can re-execute them.
///
/// [`record_missing`] arms an action for recovery without touching its attempt count.
/// [`keys_eligible_for_recovery`] lists every armed action under the configured attempt budget,
/// for the transaction layer to invalidate and hand to the executor as a [`CasRecoveryBatch`].
/// [`record_repair_attempt`] is the charge: the executor layer calls it once an action in that
/// batch has actually re-executed, incrementing its attempt count and disarming it in the same
/// step. An action stays disarmed, and absent from every later batch, until a new failure arms it
/// again through `record_missing` — so a repair that already succeeded is not handed out a second
/// time, and a transaction that takes a batch but executes none of it charges nothing against the
/// budget. Once an action's attempt count reaches the budget, `keys_eligible_for_recovery`
/// excludes it permanently: its failure is fatal from that point on.
///
/// [`record_missing`]: CasMissingRecoveryRegistry::record_missing
/// [`keys_eligible_for_recovery`]: CasMissingRecoveryRegistry::keys_eligible_for_recovery
/// [`record_repair_attempt`]: CasMissingRecoveryRegistry::record_repair_attempt
#[derive(Allocative)]
pub struct CasMissingRecoveryRegistry {
    state: BuckDashMap<ActionKey, RecoveryState>,
}

impl CasMissingRecoveryRegistry {
    pub fn new() -> Self {
        Self {
            state: BuckDashMap::new(),
        }
    }

    /// Arms `key` for recovery at the next DICE transaction. An action already armed keeps its
    /// existing attempt count: this call only ensures the key is tracked and armed.
    pub fn record_missing(&self, key: ActionKey) {
        self.state.entry(key).or_default().armed = true;
    }

    /// Lists every armed action whose attempt count is under `max_attempts`, without changing any
    /// state. Calling this more than once between failures returns the same keys each time: only
    /// [`record_repair_attempt`] disarms a key or advances its attempt count.
    ///
    /// [`record_repair_attempt`]: CasMissingRecoveryRegistry::record_repair_attempt
    pub fn keys_eligible_for_recovery(&self, max_attempts: u32) -> Vec<ActionKey> {
        self.state
            .iter()
            .filter(|entry| entry.armed && entry.attempts < max_attempts)
            .map(|entry| entry.key().dupe())
            .collect()
    }

    /// Charges one repair execution against `key`: increments its attempt count and disarms it,
    /// so it stops appearing in [`keys_eligible_for_recovery`] until a new failure arms it again.
    ///
    /// A key that is not currently armed — untracked, already disarmed by another call, or
    /// disarmed because its budget ran out — is left untouched. This makes the charge idempotent
    /// for an action DICE recomputes more than once in the same transaction after a failure:
    /// whichever recompute finds the key armed pays for the attempt, and every later recompute in
    /// that transaction is a no-op.
    ///
    /// [`keys_eligible_for_recovery`]: CasMissingRecoveryRegistry::keys_eligible_for_recovery
    pub fn record_repair_attempt(&self, key: &ActionKey) {
        if let Some(mut entry) = self.state.get_mut(key) {
            if entry.armed {
                entry.armed = false;
                entry.attempts += 1;
            }
        }
    }

    #[cfg(test)]
    fn attempts(&self, key: &ActionKey) -> Option<u32> {
        self.state.get(key).map(|entry| entry.attempts)
    }
}

pub trait SetCasMissingRecoveryRegistry {
    fn set_cas_missing_recovery_registry(&mut self, registry: Arc<CasMissingRecoveryRegistry>);
}

pub trait HasCasMissingRecoveryRegistry {
    fn get_cas_missing_recovery_registry(&self) -> Arc<CasMissingRecoveryRegistry>;
}

impl SetCasMissingRecoveryRegistry for UserComputationData {
    fn set_cas_missing_recovery_registry(&mut self, registry: Arc<CasMissingRecoveryRegistry>) {
        self.data.set(registry);
    }
}

impl HasCasMissingRecoveryRegistry for UserComputationData {
    fn get_cas_missing_recovery_registry(&self) -> Arc<CasMissingRecoveryRegistry> {
        self.data
            .get::<Arc<CasMissingRecoveryRegistry>>()
            .expect("CasMissingRecoveryRegistry should be set")
            .dupe()
    }
}

/// The actions selected for re-execution during the current DICE transaction under CAS-missing
/// recovery.
///
/// A key appears here only for the transaction that invalidated it. The executor layer consults
/// this set to route that action's execution around cached results that would hand back the
/// digest that was reported missing.
#[derive(Clone, Dupe)]
pub struct CasRecoveryBatch(Arc<HashSet<ActionKey>>);

impl CasRecoveryBatch {
    pub fn new(keys: HashSet<ActionKey>) -> Self {
        Self(Arc::new(keys))
    }

    pub fn empty() -> Self {
        Self(Arc::new(HashSet::new()))
    }

    pub fn contains(&self, key: &ActionKey) -> bool {
        self.0.contains(key)
    }
}

pub trait SetCasRecoveryBatch {
    fn set_cas_recovery_batch(&mut self, batch: CasRecoveryBatch);
}

pub trait HasCasRecoveryBatch {
    fn get_cas_recovery_batch(&self) -> CasRecoveryBatch;
}

impl SetCasRecoveryBatch for UserComputationData {
    fn set_cas_recovery_batch(&mut self, batch: CasRecoveryBatch) {
        self.data.set(batch);
    }
}

impl HasCasRecoveryBatch for UserComputationData {
    fn get_cas_recovery_batch(&self) -> CasRecoveryBatch {
        self.data
            .get::<CasRecoveryBatch>()
            .expect("CasRecoveryBatch should be set")
            .dupe()
    }
}

#[cfg(test)]
mod tests {
    use buck2_artifact::actions::key::ActionIndex;
    use buck2_core::configuration::data::ConfigurationData;
    use buck2_core::deferred::base_deferred_key::BaseDeferredKey;
    use buck2_core::deferred::key::DeferredHolderKey;
    use buck2_core::target::configured_target_label::ConfiguredTargetLabel;

    use super::*;

    fn action_key(id: u32) -> ActionKey {
        let target =
            ConfiguredTargetLabel::testing_parse("cell//pkg:foo", ConfigurationData::testing_new());
        ActionKey::new(
            DeferredHolderKey::Base(BaseDeferredKey::TargetLabel(target)),
            ActionIndex::new(id),
        )
    }

    #[test]
    fn record_missing_arms_an_untracked_key() {
        let registry = CasMissingRecoveryRegistry::new();
        let key = action_key(0);

        registry.record_missing(key.dupe());

        assert_eq!(registry.attempts(&key), Some(0));
        assert_eq!(registry.keys_eligible_for_recovery(2), vec![key]);
    }

    #[test]
    fn record_missing_on_an_already_armed_key_is_idempotent() {
        let registry = CasMissingRecoveryRegistry::new();
        let key = action_key(0);

        registry.record_missing(key.dupe());
        registry.record_missing(key.dupe());

        assert_eq!(registry.keys_eligible_for_recovery(2), vec![key]);
    }

    #[test]
    fn listing_an_armed_key_does_not_charge_or_disarm_it() {
        // A transaction that takes a batch and computes no build keys — `buck2 targets` running
        // between two builds, an IDE query, anything that never asks DICE to compute the armed
        // action — must not spend the action's budget. Seed a real armed key so the batch handed
        // out is non-empty: an empty registry would pass this even if listing charged on hand-out,
        // because there would be nothing to charge.
        let registry = CasMissingRecoveryRegistry::new();
        let key = action_key(0);
        registry.record_missing(key.dupe());

        let first_batch = registry.keys_eligible_for_recovery(2);
        let second_batch = registry.keys_eligible_for_recovery(2);

        assert_eq!(first_batch, vec![key.dupe()]);
        assert_eq!(second_batch, vec![key.dupe()]);
        assert_eq!(registry.attempts(&key), Some(0));
    }

    #[test]
    fn record_repair_attempt_charges_an_armed_key_and_disarms_it() {
        let registry = CasMissingRecoveryRegistry::new();
        let key = action_key(0);
        registry.record_missing(key.dupe());

        registry.record_repair_attempt(&key);

        assert_eq!(registry.attempts(&key), Some(1));
        assert_eq!(
            registry.keys_eligible_for_recovery(2),
            Vec::<ActionKey>::new()
        );
    }

    #[test]
    fn record_repair_attempt_on_an_already_disarmed_key_charges_nothing() {
        // DICE does not cache a failing action's result, so a transaction in which more than one
        // dependent recomputes the same failing key can call this more than once for the same
        // execution episode. Only the first call, which finds the key armed, may charge — every
        // later call in the same episode is a no-op.
        let registry = CasMissingRecoveryRegistry::new();
        let key = action_key(0);
        registry.record_missing(key.dupe());

        registry.record_repair_attempt(&key);
        registry.record_repair_attempt(&key);
        registry.record_repair_attempt(&key);

        assert_eq!(registry.attempts(&key), Some(1));
    }

    #[test]
    fn record_repair_attempt_on_an_untracked_key_charges_nothing() {
        let registry = CasMissingRecoveryRegistry::new();
        let key = action_key(0);

        registry.record_repair_attempt(&key);

        assert_eq!(registry.attempts(&key), None);
    }

    #[test]
    fn listing_excludes_a_disarmed_key_from_a_batch_offered_to_another_action() {
        // The registry outlives every transaction, so a repaired key stays in it, disarmed, once
        // its repair executes. A later listing for a different action must leave that key alone:
        // offering it again would re-execute the repaired action for nothing and spend its budget
        // while its own failures are still recoverable.
        let registry = CasMissingRecoveryRegistry::new();
        let repaired = action_key(0);
        let failing = action_key(1);

        registry.record_missing(repaired.dupe());
        assert_eq!(
            registry.keys_eligible_for_recovery(2),
            vec![repaired.dupe()]
        );
        registry.record_repair_attempt(&repaired);

        registry.record_missing(failing.dupe());
        assert_eq!(registry.keys_eligible_for_recovery(2), vec![failing.dupe()]);
        registry.record_repair_attempt(&failing);

        assert_eq!(registry.attempts(&repaired), Some(1));
        assert_eq!(registry.attempts(&failing), Some(1));
    }

    #[test]
    fn budget_exhaustion_still_terminates_recovery() {
        let registry = CasMissingRecoveryRegistry::new();
        let key = action_key(0);

        registry.record_missing(key.dupe());
        assert_eq!(registry.keys_eligible_for_recovery(2), vec![key.dupe()]);
        registry.record_repair_attempt(&key);

        registry.record_missing(key.dupe());
        assert_eq!(registry.keys_eligible_for_recovery(2), vec![key.dupe()]);
        registry.record_repair_attempt(&key);
        assert_eq!(registry.attempts(&key), Some(2));

        // A third failure arms the key again, but the budget is already exhausted: the listing
        // excludes it and its failure is fatal from here on.
        registry.record_missing(key.dupe());
        assert_eq!(
            registry.keys_eligible_for_recovery(2),
            Vec::<ActionKey>::new()
        );
        assert_eq!(registry.attempts(&key), Some(2));
    }

    #[test]
    fn listing_on_empty_registry_returns_nothing() {
        let registry = CasMissingRecoveryRegistry::new();
        assert_eq!(
            registry.keys_eligible_for_recovery(2),
            Vec::<ActionKey>::new()
        );
    }

    #[test]
    fn recovery_batch_contains_only_recorded_keys() {
        let tracked = action_key(0);
        let untracked = action_key(1);
        let batch = CasRecoveryBatch::new(HashSet::from([tracked.dupe()]));

        assert!(batch.contains(&tracked));
        assert!(!batch.contains(&untracked));
    }

    #[test]
    fn empty_recovery_batch_contains_nothing() {
        let batch = CasRecoveryBatch::empty();
        assert!(!batch.contains(&action_key(0)));
    }
}
