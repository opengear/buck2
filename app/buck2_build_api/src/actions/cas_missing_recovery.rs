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
//! RE CAS. [`CasRecoveryBatch`] holds the keys the daemon invalidated for the transaction now
//! running, consulted by the executor layer to route those actions around cached results that
//! would hand back the digest that was reported missing, and to charge the registry once one of
//! them actually re-executes.
//!
//! One transaction heals one layer of a dependency chain. An action whose input digest is gone
//! reports the failure, which invalidates the producer of that digest; a producer deeper in the
//! chain whose own output was evicted stays hidden until the layer above it resolves, because
//! nothing has requested it yet. [`stage_cas_recovery_round`] takes the registry from armed to
//! invalidated once per transaction, so a build command repeats it against a fresh transaction
//! for as long as failures keep arming actions.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use allocative::Allocative;
use buck2_artifact::actions::key::ActionKey;
use buck2_hash::BuckDashMap;
use buck2_hash::StdBuckHashSet;
use dice::DiceTransactionUpdater;
use dice::UserComputationData;
use dupe::Dupe;
use itertools::Itertools;
use tracing::info;
use tracing::warn;

use crate::actions::calculation::BuildKey;

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
    /// Reports whether this call was the one that charged the attempt.
    ///
    /// [`keys_eligible_for_recovery`]: CasMissingRecoveryRegistry::keys_eligible_for_recovery
    pub fn record_repair_attempt(&self, key: &ActionKey) -> bool {
        match self.state.get_mut(key) {
            Some(mut entry) if entry.armed => {
                entry.armed = false;
                entry.attempts += 1;
                true
            }
            _ => false,
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
/// recovery, and how many of them have gone on to re-execute.
///
/// A key appears here only for the transaction that invalidated it. The executor layer consults
/// this set to route that action's execution around cached results that would hand back the
/// digest that was reported missing.
///
/// The batch outlives the transaction that filled it so that a command running several repair
/// rounds keeps one object in its DICE user data across all of them. [`replace`] swaps the whole
/// key set at once, so one read returns some round's keys entire rather than a set halfway
/// between two of them. Reads are independent: an execution left over from a finished round can
/// ask twice and be answered from either side of a swap, which costs that execution its charge
/// and leaves the round it belonged to looking idle.
///
/// The repair count belongs here rather than on the daemon-lifetime registry because a command
/// reads it to tell a round that made progress from one that made none. A registry counter would
/// also advance for concurrent commands repairing their own actions, which would answer a
/// different question than the one the reader is asking.
///
/// [`replace`]: CasRecoveryBatch::replace
#[derive(Clone, Dupe)]
pub struct CasRecoveryBatch(Arc<CasRecoveryBatchState>);

struct CasRecoveryBatchState {
    keys: Mutex<Arc<StdBuckHashSet<ActionKey>>>,
    repairs_charged: AtomicU64,
}

impl CasRecoveryBatch {
    pub fn empty() -> Self {
        Self(Arc::new(CasRecoveryBatchState {
            keys: Mutex::new(Arc::new(StdBuckHashSet::default())),
            repairs_charged: AtomicU64::new(0),
        }))
    }

    fn keys(&self) -> Arc<StdBuckHashSet<ActionKey>> {
        self.0
            .keys
            .lock()
            .expect("CAS-missing recovery batch lock is never held across a panic")
            .dupe()
    }

    pub fn contains(&self, key: &ActionKey) -> bool {
        self.keys().contains(key)
    }

    /// The actions staged for the round now running.
    pub fn staged(&self) -> Arc<StdBuckHashSet<ActionKey>> {
        self.keys()
    }

    /// Counts the repair executions charged against this command.
    ///
    /// A command reads this on either side of a round: the count advances exactly when an action
    /// the round staged went on to re-execute. A round whose staged actions the build never
    /// requested leaves it unchanged.
    pub fn repairs_charged(&self) -> u64 {
        self.0.repairs_charged.load(Ordering::Relaxed)
    }

    /// Records that one of the staged actions re-executed.
    pub fn record_repair_charged(&self) {
        self.0.repairs_charged.fetch_add(1, Ordering::Relaxed);
    }

    /// Makes `keys` the batch, discarding whatever the previous round left behind.
    ///
    /// On a live build, [`stage_cas_recovery_round`] calls this with the keys it just
    /// invalidated. Any other set routes the executor layer around caches for actions that will
    /// hand back a result the build can still use.
    pub(crate) fn replace(&self, keys: impl IntoIterator<Item = ActionKey>) {
        *self
            .0
            .keys
            .lock()
            .expect("CAS-missing recovery batch lock is never held across a panic") =
            Arc::new(keys.into_iter().collect());
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

/// The limits a build applies to CAS-missing recovery.
#[derive(Clone, Copy, Dupe, Debug, PartialEq, Eq)]
pub struct CasMissingRecoveryConfig {
    /// How many times one action may re-execute under recovery before its failure is fatal.
    pub max_action_attempts: u32,
    /// How many repair rounds one command may stage beyond the one it opens with. Each round
    /// heals one layer of a dependency chain, so this sets how deep an eviction cascade a single
    /// command works through, and bounds a build whose repairs keep arming actions without ever
    /// converging.
    pub max_rounds: u32,
}

pub trait SetCasMissingRecoveryConfig {
    fn set_cas_missing_recovery_config(&mut self, config: CasMissingRecoveryConfig);
}

pub trait HasCasMissingRecoveryConfig {
    fn get_cas_missing_recovery_config(&self) -> CasMissingRecoveryConfig;
}

impl SetCasMissingRecoveryConfig for UserComputationData {
    fn set_cas_missing_recovery_config(&mut self, config: CasMissingRecoveryConfig) {
        self.data.set(config);
    }
}

impl HasCasMissingRecoveryConfig for UserComputationData {
    fn get_cas_missing_recovery_config(&self) -> CasMissingRecoveryConfig {
        self.data
            .get::<CasMissingRecoveryConfig>()
            .expect("CasMissingRecoveryConfig should be set")
            .dupe()
    }
}

/// Invalidates `keys` in `updater`, so the next computation of each one re-executes instead of
/// returning DICE's cached result.
fn invalidate_actions_for_recovery(
    keys: &[ActionKey],
    updater: &mut DiceTransactionUpdater,
) -> buck2_error::Result<()> {
    let build_keys: Vec<BuildKey> = keys.iter().map(|key| BuildKey(key.dupe())).collect();
    updater.changed(build_keys)?;
    Ok(())
}

/// Takes every action `registry` has armed and under its attempt budget, invalidates it in
/// `updater` so the next computation re-executes it instead of returning DICE's cached success
/// naming the digest that went missing, and records it in `batch` so the executor layer routes
/// that re-execution around caches holding the same result.
///
/// Returns how many actions were staged. Zero means the registry is quiet: every action it tracks
/// has been repaired already or has spent its attempt budget, so a caller running repair rounds is
/// done. An error means the invalidation itself did not go through, which no further round can get
/// past, so a caller running repair rounds has to stop and say so rather than stage again.
///
/// The registry is left as it was found. An action is charged against its attempt budget only
/// once the executor layer confirms it re-executed, so an action staged here and then never
/// requested by the build stays armed and reappears in the next round.
///
/// Either outcome other than a successful stage empties `batch`, so the executor layer only routes
/// around caches for actions this transaction really did invalidate.
pub fn stage_cas_recovery_round(
    registry: &CasMissingRecoveryRegistry,
    max_attempts: u32,
    batch: &CasRecoveryBatch,
    updater: &mut DiceTransactionUpdater,
) -> buck2_error::Result<usize> {
    let keys = registry.keys_eligible_for_recovery(max_attempts);
    if keys.is_empty() {
        batch.replace([]);
        return Ok(0);
    }

    let actions = keys.iter().map(|key| key.to_string()).join(", ");

    if let Err(e) = invalidate_actions_for_recovery(&keys, updater) {
        warn!(
            error = %e,
            action_count = keys.len(),
            actions = %actions,
            "invalidating actions for CAS-missing recovery"
        );
        batch.replace([]);
        return Err(e);
    }

    info!(
        action_count = keys.len(),
        actions = %actions,
        "invalidated actions for CAS-missing recovery"
    );

    let staged = keys.len();
    batch.replace(keys);
    Ok(staged)
}

#[cfg(test)]
mod tests {
    use buck2_artifact::actions::key::ActionIndex;
    use buck2_core::configuration::data::ConfigurationData;
    use buck2_core::deferred::base_deferred_key::BaseDeferredKey;
    use buck2_core::deferred::key::DeferredHolderKey;
    use buck2_core::target::configured_target_label::ConfiguredTargetLabel;
    use dice::DetectCycles;
    use dice::Dice;

    use super::*;

    /// An updater over a DICE instance with nothing computed in it. Staging a round only records
    /// invalidations, so the keys it names never have to resolve to anything.
    fn dice_updater() -> DiceTransactionUpdater {
        Dice::builder().build(DetectCycles::Disabled).updater()
    }

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
        let batch = CasRecoveryBatch::empty();
        batch.replace([tracked.dupe()]);

        assert!(batch.contains(&tracked));
        assert!(!batch.contains(&untracked));
    }

    #[test]
    fn empty_recovery_batch_contains_nothing() {
        let batch = CasRecoveryBatch::empty();
        assert!(!batch.contains(&action_key(0)));
    }

    #[test]
    fn record_repair_attempt_reports_only_the_charge_that_lands() {
        // The batch counts a repair only when this reports one, and a command reads that count to
        // tell a round that made progress from one that made none. A call that changes no state
        // reporting a charge would make an idle round look like a productive one.
        let registry = CasMissingRecoveryRegistry::new();
        let key = action_key(0);
        registry.record_missing(key.dupe());

        assert!(registry.record_repair_attempt(&key));
        // The key is disarmed by now, and this one was never tracked at all.
        assert!(!registry.record_repair_attempt(&key));
        assert!(!registry.record_repair_attempt(&action_key(1)));
    }

    #[test]
    fn a_batch_counts_only_the_repairs_it_is_told_about() {
        let batch = CasRecoveryBatch::empty();
        assert_eq!(batch.repairs_charged(), 0);

        batch.record_repair_charged();
        batch.record_repair_charged();

        assert_eq!(batch.repairs_charged(), 2);
    }

    #[test]
    fn replacing_the_batch_leaves_a_snapshot_already_taken_intact() {
        // A reader holds the keys it was answered with rather than a view onto the live set, so a
        // round replacing the batch cannot change what an earlier read already reported.
        let batch = CasRecoveryBatch::empty();
        let earlier = action_key(0);
        batch.replace([earlier.dupe()]);

        let held = batch.staged();
        batch.replace([action_key(1)]);

        assert!(held.contains(&earlier));
        assert!(!batch.contains(&earlier));
    }

    #[test]
    fn staging_a_round_batches_every_armed_key() {
        let registry = CasMissingRecoveryRegistry::new();
        let first = action_key(0);
        let second = action_key(1);
        registry.record_missing(first.dupe());
        registry.record_missing(second.dupe());
        let batch = CasRecoveryBatch::empty();

        stage_cas_recovery_round(&registry, 2, &batch, &mut dice_updater()).unwrap();

        assert_eq!(batch.staged().len(), 2);
        assert!(batch.contains(&first));
        assert!(batch.contains(&second));
    }

    #[test]
    fn staging_a_round_leaves_its_keys_armed_for_the_executor_to_charge() {
        // Staging invalidates an action but does not run it. An action the build turns out not to
        // depend on is never charged, and has to still be armed for the round that follows.
        let registry = CasMissingRecoveryRegistry::new();
        let key = action_key(0);
        registry.record_missing(key.dupe());

        stage_cas_recovery_round(
            &registry,
            2,
            &CasRecoveryBatch::empty(),
            &mut dice_updater(),
        )
        .unwrap();

        assert_eq!(registry.keys_eligible_for_recovery(2), vec![key]);
    }

    #[test]
    fn staging_a_quiet_registry_drops_what_the_previous_round_batched() {
        // Every round of one command shares a single batch object, so a round that finds nothing
        // armed has to empty it. Leaving the previous round's keys in place would route a repaired
        // action around its caches for the rest of the command.
        let registry = CasMissingRecoveryRegistry::new();
        let key = action_key(0);
        registry.record_missing(key.dupe());
        let batch = CasRecoveryBatch::empty();
        stage_cas_recovery_round(&registry, 2, &batch, &mut dice_updater()).unwrap();
        registry.record_repair_attempt(&key);

        stage_cas_recovery_round(&registry, 2, &batch, &mut dice_updater()).unwrap();

        assert_eq!(batch.staged().len(), 0);
        assert!(!batch.contains(&key));
    }

    #[test]
    fn staging_a_later_round_replaces_the_keys_of_the_earlier_one() {
        // Repairing one layer of an eviction cascade exposes the next, which arms a different
        // action. The batch has to name what this round invalidated, not what an earlier round
        // already repaired.
        let registry = CasMissingRecoveryRegistry::new();
        let earlier = action_key(0);
        let later = action_key(1);
        registry.record_missing(earlier.dupe());
        let batch = CasRecoveryBatch::empty();
        stage_cas_recovery_round(&registry, 2, &batch, &mut dice_updater()).unwrap();
        registry.record_repair_attempt(&earlier);
        registry.record_missing(later.dupe());

        stage_cas_recovery_round(&registry, 2, &batch, &mut dice_updater()).unwrap();

        assert_eq!(batch.staged().len(), 1);
        assert!(batch.contains(&later));
        assert!(!batch.contains(&earlier));
    }

    #[test]
    fn staging_skips_a_key_that_has_spent_its_attempt_budget() {
        // An action that keeps failing has to stop being repaired, or a cascade that cannot be
        // healed would keep every round finding work to do.
        let registry = CasMissingRecoveryRegistry::new();
        let key = action_key(0);
        registry.record_missing(key.dupe());
        registry.record_repair_attempt(&key);
        registry.record_missing(key.dupe());
        let batch = CasRecoveryBatch::empty();

        stage_cas_recovery_round(&registry, 1, &batch, &mut dice_updater()).unwrap();

        assert_eq!(batch.staged().len(), 0);
        assert_eq!(
            registry.keys_eligible_for_recovery(1),
            Vec::<ActionKey>::new()
        );
    }
}
