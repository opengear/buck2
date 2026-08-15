/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::iter::zip;
use std::sync::Arc;

use allocative::Allocative;
use async_trait::async_trait;
use buck2_artifact::actions::key::ActionKey;
use buck2_artifact::artifact::artifact_type::BaseArtifactKind;
use buck2_artifact::artifact::build_artifact::BuildArtifact;
use buck2_build_signals::env::NodeDuration;
use buck2_build_signals::env::WaitingData;
use buck2_common::events::HasEvents;
use buck2_core::deferred::base_deferred_key::BaseDeferredKey;
use buck2_core::fs::artifact_path_resolver::ArtifactFs;
use buck2_core::fs::project_rel_path::ProjectRelativePathBuf;
use buck2_core::soft_error;
use buck2_core::target::configured_target_label::ConfiguredTargetLabel;
use buck2_data::ActionErrorDiagnostics;
use buck2_data::ActionSubErrors;
use buck2_data::ToProtoMessage;
use buck2_data::get_action_digest;
use buck2_directory::directory::directory::Directory;
use buck2_directory::directory::directory_iterator::DirectoryIterator;
use buck2_directory::directory::entry::DirectoryEntry;
use buck2_directory::directory::walk::unordered_entry_walk;
use buck2_error::BuckErrorContext;
use buck2_event_observer::action_util::get_execution_time_ms;
use buck2_events::dispatch::async_record_root_spans;
use buck2_events::dispatch::console_message;
use buck2_events::dispatch::get_dispatcher;
use buck2_events::dispatch::span_async;
use buck2_events::span::SpanId;
use buck2_execute::artifact::artifact_dyn::ArtifactDyn;
use buck2_execute::artifact_value::ArtifactValue;
use buck2_execute::directory::ActionDirectoryMember;
use buck2_execute::execute::missing_cas_digests::MissingCasDigests;
use buck2_execute::execute::result::CommandExecutionReport;
use buck2_execute::execute::result::CommandExecutionStatus;
use buck2_execute::output_size::OutputSize;
use buck2_hash::BuckIndexMap;
use buck2_interpreter::print_handler::EventDispatcherPrintHandler;
use buck2_interpreter::soft_error::Buck2StarlarkSoftErrorHandler;
use buck2_node::nodes::configured_frontend::ConfiguredTargetNodeCalculation;
use buck2_util::time_span::TimeSpan;
use derive_more::Display;
use dice::DiceComputations;
use dice::DiceTrackedInvalidationPath;
use dice::DiceTransactionUpdater;
use dice::Key;
use dice::OkPagableValueSerialize;
use dice::ValueSerialize;
use dice_futures::cancellation::CancellationContext;
use dupe::Dupe;
use futures::FutureExt;
use futures::future::BoxFuture;
use pagable::Pagable;
use pagable::pagable_typetag;
use ref_cast::RefCast;
use smallvec::SmallVec;
use starlark::environment::Module;
use starlark::eval::Evaluator;
use tracing::debug;
use tracing::warn;

use crate::actions::RegisteredAction;
use crate::actions::artifact::get_artifact_fs::GetArtifactFs;
use crate::actions::cas_missing_recovery::CasMissingRecoveryRegistry;
use crate::actions::cas_missing_recovery::CasRecoveryBatch;
use crate::actions::cas_missing_recovery::HasCasMissingRecoveryRegistry;
use crate::actions::error::ActionError;
use crate::actions::error_handler::ActionErrorHandlerError;
use crate::actions::error_handler::ActionSubErrorResult;
use crate::actions::error_handler::StarlarkActionErrorContext;
use crate::actions::execute::action_executor::ActionOutputs;
use crate::actions::execute::action_executor::BuckActionExecutor;
use crate::actions::execute::action_executor::HasActionExecutor;
use crate::actions::execute::error::ExecuteError;
use crate::actions::impls::run_action_knobs::RunActionKnobs;
use crate::artifact_groups::ArtifactGroup;
use crate::artifact_groups::ArtifactGroupValues;
use crate::artifact_groups::calculation::ensure_artifact_group_staged;
use crate::build::detailed_aggregated_metrics::dice::HasDetailedAggregatedMetrics;
use crate::build::detailed_aggregated_metrics::types::ActionExecutionMetrics;
use crate::deferred::calculation::ActionLookup;
use crate::deferred::calculation::lookup_deferred_holder;
use crate::keep_going::KeepGoing;
use crate::starlark::values::UnpackValue;
use crate::starlark::values::type_repr::StarlarkTypeRepr;

pub struct ActionCalculation;

async fn build_action_impl(
    ctx: &mut DiceComputations<'_>,
    cancellation: &CancellationContext,
    key: &ActionKey,
) -> buck2_error::Result<ActionOutputs> {
    let action = ActionCalculation::get_action(ctx, key).await?;

    if action.key() != key {
        // The action key we start with is on the DICE graph, and thus cached
        // and properly deduplicated. But if the underlying has a different key,
        // e.g. due to dynamic_output, then we might have two different action keys
        // pointing at the same underlying action. We need to make sure that
        // underlying action only gets called once, so call build_action once
        // again with the new key to get DICE deduplication.
        let res = ActionCalculation::build_action(ctx, action.key()).await;
        return res;
    }

    build_action_no_redirect(ctx, cancellation, action).await
}

mini_vec::size_assert::words_of_async_fn_future!(build_action_impl, (_, _, _), ~43);

async fn build_action_no_redirect(
    ctx: &mut DiceComputations<'_>,
    cancellation: &CancellationContext,
    action: Arc<RegisteredAction>,
) -> buck2_error::Result<ActionOutputs> {
    let inputs = action.inputs()?;
    let waiting_data = WaitingData::new();
    let executor = ctx
        .get_action_executor(action.execution_config())
        .await
        .buck_error_context(format!("for action `{action}`"))?;

    let _eager_guard = if executor.materializer().is_eager_materialization_enabled()
        && action.eager_materialization_enabled()
        && action.executor_preference().is_some_and(|pref| {
            !pref.prefers_remote()
                && executor.is_local_execution_possible(pref)
                && (pref.prefers_local() || executor.is_full_hybrid_enabled())
        }) {
        let artifact_fs = ctx.get_artifact_fs().await?;
        let eager_paths = collect_eager_paths(ctx, &inputs, &artifact_fs)
            .boxed()
            .await?;

        if eager_paths.is_empty() {
            None
        } else {
            Some(
                executor
                    .materializer()
                    .register_eager_paths(eager_paths, get_dispatcher())
                    .await?,
            )
        }
    } else {
        None
    };

    let ensured_inputs = if inputs.is_empty() {
        BuckIndexMap::default()
    } else {
        let ready_inputs: Vec<_> =
            KeepGoing::try_compute_join_all(ctx, inputs.iter(), async |ctx, v| {
                let resolved = v.resolved_artifact(ctx).await?;
                buck2_error::Ok(
                    ensure_artifact_group_staged(ctx, resolved)
                        .await?
                        .into_group_values(resolved)?,
                )
            })
            .await?;

        let mut results = BuckIndexMap::with_capacity(inputs.len());
        for (artifact, ready) in zip(inputs.iter(), ready_inputs) {
            results.insert(artifact.clone(), ready);
        }
        results
    };

    let now = TimeSpan::start_now();

    let target_rule_type_name = match action.key().owner() {
        BaseDeferredKey::TargetLabel(target_label) => {
            Some(get_target_rule_type_name(ctx, target_label).await?)
        }
        _ => None,
    };

    let fut = build_action_inner(
        ctx,
        cancellation,
        &executor,
        waiting_data,
        ensured_inputs,
        &action,
        target_rule_type_name,
    );

    // Don't hold this across an await point
    let start_event = buck2_data::ActionExecutionStart {
        key: Some(action.key().as_proto()),
        kind: action.kind().into(),
        name: Some(buck2_data::ActionName {
            category: action.category().as_str().to_owned(),
            identifier: action.identifier().unwrap_or("").to_owned(),
        }),
    };

    let (action_execution_data, spans) = async_record_root_spans(span_async(start_event, fut))
        // boxed() the future so that we don't need to allocate space for it while waiting on input dependencies.
        .boxed()
        .await;

    let execution_metrics = ActionExecutionMetrics {
        key: action.key().dupe(),
        execution_time_ms: action_execution_data
            .extra_data
            .execution_time_ms
            .unwrap_or_default(),
        execution_kind: action_execution_data.extra_data.execution_kind,
        output_size_bytes: action_execution_data.extra_data.output_size,
        memory_peak: action_execution_data.memory_peak,
        re_platform_name: action_execution_data.extra_data.re_platform_name.clone(),
    };
    ctx.store_evaluation_data(BuildKeyActivationData {
        action_with_extra_data: ActionWithExtraData {
            action: action.dupe(),
            extra_data: action_execution_data.extra_data,
        },
        duration: NodeDuration {
            user: action_execution_data.wall_time.unwrap_or_default(),
            total: now.end_now(),
            queue: action_execution_data.queue_duration,
        },
        spans,
        waiting_data: action_execution_data.waiting_data,
    })?;

    ctx.action_executed(execution_metrics)?;

    action_execution_data.action_result
}

/// Collect all materializable artifact paths from an `ArtifactGroup` list,
/// traversing transitive set projections via BFS.
async fn collect_eager_paths(
    ctx: &mut DiceComputations<'_>,
    inputs: &[ArtifactGroup],
    artifact_fs: &ArtifactFs,
) -> buck2_error::Result<Vec<ProjectRelativePathBuf>> {
    let mut eager_paths = HashSet::new();
    let mut queue: Vec<ArtifactGroup> = inputs.to_vec();
    let mut visited = HashSet::new();

    while let Some(input) = queue.pop() {
        if !visited.insert(input.dupe()) {
            continue;
        }

        match &input {
            ArtifactGroup::Artifact(a) => {
                if a.requires_materialization(artifact_fs) {
                    // For projected artifacts (a file inside a directory output), register
                    // the base directory's configuration path. The materializer only declares
                    // base artifact paths, so the projected sub-path would never match a
                    // Declare. Materializing the base directory covers all projected files.
                    let path = if a.is_projected() {
                        match a.as_parts().0 {
                            BaseArtifactKind::Build(b) => {
                                artifact_fs.resolve_build_configuration_hash_path(b.get_path())?
                            }
                            BaseArtifactKind::Source(s) => {
                                artifact_fs.resolve_source(s.get_path())?
                            }
                        }
                    } else {
                        a.resolve_configuration_hash_path(artifact_fs)?
                    };
                    eager_paths.insert(path);
                }
            }
            ArtifactGroup::TransitiveSetProjection(tset) => {
                let set = tset.key.key.lookup(ctx).await?;
                queue.extend(set.get_projection_sub_inputs(tset.key.projection)?);
            }
            ArtifactGroup::Promise(_) => {
                // Skip promise artifacts - they should not be eagerly materialized
            }
        }
    }

    Ok(eager_paths.into_iter().collect())
}

async fn build_action_inner(
    ctx: &mut DiceComputations<'_>,
    cancellation: &CancellationContext,
    executor: &BuckActionExecutor,
    waiting_data: WaitingData,
    ensured_inputs: BuckIndexMap<ArtifactGroup, ArtifactGroupValues>,
    action: &Arc<RegisteredAction>,
    target_rule_type_name: Option<String>,
) -> (ActionExecutionData, Box<buck2_data::ActionExecutionEnd>) {
    let is_eligible_for_dedupe = is_action_eligible_for_dedupe(action, &ensured_inputs);
    let is_expected_eligible_for_dedupe = match action.is_expected_eligible_for_dedupe() {
        Some(v) => {
            if v {
                buck2_data::ExpectedEligibleForDedupe::ExpectedEligible
            } else {
                buck2_data::ExpectedEligibleForDedupe::ExpectedIneligible
            }
        }
        None => buck2_data::ExpectedEligibleForDedupe::UnknownEligibility,
    };

    let inputs_for_recovery =
        inputs_for_cas_missing_recovery(executor.run_action_knobs(), &ensured_inputs);

    let (execute_result, command_reports) = executor
        .execute(waiting_data, ensured_inputs, action, cancellation)
        .await;

    record_cas_missing_recovery_repair(ctx, executor, action);

    let allow_omit_details = execute_result.is_ok();

    let commands = buck2_util::future::join_all(
        command_reports
            .iter()
            .map(|r| command_execution_report_to_proto(r, allow_omit_details)),
    )
    .await;

    let action_digest = get_action_digest(&commands);

    let queue_duration = command_reports.last().and_then(|r| r.timing.queue_duration);
    let memory_peak = command_reports
        .last()
        .and_then(|r| r.timing.execution_stats.and_then(|s| s.memory_peak));

    let action_key = action.key().as_proto();

    let action_name = buck2_data::ActionName {
        category: action.category().as_str().to_owned(),
        identifier: action.identifier().unwrap_or("").to_owned(),
    };

    let action_result;
    let execution_kind;
    let wall_time;
    let error;
    let output_size;

    let mut prefers_local = None;
    let mut requires_local = None;
    let mut allows_cache_upload = None;
    let mut cache_upload_result = None;
    let mut allows_dep_file_cache_upload = None;
    let mut dep_file_cache_upload_result = None;
    let mut dep_file_key = None;
    let mut eligible_for_full_hybrid = None;

    let mut buck2_revision = None;
    let mut buck2_build_time = None;
    let mut hostname = None;
    let mut input_files_bytes = None;
    let mut scheduling_mode = None;
    let mut incremental_kind = None;
    let mut waiting_data = None;
    let error_diagnostics = match execute_result {
        Ok((outputs, meta)) => {
            output_size = outputs.calc_output_count_and_bytes(false).bytes;
            action_result = Ok(outputs);
            execution_kind = Some(meta.execution_kind.as_enum());
            if matches!(
                meta.execution_kind.as_enum(),
                buck2_data::ActionExecutionKind::Local
                    | buck2_data::ActionExecutionKind::LocalWorker
                    | buck2_data::ActionExecutionKind::LocalDepFile
                    | buck2_data::ActionExecutionKind::LocalActionCache
            ) {
                hostname = buck2_events::metadata::hostname();
            }
            wall_time = Some(meta.timing.wall_time);
            error = None;
            input_files_bytes = meta.input_files_bytes;
            waiting_data = Some(meta.waiting_data);

            if let Some(command) = meta.execution_kind.command() {
                prefers_local = Some(command.prefers_local);
                requires_local = Some(command.requires_local);
                allows_cache_upload = Some(command.allows_cache_upload);
                cache_upload_result = Some(command.cache_upload_result);
                allows_dep_file_cache_upload = Some(command.allows_dep_file_cache_upload);
                dep_file_cache_upload_result = Some(command.dep_file_cache_upload_result);
                dep_file_key = *command.dep_file_key;
                eligible_for_full_hybrid = Some(command.eligible_for_full_hybrid);
                scheduling_mode = command.scheduling_mode;
                incremental_kind = Some(command.incremental_kind);
            }

            None
        }
        Err(e) => {
            // TODO (torozco): Remove (see protobuf file)?
            execution_kind = command_reports
                .last()
                .and_then(|r| r.status.execution_kind())
                .map(|e| e.as_enum());
            wall_time = command_reports
                .last()
                .map(|r| r.timing.time_span.duration());
            output_size = 0;
            // We define the below fields only in the instance of an action error
            // so as to reduce Scribe traffic and log it in buck2_action_errors
            buck2_revision = buck2_build_info::revision().map(|s| s.to_owned());
            buck2_build_time = buck2_build_info::time_iso8601().map(|s| s.to_owned());
            hostname = buck2_events::metadata::hostname();

            let last_command = commands.last().cloned();

            let e = attach_cas_missing_recovery_outcome(ctx, e, inputs_for_recovery.as_ref());

            let outputs = match &e {
                ExecuteError::CommandExecutionError { action_outputs, .. } => {
                    cache_upload_result = Some(buck2_data::UploadResult::ActionNotSuccessful);
                    Some(action_outputs)
                }
                _ => None,
            };

            let error_diagnostics = try_run_error_handler(
                action.dupe(),
                last_command.as_ref(),
                ctx.get_artifact_fs().await,
                outputs,
            );

            let infra_error_tag = check_infra_error_patterns(last_command.as_ref());

            let e = ActionError::new(
                e,
                action_name.clone(),
                action_key.clone(),
                last_command.clone(),
                error_diagnostics.clone(),
                infra_error_tag,
            );

            error = Some(e.as_proto_field());

            ctx.per_transaction_data()
                .get_dispatcher()
                .instant_event(e.as_proto_event());

            action_result = Err(buck2_error::Error::from(e)
                // Make sure to mark the error as emitted so that it is not printed out to console
                // again in this command. We still need to keep it around for the build report (and
                // in the future) other commands
                .mark_emitted({
                    let owner = action.owner().dupe();
                    Arc::new(move |f| write!(f, "Failed to build '{owner}'"))
                }));

            error_diagnostics
        }
    };

    let outputs = action_result
        .as_ref()
        .map(|outputs| {
            outputs
                .iter()
                .filter_map(|(_artifact, value)| {
                    Some(buck2_data::ActionOutput {
                        tiny_digest: value.digest()?.tiny_digest().to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let invalidation_info = if executor.invalidation_tracking_enabled() {
        fn to_proto(
            invalidation_path: &DiceTrackedInvalidationPath,
        ) -> Option<buck2_data::command_invalidation_info::InvalidationSource> {
            match invalidation_path {
                dice::DiceTrackedInvalidationPath::Clean
                | dice::DiceTrackedInvalidationPath::Unknown => None,
                dice::DiceTrackedInvalidationPath::Invalidated(_) => {
                    Some(buck2_data::command_invalidation_info::InvalidationSource {})
                }
            }
        }
        let invalidation_paths = ctx.get_invalidation_paths();
        Some(buck2_data::CommandInvalidationInfo {
            changed_any: to_proto(&invalidation_paths.normal_priority_path),
            changed_file: to_proto(&invalidation_paths.high_priority_path),
        })
    } else {
        None
    };

    let execution_kind = execution_kind.unwrap_or(buck2_data::ActionExecutionKind::NotSet);
    let cache_upload_result =
        cache_upload_result.unwrap_or(buck2_data::UploadResult::NonCommandAction);
    let dep_file_cache_upload_result =
        dep_file_cache_upload_result.unwrap_or(buck2_data::UploadResult::NotAttempted);

    let re_platform_name = command_reports
        .last()
        .and_then(|r| r.status.execution_kind())
        .and_then(|k| k.re_platform_name());

    (
        ActionExecutionData {
            action_result,
            wall_time,
            queue_duration,
            memory_peak,
            extra_data: ActionExtraData {
                execution_kind,
                target_rule_type_name: target_rule_type_name.clone(),
                action_digest,
                invalidation_info,
                execution_time_ms: get_execution_time_ms(&commands),
                output_size,
                re_platform_name,
            },
            waiting_data: waiting_data.unwrap_or_default(),
        },
        Box::new(buck2_data::ActionExecutionEnd {
            key: Some(action_key),
            kind: action.kind().into(),
            name: Some(action_name),
            failed: error.is_some(),
            error,
            always_print_stderr: action.always_print_stderr(),
            wall_time: wall_time.and_then(|d| d.try_into().ok()),
            execution_kind: execution_kind as i32,
            output_size,
            commands,
            outputs,
            prefers_local: prefers_local.unwrap_or_default(),
            requires_local: requires_local.unwrap_or_default(),
            allows_cache_upload: allows_cache_upload.unwrap_or_default(),
            cache_upload_result: cache_upload_result as i32,
            allows_dep_file_cache_upload: allows_dep_file_cache_upload.unwrap_or_default(),
            dep_file_cache_upload_result: dep_file_cache_upload_result as i32,
            dep_file_key: dep_file_key.map(|d| d.to_string()),
            eligible_for_full_hybrid,
            buck2_revision,
            buck2_build_time,
            hostname,
            error_diagnostics,
            input_files_bytes,
            invalidation_info,
            target_rule_type_name,
            scheduling_mode: scheduling_mode.map(|h| h as i32),
            incremental_kind: incremental_kind.map(|k| k as i32),
            eligible_for_dedupe: is_eligible_for_dedupe as i32,
            expected_eligible_for_dedupe: is_expected_eligible_for_dedupe as i32,
        }),
    )
}

fn is_action_eligible_for_dedupe(
    action: &Arc<RegisteredAction>,
    inputs: &BuckIndexMap<ArtifactGroup, ArtifactGroupValues>,
) -> buck2_data::EligibleForDedupe {
    let target_platform =
        if let BaseDeferredKey::TargetLabel(configured_label) = action.key().owner() {
            Some(configured_label.cfg())
        } else {
            None
        };

    if !action.all_outputs_are_content_based() {
        return buck2_data::EligibleForDedupe::IneligibleOutput;
    }

    for (ag, _agv) in inputs.iter() {
        let eligibility = ag.is_eligible_for_dedupe(target_platform);
        if eligibility != buck2_data::EligibleForDedupe::Eligible {
            return eligibility;
        }
    }

    buck2_data::EligibleForDedupe::Eligible
}

fn check_infra_error_patterns(
    last_command: Option<&buck2_data::CommandExecution>,
) -> Option<buck2_error::ErrorTag> {
    use buck2_error::ErrorTag;

    let stderr = last_command
        .and_then(|c| c.details.as_ref())
        .map_or("", |d| d.cmd_stderr.as_str());

    const INFRA_PATTERNS: &[(&str, ErrorTag)] = &[
        (
            "transport endpoint is not connected",
            ErrorTag::IoNotConnected,
        ),
        ("out of memory", ErrorTag::ActionOom),
    ];

    let stderr_lower = stderr.to_lowercase();
    INFRA_PATTERNS
        .iter()
        .find(|(pattern, _)| stderr_lower.contains(pattern))
        .map(|(_, tag)| *tag)
}

/// Builds an index from each input's canonical `hash:size` digest to the `ActionKey` of the
/// action that produced it, so a CAS-missing failure on one of these inputs can identify which
/// action to repair.
///
/// Source artifacts have no producing action and contribute nothing to the index. A digest that
/// resolves to none of this action's inputs reaches the fatal path unchanged, and fails the build
/// exactly as it would without CAS-missing recovery.
fn index_missing_digest_candidates(
    ensured_inputs: &BuckIndexMap<ArtifactGroup, ArtifactGroupValues>,
) -> HashMap<String, ActionKey> {
    let mut index = HashMap::new();
    for group_values in ensured_inputs.values() {
        for (artifact, value) in group_values.iter() {
            let Some(action_key) = artifact.action_key() else {
                continue;
            };
            index_artifact_value_digests(value, action_key, &mut index);
        }
    }
    index
}

/// Indexes every digest reachable from `value` (its own root digest, and every file digest in
/// its directory tree for a tree artifact) against `action_key`.
fn index_artifact_value_digests(
    value: &ArtifactValue,
    action_key: &ActionKey,
    index: &mut HashMap<String, ActionKey>,
) {
    if let Some(digest) = value.digest() {
        index
            .entry(digest.to_string())
            .or_insert_with(|| action_key.dupe());
    }

    let mut walk = unordered_entry_walk(value.entry().as_ref().map_dir(Directory::as_ref));
    while let Some((_, entry)) = walk.next() {
        if let DirectoryEntry::Leaf(ActionDirectoryMember::File(file)) = entry {
            index
                .entry(file.digest.to_string())
                .or_insert_with(|| action_key.dupe());
        }
    }
}

/// The guidance shown to the user when CAS-missing recovery identified at least one producing
/// action and armed it for re-execution on the next build.
const CAS_MISSING_RECOVERY_QUEUED: &str = "Buck2 identified the action(s) that produced these \
    artifacts and queued them for re-execution on your next build.";

/// The guidance shown to the user when CAS-missing recovery could not identify a producing action
/// for any of the missing digests, so the failure is fatal and the daemon's in-memory state is
/// unaffected by the failure.
const CAS_MISSING_RECOVERY_UNATTRIBUTED: &str = "This error is currently unrecoverable. To \
    proceed, you should restart Buck using `buck2 killall`.";

/// What CAS-missing recovery determined for one failure: which producing actions to arm for
/// re-execution, which missing digests resolved to no producing action, and the guidance that
/// matches the outcome.
struct CasMissingRecoveryOutcome {
    producing_actions: Vec<ActionKey>,
    unattributed: Vec<(ProjectRelativePathBuf, String)>,
    guidance: &'static str,
}

/// Resolves the digests a CAS-missing failure reported against an index built from
/// `ensured_inputs`, deciding which producing actions recovery can arm and what guidance the
/// outcome warrants.
///
/// A digest that resolves to no producing action is not an error on its own: it is an input this
/// action didn't declare, or a digest for which the daemon has no producing action on record. It
/// only changes the guidance when none of the failure's digests resolve to a producing action, at
/// which point recovery has nothing to arm and the failure stays fatal.
fn resolve_cas_missing_recovery_outcome(
    missing: &MissingCasDigests,
    ensured_inputs: &BuckIndexMap<ArtifactGroup, ArtifactGroupValues>,
) -> CasMissingRecoveryOutcome {
    let digest_to_producing_action = index_missing_digest_candidates(ensured_inputs);

    let mut producing_actions = Vec::new();
    let mut unattributed = Vec::new();
    for (path, digest) in &missing.missing {
        match digest_to_producing_action.get(digest) {
            Some(action) => producing_actions.push(action.dupe()),
            None => unattributed.push((path.clone(), digest.clone())),
        }
    }

    let guidance = if producing_actions.is_empty() {
        CAS_MISSING_RECOVERY_UNATTRIBUTED
    } else {
        CAS_MISSING_RECOVERY_QUEUED
    };

    CasMissingRecoveryOutcome {
        producing_actions,
        unattributed,
        guidance,
    }
}

/// Retains the action's inputs for CAS-missing attribution, and does so only for a build that
/// opted into recovery.
///
/// `ensured_inputs` is moved into the executor, so attribution needs its own handle on the map.
/// `ArtifactGroupValues` is `Arc`-backed, making the clone a reference-count bump per input group
/// rather than a walk of every artifact's digest tree, and the digest index that attribution
/// walks is built in the error branch, leaving a successful action paying for the clone alone.
fn inputs_for_cas_missing_recovery(
    knobs: &RunActionKnobs,
    ensured_inputs: &BuckIndexMap<ArtifactGroup, ArtifactGroupValues>,
) -> Option<BuckIndexMap<ArtifactGroup, ArtifactGroupValues>> {
    knobs
        .cas_missing_recovery_enabled
        .then(|| ensured_inputs.clone())
}

/// Attaches the outcome of CAS-missing recovery to `error`: arms every producing action it
/// identified in `registry`, and replaces the upload-time placeholder guidance with the one that
/// matches what recovery actually determined for this failure.
///
/// A build that did not opt into recovery leaves `inputs_for_recovery` `None`, so this returns
/// the error exactly as the executor produced it, leaving the registry empty. This function
/// treats an error as a CAS-missing failure only when it holds a [`MissingCasDigests`] context;
/// every other error returns unchanged.
fn apply_cas_missing_recovery_outcome(
    registry: &CasMissingRecoveryRegistry,
    error: ExecuteError,
    inputs_for_recovery: Option<&BuckIndexMap<ArtifactGroup, ArtifactGroupValues>>,
) -> ExecuteError {
    let Some(ensured_inputs) = inputs_for_recovery else {
        return error;
    };
    let ExecuteError::CommandExecutionError {
        action_outputs,
        error: Some(inner),
    } = error
    else {
        return error;
    };
    let Some(missing) = inner.find_typed_context::<MissingCasDigests>() else {
        return ExecuteError::CommandExecutionError {
            action_outputs,
            error: Some(inner),
        };
    };

    let outcome = resolve_cas_missing_recovery_outcome(&missing, ensured_inputs);

    for action in &outcome.producing_actions {
        debug!(action = %action, "arming action for CAS-missing recovery");
        registry.record_missing(action.dupe());
    }
    for (path, digest) in &outcome.unattributed {
        warn!(
            path = %path,
            digest = %digest,
            "digest missing from the RE CAS resolved to no producing action; CAS-missing recovery cannot repair it"
        );
    }
    if !outcome.producing_actions.is_empty() {
        report_recoverable_by_command_retry(outcome.producing_actions.len());
    }

    // The console reads this guidance from its own event. Converting an `ActionError` into a
    // `buck2_error::Error` rebuilds the error from the action's formatted message and keeps only
    // its tags, so context attached here reaches the build report but never the terminal.
    console_message(outcome.guidance.to_owned());

    ExecuteError::CommandExecutionError {
        action_outputs,
        error: Some(inner.context(outcome.guidance)),
    }
}

/// Reports this failure to the client as recoverable by an automatic same-daemon command retry:
/// the daemon armed at least one producing action for re-execution, so retrying the same command
/// once that action re-executes is expected to succeed.
///
/// This is a distinct `StructuredError` event from the one the CAS-missing error itself raised at
/// the point it was first detected. Attribution runs after that event already fired: upload and
/// download failure sites report a digest missing before any caller has attempted to resolve it
/// to a producing action, so only this later point in the failure's handling knows whether
/// recovery found one.
fn report_recoverable_by_command_retry(producing_action_count: usize) {
    let notice = buck2_error::buck2_error!(
        buck2_error::ErrorTag::ReCasArtifactMissingRecoverable,
        "queued {} action(s) for CAS-missing repair",
        producing_action_count,
    );
    let _ignored = soft_error!(
        "cas_missing_recovery_queued",
        notice,
        quiet: true,
        task: false,
        recoverable_by_command_retry: true,
    );
}

/// Attaches the outcome of CAS-missing recovery to `error`, arming producing actions in the
/// daemon-lifetime registry attached to this DICE transaction.
///
/// See [`apply_cas_missing_recovery_outcome`] for which failures recovery acts on.
fn attach_cas_missing_recovery_outcome(
    ctx: &DiceComputations<'_>,
    error: ExecuteError,
    inputs_for_recovery: Option<&BuckIndexMap<ArtifactGroup, ArtifactGroupValues>>,
) -> ExecuteError {
    let registry = ctx
        .per_transaction_data()
        .get_cas_missing_recovery_registry();
    apply_cas_missing_recovery_outcome(&registry, error, inputs_for_recovery)
}

/// Charges `registry` for `key`'s execution if `batch` selected it for repair.
///
/// A key absent from `batch` — a build that did not opt into recovery leaves it empty, and an
/// action this transaction never armed is never in it — leaves `registry` untouched. The charge
/// runs regardless of whether the execution that finished succeeded or failed: the budget
/// tracks how many times an action has actually re-executed under recovery, not how many of those
/// re-executions fixed the problem.
fn charge_cas_missing_recovery_repair(
    registry: &CasMissingRecoveryRegistry,
    batch: &CasRecoveryBatch,
    key: &ActionKey,
) {
    if batch.contains(key) {
        registry.record_repair_attempt(key);
    }
}

/// Charges the CAS-missing recovery registry for `action` once its execution finishes, reading
/// the registry and the recovery batch this DICE transaction attached to `ctx` and `executor`.
///
/// See [`charge_cas_missing_recovery_repair`] for which actions this charges.
fn record_cas_missing_recovery_repair(
    ctx: &DiceComputations<'_>,
    executor: &BuckActionExecutor,
    action: &RegisteredAction,
) {
    let registry = ctx
        .per_transaction_data()
        .get_cas_missing_recovery_registry();
    charge_cas_missing_recovery_repair(&registry, executor.cas_recovery_batch(), action.key());
}

// Attempt to run the error handler if one was specified. Returns either the error diagnostics, or
// an actual error if the handler failed to run successfully.
fn try_run_error_handler(
    action: Arc<RegisteredAction>,
    last_command: Option<&buck2_data::CommandExecution>,
    artifact_fs: buck2_error::Result<ArtifactFs>,
    outputs: Option<&ActionOutputs>,
) -> Option<ActionErrorDiagnostics> {
    use buck2_data::action_error_diagnostics::Data;

    fn create_error(
        e: buck2_error::Error,
    ) -> (
        Option<ActionErrorDiagnostics>,
        buck2_data::ActionErrorHandlerExecutionEnd,
    ) {
        (
            Some(ActionErrorDiagnostics {
                data: Some(Data::HandlerInvocationError(format!("{e:#}"))),
            }),
            buck2_data::ActionErrorHandlerExecutionEnd {},
        )
    }

    match action.action.error_handler() {
        Some(error_handler) => {
            let dispatcher = get_dispatcher();

            dispatcher
                .clone()
                .span(buck2_data::ActionErrorHandlerExecutionStart {}, || {
                    // patternlint-disable-next-line buck2-no-starlark-module: FIXME(JakobDegen): Wrong
                    Module::with_temp_heap(|env| {
                        let heap = env.heap();
                        let print = EventDispatcherPrintHandler(get_dispatcher());
                        let mut eval = Evaluator::new(&env);
                        eval.set_print_handler(&print);
                        eval.set_soft_error_handler(&Buck2StarlarkSoftErrorHandler);

                        let artifact_fs = match artifact_fs {
                            Ok(fs) => fs,
                            Err(e) => return create_error(e),
                        };

                        let outputs_artifacts = match action.action.failed_action_output_artifacts(
                            &artifact_fs,
                            heap,
                            outputs,
                        ) {
                            Ok(v) => v,
                            Err(e) => return create_error(e),
                        };

                        let error_handler_ctx =
                            StarlarkActionErrorContext::new_from_command_execution(
                                last_command,
                                outputs_artifacts,
                            );

                        let error_handler_result = eval.eval_function(
                            heap.access_owned_frozen_value(error_handler),
                            &[heap.alloc(error_handler_ctx)],
                            &[],
                        );

                        let data = match error_handler_result {
                            Ok(result) => match ActionSubErrorResult::unpack_value_err(result) {
                                Ok(result) => Data::SubErrors(ActionSubErrors {
                                    sub_errors: result
                                        .items
                                        .into_iter()
                                        .map(|s| s.to_proto())
                                        .collect(),
                                }),
                                Err(_) => Data::HandlerInvocationError(format!(
                                    "{}",
                                    ActionErrorHandlerError::TypeError(
                                        ActionSubErrorResult::starlark_type_repr(),
                                        result.get_type().to_owned()
                                    )
                                )),
                            },
                            Err(e) => {
                                let e = buck2_error::Error::from(e).context("Error handler failed");
                                Data::HandlerInvocationError(format!("{e:#}"))
                            }
                        };
                        (
                            Some(ActionErrorDiagnostics { data: Some(data) }),
                            buck2_data::ActionErrorHandlerExecutionEnd {},
                        )
                    })
                })
        }
        None => None,
    }
}

pub struct BuildKeyActivationData {
    pub action_with_extra_data: ActionWithExtraData,
    pub duration: NodeDuration,
    pub waiting_data: WaitingData,
    pub spans: SmallVec<[SpanId; 1]>,
}

#[derive(Clone)]
pub struct ActionWithExtraData {
    pub action: Arc<RegisteredAction>,
    pub extra_data: ActionExtraData,
}

#[derive(Clone)]
pub struct ActionExtraData {
    pub execution_kind: buck2_data::ActionExecutionKind,
    pub execution_time_ms: Option<u64>,
    pub output_size: u64,
    pub target_rule_type_name: Option<String>,
    pub action_digest: Option<String>,
    pub invalidation_info: Option<buck2_data::CommandInvalidationInfo>,
    /// RE platform name if the action ran remotely.
    pub re_platform_name: Option<String>,
}

struct ActionExecutionData {
    action_result: buck2_error::Result<ActionOutputs>,
    wall_time: Option<std::time::Duration>,
    queue_duration: Option<std::time::Duration>,
    memory_peak: Option<u64>,
    extra_data: ActionExtraData,
    waiting_data: WaitingData,
}

/// The cost of these calls are particularly critical. To control the cost (particularly size) of these calls
/// we drop the `async_trait` common in other `*Calculation` types and avoid `async fn` (for
/// build_action/build_artifact at least).
impl ActionCalculation {
    pub async fn get_action(
        ctx: &mut DiceComputations<'_>,
        action_key: &ActionKey,
    ) -> buck2_error::Result<Arc<RegisteredAction>> {
        // In the typical case, this lookup is only going to require a single deferred holder lookup. There's three cases:
        // 1. a normal action defined in analysis: lookup the holder for that analysis, get the action
        // 2. an action bound to a dynamic_output and then bound to an action there: the initial holder_key will actually
        //    point to the dynamic_output (not the analysis that first created the action key) and then the action will be found there
        // 3. an action bound to a dynamic_output, and then in that dynamic_output bound to another dynamic_output: only in this case
        //    will the initial lookup not find the key and we'll recurse.
        //
        // We could introduce a dice key to cache the recursive resolution, but that would only be valuable if we had long nested chains
        // of dynamic_output that were re-binding artifacts. In practice we've not yet encountered that.
        let deferred_holder = lookup_deferred_holder(ctx, action_key.holder_key()).await?;
        match deferred_holder.lookup_action(action_key)? {
            ActionLookup::Action(action) => Ok(action),
            ActionLookup::Deferred(action_key) => {
                fn get_action_recurse<'a>(
                    ctx: &'a mut DiceComputations<'_>,
                    action_key: &'a ActionKey,
                ) -> BoxFuture<'a, buck2_error::Result<Arc<RegisteredAction>>> {
                    async move { ActionCalculation::get_action(ctx, action_key).await }.boxed()
                }
                get_action_recurse(ctx, &action_key).await
            }
        }
    }

    pub fn build_action<'a, 'd>(
        ctx: &'a mut DiceComputations<'d>,
        action_key: &ActionKey,
    ) -> impl Future<Output = buck2_error::Result<ActionOutputs>> + use<'a, 'd> {
        ctx.compute(BuildKey::ref_cast(action_key)).map(|v| v?)
    }

    pub fn build_artifact<'a, 'd>(
        ctx: &'a mut DiceComputations<'d>,
        artifact: &BuildArtifact,
    ) -> impl Future<Output = buck2_error::Result<ActionOutputs>> + use<'a, 'd> {
        Self::build_action(ctx, artifact.key())
    }
}

#[derive(
    Clone, Dupe, Display, Debug, Eq, PartialEq, Hash, Allocative, RefCast, Pagable
)]
#[repr(transparent)]
#[pagable_typetag(dice::DiceKeyDyn)]
pub struct BuildKey(pub ActionKey);

#[async_trait]
impl Key for BuildKey {
    type Value = buck2_error::Result<ActionOutputs>;

    async fn compute(
        &self,
        ctx: &mut DiceComputations,
        cancellation: &CancellationContext,
    ) -> Self::Value {
        build_action_impl(ctx, cancellation, &self.0).await
    }

    fn equality(x: &Self::Value, y: &Self::Value) -> bool {
        match (x, y) {
            (Ok(x), Ok(y)) => x == y,
            _ => false,
        }
    }

    fn validity(x: &Self::Value) -> bool {
        // we don't cache any kind of errors. Ideally, we could try to distinguish different
        // error types and try to cache non-transient error types, but practically there
        // are too many unknowns that may cause more harm than good if we cached errors.
        // So, don't cache it for now, until someday we decide to really need to.
        x.is_ok()
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        OkPagableValueSerialize::<Self::Value>::new()
    }
}

/// Invalidates `keys` in `ctx`, so the next computation of each one re-executes instead of
/// returning DICE's cached result.
pub fn invalidate_actions_for_recovery(
    keys: &[ActionKey],
    ctx: &mut DiceTransactionUpdater,
) -> buck2_error::Result<()> {
    let build_keys: Vec<BuildKey> = keys.iter().map(|key| BuildKey(key.dupe())).collect();
    ctx.changed(build_keys)?;
    Ok(())
}

async fn command_execution_report_to_proto(
    report: &CommandExecutionReport,
    allow_omit_details: bool,
) -> buck2_data::CommandExecution {
    let details = command_details(report, allow_omit_details).await;

    let status = match &report.status {
        CommandExecutionStatus::Success { .. } => buck2_data::command_execution::Success {}.into(),
        CommandExecutionStatus::Cancelled { .. } => {
            buck2_data::command_execution::Cancelled {}.into()
        }
        CommandExecutionStatus::Failure { .. } => buck2_data::command_execution::Failure {}.into(),
        CommandExecutionStatus::WorkerFailure { .. } => {
            buck2_data::command_execution::WorkerFailure {}.into()
        }
        CommandExecutionStatus::TimedOut { duration, .. } => {
            buck2_data::command_execution::Timeout {
                duration: (*duration).try_into().ok(),
            }
            .into()
        }
        CommandExecutionStatus::Error { stage, error, .. } => {
            buck2_data::command_execution::Error {
                stage: (*stage).to_owned(),
                error: format!("{error:#}"),
            }
            .into()
        }
    };

    buck2_data::CommandExecution {
        details: Some(details),
        status: Some(status),
        inline_environment_metadata: Some(report.inline_environment_metadata),
    }
}

pub async fn command_details(
    command: &CommandExecutionReport,
    allow_omit_details: bool,
) -> buck2_data::CommandExecutionDetails {
    // If the top-level command failed then we don't want to omit any details. If it succeeded and
    // so did this command (it could succeed while not having a success here if we have rejected
    // executions), then we'll strip non-relevant stuff.
    let omit_details =
        allow_omit_details && matches!(command.status, CommandExecutionStatus::Success { .. });

    let signed_exit_code = command.exit_code;

    let stdout;
    let stderr;

    if omit_details {
        stdout = Default::default();
        stderr = command.std_streams.to_lossy_stderr().await;
    } else {
        let pair = command.std_streams.to_lossy().await;
        stdout = pair.stdout;
        stderr = pair.stderr;
    };

    let command_kind = command
        .status
        .execution_kind()
        .map(|k| k.to_proto(omit_details));

    buck2_data::CommandExecutionDetails {
        cmd_stdout: stdout,
        cmd_stderr: stderr,
        command_kind,
        signed_exit_code,
        metadata: Some(command.timing.to_proto()),
        additional_message: command.additional_message.clone(),
    }
}

pub async fn get_target_rule_type_name(
    ctx: &mut DiceComputations<'_>,
    label: &ConfiguredTargetLabel,
) -> buck2_error::Result<String> {
    Ok(ctx
        .get_configured_target_node(label)
        .await
        .require_compatible()?
        .underlying_rule_type()
        .name()
        .to_owned())
}

#[cfg(test)]
mod cas_missing_recovery_tests {
    use std::collections::HashSet;

    use buck2_artifact::actions::key::ActionIndex;
    use buck2_artifact::artifact::artifact_type::Artifact;
    use buck2_artifact::artifact::artifact_type::testing::BuildArtifactTestingExt;
    use buck2_artifact::artifact::build_artifact::BuildArtifact;
    use buck2_artifact::artifact::source_artifact::SourceArtifact;
    use buck2_common::cas_digest::CasDigest;
    use buck2_common::cas_digest::CasDigestConfig;
    use buck2_common::file_ops::metadata::FileMetadata;
    use buck2_common::file_ops::metadata::TrackedFileDigest;
    use buck2_core::configuration::data::ConfigurationData;
    use buck2_core::fs::project_rel_path::ProjectRelativePathBuf;
    use buck2_core::package::source_path::SourcePath;
    use buck2_core::target::configured_target_label::ConfiguredTargetLabel;
    use buck2_execute::digest_config::DigestConfig;
    use buck2_execute::directory::ActionDirectoryBuilder;
    use buck2_execute::directory::insert_file;

    use super::*;

    fn target() -> ConfiguredTargetLabel {
        ConfiguredTargetLabel::testing_parse("cell//pkg:foo", ConfigurationData::testing_new())
    }

    fn build_artifact(name: &str, id: u32) -> (Artifact, ActionKey) {
        let artifact = BuildArtifact::testing_new(target(), name, ActionIndex::new(id));
        let key = artifact.key().dupe();
        (Artifact::from(artifact), key)
    }

    fn source_artifact(name: &str) -> Artifact {
        Artifact::from(SourceArtifact::new(SourcePath::testing_new(
            "cell//pkg",
            name,
        )))
    }

    fn file_digest(byte: u8) -> TrackedFileDigest {
        TrackedFileDigest::new(
            CasDigest::new_sha1([byte; 20], 1),
            CasDigestConfig::testing_default(),
        )
    }

    #[test]
    fn index_artifact_value_digests_indexes_a_file_value() {
        let (_, action_key) = build_artifact("out", 0);
        let value = ArtifactValue::file(FileMetadata {
            digest: file_digest(1),
            is_executable: false,
        });

        let mut index = HashMap::new();
        index_artifact_value_digests(&value, &action_key, &mut index);

        assert_eq!(index.get(&file_digest(1).to_string()), Some(&action_key));
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn index_artifact_value_digests_maps_every_leaf_in_a_tree_to_the_same_action() {
        let (_, action_key) = build_artifact("out", 0);

        let mut builder = ActionDirectoryBuilder::empty();
        insert_file(
            &mut builder,
            ProjectRelativePathBuf::unchecked_new("out/a".to_owned()),
            FileMetadata {
                digest: file_digest(1),
                is_executable: false,
            },
        )
        .unwrap();
        insert_file(
            &mut builder,
            ProjectRelativePathBuf::unchecked_new("out/b".to_owned()),
            FileMetadata {
                digest: file_digest(2),
                is_executable: false,
            },
        )
        .unwrap();
        let dir = builder
            .fingerprint(DigestConfig::testing_default().as_directory_serializer())
            .shared(&*buck2_execute::directory::INTERNER);
        let value = ArtifactValue::dir(dir);

        let mut index = HashMap::new();
        index_artifact_value_digests(&value, &action_key, &mut index);

        assert_eq!(index.get(&file_digest(1).to_string()), Some(&action_key));
        assert_eq!(index.get(&file_digest(2).to_string()), Some(&action_key));
    }

    #[test]
    fn index_missing_digest_candidates_skips_source_artifacts() {
        let (build, build_key) = build_artifact("out", 0);
        let build_value = ArtifactValue::file(FileMetadata {
            digest: file_digest(1),
            is_executable: false,
        });

        let source = source_artifact("src");
        let source_value = ArtifactValue::file(FileMetadata {
            digest: file_digest(2),
            is_executable: false,
        });

        let mut ensured_inputs = BuckIndexMap::new();
        ensured_inputs.insert(
            ArtifactGroup::Artifact(build.dupe()),
            ArtifactGroupValues::from_artifact(build, build_value),
        );
        ensured_inputs.insert(
            ArtifactGroup::Artifact(source.dupe()),
            ArtifactGroupValues::from_artifact(source, source_value),
        );

        let index = index_missing_digest_candidates(&ensured_inputs);

        assert_eq!(index.get(&file_digest(1).to_string()), Some(&build_key));
        assert_eq!(index.get(&file_digest(2).to_string()), None);
        assert_eq!(index.len(), 1);
    }

    fn missing_entry(path: &str, digest: TrackedFileDigest) -> (ProjectRelativePathBuf, String) {
        (
            ProjectRelativePathBuf::unchecked_new(path.to_owned()),
            digest.to_string(),
        )
    }

    fn ensured_inputs_with(
        artifact: Artifact,
        value: ArtifactValue,
    ) -> BuckIndexMap<ArtifactGroup, ArtifactGroupValues> {
        let mut ensured_inputs = BuckIndexMap::new();
        ensured_inputs.insert(
            ArtifactGroup::Artifact(artifact.dupe()),
            ArtifactGroupValues::from_artifact(artifact, value),
        );
        ensured_inputs
    }

    fn command_execution_error(context: Option<MissingCasDigests>) -> ExecuteError {
        let mut error = buck2_error::buck2_error!(
            buck2_error::ErrorTag::ReCasArtifactMissingRecoverable,
            "artifact missing"
        );
        if let Some(context) = context {
            error = error.context(context);
        }
        ExecuteError::CommandExecutionError {
            action_outputs: ActionOutputs::new(BuckIndexMap::new()),
            error: Some(error),
        }
    }

    #[test]
    fn resolve_outcome_arms_the_action_that_produced_a_missing_digest() {
        let (build, build_key) = build_artifact("out", 0);
        let value = ArtifactValue::file(FileMetadata {
            digest: file_digest(1),
            is_executable: false,
        });
        let ensured_inputs = ensured_inputs_with(build, value);
        let missing = MissingCasDigests {
            missing: vec![missing_entry("out", file_digest(1))],
        };

        let outcome = resolve_cas_missing_recovery_outcome(&missing, &ensured_inputs);

        assert_eq!(outcome.producing_actions, vec![build_key]);
        assert_eq!(outcome.unattributed, Vec::new());
        assert_eq!(outcome.guidance, CAS_MISSING_RECOVERY_QUEUED);
    }

    #[test]
    fn resolve_outcome_leaves_a_digest_with_no_producing_action_unattributed() {
        // The digest here belongs to no input this action declared, e.g. a source file. This
        // must not be treated as an error: the caller falls through to the fatal guidance.
        let (build, _) = build_artifact("out", 0);
        let value = ArtifactValue::file(FileMetadata {
            digest: file_digest(1),
            is_executable: false,
        });
        let ensured_inputs = ensured_inputs_with(build, value);
        let missing = MissingCasDigests {
            missing: vec![missing_entry("src", file_digest(2))],
        };

        let outcome = resolve_cas_missing_recovery_outcome(&missing, &ensured_inputs);

        assert_eq!(outcome.producing_actions, Vec::new());
        assert_eq!(
            outcome.unattributed,
            vec![missing_entry("src", file_digest(2))]
        );
        assert_eq!(outcome.guidance, CAS_MISSING_RECOVERY_UNATTRIBUTED);
    }

    #[test]
    fn resolve_outcome_is_queued_when_only_some_digests_attribute() {
        // A partial match still lets recovery repair what it can identify: the guidance reflects
        // that at least one producing action was found, even though another digest in the same
        // failure resolved to nothing.
        let (build, build_key) = build_artifact("out", 0);
        let value = ArtifactValue::file(FileMetadata {
            digest: file_digest(1),
            is_executable: false,
        });
        let ensured_inputs = ensured_inputs_with(build, value);
        let missing = MissingCasDigests {
            missing: vec![
                missing_entry("out", file_digest(1)),
                missing_entry("src", file_digest(2)),
            ],
        };

        let outcome = resolve_cas_missing_recovery_outcome(&missing, &ensured_inputs);

        assert_eq!(outcome.producing_actions, vec![build_key]);
        assert_eq!(
            outcome.unattributed,
            vec![missing_entry("src", file_digest(2))]
        );
        assert_eq!(outcome.guidance, CAS_MISSING_RECOVERY_QUEUED);
    }

    fn message_of(error: &ExecuteError) -> String {
        let ExecuteError::CommandExecutionError {
            error: Some(inner), ..
        } = error
        else {
            panic!("expected a CommandExecutionError");
        };
        format!("{inner}")
    }

    #[test]
    fn apply_outcome_passes_through_errors_without_typed_context() {
        let registry = CasMissingRecoveryRegistry::new();
        let error = command_execution_error(None);

        let result =
            apply_cas_missing_recovery_outcome(&registry, error, Some(&BuckIndexMap::new()));

        assert_eq!(message_of(&result), "artifact missing");
    }

    #[test]
    fn apply_outcome_passes_through_non_command_execution_errors() {
        let registry = CasMissingRecoveryRegistry::new();
        let error = ExecuteError::MissingOutputs { declared: vec![] };

        let result =
            apply_cas_missing_recovery_outcome(&registry, error, Some(&BuckIndexMap::new()));

        assert!(matches!(result, ExecuteError::MissingOutputs { .. }));
    }

    #[test]
    fn apply_outcome_arms_the_registry_and_replaces_the_guidance_when_attributed() {
        let registry = CasMissingRecoveryRegistry::new();
        let (build, build_key) = build_artifact("out", 0);
        let value = ArtifactValue::file(FileMetadata {
            digest: file_digest(1),
            is_executable: false,
        });
        let ensured_inputs = ensured_inputs_with(build, value);
        let error = command_execution_error(Some(MissingCasDigests {
            missing: vec![missing_entry("out", file_digest(1))],
        }));

        let result = apply_cas_missing_recovery_outcome(&registry, error, Some(&ensured_inputs));

        assert_eq!(registry.keys_eligible_for_recovery(1), vec![build_key]);
        assert!(message_of(&result).contains(CAS_MISSING_RECOVERY_QUEUED));
    }

    #[test]
    fn apply_outcome_leaves_the_registry_untouched_when_unattributed() {
        let registry = CasMissingRecoveryRegistry::new();
        let (build, _) = build_artifact("out", 0);
        let value = ArtifactValue::file(FileMetadata {
            digest: file_digest(1),
            is_executable: false,
        });
        let ensured_inputs = ensured_inputs_with(build, value);
        let error = command_execution_error(Some(MissingCasDigests {
            missing: vec![missing_entry("src", file_digest(2))],
        }));

        let result = apply_cas_missing_recovery_outcome(&registry, error, Some(&ensured_inputs));

        assert_eq!(
            registry.keys_eligible_for_recovery(1),
            Vec::<ActionKey>::new()
        );
        assert!(message_of(&result).contains(CAS_MISSING_RECOVERY_UNATTRIBUTED));
    }

    #[test]
    fn apply_outcome_passes_through_the_failure_when_recovery_is_disabled() {
        // The failure here is the one that arms a producing action and rewrites the guidance for a
        // build that opted in. Recovery is opt-in, so a daemon that did not opt in must build
        // exactly as it does with the feature absent: the user sees the message the executor
        // produced, and the next transaction re-executes nothing.
        let registry = CasMissingRecoveryRegistry::new();
        let error = command_execution_error(Some(MissingCasDigests {
            missing: vec![missing_entry("out", file_digest(1))],
        }));

        let result = apply_cas_missing_recovery_outcome(&registry, error, None);

        assert_eq!(message_of(&result), "artifact missing");
        assert_eq!(
            registry.keys_eligible_for_recovery(1),
            Vec::<ActionKey>::new()
        );
    }

    #[test]
    fn charge_repair_ignores_an_armed_key_the_batch_did_not_select() {
        // The registry can hold more than one armed key at once — an action arms only when it is
        // in the batch this transaction was handed, never merely because it is armed somewhere in
        // the daemon-lifetime registry.
        let registry = CasMissingRecoveryRegistry::new();
        let (_, in_batch) = build_artifact("selected", 0);
        let (_, armed_elsewhere) = build_artifact("armed_elsewhere", 1);
        registry.record_missing(in_batch.dupe());
        registry.record_missing(armed_elsewhere.dupe());
        let batch = CasRecoveryBatch::new(HashSet::from([in_batch.dupe()]));

        charge_cas_missing_recovery_repair(&registry, &batch, &armed_elsewhere);

        let still_eligible: HashSet<ActionKey> =
            registry.keys_eligible_for_recovery(1).into_iter().collect();
        assert_eq!(
            still_eligible,
            HashSet::from([in_batch.dupe(), armed_elsewhere.dupe()])
        );
    }

    #[test]
    fn charge_repair_charges_a_key_the_batch_selected() {
        let registry = CasMissingRecoveryRegistry::new();
        let (_, in_batch) = build_artifact("selected", 0);
        registry.record_missing(in_batch.dupe());
        let batch = CasRecoveryBatch::new(HashSet::from([in_batch.dupe()]));

        charge_cas_missing_recovery_repair(&registry, &batch, &in_batch);

        assert_eq!(
            registry.keys_eligible_for_recovery(1),
            Vec::<ActionKey>::new()
        );
    }

    #[test]
    fn charge_repair_against_an_empty_batch_charges_nothing() {
        // A build with recovery disabled attaches an empty batch to every action it executes, as
        // does a transaction the registry armed nothing for.
        let registry = CasMissingRecoveryRegistry::new();
        let (_, key) = build_artifact("out", 0);
        registry.record_missing(key.dupe());
        let batch = CasRecoveryBatch::empty();

        charge_cas_missing_recovery_repair(&registry, &batch, &key);

        assert_eq!(registry.keys_eligible_for_recovery(1), vec![key]);
    }

    #[test]
    fn a_build_with_recovery_enabled_retains_the_inputs_attribution_resolves_digests_against() {
        let (build, build_key) = build_artifact("out", 0);
        let value = ArtifactValue::file(FileMetadata {
            digest: file_digest(1),
            is_executable: false,
        });
        let ensured_inputs = ensured_inputs_with(build, value);
        let knobs = RunActionKnobs {
            cas_missing_recovery_enabled: true,
            ..Default::default()
        };

        let retained = inputs_for_cas_missing_recovery(&knobs, &ensured_inputs)
            .expect("a build that opted into recovery retains its inputs");

        assert_eq!(
            index_missing_digest_candidates(&retained).get(&file_digest(1).to_string()),
            Some(&build_key)
        );
    }

    #[test]
    fn a_build_with_recovery_disabled_retains_no_inputs() {
        let (build, _) = build_artifact("out", 0);
        let value = ArtifactValue::file(FileMetadata {
            digest: file_digest(1),
            is_executable: false,
        });
        let ensured_inputs = ensured_inputs_with(build, value);
        let knobs = RunActionKnobs {
            cas_missing_recovery_enabled: false,
            ..Default::default()
        };

        assert!(inputs_for_cas_missing_recovery(&knobs, &ensured_inputs).is_none());
    }
}
