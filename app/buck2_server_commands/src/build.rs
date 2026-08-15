/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use buck2_build_api::actions::artifact::get_artifact_fs::GetArtifactFs;
use buck2_build_api::actions::cas_missing_recovery::HasCasMissingRecoveryConfig;
use buck2_build_api::actions::cas_missing_recovery::HasCasMissingRecoveryRegistry;
use buck2_build_api::actions::cas_missing_recovery::HasCasRecoveryBatch;
use buck2_build_api::actions::cas_missing_recovery::stage_cas_recovery_round;
use buck2_build_api::actions::impls::run_action_knobs::HasRunActionKnobs;
use buck2_build_api::build;
use buck2_build_api::build::AsyncBuildTargetResultBuilder;
use buck2_build_api::build::BuildEvent;
use buck2_build_api::build::BuildEventConsumer;
use buck2_build_api::build::BuildProviderType;
use buck2_build_api::build::BuildTargetResult;
use buck2_build_api::build::ConfiguredBuildEventVariant;
use buck2_build_api::build::HasCreateUnhashedSymlinkLock;
use buck2_build_api::build::ProvidersToBuild;
use buck2_build_api::build::build_report::build_report_opts;
use buck2_build_api::build::build_report::initialize_streaming_build_report;
use buck2_build_api::build::build_report::stream_build_report;
use buck2_build_api::build::build_report::write_build_report;
use buck2_build_api::build::detailed_aggregated_metrics::dice::HasDetailedAggregatedMetrics;
use buck2_build_api::build::detailed_aggregated_metrics::types::ActionGraphSketchResult;
use buck2_build_api::build::detailed_aggregated_metrics::types::ArtifactPathSketchResult;
use buck2_build_api::build::detailed_aggregated_metrics::types::DetailedAggregatedMetrics;
use buck2_build_api::build::graph_properties::GraphPropertiesOptions;
use buck2_build_api::materialize::MaterializationAndUploadContext;
use buck2_cli_proto::CommonBuildOptions;
use buck2_cli_proto::build_request::BuildProviders;
use buck2_cli_proto::build_request::Materializations;
use buck2_cli_proto::build_request::Uploads;
use buck2_cli_proto::build_request::build_providers::Action as BuildProviderAction;
use buck2_common::dice::cells::HasCellResolver;
use buck2_common::legacy_configs::dice::HasLegacyConfigs;
use buck2_common::legacy_configs::key::BuckconfigKeyRef;
use buck2_common::liveliness_observer::LivelinessObserver;
use buck2_common::liveliness_observer::TimeoutLivelinessObserver;
use buck2_common::pattern::parse_from_cli::parse_patterns_with_modifiers_from_cli_args;
use buck2_common::pattern::resolve::ResolveTargetPatterns;
use buck2_common::pattern::resolve::ResolvedPattern;
use buck2_core::global_cfg_options::GlobalCfgOptions;
use buck2_core::package::PackageLabelWithModifiers;
use buck2_core::pattern::pattern::Modifiers;
use buck2_core::pattern::pattern::ModifiersError;
use buck2_core::pattern::pattern::PackageSpec;
use buck2_core::pattern::pattern::ParsedPatternWithModifiers;
use buck2_core::pattern::pattern_type::ConfiguredProvidersPatternExtra;
use buck2_core::pattern::pattern_type::ProvidersPatternExtra;
use buck2_core::provider::label::ConfiguredProvidersLabel;
use buck2_core::provider::label::ProvidersLabel;
use buck2_core::provider::label::ProvidersName;
use buck2_core::soft_error;
use buck2_core::target::label::label::TargetLabel;
use buck2_data::BuildResult;
use buck2_data::ToProtoMessage;
use buck2_error::BuckErrorContext;
use buck2_error::internal_error;
use buck2_events::dispatch::console_message;
use buck2_events::dispatch::instant_event;
use buck2_events::dispatch::span_async;
use buck2_node::configured_universe::CqueryUniverse;
use buck2_node::load_patterns::MissingTargetBehavior;
use buck2_node::nodes::frontend::TargetGraphCalculation;
use buck2_node::target_calculation::ConfiguredTargetCalculation;
use buck2_server_ctx::commands::send_target_cfg_event;
use buck2_server_ctx::ctx::ServerCommandContextTrait;
use buck2_server_ctx::partial_result_dispatcher::NoPartialResult;
use buck2_server_ctx::partial_result_dispatcher::PartialResultDispatcher;
use buck2_server_ctx::target_resolution_config::TargetResolutionConfig;
use buck2_server_ctx::template::ServerCommandTemplate;
use buck2_server_ctx::template::run_server_command;
use dice::DiceTransaction;
use dice::LinearRecomputeDiceComputations;
use dupe::Dupe;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use futures::stream::futures_unordered::FuturesUnordered;
use itertools::Either;
use itertools::Itertools;
use tokio::sync::mpsc::UnboundedSender;

use crate::build::result_report::ResultReporter;
use crate::build::result_report::ResultReporterOptions;
use crate::build::unhashed_outputs::create_unhashed_outputs;

mod result_report;
mod unhashed_outputs;

pub(crate) async fn build_command(
    ctx: &dyn ServerCommandContextTrait,
    partial_result_dispatcher: PartialResultDispatcher<NoPartialResult>,
    req: buck2_cli_proto::BuildRequest,
) -> buck2_error::Result<buck2_cli_proto::BuildResponse> {
    run_server_command(BuildServerCommand { req }, ctx, partial_result_dispatcher).await
}

struct BuildServerCommand {
    req: buck2_cli_proto::BuildRequest,
}

#[async_trait]
impl ServerCommandTemplate for BuildServerCommand {
    type StartEvent = buck2_data::BuildCommandStart;
    type EndEvent = buck2_data::BuildCommandEnd;
    type Response = buck2_cli_proto::BuildResponse;
    type PartialResult = NoPartialResult;

    fn end_event(&self, _response: &buck2_error::Result<Self::Response>) -> Self::EndEvent {
        buck2_data::BuildCommandEnd {
            unresolved_target_patterns: self
                .req
                .target_patterns
                .iter()
                .map(|p| buck2_data::TargetPattern { value: p.clone() })
                .collect(),
        }
    }

    async fn command(
        &self,
        server_ctx: &dyn ServerCommandContextTrait,
        _partial_result_dispatcher: PartialResultDispatcher<Self::PartialResult>,
        ctx: DiceTransaction,
    ) -> buck2_error::Result<Self::Response> {
        build(server_ctx, ctx, &self.req).await
    }

    fn build_result(&self, response: &Self::Response) -> Option<BuildResult> {
        Some(BuildResult {
            build_completed: response.errors.is_empty(),
        })
    }
}

fn expect_build_opts(req: &buck2_cli_proto::BuildRequest) -> &CommonBuildOptions {
    req.build_opts.as_ref().expect("should have build options")
}

#[derive(buck2_error::Error, Debug)]
#[buck2(tag = Input)]
#[error(
    "`buck2 run` will require a `--` separator before target arguments in the future. \
     Please use `buck2 run <target> -- <args>` instead of `buck2 run <target> <args>`"
)]
struct RunArgsMissingSeparator;

async fn build(
    server_ctx: &dyn ServerCommandContextTrait,
    mut ctx: DiceTransaction,
    request: &buck2_cli_proto::BuildRequest,
) -> buck2_error::Result<buck2_cli_proto::BuildResponse> {
    if request.run_args_missing_separator {
        soft_error!(
            "run_args_without_separator",
            RunArgsMissingSeparator.into(),
            quiet: false,
            deprecation: true,
            error_on_oss: true,
        )?;
    }

    let cwd = server_ctx.working_dir();

    let build_opts: &CommonBuildOptions = expect_build_opts(request);

    let timeout = request
        .timeout
        .as_ref()
        .map(|t| (*t).try_into())
        .transpose()
        .with_buck_error_context(|| "Invalid `duration`")?;

    let timeout_observer = timeout.map(|timeout| {
        Arc::new(TimeoutLivelinessObserver::new(timeout)) as Arc<dyn LivelinessObserver>
    });

    let cell_resolver = ctx.ctx().get_cell_resolver().await?;

    let parsed_patterns_with_modifiers: Vec<
        ParsedPatternWithModifiers<ConfiguredProvidersPatternExtra>,
    > = parse_patterns_with_modifiers_from_cli_args(&mut ctx.ctx(), &request.target_patterns, cwd)
        .await?;

    let has_pattern_modifiers = parsed_patterns_with_modifiers
        .iter()
        .any(|p| p.modifiers.as_slice().is_some());

    server_ctx.log_target_pattern_with_modifiers(&parsed_patterns_with_modifiers);

    let build_providers = Arc::new(request.build_providers.unwrap());

    let final_artifact_materializations =
        Materializations::try_from(request.final_artifact_materializations)
            .with_buck_error_context(|| "Invalid final_artifact_materializations")
            .unwrap();
    let final_artifact_uploads = Uploads::try_from(request.final_artifact_uploads)
        .with_buck_error_context(|| "Invalid final_artifact_uploads")
        .unwrap();

    let want_configured_graph_size = ctx
        .ctx()
        .parse_legacy_config_property(
            cell_resolver.root_cell(),
            BuckconfigKeyRef {
                section: "buck2",
                property: "log_configured_graph_size",
            },
        )
        .await?
        .unwrap_or_default();

    let want_configured_graph_sketch = ctx
        .ctx()
        .parse_legacy_config_property(
            cell_resolver.root_cell(),
            BuckconfigKeyRef {
                section: "buck2",
                property: "log_configured_graph_sketch",
            },
        )
        .await?
        .unwrap_or_default();

    let want_total_configured_graph_sketch = ctx
        .ctx()
        .parse_legacy_config_property(
            cell_resolver.root_cell(),
            BuckconfigKeyRef {
                section: "buck2",
                property: "log_total_configured_graph_sketch",
            },
        )
        .await?
        .unwrap_or_default();

    let want_retained_analysis_memory_sketch = ctx
        .ctx()
        .parse_legacy_config_property(
            cell_resolver.root_cell(),
            BuckconfigKeyRef {
                section: "buck2",
                property: "log_retained_analysis_memory_sketch",
            },
        )
        .await?
        .unwrap_or_default();

    let want_action_graph_sketch = ctx
        .ctx()
        .parse_legacy_config_property(
            cell_resolver.root_cell(),
            BuckconfigKeyRef {
                section: "buck2",
                property: "log_action_graph_sketch",
            },
        )
        .await?
        .unwrap_or_default();

    let want_peak_analysis_memory_sketch = ctx
        .ctx()
        .parse_legacy_config_property(
            cell_resolver.root_cell(),
            BuckconfigKeyRef {
                section: "buck2",
                property: "log_peak_analysis_memory_sketch",
            },
        )
        .await?
        .unwrap_or_default();

    let want_peak_load_memory_sketch = ctx
        .ctx()
        .parse_legacy_config_property(
            cell_resolver.root_cell(),
            BuckconfigKeyRef {
                section: "buck2",
                property: "log_peak_load_memory_sketch",
            },
        )
        .await?
        .unwrap_or_default();

    let want_artifact_count_sketch: bool = ctx
        .ctx()
        .parse_legacy_config_property(
            cell_resolver.root_cell(),
            BuckconfigKeyRef {
                section: "buck2",
                property: "log_artifact_count_sketch",
            },
        )
        .await?
        .unwrap_or_default();

    let want_artifact_size_sketch: bool = ctx
        .ctx()
        .parse_legacy_config_property(
            cell_resolver.root_cell(),
            BuckconfigKeyRef {
                section: "buck2",
                property: "log_artifact_size_sketch",
            },
        )
        .await?
        .unwrap_or_default();

    let want_log_sketch_cardinalities: bool = ctx
        .ctx()
        .parse_legacy_config_property(
            cell_resolver.root_cell(),
            BuckconfigKeyRef {
                section: "buck2",
                property: "log_sketch_cardinalities",
            },
        )
        .await?
        .unwrap_or_default();

    let graph_properties = GraphPropertiesOptions {
        configured_graph_size: want_configured_graph_size,
        configured_graph_sketch: want_configured_graph_sketch,
        total_configured_graph_sketch: want_total_configured_graph_sketch,
        retained_analysis_memory_sketch: want_retained_analysis_memory_sketch,
        peak_analysis_memory_sketch: want_peak_analysis_memory_sketch,
        peak_load_memory_sketch: want_peak_load_memory_sketch,
        action_graph_sketch: want_action_graph_sketch,
        artifact_count_sketch: want_artifact_count_sketch,
        artifact_size_sketch: want_artifact_size_sketch,
        log_sketch_cardinalities: want_log_sketch_cardinalities,
    };

    let providers_to_skip_in_artifact_path_sketch: HashSet<BuildProviderType> = ctx
        .ctx()
        .parse_legacy_config_list_property::<SkipProvider>(
            cell_resolver.root_cell(),
            BuckconfigKeyRef {
                section: "buck2",
                property: "providers_to_skip_in_artifact_path_sketch",
            },
        )
        .await?
        .unwrap_or_default()
        .into_iter()
        .map(|s| s.into_build_provider_type())
        .collect();

    let build_start = Instant::now();
    let materialization_and_upload =
        (final_artifact_materializations, final_artifact_uploads).into();

    let build_result = build_until_cas_missing_recovery_converges(
        &mut ctx,
        server_ctx,
        request,
        build_opts,
        &parsed_patterns_with_modifiers,
        has_pattern_modifiers,
        build_providers,
        materialization_and_upload,
        graph_properties.dupe(),
        timeout_observer.as_ref(),
        build_start,
    )
    .await?;

    let want_detailed_metrics = ctx
        .ctx()
        .parse_legacy_config_property(
            cell_resolver.root_cell(),
            BuckconfigKeyRef {
                section: "buck2",
                property: "detailed_aggregated_metrics",
            },
        )
        .await?
        .unwrap_or_default();

    // We need to take per-build events if we want either detailed metrics or action graph sketch
    // or artifact path sketch
    let need_events = want_detailed_metrics
        || graph_properties.action_graph_sketch
        || graph_properties.artifact_count_sketch
        || graph_properties.artifact_size_sketch;
    let mut events = if need_events {
        Some(ctx.ctx().take_per_build_events()?)
    } else {
        None
    };

    // Compute action graph sketch independently if requested (doesn't require detailed_metrics)
    let action_graph_sketch_result = if graph_properties.action_graph_sketch {
        if let Some(ref events) = events {
            Some(ctx.ctx().compute_action_graph_sketch(events).await?)
        } else {
            None
        }
    } else {
        None
    };

    let artifact_path_sketch_result =
        if graph_properties.artifact_count_sketch || graph_properties.artifact_size_sketch {
            let events = events.as_ref().ok_or_else(|| {
                internal_error!("events should be Some when artifact path sketch is needed")
            })?;
            // Artifact path sketching re-`ensure_artifact_group`s each target's outputs. For a
            // target that hit the `--overall-timeout` deadline that would re-demand (and, via
            // DICE, restart) an action the deadline just cancelled, hanging the command past its
            // timeout. Those targets never finished building, so skip sketching them.
            let timed_out_targets: HashSet<ConfiguredProvidersLabel> = build_result
                .configured
                .iter()
                .filter_map(|(label, result)| {
                    result
                        .as_ref()
                        .filter(|r| r.timed_out())
                        .map(|_| label.clone())
                })
                .collect();
            let artifact_fs = ctx.ctx().get_artifact_fs().await?;
            Some(
                ctx.ctx()
                    .compute_artifact_path_sketch(
                        events,
                        artifact_fs,
                        providers_to_skip_in_artifact_path_sketch,
                        &timed_out_targets,
                        graph_properties.artifact_count_sketch,
                        graph_properties.artifact_size_sketch,
                    )
                    .await?,
            )
        } else {
            None
        };

    let detailed_metrics = if want_detailed_metrics {
        let events = events.take().ok_or_else(|| {
            internal_error!("events should be Some when detailed metrics is needed")
        })?;
        let mut metrics = ctx.ctx().compute_detailed_metrics(events).await?;
        for target_metric in &mut metrics.top_level_target_metrics {
            if let Some(Some(result)) = build_result.configured.get(&target_metric.target) {
                target_metric.wall_clock_completion_ms =
                    result.wall_clock_completion().map(|d| d.as_millis() as u64);
            }
        }
        instant_event(metrics.as_proto());
        Some(metrics)
    } else {
        None
    };

    send_target_cfg_event(
        server_ctx.events(),
        build_result.configured.keys(),
        &request.target_cfg,
    );

    process_build_result(
        server_ctx,
        ctx,
        request,
        build_result,
        detailed_metrics,
        action_graph_sketch_result,
        artifact_path_sketch_result,
        graph_properties,
    )
    .await
}

async fn process_streaming_build_result(
    server_ctx: &dyn ServerCommandContextTrait,
    ctx: DiceTransaction,
    request: &buck2_cli_proto::BuildRequest,
    build_result: BuildTargetResult,
    detailed_metrics: Option<DetailedAggregatedMetrics>,
    graph_properties_opts: GraphPropertiesOptions,
    action_graph_sketch_result: Option<ActionGraphSketchResult>,
) -> buck2_error::Result<()> {
    let build_opts = expect_build_opts(request);
    let fs = server_ctx.project_root();
    let cwd: &buck2_core::fs::project_rel_path::ProjectRelativePath = server_ctx.working_dir();
    let cell_resolver = ctx.ctx().get_cell_resolver().await?;
    let artifact_fs = ctx.ctx().get_artifact_fs().await?;

    let build_report_opts = build_report_opts(
        &mut ctx.ctx(),
        &cell_resolver,
        build_opts,
        graph_properties_opts,
    )
    .await?;

    stream_build_report(
        build_report_opts,
        &artifact_fs,
        &cell_resolver,
        fs,
        cwd,
        server_ctx.events().trace_id(),
        &build_result.configured,
        &build_result.configured_to_pattern_modifiers,
        &build_result.other_errors,
        detailed_metrics,
        action_graph_sketch_result,
        None, // no artifact_path_sketch_result for streaming build reports
    )?;

    Ok(())
}

async fn init_streaming_build_report(
    server_ctx: &dyn ServerCommandContextTrait,
    ctx: DiceTransaction,
    request: &buck2_cli_proto::BuildRequest,
    graph_properties_opts: GraphPropertiesOptions,
) -> buck2_error::Result<()> {
    let build_opts = expect_build_opts(request);
    let fs = server_ctx.project_root();
    let cwd: &buck2_core::fs::project_rel_path::ProjectRelativePath = server_ctx.working_dir();
    let cell_resolver = ctx.ctx().get_cell_resolver().await?;

    let build_report_opts = build_report_opts(
        &mut ctx.ctx(),
        &cell_resolver,
        build_opts,
        graph_properties_opts,
    )
    .await?;

    initialize_streaming_build_report(build_report_opts, fs, cwd)?;

    Ok(())
}

/// What a finished build round says about the one that should follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CasRecoveryStep {
    /// Hand the build result back. Either it succeeded, or nothing further can repair it.
    Finish,
    /// Hand the build result back, telling the user the round budget ran out first.
    ReportBudgetSpent,
    /// Invalidate what the registry has armed and build again.
    StageAnotherRound,
}

/// What a finished build round leaves for the next decision.
struct CasRecoveryRoundResult {
    /// Whether the build this round ran came back failing.
    build_failed: bool,
    /// Whether a CAS-missing failure arms its producing actions at all.
    recovery_enabled: bool,
    /// Whether the registry holds any action still under its attempt budget.
    anything_armed: bool,
    /// Whether the round finished without any action it staged going on to re-execute.
    repaired_nothing: bool,
    /// Whether an armed action sits outside what this round already staged. A failure uncovered
    /// deeper in the chain shows up here, which is what separates a build that has more to repair
    /// from one that is spinning on actions it does not depend on.
    armed_beyond_staged: bool,
    /// How many further rounds the command may stage.
    rounds_left: u32,
}

/// Decides whether a command repairs again after a round.
///
/// Repairing again is worth doing only while the build is failing, the registry has something left
/// it will repair, and either the last round accomplished something or the failure uncovered an
/// action no round has staged yet. A round that staged actions and repaired none of them, with
/// nothing newly armed, would stage the same actions to the same effect.
fn next_cas_recovery_step(round: CasRecoveryRoundResult) -> CasRecoveryStep {
    if !round.build_failed || !round.recovery_enabled || !round.anything_armed {
        return CasRecoveryStep::Finish;
    }

    if round.repaired_nothing && !round.armed_beyond_staged {
        return CasRecoveryStep::Finish;
    }

    if round.rounds_left == 0 {
        return CasRecoveryStep::ReportBudgetSpent;
    }

    CasRecoveryStep::StageAnotherRound
}

/// Builds the requested targets, repairing CAS-missing failures until the build stops reporting
/// them.
///
/// An action that fails because an input digest has gone from the RE CAS arms the action that
/// produced that digest. Repairing it takes a new DICE transaction, because invalidating a key
/// that DICE has already computed is only possible between transactions, and one such transaction
/// only reaches one layer of a dependency chain: an evicted output deeper in the chain stays
/// hidden behind the failure above it, since nothing requested it while that failure stood. Each
/// round therefore uncovers the next layer, and `ctx` advances to the transaction the next round
/// runs in.
///
/// Rounds stop as soon as the build succeeds, and otherwise when the registry has nothing armed
/// left to repair, when a round stages actions the build then never requests, or when the round
/// budget runs out. `ctx` is left holding the transaction the returned result came from, so the
/// metrics, sketches and build report the caller derives from it describe the build the user
/// ended up with.
async fn build_until_cas_missing_recovery_converges(
    ctx: &mut DiceTransaction,
    server_ctx: &dyn ServerCommandContextTrait,
    request: &buck2_cli_proto::BuildRequest,
    build_opts: &CommonBuildOptions,
    parsed_patterns_with_modifiers: &[ParsedPatternWithModifiers<
        ConfiguredProvidersPatternExtra,
    >],
    has_pattern_modifiers: bool,
    build_providers: Arc<BuildProviders>,
    materialization_and_upload: MaterializationAndUploadContext,
    graph_properties: GraphPropertiesOptions,
    timeout_observer: Option<&Arc<dyn LivelinessObserver>>,
    build_start: Instant,
) -> buck2_error::Result<BuildTargetResult> {
    let recovery = ctx.per_transaction_data().get_cas_missing_recovery_config();
    let recovery_enabled = ctx
        .per_transaction_data()
        .get_run_action_knobs()
        .cas_missing_recovery_enabled;
    let registry = ctx
        .per_transaction_data()
        .get_cas_missing_recovery_registry();
    // The batch outlives each transaction so that restaging it reaches the executor layer of the
    // round that follows, which reads the same object out of the user data DICE carries forward.
    let batch = ctx.per_transaction_data().get_cas_recovery_batch();
    let mut rounds_left = recovery.max_rounds;

    loop {
        let staged = batch.staged();
        let repairs_before = batch.repairs_charged();

        let result = run_build_round(
            ctx,
            server_ctx,
            request,
            build_opts,
            parsed_patterns_with_modifiers,
            has_pattern_modifiers,
            build_providers.dupe(),
            materialization_and_upload,
            graph_properties.dupe(),
            timeout_observer,
            build_start,
        )
        .await?;

        let armed = registry.keys_eligible_for_recovery(recovery.max_action_attempts);
        let step = next_cas_recovery_step(CasRecoveryRoundResult {
            build_failed: result.build_failed,
            recovery_enabled,
            anything_armed: !armed.is_empty(),
            repaired_nothing: batch.repairs_charged() == repairs_before,
            armed_beyond_staged: armed.iter().any(|key| !staged.contains(key)),
            rounds_left,
        });

        match step {
            CasRecoveryStep::Finish => return Ok(result),
            CasRecoveryStep::ReportBudgetSpent => {
                console_message(format!(
                    "Stopping after {} repair round(s) for artifacts that expired in the RE CAS. \
                     Set `buck2.cas_missing_recovery_max_rounds` higher to work through a deeper \
                     chain.",
                    recovery.max_rounds
                ));
                return Ok(result);
            }
            CasRecoveryStep::StageAnotherRound => {}
        }

        // A round commits its own DICE version rather than going through the concurrency handler
        // the command entered on. Another command holding an older transaction keeps computing
        // against the version it entered on, so what it sees stays internally consistent; the
        // handler negotiates which version a command starts at, which is settled for this one.
        let mut updater = ctx.dupe().into_updater();
        let staged_now = match stage_cas_recovery_round(
            &registry,
            recovery.max_action_attempts,
            &batch,
            &mut updater,
        ) {
            Ok(staged_now) => staged_now,
            Err(e) => {
                // Every later round would fail to invalidate for the same reason, so saying this
                // once beats repeating a promise to re-run actions that stay as they are.
                console_message(format!(
                    "Cannot re-run the actions whose output expired in the RE CAS: {e}"
                ));
                return Ok(result);
            }
        };

        // The registry has nothing left it is willing to repair: every action it tracks has been
        // repaired already or has spent its attempt budget.
        if staged_now == 0 {
            return Ok(result);
        }

        *ctx = updater.commit().await;
        console_message(format!(
            "Re-running {} whose output expired in the RE CAS, then rebuilding everything that \
             depends on {}.",
            if staged_now == 1 {
                "1 action".to_owned()
            } else {
                format!("{staged_now} actions")
            },
            if staged_now == 1 { "it" } else { "them" }
        ));
        rounds_left -= 1;
    }
}

/// Builds the requested targets once against `ctx`.
///
/// Resolving the target patterns is part of the round because the build consumes the resolved
/// pattern and the target resolution config, and CAS-missing recovery runs rounds against a
/// succession of transactions. Both resolve out of DICE's cache from the second round on.
async fn run_build_round(
    ctx: &mut DiceTransaction,
    server_ctx: &dyn ServerCommandContextTrait,
    request: &buck2_cli_proto::BuildRequest,
    build_opts: &CommonBuildOptions,
    parsed_patterns_with_modifiers: &[ParsedPatternWithModifiers<
        ConfiguredProvidersPatternExtra,
    >],
    has_pattern_modifiers: bool,
    build_providers: Arc<BuildProviders>,
    materialization_and_upload: MaterializationAndUploadContext,
    graph_properties: GraphPropertiesOptions,
    timeout_observer: Option<&Arc<dyn LivelinessObserver>>,
    build_start: Instant,
) -> buck2_error::Result<BuildTargetResult> {
    let resolved_pattern: ResolvedPattern<ConfiguredProvidersPatternExtra> =
        ResolveTargetPatterns::resolve_with_modifiers(&mut ctx.ctx(), parsed_patterns_with_modifiers)
            .await?;

    let target_resolution_config = TargetResolutionConfig::from_args(
        &mut ctx.ctx(),
        request
            .target_cfg
            .as_ref()
            .ok_or_else(|| internal_error!("target_cfg must be set"))?,
        server_ctx,
        &request.target_universe,
    )
    .await?;

    match &target_resolution_config {
        TargetResolutionConfig::Default(global_cfg_options) => {
            if !global_cfg_options.cli_modifiers.is_empty() && has_pattern_modifiers {
                return Err(ModifiersError::PatternModifiersWithGlobalModifiers.into());
            }
        }
        TargetResolutionConfig::Universe(_) => {
            if has_pattern_modifiers {
                return Err(ModifiersError::PatternModifiersWithTargetUniverse.into());
            }
        }
    }

    let (streaming_build_result_tx, streaming_build_result_rx) =
        tokio::sync::mpsc::unbounded_channel();
    // Avoid computing and generating streaming build results if we don't have to
    let build_command_streaming_build_result_tx = if !build_opts
        .unstable_streaming_build_report_filename
        .is_empty()
    {
        Some(streaming_build_result_tx)
    } else {
        None
    };

    let return_run_args = request
        .response_options
        .as_ref()
        .is_some_and(|o| o.return_run_args);
    let cloned_ctx = ctx.dupe(); // build_future does a mutable borrow on the context, so we clone it first
    let mut dice = ctx.ctx();
    let build_future = dice.with_linear_recompute(|ctx| {
        async move {
            build_targets(
                ctx,
                resolved_pattern,
                target_resolution_config,
                build_providers,
                materialization_and_upload,
                build_opts.fail_fast,
                MissingTargetBehavior::from_skip(build_opts.skip_missing_targets),
                build_opts.skip_incompatible_targets,
                graph_properties.dupe(),
                return_run_args,
                timeout_observer,
                build_command_streaming_build_result_tx,
                build_start,
            )
            .await
        }
        .boxed()
    });

    maybe_stream_build_reports(
        build_future,
        build_opts,
        cloned_ctx,
        graph_properties,
        server_ctx,
        request,
        streaming_build_result_rx,
    )
    .await
}

async fn maybe_stream_build_reports(
    build_future: impl std::future::Future<Output = buck2_error::Result<BuildTargetResult>>,
    build_opts: &CommonBuildOptions,
    ctx: DiceTransaction,
    graph_properties: GraphPropertiesOptions,
    server_ctx: &dyn ServerCommandContextTrait,
    request: &buck2_cli_proto::BuildRequest,
    mut streaming_build_result_rx: tokio::sync::mpsc::UnboundedReceiver<BuildTargetResult>,
) -> buck2_error::Result<BuildTargetResult> {
    if build_opts
        .unstable_streaming_build_report_filename
        .is_empty()
    {
        return build_future.await;
    }

    init_streaming_build_report(server_ctx, ctx.clone(), request, graph_properties).await?;

    let mut build_future = std::pin::pin!(build_future);
    loop {
        tokio::select! {
            // Wait for the final build result
            result = &mut build_future => {
                // Drain any remaining streaming results
                while let Ok(streaming_result) = streaming_build_result_rx.try_recv() {
                    process_streaming_build_result(
                            server_ctx,
                            ctx.clone(),
                            request,
                            streaming_result,
                            None, // no detailed metrics for streaming build reports to avoid the computation/copy
                            graph_properties,
                            None, // no action graph sketch for streaming build reports to avoid the computation/copy
                        ).await?;
                }
                return result;
            }
            // Process streaming build results as they arrive
            streaming_result = streaming_build_result_rx.recv() => {
                match streaming_result {
                    Some(result) => {
                        process_streaming_build_result(
                            server_ctx,
                            ctx.clone(),
                            request,
                            result,
                            None, // no detailed metrics for streaming build reports to avoid the computation/copy
                            graph_properties,
                            None, // no action graph sketch for streaming build reports to avoid the computation/copy
                        ).await?;
                    }
                    None => {
                        // Channel closed, but continue waiting for build completion
                    }
                }
            }
        }
    }
}

async fn process_build_result(
    server_ctx: &dyn ServerCommandContextTrait,
    ctx: DiceTransaction,
    request: &buck2_cli_proto::BuildRequest,
    build_result: BuildTargetResult,
    detailed_metrics: Option<DetailedAggregatedMetrics>,
    action_graph_sketch_result: Option<ActionGraphSketchResult>,
    artifact_path_sketch_result: Option<ArtifactPathSketchResult>,
    graph_properties_opts: GraphPropertiesOptions,
) -> buck2_error::Result<buck2_cli_proto::BuildResponse> {
    let fs = server_ctx.project_root();
    let cwd = server_ctx.working_dir();

    let build_opts = expect_build_opts(request);
    let response_options = request.response_options.unwrap_or_default();

    let cell_resolver = ctx.ctx().get_cell_resolver().await?;
    let artifact_fs = ctx.ctx().get_artifact_fs().await?;

    let result_reports = ResultReporter::convert(
        &artifact_fs,
        server_ctx.cert_state(),
        ResultReporterOptions {
            return_outputs: response_options.return_outputs,
        },
        &build_result,
    )
    .await?;

    let serialized_build_report = if build_opts.unstable_print_build_report {
        let build_report_opts = build_report_opts(
            &mut ctx.ctx(),
            &cell_resolver,
            build_opts,
            graph_properties_opts,
        )
        .await?;

        write_build_report(
            build_report_opts,
            &artifact_fs,
            &cell_resolver,
            fs,
            cwd,
            server_ctx.events().trace_id(),
            &build_result.configured,
            &build_result.configured_to_pattern_modifiers,
            &build_result.other_errors,
            detailed_metrics,
            action_graph_sketch_result,
            artifact_path_sketch_result,
        )?
    } else {
        None
    };

    let mut provider_artifacts = Vec::new();
    for v in build_result.configured.into_values() {
        // We omit skipped targets here.
        let Some(v) = v else { continue };
        let mut outputs = v.outputs.into_iter().filter_map(|t| t.inner.ok());
        provider_artifacts.extend(&mut outputs);
    }

    let should_create_unhashed_links = ctx
        .ctx()
        .parse_legacy_config_property(
            cell_resolver.root_cell(),
            BuckconfigKeyRef {
                section: "buck2",
                property: "create_unhashed_links",
            },
        )
        .await?;

    if should_create_unhashed_links.unwrap_or(false) {
        span_async(buck2_data::CreateOutputSymlinksStart {}, async {
            let lock = ctx
                .per_transaction_data()
                .get_create_unhashed_symlink_lock();
            let _guard = lock.lock().await;
            let res = create_unhashed_outputs(provider_artifacts, &artifact_fs, fs);

            let created = match res.as_ref() {
                Ok(n) => *n,
                Err(..) => 0,
            };
            (res, buck2_data::CreateOutputSymlinksEnd { created })
        })
        .await?;
    }

    let build_targets = result_reports.build_targets;
    let errors = result_reports
        .build_errors
        .errors
        .iter()
        .map(buck2_data::ErrorReport::from)
        .unique_by(|e| e.message.clone())
        .collect();

    let project_root = server_ctx.project_root().to_string();

    Ok(buck2_cli_proto::BuildResponse {
        build_targets,
        project_root,
        serialized_build_report,
        errors,
    })
}

async fn build_targets(
    ctx: LinearRecomputeDiceComputations<'_, '_>,
    spec: ResolvedPattern<ConfiguredProvidersPatternExtra>,
    target_resolution_config: TargetResolutionConfig,
    build_providers: Arc<BuildProviders>,
    materialization_and_upload: MaterializationAndUploadContext,
    fail_fast: bool,
    missing_target_behavior: MissingTargetBehavior,
    skip_incompatible_targets: bool,
    graph_properties: GraphPropertiesOptions,
    return_run_args: bool,
    timeout_observer: Option<&Arc<dyn LivelinessObserver>>,
    streaming_build_result_tx: Option<UnboundedSender<BuildTargetResult>>,
    build_start: Instant,
) -> buck2_error::Result<BuildTargetResult> {
    let (builder, consumer) =
        AsyncBuildTargetResultBuilder::new(streaming_build_result_tx, build_start);
    let fut = match target_resolution_config {
        TargetResolutionConfig::Default(global_cfg_options) => {
            let spec = spec.convert_pattern().buck_error_context(
                "Targets with explicit configuration can only be built when the `--target-universe=` flag is provided",
            )?;
            build_targets_with_global_target_platform(
                &consumer,
                ctx,
                spec,
                global_cfg_options,
                build_providers,
                materialization_and_upload,
                missing_target_behavior,
                skip_incompatible_targets,
                graph_properties,
                return_run_args,
                timeout_observer,
            )
            .left_future()
        }
        TargetResolutionConfig::Universe(universe) => build_targets_in_universe(
            &consumer,
            ctx,
            spec,
            universe,
            build_providers,
            materialization_and_upload,
            graph_properties,
            return_run_args,
            timeout_observer,
        )
        .right_future(),
    };

    builder.wait_for(fail_fast, fut).await
}

async fn build_targets_in_universe(
    event_consumer: &dyn BuildEventConsumer,
    ctx: LinearRecomputeDiceComputations<'_, '_>,
    spec: ResolvedPattern<ConfiguredProvidersPatternExtra>,
    universe: CqueryUniverse,
    build_providers: Arc<BuildProviders>,
    materialization_and_upload: MaterializationAndUploadContext,
    graph_properties: GraphPropertiesOptions,
    return_run_args: bool,
    timeout_observer: Option<&Arc<dyn LivelinessObserver>>,
) {
    let providers_to_build = build_providers_to_providers_to_build(&build_providers);
    let provider_labels = universe.get_provider_labels(&spec);
    if provider_labels.is_empty() {
        console_message(
            "\nNo targets found inside the specified universe, nothing will be built\n\n"
                .to_owned(),
        );
    }
    provider_labels
        .into_iter()
        .map(|p| {
            buck2_util::async_move_clone!(providers_to_build, {
                build::build_configured_label(
                    event_consumer,
                    ctx,
                    materialization_and_upload,
                    p,
                    &providers_to_build,
                    build::BuildConfiguredLabelOptions {
                        skippable: false,
                        graph_properties,
                        return_run_args,
                    },
                    timeout_observer,
                )
                .await
            })
        })
        .collect::<FuturesUnordered<_>>()
        .collect()
        .await
}

async fn build_targets_with_global_target_platform(
    event_consumer: &dyn BuildEventConsumer,
    ctx: LinearRecomputeDiceComputations<'_, '_>,
    spec: ResolvedPattern<ProvidersPatternExtra>,
    global_cfg_options: GlobalCfgOptions,
    build_providers: Arc<BuildProviders>,
    materialization_and_upload: MaterializationAndUploadContext,
    missing_target_behavior: MissingTargetBehavior,
    skip_incompatible_targets: bool,
    graph_properties: GraphPropertiesOptions,
    return_run_args: bool,
    timeout_observer: Option<&Arc<dyn LivelinessObserver>>,
) {
    let global_cfg_options = &global_cfg_options;
    let build_providers = &build_providers;
    spec.specs
        .into_iter()
        .map(move |(package_with_modifiers, spec)| async move {
            build_targets_for_spec(
                event_consumer,
                ctx,
                spec,
                package_with_modifiers,
                global_cfg_options.dupe(),
                build_providers.dupe(),
                materialization_and_upload,
                missing_target_behavior,
                skip_incompatible_targets,
                graph_properties,
                return_run_args,
                timeout_observer,
            )
            .await
        })
        .collect::<FuturesUnordered<_>>()
        .collect()
        .await
}

struct TargetBuildSpec {
    target: ProvidersLabel,
    global_cfg_options: GlobalCfgOptions,
    modifiers: Modifiers,
    // Indicates whether this target was explicitly requested or not. If it's the result
    // of something like `//foo/...` we can skip it (for example if it's incompatible with
    // the target platform).
    skippable: bool,
    graph_properties: GraphPropertiesOptions,
    return_run_args: bool,
}

fn build_providers_to_providers_to_build(build_providers: &BuildProviders) -> ProvidersToBuild {
    let mut providers_to_build = ProvidersToBuild::default();

    if build_providers.default_info != BuildProviderAction::Skip as i32 {
        providers_to_build.default = true;
        providers_to_build.default_other = true;
    }

    if build_providers.test_info != BuildProviderAction::Skip as i32 {
        providers_to_build.tests = true;
    }

    if build_providers.run_info != BuildProviderAction::Skip as i32 {
        providers_to_build.run = true;
    }

    providers_to_build
}

async fn build_targets_for_spec(
    event_consumer: &dyn BuildEventConsumer,
    ctx: LinearRecomputeDiceComputations<'_, '_>,
    spec: PackageSpec<ProvidersPatternExtra>,
    package_with_modifiers: PackageLabelWithModifiers,
    global_cfg_options: GlobalCfgOptions,
    build_providers: Arc<BuildProviders>,
    materialization_and_upload: MaterializationAndUploadContext,
    missing_target_behavior: MissingTargetBehavior,
    skip_incompatible_targets: bool,
    graph_properties: GraphPropertiesOptions,
    return_run_args: bool,
    timeout_observer: Option<&Arc<dyn LivelinessObserver>>,
) {
    let skippable = match spec {
        PackageSpec::Targets(..) => skip_incompatible_targets,
        PackageSpec::All() => true,
    };

    let PackageLabelWithModifiers { package, modifiers } = package_with_modifiers;

    let res = match ctx.get().get_interpreter_results(package.dupe()).await {
        Ok(res) => res,
        Err(e) => {
            let e: buck2_error::Error = e;
            // Try to associate the error to concrete targets, if possible
            let targets = match spec {
                PackageSpec::Targets(targets) => Either::Left(
                    targets
                        .into_iter()
                        .map(move |(t, providers)| {
                            ProvidersLabel::new(
                                TargetLabel::new(package.dupe(), t.as_ref()),
                                providers.providers,
                            )
                        })
                        .map(Some),
                ),
                PackageSpec::All() => Either::Right(std::iter::once(None)),
            };
            for t in targets {
                event_consumer.consume(BuildEvent::OtherError {
                    label: t,
                    err: e.dupe(),
                });
            }
            return;
        }
    };
    let (targets, missing) = res.apply_spec(spec);
    if let Some(missing) = missing {
        match missing_target_behavior {
            MissingTargetBehavior::Fail => {
                for err in missing.into_all_errors() {
                    event_consumer.consume(BuildEvent::OtherError {
                        label: Some(ProvidersLabel::new(
                            TargetLabel::new(err.package.dupe(), err.target.as_ref()),
                            ProvidersName::Default,
                        )),
                        err: err.into(),
                    });
                }
            }
            MissingTargetBehavior::Warn => {
                // TODO: This should be reported in the build report eventually.
                console_message(missing.missing_targets_warning());
            }
        }
    }
    let todo_targets: Vec<TargetBuildSpec> = targets
        .into_iter()
        .map(|((_target_name, extra), target)| TargetBuildSpec {
            target: ProvidersLabel::new(target.label().dupe(), extra.providers),
            global_cfg_options: global_cfg_options.dupe(),
            modifiers: modifiers.dupe(),
            skippable,
            graph_properties,
            return_run_args,
        })
        .collect();

    let providers_to_build = build_providers_to_providers_to_build(&build_providers);

    todo_targets
        .into_iter()
        .map(|build_spec| {
            buck2_util::async_move_clone!(providers_to_build, {
                build_target(
                    event_consumer,
                    ctx,
                    build_spec,
                    &providers_to_build,
                    materialization_and_upload,
                    timeout_observer,
                )
                .await
            })
        })
        .collect::<FuturesUnordered<_>>()
        .collect()
        .await
}

async fn build_target(
    event_consumer: &dyn BuildEventConsumer,
    ctx: LinearRecomputeDiceComputations<'_, '_>,
    spec: TargetBuildSpec,
    providers_to_build: &ProvidersToBuild,
    materialization_and_upload: MaterializationAndUploadContext,
    timeout_observer: Option<&Arc<dyn LivelinessObserver>>,
) {
    let local_cfg_options = match spec.modifiers.as_slice() {
        None => spec.global_cfg_options.dupe(),
        Some(modifiers) => GlobalCfgOptions {
            target_platform: spec.global_cfg_options.target_platform.dupe(),
            cli_modifiers: modifiers.to_vec().into(),
        },
    };
    let providers_label = match ctx
        .get()
        .get_configured_provider_label(&spec.target, &local_cfg_options)
        .await
    {
        Ok(configured_label) => {
            event_consumer.consume(BuildEvent::new_configured(
                configured_label.dupe(),
                ConfiguredBuildEventVariant::MapModifiers {
                    modifiers: spec.modifiers,
                },
            ));
            configured_label
        }
        Err(e) => {
            event_consumer.consume(BuildEvent::OtherError {
                label: Some(spec.target.dupe()),
                err: e,
            });
            return;
        }
    };

    build::build_configured_label(
        event_consumer,
        ctx,
        materialization_and_upload,
        providers_label,
        providers_to_build,
        build::BuildConfiguredLabelOptions {
            skippable: spec.skippable,
            graph_properties: spec.graph_properties,
            return_run_args: spec.return_run_args,
        },
        timeout_observer,
    )
    .await;
}

/// Provider types that can be skipped in artifact path sketch computation.
/// Parsed from `buck2.providers_to_skip_in_artifact_path_sketch` buckconfig.
enum SkipProvider {
    Build,
    Run,
    Test,
}

impl SkipProvider {
    fn into_build_provider_type(self) -> BuildProviderType {
        match self {
            SkipProvider::Build => BuildProviderType::Default,
            SkipProvider::Run => BuildProviderType::Run,
            SkipProvider::Test => BuildProviderType::Test,
        }
    }
}

#[derive(Debug, buck2_error::Error)]
#[error("Invalid skip provider: `{0}`. Valid values are: [`build`, `run`, `test`]")]
#[buck2(input)]
struct SkipProviderParseError(String);

impl std::str::FromStr for SkipProvider {
    type Err = SkipProviderParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "build" => Ok(SkipProvider::Build),
            "run" => Ok(SkipProvider::Run),
            "test" => Ok(SkipProvider::Test),
            other => Err(SkipProviderParseError(other.to_owned())),
        }
    }
}

#[cfg(test)]
mod cas_recovery_step_tests {
    use super::CasRecoveryRoundResult;
    use super::CasRecoveryStep;
    use super::next_cas_recovery_step;

    /// A round that failed with one action armed, none of it staged yet, and budget to spare —
    /// the shape every case below varies one field of.
    fn repairable_round() -> CasRecoveryRoundResult {
        CasRecoveryRoundResult {
            build_failed: true,
            recovery_enabled: true,
            anything_armed: true,
            repaired_nothing: false,
            armed_beyond_staged: true,
            rounds_left: 1,
        }
    }

    #[test]
    fn a_repairable_round_stages_another() {
        assert_eq!(
            next_cas_recovery_step(repairable_round()),
            CasRecoveryStep::StageAnotherRound
        );
    }

    #[test]
    fn a_build_that_succeeded_finishes() {
        // Repairing exists to rescue a failing build. A succeeding one is done no matter what the
        // registry still holds, which it can hold from an earlier command.
        assert_eq!(
            next_cas_recovery_step(CasRecoveryRoundResult {
                build_failed: false,
                ..repairable_round()
            }),
            CasRecoveryStep::Finish
        );
    }

    #[test]
    fn a_build_with_recovery_disabled_finishes() {
        assert_eq!(
            next_cas_recovery_step(CasRecoveryRoundResult {
                recovery_enabled: false,
                ..repairable_round()
            }),
            CasRecoveryStep::Finish
        );
    }

    #[test]
    fn a_quiet_registry_finishes() {
        // Every action the registry tracks has been repaired or has spent its attempt budget, so
        // the failure that remains is one no round can act on.
        assert_eq!(
            next_cas_recovery_step(CasRecoveryRoundResult {
                anything_armed: false,
                ..repairable_round()
            }),
            CasRecoveryStep::Finish
        );
    }

    #[test]
    fn a_round_that_repaired_nothing_new_finishes() {
        // The staged actions never re-executed and nothing else armed, so the build does not
        // depend on them and the next round would select the same actions to the same effect.
        assert_eq!(
            next_cas_recovery_step(CasRecoveryRoundResult {
                repaired_nothing: true,
                armed_beyond_staged: false,
                ..repairable_round()
            }),
            CasRecoveryStep::Finish
        );
    }

    #[test]
    fn a_round_that_repaired_nothing_still_stages_for_a_newly_armed_action() {
        // A previous command can leave an action armed that this build never requests. Staging it
        // charges nothing, and treating that as the end would abandon a failure uncovered deeper
        // in the chain that one more round would repair.
        assert_eq!(
            next_cas_recovery_step(CasRecoveryRoundResult {
                repaired_nothing: true,
                armed_beyond_staged: true,
                ..repairable_round()
            }),
            CasRecoveryStep::StageAnotherRound
        );
    }

    #[test]
    fn a_spent_budget_is_reported_rather_than_staged() {
        assert_eq!(
            next_cas_recovery_step(CasRecoveryRoundResult {
                rounds_left: 0,
                ..repairable_round()
            }),
            CasRecoveryStep::ReportBudgetSpent
        );
    }

    #[test]
    fn a_spent_budget_on_a_failure_recovery_cannot_act_on_stays_silent() {
        // The budget message names the RE CAS, so a failure with nothing armed — an ordinary
        // compile error, say — has to finish quietly rather than blame an eviction.
        assert_eq!(
            next_cas_recovery_step(CasRecoveryRoundResult {
                anything_armed: false,
                rounds_left: 0,
                ..repairable_round()
            }),
            CasRecoveryStep::Finish
        );
    }
}
