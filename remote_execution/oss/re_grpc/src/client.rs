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
use std::env::VarError;
use std::io;
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use async_compression::tokio::bufread::BrotliDecoder;
use async_compression::tokio::bufread::BrotliEncoder;
use async_compression::tokio::bufread::DeflateDecoder;
use async_compression::tokio::bufread::DeflateEncoder;
use async_compression::tokio::bufread::ZstdDecoder;
use async_compression::tokio::bufread::ZstdEncoder;
use buck2_re_configuration::Buck2OssReConfiguration;
use buck2_re_configuration::HttpHeader;
use dupe::Dupe;
use futures::Stream;
use futures::future::BoxFuture;
use futures::future::Future;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use futures::stream::TryStreamExt;
use gazebo::prelude::*;
use lru::LruCache;
use prost::Message;
use re_grpc_proto::build::bazel::remote::execution::v2::ActionResult;
use re_grpc_proto::build::bazel::remote::execution::v2::BatchReadBlobsRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::BatchReadBlobsResponse;
use re_grpc_proto::build::bazel::remote::execution::v2::BatchUpdateBlobsRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::BatchUpdateBlobsResponse;
use re_grpc_proto::build::bazel::remote::execution::v2::Digest;
use re_grpc_proto::build::bazel::remote::execution::v2::ExecuteOperationMetadata;
use re_grpc_proto::build::bazel::remote::execution::v2::ExecuteRequest as GExecuteRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::ExecuteResponse as GExecuteResponse;
use re_grpc_proto::build::bazel::remote::execution::v2::ExecutedActionMetadata;
use re_grpc_proto::build::bazel::remote::execution::v2::ExecutionPolicy;
use re_grpc_proto::build::bazel::remote::execution::v2::FindMissingBlobsRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::FindMissingBlobsResponse;
use re_grpc_proto::build::bazel::remote::execution::v2::GetActionResultRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::GetCapabilitiesRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::OutputDirectory;
use re_grpc_proto::build::bazel::remote::execution::v2::OutputFile;
use re_grpc_proto::build::bazel::remote::execution::v2::OutputSymlink;
use re_grpc_proto::build::bazel::remote::execution::v2::RequestMetadata;
use re_grpc_proto::build::bazel::remote::execution::v2::ResultsCachePolicy;
use re_grpc_proto::build::bazel::remote::execution::v2::ToolDetails;
use re_grpc_proto::build::bazel::remote::execution::v2::UpdateActionResultRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::WaitExecutionRequest as GWaitExecutionRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::action_cache_client::ActionCacheClient;
use re_grpc_proto::build::bazel::remote::execution::v2::batch_update_blobs_request::Request;
use re_grpc_proto::build::bazel::remote::execution::v2::capabilities_client::CapabilitiesClient;
use re_grpc_proto::build::bazel::remote::execution::v2::compressor;
use re_grpc_proto::build::bazel::remote::execution::v2::content_addressable_storage_client::ContentAddressableStorageClient;
use re_grpc_proto::build::bazel::remote::execution::v2::execution_client::ExecutionClient;
use re_grpc_proto::build::bazel::remote::execution::v2::execution_stage;
use re_grpc_proto::google::bytestream::ReadRequest;
use re_grpc_proto::google::bytestream::ReadResponse;
use re_grpc_proto::google::bytestream::WriteRequest;
use re_grpc_proto::google::bytestream::WriteResponse;
use re_grpc_proto::google::bytestream::byte_stream_client::ByteStreamClient;
use re_grpc_proto::google::longrunning::Operation;
use re_grpc_proto::google::longrunning::operation::Result as OpResult;
use re_grpc_proto::google::rpc::Code;
use re_grpc_proto::google::rpc::Status;
use regex::Regex;
use tokio::fs::OpenOptions;
use tokio::io::AsyncBufRead;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::sync::Semaphore;
use tokio_util::io::StreamReader;
use tonic::codegen::InterceptedService;
use tonic::metadata;
use tonic::metadata::MetadataKey;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::Channel;

use crate::error::*;
use crate::metadata::*;
use crate::pool::ChannelConfig;
use crate::pool::ChannelPool;
use crate::pool::PoolConfig;
use crate::pool::PooledChannel;
use crate::pool::create_channel;
use crate::pool::resolve_optional_secs;
use crate::reattach::ReattachState;
use crate::reattach::RetryCause;
use crate::reattach::classify;
use crate::request::*;
use crate::response::*;

const DEFAULT_MAX_TOTAL_BATCH_SIZE: usize = 4 * 1000 * 1000;

// Defaults for the execute-stream reattach settings of `Buck2OssReConfiguration`, whose field
// docs describe the behavior each one gates. Connection keepalive defaults live in `crate::pool`,
// next to the endpoint construction they configure.
const EXECUTE_REATTACH_BUDGET_SECS_DEFAULT: u64 = 60;
const EXECUTE_REATTACH_CONCURRENCY_DEFAULT: usize = 8;
const EXECUTE_REATTACH_CONCURRENCY_MIN: usize = 1;

/// Resolves the wall-clock budget for reattaching a severed execute stream. Reattach is off
/// unless `execute_reattach_enabled` is `Some(true)`; once enabled, the budget follows
/// `resolve_optional_secs`'s convention, where `None` means disabled.
fn execute_reattach_budget(opts: &Buck2OssReConfiguration) -> Option<Duration> {
    if !opts.execute_reattach_enabled.unwrap_or(false) {
        return None;
    }
    resolve_optional_secs(
        opts.execute_reattach_budget_secs,
        EXECUTE_REATTACH_BUDGET_SECS_DEFAULT,
    )
}

/// Resolves the upper bound on concurrent execute-stream reattach dials. A configured `0` would
/// build a limiter that blocks every reattach forever, so it is clamped to the minimum instead.
fn execute_reattach_concurrency(opts: &Buck2OssReConfiguration) -> usize {
    match opts.execute_reattach_concurrency {
        Some(0) => {
            tracing::warn!(
                "RE execute reattach concurrency of 0 would block every reattach forever; \
                 clamping to {EXECUTE_REATTACH_CONCURRENCY_MIN}",
            );
            EXECUTE_REATTACH_CONCURRENCY_MIN
        }
        Some(n) => n,
        None => EXECUTE_REATTACH_CONCURRENCY_DEFAULT,
    }
}

fn tdigest_to(tdigest: TDigest) -> Digest {
    Digest {
        hash: tdigest.hash,
        size_bytes: tdigest.size_in_bytes,
    }
}

fn tdigest_from(digest: Digest) -> TDigest {
    TDigest {
        hash: digest.hash,
        size_in_bytes: digest.size_bytes,
        ..Default::default()
    }
}

fn tstatus_ok() -> TStatus {
    TStatus {
        code: TCode::OK,
        message: "".to_owned(),
        ..Default::default()
    }
}

fn check_status(status: Status) -> Result<(), REClientError> {
    if status.code == 0 {
        return Ok(());
    }

    Err(REClientError {
        code: TCode(status.code),
        message: status.message,
        group: TCodeReasonGroup::UNKNOWN,
    })
}

fn ttimestamp_to(ts: TTimestamp) -> ::prost_types::Timestamp {
    ::prost_types::Timestamp {
        seconds: ts.seconds,
        nanos: ts.nanos,
    }
}

fn ttimestamp_from(ts: Option<::prost_types::Timestamp>) -> TTimestamp {
    match ts {
        Some(timestamp) => TTimestamp {
            seconds: timestamp.seconds,
            nanos: timestamp.nanos,
            ..Default::default()
        },
        None => TTimestamp::unix_epoch(),
    }
}

/// Contains information queried from the Remote Execution Capabilities service.
pub struct RECapabilities {
    /// Largest size of a message before being uploaded using bytestream service.
    /// 0 indicates no limit beyond constraint of underlying transport (which is unknown).
    max_total_batch_size: usize,
    /// Compressors supported by the "compressed-blobs" bytestream resources.
    supported_compressors: Vec<Compressor>,
}

/// Contains runtime options for the remote execution client as set under `buck2_re_client`
pub struct RERuntimeOpts {
    /// Use the Meta version of the request metadata
    use_fbcode_metadata: bool,
    /// Maximum number of concurrent upload requests.
    max_concurrent_uploads_per_action: Option<usize>,
    /// Time that digests are assumed to live in CAS after being touched.
    cas_ttl_secs: i64,
    /// Maximum number of digests per `FindMissingBlobs` RPC.
    find_missing_blobs_batch_size: usize,
    /// Wall-clock budget for recovering a severed execute stream, measured from when recovery
    /// for that severance begins. `None` disables reattach: every severance propagates
    /// unmodified.
    execute_reattach_budget: Option<Duration>,
}

struct InstanceName(Option<String>);

impl InstanceName {
    fn as_str(&self) -> &str {
        match &self.0 {
            Some(instance_name) => instance_name,
            None => "",
        }
    }

    fn as_resource_prefix(&self) -> String {
        match &self.0 {
            Some(instance_name) => format!("{instance_name}/"),
            None => "".to_owned(),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum Compressor {
    Zstd,
    Deflate,
    Brotli,
}

impl Compressor {
    fn from_grpc(val: i32) -> Option<Self> {
        if val == compressor::Value::Zstd as i32 {
            Some(Self::Zstd)
        } else if val == compressor::Value::Deflate as i32 {
            Some(Self::Deflate)
        } else if val == compressor::Value::Brotli as i32 {
            Some(Self::Brotli)
        } else {
            None
        }
    }

    /// The compressor name used in compressed-blob resource paths
    fn name(&self) -> &str {
        match self {
            Self::Zstd => "zstd",
            Self::Deflate => "deflate",
            Self::Brotli => "brotli",
        }
    }
}

pub struct REClientBuilder;

impl REClientBuilder {
    pub async fn build_and_connect(opts: &Buck2OssReConfiguration) -> anyhow::Result<REClient> {
        // Create channel config once (reads TLS files)
        let channel_config = ChannelConfig::new(opts)
            .await
            .context("Failed to create channel config")?;

        // Create a single channel for fetching capabilities. Other channels are created
        // on-demand through the connection pool.
        let engine_address = opts.engine_address.as_ref().context("No engine address")?;
        let capabilities_channel = create_channel(&channel_config, engine_address)
            .context("Error creating Capabilities channel")?;

        let interceptor = InjectHeadersInterceptor::new(&opts.http_headers)?;

        let mut capabilities_client =
            CapabilitiesClient::with_interceptor(capabilities_channel, interceptor.dupe());

        if let Some(max_decoding_message_size) = opts.max_decoding_message_size {
            capabilities_client =
                capabilities_client.max_decoding_message_size(max_decoding_message_size);
        }

        let instance_name = InstanceName(opts.instance_name.clone());

        let capabilities = if opts.capabilities.unwrap_or(true) {
            Self::fetch_rbe_capabilities(
                &mut capabilities_client,
                &instance_name,
                opts.max_total_batch_size,
            )
            .await?
        } else {
            RECapabilities {
                max_total_batch_size: DEFAULT_MAX_TOTAL_BATCH_SIZE,
                supported_compressors: Vec::new(),
            }
        };

        let max_decoding_msg_size = opts
            .max_decoding_message_size
            .unwrap_or(capabilities.max_total_batch_size * 2);

        if max_decoding_msg_size < capabilities.max_total_batch_size {
            return Err(anyhow::anyhow!(
                "Attribute `max_decoding_message_size` must always be equal or higher to `max_total_batch_size`"
            ));
        }

        // Choose a ByteStream compressor
        let bystream_compressor = if capabilities
            .supported_compressors
            .contains(&Compressor::Zstd)
        {
            Some(Compressor::Zstd)
        } else if capabilities
            .supported_compressors
            .contains(&Compressor::Brotli)
        {
            Some(Compressor::Brotli)
        } else if capabilities
            .supported_compressors
            .contains(&Compressor::Deflate)
        {
            Some(Compressor::Deflate)
        } else {
            None
        };

        // Extract addresses
        let cas_address = opts.cas_address.clone().context("No CAS address")?;
        let action_cache_address = opts
            .action_cache_address
            .clone()
            .context("No action cache address")?;

        // Create connection pool
        let min_connections = opts.min_connections.unwrap_or(1).max(1);
        let max_connections = opts.max_connections.unwrap_or(100).max(min_connections);
        let pool_config = PoolConfig {
            min_connections,
            max_connections,
            max_concurrency_per_connection: opts.max_concurrency_per_connection.unwrap_or(100),
        };
        let pool = Arc::new(ChannelPool::new(pool_config, channel_config));

        let execute_reattach_limiter = Arc::new(Semaphore::new(execute_reattach_concurrency(opts)));

        Ok(REClient::new(
            RERuntimeOpts {
                use_fbcode_metadata: opts.use_fbcode_metadata,
                max_concurrent_uploads_per_action: opts.max_concurrent_uploads_per_action,
                // NOTE: This is an arbitrary number because RBE does not return information
                // on the TTL of the remote blob.
                cas_ttl_secs: opts.cas_ttl_secs.unwrap_or(3 * 60 * 60),
                find_missing_blobs_batch_size: opts.find_missing_blobs_batch_size.unwrap_or(100),
                execute_reattach_budget: execute_reattach_budget(opts),
            },
            capabilities,
            instance_name,
            bystream_compressor,
            pool,
            max_decoding_msg_size,
            interceptor,
            cas_address,
            engine_address.clone(),
            action_cache_address,
            execute_reattach_limiter,
        ))
    }

    async fn fetch_rbe_capabilities(
        client: &mut CapabilitiesClient<InterceptedService<Channel, InjectHeadersInterceptor>>,
        instance_name: &InstanceName,
        max_total_batch_size: Option<usize>,
    ) -> anyhow::Result<RECapabilities> {
        // TODO use more of the capabilities of the remote build executor

        let resp = client
            .get_capabilities(GetCapabilitiesRequest {
                instance_name: instance_name.as_str().to_owned(),
            })
            .await
            .context("Failed to query capabilities of remote")?
            .into_inner();

        let supported_compressors = if let Some(cache_cap) = &resp.cache_capabilities {
            cache_cap
                .supported_compressors
                .iter()
                .copied()
                .filter_map(Compressor::from_grpc)
                .collect()
        } else {
            Vec::new()
        };

        let max_total_batch_size_from_capabilities: Option<usize> =
            if let Some(cache_cap) = resp.cache_capabilities {
                let size = cache_cap.max_batch_total_size_bytes as usize;
                // A value of 0 means no limit is set
                if size != 0 { Some(size) } else { None }
            } else {
                None
            };

        let max_total_batch_size =
            match (max_total_batch_size_from_capabilities, max_total_batch_size) {
                (Some(cap), Some(config)) => std::cmp::min(cap, config),
                (Some(cap), None) => cap,
                (None, Some(config)) => config,
                (None, None) => DEFAULT_MAX_TOTAL_BATCH_SIZE,
            };

        Ok(RECapabilities {
            max_total_batch_size,
            supported_compressors,
        })
    }
}

#[derive(Clone, Dupe)]
struct InjectHeadersInterceptor {
    headers: Arc<Vec<(MetadataKey<metadata::Ascii>, MetadataValue<metadata::Ascii>)>>,
}

impl InjectHeadersInterceptor {
    pub fn new(headers: &[HttpHeader]) -> anyhow::Result<Self> {
        let headers = headers
            .iter()
            .map(|h| {
                // This means we can't have `$` in a header key or value, which isn't great. On the
                // flip side, env vars are good for things like credentials, which those headers
                // are likely to contain. In time, we should allow escaping.
                let key = substitute_env_vars(&h.key)?;
                let value = substitute_env_vars(&h.value)?;

                let key = MetadataKey::<metadata::Ascii>::from_bytes(key.as_bytes())
                    .with_context(|| format!("Invalid key in header: `{key}: {value}`"))?;

                let value = MetadataValue::try_from(&value)
                    .with_context(|| format!("Invalid value in header: `{key}: {value}`"))?;

                anyhow::Ok((key, value))
            })
            .collect::<Result<_, _>>()
            .context("Error converting headers")?;

        Ok(Self {
            headers: Arc::new(headers),
        })
    }
}

impl Interceptor for InjectHeadersInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        for (k, v) in self.headers.iter() {
            request.metadata_mut().insert(k.clone(), v.clone());
        }
        Ok(request)
    }
}

type GrpcService = InterceptedService<PooledChannel, InjectHeadersInterceptor>;

#[derive(Debug, Copy, Clone)]
enum DigestRemoteState {
    ExistsOnRemote,
    Missing,
}

struct FindMissingCache {
    cache: LruCache<TDigest, DigestRemoteState>,
    /// To avoid a situation where we cache that an artifact is available remotely, but the artifact then expires
    /// we clear our local cache once every `ttl`.
    ttl: Duration,
    last_check: Instant,
}

impl FindMissingCache {
    fn clear_if_ttl_expires(&mut self) {
        if self.last_check.elapsed() > self.ttl {
            self.cache.clear();
            self.last_check = Instant::now();
        }
    }

    pub fn get(&mut self, digest: &TDigest) -> Option<DigestRemoteState> {
        self.clear_if_ttl_expires();
        self.cache.get(digest).copied()
    }

    pub fn put(&mut self, digest: TDigest, state: DigestRemoteState) {
        self.clear_if_ttl_expires();
        self.cache.put(digest, state);
    }
}

pub struct REClient {
    runtime_opts: RERuntimeOpts,
    // `Arc` so `execute_with_progress` can hand a pool handle to its 'static reattach closures
    // without borrowing `self` for the lifetime of the returned stream.
    pool: Arc<ChannelPool>,
    capabilities: RECapabilities,
    instance_name: InstanceName,
    // buck2 calls find_missing for same blobs
    find_missing_cache: Mutex<FindMissingCache>,
    bystream_compressor: Option<Compressor>,
    max_decoding_msg_size: usize,
    interceptor: InjectHeadersInterceptor,
    cas_address: String,
    engine_address: String,
    action_cache_address: String,
    // Bounds concurrent execute-stream reattach dials across every action sharing this client,
    // keeping a frontend that is still restarting from taking the whole in-flight fleet's
    // reattaches at once.
    execute_reattach_limiter: Arc<Semaphore>,
    // Set the first time a `WaitExecution` call on this client returns `UNIMPLEMENTED`, so every
    // action sharing the client pays the discovery cost once.
    execute_reattach_wait_execution_unimplemented: Arc<AtomicBool>,
}

impl Drop for REClient {
    fn drop(&mut self) {
        // Important we have a drop implementation since the real one does, and we
        // don't want errors coming from the stub not having one
    }
}

/// Information on components of a batch upload.
/// Used to defer reading of NamedDigest contents till
/// actual execution of upload and prevent opening too many
/// files at the same time.
enum BatchUploadRequest {
    Blob(InlinedBlobWithDigest),
    File(NamedDigest),
}

/// Builds up a vector of batch upload requests based upon the maximum allowed message size.
#[derive(Default)]
struct BatchUploadReqAggregator {
    max_msg_size: i64,
    curr_req: Vec<BatchUploadRequest>,
    requests: Vec<Vec<BatchUploadRequest>>,
    curr_request_size: i64,
}

impl BatchUploadReqAggregator {
    pub fn new(max_msg_size: usize) -> Self {
        BatchUploadReqAggregator {
            max_msg_size: max_msg_size as i64,
            ..Default::default()
        }
    }

    pub fn push(&mut self, req: BatchUploadRequest) {
        let size_in_bytes = match &req {
            BatchUploadRequest::Blob(blob) => blob.digest.size_in_bytes,
            BatchUploadRequest::File(file) => file.digest.size_in_bytes,
        };

        // As an optimization, we can silently skip uploading empty blobs
        if size_in_bytes == 0 {
            return;
        }

        self.curr_request_size += size_in_bytes;

        if self.curr_request_size >= self.max_msg_size {
            self.requests.push(std::mem::take(&mut self.curr_req));
            self.curr_request_size = size_in_bytes;
        }
        self.curr_req.push(req);
    }

    pub fn done(mut self) -> Vec<Vec<BatchUploadRequest>> {
        if !self.curr_req.is_empty() {
            self.requests.push(std::mem::take(&mut self.curr_req));
        }
        self.requests
    }
}

/// Returns true if an error is a transient connection/transport error worth
/// retrying. Walks the error chain checking for:
///   - `tonic::Status` with codes the gRPC retry policy treats as transient
///     (Unavailable, ResourceExhausted, Aborted)
///   - `io::Error` of a kind that indicates a transport-level disruption
///     (BrokenPipe, ConnectionReset/Aborted, UnexpectedEof, TimedOut)
///
/// `tonic::transport::Error` is not matched directly — its transient subset
/// surfaces as an `io::Error` somewhere in the chain, which we catch above.
/// Non-transient transport errors (TLS handshake, invalid URI) do not have
/// an `io::Error` source, so they correctly do not retry.
fn is_retryable(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if let Some(status) = cause.downcast_ref::<tonic::Status>() {
            match status.code() {
                tonic::Code::Unavailable
                | tonic::Code::ResourceExhausted
                | tonic::Code::Aborted => return true,
                _ => {}
            }
        }
        if let Some(io_err) = cause.downcast_ref::<std::io::Error>() {
            match io_err.kind() {
                std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::TimedOut => return true,
                _ => {}
            }
        }
    }
    false
}

/// Retry a fallible async operation on transient connection errors.
///
/// On retryable failure, the closure is called again from scratch — acquiring
/// a fresh connection from the pool, rebuilding the request, etc. Up to 5
/// attempts with exponential backoff (100ms, 200ms, 400ms, 800ms — capped at
/// 5s) plus 0–50ms of jitter to avoid synchronized retry storms across
/// concurrent in-flight requests.
async fn retry<F, Fut, T>(f: F) -> anyhow::Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    use rand::RngExt;

    const MAX_ATTEMPTS: u32 = 5;
    const INITIAL_DELAY: Duration = Duration::from_millis(100);
    const MAX_DELAY: Duration = Duration::from_secs(5);

    let mut delay = INITIAL_DELAY;
    for attempt in 1..=MAX_ATTEMPTS {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if is_retryable(&e) && attempt < MAX_ATTEMPTS => {
                let jitter = Duration::from_millis(rand::rng().random_range(0..50));
                let sleep_for = delay + jitter;
                tracing::warn!(
                    "Transient error (attempt {}/{}), retrying in {:?}: {:#}",
                    attempt,
                    MAX_ATTEMPTS,
                    sleep_for,
                    e
                );
                tokio::time::sleep(sleep_for).await;
                delay = (delay * 2).min(MAX_DELAY);
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}

impl REClient {
    fn new(
        runtime_opts: RERuntimeOpts,
        capabilities: RECapabilities,
        instance_name: InstanceName,
        bystream_compressor: Option<Compressor>,
        pool: Arc<ChannelPool>,
        max_decoding_msg_size: usize,
        interceptor: InjectHeadersInterceptor,
        cas_address: String,
        engine_address: String,
        action_cache_address: String,
        execute_reattach_limiter: Arc<Semaphore>,
    ) -> Self {
        REClient {
            runtime_opts,
            pool,
            capabilities,
            instance_name,
            find_missing_cache: Mutex::new(FindMissingCache {
                cache: LruCache::new(NonZeroUsize::new(500_000).unwrap()),
                ttl: Duration::from_hours(12), // 12 hours TODO: Tune this parameter
                last_check: Instant::now(),
            }),
            bystream_compressor,
            max_decoding_msg_size,
            interceptor,
            cas_address,
            engine_address,
            action_cache_address,
            execute_reattach_limiter,
            execute_reattach_wait_execution_unimplemented: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn get_action_result(
        &self,
        metadata: &RemoteExecutionMetadata,
        request: ActionResultRequest,
    ) -> anyhow::Result<ActionResultResponse> {
        retry(|| async {
            let res = self
                .action_cache_client()
                .await?
                .get_action_result(with_re_metadata(
                    GetActionResultRequest {
                        instance_name: self.instance_name.as_str().to_owned(),
                        action_digest: Some(tdigest_to(request.digest.clone())),
                        ..Default::default()
                    },
                    metadata,
                    self.runtime_opts.use_fbcode_metadata,
                ))
                .await?;

            Ok(ActionResultResponse {
                action_result: convert_action_result(res.into_inner())?,
                ttl: 0,
            })
        })
        .await
    }

    pub async fn write_action_result(
        &self,
        metadata: &RemoteExecutionMetadata,
        request: WriteActionResultRequest,
    ) -> anyhow::Result<WriteActionResultResponse> {
        let action_result = convert_t_action_result2(request.action_result)?;

        retry(|| async {
            let res = self
                .action_cache_client()
                .await?
                .update_action_result(with_re_metadata(
                    UpdateActionResultRequest {
                        instance_name: self.instance_name.as_str().to_owned(),
                        action_digest: Some(tdigest_to(request.action_digest.clone())),
                        action_result: Some(action_result.clone()),
                        results_cache_policy: None,
                        ..Default::default()
                    },
                    metadata,
                    self.runtime_opts.use_fbcode_metadata,
                ))
                .await?;

            Ok(WriteActionResultResponse {
                actual_action_result: convert_action_result(res.into_inner())?,
                ttl_seconds: 0,
            })
        })
        .await
    }

    pub async fn execute_with_progress(
        &self,
        metadata: &RemoteExecutionMetadata,
        execute_request: ExecuteRequest,
    ) -> anyhow::Result<BoxStream<'static, anyhow::Result<ExecuteWithProgressResponse>>> {
        let use_fbcode_metadata = self.runtime_opts.use_fbcode_metadata;

        // Each closure pulls a fresh channel from the pool per call — for the initial `Execute`
        // dial, every `retry()` attempt in `execute_with_progress_impl`, and every reattach
        // `ReattachState::recover` drives — rather than holding one connection for the stream's
        // whole lifetime.
        let execute_pool = self.pool.dupe();
        let execute_interceptor = self.interceptor.dupe();
        let execute_address = self.engine_address.clone();
        let execute_metadata = metadata.clone();
        let execute_f = move |request: GExecuteRequest| {
            let pool = execute_pool.dupe();
            let interceptor = execute_interceptor.dupe();
            let address = execute_address.clone();
            let metadata = execute_metadata.clone();
            async move {
                let channel = pool.get(&address).await?;
                let mut client =
                    ExecutionClient::new(InterceptedService::new(channel, interceptor));
                let stream = client
                    .execute(with_re_metadata(request, &metadata, use_fbcode_metadata))
                    .await?
                    .into_inner();
                Ok(stream.boxed())
            }
        };

        let wait_execution_pool = self.pool.dupe();
        let wait_execution_interceptor = self.interceptor.dupe();
        let wait_execution_address = self.engine_address.clone();
        let wait_execution_metadata = metadata.clone();
        let wait_execution_f = move |name: String| {
            let pool = wait_execution_pool.dupe();
            let interceptor = wait_execution_interceptor.dupe();
            let address = wait_execution_address.clone();
            let metadata = wait_execution_metadata.clone();
            async move {
                let channel = pool.get(&address).await?;
                let mut client =
                    ExecutionClient::new(InterceptedService::new(channel, interceptor));
                let stream = client
                    .wait_execution(with_re_metadata(
                        GWaitExecutionRequest { name },
                        &metadata,
                        use_fbcode_metadata,
                    ))
                    .await?
                    .into_inner();
                Ok(stream.boxed())
            }
        };

        execute_with_progress_impl(
            &self.instance_name,
            execute_request,
            execute_f,
            wait_execution_f,
            self.runtime_opts.execute_reattach_budget,
            self.execute_reattach_limiter.dupe(),
            self.execute_reattach_wait_execution_unimplemented.dupe(),
        )
        .await
    }

    pub async fn upload(
        &self,
        metadata: &RemoteExecutionMetadata,
        request: UploadRequest,
    ) -> anyhow::Result<UploadResponse> {
        upload_impl(
            &self.instance_name,
            request,
            self.bystream_compressor,
            self.capabilities.max_total_batch_size,
            self.runtime_opts.max_concurrent_uploads_per_action,
            |re_request| async move {
                let resp = self
                    .cas_client()
                    .await?
                    .batch_update_blobs(with_re_metadata(
                        re_request,
                        metadata,
                        self.runtime_opts.use_fbcode_metadata,
                    ))
                    .await?;
                Ok(resp.into_inner())
            },
            |segments| async move {
                let resp = self
                    .bytestream_client()
                    .await?
                    .write(with_re_metadata(
                        futures::stream::iter(segments),
                        metadata,
                        self.runtime_opts.use_fbcode_metadata,
                    ))
                    .await?;
                Ok(resp.into_inner())
            },
        )
        .await
    }

    pub async fn upload_blob_with_digest(
        &self,
        blob: Vec<u8>,
        digest: TDigest,
        metadata: &RemoteExecutionMetadata,
    ) -> anyhow::Result<TDigest> {
        let blob = InlinedBlobWithDigest {
            digest: digest.clone(),
            blob,
            ..Default::default()
        };
        self.upload(
            metadata,
            UploadRequest {
                inlined_blobs_with_digest: Some(vec![blob]),
                files_with_digest: None,
                directories: None,
                upload_only_missing: false,
                ..Default::default()
            },
        )
        .await?;
        Ok(digest)
    }

    pub async fn download(
        &self,
        metadata: &RemoteExecutionMetadata,
        request: DownloadRequest,
    ) -> anyhow::Result<DownloadResponse> {
        download_impl(
            &self.instance_name,
            request,
            self.bystream_compressor,
            self.capabilities.max_total_batch_size,
            |re_request| async move {
                let resp = self
                    .cas_client()
                    .await?
                    .batch_read_blobs(with_re_metadata(
                        re_request,
                        metadata,
                        self.runtime_opts.use_fbcode_metadata,
                    ))
                    .await?;
                Ok(resp.into_inner())
            },
            |read_request| async move {
                let response = self
                    .bytestream_client()
                    .await?
                    .read(with_re_metadata(
                        read_request,
                        metadata,
                        self.runtime_opts.use_fbcode_metadata,
                    ))
                    .await?
                    .into_inner();
                Ok(Box::pin(response.into_stream()))
            },
        )
        .await
    }

    pub async fn get_digests_ttl(
        &self,
        metadata: &RemoteExecutionMetadata,
        request: GetDigestsTtlRequest,
    ) -> anyhow::Result<GetDigestsTtlResponse> {
        let mut remote_results: HashMap<TDigest, DigestRemoteState> = HashMap::new();
        let mut digests_to_check: Vec<TDigest> = Vec::new();

        let batch_size = self.runtime_opts.find_missing_blobs_batch_size;
        let mut digest_iter = request.digests.iter();
        while digest_iter.len() > 0 {
            // Sort our blobs based on what action we need to take
            {
                let mut find_missing_cache = self.find_missing_cache.lock().unwrap();
                for digest in digest_iter.by_ref() {
                    if let Some(rs) = find_missing_cache.get(digest) {
                        // We have our final result already cached
                        remote_results.insert(digest.clone(), rs);
                    } else {
                        // We can check this blob
                        digests_to_check.push(digest.clone());
                    }
                    if digests_to_check.len() >= batch_size {
                        break;
                    }
                }
            }

            // Send a request and notify others of the result
            if !digests_to_check.is_empty() {
                tracing::debug!(num_digests = digests_to_check.len(), "FindMissingBlobs");
                let blob_digests: Vec<_> = digests_to_check.map(|b| tdigest_to(b.clone()));
                let resp: FindMissingBlobsResponse = retry(|| async {
                    let resp = self
                        .cas_client()
                        .await?
                        .find_missing_blobs(with_re_metadata(
                            FindMissingBlobsRequest {
                                instance_name: self.instance_name.as_str().to_owned(),
                                blob_digests: blob_digests.clone(),
                                ..Default::default()
                            },
                            metadata,
                            self.runtime_opts.use_fbcode_metadata,
                        ))
                        .await
                        .context("Failed to request what blobs are not present on remote")?;
                    Ok(resp.into_inner())
                })
                .await?;

                // Update the results and the cache
                let mut find_missing_cache = self.find_missing_cache.lock().unwrap();
                for digest in &digests_to_check {
                    remote_results.insert(digest.clone(), DigestRemoteState::ExistsOnRemote);
                    find_missing_cache.put(digest.clone(), DigestRemoteState::ExistsOnRemote);
                }

                for digest in &resp.missing_blob_digests.map(|d| tdigest_from(d.clone())) {
                    // If it's present in the MissingBlobsResponse, it's expired on the remote and
                    // needs to be refetched.
                    remote_results.insert(digest.clone(), DigestRemoteState::Missing);
                    find_missing_cache.put(digest.clone(), DigestRemoteState::Missing);
                }
                digests_to_check.clear();
            }
        }

        Ok(GetDigestsTtlResponse {
            digests_with_ttl: remote_results
                .iter()
                .map(|(digest, rs)| match rs {
                    DigestRemoteState::Missing => DigestWithTtl {
                        digest: digest.clone(),
                        ttl: 0,
                    },
                    DigestRemoteState::ExistsOnRemote => DigestWithTtl {
                        digest: digest.clone(),
                        ttl: self.runtime_opts.cas_ttl_secs,
                    },
                })
                .collect::<Vec<DigestWithTtl>>(),
        })
    }

    pub async fn extend_digest_ttl(
        &self,
        _metadata: &RemoteExecutionMetadata,
        _request: ExtendDigestsTtlRequest,
    ) -> anyhow::Result<TDigest> {
        // TODO(arr)
        Err(anyhow::anyhow!("Not implemented (RE extend_digest_ttl)"))
    }

    pub fn get_execution_client(&self) -> &Self {
        self
    }

    pub fn get_cas_client(&self) -> &Self {
        self
    }

    pub fn get_action_cache_client(&self) -> &Self {
        self
    }

    async fn cas_client(&self) -> anyhow::Result<ContentAddressableStorageClient<GrpcService>> {
        let channel = self.pool.get(&self.cas_address).await?;
        Ok(
            ContentAddressableStorageClient::new(InterceptedService::new(
                channel,
                self.interceptor.dupe(),
            ))
            .max_decoding_message_size(self.max_decoding_msg_size),
        )
    }

    async fn bytestream_client(&self) -> anyhow::Result<ByteStreamClient<GrpcService>> {
        let channel = self.pool.get(&self.cas_address).await?;
        Ok(
            ByteStreamClient::new(InterceptedService::new(channel, self.interceptor.dupe()))
                .max_decoding_message_size(self.max_decoding_msg_size),
        )
    }

    async fn action_cache_client(&self) -> anyhow::Result<ActionCacheClient<GrpcService>> {
        let channel = self.pool.get(&self.action_cache_address).await?;
        Ok(ActionCacheClient::new(InterceptedService::new(
            channel,
            self.interceptor.dupe(),
        )))
    }

    pub fn get_metrics_client(&self) -> &Self {
        self
    }

    pub fn get_session_id(&self) -> &str {
        // TODO(aloiscochard): Return a unique ID, ideally from the GRPC client
        "GRPC-SESSION-ID"
    }

    pub fn get_experiment_name(&self) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
}

fn convert_action_result(action_result: ActionResult) -> anyhow::Result<TActionResult2> {
    let execution_metadata = action_result
        .execution_metadata
        .with_context(|| "The execution metadata are not defined.")?;

    let output_files = action_result.output_files.into_try_map(|output_file| {
        let output_file_digest = output_file.digest.with_context(|| "Digest not found.")?;

        anyhow::Ok(TFile {
            digest: DigestWithStatus {
                status: tstatus_ok(),
                digest: tdigest_from(output_file_digest),
                _dot_dot_default: (),
            },
            name: output_file.path,
            existed: false,
            executable: output_file.is_executable,
            ttl: 0,
            _dot_dot_default: (),
        })
    })?;

    let output_symlinks = action_result
        .output_symlinks
        .into_try_map(|output_symlink| {
            anyhow::Ok(TSymlink {
                name: output_symlink.path,
                target: output_symlink.target,
                _dot_dot_default: (),
            })
        })?;

    let output_directories = action_result
        .output_directories
        .into_try_map(|output_directory| {
            let digest = tdigest_from(
                output_directory
                    .tree_digest
                    .with_context(|| "Tree digest not defined.")?,
            );
            anyhow::Ok(TDirectory2 {
                path: output_directory.path,
                tree_digest: digest.clone(),
                root_directory_digest: digest,
                _dot_dot_default: (),
            })
        })?;

    let action_result = TActionResult2 {
        output_files,
        output_symlinks,
        output_directories,
        exit_code: action_result.exit_code,
        stdout_raw: Some(action_result.stdout_raw),
        stdout_digest: action_result.stdout_digest.map(tdigest_from),
        stderr_raw: Some(action_result.stderr_raw),
        stderr_digest: action_result.stderr_digest.map(tdigest_from),

        execution_metadata: TExecutedActionMetadata {
            worker: execution_metadata.worker,
            queued_timestamp: ttimestamp_from(execution_metadata.queued_timestamp),
            worker_start_timestamp: ttimestamp_from(execution_metadata.worker_start_timestamp),
            worker_completed_timestamp: ttimestamp_from(
                execution_metadata.worker_completed_timestamp,
            ),
            input_fetch_start_timestamp: ttimestamp_from(
                execution_metadata.input_fetch_start_timestamp,
            ),
            input_fetch_completed_timestamp: ttimestamp_from(
                execution_metadata.input_fetch_completed_timestamp,
            ),
            execution_start_timestamp: ttimestamp_from(
                execution_metadata.execution_start_timestamp,
            ),
            execution_completed_timestamp: ttimestamp_from(
                execution_metadata.execution_completed_timestamp,
            ),
            output_upload_start_timestamp: ttimestamp_from(
                execution_metadata.output_upload_start_timestamp,
            ),
            output_upload_completed_timestamp: ttimestamp_from(
                execution_metadata.output_upload_completed_timestamp,
            ),
            input_analyzing_start_timestamp: Default::default(),
            input_analyzing_completed_timestamp: Default::default(),
            execution_dir: "".to_owned(),
            execution_attempts: 0,
            last_queued_timestamp: Default::default(),
            ..Default::default()
        },
        ..Default::default()
    };

    Ok(action_result)
}

fn convert_t_action_result2(t_action_result: TActionResult2) -> anyhow::Result<ActionResult> {
    let t_execution_metadata = t_action_result.execution_metadata;
    let virtual_execution_duration = prost_types::Duration::try_from(
        t_execution_metadata
            .execution_completed_timestamp
            .saturating_duration_since(&t_execution_metadata.execution_start_timestamp),
    )?;
    let execution_metadata = Some(ExecutedActionMetadata {
        worker: t_execution_metadata.worker,
        queued_timestamp: Some(ttimestamp_to(t_execution_metadata.queued_timestamp)),
        worker_start_timestamp: Some(ttimestamp_to(t_execution_metadata.worker_start_timestamp)),
        worker_completed_timestamp: Some(ttimestamp_to(
            t_execution_metadata.worker_completed_timestamp,
        )),
        input_fetch_start_timestamp: Some(ttimestamp_to(
            t_execution_metadata.input_fetch_start_timestamp,
        )),
        input_fetch_completed_timestamp: Some(ttimestamp_to(
            t_execution_metadata.input_fetch_completed_timestamp,
        )),
        execution_start_timestamp: Some(ttimestamp_to(
            t_execution_metadata.execution_start_timestamp,
        )),
        execution_completed_timestamp: Some(ttimestamp_to(
            t_execution_metadata.execution_completed_timestamp,
        )),
        virtual_execution_duration: Some(virtual_execution_duration),
        output_upload_start_timestamp: Some(ttimestamp_to(
            t_execution_metadata.output_upload_start_timestamp,
        )),
        output_upload_completed_timestamp: Some(ttimestamp_to(
            t_execution_metadata.output_upload_completed_timestamp,
        )),
        auxiliary_metadata: Vec::new(),
    });

    let output_files = t_action_result
        .output_files
        .into_map(|output_file| OutputFile {
            path: output_file.name,
            digest: Some(tdigest_to(output_file.digest.digest)),
            is_executable: output_file.executable,
            contents: Vec::new(),
            node_properties: None,
        });

    let output_symlinks =
        t_action_result
            .output_symlinks
            .into_map(|output_symlink| OutputSymlink {
                path: output_symlink.name,
                target: output_symlink.target,
                node_properties: None,
            });

    let output_directories = t_action_result
        .output_directories
        .into_map(|output_directory| {
            let digest = tdigest_to(output_directory.tree_digest);
            OutputDirectory {
                path: output_directory.path,
                tree_digest: Some(digest.clone()),
                is_topologically_sorted: false,
                root_directory_digest: None,
            }
        });

    let action_result = ActionResult {
        output_files,
        output_symlinks,
        output_directories,
        exit_code: t_action_result.exit_code,
        stdout_raw: Vec::new(),
        stdout_digest: t_action_result.stdout_digest.map(tdigest_to),
        stderr_raw: Vec::new(),
        stderr_digest: t_action_result.stderr_digest.map(tdigest_to),
        execution_metadata,
        ..Default::default()
    };

    Ok(action_result)
}

/// Decodes one `Operation` message from the RE execution stream into the stage or terminal
/// response it represents. Returns an error for a malformed message or a `done` operation that
/// carries an application-level failure. Neither is recoverable by reattaching.
fn decode_operation(msg: Operation) -> anyhow::Result<ExecuteWithProgressResponse> {
    if msg.done {
        match msg
            .result
            .context("Missing `result` when message was `done`")?
        {
            OpResult::Error(rpc_status) => Err(REClientError {
                code: TCode(rpc_status.code),
                message: rpc_status.message,
                group: TCodeReasonGroup::UNKNOWN,
            }
            .into()),
            OpResult::Response(any) => {
                let execute_response_grpc: GExecuteResponse =
                    GExecuteResponse::decode(&any.value[..])?;

                check_status(execute_response_grpc.status.unwrap_or_default())?;

                let action_result = execute_response_grpc
                    .result
                    .with_context(|| "The action result is not defined.")?;

                let action_result = convert_action_result(action_result)?;

                let execute_response = ExecuteResponse {
                    action_result,
                    action_result_digest: TDigest::default(),
                    action_result_ttl: 0,
                    status: TStatus {
                        code: TCode::OK,
                        message: execute_response_grpc.message,
                        ..Default::default()
                    },
                    cached_result: execute_response_grpc.cached_result,
                    action_digest: Default::default(), // Filled in by execute_with_progress_impl.
                };

                Ok(ExecuteWithProgressResponse {
                    stage: Stage::COMPLETED,
                    execute_response: Some(execute_response),
                    ..Default::default()
                })
            }
        }
    } else {
        let meta = ExecuteOperationMetadata::decode(&msg.metadata.unwrap_or_default().value[..])?;

        let stage = match execution_stage::Value::try_from(meta.stage) {
            Ok(execution_stage::Value::Unknown) => Stage::UNKNOWN,
            Ok(execution_stage::Value::CacheCheck) => Stage::CACHE_CHECK,
            Ok(execution_stage::Value::Queued) => Stage::QUEUED,
            Ok(execution_stage::Value::Executing) => Stage::EXECUTING,
            Ok(execution_stage::Value::Completed) => Stage::COMPLETED,
            _ => Stage::UNKNOWN,
        };

        Ok(ExecuteWithProgressResponse {
            stage,
            execute_response: None,
            ..Default::default()
        })
    }
}

/// Drives an `Execute` call to completion, decoding each streamed `Operation` into a
/// progress or terminal response and recovering transparently from a severed stream.
///
/// `execute_f` and `wait_execution_f` carry the RPC calls, so one recovery path serves the
/// initial `Execute` call, every `Execute` reattach, and every `WaitExecution` reattach, and so
/// tests can drive the path with scripted streams. `execute_f` issues `Execute` with the
/// request built here; `wait_execution_f` reattaches to the operation name last seen.
/// `execute_reattach_budget`, `execute_reattach_limiter`, and
/// `execute_reattach_wait_execution_unimplemented` are threaded through to the `ReattachState`
/// that owns recovery for the lifetime of the returned stream; see
/// [`ReattachState::recover`].
async fn execute_with_progress_impl<F, Fut, WF, WFut>(
    instance_name: &InstanceName,
    mut execute_request: ExecuteRequest,
    execute_f: F,
    wait_execution_f: WF,
    execute_reattach_budget: Option<Duration>,
    execute_reattach_limiter: Arc<Semaphore>,
    execute_reattach_wait_execution_unimplemented: Arc<AtomicBool>,
) -> anyhow::Result<BoxStream<'static, anyhow::Result<ExecuteWithProgressResponse>>>
where
    F: Fn(GExecuteRequest) -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<BoxStream<'static, Result<Operation, tonic::Status>>>>
        + Send
        + 'static,
    WF: Fn(String) -> WFut + Send + 'static,
    WFut: Future<Output = anyhow::Result<BoxStream<'static, Result<Operation, tonic::Status>>>>
        + Send
        + 'static,
{
    let action_digest = tdigest_to(execute_request.action_digest.clone());
    let priority = execute_request
        .execution_policy
        .as_ref()
        .map(|ep| ep.priority)
        .unwrap_or_default();

    let request = GExecuteRequest {
        instance_name: instance_name.as_str().to_owned(),
        skip_cache_lookup: execute_request.skip_cache_lookup,
        execution_policy: Some(ExecutionPolicy { priority }),
        results_cache_policy: Some(ResultsCachePolicy { priority: 0 }),
        action_digest: Some(action_digest.clone()),
        ..Default::default()
    };

    let stream = retry(|| execute_f(request.clone())).await?;

    let state = ReattachState::new(
        execute_f,
        wait_execution_f,
        request,
        stream,
        execute_reattach_budget,
        execute_reattach_limiter,
        execute_reattach_wait_execution_unimplemented,
    );

    let stream = futures::stream::try_unfold(state, move |mut state| async move {
        loop {
            if state.seen_done {
                return Ok(None);
            }

            match state.stream.try_next().await {
                Ok(Some(msg)) => {
                    state.observe_message(&msg);
                    let mut response = decode_operation(msg)?;
                    response.reattach_stats = state.stats();
                    return Ok(Some((response, state)));
                }
                Ok(None) => {
                    // With reattach disabled, a stream ending before a terminal response ends
                    // here as `stream.try_next()` reports it, and the consumer's own
                    // end-of-stream handling applies.
                    if state.is_disabled() {
                        return Ok(None);
                    }
                    state
                        .recover(
                            RetryCause::CleanEof,
                            anyhow::anyhow!(
                                "the RE execute stream ended before a terminal response"
                            ),
                        )
                        .await?;
                }
                Err(status) => match classify(&status, state.origin) {
                    Some(cause) => {
                        let trigger = anyhow::Error::new(status).context("RE channel error");
                        state.recover(cause, trigger).await?;
                    }
                    None => {
                        return Err(anyhow::Error::new(status).context("RE channel error"));
                    }
                },
            }
        }
    });

    // The action digest is filled in here, downstream of recovery, which keeps
    // `execute_request` out of `decode_operation` and the reattach closures. No reattach
    // clones it.

    let stream = stream.map(move |mut r| {
        match &mut r {
            Ok(ExecuteWithProgressResponse {
                execute_response: Some(response),
                ..
            }) => {
                response.action_digest = std::mem::take(&mut execute_request.action_digest);
            }
            _ => {}
        };

        r
    });

    Ok(stream.boxed())
}

async fn download_impl<Byt, BytRet, Cas>(
    instance_name: &InstanceName,
    request: DownloadRequest,
    bystream_compressor: Option<Compressor>,
    max_total_batch_size: usize,
    cas_f: impl Fn(BatchReadBlobsRequest) -> Cas,
    bystream_fut: impl Fn(ReadRequest) -> Byt + Sync + Send + Copy,
) -> anyhow::Result<DownloadResponse>
where
    Byt: Future<Output = anyhow::Result<Pin<Box<BytRet>>>>,
    BytRet: Stream<Item = Result<ReadResponse, tonic::Status>> + Send,
    Cas: Future<Output = anyhow::Result<BatchReadBlobsResponse>>,
{
    fn resource_name(
        instance_name: &InstanceName,
        compressor: Option<Compressor>,
        digest: &TDigest,
    ) -> String {
        if let Some(compressor) = compressor {
            format!(
                "{}compressed-blobs/{}/{}/{}",
                instance_name.as_resource_prefix(),
                compressor.name(),
                digest.hash,
                digest.size_in_bytes,
            )
        } else {
            format!(
                "{}blobs/{}/{}",
                instance_name.as_resource_prefix(),
                digest.hash,
                digest.size_in_bytes,
            )
        }
    }

    let bystream_fut = |digest: TDigest| async move {
        let resource_name = resource_name(instance_name, bystream_compressor, &digest);

        bystream_fut(ReadRequest {
            resource_name: resource_name.clone(),
            read_offset: 0,
            read_limit: 0,
        })
        .await
        // adapt the tokio Stream of ReadResponse into a StreamReader
        .map(|p| {
            let blob_reader = StreamReader::new(
                p.map(|r| r.map(|rr| Cursor::new(rr.data)).map_err(io::Error::other)),
            );
            let reader: Pin<Box<dyn AsyncRead + Unpin + Send>> = match bystream_compressor {
                None => Pin::new(Box::new(blob_reader)),
                Some(Compressor::Zstd) => {
                    let mut decoder = ZstdDecoder::new(blob_reader);
                    decoder.multiple_members(true);
                    Pin::new(Box::new(decoder))
                }
                Some(Compressor::Deflate) => {
                    let mut decoder = DeflateDecoder::new(blob_reader);
                    decoder.multiple_members(true);
                    Pin::new(Box::new(decoder))
                }
                Some(Compressor::Brotli) => {
                    let mut decoder = BrotliDecoder::new(blob_reader);
                    decoder.multiple_members(true);
                    Pin::new(Box::new(decoder))
                }
            };

            reader
        })
        .with_context(|| format!("Failed to read {resource_name} from Bytestream service"))
    };

    let inlined_digests = request.inlined_digests.unwrap_or_default();
    let file_digests = request.file_digests.unwrap_or_default();

    let mut curr_size = 0;
    let mut requests = vec![];
    let mut curr_digests = vec![];
    for digest in file_digests
        .iter()
        .map(|req| &req.named_digest.digest)
        .chain(inlined_digests.iter())
        .map(|d| tdigest_to(d.clone()))
        .filter(|d| d.size_bytes > 0)
    {
        if digest.size_bytes as usize >= max_total_batch_size {
            // digest is too big to download in a BatchReadBlobsRequest
            // need to use the bytstream api
            continue;
        }
        curr_size += digest.size_bytes;
        if curr_size >= max_total_batch_size as i64 {
            let read_blob_req = BatchReadBlobsRequest {
                instance_name: instance_name.as_str().to_owned(),
                digests: std::mem::take(&mut curr_digests),
                acceptable_compressors: vec![compressor::Value::Identity as i32],
                ..Default::default()
            };
            requests.push(read_blob_req);
            curr_size = digest.size_bytes;
        }
        curr_digests.push(digest.clone());
    }

    if !curr_digests.is_empty() {
        let read_blob_req = BatchReadBlobsRequest {
            instance_name: instance_name.as_str().to_owned(),
            digests: std::mem::take(&mut curr_digests),
            acceptable_compressors: vec![compressor::Value::Identity as i32],
            ..Default::default()
        };
        requests.push(read_blob_req);
    }

    let mut batched_blobs_response = HashMap::new();
    for read_blob_req in requests {
        let resp = retry(|| async {
            cas_f(read_blob_req.clone())
                .await
                .context("Failed to make BatchReadBlobs request")
        })
        .await?;
        for r in resp.responses.into_iter() {
            let digest = tdigest_from(r.digest.context("Response digest not found.")?);
            check_status(r.status.unwrap_or_default())?;
            batched_blobs_response.insert(digest, r.data);
        }
    }

    let get = |digest: &TDigest| -> anyhow::Result<Vec<u8>> {
        if digest.size_in_bytes == 0 {
            return Ok(Vec::new());
        }

        Ok(batched_blobs_response
            .get(digest)
            .with_context(|| format!("Did not receive digest data for `{digest}`"))?
            .clone())
    };

    let mut inlined_blobs = vec![];
    for digest in inlined_digests {
        let data = if digest.size_in_bytes as usize >= max_total_batch_size {
            retry(|| async {
                let mut accum = vec![];
                let mut reader = bystream_fut(digest.clone()).await?;
                tokio::io::copy(&mut reader, &mut accum).await?;
                Ok(accum)
            })
            .await?
        } else {
            get(&digest)?
        };
        inlined_blobs.push(InlinedDigestWithStatus {
            digest,
            status: tstatus_ok(),
            blob: data,
        })
    }

    let writes = file_digests.iter().map(|req| async {
        let mut opts = OpenOptions::new();
        opts.read(true).write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            if req.is_executable {
                opts.mode(0o755);
            } else {
                opts.mode(0o644);
            }
        }

        retry(|| async {
            let mut file = opts
                .open(&req.named_digest.name)
                .await
                .context("Error opening")?;

            // If the data is small enough to be transferred in a batch
            // blob update, write it all at once to the file. Otherwise, it'll
            // be streamed in chunks as the remote responds.
            if req.named_digest.digest.size_in_bytes < max_total_batch_size as i64 {
                let data = get(&req.named_digest.digest)?;
                file.write_all(&data)
                    .await
                    .with_context(|| format!("Error writing: {}", req.named_digest.digest))?;
            } else {
                let mut reader = bystream_fut(req.named_digest.digest.clone()).await?;
                tokio::io::copy(&mut reader, &mut file)
                    .await
                    .with_context(|| {
                        format!("Error writing chunk of: {}", req.named_digest.digest)
                    })?;
            }
            file.flush().await.context("Error flushing")?;
            anyhow::Ok(())
        })
        .await
        .with_context(|| {
            format!(
                "Error downloading digest `{}` to `{}`",
                req.named_digest.digest, req.named_digest.name,
            )
        })
    });

    buck2_util::future::try_join_all(writes).await?;

    Ok(DownloadResponse {
        inlined_blobs: Some(inlined_blobs),
        directories: None,
        local_cache_stats: Default::default(),
    })
}

async fn upload_impl<Byt, Cas>(
    instance_name: &InstanceName,
    request: UploadRequest,
    bystream_compressor: Option<Compressor>,
    max_total_batch_size: usize,
    max_concurrent_uploads: Option<usize>,
    cas_f: impl Fn(BatchUpdateBlobsRequest) -> Cas + Sync + Send + Copy,
    bystream_fut: impl Fn(Vec<WriteRequest>) -> Byt + Sync + Send + Copy,
) -> anyhow::Result<UploadResponse>
where
    Cas: Future<Output = anyhow::Result<BatchUpdateBlobsResponse>> + Send,
    Byt: Future<Output = anyhow::Result<WriteResponse>> + Send,
{
    fn resource_name(
        instance_name: &InstanceName,
        client_uuid: &str,
        compressor: Option<Compressor>,
        digest: &TDigest,
    ) -> String {
        if let Some(compressor) = compressor {
            format!(
                "{}uploads/{}/compressed-blobs/{}/{}/{}",
                instance_name.as_resource_prefix(),
                client_uuid,
                compressor.name(),
                digest.hash,
                digest.size_in_bytes,
            )
        } else {
            format!(
                "{}uploads/{}/blobs/{}/{}",
                instance_name.as_resource_prefix(),
                client_uuid,
                digest.hash,
                digest.size_in_bytes,
            )
        }
    }

    // NOTE if we stop recording blob_hashes, we can drop out a lot of allocations.
    let mut upload_futures: Vec<BoxFuture<anyhow::Result<Vec<String>>>> = vec![];

    // For small file uploads the client should group them together and call `BatchUpdateBlobs`
    // https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto#L205
    let mut batched_blob_updates = BatchUploadReqAggregator::new(max_total_batch_size);

    // Adapt the given bystream_fut to take in an AsyncBufRead
    let bystream_fut = |resource_name: String, reader: Box<dyn AsyncBufRead + Unpin + Send>| async move {
        let mut reader: Pin<Box<dyn AsyncRead + Unpin + Send>> = match bystream_compressor {
            None => Pin::new(Box::new(reader)),
            Some(Compressor::Zstd) => Pin::new(Box::new(ZstdEncoder::new(reader))),
            Some(Compressor::Deflate) => Pin::new(Box::new(DeflateEncoder::new(reader))),
            Some(Compressor::Brotli) => Pin::new(Box::new(BrotliEncoder::new(reader))),
        };

        let mut current_offset = 0;
        let mut upload_segments = Vec::new();
        let mut buf = vec![0; max_total_batch_size];
        loop {
            let n_read = reader.read(&mut buf).await?;
            if n_read == 0 {
                break;
            }
            upload_segments.push(WriteRequest {
                resource_name: resource_name.clone(),
                write_offset: current_offset,
                finish_write: false,
                data: buf[0..n_read].to_vec(),
            });
            current_offset += n_read as i64;
        }
        if let Some(last_segment) = upload_segments.last_mut() {
            last_segment.finish_write = true;
        }

        if upload_segments.is_empty() {
            // As an optimization, we can silently skip uploading empty blobs
            return Ok(());
        }

        let response = bystream_fut(upload_segments).await?;
        if response.committed_size != current_offset && response.committed_size != -1 {
            return Err(anyhow::anyhow!(
                "Failed to upload `{resource_name}`: invalid committed_size from WriteResponse"
            ));
        }

        Ok(())
    };

    // Create futures for any blobs that need uploading.
    for blob in request.inlined_blobs_with_digest.unwrap_or_default() {
        let hash = blob.digest.hash.clone();
        let size = blob.digest.size_in_bytes;

        if size < max_total_batch_size as i64 {
            batched_blob_updates.push(BatchUploadRequest::Blob(blob));
            continue;
        }

        let data = prost::bytes::Bytes::from(blob.blob);
        let client_uuid = uuid::Uuid::new_v4().to_string();
        let resource_name = resource_name(
            instance_name,
            &client_uuid,
            bystream_compressor,
            &blob.digest,
        );
        let fut = async move {
            retry(|| async {
                bystream_fut(resource_name.clone(), Box::new(Cursor::new(data.clone()))).await?;
                Ok(vec![hash.clone()])
            })
            .await
        };
        upload_futures.push(Box::pin(fut));
    }

    // Create futures for any files that needs uploading.
    for file in request.files_with_digest.unwrap_or_default() {
        let hash = file.digest.hash.clone();
        let size = file.digest.size_in_bytes;
        let name = file.name.clone();
        if size < max_total_batch_size as i64 {
            batched_blob_updates.push(BatchUploadRequest::File(file));
            continue;
        }
        let client_uuid = uuid::Uuid::new_v4().to_string();
        let resource_name = resource_name(
            instance_name,
            &client_uuid,
            bystream_compressor,
            &file.digest,
        );

        let fut = async move {
            retry(|| async {
                let file = tokio::fs::File::open(&name)
                    .await
                    .with_context(|| format!("Opening `{name}` for reading failed"))?;

                bystream_fut(resource_name.clone(), Box::new(BufReader::new(file))).await?;
                Ok(vec![hash.clone()])
            })
            .await
        };
        upload_futures.push(Box::pin(fut));
    }

    // Create futures for any files small enough that they
    // should be uploaded in batches.
    let batched_blob_updates = batched_blob_updates.done();
    for batch in batched_blob_updates {
        let fut = async move {
            let mut re_request = BatchUpdateBlobsRequest {
                instance_name: instance_name.as_str().to_owned(),
                requests: vec![],
                ..Default::default()
            };
            for blob in batch {
                match blob {
                    BatchUploadRequest::Blob(blob) => {
                        re_request.requests.push(Request {
                            digest: Some(tdigest_to(blob.digest.clone())),
                            data: blob.blob.clone(),
                            compressor: compressor::Value::Identity as i32,
                        });
                    }
                    BatchUploadRequest::File(file) => {
                        // These should be small files, so no need to use a buffered reader.
                        let mut fin = tokio::fs::File::open(&file.name)
                            .await
                            .with_context(|| format!("Opening {} for reading failed", file.name))?;
                        let mut data = vec![];
                        fin.read_to_end(&mut data).await?;

                        re_request.requests.push(Request {
                            digest: Some(tdigest_to(file.digest.clone())),
                            data,
                            compressor: compressor::Value::Identity as i32,
                        });
                    }
                }
            }
            let blob_hashes = re_request
                .requests
                .iter()
                .map(|x| x.digest.as_ref().unwrap().hash.clone())
                .collect::<Vec<String>>();

            let response = retry(|| async { cas_f(re_request.clone()).await }).await?;
            let failures: Vec<String> = response
                .responses
                .iter()
                .filter_map(|r| {
                    r.status.as_ref().and_then(|s| {
                        if s.code == (Code::Ok as i32) {
                            None
                        } else {
                            Some(format!(
                                "Unable to upload blob '{}', rpc status code: {}, message: \"{}\"",
                                r.digest.as_ref().map_or("N/A", |d| &d.hash),
                                s.code,
                                s.message
                            ))
                        }
                    })
                })
                .collect();

            if !failures.is_empty() {
                return Err(anyhow::anyhow!("Batch upload failed: {:?}", failures));
            }
            Ok(blob_hashes)
        };
        upload_futures.push(Box::pin(fut));
    }

    let blob_hashes = if let Some(concurrency_limit) = max_concurrent_uploads {
        futures::stream::iter(upload_futures)
            .buffer_unordered(concurrency_limit)
            .try_collect::<Vec<Vec<String>>>()
            .await?
    } else {
        futures::future::try_join_all(upload_futures).await?
    };

    tracing::debug!("uploaded: {:?}", blob_hashes);
    Ok(UploadResponse {})
}

fn with_re_metadata<T>(
    t: T,
    metadata: &RemoteExecutionMetadata,
    use_fbcode_metadata: bool,
) -> tonic::Request<T> {
    // This creates a new Tonic request with attached metadata for the RE
    // backend. There are two cases here we need to support:
    //
    //   - Servers that abide by the remote execution apis defined with Bazel,
    //     AKA the "OSS RE API", which this package implements
    //   - The internal RE solution used at Meta, which uses a different API,
    //     but is compatible with the OSS RE API to some extent.
    //
    // The second case is supported only through attaching some metadata to the
    // request, which the fbcode RE service understands; and the reason for all
    // of this is that it allows this OSS client package to be tested inside of
    // fbcode builds within Meta. So there doesn't need to be a separate CI
    // check.
    //
    // However, we don't need it for FOSS builds of Buck2. And in theory we
    // could test the OSS Bazel API in the upstream GitHub CI, but doing it this
    // way is only a little ugly, it's hidden, and it helps ensure the internal
    // Meta builds catch those issues earlier.

    let mut msg = tonic::Request::new(t);

    if use_fbcode_metadata {
        // This is pretty ugly, but the protobuf spec that defines this is
        // internal, so considering field numbers need to be stable anyway (=
        // low risk), and this is not used in prod (= low impact if this goes
        // wrong), we just inline it here. This is a small hack that lets us use
        // our internal RE using this GRPC client for testing.
        //
        // This is defined in `fbcode/remote_execution/re_cas_common/grpc/proto/metadata.proto`.
        #[derive(prost::Message)]
        struct Metadata {
            #[prost(message, optional, tag = "15")]
            platform: Option<crate::grpc::Platform>,
            #[prost(string, optional, tag = "18")]
            use_case_id: Option<String>,
        }

        let mut encoded = Vec::new();
        Metadata {
            platform: metadata.platform.clone(),
            use_case_id: Some(metadata.use_case_id.clone()),
        }
        .encode(&mut encoded)
        .expect("Encoding into a Vec cannot not fail");

        msg.metadata_mut()
            .insert_bin("re-metadata-bin", MetadataValue::from_bytes(&encoded));
    } else {
        let mut encoded = Vec::new();
        RequestMetadata {
            tool_details: Some(ToolDetails {
                tool_name: "buck2".to_owned(),
                // TODO(#503): Pull the BuckVersion::get_unique_id() from BuckDaemon
                tool_version: "0.1.0".to_owned(),
            }),
            action_id: "".to_owned(),
            tool_invocation_id: metadata
                .buck_info
                .as_ref()
                .map_or(String::new(), |buck_info| buck_info.build_id.clone()),
            correlated_invocations_id: "".to_owned(),
            action_mnemonic: "".to_owned(),
            target_id: "".to_owned(),
            configuration_id: "".to_owned(),
        }
        .encode(&mut encoded)
        .expect("Encoding into a Vec cannot not fail");

        msg.metadata_mut().insert_bin(
            "build.bazel.remote.execution.v2.requestmetadata-bin",
            MetadataValue::from_bytes(&encoded),
        );
    };
    msg
}

/// Replace occurrences of $FOO in a string with the value of the env var $FOO.
fn substitute_env_vars(s: &str) -> anyhow::Result<String> {
    substitute_env_vars_impl(s, |v| std::env::var(v))
}

fn substitute_env_vars_impl(
    s: &str,
    getter: impl Fn(&str) -> Result<String, VarError>,
) -> anyhow::Result<String> {
    static ENV_REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new("\\$[a-zA-Z_][a-zA-Z_0-9]*").unwrap());

    let mut out = String::with_capacity(s.len());
    let mut last_idx = 0;

    for mat in ENV_REGEX.find_iter(s) {
        out.push_str(&s[last_idx..mat.start()]);
        let var = &mat.as_str()[1..];
        let val = getter(var).with_context(|| format!("Error substituting `{}`", mat.as_str()))?;
        out.push_str(&val);
        last_idx = mat.end();
    }

    if last_idx < s.len() {
        out.push_str(&s[last_idx..s.len()]);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::Ordering;
    use std::sync::atomic::AtomicU16;
    use std::sync::atomic::AtomicUsize;

    use re_grpc_proto::build::bazel::remote::execution::v2::batch_read_blobs_response;
    use re_grpc_proto::build::bazel::remote::execution::v2::batch_update_blobs_response;

    use super::*;

    #[tokio::test]
    async fn test_download_named() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;

        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;

        let path2 = work.path().join("path2");
        let path2 = path2.to_str().context("tempdir is not utf8")?;

        let digest1 = TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let digest2 = TDigest {
            hash: "bb".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let req = DownloadRequest {
            file_digests: Some(vec![
                NamedDigestWithPermissions {
                    named_digest: NamedDigest {
                        name: path1.to_owned(),
                        digest: digest1.clone(),
                        ..Default::default()
                    },
                    is_executable: true,
                    ..Default::default()
                },
                NamedDigestWithPermissions {
                    named_digest: NamedDigest {
                        name: path2.to_owned(),
                        digest: digest2.clone(),
                        ..Default::default()
                    },
                    is_executable: false,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        let res = BatchReadBlobsResponse {
            responses: vec![
                // Reply out of order
                batch_read_blobs_response::Response {
                    digest: Some(tdigest_to(digest2.clone())),
                    data: vec![4, 5, 6],
                    ..Default::default()
                },
                batch_read_blobs_response::Response {
                    digest: Some(tdigest_to(digest1.clone())),
                    data: vec![1, 2, 3],
                    ..Default::default()
                },
            ],
        };

        download_impl(
            &InstanceName(None),
            req,
            None,
            10000,
            |req| {
                let res = res.clone();
                let digest1 = digest1.clone();
                let digest2 = digest2.clone();
                async move {
                    assert_eq!(req.digests.len(), 2);
                    assert_eq!(req.digests[0], tdigest_to(digest1));
                    assert_eq!(req.digests[1], tdigest_to(digest2));
                    Ok(res.clone())
                }
            },
            |_digest| async move { anyhow::Ok(Box::pin(futures::stream::iter(vec![]))) },
        )
        .await?;

        assert_eq!(tokio::fs::read(&path1).await?, vec![1, 2, 3]);
        assert_eq!(tokio::fs::read(&path2).await?, vec![4, 5, 6]);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                tokio::fs::metadata(&path1).await?.permissions().mode() & 0o111,
                0o111
            );
            assert_eq!(
                tokio::fs::metadata(&path2).await?.permissions().mode() & 0o111,
                0o000
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_download_large_named() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;

        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;

        let path2 = work.path().join("path2");
        let path2 = path2.to_str().context("tempdir is not utf8")?;

        let digest1 = TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let blob_data = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];

        let digest2 = TDigest {
            hash: "xl".to_owned(),
            size_in_bytes: 18,
            ..Default::default()
        };

        let req = DownloadRequest {
            file_digests: Some(vec![
                NamedDigestWithPermissions {
                    named_digest: NamedDigest {
                        name: path1.to_owned(),
                        digest: digest1.clone(),
                        ..Default::default()
                    },
                    is_executable: true,
                    ..Default::default()
                },
                NamedDigestWithPermissions {
                    named_digest: NamedDigest {
                        name: path2.to_owned(),
                        digest: digest2.clone(),
                        ..Default::default()
                    },
                    is_executable: false,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        let res = BatchReadBlobsResponse {
            responses: vec![
                // Reply out of order
                batch_read_blobs_response::Response {
                    digest: Some(tdigest_to(digest1.clone())),
                    data: vec![1, 2, 3],
                    ..Default::default()
                },
            ],
        };

        let read_response1 = ReadResponse {
            data: blob_data[..10].to_vec(),
        };
        let read_response2 = ReadResponse {
            data: blob_data[10..].to_vec(),
        };

        download_impl(
            &InstanceName(None),
            req,
            None,
            10, // kept small to simulate a large file download
            |req| {
                let res = res.clone();
                let digest1 = digest1.clone();
                async move {
                    assert_eq!(req.digests.len(), 1);
                    assert_eq!(req.digests[0], tdigest_to(digest1));
                    Ok(res.clone())
                }
            },
            |req| {
                let read_response1 = read_response1.clone();
                let read_response2 = read_response2.clone();
                async move {
                    assert_eq!(req.resource_name, "blobs/xl/18");
                    anyhow::Ok(Box::pin(futures::stream::iter(vec![
                        Ok(read_response1),
                        Ok(read_response2),
                    ])))
                }
            },
        )
        .await?;

        assert_eq!(tokio::fs::read(&path1).await?, vec![1, 2, 3]);
        assert_eq!(tokio::fs::read(&path2).await?, blob_data);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                tokio::fs::metadata(&path1).await?.permissions().mode() & 0o111,
                0o111
            );
            assert_eq!(
                tokio::fs::metadata(&path2).await?.permissions().mode() & 0o111,
                0o000
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_download_inlined() -> anyhow::Result<()> {
        let digest1 = &TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let digest2 = &TDigest {
            hash: "bb".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let req = DownloadRequest {
            inlined_digests: Some(vec![digest1.clone(), digest2.clone()]),
            ..Default::default()
        };

        let res = BatchReadBlobsResponse {
            responses: vec![
                // Reply out of order
                batch_read_blobs_response::Response {
                    digest: Some(tdigest_to(digest2.clone())),
                    data: vec![4, 5, 6],
                    ..Default::default()
                },
                batch_read_blobs_response::Response {
                    digest: Some(tdigest_to(digest1.clone())),
                    data: vec![1, 2, 3],
                    ..Default::default()
                },
            ],
        };

        let res = download_impl(
            &InstanceName(None),
            req,
            None,
            100000,
            |req| {
                let res = res.clone();
                let digest1 = digest1.clone();
                let digest2 = digest2.clone();
                async move {
                    assert_eq!(req.digests.len(), 2);
                    assert_eq!(req.digests[0], tdigest_to(digest1));
                    assert_eq!(req.digests[1], tdigest_to(digest2));
                    Ok(res)
                }
            },
            |_digest| async move { anyhow::Ok(Box::pin(futures::stream::iter(vec![]))) },
        )
        .await?;

        let inlined_blobs = res.inlined_blobs.unwrap();

        assert_eq!(inlined_blobs.len(), 2);

        assert_eq!(inlined_blobs[0].digest, *digest1);
        assert_eq!(inlined_blobs[0].blob, vec![1, 2, 3]);

        assert_eq!(inlined_blobs[1].digest, *digest2);
        assert_eq!(inlined_blobs[1].blob, vec![4, 5, 6]);

        Ok(())
    }

    #[tokio::test]
    async fn test_download_multiple_batches() -> anyhow::Result<()> {
        let digest1 = &TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let digest2 = &TDigest {
            hash: "bb".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let digest3 = &TDigest {
            hash: "cc".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let digest4 = &TDigest {
            hash: "dd".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let digest5 = &TDigest {
            hash: "dd".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let digest6 = &TDigest {
            hash: "dd".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let digests = vec![
            digest1.clone(),
            digest2.clone(),
            digest3.clone(),
            digest4.clone(),
            digest5.clone(),
            digest6.clone(),
        ];

        let req = DownloadRequest {
            inlined_digests: Some(digests.clone()),
            ..Default::default()
        };

        let counter = AtomicU16::new(0);

        let res = download_impl(
            &InstanceName(None),
            req,
            None,
            7,
            |req| {
                counter.fetch_add(1, Ordering::Relaxed);
                let res = BatchReadBlobsResponse {
                    responses: req.digests.map(|d| batch_read_blobs_response::Response {
                        digest: Some(d.clone()),
                        data: vec![0, 1, 2],
                        ..Default::default()
                    }),
                };
                async { Ok(res) }
            },
            |_digest| async move { anyhow::Ok(Box::pin(futures::stream::iter(vec![]))) },
        )
        .await?;

        let inlined_blobs = res.inlined_blobs.unwrap();

        assert_eq!(inlined_blobs.len(), digests.len());
        assert_eq!(counter.load(Ordering::Relaxed), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_download_large_inlined() -> anyhow::Result<()> {
        let digest1 = &TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let digest2 = &TDigest {
            hash: "xl".to_owned(),
            size_in_bytes: 18,
            ..Default::default()
        };

        let req = DownloadRequest {
            inlined_digests: Some(vec![digest1.clone(), digest2.clone()]),
            ..Default::default()
        };

        let res = BatchReadBlobsResponse {
            responses: vec![
                // Reply out of order
                batch_read_blobs_response::Response {
                    digest: Some(tdigest_to(digest1.clone())),
                    data: vec![1, 2, 3],
                    ..Default::default()
                },
            ],
        };

        let blob_data = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];

        let read_response1 = ReadResponse {
            data: blob_data[..10].to_vec(),
        };
        let read_response2 = ReadResponse {
            data: blob_data[10..].to_vec(),
        };

        let res = download_impl(
            &InstanceName(None),
            req,
            None,
            10, // intentionally small value to keep data in the test blobs small
            |req| {
                let res = res.clone();
                let digest1 = digest1.clone();
                async move {
                    assert_eq!(req.digests.len(), 1);
                    assert_eq!(req.digests[0], tdigest_to(digest1));
                    Ok(res)
                }
            },
            |req| {
                let read_response1 = read_response1.clone();
                let read_response2 = read_response2.clone();
                async move {
                    assert_eq!(req.resource_name, "blobs/xl/18");
                    anyhow::Ok(Box::pin(futures::stream::iter(vec![
                        Ok(read_response1),
                        Ok(read_response2),
                    ])))
                }
            },
        )
        .await?;

        let inlined_blobs = res.inlined_blobs.unwrap();

        assert_eq!(inlined_blobs.len(), 2);

        assert_eq!(inlined_blobs[0].digest, *digest1);
        assert_eq!(inlined_blobs[0].blob, vec![1, 2, 3]);

        assert_eq!(inlined_blobs[1].digest, *digest2);
        assert_eq!(inlined_blobs[1].blob, blob_data);

        Ok(())
    }

    #[tokio::test]
    async fn test_download_empty() -> anyhow::Result<()> {
        let digest1 = &TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 0,
            ..Default::default()
        };

        let req = DownloadRequest {
            inlined_digests: Some(vec![digest1.clone()]),
            ..Default::default()
        };

        let res = BatchReadBlobsResponse { responses: vec![] };

        let res = download_impl(
            &InstanceName(None),
            req,
            None,
            100000,
            |req| {
                let res = res.clone();
                async move {
                    assert_eq!(req.digests.len(), 0);
                    Ok(res)
                }
            },
            |_digest| async move { anyhow::Ok(Box::pin(futures::stream::iter(vec![]))) },
        )
        .await?;

        let inlined_blobs = res.inlined_blobs.unwrap();

        assert_eq!(inlined_blobs.len(), 1);

        assert_eq!(inlined_blobs[0].digest, *digest1);
        assert!(inlined_blobs[0].blob.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_download_resource_name() -> anyhow::Result<()> {
        let digest1 = &TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 0,
            ..Default::default()
        };

        let req = DownloadRequest {
            inlined_digests: Some(vec![digest1.clone()]),
            ..Default::default()
        };

        download_impl(
            &InstanceName(Some("instance".to_owned())),
            req,
            None,
            0,
            |_req| async { panic!("not called") },
            |req| async move {
                assert_eq!(req.resource_name, "instance/blobs/aa/0");
                anyhow::Ok(Box::pin(futures::stream::iter(vec![])))
            },
        )
        .await?;

        Ok(())
    }

    fn default_reattach_budget() -> Option<Duration> {
        Some(Duration::from_secs(EXECUTE_REATTACH_BUDGET_SECS_DEFAULT))
    }

    fn default_reattach_limiter() -> Arc<Semaphore> {
        Arc::new(Semaphore::new(EXECUTE_REATTACH_CONCURRENCY_DEFAULT))
    }

    fn fresh_wait_execution_unimplemented_latch() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    fn test_execute_request(skip_cache_lookup: bool) -> ExecuteRequest {
        ExecuteRequest {
            action_digest: TDigest {
                hash: "aa".to_owned(),
                size_in_bytes: 3,
                ..Default::default()
            },
            skip_cache_lookup,
            ..Default::default()
        }
    }

    fn any_from(msg: &impl Message) -> prost_types::Any {
        prost_types::Any {
            type_url: String::new(),
            value: msg.encode_to_vec(),
        }
    }

    fn progress_operation(name: &str, stage: execution_stage::Value) -> Operation {
        Operation {
            name: name.to_owned(),
            done: false,
            metadata: Some(any_from(&ExecuteOperationMetadata {
                stage: stage as i32,
                ..Default::default()
            })),
            result: None,
        }
    }

    fn done_operation(name: &str) -> Operation {
        let action_result = ActionResult {
            execution_metadata: Some(ExecutedActionMetadata::default()),
            ..Default::default()
        };
        let execute_response = GExecuteResponse {
            result: Some(action_result),
            status: Some(Status::default()),
            ..Default::default()
        };
        Operation {
            name: name.to_owned(),
            done: true,
            metadata: None,
            result: Some(OpResult::Response(any_from(&execute_response))),
        }
    }

    fn done_error_operation(name: &str, code: i32, message: &str) -> Operation {
        Operation {
            name: name.to_owned(),
            done: true,
            metadata: None,
            result: Some(OpResult::Error(Status {
                code,
                message: message.to_owned(),
                ..Default::default()
            })),
        }
    }

    fn done_bad_status_operation(name: &str, code: i32, message: &str) -> Operation {
        let execute_response = GExecuteResponse {
            result: None,
            status: Some(Status {
                code,
                message: message.to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        Operation {
            name: name.to_owned(),
            done: true,
            metadata: None,
            result: Some(OpResult::Response(any_from(&execute_response))),
        }
    }

    /// A `tonic::Status` shaped the way tonic itself builds one from a transport-level `io`
    /// error: the error becomes both the status message and the head of its `source()` chain.
    fn severed_status(kind: std::io::ErrorKind) -> tonic::Status {
        tonic::Status::from_error(Box::new(std::io::Error::from(kind)))
    }

    /// A `tonic::Status` shaped the way tonic itself builds one from an HTTP/2 protocol error:
    /// the `h2::Error` becomes the head of the status's `source()` chain.
    fn goaway_status(reason: h2::Reason) -> tonic::Status {
        tonic::Status::from_error(Box::new(h2::Error::from(reason)))
    }

    #[tokio::test]
    async fn test_execute_with_progress_skip_cache_lookup() -> anyhow::Result<()> {
        for skip_cache_lookup in [true, false] {
            let req = test_execute_request(skip_cache_lookup);

            let stream = execute_with_progress_impl(
                &InstanceName(None),
                req,
                move |grpc_request| async move {
                    assert_eq!(grpc_request.skip_cache_lookup, skip_cache_lookup);
                    anyhow::Ok(futures::stream::iter(vec![Ok(done_operation("op-1"))]).boxed())
                },
                |_name| async move { panic!("WaitExecution should not be triggered") },
                default_reattach_budget(),
                default_reattach_limiter(),
                fresh_wait_execution_unimplemented_latch(),
            )
            .await?;

            let responses: Vec<_> = stream.try_collect().await?;
            assert_eq!(responses.len(), 1);
            assert_eq!(responses[0].stage, Stage::COMPLETED);
        }

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_reattach_wait_execution_after_severance() -> anyhow::Result<()> {
        let action_digest = TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };
        let req = ExecuteRequest {
            action_digest: action_digest.clone(),
            ..Default::default()
        };

        let execute_calls = Arc::new(AtomicUsize::new(0));
        let wait_execution_calls = Arc::new(AtomicUsize::new(0));

        let execute_f = {
            let execute_calls = execute_calls.dupe();
            move |_req: GExecuteRequest| {
                let execute_calls = execute_calls.dupe();
                async move {
                    execute_calls.fetch_add(1, Ordering::SeqCst);
                    anyhow::Ok(
                        futures::stream::iter(vec![
                            Ok(progress_operation("op-1", execution_stage::Value::Queued)),
                            Err(severed_status(std::io::ErrorKind::ConnectionReset)),
                        ])
                        .boxed(),
                    )
                }
            }
        };

        let wait_execution_f = {
            let wait_execution_calls = wait_execution_calls.dupe();
            move |name: String| {
                let wait_execution_calls = wait_execution_calls.dupe();
                async move {
                    assert_eq!(name, "op-1");
                    wait_execution_calls.fetch_add(1, Ordering::SeqCst);
                    anyhow::Ok(
                        futures::stream::iter(vec![
                            Ok(progress_operation(
                                "op-1",
                                execution_stage::Value::Executing,
                            )),
                            Ok(done_operation("op-1")),
                        ])
                        .boxed(),
                    )
                }
            }
        };

        let stream = execute_with_progress_impl(
            &InstanceName(None),
            req,
            execute_f,
            wait_execution_f,
            default_reattach_budget(),
            default_reattach_limiter(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        let responses: Vec<_> = stream.try_collect().await?;

        assert_eq!(responses.len(), 3);
        assert_eq!(responses[0].stage, Stage::QUEUED);
        assert_eq!(responses[1].stage, Stage::EXECUTING);
        assert_eq!(responses[2].stage, Stage::COMPLETED);
        let terminal = responses[2]
            .execute_response
            .as_ref()
            .context("terminal response missing execute_response")?;
        assert_eq!(terminal.action_digest, action_digest);
        assert_eq!(execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(wait_execution_calls.load(Ordering::SeqCst), 1);
        assert_eq!(responses[2].reattach_stats.wait_execution_reattaches, 1);
        assert_eq!(responses[2].reattach_stats.severed_io, 1);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_reattach_uses_latest_operation_name() -> anyhow::Result<()> {
        let req = test_execute_request(false);

        let wait_execution_names = Arc::new(Mutex::new(Vec::new()));

        let execute_f = move |_req: GExecuteRequest| async move {
            anyhow::Ok(
                futures::stream::iter(vec![
                    Ok(progress_operation("op-1", execution_stage::Value::Queued)),
                    Err(severed_status(std::io::ErrorKind::ConnectionReset)),
                ])
                .boxed(),
            )
        };

        let wait_execution_f = {
            let wait_execution_names = wait_execution_names.dupe();
            move |name: String| {
                let wait_execution_names = wait_execution_names.dupe();
                async move {
                    let call = {
                        let mut names = wait_execution_names.lock().unwrap();
                        names.push(name.clone());
                        names.len()
                    };
                    if call == 1 {
                        anyhow::Ok(
                            futures::stream::iter(vec![
                                Ok(progress_operation(
                                    "op-2",
                                    execution_stage::Value::Executing,
                                )),
                                Err(severed_status(std::io::ErrorKind::ConnectionReset)),
                            ])
                            .boxed(),
                        )
                    } else {
                        anyhow::Ok(futures::stream::iter(vec![Ok(done_operation("op-2"))]).boxed())
                    }
                }
            }
        };

        let stream = execute_with_progress_impl(
            &InstanceName(None),
            req,
            execute_f,
            wait_execution_f,
            default_reattach_budget(),
            default_reattach_limiter(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        let responses: Vec<_> = stream.try_collect().await?;
        assert_eq!(responses.len(), 3);

        let names = wait_execution_names.lock().unwrap().clone();
        assert_eq!(names, vec!["op-1".to_owned(), "op-2".to_owned()]);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_reattach_on_clean_eof() -> anyhow::Result<()> {
        let req = test_execute_request(false);

        let wait_execution_calls = Arc::new(AtomicUsize::new(0));

        let execute_f = move |_req: GExecuteRequest| async move {
            anyhow::Ok(
                futures::stream::iter(vec![Ok(progress_operation(
                    "op-1",
                    execution_stage::Value::Queued,
                ))])
                .boxed(),
            )
        };

        let wait_execution_f = {
            let wait_execution_calls = wait_execution_calls.dupe();
            move |name: String| {
                let wait_execution_calls = wait_execution_calls.dupe();
                async move {
                    assert_eq!(name, "op-1");
                    wait_execution_calls.fetch_add(1, Ordering::SeqCst);
                    anyhow::Ok(futures::stream::iter(vec![Ok(done_operation("op-1"))]).boxed())
                }
            }
        };

        let stream = execute_with_progress_impl(
            &InstanceName(None),
            req,
            execute_f,
            wait_execution_f,
            default_reattach_budget(),
            default_reattach_limiter(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        let responses: Vec<_> = stream.try_collect().await?;
        assert_eq!(responses.len(), 2);
        assert_eq!(responses[1].stage, Stage::COMPLETED);
        assert_eq!(wait_execution_calls.load(Ordering::SeqCst), 1);
        assert_eq!(responses[1].reattach_stats.clean_eof, 1);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_reattach_not_found_reissues_execute() -> anyhow::Result<()> {
        for skip_cache_lookup in [true, false] {
            let req = test_execute_request(skip_cache_lookup);

            let execute_requests = Arc::new(Mutex::new(Vec::new()));
            let execute_call_count = Arc::new(AtomicUsize::new(0));
            let wait_execution_calls = Arc::new(AtomicUsize::new(0));

            let execute_f = {
                let execute_requests = execute_requests.dupe();
                let execute_call_count = execute_call_count.dupe();
                move |req: GExecuteRequest| {
                    let execute_requests = execute_requests.dupe();
                    let execute_call_count = execute_call_count.dupe();
                    async move {
                        execute_requests.lock().unwrap().push(req);
                        let call = execute_call_count.fetch_add(1, Ordering::SeqCst);
                        if call == 0 {
                            anyhow::Ok(
                                futures::stream::iter(vec![
                                    Ok(progress_operation("op-1", execution_stage::Value::Queued)),
                                    Err(severed_status(std::io::ErrorKind::ConnectionReset)),
                                ])
                                .boxed(),
                            )
                        } else {
                            anyhow::Ok(
                                futures::stream::iter(vec![Ok(done_operation("op-1"))]).boxed(),
                            )
                        }
                    }
                }
            };

            let wait_execution_f = {
                let wait_execution_calls = wait_execution_calls.dupe();
                move |name: String| {
                    let wait_execution_calls = wait_execution_calls.dupe();
                    async move {
                        assert_eq!(name, "op-1");
                        wait_execution_calls.fetch_add(1, Ordering::SeqCst);
                        Err::<BoxStream<'static, Result<Operation, tonic::Status>>, _>(
                            anyhow::Error::new(tonic::Status::not_found("operation gone")),
                        )
                    }
                }
            };

            let stream = execute_with_progress_impl(
                &InstanceName(None),
                req,
                execute_f,
                wait_execution_f,
                default_reattach_budget(),
                default_reattach_limiter(),
                fresh_wait_execution_unimplemented_latch(),
            )
            .await?;

            let responses: Vec<_> = stream.try_collect().await?;
            let terminal = responses.last().context("expected a terminal response")?;
            assert_eq!(terminal.stage, Stage::COMPLETED);
            assert_eq!(terminal.reattach_stats.operation_not_found, 1);
            assert_eq!(terminal.reattach_stats.re_executes, 1);

            let requests = execute_requests.lock().unwrap().clone();
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[0], requests[1]);
            assert_eq!(wait_execution_calls.load(Ordering::SeqCst), 1);
        }

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_reattach_call_fails_non_retryably() -> anyhow::Result<()> {
        let req = test_execute_request(false);

        let wait_execution_calls = Arc::new(AtomicUsize::new(0));

        let execute_f = move |_req: GExecuteRequest| async move {
            anyhow::Ok(
                futures::stream::iter(vec![
                    Ok(progress_operation("op-1", execution_stage::Value::Queued)),
                    Err(severed_status(std::io::ErrorKind::ConnectionReset)),
                ])
                .boxed(),
            )
        };

        let wait_execution_f = {
            let wait_execution_calls = wait_execution_calls.dupe();
            move |name: String| {
                let wait_execution_calls = wait_execution_calls.dupe();
                async move {
                    assert_eq!(name, "op-1");
                    wait_execution_calls.fetch_add(1, Ordering::SeqCst);
                    Err::<BoxStream<'static, Result<Operation, tonic::Status>>, _>(
                        anyhow::Error::new(tonic::Status::permission_denied("credentials expired")),
                    )
                }
            }
        };

        let stream = execute_with_progress_impl(
            &InstanceName(None),
            req,
            execute_f,
            wait_execution_f,
            default_reattach_budget(),
            default_reattach_limiter(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        let err = stream
            .try_collect::<Vec<_>>()
            .await
            .err()
            .expect("a non-retryable reattach-call failure must propagate immediately");

        let chain = format!("{err:#}");
        assert!(
            chain.contains("Reattaching RE execute stream via WaitExecution"),
            "chain: {chain}"
        );
        assert!(chain.contains("credentials expired"), "chain: {chain}");
        assert!(
            chain.contains("action `aa`") && chain.contains("operation `op-1`"),
            "chain does not identify the action and operation being reattached: {chain}"
        );
        assert!(
            !chain.contains("Exceeding RE execute reattach budget"),
            "a non-retryable reattach failure must not be framed as a budget timeout: {chain}"
        );

        // The original severance is the root cause: it must appear in the chain, after (not
        // instead of) the failed WaitExecution attempt that replaced it as the surfaced error.
        let reattach_position = chain
            .find("Reattaching RE execute stream via WaitExecution")
            .context("chain missing the reattach context")?;
        let severance_position = chain
            .find("connection reset")
            .context("chain does not name the original severance as its root cause")?;
        assert!(
            reattach_position < severance_position,
            "the failed WaitExecution attempt must be outer context, the original severance \
             the root cause: {chain}"
        );

        assert_eq!(
            wait_execution_calls.load(Ordering::SeqCst),
            1,
            "the reattach call must not be retried once classified non-retryable"
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_reattach_wait_execution_unimplemented_latches_reattach_off() -> anyhow::Result<()>
    {
        let latch = fresh_wait_execution_unimplemented_latch();
        let wait_execution_calls = Arc::new(AtomicUsize::new(0));

        let execute_f = move |_req: GExecuteRequest| async move {
            anyhow::Ok(
                futures::stream::iter(vec![
                    Ok(progress_operation("op-1", execution_stage::Value::Queued)),
                    Err(severed_status(std::io::ErrorKind::ConnectionReset)),
                ])
                .boxed(),
            )
        };

        let wait_execution_f = {
            let wait_execution_calls = wait_execution_calls.dupe();
            move |name: String| {
                let wait_execution_calls = wait_execution_calls.dupe();
                async move {
                    assert_eq!(name, "op-1");
                    wait_execution_calls.fetch_add(1, Ordering::SeqCst);
                    Err::<BoxStream<'static, Result<Operation, tonic::Status>>, _>(
                        anyhow::Error::new(tonic::Status::unimplemented(
                            "WaitExecution is not supported",
                        )),
                    )
                }
            }
        };

        let stream = execute_with_progress_impl(
            &InstanceName(None),
            test_execute_request(false),
            execute_f,
            wait_execution_f,
            default_reattach_budget(),
            default_reattach_limiter(),
            latch.dupe(),
        )
        .await?;

        let err = stream
            .try_collect::<Vec<_>>()
            .await
            .err()
            .expect("an UNIMPLEMENTED WaitExecution reply must still surface an error");

        let chain = format!("{err:#}");
        assert!(
            chain.contains("Reattaching RE execute stream via WaitExecution"),
            "chain: {chain}"
        );
        assert!(
            chain.contains("connection reset"),
            "chain must name the original severance as its root cause: {chain}"
        );
        assert_eq!(
            wait_execution_calls.load(Ordering::SeqCst),
            1,
            "an UNIMPLEMENTED reply must not be retried"
        );
        assert!(
            latch.load(Ordering::SeqCst),
            "discovering UNIMPLEMENTED must latch reattach off for the client"
        );

        // A second action sharing the same client hits a fresh severance. The latch must stop
        // it from ever dialing WaitExecution again.
        let second_execute_f = move |_req: GExecuteRequest| async move {
            anyhow::Ok(
                futures::stream::iter(vec![
                    Ok(progress_operation("op-2", execution_stage::Value::Queued)),
                    Err(severed_status(std::io::ErrorKind::ConnectionReset)),
                ])
                .boxed(),
            )
        };

        let stream =
            execute_with_progress_impl(
                &InstanceName(None),
                test_execute_request(false),
                second_execute_f,
                |_name: String| async move {
                    panic!("a latched client must never attempt WaitExecution")
                },
                default_reattach_budget(),
                default_reattach_limiter(),
                latch.dupe(),
            )
            .await?;

        let err = stream
            .try_collect::<Vec<_>>()
            .await
            .err()
            .expect("a severance on a latched client must still propagate an error");
        assert!(
            format!("{err:#}").contains("connection reset"),
            "a latched client propagates the severance trigger unmodified"
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_reattach_dial_failure_then_recovers() -> anyhow::Result<()> {
        let req = test_execute_request(false);

        let wait_execution_calls = Arc::new(AtomicUsize::new(0));

        let execute_f = move |_req: GExecuteRequest| async move {
            anyhow::Ok(
                futures::stream::iter(vec![
                    Ok(progress_operation("op-1", execution_stage::Value::Queued)),
                    Err(severed_status(std::io::ErrorKind::ConnectionReset)),
                ])
                .boxed(),
            )
        };

        let wait_execution_f = {
            let wait_execution_calls = wait_execution_calls.dupe();
            move |name: String| {
                let wait_execution_calls = wait_execution_calls.dupe();
                async move {
                    assert_eq!(name, "op-1");
                    let call = wait_execution_calls.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        Err::<BoxStream<'static, Result<Operation, tonic::Status>>, _>(
                            anyhow::Error::new(severed_status(
                                std::io::ErrorKind::ConnectionRefused,
                            )),
                        )
                    } else {
                        anyhow::Ok(futures::stream::iter(vec![Ok(done_operation("op-1"))]).boxed())
                    }
                }
            }
        };

        let stream = execute_with_progress_impl(
            &InstanceName(None),
            req,
            execute_f,
            wait_execution_f,
            default_reattach_budget(),
            default_reattach_limiter(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        let responses: Vec<_> = stream.try_collect().await?;
        let terminal = responses.last().context("expected a terminal response")?;
        assert_eq!(terminal.stage, Stage::COMPLETED);
        assert_eq!(terminal.reattach_stats.dial_failures, 1);
        assert_eq!(terminal.reattach_stats.wait_execution_reattaches, 1);
        assert_eq!(
            wait_execution_calls.load(Ordering::SeqCst),
            2,
            "a dial failure must retry the same reattach call rather than propagate"
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_reattach_before_any_message_uses_execute() -> anyhow::Result<()> {
        let req = test_execute_request(false);

        let execute_calls = Arc::new(AtomicUsize::new(0));

        let execute_f = {
            let execute_calls = execute_calls.dupe();
            move |_req: GExecuteRequest| {
                let execute_calls = execute_calls.dupe();
                async move {
                    let call = execute_calls.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        anyhow::Ok(
                            futures::stream::iter(vec![Err(severed_status(
                                std::io::ErrorKind::ConnectionReset,
                            ))])
                            .boxed(),
                        )
                    } else {
                        anyhow::Ok(futures::stream::iter(vec![Ok(done_operation("op-1"))]).boxed())
                    }
                }
            }
        };

        let stream = execute_with_progress_impl(
            &InstanceName(None),
            req,
            execute_f,
            |_name: String| async move {
                panic!("WaitExecution should not be triggered before any message is seen")
            },
            default_reattach_budget(),
            default_reattach_limiter(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        let responses: Vec<_> = stream.try_collect().await?;
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].stage, Stage::COMPLETED);
        assert_eq!(execute_calls.load(Ordering::SeqCst), 2);
        assert_eq!(responses[0].reattach_stats.re_executes, 1);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_reattach_budget_exceeded_fails() -> anyhow::Result<()> {
        let req = test_execute_request(false);

        let wait_execution_calls = Arc::new(AtomicUsize::new(0));

        let execute_f = move |_req: GExecuteRequest| async move {
            anyhow::Ok(
                futures::stream::iter(vec![
                    Ok(progress_operation("op-1", execution_stage::Value::Queued)),
                    Err(severed_status(std::io::ErrorKind::ConnectionReset)),
                ])
                .boxed(),
            )
        };

        let wait_execution_f = {
            let wait_execution_calls = wait_execution_calls.dupe();
            move |name: String| {
                let wait_execution_calls = wait_execution_calls.dupe();
                async move {
                    assert_eq!(name, "op-1");
                    wait_execution_calls.fetch_add(1, Ordering::SeqCst);
                    anyhow::Ok(
                        futures::stream::iter(vec![Err(severed_status(
                            std::io::ErrorKind::ConnectionReset,
                        ))])
                        .boxed(),
                    )
                }
            }
        };

        let stream = execute_with_progress_impl(
            &InstanceName(None),
            req,
            execute_f,
            wait_execution_f,
            Some(Duration::from_secs(5)),
            default_reattach_limiter(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        let err = stream
            .try_collect::<Vec<_>>()
            .await
            .err()
            .expect("a permanently severed endpoint must eventually fail");

        let chain = format!("{err:#}");
        assert!(
            chain.contains("Exceeding RE execute reattach budget"),
            "chain: {chain}"
        );
        assert!(
            chain.to_lowercase().contains("reset"),
            "chain does not surface the underlying cause: {chain}"
        );

        let calls = wait_execution_calls.load(Ordering::SeqCst);
        assert!(calls > 0, "expected at least one reattach attempt");
        assert!(
            calls < 100,
            "reattach attempts should be budget-bounded, got {calls}"
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_reattach_survives_severance_after_budget_length_silence() -> anyhow::Result<()> {
        let budget = Duration::from_secs(5);
        let req = test_execute_request(false);

        let wait_execution_calls = Arc::new(AtomicUsize::new(0));

        // The QUEUED message lands immediately, resetting the stream's last-progress clock to
        // t=0. The severance then arrives only after a full budget's worth of silence — the
        // shape of a long QUEUED action whose disconnect is detected only once TCP keepalive
        // notices, well after the connection actually died.
        let execute_f = move |_req: GExecuteRequest| async move {
            anyhow::Ok(
                futures::stream::iter(vec![Ok(progress_operation(
                    "op-1",
                    execution_stage::Value::Queued,
                ))])
                .chain(futures::stream::once(async move {
                    tokio::time::sleep(budget).await;
                    Err(severed_status(std::io::ErrorKind::ConnectionReset))
                }))
                .boxed(),
            )
        };

        let wait_execution_f = {
            let wait_execution_calls = wait_execution_calls.dupe();
            move |name: String| {
                let wait_execution_calls = wait_execution_calls.dupe();
                async move {
                    assert_eq!(name, "op-1");
                    wait_execution_calls.fetch_add(1, Ordering::SeqCst);
                    anyhow::Ok(futures::stream::iter(vec![Ok(done_operation("op-1"))]).boxed())
                }
            }
        };

        let stream = execute_with_progress_impl(
            &InstanceName(None),
            req,
            execute_f,
            wait_execution_f,
            Some(budget),
            default_reattach_limiter(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        let responses: Vec<_> = stream.try_collect().await?;
        assert_eq!(
            responses
                .last()
                .context("expected a terminal response")?
                .stage,
            Stage::COMPLETED
        );
        assert_eq!(
            wait_execution_calls.load(Ordering::SeqCst),
            1,
            "a severance after a budget-length silence must still get a reattach attempt"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_reattach_disabled_propagates_trigger_unmodified() -> anyhow::Result<()> {
        let req = test_execute_request(false);

        let execute_calls = Arc::new(AtomicUsize::new(0));

        let execute_f = {
            let execute_calls = execute_calls.dupe();
            move |_req: GExecuteRequest| {
                let execute_calls = execute_calls.dupe();
                async move {
                    execute_calls.fetch_add(1, Ordering::SeqCst);
                    anyhow::Ok(
                        futures::stream::iter(vec![
                            Ok(progress_operation("op-1", execution_stage::Value::Queued)),
                            Err(severed_status(std::io::ErrorKind::ConnectionReset)),
                        ])
                        .boxed(),
                    )
                }
            }
        };

        let stream = execute_with_progress_impl(
            &InstanceName(None),
            req,
            execute_f,
            |_name: String| async move { panic!("WaitExecution should not be triggered") },
            None,
            default_reattach_limiter(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        let err = stream
            .try_collect::<Vec<_>>()
            .await
            .err()
            .expect("a severed stream must still propagate an error when reattach is disabled");

        let chain = format!("{err:#}");
        assert!(
            chain.contains("RE channel error"),
            "a disabled client must propagate the same trigger a client built without \
             reattach would, not a budget-timeout frame: {chain}"
        );
        assert!(
            !chain.contains("Exceeding RE execute reattach budget"),
            "chain: {chain}"
        );
        assert_eq!(
            execute_calls.load(Ordering::SeqCst),
            1,
            "reattach must never be attempted while disabled"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_reattach_disabled_ends_stream_cleanly_on_clean_eof() -> anyhow::Result<()> {
        let req = test_execute_request(false);

        let execute_f = move |_req: GExecuteRequest| async move {
            anyhow::Ok(
                futures::stream::iter(vec![Ok(progress_operation(
                    "op-1",
                    execution_stage::Value::Queued,
                ))])
                .boxed(),
            )
        };

        let stream = execute_with_progress_impl(
            &InstanceName(None),
            req,
            execute_f,
            |_name: String| async move { panic!("WaitExecution should not be triggered") },
            None,
            default_reattach_limiter(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        let responses: Vec<_> = stream.try_collect().await?;
        assert_eq!(
            responses.len(),
            1,
            "a disabled client must end the stream on clean EOF exactly as a client built \
             without reattach would, leaving end-of-stream handling to the consumer"
        );
        assert_eq!(responses[0].stage, Stage::QUEUED);

        Ok(())
    }

    #[test]
    fn test_execute_reattach_concurrency_zero_is_clamped() {
        let opts = Buck2OssReConfiguration {
            execute_reattach_concurrency: Some(0),
            ..Default::default()
        };
        assert_eq!(
            execute_reattach_concurrency(&opts),
            EXECUTE_REATTACH_CONCURRENCY_MIN,
            "a configured concurrency of 0 would build a limiter that blocks every reattach \
             forever"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_reattach_limiter_bounds_concurrent_dials() -> anyhow::Result<()> {
        let limiter = Arc::new(Semaphore::new(1));
        let concurrent = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));

        // Both streams sever immediately and race for the same single permit. Each dial holds
        // the permit for a fixed delay, so two dials overlapping in time would both increment
        // `concurrent` while the other is still inside its own delay, which a size-1 limiter
        // prevents.
        let severed_execute_f = |name: &'static str| {
            move |_req: GExecuteRequest| async move {
                anyhow::Ok(
                    futures::stream::iter(vec![
                        Ok(progress_operation(name, execution_stage::Value::Queued)),
                        Err(severed_status(std::io::ErrorKind::ConnectionReset)),
                    ])
                    .boxed(),
                )
            }
        };

        let stream1 = execute_with_progress_impl(
            &InstanceName(None),
            test_execute_request(false),
            severed_execute_f("op-1"),
            {
                let concurrent = concurrent.dupe();
                let max_concurrent = max_concurrent.dupe();
                move |name: String| {
                    let concurrent = concurrent.dupe();
                    let max_concurrent = max_concurrent.dupe();
                    async move {
                        let now = concurrent.fetch_add(1, Ordering::SeqCst) + 1;
                        max_concurrent.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        concurrent.fetch_sub(1, Ordering::SeqCst);
                        anyhow::Ok(futures::stream::iter(vec![Ok(done_operation(&name))]).boxed())
                    }
                }
            },
            default_reattach_budget(),
            limiter.dupe(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        let stream2 = execute_with_progress_impl(
            &InstanceName(None),
            test_execute_request(false),
            severed_execute_f("op-2"),
            {
                let concurrent = concurrent.dupe();
                let max_concurrent = max_concurrent.dupe();
                move |name: String| {
                    let concurrent = concurrent.dupe();
                    let max_concurrent = max_concurrent.dupe();
                    async move {
                        let now = concurrent.fetch_add(1, Ordering::SeqCst) + 1;
                        max_concurrent.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        concurrent.fetch_sub(1, Ordering::SeqCst);
                        anyhow::Ok(futures::stream::iter(vec![Ok(done_operation(&name))]).boxed())
                    }
                }
            },
            default_reattach_budget(),
            limiter.dupe(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        let (r1, r2) = tokio::join!(
            stream1.try_collect::<Vec<_>>(),
            stream2.try_collect::<Vec<_>>(),
        );
        r1?;
        r2?;

        assert_eq!(
            max_concurrent.load(Ordering::SeqCst),
            1,
            "a size-1 limiter must serialize concurrent reattach dials across both streams"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_response_error_propagates_without_retry() -> anyhow::Result<()> {
        let req = test_execute_request(false);

        let execute_calls = Arc::new(AtomicUsize::new(0));

        let execute_f = {
            let execute_calls = execute_calls.dupe();
            move |_req: GExecuteRequest| {
                let execute_calls = execute_calls.dupe();
                async move {
                    execute_calls.fetch_add(1, Ordering::SeqCst);
                    anyhow::Ok(
                        futures::stream::iter(vec![Ok(done_error_operation(
                            "op-1",
                            TCode::INTERNAL.0,
                            "boom",
                        ))])
                        .boxed(),
                    )
                }
            }
        };

        let stream = execute_with_progress_impl(
            &InstanceName(None),
            req,
            execute_f,
            |_name: String| async move { panic!("WaitExecution should not be triggered") },
            default_reattach_budget(),
            default_reattach_limiter(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        let err = stream
            .try_collect::<Vec<_>>()
            .await
            .err()
            .expect("an application-level operation error must propagate");
        assert!(format!("{err:#}").contains("boom"));
        assert_eq!(execute_calls.load(Ordering::SeqCst), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_response_bad_status_propagates_without_retry() -> anyhow::Result<()> {
        let req = test_execute_request(false);

        let execute_calls = Arc::new(AtomicUsize::new(0));

        let execute_f = {
            let execute_calls = execute_calls.dupe();
            move |_req: GExecuteRequest| {
                let execute_calls = execute_calls.dupe();
                async move {
                    execute_calls.fetch_add(1, Ordering::SeqCst);
                    anyhow::Ok(
                        futures::stream::iter(vec![Ok(done_bad_status_operation(
                            "op-1",
                            TCode::INVALID_ARGUMENT.0,
                            "bad request",
                        ))])
                        .boxed(),
                    )
                }
            }
        };

        let stream = execute_with_progress_impl(
            &InstanceName(None),
            req,
            execute_f,
            |_name: String| async move { panic!("WaitExecution should not be triggered") },
            default_reattach_budget(),
            default_reattach_limiter(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        let err = stream
            .try_collect::<Vec<_>>()
            .await
            .err()
            .expect("a non-OK ExecuteResponse status must propagate");
        assert!(format!("{err:#}").contains("bad request"));
        assert_eq!(execute_calls.load(Ordering::SeqCst), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_plain_status_propagates() -> anyhow::Result<()> {
        let req = test_execute_request(false);

        let execute_calls = Arc::new(AtomicUsize::new(0));

        let execute_f = {
            let execute_calls = execute_calls.dupe();
            move |_req: GExecuteRequest| {
                let execute_calls = execute_calls.dupe();
                async move {
                    execute_calls.fetch_add(1, Ordering::SeqCst);
                    anyhow::Ok(
                        futures::stream::iter(vec![Err(tonic::Status::unavailable(
                            "backend is down",
                        ))])
                        .boxed(),
                    )
                }
            }
        };

        let stream = execute_with_progress_impl(
            &InstanceName(None),
            req,
            execute_f,
            |_name: String| async move { panic!("WaitExecution should not be triggered") },
            default_reattach_budget(),
            default_reattach_limiter(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        let err = stream
            .try_collect::<Vec<_>>()
            .await
            .err()
            .expect("a server-spoken status with no h2/io source must propagate");
        assert!(format!("{err:#}").contains("backend is down"));
        assert_eq!(execute_calls.load(Ordering::SeqCst), 1);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_reattach_goaway_no_error_retries() -> anyhow::Result<()> {
        let req = test_execute_request(false);

        let wait_execution_calls = Arc::new(AtomicUsize::new(0));

        let execute_f = move |_req: GExecuteRequest| async move {
            anyhow::Ok(
                futures::stream::iter(vec![
                    Ok(progress_operation("op-1", execution_stage::Value::Queued)),
                    Err(goaway_status(h2::Reason::NO_ERROR)),
                ])
                .boxed(),
            )
        };

        let wait_execution_f = {
            let wait_execution_calls = wait_execution_calls.dupe();
            move |name: String| {
                let wait_execution_calls = wait_execution_calls.dupe();
                async move {
                    assert_eq!(name, "op-1");
                    wait_execution_calls.fetch_add(1, Ordering::SeqCst);
                    anyhow::Ok(futures::stream::iter(vec![Ok(done_operation("op-1"))]).boxed())
                }
            }
        };

        let stream = execute_with_progress_impl(
            &InstanceName(None),
            req,
            execute_f,
            wait_execution_f,
            default_reattach_budget(),
            default_reattach_limiter(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        let responses: Vec<_> = stream.try_collect().await?;
        let terminal = responses.last().context("expected a terminal response")?;
        assert_eq!(terminal.stage, Stage::COMPLETED);
        assert_eq!(terminal.reattach_stats.severed_goaway, 1);
        assert_eq!(wait_execution_calls.load(Ordering::SeqCst), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_enhance_your_calm_propagates() -> anyhow::Result<()> {
        let req = test_execute_request(false);

        let execute_calls = Arc::new(AtomicUsize::new(0));

        let execute_f = {
            let execute_calls = execute_calls.dupe();
            move |_req: GExecuteRequest| {
                let execute_calls = execute_calls.dupe();
                async move {
                    execute_calls.fetch_add(1, Ordering::SeqCst);
                    anyhow::Ok(
                        futures::stream::iter(vec![Err(goaway_status(
                            h2::Reason::ENHANCE_YOUR_CALM,
                        ))])
                        .boxed(),
                    )
                }
            }
        };

        let stream = execute_with_progress_impl(
            &InstanceName(None),
            req,
            execute_f,
            |_name: String| async move { panic!("WaitExecution should not be triggered") },
            default_reattach_budget(),
            default_reattach_limiter(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        stream
            .try_collect::<Vec<_>>()
            .await
            .err()
            .expect("ENHANCE_YOUR_CALM must not be treated as retryable");
        assert_eq!(execute_calls.load(Ordering::SeqCst), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_post_terminal_poll_returns_none() -> anyhow::Result<()> {
        let req = test_execute_request(false);

        let execute_calls = Arc::new(AtomicUsize::new(0));

        let execute_f = {
            let execute_calls = execute_calls.dupe();
            move |_req: GExecuteRequest| {
                let execute_calls = execute_calls.dupe();
                async move {
                    execute_calls.fetch_add(1, Ordering::SeqCst);
                    anyhow::Ok(futures::stream::iter(vec![Ok(done_operation("op-1"))]).boxed())
                }
            }
        };

        let mut stream = execute_with_progress_impl(
            &InstanceName(None),
            req,
            execute_f,
            |_name: String| async move { panic!("WaitExecution should not be triggered") },
            default_reattach_budget(),
            default_reattach_limiter(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        let first = stream
            .next()
            .await
            .context("expected the terminal response")??;
        assert_eq!(first.stage, Stage::COMPLETED);

        let second = stream.next().await;
        assert!(
            second.is_none(),
            "a post-terminal poll must not yield another item"
        );
        assert_eq!(execute_calls.load(Ordering::SeqCst), 1);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_reattach_cancelled_during_backoff_sleep() -> anyhow::Result<()> {
        let req = test_execute_request(false);

        let execute_calls = Arc::new(AtomicUsize::new(0));
        let wait_execution_calls = Arc::new(AtomicUsize::new(0));

        let execute_f = {
            let execute_calls = execute_calls.dupe();
            move |_req: GExecuteRequest| {
                let execute_calls = execute_calls.dupe();
                async move {
                    execute_calls.fetch_add(1, Ordering::SeqCst);
                    anyhow::Ok(
                        futures::stream::iter(vec![
                            Ok(progress_operation("op-1", execution_stage::Value::Queued)),
                            Err(severed_status(std::io::ErrorKind::ConnectionReset)),
                        ])
                        .boxed(),
                    )
                }
            }
        };

        let wait_execution_f = {
            let wait_execution_calls = wait_execution_calls.dupe();
            move |_name: String| {
                let wait_execution_calls = wait_execution_calls.dupe();
                async move {
                    wait_execution_calls.fetch_add(1, Ordering::SeqCst);
                    anyhow::Ok(futures::stream::iter(vec![Ok(done_operation("op-1"))]).boxed())
                }
            }
        };

        let mut stream = execute_with_progress_impl(
            &InstanceName(None),
            req,
            execute_f,
            wait_execution_f,
            default_reattach_budget(),
            default_reattach_limiter(),
            fresh_wait_execution_unimplemented_latch(),
        )
        .await?;

        // The first item is the QUEUED progress message, resolved synchronously from the
        // scripted stream. Recovery only begins on the next poll, once the severance is read.
        let first = stream
            .next()
            .await
            .context("expected the initial progress message")??;
        assert_eq!(first.stage, Stage::QUEUED);

        // Drives the stream to the point where it suspends inside the backoff sleep, without
        // letting the paused clock auto-advance past it.
        let mut next = std::pin::pin!(stream.next());
        let poll = futures::poll!(&mut next);
        assert!(
            matches!(poll, std::task::Poll::Pending),
            "expected the stream to suspend inside the backoff sleep"
        );
        drop(stream);

        assert_eq!(execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            wait_execution_calls.load(Ordering::SeqCst),
            0,
            "dropping the stream during backoff must cancel the pending reattach"
        );

        Ok(())
    }

    #[test]
    fn test_execute_reattach_budget_resolution() {
        struct Case {
            name: &'static str,
            enabled: Option<bool>,
            budget_secs: Option<u64>,
            expected: Option<Duration>,
        }

        let cases = [
            Case {
                name: "defaults to disabled",
                enabled: None,
                budget_secs: None,
                expected: None,
            },
            Case {
                name: "a configured budget alone does not enable reattach",
                enabled: None,
                budget_secs: Some(30),
                expected: None,
            },
            Case {
                name: "explicit enable uses the default budget",
                enabled: Some(true),
                budget_secs: None,
                expected: Some(Duration::from_secs(60)),
            },
            Case {
                name: "explicit enable honors a configured budget",
                enabled: Some(true),
                budget_secs: Some(30),
                expected: Some(Duration::from_secs(30)),
            },
            Case {
                name: "explicit enable with a zero budget still disables",
                enabled: Some(true),
                budget_secs: Some(0),
                expected: None,
            },
            Case {
                name: "explicit disable wins over a configured budget",
                enabled: Some(false),
                budget_secs: Some(30),
                expected: None,
            },
        ];

        for case in cases {
            let opts = Buck2OssReConfiguration {
                execute_reattach_enabled: case.enabled,
                execute_reattach_budget_secs: case.budget_secs,
                ..Default::default()
            };
            assert_eq!(
                execute_reattach_budget(&opts),
                case.expected,
                "case: {}",
                case.name
            );
        }
    }

    #[tokio::test]
    async fn test_upload_named() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;

        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path1, "aaa").await?;

        let path2 = work.path().join("path2");
        let path2 = path2.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path2, "bbb").await?;

        let digest1 = TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let digest2 = TDigest {
            hash: "bb".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let req = UploadRequest {
            files_with_digest: Some(vec![
                NamedDigest {
                    name: path1.to_owned(),
                    digest: digest1.clone(),
                    ..Default::default()
                },
                NamedDigest {
                    name: path2.to_owned(),
                    digest: digest2.clone(),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        let res = BatchUpdateBlobsResponse {
            responses: vec![
                // Reply out of order
                batch_update_blobs_response::Response {
                    digest: Some(tdigest_to(digest2.clone())),
                    status: Some(Status::default()),
                },
                batch_update_blobs_response::Response {
                    digest: Some(tdigest_to(digest1.clone())),
                    status: Some(Status::default()),
                },
            ],
        };

        upload_impl(
            &InstanceName(None),
            req,
            None,
            10000,
            None,
            |req| {
                let res = res.clone();
                let digest1 = digest1.clone();
                let digest2 = digest2.clone();
                async move {
                    assert_eq!(req.requests.len(), 2);
                    assert_eq!(req.requests[0].digest, Some(tdigest_to(digest1)));
                    assert_eq!(req.requests[0].data, b"aaa");
                    assert_eq!(req.requests[1].digest, Some(tdigest_to(digest2)));
                    assert_eq!(req.requests[1].data, b"bbb");
                    Ok(res)
                }
            },
            |_req| async { panic!("A Bytestream upload should not be triggered") },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_upload_large_named() -> anyhow::Result<()> {
        let blob_data = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];

        let work = tempfile::tempdir()?;

        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path1, "aaa").await?;

        let path2 = work.path().join("path2");
        let path2 = path2.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path2, &blob_data).await?;

        let digest1 = TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let digest2 = TDigest {
            hash: "xl".to_owned(),
            size_in_bytes: 18,
            ..Default::default()
        };

        let req = UploadRequest {
            files_with_digest: Some(vec![
                NamedDigest {
                    name: path1.to_owned(),
                    digest: digest1.clone(),
                    ..Default::default()
                },
                NamedDigest {
                    name: path2.to_owned(),
                    digest: digest2.clone(),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        let res = BatchUpdateBlobsResponse {
            responses: vec![
                // Reply out of order
                batch_update_blobs_response::Response {
                    digest: Some(tdigest_to(digest2.clone())),
                    status: Some(Status::default()),
                },
                batch_update_blobs_response::Response {
                    digest: Some(tdigest_to(digest1.clone())),
                    status: Some(Status::default()),
                },
            ],
        };

        upload_impl(
            &InstanceName(None),
            req,
            None,
            10, // kept small to simulate a large file upload
            None,
            |req| {
                let res = res.clone();
                let digest1 = digest1.clone();
                async move {
                    assert_eq!(req.requests.len(), 1);
                    assert_eq!(req.requests[0].digest, Some(tdigest_to(digest1)));
                    assert_eq!(req.requests[0].data, b"aaa");
                    Ok(res)
                }
            },
            |write_reqs| {
                let blob_data = blob_data.clone();
                async move {
                    assert_eq!(write_reqs.len(), 2);
                    assert_eq!(write_reqs[0].write_offset, 0);
                    assert!(!write_reqs[0].finish_write);
                    assert_eq!(write_reqs[0].data, blob_data[..10]);
                    assert_eq!(write_reqs[1].write_offset, 10);
                    assert!(write_reqs[1].finish_write);
                    assert_eq!(write_reqs[1].data, blob_data[10..]);
                    anyhow::Ok(WriteResponse { committed_size: 18 })
                }
            },
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_upload_large_inlined() -> anyhow::Result<()> {
        let digest1 = TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };
        let blob_data1 = b"aaa".to_vec();

        let digest2 = TDigest {
            hash: "xl".to_owned(),
            size_in_bytes: 18,
            ..Default::default()
        };
        let blob_data2 = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];

        let req = UploadRequest {
            inlined_blobs_with_digest: Some(vec![
                InlinedBlobWithDigest {
                    blob: blob_data2.clone(),
                    digest: digest2.clone(),
                    ..Default::default()
                },
                InlinedBlobWithDigest {
                    blob: blob_data1.clone(),
                    digest: digest1.clone(),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        let res = BatchUpdateBlobsResponse {
            responses: vec![batch_update_blobs_response::Response {
                digest: Some(tdigest_to(digest2.clone())),
                status: Some(Status::default()),
            }],
        };

        upload_impl(
            &InstanceName(None),
            req,
            None,
            10, // kept small to simulate a large inlined upload
            None,
            |req| {
                let res = res.clone();
                let digest1 = digest1.clone();
                let blob_data1 = blob_data1.clone();
                async move {
                    assert_eq!(req.requests.len(), 1);
                    assert_eq!(req.requests[0].digest, Some(tdigest_to(digest1)));
                    assert_eq!(req.requests[0].data, blob_data1);
                    Ok(res)
                }
            },
            |write_reqs| {
                let blob_data2 = blob_data2.clone();
                async move {
                    assert_eq!(write_reqs.len(), 2);
                    assert_eq!(write_reqs[0].write_offset, 0);
                    assert!(!write_reqs[0].finish_write);
                    assert_eq!(write_reqs[0].data, blob_data2[..10]);
                    assert_eq!(write_reqs[1].write_offset, 10);
                    assert!(write_reqs[1].finish_write);
                    assert_eq!(write_reqs[1].data, blob_data2[10..]);
                    anyhow::Ok(WriteResponse { committed_size: 18 })
                }
            },
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_upload_invalid_committed_size() -> anyhow::Result<()> {
        let blob_data = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];

        let work = tempfile::tempdir()?;

        let path2 = work.path().join("path2");
        let path2 = path2.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path2, &blob_data).await?;

        let digest2 = TDigest {
            hash: "xl".to_owned(),
            size_in_bytes: 18,
            ..Default::default()
        };

        let req = UploadRequest {
            files_with_digest: Some(vec![NamedDigest {
                name: path2.to_owned(),
                digest: digest2.clone(),
                ..Default::default()
            }]),
            ..Default::default()
        };

        let resp: Result<UploadResponse, anyhow::Error> = upload_impl(
            &InstanceName(None), // TODO
            req,
            None,
            10,
            None,
            |_req| async move {
                panic!("This should not be called as there are no blobs to upload in batch");
            },
            |_write_reqs| async move {
                // Not the right size
                anyhow::Ok(WriteResponse { committed_size: 10 })
            },
        )
        .await;

        let err: anyhow::Error = resp.unwrap_err();
        // can't compare the full message because tempfile is used
        assert!(
            err.root_cause()
                .to_string()
                .contains("invalid committed_size")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_upload_exact() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;

        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path1, "aaabbb").await?;

        let digest1 = TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 6,
            ..Default::default()
        };

        let digest2 = TDigest {
            hash: "bb".to_owned(),
            size_in_bytes: 6,
            ..Default::default()
        };
        let blob_data2 = vec![1, 2, 3, 4, 5, 6];

        let req = UploadRequest {
            files_with_digest: Some(vec![NamedDigest {
                name: path1.to_owned(),
                digest: digest1.clone(),
                ..Default::default()
            }]),
            inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                blob: blob_data2.clone(),
                digest: digest2.clone(),
                ..Default::default()
            }]),
            ..Default::default()
        };

        upload_impl(
            &InstanceName(None),
            req,
            None,
            3,
            None,
            |_req| async move {
                panic!("Not called");
            },
            |write_reqs| async move {
                assert_eq!(write_reqs.len(), 2);
                assert!(write_reqs[1].finish_write);
                anyhow::Ok(WriteResponse { committed_size: 6 })
            },
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_upload_empty() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;

        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path1, "").await?;

        let digest1 = TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 0,
            ..Default::default()
        };

        for compressor in [
            None,
            Some(Compressor::Deflate),
            Some(Compressor::Brotli),
            Some(Compressor::Zstd),
        ] {
            assert!(
                upload_impl(
                    &InstanceName(None),
                    UploadRequest {
                        files_with_digest: Some(vec![NamedDigest {
                            name: path1.to_owned(),
                            digest: digest1.clone(),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    },
                    compressor,
                    0, // max_total_batch_size=0 forces bytestream API
                    None,
                    |_req| async move {
                        panic!("Not called");
                    },
                    |_write_reqs| async move {
                        panic!("Not called");
                    },
                )
                .await
                .is_ok()
            );

            assert!(
                upload_impl(
                    &InstanceName(None),
                    UploadRequest {
                        files_with_digest: Some(vec![NamedDigest {
                            name: path1.to_owned(),
                            digest: digest1.clone(),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    },
                    compressor,
                    1024, // forces the batch API
                    None,
                    |_req| async move {
                        panic!("Not called");
                    },
                    |_write_reqs| async move {
                        panic!("Not called");
                    },
                )
                .await
                .is_ok()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_upload_resource_name() -> anyhow::Result<()> {
        let digest1 = TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };
        let work = tempfile::tempdir()?;

        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path1, "aaa").await?;

        let req = UploadRequest {
            inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                digest: digest1.clone(),
                blob: b"aaa".to_vec(),
                ..Default::default()
            }]),
            files_with_digest: Some(vec![NamedDigest {
                name: path1.to_owned(),
                digest: digest1.clone(),
                ..Default::default()
            }]),
            ..Default::default()
        };

        upload_impl(
            &InstanceName(Some("instance".to_owned())),
            req,
            None,
            1,
            None,
            |_req| async move {
                panic!("Not called");
            },
            |write_reqs| async move {
                assert!(write_reqs[0].resource_name.starts_with("instance/uploads/"));
                assert!(write_reqs[0].resource_name.ends_with("/blobs/aa/3"));
                anyhow::Ok(WriteResponse { committed_size: 3 })
            },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_upload_resource_name_compressed() -> anyhow::Result<()> {
        let digest1 = TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };
        let work = tempfile::tempdir()?;

        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path1, "aaa").await?;

        let req = UploadRequest {
            inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                digest: digest1.clone(),
                blob: b"aaa".to_vec(),
                ..Default::default()
            }]),
            files_with_digest: Some(vec![NamedDigest {
                name: path1.to_owned(),
                digest: digest1.clone(),
                ..Default::default()
            }]),
            ..Default::default()
        };

        upload_impl(
            &InstanceName(Some("instance".to_owned())),
            req,
            Some(Compressor::Zstd),
            1,
            None,
            |_req| async move {
                panic!("Not called");
            },
            |write_reqs| async move {
                assert!(write_reqs[0].resource_name.starts_with("instance/uploads/"));
                assert!(
                    write_reqs[0]
                        .resource_name
                        .ends_with("/compressed-blobs/zstd/aa/3")
                );
                anyhow::Ok(WriteResponse { committed_size: -1 })
            },
        )
        .await?;

        Ok(())
    }

    #[test]
    fn test_substitute_env_vars() {
        let getter = |s: &str| match s {
            "FOO" => Ok("foo_value".to_owned()),
            "BAR" => Ok("bar_value".to_owned()),
            "BAZ" => Err(VarError::NotPresent),
            _ => panic!("Unexpected"),
        };

        assert_eq!(
            substitute_env_vars_impl("$FOO", getter).unwrap(),
            "foo_value"
        );
        assert_eq!(
            substitute_env_vars_impl("$FOO$BAR", getter).unwrap(),
            "foo_valuebar_value"
        );
        assert_eq!(
            substitute_env_vars_impl("some$FOO.bar", getter).unwrap(),
            "somefoo_value.bar"
        );
        assert_eq!(substitute_env_vars_impl("foo", getter).unwrap(), "foo");
        assert_eq!(substitute_env_vars_impl("FOO", getter).unwrap(), "FOO");
        assert!(substitute_env_vars_impl("$FOO$BAZ", getter).is_err());
    }
}

#[tokio::test]
async fn test_upload_compressed() -> anyhow::Result<()> {
    let blob_data = vec![1; 10 * 1024 * 1024];
    let digest1 = TDigest {
        hash: "aa".to_owned(),
        size_in_bytes: blob_data.len() as i64,
        ..Default::default()
    };

    let req = UploadRequest {
        inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
            digest: digest1.clone(),
            blob: blob_data.clone(),
            ..Default::default()
        }]),
        ..Default::default()
    };

    let blob_data_ref = &blob_data;
    upload_impl(
        &InstanceName(Some("instance".to_owned())),
        req,
        Some(Compressor::Zstd),
        1,
        None,
        |_req| async move {
            panic!("Not called");
        },
        {
            |write_reqs| async move {
                let compressed_data: Vec<u8> =
                    write_reqs.iter().flat_map(|wr| wr.data.clone()).collect();
                let mut data = vec![];
                ZstdDecoder::new(Cursor::new(compressed_data))
                    .read_to_end(&mut data)
                    .await
                    .unwrap();
                assert_eq!(&data, blob_data_ref);
                anyhow::Ok(WriteResponse { committed_size: -1 })
            }
        },
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_download_compressed() -> anyhow::Result<()> {
    let blob_data = vec![1; 1024];

    let mut compressed_data = vec![];
    ZstdEncoder::new(Cursor::new(blob_data.clone()))
        .read_to_end(&mut compressed_data)
        .await
        .unwrap();
    let compressed_data_ref = &compressed_data;

    let d_resp = download_impl(
        &InstanceName(None),
        DownloadRequest {
            inlined_digests: Some(vec![TDigest {
                hash: "aa".to_owned(),
                size_in_bytes: blob_data.len() as i64,
                ..Default::default()
            }]),
            file_digests: None,
            ..Default::default()
        },
        Some(Compressor::Zstd),
        10,
        |_req| async { panic!("not called") },
        |_req| async move {
            Ok(Box::pin(futures::stream::iter(
                compressed_data_ref
                    .chunks(10)
                    .map(|d| Result::Ok(ReadResponse { data: d.to_vec() })),
            )))
        },
    )
    .await?;

    assert_eq!(
        d_resp.inlined_blobs.as_ref().unwrap()[0].blob.len(),
        blob_data.len()
    );
    assert_eq!(d_resp.inlined_blobs.unwrap()[0].blob, blob_data);
    Ok(())
}
