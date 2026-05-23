//! Pipeline orchestrator for gRPC router request processing
//!
//! This module defines the RequestPipeline orchestrator that coordinates
//! the execution of pipeline stages from request preparation to response delivery.

use std::{sync::Arc, time::Instant};

use axum::response::{IntoResponse, Response};
use openai_protocol::{
    chat::{ChatCompletionRequest, ChatCompletionResponse},
    classify::ClassifyRequest,
    completion::CompletionRequest,
    embedding::EmbeddingRequest,
    generate::GenerateRequest,
    messages::CreateMessageRequest,
};
use reasoning_parser::ParserFactory as ReasoningParserFactory;
use tool_parser::ParserFactory as ToolParserFactory;
use tracing::{debug, error};

// Import embedding-specific, classify-specific, messages-specific, and completion-specific stages
use super::regular::stages::classify::ClassifyResponseProcessingStage;
use super::{
    common::{responses::ResponsesContext, stages::*},
    context::*,
    harmony,
    regular::{
        processor,
        stages::{
            completion::{
                CompletionPreparationStage, CompletionRequestBuildingStage,
                CompletionResponseProcessingStage,
            },
            embedding::{
                preparation::EmbeddingPreparationStage,
                request_building::EmbeddingRequestBuildingStage,
                response_processing::EmbeddingResponseProcessingStage,
            },
            messages::{
                MessagePreparationStage, MessageRequestBuildingStage,
                MessageResponseProcessingStage,
            },
            ChatGeneratePreparationStage, ChatGenerateRequestBuildingStage,
            ChatGenerateResponseProcessingStage,
        },
        streaming,
    },
    utils::error_type_from_status,
};
use crate::{
    middleware::TenantRequestMeta,
    observability::{
        metrics::{bool_to_static_str, metrics_labels, Metrics},
        usage_events::{
            publish_non_streaming_response, wrap_streaming_response, TokenInfo, UsageEventContext,
        },
    },
    policies::PolicyRegistry,
    routers::error,
    worker::{Worker, WorkerRegistry},
};

/// Generic request pipeline for all request types
///
/// Orchestrates all stages from request preparation to response delivery.
/// Configured differently for regular vs PD mode.
#[derive(Clone)]
pub(crate) struct RequestPipeline {
    stages: Arc<Vec<Box<dyn PipelineStage>>>,
    /// Backend type for metrics labeling
    backend_type: &'static str,
}

impl RequestPipeline {
    fn selected_backend(ctx: &RequestContext) -> Option<(Arc<dyn Worker>, String)> {
        let workers = ctx.state.workers.as_ref()?;
        match workers {
            WorkerSelection::Single { worker } => Some((Arc::clone(worker), worker.url().into())),
            WorkerSelection::Dual {
                prefill, decode, ..
            } => Some((
                Arc::clone(decode),
                format!("prefill={},decode={}", prefill.url(), decode.url()),
            )),
        }
    }

    fn usage_context(
        ctx: &RequestContext,
        route: &'static str,
        model_id: &str,
        streaming: bool,
        started_at: Instant,
    ) -> UsageEventContext {
        let usage_ctx = match Self::selected_backend(ctx) {
            Some((worker, selected_pool)) => {
                let served_model_name = worker
                    .metadata()
                    .spec
                    .served_model_name
                    .as_deref()
                    .unwrap_or("");
                let request_model = if served_model_name.is_empty() {
                    model_id
                } else {
                    served_model_name
                };
                UsageEventContext::from_backend(
                    ctx.input.headers.as_ref(),
                    &ctx.components.kafka_event_header_keys,
                    route,
                    model_id,
                    request_model,
                    worker.url(),
                    &selected_pool,
                    served_model_name,
                    streaming,
                    started_at,
                )
            }
            None => UsageEventContext::unrouted(
                ctx.input.headers.as_ref(),
                &ctx.components.kafka_event_header_keys,
                route,
                model_id,
                streaming,
                started_at,
            ),
        };
        usage_ctx.with_audit_capture(
            None,
            ctx.components.kafka_capture_response_body,
            ctx.components.kafka_body_capture_max_bytes,
        )
    }

    async fn publish_grpc_response(
        ctx: &RequestContext,
        response: Response,
        route: &'static str,
        model_id: &str,
        streaming: bool,
        started_at: Instant,
    ) -> Response {
        let usage_ctx = Self::usage_context(ctx, route, model_id, streaming, started_at);
        if streaming {
            wrap_streaming_response(
                response,
                usage_ctx,
                ctx.components.usage_event_publisher.clone(),
            )
        } else {
            publish_non_streaming_response(
                response,
                usage_ctx,
                ctx.components.usage_event_publisher.clone(),
            )
            .await
        }
    }

    fn publish_grpc_error(
        ctx: &RequestContext,
        response: &Response,
        route: &'static str,
        model_id: &str,
        streaming: bool,
        started_at: Instant,
    ) {
        let usage_ctx = Self::usage_context(ctx, route, model_id, streaming, started_at);
        let event = usage_ctx.build_event(
            response.status(),
            None,
            TokenInfo::default(),
            None,
            None,
            Some(error_type_from_status(response.status())),
        );
        ctx.components.usage_event_publisher.publish(event);
    }

    fn build_and_publish_grpc_error(
        ctx: &RequestContext,
        response: Response,
        route: &'static str,
        model_id: &str,
        streaming: bool,
        started_at: Instant,
    ) -> Response {
        Self::publish_grpc_error(ctx, &response, route, model_id, streaming, started_at);
        response
    }

    fn wrong_response_type(
        &self,
        function: &'static str,
        expected: &'static str,
        response_type: &FinalResponse,
        model: &str,
        endpoint: &'static str,
    ) -> Response {
        error!(
            function = function,
            response_type = %response_type,
            "Wrong response type: expected {expected}, got {response_type}"
        );
        Metrics::record_router_error(
            metrics_labels::ROUTER_GRPC,
            self.backend_type,
            metrics_labels::CONNECTION_GRPC,
            model,
            endpoint,
            metrics_labels::ERROR_INTERNAL,
        );
        error::internal_error("wrong_response_type", "Internal error: wrong response type")
    }

    fn no_response_produced(
        &self,
        function: &'static str,
        model: &str,
        endpoint: &'static str,
    ) -> Response {
        error!(function = function, "No response produced by pipeline");
        Metrics::record_router_error(
            metrics_labels::ROUTER_GRPC,
            self.backend_type,
            metrics_labels::CONNECTION_GRPC,
            model,
            endpoint,
            metrics_labels::ERROR_INTERNAL,
        );
        error::internal_error("no_response_produced", "No response produced")
    }

    /// Create a regular (single-worker) pipeline
    pub fn new_regular(
        worker_registry: Arc<WorkerRegistry>,
        policy_registry: Arc<PolicyRegistry>,
        tool_parser_factory: ToolParserFactory,
        reasoning_parser_factory: ReasoningParserFactory,
        configured_tool_parser: Option<String>,
        configured_reasoning_parser: Option<String>,
    ) -> Self {
        let processor = processor::ResponseProcessor::new(
            tool_parser_factory.clone(),
            reasoning_parser_factory.clone(),
            configured_tool_parser.clone(),
            configured_reasoning_parser.clone(),
        );

        let streaming_processor = Arc::new(streaming::StreamingProcessor::new(
            tool_parser_factory,
            reasoning_parser_factory,
            configured_tool_parser,
            configured_reasoning_parser,
            metrics_labels::BACKEND_REGULAR,
        ));

        let stages: Vec<Box<dyn PipelineStage>> = vec![
            Box::new(ChatGeneratePreparationStage::new()),
            Box::new(WorkerSelectionStage::new(
                worker_registry,
                policy_registry,
                WorkerSelectionMode::Regular,
            )),
            Box::new(ClientAcquisitionStage),
            Box::new(ChatGenerateRequestBuildingStage::new(false)), // No PD metadata
            Box::new(DispatchMetadataStage),
            Box::new(RequestExecutionStage::new(ExecutionMode::Single)),
            Box::new(ChatGenerateResponseProcessingStage::new(
                processor,
                streaming_processor,
            )),
        ];

        Self {
            stages: Arc::new(stages),
            backend_type: metrics_labels::BACKEND_REGULAR,
        }
    }

    /// Create a Harmony (single-worker) pipeline for Harmony-capable models
    pub fn new_harmony(
        worker_registry: Arc<WorkerRegistry>,
        policy_registry: Arc<PolicyRegistry>,
        _tool_parser_factory: ToolParserFactory,
        _reasoning_parser_factory: ReasoningParserFactory,
        _configured_tool_parser: Option<String>,
        _configured_reasoning_parser: Option<String>,
    ) -> Self {
        let stages: Vec<Box<dyn PipelineStage>> = vec![
            Box::new(harmony::stages::HarmonyPreparationStage::new()),
            Box::new(WorkerSelectionStage::new(
                worker_registry,
                policy_registry,
                WorkerSelectionMode::Regular,
            )),
            Box::new(ClientAcquisitionStage),
            Box::new(harmony::stages::HarmonyRequestBuildingStage::new(false)),
            Box::new(DispatchMetadataStage),
            Box::new(RequestExecutionStage::new(ExecutionMode::Single)),
            Box::new(harmony::stages::HarmonyResponseProcessingStage::new()),
        ];

        Self {
            stages: Arc::new(stages),
            backend_type: metrics_labels::BACKEND_REGULAR,
        }
    }

    /// Create a Harmony PD (prefill-decode) pipeline
    #[expect(dead_code)]
    pub fn new_harmony_pd(
        worker_registry: Arc<WorkerRegistry>,
        policy_registry: Arc<PolicyRegistry>,
        _tool_parser_factory: ToolParserFactory,
        _reasoning_parser_factory: ReasoningParserFactory,
        _configured_tool_parser: Option<String>,
        _configured_reasoning_parser: Option<String>,
    ) -> Self {
        let stages: Vec<Box<dyn PipelineStage>> = vec![
            Box::new(harmony::stages::HarmonyPreparationStage::new()),
            Box::new(WorkerSelectionStage::new(
                worker_registry,
                policy_registry,
                WorkerSelectionMode::PrefillDecode,
            )),
            Box::new(ClientAcquisitionStage),
            Box::new(harmony::stages::HarmonyRequestBuildingStage::new(true)),
            Box::new(DispatchMetadataStage),
            Box::new(RequestExecutionStage::new(ExecutionMode::DualDispatch)),
            Box::new(harmony::stages::HarmonyResponseProcessingStage::new()),
        ];

        Self {
            stages: Arc::new(stages),
            backend_type: metrics_labels::BACKEND_PD,
        }
    }

    /// Create a PD (prefill-decode) pipeline
    pub fn new_pd(
        worker_registry: Arc<WorkerRegistry>,
        policy_registry: Arc<PolicyRegistry>,
        tool_parser_factory: ToolParserFactory,
        reasoning_parser_factory: ReasoningParserFactory,
        configured_tool_parser: Option<String>,
        configured_reasoning_parser: Option<String>,
    ) -> Self {
        let processor = processor::ResponseProcessor::new(
            tool_parser_factory.clone(),
            reasoning_parser_factory.clone(),
            configured_tool_parser.clone(),
            configured_reasoning_parser.clone(),
        );

        let streaming_processor = Arc::new(streaming::StreamingProcessor::new(
            tool_parser_factory,
            reasoning_parser_factory,
            configured_tool_parser,
            configured_reasoning_parser,
            metrics_labels::BACKEND_PD,
        ));

        let stages: Vec<Box<dyn PipelineStage>> = vec![
            Box::new(ChatGeneratePreparationStage::new()),
            Box::new(WorkerSelectionStage::new(
                worker_registry,
                policy_registry,
                WorkerSelectionMode::PrefillDecode,
            )),
            Box::new(ClientAcquisitionStage),
            Box::new(ChatGenerateRequestBuildingStage::new(true)), // Inject PD metadata
            Box::new(DispatchMetadataStage),
            Box::new(RequestExecutionStage::new(ExecutionMode::DualDispatch)),
            Box::new(ChatGenerateResponseProcessingStage::new(
                processor,
                streaming_processor,
            )),
        ];

        Self {
            stages: Arc::new(stages),
            backend_type: metrics_labels::BACKEND_PD,
        }
    }

    /// Create an embeddings pipeline
    pub fn new_embeddings(
        worker_registry: Arc<WorkerRegistry>,
        policy_registry: Arc<PolicyRegistry>,
    ) -> Self {
        let stages: Vec<Box<dyn PipelineStage>> = vec![
            Box::new(EmbeddingPreparationStage::new()),
            Box::new(WorkerSelectionStage::new(
                worker_registry,
                policy_registry,
                WorkerSelectionMode::Regular, // Embeddings are always single
            )),
            Box::new(ClientAcquisitionStage),
            Box::new(EmbeddingRequestBuildingStage::new()),
            Box::new(DispatchMetadataStage),
            Box::new(RequestExecutionStage::new(ExecutionMode::Single)),
            Box::new(EmbeddingResponseProcessingStage::new()),
        ];

        Self {
            stages: Arc::new(stages),
            backend_type: metrics_labels::BACKEND_REGULAR, // Embeddings are regular for now
        }
    }

    /// Create a classify pipeline
    ///
    /// Classify reuses embedding stages for preparation and request building,
    /// but uses its own response processing for softmax + label mapping.
    pub fn new_classify(
        worker_registry: Arc<WorkerRegistry>,
        policy_registry: Arc<PolicyRegistry>,
    ) -> Self {
        let stages: Vec<Box<dyn PipelineStage>> = vec![
            Box::new(EmbeddingPreparationStage::new()),
            Box::new(WorkerSelectionStage::new(
                worker_registry,
                policy_registry,
                WorkerSelectionMode::Regular, // Classify is always single worker
            )),
            Box::new(ClientAcquisitionStage),
            Box::new(EmbeddingRequestBuildingStage::new()),
            Box::new(DispatchMetadataStage),
            Box::new(RequestExecutionStage::new(ExecutionMode::Single)),
            Box::new(ClassifyResponseProcessingStage::new()),
        ];

        Self {
            stages: Arc::new(stages),
            backend_type: metrics_labels::BACKEND_REGULAR,
        }
    }

    /// Create a Messages API pipeline (single-worker)
    ///
    /// Uses Messages-specific stages for preparation, request building, and response
    /// processing. Shares worker selection, client acquisition, dispatch metadata,
    /// and request execution stages with other pipelines.
    pub fn new_messages(
        worker_registry: Arc<WorkerRegistry>,
        policy_registry: Arc<PolicyRegistry>,
        tool_parser_factory: ToolParserFactory,
        reasoning_parser_factory: ReasoningParserFactory,
        configured_tool_parser: Option<String>,
        configured_reasoning_parser: Option<String>,
    ) -> Self {
        let processor = processor::ResponseProcessor::new(
            tool_parser_factory.clone(),
            reasoning_parser_factory.clone(),
            configured_tool_parser.clone(),
            configured_reasoning_parser.clone(),
        );

        let streaming_processor = Arc::new(streaming::StreamingProcessor::new(
            tool_parser_factory,
            reasoning_parser_factory,
            configured_tool_parser,
            configured_reasoning_parser,
            metrics_labels::BACKEND_REGULAR,
        ));

        let stages: Vec<Box<dyn PipelineStage>> = vec![
            Box::new(MessagePreparationStage),
            Box::new(WorkerSelectionStage::new(
                worker_registry,
                policy_registry,
                WorkerSelectionMode::Regular,
            )),
            Box::new(ClientAcquisitionStage),
            Box::new(MessageRequestBuildingStage::new(false)), // No PD metadata
            Box::new(DispatchMetadataStage),
            Box::new(RequestExecutionStage::new(ExecutionMode::Single)),
            Box::new(MessageResponseProcessingStage::new(
                processor,
                streaming_processor,
            )),
        ];

        Self {
            stages: Arc::new(stages),
            backend_type: metrics_labels::BACKEND_REGULAR,
        }
    }

    /// Create a Messages API PD (prefill-decode) pipeline
    pub fn new_messages_pd(
        worker_registry: Arc<WorkerRegistry>,
        policy_registry: Arc<PolicyRegistry>,
        tool_parser_factory: ToolParserFactory,
        reasoning_parser_factory: ReasoningParserFactory,
        configured_tool_parser: Option<String>,
        configured_reasoning_parser: Option<String>,
    ) -> Self {
        let processor = processor::ResponseProcessor::new(
            tool_parser_factory.clone(),
            reasoning_parser_factory.clone(),
            configured_tool_parser.clone(),
            configured_reasoning_parser.clone(),
        );

        let streaming_processor = Arc::new(streaming::StreamingProcessor::new(
            tool_parser_factory,
            reasoning_parser_factory,
            configured_tool_parser,
            configured_reasoning_parser,
            metrics_labels::BACKEND_PD,
        ));

        let stages: Vec<Box<dyn PipelineStage>> = vec![
            Box::new(MessagePreparationStage),
            Box::new(WorkerSelectionStage::new(
                worker_registry,
                policy_registry,
                WorkerSelectionMode::PrefillDecode,
            )),
            Box::new(ClientAcquisitionStage),
            Box::new(MessageRequestBuildingStage::new(true)), // Inject PD metadata
            Box::new(DispatchMetadataStage),
            Box::new(RequestExecutionStage::new(ExecutionMode::DualDispatch)),
            Box::new(MessageResponseProcessingStage::new(
                processor,
                streaming_processor,
            )),
        ];

        Self {
            stages: Arc::new(stages),
            backend_type: metrics_labels::BACKEND_PD,
        }
    }

    /// Create a Completion API pipeline (single-worker)
    ///
    /// Uses Completion-specific stages for preparation, request building, and response
    /// processing. Shares worker selection, client acquisition, dispatch metadata,
    /// and request execution stages with other pipelines.
    pub fn new_completion(
        worker_registry: Arc<WorkerRegistry>,
        policy_registry: Arc<PolicyRegistry>,
    ) -> Self {
        let processor = processor::ResponseProcessor::new(
            ToolParserFactory::default(),
            ReasoningParserFactory::default(),
            None,
            None,
        );

        let streaming_processor = Arc::new(streaming::StreamingProcessor::new(
            ToolParserFactory::default(),
            ReasoningParserFactory::default(),
            None,
            None,
            metrics_labels::BACKEND_REGULAR,
        ));

        let stages: Vec<Box<dyn PipelineStage>> = vec![
            Box::new(CompletionPreparationStage),
            Box::new(WorkerSelectionStage::new(
                worker_registry,
                policy_registry,
                WorkerSelectionMode::Regular,
            )),
            Box::new(ClientAcquisitionStage),
            Box::new(CompletionRequestBuildingStage::new(false)), // No PD metadata
            Box::new(DispatchMetadataStage),
            Box::new(RequestExecutionStage::new(ExecutionMode::Single)),
            Box::new(CompletionResponseProcessingStage::new(
                processor,
                streaming_processor,
            )),
        ];

        Self {
            stages: Arc::new(stages),
            backend_type: metrics_labels::BACKEND_REGULAR,
        }
    }

    /// Create a Completion API PD (prefill-decode) pipeline
    pub fn new_completion_pd(
        worker_registry: Arc<WorkerRegistry>,
        policy_registry: Arc<PolicyRegistry>,
    ) -> Self {
        let processor = processor::ResponseProcessor::new(
            ToolParserFactory::default(),
            ReasoningParserFactory::default(),
            None,
            None,
        );

        let streaming_processor = Arc::new(streaming::StreamingProcessor::new(
            ToolParserFactory::default(),
            ReasoningParserFactory::default(),
            None,
            None,
            metrics_labels::BACKEND_PD,
        ));

        let stages: Vec<Box<dyn PipelineStage>> = vec![
            Box::new(CompletionPreparationStage),
            Box::new(WorkerSelectionStage::new(
                worker_registry,
                policy_registry,
                WorkerSelectionMode::PrefillDecode,
            )),
            Box::new(ClientAcquisitionStage),
            Box::new(CompletionRequestBuildingStage::new(true)), // Inject PD metadata
            Box::new(DispatchMetadataStage),
            Box::new(RequestExecutionStage::new(ExecutionMode::DualDispatch)),
            Box::new(CompletionResponseProcessingStage::new(
                processor,
                streaming_processor,
            )),
        ];

        Self {
            stages: Arc::new(stages),
            backend_type: metrics_labels::BACKEND_PD,
        }
    }

    /// Execute the complete pipeline for a chat request
    pub async fn execute_chat(
        &self,
        request: Arc<ChatCompletionRequest>,
        headers: Option<http::HeaderMap>,
        model_id: String,
        components: Arc<SharedComponents>,
        tenant_request_meta: Option<TenantRequestMeta>,
    ) -> Response {
        let start = Instant::now();
        // Clone Arc for metrics (cheap atomic increment) to avoid borrow issues
        let request_for_metrics = Arc::clone(&request);
        let streaming = request.stream;

        // Record request start
        Metrics::record_router_request(
            metrics_labels::ROUTER_GRPC,
            self.backend_type,
            metrics_labels::CONNECTION_GRPC,
            &request_for_metrics.model,
            metrics_labels::ENDPOINT_CHAT,
            bool_to_static_str(streaming),
        );

        let model = request_for_metrics.model.clone();
        let mut ctx = RequestContext::for_chat(request, headers, model_id, components);
        ctx.input.tenant_request_meta = tenant_request_meta;

        for stage in self.stages.iter() {
            match stage.execute(&mut ctx).await {
                Ok(Some(response)) => {
                    // Stage completed with streaming response - record success and return
                    Metrics::record_router_duration(
                        metrics_labels::ROUTER_GRPC,
                        self.backend_type,
                        metrics_labels::CONNECTION_GRPC,
                        &model,
                        metrics_labels::ENDPOINT_CHAT,
                        start.elapsed(),
                    );
                    return Self::publish_grpc_response(
                        &ctx,
                        response,
                        "/v1/chat/completions",
                        &model,
                        streaming,
                        start,
                    )
                    .await;
                }
                Ok(None) => continue,
                Err(response) => {
                    Metrics::record_router_error(
                        metrics_labels::ROUTER_GRPC,
                        self.backend_type,
                        metrics_labels::CONNECTION_GRPC,
                        &model,
                        metrics_labels::ENDPOINT_CHAT,
                        error_type_from_status(response.status()),
                    );
                    error!(
                        "Stage {} failed with status {}",
                        stage.name(),
                        response.status()
                    );
                    Self::publish_grpc_error(
                        &ctx,
                        &response,
                        "/v1/chat/completions",
                        &model,
                        streaming,
                        start,
                    );
                    return response;
                }
            }
        }

        match ctx.state.response.final_response {
            Some(FinalResponse::Chat(ref response)) => {
                Metrics::record_router_duration(
                    metrics_labels::ROUTER_GRPC,
                    self.backend_type,
                    metrics_labels::CONNECTION_GRPC,
                    &model,
                    metrics_labels::ENDPOINT_CHAT,
                    start.elapsed(),
                );
                Self::publish_grpc_response(
                    &ctx,
                    axum::Json(response).into_response(),
                    "/v1/chat/completions",
                    &model,
                    streaming,
                    start,
                )
                .await
            }
            Some(
                ref response_type @ (FinalResponse::Generate(_)
                | FinalResponse::Completion(_)
                | FinalResponse::Embedding(_)
                | FinalResponse::Classify(_)
                | FinalResponse::Messages(_)),
            ) => Self::build_and_publish_grpc_error(
                &ctx,
                self.wrong_response_type(
                    "execute_chat",
                    "Chat",
                    &response_type,
                    &model,
                    metrics_labels::ENDPOINT_CHAT,
                ),
                "/v1/chat/completions",
                &model,
                streaming,
                start,
            ),
            None => Self::build_and_publish_grpc_error(
                &ctx,
                self.no_response_produced("execute_chat", &model, metrics_labels::ENDPOINT_CHAT),
                "/v1/chat/completions",
                &model,
                streaming,
                start,
            ),
        }
    }

    /// Execute the complete pipeline for a generate request
    pub async fn execute_generate(
        &self,
        request: Arc<GenerateRequest>,
        headers: Option<http::HeaderMap>,
        model_id: String,
        components: Arc<SharedComponents>,
        tenant_request_meta: Option<TenantRequestMeta>,
    ) -> Response {
        let start = Instant::now();
        let streaming = request.stream;

        // Record request start
        Metrics::record_router_request(
            metrics_labels::ROUTER_GRPC,
            self.backend_type,
            metrics_labels::CONNECTION_GRPC,
            &model_id,
            metrics_labels::ENDPOINT_GENERATE,
            bool_to_static_str(streaming),
        );

        let mut ctx = RequestContext::for_generate(request, headers, model_id.clone(), components);
        ctx.input.tenant_request_meta = tenant_request_meta;

        for stage in self.stages.iter() {
            match stage.execute(&mut ctx).await {
                Ok(Some(response)) => {
                    Metrics::record_router_duration(
                        metrics_labels::ROUTER_GRPC,
                        self.backend_type,
                        metrics_labels::CONNECTION_GRPC,
                        &model_id,
                        metrics_labels::ENDPOINT_GENERATE,
                        start.elapsed(),
                    );
                    return Self::publish_grpc_response(
                        &ctx,
                        response,
                        "/generate",
                        &model_id,
                        streaming,
                        start,
                    )
                    .await;
                }
                Ok(None) => continue,
                Err(response) => {
                    Metrics::record_router_error(
                        metrics_labels::ROUTER_GRPC,
                        self.backend_type,
                        metrics_labels::CONNECTION_GRPC,
                        &model_id,
                        metrics_labels::ENDPOINT_GENERATE,
                        error_type_from_status(response.status()),
                    );
                    error!(
                        "Stage {} failed with status {}",
                        stage.name(),
                        response.status()
                    );
                    Self::publish_grpc_error(
                        &ctx,
                        &response,
                        "/generate",
                        &model_id,
                        streaming,
                        start,
                    );
                    return response;
                }
            }
        }

        match ctx.state.response.final_response {
            Some(FinalResponse::Generate(ref response)) => {
                Metrics::record_router_duration(
                    metrics_labels::ROUTER_GRPC,
                    self.backend_type,
                    metrics_labels::CONNECTION_GRPC,
                    &model_id,
                    metrics_labels::ENDPOINT_GENERATE,
                    start.elapsed(),
                );
                Self::publish_grpc_response(
                    &ctx,
                    axum::Json(response).into_response(),
                    "/generate",
                    &model_id,
                    streaming,
                    start,
                )
                .await
            }
            Some(
                ref response_type @ (FinalResponse::Chat(_)
                | FinalResponse::Completion(_)
                | FinalResponse::Embedding(_)
                | FinalResponse::Classify(_)
                | FinalResponse::Messages(_)),
            ) => Self::build_and_publish_grpc_error(
                &ctx,
                self.wrong_response_type(
                    "execute_generate",
                    "Generate",
                    &response_type,
                    &model_id,
                    metrics_labels::ENDPOINT_GENERATE,
                ),
                "/generate",
                &model_id,
                streaming,
                start,
            ),
            None => Self::build_and_publish_grpc_error(
                &ctx,
                self.no_response_produced(
                    "execute_generate",
                    &model_id,
                    metrics_labels::ENDPOINT_GENERATE,
                ),
                "/generate",
                &model_id,
                streaming,
                start,
            ),
        }
    }

    /// Execute the complete pipeline for a completion request
    pub async fn execute_completion(
        &self,
        request: Arc<CompletionRequest>,
        headers: Option<http::HeaderMap>,
        model_id: String,
        components: Arc<SharedComponents>,
        tenant_request_meta: Option<TenantRequestMeta>,
    ) -> Response {
        let start = Instant::now();
        let model = request.model.clone();
        let streaming = request.stream;

        Metrics::record_router_request(
            metrics_labels::ROUTER_GRPC,
            self.backend_type,
            metrics_labels::CONNECTION_GRPC,
            &model,
            metrics_labels::ENDPOINT_COMPLETIONS,
            bool_to_static_str(streaming),
        );

        let mut ctx =
            RequestContext::for_completion(request, headers, model_id.clone(), components);
        ctx.input.tenant_request_meta = tenant_request_meta;

        for stage in self.stages.iter() {
            match stage.execute(&mut ctx).await {
                Ok(Some(response)) => {
                    Metrics::record_router_duration(
                        metrics_labels::ROUTER_GRPC,
                        self.backend_type,
                        metrics_labels::CONNECTION_GRPC,
                        &model,
                        metrics_labels::ENDPOINT_COMPLETIONS,
                        start.elapsed(),
                    );
                    return Self::publish_grpc_response(
                        &ctx,
                        response,
                        "/v1/completions",
                        &model,
                        streaming,
                        start,
                    )
                    .await;
                }
                Ok(None) => continue,
                Err(response) => {
                    Metrics::record_router_error(
                        metrics_labels::ROUTER_GRPC,
                        self.backend_type,
                        metrics_labels::CONNECTION_GRPC,
                        &model,
                        metrics_labels::ENDPOINT_COMPLETIONS,
                        error_type_from_status(response.status()),
                    );
                    error!(
                        "Stage {} failed with status {}",
                        stage.name(),
                        response.status()
                    );
                    Self::publish_grpc_error(
                        &ctx,
                        &response,
                        "/v1/completions",
                        &model,
                        streaming,
                        start,
                    );
                    return response;
                }
            }
        }

        match ctx.state.response.final_response {
            Some(FinalResponse::Completion(ref response)) => {
                Metrics::record_router_duration(
                    metrics_labels::ROUTER_GRPC,
                    self.backend_type,
                    metrics_labels::CONNECTION_GRPC,
                    &model,
                    metrics_labels::ENDPOINT_COMPLETIONS,
                    start.elapsed(),
                );
                Self::publish_grpc_response(
                    &ctx,
                    axum::Json(response).into_response(),
                    "/v1/completions",
                    &model,
                    streaming,
                    start,
                )
                .await
            }
            Some(
                ref response_type @ (FinalResponse::Chat(_)
                | FinalResponse::Generate(_)
                | FinalResponse::Embedding(_)
                | FinalResponse::Classify(_)
                | FinalResponse::Messages(_)),
            ) => Self::build_and_publish_grpc_error(
                &ctx,
                self.wrong_response_type(
                    "execute_completion",
                    "Completion",
                    &response_type,
                    &model,
                    metrics_labels::ENDPOINT_COMPLETIONS,
                ),
                "/v1/completions",
                &model,
                streaming,
                start,
            ),
            None => Self::build_and_publish_grpc_error(
                &ctx,
                self.no_response_produced(
                    "execute_completion",
                    &model,
                    metrics_labels::ENDPOINT_COMPLETIONS,
                ),
                "/v1/completions",
                &model,
                streaming,
                start,
            ),
        }
    }

    /// Execute the complete pipeline for an embedding request
    pub async fn execute_embeddings(
        &self,
        request: Arc<EmbeddingRequest>,
        headers: Option<http::HeaderMap>,
        model_id: String,
        components: Arc<SharedComponents>,
        tenant_request_meta: Option<TenantRequestMeta>,
    ) -> Response {
        debug!(
            "execute_embeddings: Starting execution for model: {}",
            &model_id
        );
        let start = Instant::now();

        // Record request start
        Metrics::record_router_request(
            metrics_labels::ROUTER_GRPC,
            self.backend_type,
            metrics_labels::CONNECTION_GRPC,
            &model_id,
            metrics_labels::ENDPOINT_EMBEDDINGS,
            bool_to_static_str(false),
        );

        let mut ctx = RequestContext::for_embedding(request, headers, model_id.clone(), components);
        ctx.input.tenant_request_meta = tenant_request_meta;

        for stage in self.stages.iter() {
            debug!("execute_embeddings: Executing stage: {}", stage.name());
            match stage.execute(&mut ctx).await {
                Ok(Some(response)) => {
                    debug!(
                        "execute_embeddings: Stage {} returned final response.",
                        stage.name()
                    );
                    Metrics::record_router_duration(
                        metrics_labels::ROUTER_GRPC,
                        self.backend_type,
                        metrics_labels::CONNECTION_GRPC,
                        &model_id,
                        metrics_labels::ENDPOINT_EMBEDDINGS,
                        start.elapsed(),
                    );
                    return Self::publish_grpc_response(
                        &ctx,
                        response,
                        "/v1/embeddings",
                        &model_id,
                        false,
                        start,
                    )
                    .await;
                }
                Ok(None) => {
                    debug!(
                        "execute_embeddings: Stage {} completed, continuing to next stage.",
                        stage.name()
                    );
                    continue;
                }
                Err(response) => {
                    error!(
                        "execute_embeddings: Stage {} failed with status {:?}, returning error response.",
                        stage.name(),
                        response.status()
                    );
                    Metrics::record_router_error(
                        metrics_labels::ROUTER_GRPC,
                        self.backend_type,
                        metrics_labels::CONNECTION_GRPC,
                        &model_id,
                        metrics_labels::ENDPOINT_EMBEDDINGS,
                        error_type_from_status(response.status()),
                    );
                    Self::publish_grpc_error(
                        &ctx,
                        &response,
                        "/v1/embeddings",
                        &model_id,
                        false,
                        start,
                    );
                    return response;
                }
            }
        }

        debug!(
            "execute_embeddings: Pipeline finished, processing final_response. Current state: {:?}",
            ctx.state.response.final_response
        );
        match ctx.state.response.final_response {
            Some(FinalResponse::Embedding(ref response)) => {
                Metrics::record_router_duration(
                    metrics_labels::ROUTER_GRPC,
                    self.backend_type,
                    metrics_labels::CONNECTION_GRPC,
                    &model_id,
                    metrics_labels::ENDPOINT_EMBEDDINGS,
                    start.elapsed(),
                );
                Self::publish_grpc_response(
                    &ctx,
                    axum::Json(response).into_response(),
                    "/v1/embeddings",
                    &model_id,
                    false,
                    start,
                )
                .await
            }
            Some(_) => {
                error!(function = "execute_embeddings", "Wrong response type");
                Self::build_and_publish_grpc_error(
                    &ctx,
                    error::internal_error(
                        "wrong_response_type",
                        "Internal error: wrong response type",
                    ),
                    "/v1/embeddings",
                    &model_id,
                    false,
                    start,
                )
            }
            None => {
                error!(
                    function = "execute_embeddings",
                    "No final response produced by pipeline."
                );
                Self::build_and_publish_grpc_error(
                    &ctx,
                    error::internal_error("no_response_produced", "No response produced"),
                    "/v1/embeddings",
                    &model_id,
                    false,
                    start,
                )
            }
        }
    }

    /// Execute the complete pipeline for a classify request
    pub async fn execute_classify(
        &self,
        request: Arc<ClassifyRequest>,
        headers: Option<http::HeaderMap>,
        model_id: String,
        components: Arc<SharedComponents>,
        tenant_request_meta: Option<TenantRequestMeta>,
    ) -> Response {
        debug!(
            "execute_classify: Starting execution for model: {}",
            &model_id
        );
        let start = Instant::now();

        // Record request start
        Metrics::record_router_request(
            metrics_labels::ROUTER_GRPC,
            self.backend_type,
            metrics_labels::CONNECTION_GRPC,
            &model_id,
            metrics_labels::ENDPOINT_CLASSIFY,
            bool_to_static_str(false), // Classify is never streaming
        );

        let mut ctx = RequestContext::for_classify(request, headers, model_id.clone(), components);
        ctx.input.tenant_request_meta = tenant_request_meta;

        for stage in self.stages.iter() {
            debug!("execute_classify: Executing stage: {}", stage.name());
            match stage.execute(&mut ctx).await {
                Ok(Some(response)) => {
                    debug!(
                        "execute_classify: Stage {} returned final response.",
                        stage.name()
                    );
                    Metrics::record_router_duration(
                        metrics_labels::ROUTER_GRPC,
                        self.backend_type,
                        metrics_labels::CONNECTION_GRPC,
                        &model_id,
                        metrics_labels::ENDPOINT_CLASSIFY,
                        start.elapsed(),
                    );
                    return response;
                }
                Ok(None) => {
                    debug!(
                        "execute_classify: Stage {} completed, continuing to next stage.",
                        stage.name()
                    );
                    continue;
                }
                Err(response) => {
                    error!(
                        "execute_classify: Stage {} failed with status {:?}, returning error response.",
                        stage.name(),
                        response.status()
                    );
                    Metrics::record_router_error(
                        metrics_labels::ROUTER_GRPC,
                        self.backend_type,
                        metrics_labels::CONNECTION_GRPC,
                        &model_id,
                        metrics_labels::ENDPOINT_CLASSIFY,
                        error_type_from_status(response.status()),
                    );
                    Self::publish_grpc_error(
                        &ctx,
                        &response,
                        "/v1/classify",
                        &model_id,
                        false,
                        start,
                    );
                    return response;
                }
            }
        }

        debug!(
            "execute_classify: Pipeline finished, processing final_response. Current state: {:?}",
            ctx.state.response.final_response
        );
        match ctx.state.response.final_response {
            Some(FinalResponse::Classify(ref response)) => {
                Metrics::record_router_duration(
                    metrics_labels::ROUTER_GRPC,
                    self.backend_type,
                    metrics_labels::CONNECTION_GRPC,
                    &model_id,
                    metrics_labels::ENDPOINT_CLASSIFY,
                    start.elapsed(),
                );
                Self::publish_grpc_response(
                    &ctx,
                    axum::Json(response).into_response(),
                    "/v1/classify",
                    &model_id,
                    false,
                    start,
                )
                .await
            }
            Some(_) => {
                error!(function = "execute_classify", "Wrong response type");
                Self::build_and_publish_grpc_error(
                    &ctx,
                    error::internal_error(
                        "wrong_response_type",
                        "Internal error: wrong response type",
                    ),
                    "/v1/classify",
                    &model_id,
                    false,
                    start,
                )
            }
            None => {
                error!(
                    function = "execute_classify",
                    "No final response produced by pipeline."
                );
                Self::build_and_publish_grpc_error(
                    &ctx,
                    error::internal_error("no_response_produced", "No response produced"),
                    "/v1/classify",
                    &model_id,
                    false,
                    start,
                )
            }
        }
    }

    /// Execute the complete pipeline for a Messages API request
    pub async fn execute_messages(
        &self,
        request: Arc<CreateMessageRequest>,
        headers: Option<http::HeaderMap>,
        model_id: String,
        components: Arc<SharedComponents>,
        tenant_request_meta: Option<TenantRequestMeta>,
    ) -> Response {
        let start = Instant::now();
        let streaming = request.stream.unwrap_or(false);

        // Record request start
        Metrics::record_router_request(
            metrics_labels::ROUTER_GRPC,
            self.backend_type,
            metrics_labels::CONNECTION_GRPC,
            &request.model,
            metrics_labels::ENDPOINT_MESSAGES,
            bool_to_static_str(streaming),
        );

        let mut ctx = RequestContext::for_messages(request.clone(), headers, model_id, components);
        ctx.input.tenant_request_meta = tenant_request_meta;

        for stage in self.stages.iter() {
            match stage.execute(&mut ctx).await {
                Ok(Some(response)) => {
                    // Stage completed with streaming response
                    Metrics::record_router_duration(
                        metrics_labels::ROUTER_GRPC,
                        self.backend_type,
                        metrics_labels::CONNECTION_GRPC,
                        &request.model,
                        metrics_labels::ENDPOINT_MESSAGES,
                        start.elapsed(),
                    );
                    return response;
                }
                Ok(None) => continue,
                Err(response) => {
                    Metrics::record_router_error(
                        metrics_labels::ROUTER_GRPC,
                        self.backend_type,
                        metrics_labels::CONNECTION_GRPC,
                        &request.model,
                        metrics_labels::ENDPOINT_MESSAGES,
                        error_type_from_status(response.status()),
                    );
                    error!(
                        "Stage {} failed with status {}",
                        stage.name(),
                        response.status()
                    );
                    Self::publish_grpc_error(
                        &ctx,
                        &response,
                        "/v1/messages",
                        &request.model,
                        streaming,
                        start,
                    );
                    return response;
                }
            }
        }

        match ctx.state.response.final_response {
            Some(FinalResponse::Messages(ref response)) => {
                Metrics::record_router_duration(
                    metrics_labels::ROUTER_GRPC,
                    self.backend_type,
                    metrics_labels::CONNECTION_GRPC,
                    &request.model,
                    metrics_labels::ENDPOINT_MESSAGES,
                    start.elapsed(),
                );
                Self::publish_grpc_response(
                    &ctx,
                    axum::Json(response).into_response(),
                    "/v1/messages",
                    &request.model,
                    streaming,
                    start,
                )
                .await
            }
            Some(
                ref response_type @ (FinalResponse::Chat(_)
                | FinalResponse::Generate(_)
                | FinalResponse::Completion(_)
                | FinalResponse::Embedding(_)
                | FinalResponse::Classify(_)),
            ) => Self::build_and_publish_grpc_error(
                &ctx,
                self.wrong_response_type(
                    "execute_messages",
                    "Messages",
                    &response_type,
                    &request.model,
                    metrics_labels::ENDPOINT_MESSAGES,
                ),
                "/v1/messages",
                &request.model,
                streaming,
                start,
            ),
            None => Self::build_and_publish_grpc_error(
                &ctx,
                self.no_response_produced(
                    "execute_messages",
                    &request.model,
                    metrics_labels::ENDPOINT_MESSAGES,
                ),
                "/v1/messages",
                &request.model,
                streaming,
                start,
            ),
        }
    }

    /// Execute chat pipeline for responses endpoint
    ///
    /// Used by ALL non-streaming /v1/responses requests.
    /// Uses the same 7 pipeline stages as execute_chat(), with two differences:
    /// 1. Returns Result<ChatCompletionResponse, Response> for tool_loop composition
    /// 2. Disallows streaming (responses endpoint uses different SSE format)
    pub async fn execute_chat_for_responses(
        &self,
        request: Arc<ChatCompletionRequest>,
        headers: Option<http::HeaderMap>,
        model_id: String,
        components: Arc<SharedComponents>,
        tenant_request_meta: Option<TenantRequestMeta>,
    ) -> Result<ChatCompletionResponse, Response> {
        let mut ctx = RequestContext::for_chat(request, headers, model_id, components);
        ctx.input.tenant_request_meta = tenant_request_meta;

        for (idx, stage) in self.stages.iter().enumerate() {
            match stage.execute(&mut ctx).await {
                Ok(Some(_response)) => {
                    // Streaming not supported for responses sync mode
                    error!(
                        function = "execute_chat_for_responses",
                        "Streaming attempted in responses context"
                    );
                    return Err(error::bad_request(
                        "streaming_not_supported",
                        "Streaming is not supported in this context".to_string(),
                    ));
                }
                Ok(None) => {
                    continue;
                }
                Err(response) => {
                    // Error occurred - return the response as-is to preserve HTTP status codes
                    error!(
                        "Stage {} ({}) failed with status {}",
                        idx + 1,
                        stage.name(),
                        response.status()
                    );
                    return Err(response);
                }
            }
        }

        match ctx.state.response.final_response {
            Some(FinalResponse::Chat(response)) => Ok(response),
            Some(FinalResponse::Generate(_))
            | Some(FinalResponse::Completion(_))
            | Some(FinalResponse::Embedding(_))
            | Some(FinalResponse::Classify(_))
            | Some(FinalResponse::Messages(_)) => {
                error!(
                    function = "execute_chat_for_responses",
                    "Wrong response type: expected Chat, got Generate/Embedding/Classify/Messages"
                );
                Err(error::internal_error(
                    "wrong_response_type",
                    "Internal error: wrong response type",
                ))
            }
            None => {
                error!(
                    function = "execute_chat_for_responses",
                    "No response produced by pipeline"
                );
                Err(error::internal_error(
                    "no_response_produced",
                    "No response produced",
                ))
            }
        }
    }

    /// Execute Harmony Responses API request through all pipeline stages
    ///
    /// This method runs a single iteration of the Responses API request,
    /// returning either ToolCallsFound (continue serving) or Completed (final response).
    ///
    /// Called by harmony::responses::serve_harmony_responses() for each iteration.
    ///
    /// # Arguments
    ///
    /// * `request` - Responses API request
    /// * `ctx` - Harmony Responses context with MCP manager and components
    ///
    /// # Returns
    ///
    /// ResponsesIterationResult indicating whether to continue iteration or return
    pub async fn execute_harmony_responses(
        &self,
        request: &openai_protocol::responses::ResponsesRequest,
        harmony_ctx: &ResponsesContext,
        tenant_request_meta: Option<TenantRequestMeta>,
    ) -> Result<harmony::ResponsesIterationResult, Response> {
        // Create RequestContext for this Responses request
        let mut ctx = RequestContext::for_responses(
            Arc::new(request.clone()),
            None,                  // No headers needed for internal pipeline execution
            request.model.clone(), // Model ID from request
            harmony_ctx.components.clone(),
        );
        ctx.input.tenant_request_meta = tenant_request_meta;

        for (idx, stage) in self.stages.iter().enumerate() {
            match stage.execute(&mut ctx).await {
                Ok(Some(response)) => {
                    // Stage returned early response (e.g., streaming) - not expected for Responses iteration
                    error!(
                        "Stage {} ({}) returned unexpected response during Responses iteration",
                        idx + 1,
                        stage.name()
                    );
                    return Err(response);
                }
                Ok(None) => {
                    continue;
                }
                Err(response) => {
                    // Stage failed
                    error!(
                        "Stage {} ({}) failed with status {}",
                        idx + 1,
                        stage.name(),
                        response.status()
                    );
                    return Err(response);
                }
            }
        }

        // Extract ResponsesIterationResult from context
        // This should have been set by HarmonyResponseProcessingStage
        ctx.state
            .response
            .responses_iteration_result
            .take()
            .ok_or_else(|| {
                error!(
                    function = "execute_harmony_responses",
                    "No ResponsesIterationResult produced by pipeline"
                );
                error::internal_error(
                    "no_responses_iteration_result",
                    "No ResponsesIterationResult produced by pipeline",
                )
            })
    }

    /// Execute Harmony Responses pipeline iteration with streaming support
    ///
    /// This version executes the pipeline up to the dispatch stage and returns
    /// the raw ExecutionResult (with stream) and LoadGuards for token-level streaming processing.
    /// The caller is responsible for keeping load_guards alive until stream processing completes.
    pub async fn execute_harmony_responses_streaming(
        &self,
        request: &openai_protocol::responses::ResponsesRequest,
        harmony_ctx: &ResponsesContext,
        tenant_request_meta: Option<TenantRequestMeta>,
    ) -> Result<(ExecutionResult, Option<LoadGuards>), Response> {
        // Create RequestContext for this Responses request
        let mut ctx = RequestContext::for_responses(
            Arc::new(request.clone()),
            None,
            request.model.clone(),
            harmony_ctx.components.clone(),
        );
        ctx.input.tenant_request_meta = tenant_request_meta;

        for (idx, stage) in self.stages.iter().enumerate() {
            match stage.execute(&mut ctx).await {
                Ok(Some(response)) => {
                    error!(
                        "Stage {} ({}) returned unexpected response during streaming Responses",
                        idx + 1,
                        stage.name()
                    );
                    return Err(response);
                }
                Ok(None) => continue,
                Err(response) => {
                    error!(
                        "Stage {} ({}) failed with status {}",
                        idx + 1,
                        stage.name(),
                        response.status()
                    );
                    return Err(response);
                }
            }
        }

        // Extract execution_result (the raw stream from workers) and load_guards
        let execution_result = ctx.state.response.execution_result.take().ok_or_else(|| {
            error!(
                function = "execute_harmony_responses_streaming",
                "No ExecutionResult produced by pipeline"
            );
            error::internal_error(
                "no_execution_result_produced",
                "No ExecutionResult produced by pipeline",
            )
        })?;

        let load_guards = ctx.state.load_guards.take();

        Ok((execution_result, load_guards))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::{body::to_bytes, http::HeaderMap, response::IntoResponse};
    use llm_tokenizer::TokenizerRegistry;
    use openai_protocol::{
        chat::ChatCompletionResponse,
        common::Usage,
        generate::GenerateRequest,
        model_card::ModelCard,
        worker::{ConnectionMode, HealthCheckConfig, WorkerStatus, WorkerType},
    };
    use reasoning_parser::ParserFactory as ReasoningParserFactory;
    use tool_parser::ParserFactory as ToolParserFactory;

    use super::*;
    use crate::{
        observability::usage_events::{UsageEvent, UsageEventPublisher},
        worker::BasicWorkerBuilder,
    };

    #[derive(Clone, Default)]
    struct CapturingUsageEventPublisher {
        events: Arc<Mutex<Vec<UsageEvent>>>,
    }

    impl CapturingUsageEventPublisher {
        fn events(&self) -> Vec<UsageEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    impl UsageEventPublisher for CapturingUsageEventPublisher {
        fn publish(&self, event: UsageEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    fn no_health_check() -> HealthCheckConfig {
        HealthCheckConfig {
            disable_health_check: true,
            ..Default::default()
        }
    }

    fn test_components(
        publisher: Arc<dyn UsageEventPublisher>,
        header_keys: Vec<String>,
    ) -> Arc<SharedComponents> {
        Arc::new(SharedComponents {
            tokenizer_registry: Arc::new(TokenizerRegistry::new()),
            tool_parser_factory: ToolParserFactory::default(),
            reasoning_parser_factory: ReasoningParserFactory::default(),
            configured_tool_parser: None,
            multimodal: None,
            usage_event_publisher: publisher,
            kafka_event_header_keys: header_keys,
            kafka_capture_response_body: false,
            kafka_body_capture_max_bytes: 8192,
        })
    }

    fn test_worker(url: &str, worker_type: WorkerType, served_model_name: &str) -> Arc<dyn Worker> {
        Arc::new(
            BasicWorkerBuilder::new(url)
                .worker_type(worker_type)
                .connection_mode(ConnectionMode::Grpc)
                .model(ModelCard::new("gpt-public"))
                .served_model_name(served_model_name)
                .health_config(no_health_check())
                .status(WorkerStatus::Ready)
                .build(),
        )
    }

    fn test_generate_request() -> GenerateRequest {
        serde_json::from_value(serde_json::json!({
            "model": "gpt-public",
            "text": "hello",
            "stream": false
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn grpc_non_streaming_response_publishes_usage_event() {
        let publisher = CapturingUsageEventPublisher::default();
        let publisher_arc: Arc<dyn UsageEventPublisher> = Arc::new(publisher.clone());
        let mut headers = HeaderMap::new();
        headers.insert("x-project-id", "project-1".parse().unwrap());
        headers.insert("x-model-id", "model-1".parse().unwrap());
        let mut ctx = RequestContext::for_generate(
            Arc::new(test_generate_request()),
            Some(headers),
            "gpt-public".to_string(),
            test_components(
                publisher_arc,
                vec!["x-project-id".to_string(), "x-model-id".to_string()],
            ),
        );
        ctx.state.workers = Some(WorkerSelection::Single {
            worker: test_worker("grpc://decode:8000", WorkerType::Regular, "llama-real"),
        });

        let response = axum::Json(
            ChatCompletionResponse::builder("chatcmpl-test", "llama-real")
                .usage(Usage::from_counts(3, 5))
                .build(),
        )
        .into_response();
        let response = RequestPipeline::publish_grpc_response(
            &ctx,
            response,
            "/v1/chat/completions",
            "gpt-public",
            false,
            Instant::now(),
        )
        .await;
        assert!(response.status().is_success());
        let _ = to_bytes(response.into_body(), usize::MAX).await.unwrap();

        let events = publisher.events();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.operation, "chat");
        assert_eq!(event.original_model, "gpt-public");
        assert_eq!(event.request_model, "llama-real");
        assert_eq!(event.response_model, "llama-real");
        assert_eq!(event.backend_name, "grpc://decode:8000");
        assert_eq!(event.selected_pool, "grpc://decode:8000");
        assert_eq!(event.model_name_override, "llama-real");
        assert_eq!(event.tokens.input_tokens, 3);
        assert_eq!(event.tokens.output_tokens, 5);
        assert_eq!(event.tokens.total_tokens, 8);
        assert_eq!(event.x_project_id.as_deref(), Some("project-1"));
        assert_eq!(event.x_model_id.as_deref(), Some("model-1"));
    }

    #[test]
    fn grpc_pd_usage_context_uses_decode_worker_and_selected_pool() {
        let publisher: Arc<dyn UsageEventPublisher> =
            Arc::new(CapturingUsageEventPublisher::default());
        let mut ctx = RequestContext::for_generate(
            Arc::new(test_generate_request()),
            None,
            "gpt-public".to_string(),
            test_components(publisher, Vec::new()),
        );
        ctx.state.workers = Some(WorkerSelection::Dual {
            prefill: test_worker("grpc://prefill:8000", WorkerType::Prefill, "llama-prefill"),
            decode: test_worker("grpc://decode:8000", WorkerType::Decode, "llama-decode"),
            runtime_type: crate::worker::RuntimeType::Sglang,
        });

        let usage_ctx =
            RequestPipeline::usage_context(&ctx, "/generate", "gpt-public", false, Instant::now());
        let event = usage_ctx.build_event(
            http::StatusCode::OK,
            None,
            TokenInfo::default(),
            None,
            None,
            None,
        );

        assert_eq!(event.operation, "generate");
        assert_eq!(event.request_model, "llama-decode");
        assert_eq!(event.backend_name, "grpc://decode:8000");
        assert_eq!(
            event.selected_pool,
            "prefill=grpc://prefill:8000,decode=grpc://decode:8000"
        );
        assert_eq!(event.model_name_override, "llama-decode");
    }
}
