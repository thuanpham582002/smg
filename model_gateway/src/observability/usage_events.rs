use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use axum::{
    body::Body,
    http::{HeaderMap, StatusCode},
    response::Response,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use http_body::Frame;
use rskafka::{
    client::{
        partition::{Compression, UnknownTopicHandling},
        ClientBuilder, Credentials, SaslConfig,
    },
    record::Record,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::{
    config::KafkaUsageConfig, observability::metrics::Metrics, routers::common::sse::SseDecoder,
    worker::Worker,
};

const EVENT_BACKEND_KAFKA: &str = "kafka";
const EVENT_BACKEND_NOOP: &str = "noop";
const RESULT_SUCCESS: &str = "success";
const RESULT_FAILURE: &str = "failure";
const RESULT_DISABLED: &str = "disabled";
const HEADER_X_PROJECT_ID: &str = "x-project-id";
const HEADER_X_USER_ID: &str = "x-user-id";
const HEADER_X_API_KEY_ID: &str = "x-api-key-id";
const HEADER_X_MODEL_NAME: &str = "x-model-name";
const HEADER_X_AI_EG_MODEL: &str = "x-ai-eg-model";
const HEADER_X_MODEL_ID: &str = "x-model-id";
const HEADER_X_INPUT_PRICE: &str = "x-input-price";
const HEADER_X_OUTPUT_PRICE: &str = "x-output-price";
const HEADER_X_IS_FREE: &str = "x-is-free";

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TokenInfo {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UsageEvent {
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub request_id: String,
    pub operation: String,
    pub original_model: String,
    pub request_model: String,
    pub response_model: String,
    pub backend: String,
    pub backend_name: String,
    pub success: bool,
    pub error_type: String,
    pub latency_ms: u64,
    pub stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_to_first_token_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inter_token_latency_ms: Option<f64>,
    pub tokens: TokenInfo,
    pub selected_pool: String,
    pub model_name_override: String,
    pub headers: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_body: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_body_truncated: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_body: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_body_truncated: Option<bool>,
    #[serde(rename = "x-project-id", skip_serializing_if = "Option::is_none")]
    pub x_project_id: Option<String>,
    #[serde(rename = "x-user-id", skip_serializing_if = "Option::is_none")]
    pub x_user_id: Option<String>,
    #[serde(rename = "x-api-key-id", skip_serializing_if = "Option::is_none")]
    pub x_api_key_id: Option<String>,
    #[serde(rename = "x-model-name", skip_serializing_if = "Option::is_none")]
    pub x_model_name: Option<String>,
    #[serde(rename = "x-ai-eg-model", skip_serializing_if = "Option::is_none")]
    pub x_ai_eg_model: Option<String>,
    #[serde(rename = "x-model-id", skip_serializing_if = "Option::is_none")]
    pub x_model_id: Option<String>,
    #[serde(rename = "x-input-price", skip_serializing_if = "Option::is_none")]
    pub x_input_price: Option<String>,
    #[serde(rename = "x-output-price", skip_serializing_if = "Option::is_none")]
    pub x_output_price: Option<String>,
    #[serde(rename = "x-is-free", skip_serializing_if = "Option::is_none")]
    pub x_is_free: Option<String>,
}

pub trait UsageEventPublisher: Send + Sync {
    fn publish(&self, event: UsageEvent);
}

pub struct NoopUsageEventPublisher;

impl UsageEventPublisher for NoopUsageEventPublisher {
    fn publish(&self, _event: UsageEvent) {
        Metrics::record_usage_event_publish(EVENT_BACKEND_NOOP, RESULT_DISABLED);
    }
}

pub struct KafkaUsageEventPublisher {
    sender: mpsc::Sender<UsageEvent>,
}

impl KafkaUsageEventPublisher {
    pub fn new(config: &KafkaUsageConfig) -> Result<Self, String> {
        if config.tls_enabled {
            return Err("KAFKA_TLS_ENABLED requires rskafka transport-tls feature; rebuild SMG with that feature or use plaintext Kafka".to_string());
        }
        let sasl_config = match (config.sasl_user.clone(), config.sasl_password.clone()) {
            (Some(user), Some(password)) => {
                let credentials = Credentials::new(user, password);
                match config
                    .sasl_mechanism
                    .as_deref()
                    .unwrap_or("PLAIN")
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "PLAIN" => Some(SaslConfig::Plain(credentials)),
                    "SCRAM-SHA-256" => Some(SaslConfig::ScramSha256(credentials)),
                    "SCRAM-SHA-512" => Some(SaslConfig::ScramSha512(credentials)),
                    mechanism => {
                        return Err(format!("unsupported Kafka SASL mechanism: {mechanism}"))
                    }
                }
            }
            _ => None,
        };
        let (sender, receiver) = mpsc::channel(1024);
        let brokers = config.brokers.clone();
        let topic = config.topic.clone();
        #[expect(
            clippy::disallowed_methods,
            reason = "usage event publishing worker must outlive individual request tasks"
        )]
        tokio::spawn(async move {
            run_kafka_usage_event_worker(brokers, topic, sasl_config, receiver).await;
        });
        Ok(Self { sender })
    }
}

impl UsageEventPublisher for KafkaUsageEventPublisher {
    fn publish(&self, event: UsageEvent) {
        if let Err(error) = self.sender.try_send(event) {
            warn!(error = %error, "failed to enqueue usage event");
            Metrics::record_usage_event_publish(EVENT_BACKEND_KAFKA, RESULT_FAILURE);
        }
    }
}

async fn run_kafka_usage_event_worker(
    brokers: Vec<String>,
    topic: String,
    sasl_config: Option<SaslConfig>,
    mut receiver: mpsc::Receiver<UsageEvent>,
) {
    let mut partition_client = None;

    while let Some(event) = receiver.recv().await {
        let request_id = event.request_id.clone();
        let payload = match serde_json::to_string(&event) {
            Ok(payload) => payload,
            Err(e) => {
                warn!(request_id = %request_id, error = %e, "failed to serialize usage event");
                Metrics::record_usage_event_publish(EVENT_BACKEND_KAFKA, RESULT_FAILURE);
                continue;
            }
        };

        if partition_client.is_none() {
            let mut builder = ClientBuilder::new(brokers.clone()).client_id("smg-usage-events");
            if let Some(sasl_config) = sasl_config.clone() {
                builder = builder.sasl_config(sasl_config);
            }
            match async {
                let client = builder.build().await?;
                client
                    .partition_client(&topic, 0, UnknownTopicHandling::Retry)
                    .await
            }
            .await
            {
                Ok(client) => {
                    partition_client = Some(client);
                }
                Err(error) => {
                    warn!(request_id = %request_id, error = %error, "failed to initialize Kafka usage producer");
                    Metrics::record_usage_event_publish(EVENT_BACKEND_KAFKA, RESULT_FAILURE);
                    continue;
                }
            }
        }

        let record = Record {
            key: Some(request_id.as_bytes().to_vec()),
            value: Some(payload.into_bytes()),
            headers: BTreeMap::new(),
            timestamp: Utc::now(),
        };

        let Some(client) = partition_client.as_ref() else {
            Metrics::record_usage_event_publish(EVENT_BACKEND_KAFKA, RESULT_FAILURE);
            continue;
        };
        match client
            .produce(vec![record], Compression::NoCompression)
            .await
        {
            Ok(_) => Metrics::record_usage_event_publish(EVENT_BACKEND_KAFKA, RESULT_SUCCESS),
            Err(error) => {
                warn!(request_id = %request_id, error = %error, "failed to publish usage event");
                Metrics::record_usage_event_publish(EVENT_BACKEND_KAFKA, RESULT_FAILURE);
                partition_client = None;
            }
        }
    }
}

pub fn create_usage_event_publisher(
    config: &crate::config::RouterConfig,
) -> Arc<dyn UsageEventPublisher> {
    if !config.kafka_usage.is_enabled() {
        return Arc::new(NoopUsageEventPublisher);
    }

    match KafkaUsageEventPublisher::new(&config.kafka_usage) {
        Ok(publisher) => Arc::new(publisher),
        Err(error) => {
            warn!(error = %error, "Kafka usage event publisher disabled");
            Arc::new(NoopUsageEventPublisher)
        }
    }
}

#[derive(Debug, Clone)]
pub struct UsageEventContext {
    pub request_id: String,
    pub operation: String,
    pub original_model: String,
    pub request_model: String,
    pub backend_name: String,
    pub selected_pool: String,
    pub model_name_override: String,
    pub stream: bool,
    pub started_at: Instant,
    pub headers: HashMap<String, String>,
    pub request_body: Option<CapturedBody>,
    pub capture_response_body: bool,
    pub body_capture_max_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapturedBody {
    pub value: String,
    pub truncated: bool,
}

impl UsageEventContext {
    pub fn unrouted(
        request_headers: Option<&HeaderMap>,
        header_keys: &[String],
        route: &str,
        model_id: &str,
        stream: bool,
        started_at: Instant,
    ) -> Self {
        Self::from_parts(
            request_headers,
            header_keys,
            route,
            model_id,
            model_id,
            "",
            "",
            "",
            stream,
            started_at,
        )
    }

    pub fn with_audit_capture(
        mut self,
        request_body: Option<CapturedBody>,
        capture_response_body: bool,
        body_capture_max_bytes: usize,
    ) -> Self {
        self.request_body = request_body;
        self.capture_response_body = capture_response_body;
        self.body_capture_max_bytes = body_capture_max_bytes;
        self
    }

    pub fn from_request(
        request_headers: Option<&HeaderMap>,
        header_keys: &[String],
        route: &str,
        model_id: &str,
        worker: &dyn Worker,
        stream: bool,
        started_at: Instant,
    ) -> Self {
        let served_model_name = worker
            .metadata()
            .spec
            .served_model_name
            .as_deref()
            .unwrap_or("");

        Self::from_parts(
            request_headers,
            header_keys,
            route,
            model_id,
            if served_model_name.is_empty() {
                model_id
            } else {
                served_model_name
            },
            worker.url(),
            worker.url(),
            served_model_name,
            stream,
            started_at,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_backend(
        request_headers: Option<&HeaderMap>,
        header_keys: &[String],
        route: &str,
        model_id: &str,
        request_model: &str,
        backend_name: &str,
        selected_pool: &str,
        model_name_override: &str,
        stream: bool,
        started_at: Instant,
    ) -> Self {
        Self::from_parts(
            request_headers,
            header_keys,
            route,
            model_id,
            request_model,
            backend_name,
            selected_pool,
            model_name_override,
            stream,
            started_at,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_parts(
        request_headers: Option<&HeaderMap>,
        header_keys: &[String],
        route: &str,
        model_id: &str,
        request_model: &str,
        backend_name: &str,
        selected_pool: &str,
        model_name_override: &str,
        stream: bool,
        started_at: Instant,
    ) -> Self {
        let request_id = request_headers
            .and_then(|headers| headers.get("x-request-id"))
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
            .unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
        let headers = filter_headers(request_headers, header_keys);
        let request_id = headers.get("x-request-id").cloned().unwrap_or(request_id);

        Self {
            request_id,
            operation: operation_from_route(route).to_string(),
            original_model: model_id.to_string(),
            request_model: request_model.to_string(),
            backend_name: backend_name.to_string(),
            selected_pool: selected_pool.to_string(),
            model_name_override: model_name_override.to_string(),
            stream,
            started_at,
            headers,
            request_body: None,
            capture_response_body: false,
            body_capture_max_bytes: 0,
        }
    }

    pub fn build_event(
        &self,
        status: StatusCode,
        response_model: Option<String>,
        tokens: TokenInfo,
        ttft: Option<Duration>,
        inter_token_latency: Option<Duration>,
        error_type: Option<&str>,
    ) -> UsageEvent {
        UsageEvent {
            event_type: "request_completed".to_string(),
            timestamp: Utc::now(),
            request_id: self.request_id.clone(),
            operation: self.operation.clone(),
            original_model: self.original_model.clone(),
            request_model: self.request_model.clone(),
            response_model: response_model
                .filter(|model| !model.is_empty())
                .unwrap_or_else(|| self.request_model.clone()),
            backend: "SMG".to_string(),
            backend_name: self.backend_name.clone(),
            success: status.is_success(),
            error_type: error_type
                .filter(|error| !error.is_empty())
                .unwrap_or_else(|| {
                    if status.is_success() {
                        ""
                    } else {
                        status.canonical_reason().unwrap_or("upstream_error")
                    }
                })
                .to_string(),
            latency_ms: self.started_at.elapsed().as_millis() as u64,
            stream: self.stream,
            time_to_first_token_ms: ttft.map(|duration| duration.as_millis() as u64),
            inter_token_latency_ms: inter_token_latency
                .map(|duration| duration.as_secs_f64() * 1000.0),
            tokens,
            selected_pool: self.selected_pool.clone(),
            model_name_override: self.model_name_override.clone(),
            x_project_id: self.headers.get(HEADER_X_PROJECT_ID).cloned(),
            x_user_id: self.headers.get(HEADER_X_USER_ID).cloned(),
            x_api_key_id: self.headers.get(HEADER_X_API_KEY_ID).cloned(),
            x_model_name: self.headers.get(HEADER_X_MODEL_NAME).cloned(),
            x_ai_eg_model: self.headers.get(HEADER_X_AI_EG_MODEL).cloned(),
            x_model_id: self.headers.get(HEADER_X_MODEL_ID).cloned(),
            x_input_price: self.headers.get(HEADER_X_INPUT_PRICE).cloned(),
            x_output_price: self.headers.get(HEADER_X_OUTPUT_PRICE).cloned(),
            x_is_free: self.headers.get(HEADER_X_IS_FREE).cloned(),
            headers: self.headers.clone(),
            request_body: self.request_body.as_ref().map(|body| body.value.clone()),
            request_body_truncated: self.request_body.as_ref().map(|body| body.truncated),
            response_body: None,
            response_body_truncated: None,
        }
    }

    fn attach_response_body(&self, event: &mut UsageEvent, body: CapturedBody) {
        if self.capture_response_body {
            event.response_body = Some(body.value);
            event.response_body_truncated = Some(body.truncated);
        }
    }
}

pub fn capture_body(body: &[u8], max_bytes: usize) -> Option<CapturedBody> {
    if max_bytes == 0 {
        return None;
    }
    let captured_len = body.len().min(max_bytes);
    Some(CapturedBody {
        value: String::from_utf8_lossy(&body[..captured_len]).into_owned(),
        truncated: body.len() > captured_len,
    })
}

pub(crate) fn filter_headers(
    headers: Option<&HeaderMap>,
    configured_keys: &[String],
) -> HashMap<String, String> {
    let allowed: HashSet<String> = configured_keys
        .iter()
        .map(|key| key.to_ascii_lowercase())
        .collect();
    let Some(headers) = headers else {
        return HashMap::new();
    };

    headers
        .iter()
        .filter_map(|(name, value)| {
            let key = name.as_str().to_ascii_lowercase();
            if !key.starts_with("x-") || !allowed.contains(&key) {
                return None;
            }
            let value = value.to_str().ok()?.to_string();
            Some((key, value))
        })
        .collect()
}

pub(crate) fn operation_from_route(route: &str) -> &'static str {
    match route {
        "/v1/chat/completions" => "chat",
        "/v1/completions" => "completion",
        "/v1/responses" => "responses",
        "/v1/embeddings" => "embedding",
        "/v1/classify" => "classify",
        "/v1/rerank" => "rerank",
        "/generate" => "generate",
        _ => "request",
    }
}

pub async fn publish_non_streaming_response(
    response: Response,
    ctx: UsageEventContext,
    publisher: Arc<dyn UsageEventPublisher>,
) -> Response {
    let status = response.status();
    let (parts, body) = response.into_parts();
    let body_bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(error) => {
            let event = ctx.build_event(
                StatusCode::INTERNAL_SERVER_ERROR,
                None,
                TokenInfo::default(),
                None,
                None,
                Some("read_response_body_failed"),
            );
            publisher.publish(event);
            return crate::routers::error::internal_error(
                "read_response_body_failed",
                format!("Failed to read response body for usage event: {error}"),
            );
        }
    };

    let (tokens, response_model) = extract_usage_from_json(&body_bytes);
    let mut event = ctx.build_event(status, response_model, tokens, None, None, None);
    if let Some(body) = capture_body(&body_bytes, ctx.body_capture_max_bytes) {
        ctx.attach_response_body(&mut event, body);
    }
    publisher.publish(event);
    Response::from_parts(parts, Body::from(body_bytes))
}

pub fn wrap_streaming_response(
    response: Response,
    ctx: UsageEventContext,
    publisher: Arc<dyn UsageEventPublisher>,
) -> Response {
    let (parts, body) = response.into_parts();
    let status = parts.status;
    let observer = Arc::new(UsageStreamObserver::new(ctx, publisher, status));
    Response::from_parts(
        parts,
        Body::new(UsageEventBody {
            inner: body,
            observer,
        }),
    )
}

struct UsageEventBody {
    inner: Body,
    observer: Arc<UsageStreamObserver>,
}

impl http_body::Body for UsageEventBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        match std::pin::Pin::new(&mut this.inner).poll_frame(cx) {
            std::task::Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.observer.observe_data(data);
                }
                std::task::Poll::Ready(Some(Ok(frame)))
            }
            std::task::Poll::Ready(Some(Err(error))) => {
                this.observer.finish(
                    StatusCode::BAD_GATEWAY,
                    Some("upstream_stream_error".to_string()),
                );
                std::task::Poll::Ready(Some(Err(error)))
            }
            std::task::Poll::Ready(None) => {
                this.observer.finish(this.observer.status, None);
                std::task::Poll::Ready(None)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for UsageEventBody {
    fn drop(&mut self) {
        self.observer.finish(self.observer.status, None);
    }
}

struct UsageStreamObserver {
    state: Mutex<UsageStreamState>,
    ctx: UsageEventContext,
    publisher: Arc<dyn UsageEventPublisher>,
    status: StatusCode,
    emitted: AtomicBool,
}

#[derive(Default)]
struct UsageStreamState {
    decoder: SseDecoder,
    tokens: TokenInfo,
    response_model: Option<String>,
    captured_response: Vec<u8>,
    response_truncated: bool,
    first_token_at: Option<Instant>,
    last_token_at: Option<Instant>,
    inter_token_total: Duration,
    inter_token_count: u64,
}

impl UsageStreamObserver {
    fn new(
        ctx: UsageEventContext,
        publisher: Arc<dyn UsageEventPublisher>,
        status: StatusCode,
    ) -> Self {
        Self {
            state: Mutex::new(UsageStreamState::default()),
            ctx,
            publisher,
            status,
            emitted: AtomicBool::new(false),
        }
    }

    fn observe_data(&self, data: &Bytes) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if self.ctx.capture_response_body && self.ctx.body_capture_max_bytes > 0 {
            let remaining = self
                .ctx
                .body_capture_max_bytes
                .saturating_sub(state.captured_response.len());
            if remaining > 0 {
                let take = remaining.min(data.len());
                state.captured_response.extend_from_slice(&data[..take]);
            }
            if data.len() > remaining {
                state.response_truncated = true;
            }
        }
        if state.decoder.push(data).is_err() {
            return;
        }
        while let Some(frame) = state.decoder.next_frame() {
            let Ok(frame) = frame else {
                continue;
            };
            if frame.is_done() {
                continue;
            }
            let Ok(value) = frame.decode_data::<Value>() else {
                continue;
            };
            record_stream_value(&mut state, &value, self.ctx.started_at);
        }
        state.decoder.compact();
    }

    fn finish(&self, status: StatusCode, error_type: Option<String>) {
        if self.emitted.swap(true, Ordering::AcqRel) {
            return;
        }

        let Ok(state) = self.state.lock() else {
            return;
        };
        let ttft = state
            .first_token_at
            .map(|instant| instant - self.ctx.started_at);
        let inter_token_latency = if state.inter_token_count > 0 {
            Some(Duration::from_secs_f64(
                state.inter_token_total.as_secs_f64() / state.inter_token_count as f64,
            ))
        } else {
            None
        };
        let mut event = self.ctx.build_event(
            status,
            state.response_model.clone(),
            state.tokens.clone(),
            ttft,
            inter_token_latency,
            error_type.as_deref(),
        );
        if self.ctx.capture_response_body && !state.captured_response.is_empty() {
            self.ctx.attach_response_body(
                &mut event,
                CapturedBody {
                    value: String::from_utf8_lossy(&state.captured_response).into_owned(),
                    truncated: state.response_truncated,
                },
            );
        }
        self.publisher.publish(event);
    }
}

fn record_stream_value(state: &mut UsageStreamState, value: &Value, started_at: Instant) {
    if let Some(model) = value.get("model").and_then(Value::as_str) {
        state.response_model = Some(model.to_string());
    }
    if let Some(usage) = value.get("usage") {
        state.tokens = extract_usage_tokens(usage);
    }
    if stream_chunk_has_content(value) {
        let now = Instant::now();
        if let Some(last) = state.last_token_at {
            state.inter_token_total += now - last;
            state.inter_token_count += 1;
        } else {
            debug!(
                ttft_ms = (now - started_at).as_millis() as u64,
                "observed first streaming token"
            );
            state.first_token_at = Some(now);
        }
        state.last_token_at = Some(now);
    }
}

fn stream_chunk_has_content(value: &Value) -> bool {
    value
        .get("choices")
        .and_then(Value::as_array)
        .is_some_and(|choices| {
            choices.iter().any(|choice| {
                choice
                    .get("delta")
                    .or_else(|| choice.get("text"))
                    .is_some_and(|content| match content {
                        Value::String(text) => !text.is_empty(),
                        Value::Object(map) => !map.is_empty(),
                        _ => false,
                    })
            })
        })
}

pub fn extract_usage_from_json(body: &[u8]) -> (TokenInfo, Option<String>) {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return (TokenInfo::default(), None);
    };
    let response_model = value
        .get("model")
        .and_then(Value::as_str)
        .map(str::to_string);
    let tokens = value
        .get("usage")
        .map(extract_usage_tokens)
        .unwrap_or_default();
    (tokens, response_model)
}

fn extract_usage_tokens(usage: &Value) -> TokenInfo {
    let input_tokens = number_field(usage, &["input_tokens", "prompt_tokens"]);
    let output_tokens = number_field(usage, &["output_tokens", "completion_tokens"]);
    let total_tokens =
        number_field(usage, &["total_tokens"]).max(input_tokens.saturating_add(output_tokens));
    let cached_tokens = number_field(
        usage,
        &[
            "cached_tokens",
            "cached_input_tokens",
            "prompt_tokens_details.cached_tokens",
            "input_token_details.cached_tokens",
        ],
    );

    TokenInfo {
        input_tokens,
        output_tokens,
        total_tokens,
        cached_tokens: (cached_tokens > 0).then_some(cached_tokens),
        cached_input_tokens: (cached_tokens > 0).then_some(cached_tokens),
        cache_creation_input_tokens: None,
    }
}

fn number_field(value: &Value, paths: &[&str]) -> u64 {
    paths
        .iter()
        .find_map(|path| {
            path.split('.')
                .try_fold(value, |current, part| current.get(part))
                .and_then(Value::as_u64)
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue, StatusCode};
    use serde_json::json;

    fn header_keys() -> Vec<String> {
        vec![
            "x-project-id",
            "x-user-id",
            "x-api-key-id",
            "x-model-name",
            "x-ai-eg-model",
            "x-model-id",
            "x-input-price",
            "x-output-price",
            "x-is-free",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    }

    fn test_event() -> UsageEvent {
        UsageEventContext::unrouted(None, &[], "/generate", "model", false, Instant::now())
            .build_event(StatusCode::OK, None, TokenInfo::default(), None, None, None)
    }

    #[test]
    fn extracts_openai_usage_tokens() {
        let body = br#"{"model":"served","usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7,"prompt_tokens_details":{"cached_tokens":2}}}"#;
        let (tokens, model) = extract_usage_from_json(body);
        assert_eq!(model.as_deref(), Some("served"));
        assert_eq!(tokens.input_tokens, 3);
        assert_eq!(tokens.output_tokens, 4);
        assert_eq!(tokens.total_tokens, 7);
        assert_eq!(tokens.cached_tokens, Some(2));
    }

    #[test]
    fn filters_only_configured_x_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("x-project-id", HeaderValue::from_static("project-1"));
        headers.insert("x-user-id", HeaderValue::from_static("user-1"));
        headers.insert("x-secret-token", HeaderValue::from_static("secret"));
        headers.insert("authorization", HeaderValue::from_static("bearer token"));

        let filtered = filter_headers(Some(&headers), &header_keys());

        assert_eq!(
            filtered.get("x-project-id").map(String::as_str),
            Some("project-1")
        );
        assert_eq!(
            filtered.get("x-user-id").map(String::as_str),
            Some("user-1")
        );
        assert!(!filtered.contains_key("x-secret-token"));
        assert!(!filtered.contains_key("authorization"));
    }

    #[test]
    fn builds_usage_consumer_compatible_event_shape() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_static("req-1"));
        headers.insert("x-project-id", HeaderValue::from_static("project-1"));
        headers.insert("x-user-id", HeaderValue::from_static("user-1"));
        headers.insert("x-api-key-id", HeaderValue::from_static("api-key-1"));
        headers.insert("x-model-name", HeaderValue::from_static("catalog-model"));
        headers.insert("x-ai-eg-model", HeaderValue::from_static("virtual-model"));
        headers.insert("x-model-id", HeaderValue::from_static("model-uuid"));
        headers.insert("x-input-price", HeaderValue::from_static("1000"));
        headers.insert("x-output-price", HeaderValue::from_static("2000"));
        headers.insert("x-is-free", HeaderValue::from_static("false"));

        let ctx = UsageEventContext::unrouted(
            Some(&headers),
            &header_keys(),
            "/v1/chat/completions",
            "catalog-model",
            false,
            Instant::now(),
        );
        let event = ctx.build_event(
            StatusCode::OK,
            Some("served-model".to_string()),
            TokenInfo {
                input_tokens: 11,
                output_tokens: 12,
                total_tokens: 23,
                cached_tokens: Some(3),
                cached_input_tokens: Some(3),
                cache_creation_input_tokens: None,
            },
            None,
            None,
            None,
        );
        let value = serde_json::to_value(&event).expect("event should serialize");

        assert_eq!(value["event_type"], "request_completed");
        assert_eq!(value["request_id"], "req-1");
        assert_eq!(value["operation"], "chat");
        assert_eq!(value["original_model"], "catalog-model");
        assert_eq!(value["response_model"], "served-model");
        assert_eq!(value["success"], true);
        assert_eq!(value["tokens"]["input_tokens"], 11);
        assert_eq!(value["tokens"]["output_tokens"], 12);
        assert_eq!(value["tokens"]["total_tokens"], 23);
        assert_eq!(value["tokens"]["cached_tokens"], 3);
        assert_eq!(value["headers"]["x-project-id"], "project-1");
        assert_eq!(value["x-project-id"], "project-1");
        assert_eq!(value["x-model-name"], "catalog-model");

        let minimal_usage_consumer_view = json!({
            "request_id": value["request_id"],
            "original_model": value["original_model"],
            "success": value["success"],
            "latency_ms": value["latency_ms"],
            "time_to_first_token_ms": value.get("time_to_first_token_ms").cloned().unwrap_or_default(),
            "tokens": value["tokens"],
            "model_name_override": value["model_name_override"],
            "x-project-id": value["x-project-id"],
            "x-user-id": value["x-user-id"],
            "x-api-key-id": value["x-api-key-id"],
            "x-model-name": value["x-model-name"],
            "x-ai-eg-model": value["x-ai-eg-model"],
            "x-input-price": value["x-input-price"],
            "x-output-price": value["x-output-price"],
            "x-model-id": value["x-model-id"],
            "x-is-free": value["x-is-free"],
            "headers": value["headers"],
        });
        assert_eq!(minimal_usage_consumer_view["tokens"]["cached_tokens"], 3);
    }

    #[test]
    fn audit_body_capture_is_optional_and_truncated() {
        let request = capture_body(br#"{"prompt":"hello world"}"#, 12).expect("captured");
        assert_eq!(request.value, r#"{"prompt":"h"#);
        assert!(request.truncated);

        let ctx =
            UsageEventContext::unrouted(None, &[], "/generate", "model", false, Instant::now())
                .with_audit_capture(Some(request), true, 8);
        let mut event =
            ctx.build_event(StatusCode::OK, None, TokenInfo::default(), None, None, None);
        ctx.attach_response_body(
            &mut event,
            capture_body(br#"{"text":"answer"}"#, ctx.body_capture_max_bytes).expect("captured"),
        );

        assert_eq!(event.request_body.as_deref(), Some(r#"{"prompt":"h"#));
        assert_eq!(event.request_body_truncated, Some(true));
        assert_eq!(event.response_body.as_deref(), Some(r#"{"text":"#));
        assert_eq!(event.response_body_truncated, Some(true));
    }

    #[test]
    fn unsupported_sasl_mechanism_disables_kafka_creation() {
        let config = KafkaUsageConfig {
            brokers: vec!["localhost:9092".to_string()],
            sasl_user: Some("user".to_string()),
            sasl_password: Some("password".to_string()),
            sasl_mechanism: Some("GSSAPI".to_string()),
            ..KafkaUsageConfig::default()
        };

        assert!(KafkaUsageEventPublisher::new(&config).is_err());
    }

    #[test]
    fn default_config_uses_noop_publisher_path() {
        let config = crate::config::RouterConfig::default();
        assert!(!config.kafka_usage.is_enabled());

        let publisher = create_usage_event_publisher(&config);
        publisher.publish(test_event());
    }

    #[test]
    fn kafka_enqueue_failure_does_not_panic() {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let publisher = KafkaUsageEventPublisher { sender };

        publisher.publish(test_event());
    }
}
