//! Chat completion routing for the OpenAI router.

use std::{sync::Arc, time::Instant};

use axum::{
    body::Body,
    http::{header::CONTENT_TYPE, HeaderMap, HeaderValue, StatusCode},
    response::Response,
};
use futures_util::StreamExt;
use openai_protocol::chat::ChatCompletionRequest;
use serde_json::to_value;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::{
    context::{ComponentRefs, PayloadState, RequestContext, SharedComponents, WorkerSelection},
    provider::ProviderRegistry,
    router::resolve_provider,
};
use crate::{
    config::types::RetryConfig,
    middleware::TenantRequestMeta,
    observability::{
        metrics::{bool_to_static_str, metrics_labels, Metrics},
        usage_events::{
            capture_body, publish_non_streaming_response, wrap_streaming_response, TokenInfo,
            UsageEventContext, UsageEventPublisher,
        },
    },
    routers::{
        common::{
            header_utils::{apply_provider_headers, extract_auth_header},
            retry::{is_retryable_status, RetryExecutor},
            worker_selection::{SelectWorkerRequest, WorkerSelector},
        },
        error,
    },
    worker::{Endpoint, ProviderType, WorkerRegistry},
};

/// Shared context passed to chat routing functions.
pub(super) struct ChatRouterContext<'a> {
    pub worker_registry: &'a WorkerRegistry,
    pub provider_registry: &'a ProviderRegistry,
    pub shared_components: &'a Arc<SharedComponents>,
    pub retry_config: &'a RetryConfig,
    pub usage_event_publisher: &'a Arc<dyn UsageEventPublisher>,
    pub kafka_event_header_keys: &'a [String],
    pub kafka_capture_request_body: bool,
    pub kafka_capture_response_body: bool,
    pub kafka_body_capture_max_bytes: usize,
}

/// Route a chat completion request to the appropriate upstream worker.
pub(super) async fn route_chat(
    deps: &ChatRouterContext<'_>,
    headers: Option<&HeaderMap>,
    tenant_meta: &TenantRequestMeta,
    body: &ChatCompletionRequest,
    model_id: &str,
) -> Response {
    let start = Instant::now();
    let model = model_id;
    let streaming = body.stream;
    let request_body_capture = deps
        .kafka_capture_request_body
        .then(|| {
            serde_json::to_vec(body)
                .ok()
                .and_then(|body| capture_body(&body, deps.kafka_body_capture_max_bytes))
        })
        .flatten();

    Metrics::record_router_request(
        metrics_labels::ROUTER_OPENAI,
        metrics_labels::BACKEND_EXTERNAL,
        metrics_labels::CONNECTION_HTTP,
        model,
        metrics_labels::ENDPOINT_CHAT,
        bool_to_static_str(streaming),
    );

    let selector = WorkerSelector::new(deps.worker_registry, &deps.shared_components.client);
    let worker = match selector
        .select_worker(&SelectWorkerRequest {
            model_id: model,
            headers,
            provider: Some(ProviderType::OpenAI),
            ..Default::default()
        })
        .await
    {
        Ok(w) => w,
        Err(response) => {
            Metrics::record_router_error(
                metrics_labels::ROUTER_OPENAI,
                metrics_labels::BACKEND_EXTERNAL,
                metrics_labels::CONNECTION_HTTP,
                model,
                metrics_labels::ENDPOINT_CHAT,
                metrics_labels::ERROR_NO_WORKERS,
            );
            let usage_ctx = UsageEventContext::unrouted(
                headers,
                deps.kafka_event_header_keys,
                "/v1/chat/completions",
                model,
                streaming,
                start,
            );
            deps.usage_event_publisher.publish(usage_ctx.build_event(
                response.status(),
                None,
                TokenInfo::default(),
                None,
                None,
                Some("no_workers"),
            ));
            return response;
        }
    };

    let mut payload = match to_value(body) {
        Ok(v) => v,
        Err(e) => {
            Metrics::record_router_error(
                metrics_labels::ROUTER_OPENAI,
                metrics_labels::BACKEND_EXTERNAL,
                metrics_labels::CONNECTION_HTTP,
                model,
                metrics_labels::ENDPOINT_CHAT,
                metrics_labels::ERROR_VALIDATION,
            );
            let response = error::bad_request(
                "invalid_request",
                format!("Failed to serialize request: {e}"),
            );
            let usage_ctx = UsageEventContext::from_request(
                headers,
                deps.kafka_event_header_keys,
                "/v1/chat/completions",
                model,
                worker.as_ref(),
                streaming,
                start,
            );
            deps.usage_event_publisher.publish(usage_ctx.build_event(
                response.status(),
                None,
                TokenInfo::default(),
                None,
                None,
                Some("invalid_request"),
            ));
            return response;
        }
    };

    // Patch the serialized payload to use the effective model consistently.
    payload["model"] = serde_json::Value::String(model.to_owned());

    let provider = resolve_provider(deps.provider_registry, worker.as_ref(), model);
    if let Err(e) = provider.transform_request(&mut payload, Endpoint::Chat) {
        Metrics::record_router_error(
            metrics_labels::ROUTER_OPENAI,
            metrics_labels::BACKEND_EXTERNAL,
            metrics_labels::CONNECTION_HTTP,
            model,
            metrics_labels::ENDPOINT_CHAT,
            metrics_labels::ERROR_VALIDATION,
        );
        let response =
            error::bad_request("invalid_request", format!("Provider transform error: {e}"));
        let usage_ctx = UsageEventContext::from_request(
            headers,
            deps.kafka_event_header_keys,
            "/v1/chat/completions",
            model,
            worker.as_ref(),
            streaming,
            start,
        );
        deps.usage_event_publisher.publish(usage_ctx.build_event(
            response.status(),
            None,
            TokenInfo::default(),
            None,
            None,
            Some("invalid_request"),
        ));
        return response;
    }

    let usage_ctx = UsageEventContext::from_request(
        headers,
        deps.kafka_event_header_keys,
        "/v1/chat/completions",
        model,
        worker.as_ref(),
        streaming,
        start,
    )
    .with_audit_capture(
        request_body_capture,
        deps.kafka_capture_response_body,
        deps.kafka_body_capture_max_bytes,
    );

    let mut ctx = RequestContext::for_chat(
        Arc::new(body.clone()),
        headers.cloned(),
        Some(model_id.to_string()),
        ComponentRefs::Shared(Arc::clone(deps.shared_components)),
    );
    ctx.tenant_request_meta = Some(tenant_meta.clone());

    ctx.state.worker = Some(WorkerSelection {
        worker: Arc::clone(&worker),
        provider,
    });

    let url = format!("{}/v1/chat/completions", worker.url());
    ctx.state.payload = Some(PayloadState {
        json: payload,
        url: url.clone(),
    });

    // Wrap values in Arc to avoid cloning large objects on each retry attempt
    #[expect(
        clippy::expect_used,
        reason = "payload is set earlier in this function; absence is a logic error"
    )]
    let payload_ref = ctx.payload().expect("Payload not prepared");
    let payload_json = Arc::new(payload_ref.json.clone());
    let client = ctx.components.client().clone();
    let headers_cloned = Arc::new(ctx.headers().cloned());
    let worker_api_key = Arc::new(worker.api_key().cloned());
    let is_streaming = ctx.is_streaming();

    let response = RetryExecutor::execute_response_with_retry(
        deps.retry_config,
        |_attempt| {
            let client = client.clone();
            let url = url.clone();
            let payload = Arc::clone(&payload_json);
            let headers = Arc::clone(&headers_cloned);
            let worker_api_key = Arc::clone(&worker_api_key);
            let worker = Arc::clone(&worker);

            async move {
                let mut req = client.post(&url).json(&*payload);
                let auth_header =
                    extract_auth_header((*headers).as_ref(), (*worker_api_key).as_ref());
                req = apply_provider_headers(req, &url, auth_header.as_ref());

                if is_streaming {
                    req = req.header("Accept", "text/event-stream");
                }

                let resp = match req.send().await {
                    Ok(r) => r,
                    Err(e) => {
                        worker.record_outcome(503);
                        return error::service_unavailable(
                            "upstream_error",
                            format!("Failed to contact upstream: {e}"),
                        );
                    }
                };

                let status = StatusCode::from_u16(resp.status().as_u16())
                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

                // Record CB outcome based on HTTP status.
                // For streaming: status is known upfront (200 = success).
                // For non-streaming: we record here too — body read errors
                // are connection issues, not worker health issues.
                worker.record_outcome(status.as_u16());

                if is_streaming {
                    let stream = resp.bytes_stream();
                    let (tx, rx) = mpsc::unbounded_channel();
                    #[expect(clippy::disallowed_methods, reason = "fire-and-forget stream relay; gateway shutdown need not wait for individual stream forwarding")]
                    tokio::spawn(async move {
                        let mut s = stream;
                        while let Some(chunk) = s.next().await {
                            match chunk {
                                Ok(bytes) => {
                                    if tx.send(Ok(bytes)).is_err() {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    let _ = tx.send(Err(format!("Stream error: {e}")));
                                    break;
                                }
                            }
                        }
                    });
                    let mut response =
                        Response::new(Body::from_stream(UnboundedReceiverStream::new(rx)));
                    *response.status_mut() = status;
                    response
                        .headers_mut()
                        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
                    response
                } else {
                    let content_type = resp.headers().get(CONTENT_TYPE).cloned();
                    match resp.bytes().await {
                        Ok(body) => {
                            let mut response = Response::new(Body::from(body));
                            *response.status_mut() = status;
                            if let Some(ct) = content_type {
                                response.headers_mut().insert(CONTENT_TYPE, ct);
                            }
                            response
                        }
                        Err(e) => {
                            error::internal_error(
                                "upstream_error",
                                format!("Failed to read response: {e}"),
                            )
                        }
                    }
                }
            }
        },
        |res, _attempt| is_retryable_status(res.status()),
        |delay, attempt| {
            Metrics::record_worker_retry(
                metrics_labels::BACKEND_EXTERNAL,
                metrics_labels::ENDPOINT_CHAT,
            );
            Metrics::record_worker_retry_backoff(attempt, delay);
        },
        || {
            Metrics::record_worker_retries_exhausted(
                metrics_labels::BACKEND_EXTERNAL,
                metrics_labels::ENDPOINT_CHAT,
            );
        },
    )
    .await;

    if response.status().is_success() {
        Metrics::record_router_duration(
            metrics_labels::ROUTER_OPENAI,
            metrics_labels::BACKEND_EXTERNAL,
            metrics_labels::CONNECTION_HTTP,
            model,
            metrics_labels::ENDPOINT_CHAT,
            start.elapsed(),
        );
    } else {
        Metrics::record_router_error(
            metrics_labels::ROUTER_OPENAI,
            metrics_labels::BACKEND_EXTERNAL,
            metrics_labels::CONNECTION_HTTP,
            model,
            metrics_labels::ENDPOINT_CHAT,
            metrics_labels::ERROR_BACKEND,
        );
    }

    if streaming {
        wrap_streaming_response(response, usage_ctx, deps.usage_event_publisher.clone())
    } else {
        publish_non_streaming_response(response, usage_ctx, deps.usage_event_publisher.clone())
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::body::to_bytes;
    use openai_protocol::{
        chat::{ChatCompletionRequest, ChatMessage, MessageContent},
        model_card::ModelCard,
        worker::{HealthCheckConfig, ProviderType},
    };

    use super::*;
    use crate::{
        config::types::{PolicyConfig, RetryConfig, RouterConfig},
        middleware::{RouteRequestMeta, TenantKey},
        observability::usage_events::{UsageEvent, UsageEventPublisher},
        worker::{BasicWorkerBuilder, WorkerType},
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

    fn test_tenant_meta() -> RouteRequestMeta {
        RouteRequestMeta::new(TenantKey::from("test-tenant"))
    }

    fn test_chat_request(stream: bool) -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: "gpt-public".to_string(),
            messages: vec![ChatMessage::User {
                content: MessageContent::Text("hello".to_string()),
                name: None,
            }],
            stream,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn openai_chat_non_streaming_publishes_usage_event() {
        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(|| async {
                axum::Json(serde_json::json!({
                    "id": "chatcmpl-test",
                    "object": "chat.completion",
                    "model": "llama-real",
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 4, "completion_tokens": 6, "total_tokens": 10}
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let worker_registry = WorkerRegistry::new();
        let worker = BasicWorkerBuilder::new(backend_url)
            .worker_type(WorkerType::Regular)
            .provider(ProviderType::OpenAI)
            .model(ModelCard::new("gpt-public"))
            .served_model_name("llama-real")
            .health_config(no_health_check())
            .build();
        worker_registry.register_or_replace(Arc::new(worker));

        let router_config = Arc::new(RouterConfig {
            policy: PolicyConfig::RoundRobin,
            ..Default::default()
        });
        let shared_components = Arc::new(SharedComponents {
            client: reqwest::Client::new(),
            router_config,
        });
        let publisher = CapturingUsageEventPublisher::default();
        let publisher_arc: Arc<dyn UsageEventPublisher> = Arc::new(publisher.clone());
        let header_keys = vec!["x-project-id".to_string(), "x-model-id".to_string()];
        let deps = ChatRouterContext {
            worker_registry: &worker_registry,
            provider_registry: &ProviderRegistry::new(),
            shared_components: &shared_components,
            retry_config: &RetryConfig::default(),
            usage_event_publisher: &publisher_arc,
            kafka_event_header_keys: &header_keys,
            kafka_capture_request_body: false,
            kafka_capture_response_body: false,
            kafka_body_capture_max_bytes: 8192,
        };
        let mut headers = HeaderMap::new();
        headers.insert("x-project-id", HeaderValue::from_static("project-1"));
        headers.insert("x-model-id", HeaderValue::from_static("model-1"));

        let response = route_chat(
            &deps,
            Some(&headers),
            &test_tenant_meta(),
            &test_chat_request(false),
            "gpt-public",
        )
        .await;
        assert!(response.status().is_success());
        let _body = to_bytes(response.into_body(), usize::MAX).await.unwrap();

        let events = publisher.events();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.operation, "chat");
        assert_eq!(event.original_model, "gpt-public");
        assert_eq!(event.request_model, "llama-real");
        assert_eq!(event.response_model, "llama-real");
        assert_eq!(event.model_name_override, "llama-real");
        assert_eq!(event.tokens.input_tokens, 4);
        assert_eq!(event.tokens.output_tokens, 6);
        assert_eq!(event.tokens.total_tokens, 10);
        assert_eq!(event.x_project_id.as_deref(), Some("project-1"));
        assert_eq!(event.x_model_id.as_deref(), Some("model-1"));
    }

    #[tokio::test]
    async fn openai_chat_no_worker_publishes_failure_event() {
        let worker_registry = WorkerRegistry::new();
        let router_config = Arc::new(RouterConfig::default());
        let shared_components = Arc::new(SharedComponents {
            client: reqwest::Client::new(),
            router_config,
        });
        let publisher = CapturingUsageEventPublisher::default();
        let publisher_arc: Arc<dyn UsageEventPublisher> = Arc::new(publisher.clone());
        let deps = ChatRouterContext {
            worker_registry: &worker_registry,
            provider_registry: &ProviderRegistry::new(),
            shared_components: &shared_components,
            retry_config: &RetryConfig::default(),
            usage_event_publisher: &publisher_arc,
            kafka_event_header_keys: &[],
            kafka_capture_request_body: false,
            kafka_capture_response_body: false,
            kafka_body_capture_max_bytes: 8192,
        };

        let response = route_chat(
            &deps,
            None,
            &test_tenant_meta(),
            &test_chat_request(false),
            "missing-model",
        )
        .await;
        assert!(!response.status().is_success());

        let events = publisher.events();
        assert_eq!(events.len(), 1);
        assert!(!events[0].success);
        assert_eq!(events[0].error_type, "no_workers");
        assert_eq!(events[0].original_model, "missing-model");
    }
}
