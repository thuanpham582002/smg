//! External authorization middleware.
//!
//! When configured, every protected inference request is gated by a POST to a
//! remote ext-auth endpoint (e.g. Model Registry's `/ext-auth`). The middleware
//! forwards identity headers, surfaces the upstream status verbatim on rejection,
//! and copies allow-listed response headers from the auth response into the
//! request that proceeds to the chosen worker — matching the Envoy ext-authz
//! contract used by the AI Gateway.

use std::time::Duration;

use axum::{
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use reqwest::Client;
use tracing::{debug, warn};

const DEFAULT_TIMEOUT_MS: u64 = 500;

/// The model-selector header authz resolves identity/region from. OpenAI-SDK
/// clients carry the model only in the JSON body, so when this header is absent
/// the middleware synthesizes it from `body.model` before calling authz —
/// otherwise authz has no model to resolve and rejects the request.
const MODEL_HEADER: &str = "x-ai-eg-model";

/// Upper bound on the body buffered solely to extract `body.model` when the
/// model header is missing. The real payload limit is enforced downstream by
/// the DefaultBodyLimit layer; this only guards the synthesis read.
const MAX_MODEL_PROBE_BYTES: usize = 1024 * 1024;

/// HTTP headers forwarded from the inbound request to the ext-auth endpoint.
/// The ext-auth handler reads identity solely from headers (body is empty),
/// so this list is the input contract. Kept in lockstep with
/// model-registry-service routesecurity.requestHeadersToExtAuth (the
/// SecurityPolicy.spec.extAuth.headersToExtAuth the Envoy AI Gateway sends to
/// the SAME regional-auth-service /ext-auth): Authorization, X-AI-EG-Model,
/// X-Request-ID, X-Project-ID, X-User-ID. x-api-key / x-region-id / region-id
/// are SMG supersets the handler ignores when absent — harmless to forward.
const FORWARD_HEADERS: &[&str] = &[
    "authorization",
    "x-ai-eg-model",
    "x-request-id",
    "x-project-id",
    "x-user-id",
    "x-api-key",
    "x-region-id",
    "region-id",
];

/// HTTP headers copied from the ext-auth response onto the request that proceeds
/// to the worker. These carry the resolved tenant / pricing / concurrency
/// context downstream Kafka usage events and the rate/credit/concurrency
/// enforcement depend on. Kept in lockstep with model-registry-service
/// routesecurity.responseHeadersToBackend (the SecurityPolicy
/// headersToBackend the Envoy AI Gateway path injects) so SMG's in-process
/// ext-auth enforces the SAME contract: missing entries here silently drop
/// free-tier (x-is-free), credit (x-credit-remaining), rate-limit, and MaaS
/// concurrency-slot enforcement.
const INJECT_HEADERS: &[&str] = &[
    "authorization",
    "x-ai-eg-model",
    "x-request-id",
    "x-api-key-id",
    "x-project-id",
    "x-model-id",
    "x-model-name",
    "x-input-price",
    "x-output-price",
    "x-credit-remaining",
    "x-is-free",
    "x-rate-limit-remaining",
    "x-user-id",
    "x-maas-concurrency-slot",
    "x-maas-model-concurrency-slot",
];

/// Static config for the ext-auth middleware. Lives in [`ServerConfig`].
#[derive(Clone, Debug)]
pub struct ExtAuthConfig {
    /// Fully-qualified URL of the ext-auth endpoint, e.g.
    /// `http://mr-model-registry-service.demo-project.svc.cluster.local:8080/ext-auth`.
    /// When `None` the middleware is a no-op pass-through.
    pub url: Option<String>,
    /// Per-call timeout. Defaults to 500 ms — ext-auth is on the request hot path.
    pub timeout_ms: u64,
    /// When `true`, a transport/IO failure calling ext-auth lets the request through
    /// (fail-open). When `false` (default), a transport failure returns 502.
    pub fail_open_on_transport_error: bool,
}

impl ExtAuthConfig {
    pub fn new(url: Option<String>) -> Self {
        Self {
            url,
            timeout_ms: DEFAULT_TIMEOUT_MS,
            fail_open_on_transport_error: false,
        }
    }

    pub fn with_timeout_ms(mut self, ms: u64) -> Self {
        self.timeout_ms = ms;
        self
    }

    pub fn with_fail_open_on_transport_error(mut self, fail_open: bool) -> Self {
        self.fail_open_on_transport_error = fail_open;
        self
    }

    pub fn is_enabled(&self) -> bool {
        self.url.is_some()
    }
}

/// Runtime state: a shared reqwest client + the resolved config.
#[derive(Clone)]
pub struct ExtAuthState {
    config: ExtAuthConfig,
    client: Client,
}

impl ExtAuthState {
    pub fn try_init(config: ExtAuthConfig) -> Option<Self> {
        if !config.is_enabled() {
            return None;
        }
        let client = Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .build()
            .ok()?;
        Some(Self { config, client })
    }
}

pub async fn ext_auth_middleware(
    State(state): State<ExtAuthState>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let Some(url) = state.config.url.as_deref() else {
        return next.run(request).await;
    };

    // SDK clients send the model only in the body; authz reads it from a header.
    // When the header is absent, buffer the body to recover body.model and feed
    // it to authz. The synthesized value is forwarded to authz ONLY — it must
    // not persist on the request, because authz RESOLVES the model (alias +
    // region) and returns the routing model in its own x-ai-eg-model response
    // header, which copy_inject_headers then lands (it skips headers already
    // present, so a persisted synthesis would shadow the resolved value).
    let mut synthesized_model: Option<String> = None;
    if !request.headers().contains_key(MODEL_HEADER) {
        let (req, model) = match recover_body_model(request).await {
            Ok(pair) => pair,
            Err(resp) => return resp,
        };
        request = req;
        synthesized_model = model;
    }

    let mut req_builder = state.client.post(url);
    let inbound_headers = request.headers().clone();
    for name in FORWARD_HEADERS {
        if let Some(value) = inbound_headers.get(*name) {
            if let Ok(s) = value.to_str() {
                req_builder = req_builder.header(*name, s);
            }
        }
    }
    if let Some(model) = synthesized_model.as_deref() {
        req_builder = req_builder.header(MODEL_HEADER, model);
    }

    let resp = match req_builder.send().await {
        Ok(r) => r,
        Err(err) => {
            warn!(error = %err, url = url, "ext-auth transport error");
            if state.config.fail_open_on_transport_error {
                return next.run(request).await;
            }
            return (
                StatusCode::BAD_GATEWAY,
                format!("ext-auth transport error: {err}"),
            )
                .into_response();
        }
    };

    let status = resp.status();
    let resp_headers = resp.headers().clone();
    let body_bytes = resp.bytes().await.unwrap_or_default();

    if !status.is_success() {
        debug!(
            status = status.as_u16(),
            "ext-auth rejected request, surfacing upstream status"
        );
        let mut response = (
            StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::UNAUTHORIZED),
            body_bytes,
        )
            .into_response();
        copy_inject_headers(&resp_headers, response.headers_mut());
        return response;
    }

    copy_inject_headers(&resp_headers, request.headers_mut());
    next.run(request).await
}

/// Buffers the request body and extracts `model` from the JSON payload so the
/// caller can forward it to authz. The body is always reattached unchanged — a
/// parse miss or non-JSON payload yields `None`, never a failure, so
/// non-inference and malformed requests fall through untouched.
async fn recover_body_model(
    request: Request<Body>,
) -> Result<(Request<Body>, Option<String>), Response> {
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, MAX_MODEL_PROBE_BYTES).await {
        Ok(b) => b,
        Err(err) => {
            warn!(error = %err, "ext-auth: failed to buffer body for model recovery");
            return Err((StatusCode::BAD_REQUEST, "failed to read request body").into_response());
        }
    };

    let model = serde_json::from_slice::<ModelProbe>(&bytes)
        .ok()
        .and_then(|p| p.model)
        .map(|m| m.trim().to_string())
        .filter(|m| !m.is_empty());

    Ok((Request::from_parts(parts, Body::from(bytes)), model))
}

#[derive(serde::Deserialize)]
struct ModelProbe {
    model: Option<String>,
}

fn copy_inject_headers(src: &HeaderMap, dst: &mut HeaderMap) {
    for name in INJECT_HEADERS {
        if dst.contains_key(*name) {
            continue;
        }

        if let Some(value) = src.get(*name) {
            if let (Ok(header_name), Ok(header_value)) = (
                HeaderName::from_bytes(name.as_bytes()),
                HeaderValue::from_bytes(value.as_bytes()),
            ) {
                dst.insert(header_name, header_value);
            }
        }
    }
}
