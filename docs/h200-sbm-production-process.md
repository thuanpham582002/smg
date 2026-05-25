# H200 SBM Production Verification Process

This records the production verification flow for running SMG/SBM without AI Gateway or Gateway API.

## Objective

- Run SMG/SBM as the ingress-facing model router.
- Support weighted traffic split and sticky routing.
- Rewrite OpenAI request body `model` to the selected backend `served_model_name`.
- Emit AI-Gateway-compatible Kafka usage events, including prompt/response capture when enabled.
- Preserve model-registry accounting identity through request headers and consumed usage rows.

## Build

Local source was synced to H200 and built into the cluster registry:

```bash
rsync -rahPc --delete --exclude='.git/' --exclude='target/' --exclude='.DS_Store' \
  -e "ssh -J shared@171.253.168.88 -o StrictHostKeyChecking=accept-new" \
  ./ root@10.29.252.145:/mnt/models/thuanpt10/smg/
```

Dockerfile used on H200:

```bash
docker build -f docker/h200-sbm-smoke.Dockerfile \
  -t 10.29.252.145:5000/smg-sbm-test:20260523-weighted-sticky-cli .
```

Published image:

```text
10.29.252.145:5000/smg-sbm-test:20260523-weighted-sticky-cli
digest: sha256:49f76c2b0840f64c42420024c300aedb3dd7a85165101c463ab9986a773d6cc7
```

## Kubernetes Deployment

Namespace and service:

```text
namespace: sbm-smoke
service: sbm-smoke.sbm-smoke.svc.cluster.local:30000
```

Deployment args:

```yaml
args:
  - launch
  - --host
  - 0.0.0.0
  - --port
  - "30000"
  - --policy
  - weighted_sticky
  - --disable-health-check
```

Kafka usage event environment:

```yaml
KAFKA_BROKERS: ai-gateway-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092
KAFKA_TOPIC: ai-gateway-events
KAFKA_CAPTURE_REQUEST_BODY: "true"
KAFKA_CAPTURE_RESPONSE_BODY: "true"
KAFKA_BODY_CAPTURE_MAX_BYTES: "1024"
RUST_LOG: info
```

Patch command used:

```bash
kubectl --context kubernetes-admin@kubernetes -n sbm-smoke patch deploy sbm-smoke --type=json -p='[
  {"op":"replace","path":"/spec/template/spec/containers/0/image","value":"10.29.252.145:5000/smg-sbm-test:20260523-weighted-sticky-cli"},
  {"op":"replace","path":"/spec/template/spec/containers/0/args","value":["launch","--host","0.0.0.0","--port","30000","--policy","weighted_sticky","--disable-health-check"]}
]'
```

Current known state at interruption time:

```text
deployment.apps/sbm-smoke image: 10.29.252.145:5000/smg-sbm-test:20260523-weighted-sticky-cli
new pod: Running, readiness 503 until workers are registered
old pod: still serving previous single-backend setup
```

The readiness 503 is expected before workers are registered because `/readiness` requires routable workers.

## Backend Candidates

Validated candidates:

```text
http://benchmark-v2-proxy.benchmark-v2.svc.cluster.local:8000
served_model_name: gpt-oss-120b
status: /v1/models and chat completions worked

http://kimi-vllm-baseline.kimi-mooncake-bench.svc.cluster.local:8000
served_model_name: Kimi-K2.6
status: /v1/models worked; chat completion should be verified before final traffic split proof
```

Rejected candidate:

```text
http://ep-66c2dea2-vllm.ai-demo-project.svc.cluster.local:8000
status: connection failed from sbm-smoke
```

## Worker Registration

Register workers through the SMG admin API after the new pod is reachable:

```bash
kubectl --context kubernetes-admin@kubernetes -n sbm-smoke run register-workers-$(date +%s) \
  --rm -i --restart=Never --image=curlimages/curl:8.11.1 -- sh -lc '
curl -sS -X POST http://sbm-smoke.sbm-smoke.svc.cluster.local:30000/workers \
  -H content-type:application/json \
  -d "{\"url\":\"http://benchmark-v2-proxy.benchmark-v2.svc.cluster.local:8000\",\"runtime_type\":\"vllm\",\"worker_type\":\"regular\",\"routing_weight\":80,\"served_model_name\":\"gpt-oss-120b\",\"models\":[{\"id\":\"gpt-oss-120b\",\"aliases\":[\"sbm-public\"]}]}" ; echo
curl -sS -X POST http://sbm-smoke.sbm-smoke.svc.cluster.local:30000/workers \
  -H content-type:application/json \
  -d "{\"url\":\"http://kimi-vllm-baseline.kimi-mooncake-bench.svc.cluster.local:8000\",\"runtime_type\":\"vllm\",\"worker_type\":\"regular\",\"routing_weight\":20,\"served_model_name\":\"Kimi-K2.6\",\"models\":[{\"id\":\"Kimi-K2.6\",\"aliases\":[\"sbm-public\"]}]}" ; echo
sleep 3
curl -sS http://sbm-smoke.sbm-smoke.svc.cluster.local:30000/workers
'
```

If service routing still targets the old ready pod during rollout, register directly against the new pod IP until readiness flips.

## Request Headers For Accounting

Use these headers when sending model-registry-accounted requests:

```text
x-project-id: 11111111-1111-4111-8111-111111111111
x-user-id: 22222222-2222-4222-8222-222222222222
x-model-id: 077a0393-ceb6-418a-b83f-ca26470e25c0
x-model-name: aigw-smoke-mc-v2
x-ai-eg-model: sbm-public
x-input-price: 0
x-output-price: 0
x-is-free: true
```

## Verification Already Completed

Local SMG checks:

```bash
cargo check -p smg --bin smg --features vendored-openssl
cargo test -p smg weighted_sticky --lib
```

Both passed. Weighted sticky tests covered:

- stable sticky key selection
- unhealthy worker exclusion
- random fallback weighted distribution
- weighted sticky distribution over many keys

Single-backend H200 end-to-end check completed before the multi-backend rollout:

```text
Kafka request_id: 019e53ea-8351-7323-855d-ea6ca0070f6b
backend: SMG
backend_name: http://benchmark-v2-proxy.benchmark-v2.svc.cluster.local:8000
original_model: gpt-oss-120b
request_model: gpt-oss-120b
response_model: gpt-oss-120b
tokens: input=74 output=6 total=80 cached=0
request_body: present
response_body: present
headers: project, user, model identity, pricing/free headers present
```

Model Registry consumed and stored the row:

```text
request_id: 019e53ea-8351-7323-855d-ea6ca0070f6b
model_id: 077a0393-ceb6-418a-b83f-ca26470e25c0
model_name: aigw-smoke-mc-v2
virtual_model_name: gpt-oss-120b
input_tokens: 74
output_tokens: 6
cached_tokens: 0
```

Model Registry migration also verified on H200:

```text
schema_migrations version: 28
mc_usage_log.cached_tokens: exists
```

## Multi-backend Verification (2026-05-26)

Resumed the loop with two live backends (the previous "validated candidates" had been torn down):

```text
gpt-oss-120b   weight=80  http://gpt-oss-pd-lm-mooncake-frontend.kimi-mooncake-bench.svc.cluster.local:8000
Qwen3.5-9B     weight=20  http://qwen3-9b.qwen3-serve.svc.cluster.local:8000
```

### Blocker found and fixed

The first chat completion through `model=sbm-public` returned HTTP 400 from the backend:

```text
Validation: Unsupported parameter(s): no_stop_trim, return_hidden_states,
continue_final_message, separate_reasoning, stream_reasoning
```

Root cause: `ChatCompletionRequest` in `crates/protocols/src/chat.rs` re-serialized five
SGLang-only fields at their default values, and vLLM strict-rejects unknown parameters.

Fix: add `skip_serializing_if` to those five fields (and helpers `is_false` / `is_true` in
`crates/protocols/src/common.rs`). SGLang behavior is unchanged — SGLang already treats a
missing field and the default value identically.

Rebuilt image:

```text
10.29.252.145:5000/smg-sbm-test:20260526-20260526-035237-skip-sglang-fields
```

### Acceptance evidence

```text
GET  /readiness  ->  HTTP 200  {"status":"ready","healthy_workers":2,"total_workers":2}
POST /v1/chat/completions  with body model="sbm-public"  ->  HTTP 200
```

Traffic split (20 requests, no sticky header):

```text
gpt-oss-120b: 12   (60%)
Qwen3.5-9B:    8   (40%)
```

Sticky routing (3 calls per session id, 10 distinct sessions):

```text
s1..s8, s10 -> always gpt-oss-120b (3/3 each)
s9          -> always Qwen3.5-9B   (3/3)
```

Each session always landed on the same backend across repeated calls, and at least one
session routed to the minority-weight backend.

Kafka events on `ai-gateway-events` (sample request_id `019e60fd-30a0-7990-9643-6cd9ad9e8a9e`):

```text
original_model:      sbm-public                  (client-sent alias)
request_model:       Qwen3.5-9B                  (rewritten served_model_name)
response_model:      Qwen3.5-9B
backend:             SMG
selected_pool:       http://qwen3-9b.qwen3-serve.svc.cluster.local:8000
model_name_override: Qwen3.5-9B
tokens.input_tokens: 11
tokens.output_tokens: 1
request_body:        present (no_stop_trim et al. absent — fix confirmed on the wire)
response_body:       present
```

Both backends produced equivalent events with their own `model_name_override` and pool URL.

## Gaps still open

These are not blockers for the routing/rewrite/Kafka proof above but should be tracked:

- Model-registry accounting headers (`x-project-id`, `x-user-id`, `x-model-id`, `x-model-name`,
  `x-ai-eg-model`, `x-input-price`, `x-output-price`, `x-is-free`) were not exercised in the
  multi-backend batch. The single-backend run earlier this week did consume into `mc_usage_log`;
  the multi-backend variant still needs that loop closed end-to-end against the new image.
- `/metrics` scraping for routing / rewrite / Kafka counters not collected this round.
- The doc above lists `Worker Registration` against the K8s service — only safe once the pod
  is ready. During rollouts of the `weighted_sticky` deployment, register directly against the
  new pod IP (port-forward or pod exec) while the old pod still owns the service endpoint.

## External Authorization (ext-auth) middleware

SMG can call a remote ext-auth endpoint (e.g. Model Registry's `POST /ext-auth`) on every
protected inference request, matching the Envoy ext-authz contract previously implemented by
the AI Gateway. The middleware lives in `model_gateway/src/middleware/ext_auth.rs` and is
applied to `protected_routes` only (chat, completions, responses, embeddings, rerank, tokenize,
realtime REST). It is **off by default**; enable it by setting `--ext-auth-url` or the
`EXT_AUTH_URL` env var.

### Configuration

CLI flags (all also accept the matching env var):

| Flag | Env | Default | Meaning |
|------|-----|---------|---------|
| `--ext-auth-url` | `EXT_AUTH_URL` | unset | Fully-qualified ext-auth URL. Unset disables the middleware. |
| `--ext-auth-timeout-ms` | `EXT_AUTH_TIMEOUT_MS` | `500` | Per-call timeout on the ext-auth probe. |
| `--ext-auth-fail-open-on-transport-error` | `EXT_AUTH_FAIL_OPEN_ON_TRANSPORT_ERROR` | `false` | When `true`, transport/IO failures contacting ext-auth let the request through. When `false`, transport errors return `502`. |

The middleware forwards these inbound request headers to the ext-auth endpoint:

```
authorization, x-api-key, x-project-id, x-user-id,
x-ai-eg-model, x-region-id, region-id
```

On a non-2xx ext-auth response, SMG returns that status verbatim (with the upstream body) to
the client; the worker is not contacted.

On a 2xx ext-auth response, the following headers are copied from the ext-auth response into
the request that proceeds to the worker (Envoy ext-authz "additional headers" pattern), so
downstream Kafka usage events carry the resolved identity:

```
x-project-id, x-model-id, x-api-key-id, x-input-price, x-output-price,
x-model-name, x-ai-eg-model, x-ratelimit-remaining, x-credit-remaining, x-user-id
```

### Deployment example

To gate the H200 `sbm-smoke` deployment against Model Registry:

```bash
kubectl --context kubernetes-admin@kubernetes -n sbm-smoke set env deploy/sbm-smoke \
  EXT_AUTH_URL=http://mr-model-registry-service.demo-project.svc.cluster.local:8080/ext-auth \
  EXT_AUTH_TIMEOUT_MS=500
```

(or add the env entries to the Deployment spec). The pod must be running an image that
contains the ext-auth middleware (commit landing this section forward).

### Acceptance check

With the env var set and a chat completion call carrying valid identity headers:

```bash
curl -sS -X POST http://sbm-smoke.sbm-smoke.svc.cluster.local:30000/v1/chat/completions \
  -H 'authorization: Bearer <api-key>' \
  -H 'x-project-id: <project-uuid>' \
  -H 'x-ai-eg-model: sbm-public' \
  -H 'content-type: application/json' \
  -d '{"model":"sbm-public","messages":[{"role":"user","content":"hi"}],"max_tokens":4}'
```

Expected:

- If MR has the project + model alias seeded → HTTP 200, response model = backend served name,
  Kafka event contains the resolved `x-project-id` / `x-model-id` / `x-model-name`.
- If MR is missing the alias → SMG returns the MR `/ext-auth` 4xx verbatim (e.g.
  `HTTP 404 {"error":"model catalog: model not found"}`), worker is not contacted.
