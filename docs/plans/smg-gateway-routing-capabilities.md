# SMG gateway: pathmatch / regional routing / split-traffic — capability audit + plan

Status: AUDIT COMPLETE (verified against code). Implementation NOT started — awaiting
go-ahead + priority pick.

CRD in question: `deploy/helm/smg/crds/smgworkers.yaml` (`SmgWorker`, group
`smg.lightseek.io/v1alpha1`). It registers ONE backend worker. spec has
`x-kubernetes-preserve-unknown-fields: true`, so new fields don't require a CRD
schema bump (but the Rust `WorkerSpec` in `crates/protocols/src/worker.rs` must
learn them to be honored).

## What exists today (verified)

Worker selection path (HTTP): `routers/http/router.rs::select_worker_for_model`
-> `worker_registry.get_workers_filtered(model, WorkerType, ConnectionMode, runtime, …)`
-> filter `is_available()` -> `policy_registry.get_policy_or_default(model_id)`
-> `policy.select_worker(&available, &SelectWorkerInfo{ request_text, tokens, headers, hash_ring, leg })`.

`SelectWorkerInfo` ALREADY carries `headers` (so `x-region-id` is reachable inside
a policy). `WorkerSpec` (crates/protocols/src/worker.rs) has: url, models,
served_model_name, worker_type, connection_mode, runtime_type, provider,
**labels (HashMap)**, **priority (u32)**, **cost (f32)**, **routing_weight (u32)**,
api_key, bootstrap_*, dp_*, kv_*. NO dedicated `region` field — "region"/"zone"
appear only as ad-hoc labels in test code (worker/builder.rs:399, worker.rs:1529).

### (a) pathmatch — ❌ NOT supported
Routes are a FIXED OpenAI set registered in `server.rs`
(`/v1/chat/completions`, `/v1/completions`, `/v1/embeddings`, `/v1/rerank`,
`/v1/responses`, `/v1/messages`, `/v1/classify`, `/v1/audio/transcriptions`, …).
Routing key is the MODEL id (`route_typed_request(headers, body, "<path>", model_id)`),
NOT the URL path. There is no per-worker path/prefix field and no path-prefix match.
"pathmatch" in the Gateway-API sense does not exist here.

### (b) regional — ⚠️ AUTH-ONLY, no region-aware routing
`middleware/ext_auth.rs` forwards `x-region-id`/`region-id` to the ext-auth
endpoint (FORWARD_HEADERS) — so you can AUTHORIZE by region. But no policy
filters/prefers workers by region; `region` is not a first-class worker field.

### (c) split traffic — ✅ SUPPORTED today
`WorkerSpec.routing_weight` -> `policies/weighted_sticky.rs::WeightedStickyPolicy`
(`select_weighted_random` proportional to `worker.metadata().spec.routing_weight`),
wired in `PolicyFactory` and selectable per-model via the policy registry
(`policy_hint` label `policy` or default). PD prefill/decode split also exists
(`worker_type` + `pd_router`). Weighted A/B / canary split across workers works now.

## Plan (do in this order; each ships independently)

### 1. Regional routing (smallest, highest value, plumbing already present)
Goal: prefer same-region workers, fall back cross-region when none available.
- `crates/protocols/src/worker.rs`: add `pub region: Option<String>` to WorkerSpec
  (`#[serde(default)]`). Keep reading the `region` label as a fallback for back-compat.
  CRD: add an optional `region: {type: string}` to smgworkers.yaml (preserve-unknown
  already lets it through, but make it explicit + documented).
- New `policies/region_aware.rs`: a DECORATOR policy wrapping an inner policy.
  `select_worker`: read client region from `info.headers` (`x-region-id`), partition
  `available` into same-region vs rest; run the inner policy on same-region first;
  if empty, run on the full set (graceful fallback). This composes with weighted/
  cache-aware/etc. instead of duplicating them.
- Wire: `PolicyConfig::RegionAware { inner: Box<PolicyConfig> }` + factory arm +
  name "region_aware". Per-model opt-in via existing policy_hint mechanism.
- Tests: same-region preferred; cross-region fallback when same-region all
  unavailable; no-region-header -> behaves exactly like inner policy.
- Risk: LOW. Additive field + decorator; no change to existing policies or routes.

### 2. (Optional) region as a hard constraint vs soft preference
Decide with user: soft preference (plan 1) vs hard pin (reject if no same-region
worker). Hard pin = a `get_workers_filtered` region arg + 503 when empty. Only build
if the product needs isolation guarantees, not just locality.

### 3. pathmatch — only if genuinely needed (biggest lift, design tension)
SMG routes by model id, not URL path — path-based routing cuts against that model.
If the real need is "different backends for different API surfaces", that's already
expressible via model-id routing + the fixed OpenAI route set. Recommend NOT building
arbitrary pathmatch unless there's a concrete requirement the model-id model can't
meet; if so, scope it as: optional `path_prefix` on WorkerSpec + a prefix-match layer
in `server.rs` BEFORE model resolution. Confirm the requirement before any code.

## CLUSTER-VERIFIED FINDING (user clarified: "regional" = the regional AUTH service, not region routing)

Verified on the live cluster (context `ai-infer-factory`):
- The reference (Envoy AI Gateway) path: per-tenant `SecurityPolicy` named `inference-auth`
  (e.g. ns `aip-5f62...`) with `spec.extAuth` -> backendRef Service `regional-auth-backend:8080`
  path `/ext-auth`, targetRefs = the tenant's HTTPRoutes. `regional-auth-backend` is a
  selectorless Service whose endpoint is `regional-auth-service.model-catalog` (image
  `10.24.10.16:5000/regional-auth-service:...`, 3/3 running). That service IS the regional
  auth service. mr-service materializes the SecurityPolicy via
  routesecurity/reconciler.go::EnsureRouteAuth.
- SMG ALREADY HAS ext-auth support BUILT IN. `model_gateway/src/middleware/ext_auth.rs`
  (forwards Authorization/x-api-key/x-region-id/... to the ext-auth endpoint, injects
  x-project-id/x-model-id/pricing/etc back), plus Helm: `router.extAuth.{url,timeoutMs,
  failOpenOnTransportError}` -> EXT_AUTH_URL env (deployment-router.yaml + configmap-router.yaml).
  values.yaml example URL literally: `http://regional-auth.default.svc.cluster.local:8080/ext-auth`.

=> CONCLUSION: regional auth is NOT missing in SMG. SMG does ext-auth IN-PROCESS (Rust
   middleware on the request hot path) whereas the Envoy AI Gateway does it via a
   SecurityPolicy CRD pointing at the same regional-auth-service /ext-auth. Same auth
   service, two different enforcement points. To make SMG behave like the AIGatewayRoute
   regional auth, you DON'T write new behavior — you POINT SMG at the existing endpoint:
   set `router.extAuth.url = http://regional-auth-service.model-catalog.svc.cluster.local:8080/ext-auth`
   (+ confirm the header contract matches: SMG forwards a fixed FORWARD_HEADERS list and
   injects INJECT_HEADERS; regional-auth-service expects/returns the X-* set seen in the
   live SecurityPolicy headersToBackend). Gap to verify: SMG's hardcoded header lists vs
   the SecurityPolicy's headersToExtAuth/headersToBackend (e.g. X-Maas-Concurrency-Slot,
   X-Credit-Remaining, X-Is-Free) — add any missing ones to ext_auth.rs FORWARD/INJECT.

## SMG worker/gateway model vs HTTPRoute / AIGatewayRoute (verified)

SMG architecture (model_gateway/src):
- **SmgWorker CRD** -> `service_discovery.rs` watches `smg.lightseek.io/smgworkers` (RBAC: role.yaml get/list/watch smgworkers + pods). `crd_workers: bool` enables it.
  `handle_crd_worker_apply` reads `spec.worker.{url,models,...}` -> submits a worker-add job
  -> `WorkerRegistry`. Also watches Pods directly (handle_pod_event) as a second discovery source.
- **Gateway (router)** = the running SMG process. Inbound request -> fixed OpenAI routes
  (server.rs) -> `select_worker_for_model(model_id, headers)` -> `get_workers_filtered` ->
  per-model policy `select_worker`. ext_auth.rs middleware runs IN-PROCESS on the hot path.

Mapping to the Envoy/K8s model the rest of the platform uses:

| Concept                 | Envoy AI Gateway / Gateway API        | SMG equivalent |
|-------------------------|----------------------------------------|----------------|
| Backend target          | AIServiceBackend / Backend / Service   | **SmgWorker** (url+models) registered in WorkerRegistry |
| Route + match           | HTTPRoute (path/header match)          | fixed OpenAI routes + **model-id** match (no path/header rule CRD) |
| Model routing           | AIGatewayRoute (x-ai-eg-model header)  | `select_worker_for_model(model_id,...)` per-model policy |
| Traffic split / weight  | backendRefs[].weight                   | `routing_weight` -> WeightedStickyPolicy |
| Regional auth           | SecurityPolicy.extAuth -> regional-auth-service | ext_auth.rs middleware -> EXT_AUTH_URL (SAME regional-auth-service) |
| Attach point            | targetRefs -> Gateway/HTTPRoute        | the SMG router process itself |

So SmgWorker ≈ "the backend" and the SMG router process ≈ "HTTPRoute+AIGatewayRoute+SecurityPolicy
 collapsed into one in-process gateway". The behaviors are the SAME set; they live in code/config
 inside SMG instead of as separate K8s CRDs.

## DONE this session (wiring logic in the gateway)
- ext_auth.rs FORWARD_HEADERS + INJECT_HEADERS aligned to the live SecurityPolicy contract
  (model-registry-service routesecurity.{requestHeadersToExtAuth,responseHeadersToBackend}):
  added x-request-id to FORWARD; added authorization/x-ai-eg-model/x-request-id/x-is-free/
  x-maas-concurrency-slot/x-maas-model-concurrency-slot to INJECT and FIXED the wrong
  x-ratelimit-remaining -> x-rate-limit-remaining. `cargo check -p smg` GREEN. (Without this,
  free-tier/credit/rate-limit/MaaS-concurrency enforcement silently no-ops through SMG, and
  the Kafka usage pipeline — default_kafka_usage_header_keys expects x-is-free — loses data.)
- TODO (config, per-cluster, not code): set router.extAuth.url =
  http://regional-auth-service.model-catalog.svc.cluster.local:8080/ext-auth in SMG Helm values.

## Recommendation to user
- Split traffic: already done — nothing to build; document `routing_weight` usage.
- Regional: build plan 1 (region-aware decorator policy). Confirm soft-preference vs
  hard-pin.
- Pathmatch: do NOT build speculatively; confirm the concrete need first (likely
  already covered by model-id routing).
