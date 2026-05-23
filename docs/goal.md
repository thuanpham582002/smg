# Goal: Extend SMG for model-aware production routing

Implement a production-ready routing extension in SMG that does not depend on AI Gateway or Gateway API.

## Context

SMG is the chosen Rust-based model gateway/router. The desired deployment shape is:

- External Ingress or LoadBalancer sends OpenAI-compatible traffic to SMG.
- SMG owns backend selection, traffic split, sticky routing, model-name abstraction, request rewrite, auth extension, health, and metrics.
- Backend runtimes can expose different real served model names.
- Clients should keep using a stable public model name.

This must solve the concrete problem:

```text
client body.model = public model name
SMG selects backend by route policy
SMG rewrites body.model = backend served_model_name
SMG forwards request to selected backend runtime
```

Do not rely on Envoy, NGINX, HAProxy, ConfigMap routing, Gateway API filters, or AI Gateway specific behavior for request body rewrite.

## Success Criteria

1. SMG supports weighted traffic split between backend variants.
2. SMG supports sticky routing when a stable key is available.
3. SMG supports statistical weighted routing when no sticky key is available.
4. SMG can rewrite OpenAI request body `model` per selected backend before forwarding.
5. SMG can expose a public model name while forwarding the backend-specific served model name.
6. SMG can integrate third-party auth before routing.
7. SMG can discover Kubernetes backend pods or services without hardcoding pod IPs.
8. Tests and docs cover the new behavior.

## Required Routing Behavior

Add a routing policy equivalent to `weighted_sticky`.

Behavior:

- If a sticky key exists, route deterministically using weighted rendezvous hashing or another stable weighted hashing algorithm.
- If no sticky key exists, route statistically using weighted random selection.
- Respect backend health. Do not route to unhealthy backends.
- Keep traffic distribution close to configured weights over a large sample.
- Sticky routing must remain stable while the backend set and weights are unchanged.

Sticky key priority:

1. `X-SMG-Routing-Key`
2. `X-Session-ID`
3. `X-User-ID`
4. Cookie-based session key if already available in SMG config
5. Authorization subject if auth middleware extracts one

Do not use request body `model` as the sticky key.

## Required Model Abstraction

Introduce or extend config so one public model can route to multiple backend variants.

Example desired shape:

```yaml
models:
  - name: gpt-public
    routes:
      - backend: vllm-a
        weight: 80
        served_model_name: llama-3.1-8b-a
      - backend: vllm-b
        weight: 20
        served_model_name: llama-3.1-8b-b
    policy: weighted_sticky
```

The exact schema can follow existing SMG conventions, but it must preserve these concepts:

- public model name
- backend identity
- backend weight
- backend served model name
- routing policy

## Required Rewrite Behavior

For OpenAI-compatible requests:

- Parse request body structurally.
- Replace only the top-level `model` field.
- Rewrite after backend selection, because the target served model name is backend-specific.
- Preserve all other request fields unchanged.
- Forward the rewritten request to the selected backend.
- Apply the same behavior for relevant OpenAI endpoints such as chat completions, completions, embeddings, and other existing SMG-supported typed requests.

Avoid fragile string replacement.

If SMG already has a worker/request preparation hook, prefer extending that path instead of adding a parallel forwarding path.

## Auth Extension

Add or document an extension point for third-party auth before routing.

Expected behavior:

- Auth runs before route selection.
- Auth can allow or deny the request.
- Auth can attach identity metadata used by sticky routing.
- Auth can support external providers through HTTP, OIDC/JWT validation, or SMG's existing plugin/WASM mechanism if sufficient.

If SMG already has control-plane auth but not data-plane auth, keep the change scoped to data-plane request auth.

## Kubernetes Discovery

The implementation must not require users to know backend pod IPs manually.

Support or document one of:

- Kubernetes service discovery by label selector.
- Kubernetes endpoint/endpointslice discovery.
- Existing SMG worker discovery if it can discover pods by labels.

The route config should reference backend groups or discovered workers, not individual pod IPs.

## Metrics

Expose Prometheus metrics for:

- requests by public model
- requests by selected backend
- requests by served model name
- routing policy decision count
- sticky vs non-sticky decision count
- auth allow/deny count
- rewrite success/failure count

Use existing SMG metric style if present.

## Tests

Add focused tests for:

- weighted random distribution without sticky key
- sticky stability with the same key
- different sticky keys distributing across weighted backends
- unhealthy backend exclusion
- request body model rewrite per selected backend
- preserving non-model request fields
- auth allow/deny path if implemented
- config parsing for the new schema

Prefer unit tests for policy and rewrite logic, plus one integration-style test for end-to-end route selection and forwarding.

## Implementation Constraints

- Keep changes minimal and aligned with existing SMG architecture.
- Reuse existing policy, worker registry, config, middleware, and request preparation paths where possible.
- Do not introduce Gateway API, AI Gateway, Envoy, NGINX, or HAProxy as dependencies.
- Do not hardcode backend names, models, or Kubernetes labels.
- Do not rewrite request bodies with raw string replacement.
- Do not break existing routing policies or existing OpenAI-compatible behavior.

## Suggested Investigation Starting Points

Inspect these areas first:

- `crates/protocols/src/worker.rs`
- `crates/protocols/src/model_card.rs`
- `model_gateway/src/worker/worker.rs`
- `model_gateway/src/routers/http/router.rs`
- `model_gateway/src/policies/`
- `model_gateway/src/config/`
- `docs/getting-started/load-balancing.md`
- `docs/getting-started/service-discovery.md`
- `docs/reference/configuration.md`

Look specifically for:

- worker model metadata
- route policy selection
- `prepare_request` or equivalent request mutation hook
- OpenAI typed request structs
- existing auth or middleware hooks
- Kubernetes discovery mechanism
- metrics registration conventions

## Deliverables

1. Code changes implementing the behavior.
2. Tests proving the behavior.
3. Documentation showing config and deployment examples.
4. A short summary of design decisions and tradeoffs.

