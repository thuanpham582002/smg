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

## Remaining Verification

These steps were not completed before interruption and should be run before calling the production goal complete:

1. Register both workers against the new `weighted_sticky` pod.
2. Verify `/readiness` returns 200 after workers are routable.
3. Send a non-sticky request batch with `model=sbm-public` and count Kafka `backend_name` distribution.
4. Send repeated requests with the same `X-SMG-Routing-Key` and verify backend stickiness in Kafka events.
5. Confirm request bodies sent to clients keep `model=sbm-public` while backend usage events show the selected served model.
6. Confirm new usage rows land in `mc_usage_log` with stable model-registry identity headers.
7. Scrape `/metrics` and confirm routing/rewrite/Kafka counters increment.
