# Production Goal: Kafka Usage Events

SMG/SBM replaces AI Gateway for request routing, so request accounting must not depend on AI Gateway ExtProc. The gateway emits AI-Gateway-compatible JSON usage events to Kafka after auth and routing, once per final request response.

## Runtime Config

Publishing is disabled by default. Enable it with:

```bash
export KAFKA_BROKERS=kafka-0.kafka:9092,kafka-1.kafka:9092
export KAFKA_TOPIC=ai-gateway-events
export KAFKA_EVENT_HEADER_KEYS=x-project-id,x-user-id,x-api-key-id,x-model-name,x-ai-eg-model,x-model-id,x-input-price,x-output-price,x-is-free
```

Optional SASL:

```bash
export KAFKA_SASL_USER=smg
export KAFKA_SASL_PASSWORD=...
export KAFKA_SASL_MECHANISM=PLAIN
```

The current pure-Rust producer path supports plaintext Kafka and SASL PLAIN/SCRAM. TLS requires enabling `rskafka` transport-tls support in the build.

## Event Contract

Each final routed request in the regular HTTP, OpenAI-compatible HTTP, and gRPC/PD forwarding paths emits one `request_completed` event with:

- request identity: `request_id`, `timestamp`, configured `x-*` headers
- model identity: `original_model`, `request_model`, `response_model`, `model_name_override`
- routing identity: `backend=SMG`, `backend_name`, `selected_pool`
- outcome: `success`, `error_type`, `latency_ms`, `stream`
- streaming timing: `time_to_first_token_ms`, `inter_token_latency_ms` when observed
- token usage: OpenAI-compatible `usage` fields for non-stream responses and final streaming usage chunks

Kafka publish failures are logged and counted in `smg_usage_events_total`; they do not fail user requests.

## Verification

Consume events:

```bash
kafka-console-consumer \
  --bootstrap-server kafka-0.kafka:9092 \
  --topic ai-gateway-events \
  --from-beginning
```

Send a non-streaming and streaming request, then verify:

- one event per user request
- required auth/pricing headers are present in `headers` and top-level `x-*` fields
- `tokens.input_tokens`, `tokens.output_tokens`, and `tokens.total_tokens` are populated when backend usage exists
- `model_name_override` matches the served backend model when model rewrite is configured
