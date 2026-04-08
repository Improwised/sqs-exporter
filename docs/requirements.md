# SQS Exporter Requirements

## 1. Project Overview

SQS Exporter is a Node.js service that exposes Prometheus metrics for AWS SQS queues.
It is designed to run as a lightweight HTTP process in local, VM, or containerized
environments and be scraped by Prometheus.

Primary objective:
- Provide reliable, low-latency queue depth and queue health metrics for operations
  teams without requiring custom scripts per queue.

## 2. Current Functional Scope

Current implemented behavior:
- Exposes `GET /ping` for liveness.
- Exposes `GET /metrics` in Prometheus text format.
- Scrapes queue attributes for queues configured via `SQS_QUEUES`.
- Supports AWS profile credentials when `AWS_PROFILE` is set.
- Validates startup configuration for required queue list and region format.

Current metric names:
- `sqs_approximate_number_of_messages{queueName="..."}`
- `sqs_approximate_number_of_messages_not_visible{queueName="..."}`
- `sqs_approximate_number_of_messages_delayed{queueName="..."}`

## 3. Runtime Interfaces

### 3.1 HTTP Endpoints

- `GET /ping`
  - Returns `200` and body `pong`.
  - Purpose: liveness probes and quick operational checks.

- `GET /ready`
  - Returns `200` only after the initial background refresh/discovery cycle completes.
  - Purpose: readiness probes for container orchestration (signals when `/metrics` has populated data).

- `GET /metrics`
  - Returns Prometheus metrics payload.
  - Content-Type must be Prometheus-compatible (`register.contentType`).

### 3.2 Environment Variables

Required:
- `SQS_QUEUES`: comma-separated queue names.

Optional:
- `AWS_REGION`: defaults to `us-west-2`.
- `AWS_PROFILE`: optional shared credentials profile.
- `PORT`: defaults to `3001`.

Planned additions (roadmap):
- `METRICS_REFRESH_INTERVAL_MS`
- `SQS_MAX_CONCURRENCY`
- `SQS_QUEUE_URLS`
- `SQS_MAX_ATTEMPTS`
- `SQS_REQUEST_TIMEOUT_MS`
- `SQS_DISCOVERY_TTL_MS`
- `SQS_MAX_DISCOVERED_QUEUES`
- `GRACEFUL_SHUTDOWN_TIMEOUT_MS`

### 3.3 AWS Permissions

Minimum required IAM actions:
- `sqs:GetQueueUrl`
- `sqs:GetQueueAttributes`

Conditionally required (when prefix tokens ending in `*` are used in `SQS_QUEUES`):
- `sqs:ListQueues`

## 4. Quality and Non-Functional Requirements

Reliability:
- Exporter must not become unavailable due to one queue failing.
- Startup must fail fast on invalid configuration with actionable errors.
- Background refresh must implement adaptive backoff or circuit breaker to avoid hammering AWS during persistent outages.

Performance:
- `/metrics` should remain low-latency and avoid per-request AWS dependency.
- Queue refresh fan-out must be bounded to protect AWS APIs and local resources.

Operability:
- Service must support graceful shutdown on `SIGTERM` and `SIGINT`.
- Exporter internal health signals must be visible through self-metrics.
- `/ready` endpoint must signal when initial data fetch is complete.
- Prefix-based queue discovery must be capped (`SQS_MAX_DISCOVERED_QUEUES`) to prevent cardinality explosion.

Compatibility:
- Maintain Prometheus naming conventions and stable metric label contract.
- Keep Node.js LTS compatibility (18+ currently, 20 targeted in CI matrix).

## 5. Improvement Backlog (Approved)

All approved improvements are tracked in `br` under epic `sqs-exporter-oo4`.

### 5.1 Epic

- `sqs-exporter-oo4` - Production hardening roadmap for SQS exporter

### 5.2 Detailed Child Tasks

- `sqs-exporter-oo4.1` - Move SQS polling to background refresh loop (includes `/ready` endpoint and adaptive backoff)
- `sqs-exporter-oo4.2` - Isolate per-queue failures during refresh
- `sqs-exporter-oo4.3` - Add bounded concurrency for queue refresh
- `sqs-exporter-oo4.4` - Cache queue URL lookups to reduce AWS API calls
- `sqs-exporter-oo4.5` - Support `SQS_QUEUE_URLS` as configuration input
- `sqs-exporter-oo4.6` - Centralize and harden runtime configuration validation
- `sqs-exporter-oo4.7` - Expose AWS retry and timeout tuning via env vars
- `sqs-exporter-oo4.8` - Implement graceful shutdown for signals
- `sqs-exporter-oo4.9` - Add exporter self-observability metrics
- `sqs-exporter-oo4.10` - Add optional FIFO in-flight groups metric
- `sqs-exporter-oo4.11` - Expand automated tests and metric contract coverage
- `sqs-exporter-oo4.12` - Add CI workflow for tests on push and PR
- `sqs-exporter-oo4.13` - Harden container image for production deployments (includes resource/cardinality docs in README)
- `sqs-exporter-oo4.14` - Add prefix-based queue discovery with TTL cache (includes `SQS_MAX_DISCOVERED_QUEUES` cardinality limit)

### 5.3 Dependency Sequence

Execution order guidance:
1. `sqs-exporter-oo4.12` CI first — gates quality of all subsequent changes.
2. `sqs-exporter-oo4.1` background refresh architecture and `sqs-exporter-oo4.6` centralized config (parallel, independent roots).
3. `sqs-exporter-oo4.2` failure isolation and `sqs-exporter-oo4.3` concurrency bounds.
4. `sqs-exporter-oo4.4` URL cache, `sqs-exporter-oo4.8` graceful shutdown, `sqs-exporter-oo4.9` self-observability.
5. `sqs-exporter-oo4.5` queue URLs, `sqs-exporter-oo4.7` retry/timeout tuning, `sqs-exporter-oo4.14` prefix discovery.
6. `sqs-exporter-oo4.10` FIFO metric, `sqs-exporter-oo4.11` test expansion, `sqs-exporter-oo4.13` container hardening.

## 6. Definition of Done (Project Level)

An improvement is considered done only when:
- Code changes are merged with passing automated tests.
- Documentation is updated in `Readme.md` and this requirements document where relevant.
- New metrics are covered by tests and have stable names/labels.
- Operational behavior is verifiable through `/ping`, `/metrics`, and logs.

## 7. Out of Scope (Current Phase)

Not planned in this phase:
- Application-level auth/rate limiting on `/metrics`.
- Multi-region single-process aggregation.
- Full language rewrite (TypeScript/Go).
- Custom UI or GraphQL API layer.
- Kubernetes manifests or Helm charts (resource guidance in README only).
