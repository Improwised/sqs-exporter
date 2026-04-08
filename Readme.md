# SQS Exporter

SQS Exporter is a small Node.js service that exposes Prometheus metrics for one or more AWS SQS queues.

It polls queue attributes from AWS SQS and publishes these metrics:

- `sqs_approximate_number_of_messages`
- `sqs_approximate_number_of_messages_not_visible`
- `sqs_approximate_number_of_messages_delayed`

## Requirements

- Node.js 18+
- Access to AWS SQS with permission to call:
  - `sqs:GetQueueUrl`
  - `sqs:GetQueueAttributes`

## Installation

```bash
npm install
```

## Configuration

Set these environment variables before starting the service:

- `SQS_QUEUES` (required): comma-separated queue names, for example `orders,notifications`
- `AWS_REGION` (optional): AWS region, defaults to `us-west-2`
- `AWS_PROFILE` (optional): local AWS profile name from `~/.aws/credentials`
- `PORT` (optional): HTTP port, defaults to `3001`
- `METRICS_REFRESH_INTERVAL_MS` (optional): background refresh interval in milliseconds, defaults to `15000`

### Authentication

If `AWS_PROFILE` is set, the exporter uses that profile via shared credentials.

If `AWS_PROFILE` is not set, AWS SDK default credential resolution is used (for example environment variables, IAM role, ECS task role).

## Run

```bash
npm start
```

Server startup will fail fast with a clear error when:

- `SQS_QUEUES` is missing or empty
- `AWS_REGION` is invalid
- `PORT` is not a positive integer
- `METRICS_REFRESH_INTERVAL_MS` is not a positive integer
- `AWS_PROFILE` is set but cannot be loaded

## Docker

```bash
docker build -t sqs-exporter .

docker run -e SQS_QUEUES=my-queue -e AWS_REGION=us-east-1 -p 3001:3001 sqs-exporter
```

The container runs as a non-root user. A healthcheck against `/ping` is configured by default.

## Endpoints

- `GET /ping`: basic health check, returns `pong`
- `GET /ready`: readiness check, returns `200` after first successful metric refresh, `503` before
- `GET /metrics`: Prometheus metrics output (served from cache, no on-demand AWS calls)

## Resource Recommendations

| Resource | Recommended Limit | Notes |
|----------|-------------------|-------|
| CPU | 100m | Sufficient for up to ~100 queues |
| Memory | 128Mi | Includes Node.js runtime overhead |
| CPU (high queue count) | 250m | 100+ queues with short refresh intervals |
| Memory (high queue count) | 256Mi | 100+ queues with short refresh intervals |

## Metric Cardinality

Each monitored queue contributes approximately **4 time series** (3 SQS gauges + default Node.js metrics). Plan Prometheus capacity accordingly:

| Queues | Approximate Time Series |
|--------|------------------------|
| 10 | ~40 |
| 50 | ~200 |
| 100 | ~400 |
| 500 | ~2000 |

## Local Testing

```bash
npm test
```
