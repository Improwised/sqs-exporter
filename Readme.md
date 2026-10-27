# SQS Exporter

SQS Exporter is a small Node.js service that exposes Prometheus metrics for one or more AWS SQS queues.

It polls queue attributes from AWS SQS and publishes these metrics:

- `sqs_approximate_number_of_messages`
- `sqs_approximate_number_of_messages_not_visible`
- `sqs_approximate_number_of_messages_delayed`
- `sqs_approximate_number_of_groups_with_inflight_messages` (FIFO queues only)
- `sqs_exporter_queue_up`: `1` when the last poll succeeded, `0` when it failed
- `sqs_messages_deleted_last_5m` (only when `CW_METRICS_ENABLED=true`, see below)

When a queue cannot be read, its message-count series are removed (not set to `0`), so "unknown" is never mistaken for "empty"; `sqs_exporter_queue_up` reports the failure. Series for queues that stop being monitored (for example a deleted queue matched by a prefix) are removed too.

## Requirements

- Node.js 18+
- Access to AWS SQS with permission to call:
  - `sqs:GetQueueUrl`
  - `sqs:GetQueueAttributes`
- (optional) Access to AWS CloudWatch with permission to call:
  - `cloudwatch:GetMetricData` (only when `CW_METRICS_ENABLED=true`)

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
- `METRICS_REFRESH_INTERVAL_MS` (optional): background refresh interval in milliseconds, defaults to `60000`. SQS bills every `GetQueueAttributes` call (one per queue per refresh), and the values are approximate anyway, so polling faster than Prometheus scrapes only adds cost

**CloudWatch deletions metric** (all optional — the feature is disabled unless `CW_METRICS_ENABLED=true`):

- `CW_METRICS_ENABLED`: set to `true` to enable the `sqs_messages_deleted_last_5m` metric, defaults to `false`
- `CW_METRICS_REFRESH_INTERVAL_MS`: CloudWatch refresh interval in milliseconds, defaults to `300000` (5 minutes)
- `CW_METRICS_DELAY_SECONDS`: how far behind real time the 5-minute window ends, defaults to `120`. CloudWatch receives SQS metrics with a 1–2 minute lag; reading the newest minutes would undercount and look like a stall

### Stall detection with `sqs_messages_deleted_last_5m`

The exporter reads CloudWatch `AWS/SQS` `NumberOfMessagesDeleted` as 1-minute sums and adds up the five most recent *complete* minutes (the window is aligned to whole minutes and ends `CW_METRICS_DELAY_SECONDS` ago).

To keep CloudWatch cost down (it bills per metric requested, roughly 25x an SQS request):

- Only queues that currently hold messages (visible or in flight) are queried. An empty queue cannot be stalled, so it has no `sqs_messages_deleted_last_5m` series.
- All queues are fetched in one `GetMetricData` request (up to 500 queues per request).
- Queues whose URL is in a different region than `AWS_REGION` are skipped.

The series is removed (never set to `0`) when CloudWatch returns no datapoints, for example for a queue in another AWS account, or when the request fails, for example because the IAM permission is missing. A stale or missing value therefore cannot fire or mask a stall alert. Example alert:

```promql
sqs_messages_deleted_last_5m == 0 and on(queueName) sqs_approximate_number_of_messages > 0
```

### AWS request accounting

- `sqs_exporter_aws_requests_total{api, result}`: AWS API requests issued (SDK retries are not counted separately)
- `sqs_exporter_cloudwatch_metrics_requested_total`: metrics requested via `GetMetricData`, the unit CloudWatch bills on

A queue that fails repeatedly is retried with per-queue exponential backoff (up to 32 refresh intervals), so a deleted or forbidden queue does not cost a request every cycle. The cached queue URL is only discarded when SQS reports the queue no longer exists.

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
- `CW_METRICS_REFRESH_INTERVAL_MS` is not a positive integer
- `CW_METRICS_DELAY_SECONDS` is not a positive integer
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

Each monitored queue contributes approximately **4 time series** (3 SQS gauges + `sqs_exporter_queue_up`, plus 1 for FIFO queues), plus **1 additional time series** (`sqs_messages_deleted_last_5m`) for each queue holding messages when CloudWatch metrics are enabled. The default Node.js process metrics add a fixed set on top. Plan Prometheus capacity accordingly:

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
