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
- `AWS_PROFILE` is set but cannot be loaded

## Endpoints

- `GET /ping`: basic health check, returns `pong`
- `GET /metrics`: Prometheus metrics output

## Local Testing

```bash
npm test
```
