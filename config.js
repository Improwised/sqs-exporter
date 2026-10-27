const { fromIni } = require('@aws-sdk/credential-provider-ini');

function validateRegion(region) {
  return /^[a-z]{2}-[a-z]+-\d+$/.test(region);
}

function validatePositiveInt(value, name) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

function parseQueueNames(raw) {
  if (!raw) {
    return [];
  }

  return raw
    .split(',')
    .map((name) => name.trim())
    .filter(Boolean);
}

function parseQueueUrl(url) {
  let parsed;
  try {
    parsed = new URL(url);
  } catch (_) {
    throw new Error(`SQS_QUEUE_URLS contains invalid URL: "${url}"`);
  }

  const parts = parsed.pathname.split('/').filter(Boolean);
  if (parts.length < 2) {
    throw new Error(`SQS_QUEUE_URLS contains invalid URL: "${url}"`);
  }

  const name = parts[parts.length - 1];
  return { url, name };
}

function parseQueueUrls(raw) {
  if (!raw) {
    return [];
  }

  return raw
    .split(',')
    .map((u) => u.trim())
    .filter(Boolean)
    .map(parseQueueUrl);
}

function loadConfig() {
  const queueNames = parseQueueNames(process.env.SQS_QUEUES);
  const queueUrls = parseQueueUrls(process.env.SQS_QUEUE_URLS);

  if (queueNames.length === 0 && queueUrls.length === 0) {
    throw new Error('SQS_QUEUES or SQS_QUEUE_URLS must include at least one entry');
  }

  const urlNames = queueUrls.map((u) => u.name);
  const duplicates = queueNames.filter((n) => urlNames.includes(n));
  if (duplicates.length > 0) {
    throw new Error(`Duplicate queue name between SQS_QUEUES and SQS_QUEUE_URLS: "${duplicates[0]}"`);
  }

  const awsRegion = process.env.AWS_REGION || 'us-west-2';

  if (!validateRegion(awsRegion)) {
    throw new Error(`AWS_REGION is invalid: "${awsRegion}"`);
  }

  const port = validatePositiveInt(process.env.PORT || 3001, 'PORT');
  const refreshIntervalMs = validatePositiveInt(
    process.env.METRICS_REFRESH_INTERVAL_MS || 60000,
    'METRICS_REFRESH_INTERVAL_MS'
  );

  const awsProfile = process.env.AWS_PROFILE;
  const maxConcurrency = validatePositiveInt(
    process.env.SQS_MAX_CONCURRENCY || 5,
    'SQS_MAX_CONCURRENCY'
  );
  const gracefulShutdownTimeoutMs = validatePositiveInt(
    process.env.GRACEFUL_SHUTDOWN_TIMEOUT_MS || 10000,
    'GRACEFUL_SHUTDOWN_TIMEOUT_MS'
  );
  const discoveryTtlMs = validatePositiveInt(
    process.env.SQS_DISCOVERY_TTL_MS || 900000,
    'SQS_DISCOVERY_TTL_MS'
  );
  const maxDiscoveredQueues = validatePositiveInt(
    process.env.SQS_MAX_DISCOVERED_QUEUES || 100,
    'SQS_MAX_DISCOVERED_QUEUES'
  );
  const cwMetricsEnabled = process.env.CW_METRICS_ENABLED === 'true';
  const cwMetricsRefreshIntervalMs = validatePositiveInt(
    process.env.CW_METRICS_REFRESH_INTERVAL_MS || 300000,
    'CW_METRICS_REFRESH_INTERVAL_MS'
  );
  const cwMetricsDelaySeconds = validatePositiveInt(
    process.env.CW_METRICS_DELAY_SECONDS || 120,
    'CW_METRICS_DELAY_SECONDS'
  );

  // Resolve credentials once and share them across AWS clients so that
  // the SQS client and the CloudWatch client authenticate identically.
  const awsCredentials = awsProfile ? fromIni({ profile: awsProfile }) : undefined;

  const sqsConfig = { region: awsRegion };
  if (process.env.SQS_MAX_ATTEMPTS) {
    sqsConfig.maxAttempts = validatePositiveInt(process.env.SQS_MAX_ATTEMPTS, 'SQS_MAX_ATTEMPTS');
  }
  if (process.env.SQS_REQUEST_TIMEOUT_MS) {
    sqsConfig.requestTimeout = validatePositiveInt(process.env.SQS_REQUEST_TIMEOUT_MS, 'SQS_REQUEST_TIMEOUT_MS');
  }
  if (awsCredentials) {
    sqsConfig.credentials = awsCredentials;
  }

  const cloudWatchConfig = { region: awsRegion };
  if (awsCredentials) {
    cloudWatchConfig.credentials = awsCredentials;
  }

  return Object.freeze({
    port,
    queueNames,
    queueUrls,
    awsRegion,
    awsProfile,
    refreshIntervalMs,
    maxConcurrency,
    gracefulShutdownTimeoutMs,
    discoveryTtlMs,
    maxDiscoveredQueues,
    cwMetricsEnabled,
    cwMetricsRefreshIntervalMs,
    cwMetricsDelaySeconds,
    sqsConfig,
    cloudWatchConfig
  });
}

module.exports = { loadConfig, validateRegion, validatePositiveInt, parseQueueNames, parseQueueUrl, parseQueueUrls };
