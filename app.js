const express = require('express');
const { SQSClient, GetQueueUrlCommand, GetQueueAttributesCommand, ListQueuesCommand } = require('@aws-sdk/client-sqs');
const { CloudWatchClient, GetMetricDataCommand } = require('@aws-sdk/client-cloudwatch');
const client = require('@prometheus-io/client');
const { loadConfig } = require('./config');

let server;
let refreshTimer = null;
let refreshing = false;
let ready = false;
let consecutiveFailures = 0;
let shuttingDown = false;
const MAX_BACKOFF_MULTIPLIER = 32;
// Per-queue health: { state: 'UP' | 'DOWN', failures, retryAt }
const queueState = {};
const queueUrlCache = {};
// Latest SQS attributes per queue, used to decide which queues need CloudWatch data.
const queueSnapshot = {};
let monitoredQueueNames = new Set();
let discoveredQueueUrls = [];
let discoveryTimestamp = 0;

// CloudWatch deletions metric: 1-minute buckets summed over a fixed 5-minute window.
const CW_NAMESPACE = 'AWS/SQS';
const CW_METRIC_NAME = 'NumberOfMessagesDeleted';
const CW_PERIOD_SECONDS = 60;
const CW_WINDOW_SECONDS = 300;
const CW_MAX_QUERIES_PER_REQUEST = 500;

const QUEUE_MISSING_ERRORS = ['QueueDoesNotExist', 'AWS.SimpleQueueService.NonExistentQueue'];

const app = express();

let config = null;

function getConfig() {
  return config;
}

function validateRuntimeConfiguration() {
  config = loadConfig();
}

const register = new client.Registry();
client.collectDefaultMetrics({ register });

const sqsApproximateNumberOfMessagesVisible = new client.Gauge({
  name: 'sqs_approximate_number_of_messages',
  help: 'Approximate number of messages in the SQS queue',
  labelNames: ['queueName']
});
const sqsApproximateNumberOfMessagesNotVisible = new client.Gauge({
  name: 'sqs_approximate_number_of_messages_not_visible',
  help: 'Approximate number of messages not visible in the SQS queue',
  labelNames: ['queueName']
});
const sqsApproximateNumberOfMessagesDelayed = new client.Gauge({
  name: 'sqs_approximate_number_of_messages_delayed',
  help: 'Approximate number of messages delayed in the SQS queue',
  labelNames: ['queueName']
});

register.registerMetric(sqsApproximateNumberOfMessagesVisible);
register.registerMetric(sqsApproximateNumberOfMessagesNotVisible);
register.registerMetric(sqsApproximateNumberOfMessagesDelayed);

const sqsExporterQueueUp = new client.Gauge({
  name: 'sqs_exporter_queue_up',
  help: 'Whether the queue is accessible: 1 for UP, 0 for DOWN',
  labelNames: ['queueName']
});
register.registerMetric(sqsExporterQueueUp);

const sqsApproximateNumberOfGroupsWithInflightMessages = new client.Gauge({
  name: 'sqs_approximate_number_of_groups_with_inflight_messages',
  help: 'Approximate number of groups with in-flight messages (FIFO queues only)',
  labelNames: ['queueName']
});
register.registerMetric(sqsApproximateNumberOfGroupsWithInflightMessages);

const sqsExporterLastRefreshTimestamp = new client.Gauge({
  name: 'sqs_exporter_last_refresh_timestamp',
  help: 'Unix timestamp of the last successful metric refresh'
});
register.registerMetric(sqsExporterLastRefreshTimestamp);

const sqsExporterRefreshDurationSeconds = new client.Gauge({
  name: 'sqs_exporter_refresh_duration_seconds',
  help: 'Duration of the last refresh cycle in seconds'
});
register.registerMetric(sqsExporterRefreshDurationSeconds);

const sqsExporterRefreshFailuresTotal = new client.Counter({
  name: 'sqs_exporter_refresh_failures_total',
  help: 'Total number of refresh cycles where all queues failed'
});
register.registerMetric(sqsExporterRefreshFailuresTotal);

const sqsExporterQueueRefreshTotal = new client.Counter({
  name: 'sqs_exporter_queue_refresh_total',
  help: 'Total per-queue refresh attempts by result',
  labelNames: ['result']
});
register.registerMetric(sqsExporterQueueRefreshTotal);

const sqsMessagesDeletedLast5m = new client.Gauge({
  name: 'sqs_messages_deleted_last_5m',
  help: 'Messages deleted over the last 5 complete minutes (CloudWatch NumberOfMessagesDeleted). Only exported for queues with messages waiting or in flight',
  labelNames: ['queueName']
});
register.registerMetric(sqsMessagesDeletedLast5m);

const sqsExporterAwsRequestsTotal = new client.Counter({
  name: 'sqs_exporter_aws_requests_total',
  help: 'AWS API requests issued by the exporter (excluding SDK retries), by API and result',
  labelNames: ['api', 'result']
});
register.registerMetric(sqsExporterAwsRequestsTotal);

const sqsExporterCloudWatchMetricsRequestedTotal = new client.Counter({
  name: 'sqs_exporter_cloudwatch_metrics_requested_total',
  help: 'CloudWatch metrics requested via GetMetricData (the unit CloudWatch bills on)'
});
register.registerMetric(sqsExporterCloudWatchMetricsRequestedTotal);

const perQueueGauges = [
  sqsApproximateNumberOfMessagesVisible,
  sqsApproximateNumberOfMessagesNotVisible,
  sqsApproximateNumberOfMessagesDelayed,
  sqsApproximateNumberOfGroupsWithInflightMessages,
  sqsMessagesDeletedLast5m
];

let sqs = null;
let cloudwatch = null;
let cwRefreshTimer = null;
let cwRefreshing = false;

async function sendTracked(awsClient, command, api) {
  try {
    const data = await awsClient.send(command);
    sqsExporterAwsRequestsTotal.inc({ api, result: 'success' });
    return data;
  } catch (error) {
    sqsExporterAwsRequestsTotal.inc({ api, result: 'error' });
    throw error;
  }
}

function isQueueMissingError(error) {
  return QUEUE_MISSING_ERRORS.includes(error.name) || QUEUE_MISSING_ERRORS.includes(error.Code);
}

function clearQueueMetrics(queueName) {
  perQueueGauges.forEach((gauge) => gauge.remove({ queueName }));
}

async function getQueueUrl(queueName) {
  if (queueUrlCache[queueName]) {
    return queueUrlCache[queueName];
  }

  try {
    const command = new GetQueueUrlCommand({ QueueName: queueName });
    const data = await sendTracked(sqs, command, 'GetQueueUrl');
    queueUrlCache[queueName] = data.QueueUrl;
    return data.QueueUrl;
  } catch (error) {
    console.error(`Failed to resolve queue URL for "${queueName}":`, error.message);
    throw error;
  }
}

async function fetchQueueAttributes(target) {
  const queueName = target.name;
  const queueUrl = target.url || await getQueueUrl(queueName);

  try {
    const command = new GetQueueAttributesCommand({
      QueueUrl: queueUrl,
      AttributeNames: [
        'ApproximateNumberOfMessages',
        'ApproximateNumberOfMessagesNotVisible',
        'ApproximateNumberOfMessagesDelayed',
        'ApproximateNumberOfGroupsWithInflightMessages'
      ]
    });

    const data = await sendTracked(sqs, command, 'GetQueueAttributes');
    const visible = parseInt(data.Attributes.ApproximateNumberOfMessages, 10) || 0;
    const notVisible = parseInt(data.Attributes.ApproximateNumberOfMessagesNotVisible, 10) || 0;

    sqsApproximateNumberOfMessagesVisible.set({ queueName }, visible);
    sqsApproximateNumberOfMessagesNotVisible.set({ queueName }, notVisible);
    sqsApproximateNumberOfMessagesDelayed.set(
      { queueName },
      parseInt(data.Attributes.ApproximateNumberOfMessagesDelayed, 10) || 0
    );

    if (data.Attributes.ApproximateNumberOfGroupsWithInflightMessages !== undefined) {
      sqsApproximateNumberOfGroupsWithInflightMessages.set(
        { queueName },
        parseInt(data.Attributes.ApproximateNumberOfGroupsWithInflightMessages, 10) || 0
      );
    } else {
      sqsApproximateNumberOfGroupsWithInflightMessages.remove({ queueName });
    }

    queueSnapshot[queueName] = { url: queueUrl, visible, notVisible };
    return data.Attributes;
  } catch (error) {
    // Only a missing queue invalidates the cached URL; throttling or timeouts
    // would otherwise cost an extra GetQueueUrl call on every failed cycle.
    if (!target.url && isQueueMissingError(error)) {
      delete queueUrlCache[queueName];
    }
    console.error(`Failed to read queue attributes for "${queueName}" (URL: ${queueUrl}):`, error.message);
    throw error;
  }
}

async function getSQSAttributes(queueName) {
  return fetchQueueAttributes({ name: queueName, url: null });
}

async function boundedMap(items, fn, concurrency) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < items.length) {
      const index = nextIndex++;
      results[index] = await fn(items[index], index);
    }
  }

  const workers = Array.from(
    { length: Math.min(concurrency, items.length) },
    () => worker()
  );
  await Promise.all(workers);
  return results;
}

function parseQueueNameFromUrl(url) {
  const parts = url.split('/');
  return parts[parts.length - 1];
}

function parseRegionFromQueueUrl(url) {
  try {
    const match = new URL(url).hostname.match(/(?:^|\.)([a-z]{2}(?:-[a-z]+)+-\d+)\./);
    return match ? match[1] : null;
  } catch (_) {
    return null;
  }
}

async function discoverQueues() {
  const cfg = getConfig();
  const now = Date.now();

  if (discoveredQueueUrls.length > 0 && (now - discoveryTimestamp) < cfg.discoveryTtlMs) {
    return discoveredQueueUrls;
  }

  const prefixes = cfg.queueNames.filter((name) => name.endsWith('*'));
  const exactNames = cfg.queueNames.filter((name) => !name.endsWith('*'));

  if (prefixes.length === 0) {
    discoveredQueueUrls = [];
    return discoveredQueueUrls;
  }

  const allDiscovered = [];
  let anyPrefixFailed = false;
  for (const prefix of prefixes) {
    const prefixStr = prefix.slice(0, -1);
    try {
      const command = new ListQueuesCommand({ QueueNamePrefix: prefixStr });
      const data = await sendTracked(sqs, command, 'ListQueues');
      const urls = data.QueueUrls || [];
      if (urls.length === 0) {
        console.error(`Prefix "${prefix}" returned no matches`);
      }
      allDiscovered.push(...urls);
    } catch (error) {
      anyPrefixFailed = true;
      console.error(`Failed to discover queues for prefix "${prefix}": ${error.message}`);
    }
  }

  // Keep the previous result on a failed lookup (retried next cycle) rather than
  // treating its queues as removed and dropping their series.
  if (anyPrefixFailed && discoveredQueueUrls.length > 0) {
    return discoveredQueueUrls;
  }

  if (allDiscovered.length > cfg.maxDiscoveredQueues) {
    console.error(`Discovered ${allDiscovered.length} queues exceeds SQS_MAX_DISCOVERED_QUEUES=${cfg.maxDiscoveredQueues}, truncating`);
    allDiscovered.length = cfg.maxDiscoveredQueues;
  }

  discoveredQueueUrls = allDiscovered.map((url) => ({
    url,
    name: parseQueueNameFromUrl(url)
  }));

  const allNames = [
    ...exactNames,
    ...discoveredQueueUrls.map((d) => d.name)
  ];
  console.log(`Discovered queues: ${allNames.join(', ')}`);

  discoveryTimestamp = now;
  return discoveredQueueUrls;
}

function getMonitoredQueueNames() {
  return [...monitoredQueueNames];
}

// Drop every series and cached state for queues that are no longer monitored
// (e.g. a discovered queue was deleted), so they don't linger forever.
function forgetRemovedQueues(currentNames) {
  monitoredQueueNames.forEach((queueName) => {
    if (currentNames.has(queueName)) {
      return;
    }
    clearQueueMetrics(queueName);
    sqsExporterQueueUp.remove({ queueName });
    delete queueState[queueName];
    delete queueSnapshot[queueName];
    delete queueUrlCache[queueName];
  });
  monitoredQueueNames = currentNames;
}

function isQueueDue(queueName, now) {
  const state = queueState[queueName];
  return !state || !state.retryAt || state.retryAt <= now;
}

function markQueueUp(queueName) {
  const state = queueState[queueName];
  if (state && state.state === 'DOWN') {
    console.error(`Queue "${queueName}" transitioned DOWN -> UP`);
  }
  queueState[queueName] = { state: 'UP', failures: 0, retryAt: 0 };
  sqsExporterQueueUp.set({ queueName }, 1);
}

function markQueueDown(queueName, reason, refreshStart) {
  const previous = queueState[queueName];
  if (!previous) {
    console.error(`Queue "${queueName}" is DOWN: ${reason.message}`);
  } else if (previous.state === 'UP') {
    console.error(`Queue "${queueName}" transitioned UP -> DOWN: ${reason.message}`);
  }

  // Retry on the next cycle after the first failure, then skip exponentially
  // more cycles so a deleted or forbidden queue stops costing a call per cycle.
  const failures = (previous && previous.state === 'DOWN' ? previous.failures : 0) + 1;
  const multiplier = Math.min(Math.pow(2, failures - 1), MAX_BACKOFF_MULTIPLIER) - 1;
  queueState[queueName] = {
    state: 'DOWN',
    failures,
    retryAt: refreshStart + getConfig().refreshIntervalMs * multiplier
  };

  // Report "unknown" rather than 0 messages: drop the series and rely on queue_up.
  clearQueueMetrics(queueName);
  delete queueSnapshot[queueName];
  sqsExporterQueueUp.set({ queueName }, 0);
}

async function refreshOnce() {
  if (refreshing) {
    return;
  }

  refreshing = true;
  const refreshStart = Date.now();
  try {
    const cfg = getConfig();
    const exactNames = cfg.queueNames.filter((name) => !name.endsWith('*'));
    const queueUrls = cfg.queueUrls;
    const maxConcurrency = cfg.maxConcurrency;

    const discovered = await discoverQueues();

    const allTargets = [
      ...exactNames.map((name) => ({ name, url: null })),
      ...queueUrls.map((u) => ({ name: u.name, url: u.url })),
      ...discovered.map((d) => ({ name: d.name, url: d.url }))
    ];

    forgetRemovedQueues(new Set(allTargets.map((t) => t.name)));

    const dueTargets = allTargets.filter((target) => isQueueDue(target.name, refreshStart));

    const results = await boundedMap(
      dueTargets,
      (target) => fetchQueueAttributes(target).then(
        (value) => ({ status: 'fulfilled', value, name: target.name }),
        (reason) => ({ status: 'rejected', reason, name: target.name })
      ),
      maxConcurrency
    );

    let successCount = 0;
    let failureCount = 0;

    results.forEach((result) => {
      if (result.status === 'fulfilled') {
        successCount++;
        markQueueUp(result.name);
      } else {
        failureCount++;
        markQueueDown(result.name, result.reason, refreshStart);
      }
    });

    if (successCount > 0) {
      consecutiveFailures = 0;
      ready = true;
    }

    if (failureCount > 0 && successCount === 0) {
      consecutiveFailures++;
      sqsExporterRefreshFailuresTotal.inc();
    }

    sqsExporterQueueRefreshTotal.inc({ result: 'success' }, successCount);
    sqsExporterQueueRefreshTotal.inc({ result: 'failure' }, failureCount);

    const refreshDuration = (Date.now() - refreshStart) / 1000;
    sqsExporterRefreshDurationSeconds.set(refreshDuration);

    if (successCount > 0) {
      sqsExporterLastRefreshTimestamp.set(Date.now() / 1000);
    }
  } finally {
    refreshing = false;
  }
}

// Queues worth asking CloudWatch about: currently UP, in the exporter's own
// region, and holding messages (an empty queue cannot be stalled, so querying
// it would only add to the CloudWatch bill).
function getCloudWatchCandidates() {
  const cfg = getConfig();
  return getMonitoredQueueNames().filter((queueName) => {
    const snapshot = queueSnapshot[queueName];
    if (!snapshot || !queueState[queueName] || queueState[queueName].state !== 'UP') {
      return false;
    }
    const region = parseRegionFromQueueUrl(snapshot.url);
    if (region && region !== cfg.awsRegion) {
      return false;
    }
    return snapshot.visible + snapshot.notVisible > 0;
  });
}

function getCloudWatchWindow(nowMs, delaySeconds) {
  // Align to whole minutes and stay `delaySeconds` behind real time so every
  // bucket in the window is complete; CloudWatch ingests SQS metrics with a lag.
  const endMs = Math.floor((nowMs - delaySeconds * 1000) / 60000) * 60000;
  return {
    startTime: new Date(endMs - CW_WINDOW_SECONDS * 1000),
    endTime: new Date(endMs)
  };
}

async function fetchDeletedCounts(queueNames, window) {
  const queries = queueNames.map((queueName, index) => ({
    Id: `q${index}`,
    MetricStat: {
      Metric: {
        Namespace: CW_NAMESPACE,
        MetricName: CW_METRIC_NAME,
        Dimensions: [{ Name: 'QueueName', Value: queueName }]
      },
      Period: CW_PERIOD_SECONDS,
      Stat: 'Sum'
    },
    ReturnData: true
  }));

  sqsExporterCloudWatchMetricsRequestedTotal.inc(queries.length);

  const valuesById = {};
  let nextToken;
  do {
    const command = new GetMetricDataCommand({
      MetricDataQueries: queries,
      StartTime: window.startTime,
      EndTime: window.endTime,
      NextToken: nextToken
    });
    const data = await sendTracked(cloudwatch, command, 'GetMetricData');
    (data.MetricDataResults || []).forEach((result) => {
      valuesById[result.Id] = (valuesById[result.Id] || []).concat(result.Values || []);
    });
    nextToken = data.NextToken;
  } while (nextToken);

  return queueNames.map((queueName, index) => ({ queueName, values: valuesById[`q${index}`] || [] }));
}

async function refreshCloudWatchMetrics() {
  const cfg = getConfig();
  if (!cfg || !cfg.cwMetricsEnabled || !cloudwatch || cwRefreshing) {
    return;
  }

  cwRefreshing = true;
  try {
    const candidates = getCloudWatchCandidates();
    const candidateSet = new Set(candidates);
    getMonitoredQueueNames()
      .filter((queueName) => !candidateSet.has(queueName))
      .forEach((queueName) => sqsMessagesDeletedLast5m.remove({ queueName }));

    const window = getCloudWatchWindow(Date.now(), cfg.cwMetricsDelaySeconds);

    for (let i = 0; i < candidates.length; i += CW_MAX_QUERIES_PER_REQUEST) {
      const batch = candidates.slice(i, i + CW_MAX_QUERIES_PER_REQUEST);
      try {
        const results = await fetchDeletedCounts(batch, window);
        results.forEach(({ queueName, values }) => {
          if (values.length === 0) {
            // No datapoints means "unknown" (e.g. a queue in another account),
            // not "zero deletions" - don't let it trigger a stall alert.
            sqsMessagesDeletedLast5m.remove({ queueName });
            return;
          }
          sqsMessagesDeletedLast5m.set({ queueName }, values.reduce((sum, v) => sum + v, 0));
        });
      } catch (error) {
        // e.g. missing cloudwatch:GetMetricData permission. Drop the series so a
        // stale value can't mask a stall, and log once per batch, not per queue.
        batch.forEach((queueName) => sqsMessagesDeletedLast5m.remove({ queueName }));
        console.warn(`CloudWatch GetMetricData failed for ${batch.length} queue(s): ${error.message}`);
      }
    }
  } finally {
    cwRefreshing = false;
  }
}

function getBackoffDelay() {
  const multiplier = Math.min(Math.pow(2, consecutiveFailures), MAX_BACKOFF_MULTIPLIER);
  return getConfig().refreshIntervalMs * multiplier;
}

function scheduleRefresh() {
  if (refreshTimer) {
    clearTimeout(refreshTimer);
  }

  const delay = getBackoffDelay();
  refreshTimer = setTimeout(async () => {
    try {
      await refreshOnce();
    } catch (_) {}
    scheduleRefresh();
  }, delay);
}

function stopRefreshLoop() {
  if (refreshTimer) {
    clearTimeout(refreshTimer);
    refreshTimer = null;
  }
}

function scheduleCloudWatchRefresh() {
  if (cwRefreshTimer) {
    clearTimeout(cwRefreshTimer);
  }

  const cfg = getConfig();
  if (!cfg || !cfg.cwMetricsEnabled) return;

  cwRefreshTimer = setTimeout(async () => {
    try {
      await refreshCloudWatchMetrics();
    } catch (_) {}
    scheduleCloudWatchRefresh();
  }, cfg.cwMetricsRefreshIntervalMs);
}

function stopCloudWatchRefresh() {
  if (cwRefreshTimer) {
    clearTimeout(cwRefreshTimer);
    cwRefreshTimer = null;
  }
}

function isReady() {
  return ready;
}

app.get('/ping', (req, res) => {
  res.status(200).send('pong');
});

app.get('/ready', (req, res) => {
  if (isReady()) {
    res.status(200).send('ready');
  } else {
    res.status(503).send('not ready');
  }
});

app.get('/metrics', async (req, res) => {
  try {
    const metrics = await register.metrics();
    res.set('Content-Type', register.contentType);
    res.end(metrics);
  } catch (error) {
    res.status(500).send(`Failed to collect metrics: ${error.message}`);
  }
});

app.use((error, req, res, next) => {
  console.error('Unhandled server error:', error.stack);
  res.status(500).send('Internal server error.');
});

async function startServer() {
  validateRuntimeConfiguration();
  sqs = new SQSClient(getConfig().sqsConfig);
  if (getConfig().cwMetricsEnabled) {
    cloudwatch = new CloudWatchClient(getConfig().cloudWatchConfig);
  }

  return new Promise((resolve) => {
    server = app.listen(getConfig().port, () => {
      console.log(`Server is running on port ${getConfig().port}`);
      resolve(server);
    });
  });
}

function gracefulShutdown() {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  console.log('Graceful shutdown initiated');
  stopRefreshLoop();
  stopCloudWatchRefresh();

  const timeout = getConfig() ? getConfig().gracefulShutdownTimeoutMs : 10000;
  const forceTimer = setTimeout(() => {
    console.error(`Graceful shutdown timed out after ${timeout}ms, forcing exit`);
    process.exit(1);
  }, timeout);

  stopServer()
    .then(() => {
      clearTimeout(forceTimer);
      console.log('Server closed successfully');
      process.exit(0);
    })
    .catch((error) => {
      clearTimeout(forceTimer);
      console.error(`Error during shutdown: ${error.message}`);
      process.exit(1);
    });
}

function stopServer() {
  stopRefreshLoop();
  stopCloudWatchRefresh();

  if (!server) {
    return Promise.resolve();
  }

  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }

      server = undefined;
      sqs = null;
      cloudwatch = null;
      config = null;
      ready = false;
      consecutiveFailures = 0;
      shuttingDown = false;
      Object.keys(queueState).forEach((k) => delete queueState[k]);
      Object.keys(queueUrlCache).forEach((k) => delete queueUrlCache[k]);
      Object.keys(queueSnapshot).forEach((k) => delete queueSnapshot[k]);
      monitoredQueueNames = new Set();
      discoveredQueueUrls = [];
      discoveryTimestamp = 0;
      resolve();
    });
  });
}

module.exports = {
  app,
  startServer,
  stopServer,
  getQueueUrl,
  getSQSAttributes,
  parseQueueNames: () => getConfig() ? getConfig().queueNames : [],
  validateRuntimeConfiguration,
  refreshOnce,
  scheduleRefresh,
  isReady,
  stopRefreshLoop,
  refreshCloudWatchMetrics,
  scheduleCloudWatchRefresh,
  stopCloudWatchRefresh,
  getMonitoredQueueNames,
  getCloudWatchWindow,
  getBackoffDelay,
  getConfig,
  gracefulShutdown
};

if (require.main === module) {
  process.on('SIGTERM', gracefulShutdown);
  process.on('SIGINT', gracefulShutdown);

  startServer()
    .then(async () => {
      await refreshOnce();
      if (!isReady()) {
        console.error('Initial refresh failed: no queues could be fetched');
        process.exit(1);
      }
      console.log('Initial refresh completed successfully');
      scheduleRefresh();
      if (getConfig().cwMetricsEnabled) {
        await refreshCloudWatchMetrics();
        scheduleCloudWatchRefresh();
      }
    })
    .catch((error) => {
      console.error(`Failed to start server: ${error.message}`);
      process.exit(1);
    });
}
