const express = require('express');
const { SQSClient, GetQueueUrlCommand, GetQueueAttributesCommand } = require('@aws-sdk/client-sqs');
const client = require('prom-client');
const { loadConfig } = require('./config');

let server;
let refreshTimer = null;
let refreshing = false;
let ready = false;
let consecutiveFailures = 0;
const MAX_BACKOFF_MULTIPLIER = 32;
const queueState = {};

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

let sqs = null;

async function getQueueUrl(queueName) {
  try {
    const command = new GetQueueUrlCommand({ QueueName: queueName });
    const data = await sqs.send(command);
    return data.QueueUrl;
  } catch (error) {
    console.error(`Failed to resolve queue URL for "${queueName}":`, error.message);
    throw error;
  }
}

async function getSQSAttributes(queueName) {
  try {
    const queueUrl = await getQueueUrl(queueName);
    const command = new GetQueueAttributesCommand({
      QueueUrl: queueUrl,
      AttributeNames: [
        'ApproximateNumberOfMessages',
        'ApproximateNumberOfMessagesNotVisible',
        'ApproximateNumberOfMessagesDelayed'
      ]
    });

    const data = await sqs.send(command);

    sqsApproximateNumberOfMessagesVisible.set(
      { queueName },
      parseInt(data.Attributes.ApproximateNumberOfMessages, 10) || 0
    );
    sqsApproximateNumberOfMessagesNotVisible.set(
      { queueName },
      parseInt(data.Attributes.ApproximateNumberOfMessagesNotVisible, 10) || 0
    );
    sqsApproximateNumberOfMessagesDelayed.set(
      { queueName },
      parseInt(data.Attributes.ApproximateNumberOfMessagesDelayed, 10) || 0
    );

    return data.Attributes;
  } catch (error) {
    console.error(`Failed to read queue attributes for "${queueName}":`, error.message);
    throw error;
  }
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

async function refreshOnce() {
  if (refreshing) {
    return;
  }

  refreshing = true;
  try {
    const queueNames = getConfig().queueNames;
    const maxConcurrency = getConfig().maxConcurrency;

    const rawResults = await boundedMap(
      queueNames,
      (queueName) => getSQSAttributes(queueName).then(
        (value) => ({ status: 'fulfilled', value }),
        (reason) => ({ status: 'rejected', reason })
      ),
      maxConcurrency
    );

    const results = rawResults;

    let anySuccess = false;
    let anyFailure = false;

    results.forEach((result, index) => {
      const queueName = queueNames[index];
      if (result.status === 'fulfilled') {
        anySuccess = true;
        sqsExporterQueueUp.set({ queueName }, 1);
        if (queueState[queueName] === 'DOWN') {
          console.error(`Queue "${queueName}" transitioned DOWN -> UP`);
          queueState[queueName] = 'UP';
        } else {
          queueState[queueName] = 'UP';
        }
      } else {
        anyFailure = true;
        sqsExporterQueueUp.set({ queueName }, 0);
        sqsApproximateNumberOfMessagesVisible.set({ queueName }, 0);
        sqsApproximateNumberOfMessagesNotVisible.set({ queueName }, 0);
        sqsApproximateNumberOfMessagesDelayed.set({ queueName }, 0);
        if (queueState[queueName] === 'UP') {
          console.error(`Queue "${queueName}" transitioned UP -> DOWN: ${result.reason.message}`);
          queueState[queueName] = 'DOWN';
        } else if (!queueState[queueName]) {
          console.error(`Queue "${queueName}" is DOWN: ${result.reason.message}`);
          queueState[queueName] = 'DOWN';
        }
      }
    });

    if (anySuccess) {
      consecutiveFailures = 0;
      ready = true;
    }

    if (anyFailure && !anySuccess) {
      consecutiveFailures++;
    }
  } finally {
    refreshing = false;
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

let shuttingDown = false;

async function startServer() {
  validateRuntimeConfiguration();
  sqs = new SQSClient(getConfig().sqsConfig);

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
      config = null;
      ready = false;
      consecutiveFailures = 0;
      shuttingDown = false;
      Object.keys(queueState).forEach((k) => delete queueState[k]);
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
    })
    .catch((error) => {
      console.error(`Failed to start server: ${error.message}`);
      process.exit(1);
    });
}
