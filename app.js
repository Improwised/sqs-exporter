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

async function refreshOnce() {
  if (refreshing) {
    return;
  }

  refreshing = true;
  try {
    const queueNames = getConfig().queueNames;
    await Promise.all(queueNames.map((queueName) => getSQSAttributes(queueName)));
    consecutiveFailures = 0;
    ready = true;
  } catch (error) {
    consecutiveFailures++;
    throw error;
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
  getConfig
};

if (require.main === module) {
  startServer()
    .then(async () => {
      try {
        await refreshOnce();
        console.log('Initial refresh completed successfully');
      } catch (error) {
        console.error(`Initial refresh failed: ${error.message}`);
        process.exit(1);
      }
      scheduleRefresh();
    })
    .catch((error) => {
      console.error(`Failed to start server: ${error.message}`);
      process.exit(1);
    });
}
