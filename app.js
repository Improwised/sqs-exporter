const express = require('express');
const { SQSClient, GetQueueUrlCommand, GetQueueAttributesCommand } = require('@aws-sdk/client-sqs');
const { fromIni } = require('@aws-sdk/credential-provider-ini');
const client = require('prom-client');

let server;

const app = express();
const PORT = Number(process.env.PORT || 3001);
const awsProfile = process.env.AWS_PROFILE;
const awsRegion = process.env.AWS_REGION || 'us-west-2';

function parseQueueNames() {
  if (!process.env.SQS_QUEUES) {
    return [];
  }

  return process.env.SQS_QUEUES
    .split(',')
    .map((queueName) => queueName.trim())
    .filter(Boolean);
}

function validateRegion(region) {
  return /^[a-z]{2}-[a-z]+-\d+$/.test(region);
}

async function validateRuntimeConfiguration() {
  const queueNames = parseQueueNames();

  if (queueNames.length === 0) {
    throw new Error('SQS_QUEUES must include at least one queue name.');
  }

  if (!validateRegion(awsRegion)) {
    throw new Error(`AWS_REGION is invalid: "${awsRegion}"`);
  }

  if (awsProfile) {
    try {
      await fromIni({ profile: awsProfile })();
    } catch (error) {
      throw new Error(`Unable to load AWS profile "${awsProfile}". ${error.message}`);
    }
  }
}

const sqsConfig = { region: awsRegion };
if (awsProfile) {
  sqsConfig.credentials = fromIni({ profile: awsProfile });
}

const sqs = new SQSClient(sqsConfig);

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

app.get('/ping', (req, res) => {
  res.status(200).send('pong');
});

app.get('/metrics', async (req, res) => {
  try {
    const queueNames = parseQueueNames();
    await Promise.all(queueNames.map((queueName) => getSQSAttributes(queueName)));
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
  await validateRuntimeConfiguration();

  return new Promise((resolve) => {
    server = app.listen(PORT, () => {
      console.log(`Server is running on port ${PORT}`);
      resolve(server);
    });
  });
}

function stopServer() {
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
  parseQueueNames,
  validateRuntimeConfiguration
};

if (require.main === module) {
  startServer().catch((error) => {
    console.error(`Failed to start server: ${error.message}`);
    process.exit(1);
  });
}
