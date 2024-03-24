const express = require('express');
const { SQSClient, GetQueueUrlCommand, GetQueueAttributesCommand } = require('@aws-sdk/client-sqs');
const { CloudWatchClient } = require('@aws-sdk/client-cloudwatch');
const { fromIni } = require('@aws-sdk/credential-provider-ini');
const client = require('prom-client');

// Set up Express web server
const app = express();
const PORT = process.env.PORT || 3001;
const aws_access_key_id = process.env.AWS_ACCESS_KEY_ID;
const aws_secret_access_key = process.env.AWS_SECRET_ACCESS_KEY;

// Add code here for handling errors and logging
app.use((err, req, res, next) => {
  console.error('Error:', err.stack);
  res.status(500).send('Something went wrong.');
});

app.get('/ping', (req, res) => {
  console.log('Received a GET request at /ping');
  res.status(200).send('pong');
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

// Set up AWS SDK
const awsProfile = process.env.AWS_PROFILE || 'default';
const awsCredentials = fromIni({ profile: awsProfile });
const awsRegion = process.env.AWS_REGION || 'us-west-2';

const sqs = new SQSClient({ region: awsRegion, credentials: awsCredentials });
const cloudwatch = new CloudWatchClient({ region: awsRegion, credentials: awsCredentials });


// Add code here for handling errors and logging
async function getQueueUrl(queueName) {
  try {
    const params = { QueueName: queueName };
    const command = new GetQueueUrlCommand(params);
    const data = await sqs.send(command);
    return data.QueueUrl;
  } catch (error) {
    console.error('Error:', error.stack);
    throw error;
  }
}

// Add code here for handling errors and logging
async function getSQSAttributes(queueName) {
  try {
    const queueUrl = await getQueueUrl(queueName);
    const params = {
      QueueUrl: queueUrl,
      AttributeNames: [
        'ApproximateNumberOfMessagesVisible',
        'ApproximateNumberOfMessagesNotVisible',
        'ApproximateNumberOfMessagesDelayed'
      ]
    };
    const command = new GetQueueAttributesCommand(params);
    const data = await sqs.send(command);

    // Set the custom metrics with the values from SQS attributes
    sqsApproximateNumberOfMessagesVisible.set({queueName}, data.Attributes.ApproximateNumberOfMessagesVisible || 0);
    sqsApproximateNumberOfMessagesNotVisible.set({queueName}, data.Attributes.ApproximateNumberOfMessagesNotVisible || 0);
    sqsApproximateNumberOfMessagesDelayed.set({queueName}, data.Attributes.ApproximateNumberOfMessagesDelayed || 0);

    return data.Attributes; // Keep returning the raw attributes for any other potential use
  } catch (error) {
    console.error('Error:', error.stack);
    throw error;
  }
}

module.exports = {
  getQueueUrl,
  getSQSAttributes
};
// Create a Prometheus Registry
const register = new client.Registry();

// Enable the collection of default metrics
client.collectDefaultMetrics({ register });

// Define custom metrics for SQS
let sqsApproximateNumberOfMessagesVisible = {};
let sqsApproximateNumberOfMessagesNotVisible = {};
let sqsApproximateNumberOfMessagesDelayed = {};

// Initialize custom metrics for each queue and add them to the registry
const queueNames = process.env.SQS_QUEUES ? process.env.SQS_QUEUES.split(',') : [];
queueNames.forEach(queueName => {
  const sanitizedQueueName = queueName.replace(/[^a-zA-Z0-9_:]/g, '_');
  sqsApproximateNumberOfMessagesVisible = new client.Gauge({
    name: 'sqs_approximate_number_of_messages_visible',
    help: 'Approximate number of messages visible in the SQS queue',
    labelNames: ['queueName']
  });
  sqsApproximateNumberOfMessagesNotVisible = new client.Gauge({
    name: 'sqs_approximate_number_of_messages_not_visible',
    help: 'Approximate number of messages not visible in the SQS queue',
    labelNames: ['queueName']
  });
  sqsApproximateNumberOfMessagesDelayed = new client.Gauge({
    name: 'sqs_approximate_number_of_messages_delayed',
    help: 'Approximate number of messages delayed in the SQS queue',
    labelNames: ['queueName']
  });
  register.registerMetric(sqsApproximateNumberOfMessagesVisible);
  register.registerMetric(sqsApproximateNumberOfMessagesNotVisible);
  register.registerMetric(sqsApproximateNumberOfMessagesDelayed);
});

// Endpoint to serve the metrics
app.get('/metrics', async (req, res) => {
  try {
    const queueNames = process.env.SQS_QUEUES ? process.env.SQS_QUEUES.split(',') : [];
    await Promise.all(queueNames.map(queueName => getSQSAttributes(queueName)));
    const metrics = await register.metrics();
    res.set('Content-Type', register.contentType);
    res.end(metrics);
  } catch (err) {
    res.status(500).end(err);
  }
});
