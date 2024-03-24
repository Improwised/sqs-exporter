const express = require('express');
const AWS = require('aws-sdk');
const client = require('prom-client');

// Set up Express web server
const app = express();
const PORT = process.env.PORT || 3001;

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
AWS.config.update({ region: 'us-east-1' });

const aws_access_key_id = process.env.AWS_ACCESS_KEY_ID;
const aws_secret_access_key = process.env.AWS_SECRET_ACCESS_KEY;
const cloudwatch = new AWS.CloudWatch();

// Add code here for handling errors and logging
async function getQueueUrl(queueName) {
  try {
    const params = {
      QueueName: queueName,
      AttributeNames: ['QueueArn']
    };
    const data = await sqs.getQueueUrl(params).promise();
    return data.QueueUrl;
  } catch (error) {
    console.error('Error:', error.stack);
    throw error;
  }
}

// Add code here for handling errors and logging
async function getSQSMetrics(queueName) {
  try {
    const endTime = new Date();
    const startTime = new Date(endTime.getTime() - 600000); // 10 minutes ago
    const metrics = [
      { Name: 'NumberOfMessagesSent', Statistic: 'Sum' },
      { Name: 'NumberOfMessagesReceived', Statistic: 'Sum' }
    ];
    const data = await Promise.all(metrics.map(metric =>
      cloudwatch.getMetricStatistics({
        Namespace: 'AWS/SQS',
        MetricName: metric.Name,
        Dimensions: [{ Name: 'QueueName', Value: queueName }],
        StartTime: startTime,
        EndTime: endTime,
        Period: 300,
        Statistics: [metric.Statistic]
      }).promise()
    ));

    // Set the custom metrics with the values from CloudWatch
    sqsNumberOfMessagesSent.set(data[0].Datapoints[0] ? data[0].Datapoints[0].Sum : 0);
    sqsNumberOfMessagesReceived.set(data[1].Datapoints[0] ? data[1].Datapoints[0].Sum : 0);

    return data.Attributes; // Keep returning the raw attributes for any other potential use
  } catch (error) {
    console.error('Error:', error.stack);
    throw error;
  }
}

module.exports = {
  getQueueUrl,
  getSQSMetrics
};
// Create a Prometheus Registry
const register = new client.Registry();

// Enable the collection of default metrics
client.collectDefaultMetrics({ register });

// Define custom metrics for SQS
const sqsNumberOfMessagesSent = new client.Gauge({
  name: 'sqs_number_of_messages_sent',
  help: 'Number of messages sent to the SQS queue'
});

const sqsNumberOfMessagesReceived = new client.Gauge({
  name: 'sqs_number_of_messages_received',
  help: 'Number of messages received from the SQS queue'
});

// Add custom metrics to the registry
register.registerMetric(sqsNumberOfMessagesSent);
register.registerMetric(sqsNumberOfMessagesReceived);

// Endpoint to serve the metrics
app.get('/metrics', async (req, res) => {
  try {
    const queueName = 'YourQueueName'; // Replace with your actual queue name
    await getSQSMetrics(queueName);
    const metrics = await register.metrics();
    res.set('Content-Type', register.contentType);
    res.end(metrics);
  } catch (err) {
    res.status(500).end(err);
  }
});

