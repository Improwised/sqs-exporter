const express = require('express');
const AWS = require('aws-sdk');

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
const sqs = new AWS.SQS();

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
async function getSQSMetrics(queueUrl) {
  try {
    const params = {
      QueueUrl: queueUrl,
      AttributeNames: ['All']
    };
    const data = await sqs.getQueueAttributes(params).promise();
    return data.Attributes;
  } catch (error) {
    console.error('Error:', error.stack);
    throw error;
  }
}

module.exports = {
  getQueueUrl,
  getSQSMetrics
};
