const request = require('supertest');
const { app } = require('./app');
const AWSMock = require('aws-sdk-mock');
const { SQSClient } = require('@aws-sdk/client-sqs');

// Mock AWS SQS and CloudWatch services
AWSMock.setSDKInstance(SQSClient);

describe('GET /ping', () => {
  it('responds with pong', async () => {
    const response = await request(app).get('/ping');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('pong');
  });
});

describe('GET /metrics', () => {
  beforeAll(() => {
    AWSMock.mock('SQS', 'getQueueUrl', (params, callback) => {
      callback(null, { QueueUrl: `http://example.com/${params.QueueName}` });
    });

    AWSMock.mock('SQS', 'getQueueAttributes', (params, callback) => {
      callback(null, {
        Attributes: {
          ApproximateNumberOfMessages: '10',
          ApproximateNumberOfMessagesNotVisible: '5',
          ApproximateNumberOfMessagesDelayed: '2'
        }
      });
    });
  });

  afterAll(() => {
    AWSMock.restore('SQS');
  });

  it('responds with metrics', async () => {
    process.env.SQS_QUEUES = 'testQueue';
    const response = await request(app).get('/metrics');
    expect(response.statusCode).toBe(200);
    expect(response.text).toContain('sqs_approximate_number_of_messages');
    expect(response.text).toContain('sqs_approximate_number_of_messages_not_visible');
    expect(response.text).toContain('sqs_approximate_number_of_messages_delayed');
  });
});
