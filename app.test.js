const request = require('supertest');

const mockSend = jest.fn();

jest.mock('@aws-sdk/client-sqs', () => {
  class SQSClient {
    constructor() {
      this.send = mockSend;
    }
  }

  class GetQueueUrlCommand {
    constructor(input) {
      this.input = input;
    }
  }

  class GetQueueAttributesCommand {
    constructor(input) {
      this.input = input;
    }
  }

  return { SQSClient, GetQueueUrlCommand, GetQueueAttributesCommand };
});

const originalEnv = process.env;

function loadApp() {
  jest.resetModules();
  return require('./app');
}

beforeEach(() => {
  jest.clearAllMocks();
  process.env = { ...originalEnv, AWS_REGION: 'us-west-2' };
  delete process.env.AWS_PROFILE;
});

afterAll(() => {
  process.env = originalEnv;
});

describe('GET /ping', () => {
  it('responds with pong', async () => {
    const { app } = loadApp();
    const response = await request(app).get('/ping');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('pong');
  });
});

describe('GET /metrics', () => {
  it('responds with metrics for configured queues', async () => {
    process.env.SQS_QUEUES = 'queue-a,queue-b';

    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueName) {
        return { QueueUrl: `https://example.com/${command.input.QueueName}` };
      }

      if (command.input.QueueUrl) {
        return {
          Attributes: {
            ApproximateNumberOfMessages: '10',
            ApproximateNumberOfMessagesNotVisible: '5',
            ApproximateNumberOfMessagesDelayed: '2'
          }
        };
      }

      return {};
    });

    const { app } = loadApp();
    const response = await request(app).get('/metrics');

    expect(response.statusCode).toBe(200);
    expect(response.text).toContain('sqs_approximate_number_of_messages');
    expect(response.text).toContain('queueName="queue-a"');
    expect(response.text).toContain('queueName="queue-b"');
  });
});

describe('startServer', () => {
  it('fails fast when queues are not configured', async () => {
    delete process.env.SQS_QUEUES;
    const { startServer } = loadApp();
    await expect(startServer()).rejects.toThrow('SQS_QUEUES must include at least one queue name.');
  });
});
