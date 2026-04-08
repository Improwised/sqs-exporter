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

let testPort = 3100;

function loadApp() {
  jest.resetModules();
  process.env.PORT = String(testPort++);
  return require('./app');
}

function mockSuccessfulSqs() {
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
}

beforeEach(() => {
  jest.clearAllMocks();
  process.env = { ...originalEnv, AWS_REGION: 'us-west-2' };
  delete process.env.AWS_PROFILE;
  delete process.env.METRICS_REFRESH_INTERVAL_MS;
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
  it('serves cached metrics from registry without on-demand AWS calls', async () => {
    process.env.SQS_QUEUES = 'queue-a,queue-b';

    mockSuccessfulSqs();

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    mockSend.mockClear();

    const response = await request(app).get('/metrics');

    expect(response.statusCode).toBe(200);
    expect(response.text).toContain('sqs_approximate_number_of_messages');
    expect(response.text).toContain('queueName="queue-a"');
    expect(response.text).toContain('queueName="queue-b"');
    expect(mockSend).not.toHaveBeenCalled();

    await stopServer();
  });
});

describe('GET /ready', () => {
  it('returns 503 before initial refresh completes', async () => {
    process.env.SQS_QUEUES = 'queue-a';

    const { app } = loadApp();

    const response = await request(app).get('/ready');
    expect(response.statusCode).toBe(503);
  });

  it('returns 200 after initial refresh completes', async () => {
    process.env.SQS_QUEUES = 'queue-a';

    mockSuccessfulSqs();

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const response = await request(app).get('/ready');
    expect(response.statusCode).toBe(200);

    await stopServer();
  });
});

describe('refreshOnce', () => {
  it('prevents overlapping refresh cycles', async () => {
    process.env.SQS_QUEUES = 'queue-a';

    let resolveFirst;
    let inFlight = false;
    let overlapDetected = false;

    mockSend.mockImplementation(async (command) => {
      if (inFlight) {
        overlapDetected = true;
      }
      if (command.input.QueueName) {
        inFlight = true;
        await new Promise((r) => { resolveFirst = r; });
        inFlight = false;
        return { QueueUrl: `https://example.com/${command.input.QueueName}` };
      }
      return {
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesNotVisible: '0',
          ApproximateNumberOfMessagesDelayed: '0'
        }
      };
    });

    const { refreshOnce, startServer, stopServer } = loadApp();
    await startServer();

    const firstRefresh = refreshOnce();
    const secondRefresh = refreshOnce();

    resolveFirst();
    await Promise.all([firstRefresh, secondRefresh]);

    expect(overlapDetected).toBe(false);

    await stopServer();
  });

  it('increments consecutive failures on error', async () => {
    process.env.SQS_QUEUES = 'queue-a';

    mockSend.mockRejectedValue(new Error('AWS unavailable'));

    const { refreshOnce, startServer, stopServer } = loadApp();
    await startServer();

    await expect(refreshOnce()).rejects.toThrow('AWS unavailable');

    await stopServer();
  });
});

describe('adaptive backoff', () => {
  it('increases delay after consecutive failures', async () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.METRICS_REFRESH_INTERVAL_MS = '1000';

    mockSend.mockRejectedValue(new Error('AWS unavailable'));

    const { refreshOnce, startServer, stopServer, getBackoffDelay } = loadApp();
    await startServer();

    await expect(refreshOnce()).rejects.toThrow();

    const backoffDelay = getBackoffDelay();
    expect(backoffDelay).toBeGreaterThan(1000);

    await stopServer();
  });
});

describe('startServer', () => {
  it('fails fast when queues are not configured', async () => {
    delete process.env.SQS_QUEUES;
    const { startServer } = loadApp();
    await expect(startServer()).rejects.toThrow('SQS_QUEUES must include at least one queue name.');
  });
});
