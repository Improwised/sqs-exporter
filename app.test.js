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

  class ListQueuesCommand {
    constructor(input) {
      this.input = input;
    }
  }

  return { SQSClient, GetQueueUrlCommand, GetQueueAttributesCommand, ListQueuesCommand };
});

const mockCloudWatchSend = jest.fn();

jest.mock('@aws-sdk/client-cloudwatch', () => {
  class CloudWatchClient {
    constructor() {
      this.send = mockCloudWatchSend;
    }
  }

  class GetMetricDataCommand {
    constructor(input) {
      this.input = input;
    }
  }

  return { CloudWatchClient, GetMetricDataCommand };
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
    if (command.input.QueueNamePrefix !== undefined) {
      return { QueueUrls: [] };
    }
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
  delete process.env.SQS_DISCOVERY_TTL_MS;
  delete process.env.SQS_MAX_DISCOVERED_QUEUES;
  delete process.env.CW_METRICS_ENABLED;
  delete process.env.CW_METRICS_REFRESH_INTERVAL_MS;
  delete process.env.CW_METRICS_DELAY_SECONDS;
  delete process.env.SQS_QUEUE_URLS;
  delete process.env.SQS_MAX_CONCURRENCY;
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

    let inFlight = false;
    let overlapDetected = false;

    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueNamePrefix !== undefined) {
        return { QueueUrls: [] };
      }
      if (inFlight) {
        overlapDetected = true;
      }
      if (command.input.QueueName) {
        inFlight = true;
        await new Promise((r) => setTimeout(r, 50));
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

    await Promise.all([firstRefresh, secondRefresh]);

    expect(overlapDetected).toBe(false);

    await stopServer();
  });

  it('increments consecutive failures when all queues fail', async () => {
    process.env.SQS_QUEUES = 'queue-a';

    mockSend.mockRejectedValue(new Error('AWS unavailable'));

    const { refreshOnce, startServer, stopServer, getBackoffDelay } = loadApp();
    await startServer();

    await refreshOnce();
    expect(getBackoffDelay()).toBeGreaterThan(15000);

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

    await refreshOnce();

    const backoffDelay = getBackoffDelay();
    expect(backoffDelay).toBeGreaterThan(1000);

    await stopServer();
  });
});

describe('bounded concurrency', () => {
  it('respects SQS_MAX_CONCURRENCY=1 (serial execution)', async () => {
    process.env.SQS_QUEUES = 'q1,q2,q3';
    process.env.SQS_MAX_CONCURRENCY = '1';

    const callOrder = [];
    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueName) {
        callOrder.push(command.input.QueueName);
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
    await refreshOnce();

    expect(callOrder).toEqual(['q1', 'q2', 'q3']);

    await stopServer();
  });

  it('allows parallel execution when SQS_MAX_CONCURRENCY > 1', async () => {
    process.env.SQS_QUEUES = 'q1,q2,q3,q4';
    process.env.SQS_MAX_CONCURRENCY = '2';

    let maxConcurrent = 0;
    let currentConcurrent = 0;

    mockSend.mockImplementation(async (command) => {
      currentConcurrent++;
      maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
      try {
        if (command.input.QueueName) {
          return { QueueUrl: `https://example.com/${command.input.QueueName}` };
        }
        await new Promise((r) => setTimeout(r, 20));
        return {
          Attributes: {
            ApproximateNumberOfMessages: '1',
            ApproximateNumberOfMessagesNotVisible: '0',
            ApproximateNumberOfMessagesDelayed: '0'
          }
        };
      } finally {
        currentConcurrent--;
      }
    });

    const { refreshOnce, startServer, stopServer } = loadApp();
    await startServer();
    await refreshOnce();

    expect(maxConcurrent).toBeLessThanOrEqual(4);
    expect(maxConcurrent).toBeGreaterThan(1);

    await stopServer();
  });
});

describe('per-queue failure isolation', () => {
  it('one failing queue does not cause /metrics to return 500', async () => {
    process.env.SQS_QUEUES = 'queue-good,queue-bad';

    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueName === 'queue-bad') {
        throw new Error('Access denied');
      }
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

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const response = await request(app).get('/metrics');
    expect(response.statusCode).toBe(200);

    await stopServer();
  });

  it('reports queue_up=1 for healthy queues and queue_up=0 for failed queues', async () => {
    process.env.SQS_QUEUES = 'queue-good,queue-bad';

    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueName === 'queue-bad') {
        throw new Error('Access denied');
      }
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

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const response = await request(app).get('/metrics');
    expect(response.text).toContain('sqs_exporter_queue_up{queueName="queue-good"} 1');
    expect(response.text).toContain('sqs_exporter_queue_up{queueName="queue-bad"} 0');

    await stopServer();
  });

  it('still publishes metrics for healthy queues when one fails', async () => {
    process.env.SQS_QUEUES = 'queue-good,queue-bad';

    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueName === 'queue-bad') {
        throw new Error('Access denied');
      }
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

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const response = await request(app).get('/metrics');
    expect(response.text).toContain('queueName="queue-good"');
    expect(response.text).toContain('sqs_approximate_number_of_messages{queueName="queue-good"}');

    await stopServer();
  });

  it('drops stale metrics for failed queues instead of reporting zero', async () => {
    process.env.SQS_QUEUES = 'queue-failing';

    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueName) {
        return { QueueUrl: `https://example.com/${command.input.QueueName}` };
      }
      return {
        Attributes: {
          ApproximateNumberOfMessages: '100',
          ApproximateNumberOfMessagesNotVisible: '50',
          ApproximateNumberOfMessagesDelayed: '20'
        }
      };
    });

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    let response = await request(app).get('/metrics');
    expect(response.text).toContain('sqs_approximate_number_of_messages{queueName="queue-failing"} 100');

    mockSend.mockRejectedValue(new Error('AWS unavailable'));
    await refreshOnce();

    response = await request(app).get('/metrics');
    expect(response.text).not.toContain('sqs_approximate_number_of_messages{queueName="queue-failing"}');
    expect(response.text).toContain('sqs_exporter_queue_up{queueName="queue-failing"} 0');

    await stopServer();
  });

  it('logs state transition from UP to DOWN only once', async () => {
    process.env.SQS_QUEUES = 'queue-a';

    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueName) {
        return { QueueUrl: `https://example.com/${command.input.QueueName}` };
      }
      return {
        Attributes: {
          ApproximateNumberOfMessages: '5',
          ApproximateNumberOfMessagesNotVisible: '0',
          ApproximateNumberOfMessagesDelayed: '0'
        }
      };
    });

    const { startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const consoleSpy = jest.spyOn(console, 'error').mockImplementation(() => {});

    mockSend.mockRejectedValue(new Error('AWS unavailable'));
    await refreshOnce();

    const downLogs = consoleSpy.mock.calls.filter((c) =>
      c[0].includes('queue-a') && c[0].includes('DOWN')
    );
    expect(downLogs.length).toBe(1);

    await refreshOnce();
    const downLogs2 = consoleSpy.mock.calls.filter((c) =>
      c[0].includes('queue-a') && c[0].includes('DOWN')
    );
    expect(downLogs2.length).toBe(1);

    consoleSpy.mockRestore();
    await stopServer();
  });
});

describe('queue URL cache', () => {
  it('caches queue URL after first lookup', async () => {
    process.env.SQS_QUEUES = 'queue-a';

    mockSuccessfulSqs();

    const { startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    mockSend.mockClear();
    await refreshOnce();

    const getQueueUrlCalls = mockSend.mock.calls.filter(
      (c) => c[0].input && c[0].input.QueueName
    );
    expect(getQueueUrlCalls.length).toBe(0);

    await stopServer();
  });

  it('invalidates cache on queue-not-found error', async () => {
    process.env.SQS_QUEUES = 'queue-a';

    let callCount = 0;
    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueName) {
        callCount++;
        if (callCount > 1) {
          const err = new Error('Queue does not exist');
          err.name = 'QueueDoesNotExist';
          throw err;
        }
        return { QueueUrl: 'https://example.com/queue-a' };
      }
      if (command.input.QueueUrl) {
        return {
          Attributes: {
            ApproximateNumberOfMessages: '5',
            ApproximateNumberOfMessagesNotVisible: '0',
            ApproximateNumberOfMessagesDelayed: '0'
          }
        };
      }
      return {};
    });

    const { startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const err = new Error('Queue does not exist');
    err.name = 'QueueDoesNotExist';
    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueName) {
        throw err;
      }
      throw err;
    });

    await refreshOnce();

    mockSend.mockClear();
    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueName) {
        return { QueueUrl: 'https://example.com/queue-a-new' };
      }
      return {
        Attributes: {
          ApproximateNumberOfMessages: '3',
          ApproximateNumberOfMessagesNotVisible: '0',
          ApproximateNumberOfMessagesDelayed: '0'
        }
      };
    });

    await refreshOnce();

    const queueUrlCalls = mockSend.mock.calls.filter(
      (c) => c[0].input && c[0].input.QueueName
    );
    expect(queueUrlCalls.length).toBe(1);

    await stopServer();
  });
});

describe('graceful shutdown', () => {
  it('stopServer stops the refresh loop and HTTP server', async () => {
    process.env.SQS_QUEUES = 'queue-a';

    mockSuccessfulSqs();

    const { startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    await stopServer();

    expect(true).toBe(true);
  });

  it('handles SIGTERM by shutting down gracefully', async () => {
    process.env.SQS_QUEUES = 'queue-a';

    mockSuccessfulSqs();

    const { startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const shutdownPromise = new Promise((resolve) => {
      process.once('exit', resolve);
    });

    await stopServer();

    expect(true).toBe(true);
  });

  it('GRACEFUL_SHUTDOWN_TIMEOUT_MS is validated', async () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.GRACEFUL_SHUTDOWN_TIMEOUT_MS = 'abc';

    const { startServer, stopServer } = loadApp();
    await expect(startServer()).rejects.toThrow('GRACEFUL_SHUTDOWN_TIMEOUT_MS must be a positive integer');
  });

  it('GRACEFUL_SHUTDOWN_TIMEOUT_MS defaults to 10000', async () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';

    const { loadConfig } = require('./config');
    const config = loadConfig();

    expect(config.gracefulShutdownTimeoutMs).toBe(10000);
  });
});

describe('prefix-based queue discovery', () => {
  it('resolves prefix tokens ending with * via ListQueues', async () => {
    process.env.SQS_QUEUES = 'orders-*,my-exact-queue';

    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueNamePrefix !== undefined) {
        return {
          QueueUrls: [
            'https://sqs.us-west-2.amazonaws.com/123/orders-created',
            'https://sqs.us-west-2.amazonaws.com/123/orders-shipped'
          ]
        };
      }
      if (command.input.QueueName) {
        return { QueueUrl: `https://sqs.us-west-2.amazonaws.com/123/${command.input.QueueName}` };
      }
      return {
        Attributes: {
          ApproximateNumberOfMessages: '5',
          ApproximateNumberOfMessagesNotVisible: '0',
          ApproximateNumberOfMessagesDelayed: '0'
        }
      };
    });

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const response = await request(app).get('/metrics');
    expect(response.text).toContain('queueName="orders-created"');
    expect(response.text).toContain('queueName="orders-shipped"');
    expect(response.text).toContain('queueName="my-exact-queue"');

    await stopServer();
  });

  it('warns and continues when prefix has no matches', async () => {
    process.env.SQS_QUEUES = 'nonexistent-*,exact-queue';

    const consoleSpy = jest.spyOn(console, 'error').mockImplementation(() => {});

    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueNamePrefix !== undefined) {
        return { QueueUrls: [] };
      }
      if (command.input.QueueName) {
        return { QueueUrl: `https://sqs.us-west-2.amazonaws.com/123/${command.input.QueueName}` };
      }
      return {
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesNotVisible: '0',
          ApproximateNumberOfMessagesDelayed: '0'
        }
      };
    });

    const { startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const warnCalls = consoleSpy.mock.calls.filter((c) =>
      c[0].includes('nonexistent-*') && c[0].includes('no matches')
    );
    expect(warnCalls.length).toBeGreaterThanOrEqual(1);

    consoleSpy.mockRestore();
    await stopServer();
  });

  it('caches discovery results within TTL', async () => {
    process.env.SQS_QUEUES = 'orders-*';
    process.env.SQS_DISCOVERY_TTL_MS = '60000';

    let listQueuesCallCount = 0;
    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueNamePrefix !== undefined) {
        listQueuesCallCount++;
        return {
          QueueUrls: [
            'https://sqs.us-west-2.amazonaws.com/123/orders-created'
          ]
        };
      }
      if (command.input.QueueName) {
        return { QueueUrl: `https://sqs.us-west-2.amazonaws.com/123/${command.input.QueueName}` };
      }
      return {
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesNotVisible: '0',
          ApproximateNumberOfMessagesDelayed: '0'
        }
      };
    });

    const { startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const callsAfterFirst = listQueuesCallCount;

    await refreshOnce();
    expect(listQueuesCallCount).toBe(callsAfterFirst);

    await stopServer();
  });

  it('caps discovered queues at SQS_MAX_DISCOVERED_QUEUES', async () => {
    process.env.SQS_QUEUES = 'orders-*';
    process.env.SQS_MAX_DISCOVERED_QUEUES = '2';

    const consoleSpy = jest.spyOn(console, 'error').mockImplementation(() => {});

    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueNamePrefix !== undefined) {
        return {
          QueueUrls: [
            'https://sqs.us-west-2.amazonaws.com/123/orders-1',
            'https://sqs.us-west-2.amazonaws.com/123/orders-2',
            'https://sqs.us-west-2.amazonaws.com/123/orders-3'
          ]
        };
      }
      if (command.input.QueueName) {
        return { QueueUrl: `https://sqs.us-west-2.amazonaws.com/123/${command.input.QueueName}` };
      }
      return {
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesNotVisible: '0',
          ApproximateNumberOfMessagesDelayed: '0'
        }
      };
    });

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const response = await request(app).get('/metrics');
    expect(response.text).toContain('queueName="orders-1"');
    expect(response.text).toContain('queueName="orders-2"');
    expect(response.text).not.toContain('queueName="orders-3"');

    const warnCalls = consoleSpy.mock.calls.filter((c) =>
      c[0].includes('exceeds') || c[0].includes('SQS_MAX_DISCOVERED_QUEUES')
    );
    expect(warnCalls.length).toBeGreaterThanOrEqual(1);

    consoleSpy.mockRestore();
    await stopServer();
  });
});

describe('metric contract', () => {
  it('exports all required SQS metric names', async () => {
    process.env.SQS_QUEUES = 'contract-queue';

    mockSuccessfulSqs();

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const response = await request(app).get('/metrics');
    const text = response.text;

    const requiredMetrics = [
      'sqs_approximate_number_of_messages',
      'sqs_approximate_number_of_messages_not_visible',
      'sqs_approximate_number_of_messages_delayed',
      'sqs_exporter_queue_up',
      'sqs_exporter_last_refresh_timestamp',
      'sqs_exporter_refresh_duration_seconds',
      'sqs_exporter_refresh_failures_total',
      'sqs_exporter_queue_refresh_total'
    ];

    requiredMetrics.forEach((metric) => {
      expect(text).toContain(metric);
    });

    await stopServer();
  });

  it('all SQS metrics have queueName label', async () => {
    process.env.SQS_QUEUES = 'label-test-queue';

    mockSuccessfulSqs();

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const response = await request(app).get('/metrics');
    const text = response.text;

    const queueLabelMetrics = [
      'sqs_approximate_number_of_messages{queueName="label-test-queue"}',
      'sqs_approximate_number_of_messages_not_visible{queueName="label-test-queue"}',
      'sqs_approximate_number_of_messages_delayed{queueName="label-test-queue"}',
      'sqs_exporter_queue_up{queueName="label-test-queue"}'
    ];

    queueLabelMetrics.forEach((metric) => {
      expect(text).toContain(metric);
    });

    await stopServer();
  });
});

describe('self-observability metrics', () => {
  it('exposes last successful refresh timestamp', async () => {
    process.env.SQS_QUEUES = 'queue-a';

    mockSuccessfulSqs();

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const response = await request(app).get('/metrics');
    expect(response.text).toContain('sqs_exporter_last_refresh_timestamp');

    await stopServer();
  });

  it('exposes refresh duration', async () => {
    process.env.SQS_QUEUES = 'queue-a';

    mockSuccessfulSqs();

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const response = await request(app).get('/metrics');
    expect(response.text).toContain('sqs_exporter_refresh_duration_seconds');

    await stopServer();
  });

  it('increments refresh failures counter on total failure', async () => {
    process.env.SQS_QUEUES = 'queue-a';

    mockSend.mockRejectedValue(new Error('AWS unavailable'));

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const response = await request(app).get('/metrics');
    expect(response.text).toContain('sqs_exporter_refresh_failures_total 1');

    await stopServer();
  });

  it('tracks per-refresh queue success and failure counts', async () => {
    process.env.SQS_QUEUES = 'queue-good,queue-bad';

    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueName === 'queue-bad') {
        throw new Error('Access denied');
      }
      if (command.input.QueueName) {
        return { QueueUrl: `https://example.com/${command.input.QueueName}` };
      }
      return {
        Attributes: {
          ApproximateNumberOfMessages: '10',
          ApproximateNumberOfMessagesNotVisible: '5',
          ApproximateNumberOfMessagesDelayed: '2'
        }
      };
    });

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const response = await request(app).get('/metrics');
    expect(response.text).toContain('sqs_exporter_queue_refresh_total{result="success"} 1');
    expect(response.text).toContain('sqs_exporter_queue_refresh_total{result="failure"} 1');

    await stopServer();
  });
});

describe('FIFO in-flight groups metric', () => {
  it('exposes ApproximateNumberOfGroupsWithInflightMessages for FIFO queues', async () => {
    process.env.SQS_QUEUES = 'my-queue.fifo';

    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueName) {
        return { QueueUrl: `https://example.com/${command.input.QueueName}` };
      }
      if (command.input.QueueUrl) {
        return {
          Attributes: {
            ApproximateNumberOfMessages: '10',
            ApproximateNumberOfMessagesNotVisible: '5',
            ApproximateNumberOfMessagesDelayed: '2',
            ApproximateNumberOfGroupsWithInflightMessages: '3'
          }
        };
      }
      return {};
    });

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const response = await request(app).get('/metrics');
    expect(response.text).toContain('sqs_approximate_number_of_groups_with_inflight_messages{queueName="my-queue.fifo"} 3');

    await stopServer();
  });

  it('does not fail when ApproximateNumberOfGroupsWithInflightMessages is missing', async () => {
    process.env.SQS_QUEUES = 'standard-queue';

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

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const response = await request(app).get('/metrics');
    expect(response.statusCode).toBe(200);
    expect(response.text).not.toContain('sqs_approximate_number_of_groups_with_inflight_messages{queueName="standard-queue"}');

    await stopServer();
  });
});

describe('per-queue backoff', () => {
  it('retries a failed queue on the next cycle, then skips cycles after repeated failures', async () => {
    process.env.SQS_QUEUES = 'queue-good,queue-bad';

    const attributeCalls = { 'queue-good': 0, 'queue-bad': 0 };
    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueName) {
        return { QueueUrl: `https://example.com/${command.input.QueueName}` };
      }
      const queueName = command.input.QueueUrl.split('/').pop();
      attributeCalls[queueName]++;
      if (queueName === 'queue-bad') {
        throw new Error('Access denied');
      }
      return {
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesNotVisible: '0',
          ApproximateNumberOfMessagesDelayed: '0'
        }
      };
    });

    const { startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();
    await refreshOnce();
    await refreshOnce();
    await refreshOnce();

    expect(attributeCalls['queue-good']).toBe(4);
    // 1st failure retries immediately; 2nd failure starts skipping cycles.
    expect(attributeCalls['queue-bad']).toBe(2);

    await stopServer();
  });

  it('does not invalidate the cached queue URL on non-missing-queue errors', async () => {
    process.env.SQS_QUEUES = 'queue-a';

    mockSuccessfulSqs();

    const { startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    const throttled = new Error('Rate exceeded');
    throttled.name = 'ThrottlingException';
    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueUrl) {
        throw throttled;
      }
      return { QueueUrl: 'https://example.com/queue-a' };
    });
    await refreshOnce();

    mockSuccessfulSqs();
    mockSend.mockClear();
    await refreshOnce();

    const queueUrlCalls = mockSend.mock.calls.filter((c) => c[0].input.QueueName);
    expect(queueUrlCalls.length).toBe(0);

    await stopServer();
  });
});

describe('removed queues', () => {
  it('drops all series for discovered queues that disappear', async () => {
    process.env.SQS_QUEUES = 'orders-*';
    process.env.SQS_DISCOVERY_TTL_MS = '1';

    let queueUrls = [
      'https://sqs.us-west-2.amazonaws.com/123/orders-1',
      'https://sqs.us-west-2.amazonaws.com/123/orders-2'
    ];
    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueNamePrefix !== undefined) {
        return { QueueUrls: queueUrls };
      }
      return {
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesNotVisible: '0',
          ApproximateNumberOfMessagesDelayed: '0'
        }
      };
    });

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    let response = await request(app).get('/metrics');
    expect(response.text).toContain('queueName="orders-2"');

    queueUrls = ['https://sqs.us-west-2.amazonaws.com/123/orders-1'];
    await new Promise((r) => setTimeout(r, 5));
    await refreshOnce();

    response = await request(app).get('/metrics');
    expect(response.text).toContain('queueName="orders-1"');
    expect(response.text).not.toContain('queueName="orders-2"');

    await stopServer();
  });

  it('keeps previously discovered queues when ListQueues fails', async () => {
    process.env.SQS_QUEUES = 'orders-*';
    process.env.SQS_DISCOVERY_TTL_MS = '1';

    let listQueuesFails = false;
    mockSend.mockImplementation(async (command) => {
      if (command.input.QueueNamePrefix !== undefined) {
        if (listQueuesFails) {
          throw new Error('Rate exceeded');
        }
        return { QueueUrls: ['https://sqs.us-west-2.amazonaws.com/123/orders-1'] };
      }
      return {
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesNotVisible: '0',
          ApproximateNumberOfMessagesDelayed: '0'
        }
      };
    });

    const consoleSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();

    listQueuesFails = true;
    await new Promise((r) => setTimeout(r, 5));
    await refreshOnce();

    const response = await request(app).get('/metrics');
    expect(response.text).toContain('sqs_approximate_number_of_messages{queueName="orders-1"} 1');

    consoleSpy.mockRestore();
    await stopServer();
  });
});

describe('AWS request accounting', () => {
  it('counts AWS API requests by api and result', async () => {
    process.env.SQS_QUEUES = 'queue-a';

    mockSuccessfulSqs();

    const { app, startServer, stopServer, refreshOnce } = loadApp();
    await startServer();
    await refreshOnce();
    await refreshOnce();

    const response = await request(app).get('/metrics');
    expect(response.text).toContain('sqs_exporter_aws_requests_total{api="GetQueueUrl",result="success"} 1');
    expect(response.text).toContain('sqs_exporter_aws_requests_total{api="GetQueueAttributes",result="success"} 2');

    await stopServer();
  });
});

function mockSqsWithCounts(countsByQueue) {
  mockSend.mockImplementation(async (command) => {
    if (command.input.QueueNamePrefix !== undefined) {
      return { QueueUrls: [] };
    }
    if (command.input.QueueName) {
      return { QueueUrl: `https://sqs.us-west-2.amazonaws.com/123/${command.input.QueueName}` };
    }
    const queueName = command.input.QueueUrl.split('/').pop();
    const [visible, notVisible] = countsByQueue[queueName] || [0, 0];
    return {
      Attributes: {
        ApproximateNumberOfMessages: String(visible),
        ApproximateNumberOfMessagesNotVisible: String(notVisible),
        ApproximateNumberOfMessagesDelayed: '0'
      }
    };
  });
}

function mockCloudWatchValues(valuesByQueue) {
  mockCloudWatchSend.mockImplementation(async (command) => ({
    MetricDataResults: command.input.MetricDataQueries.map((q) => ({
      Id: q.Id,
      Values: valuesByQueue[q.MetricStat.Metric.Dimensions[0].Value] || []
    }))
  }));
}

describe('CloudWatch deletions metric', () => {
  it('exposes the sum of 1-minute buckets as sqs_messages_deleted_last_5m', async () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.CW_METRICS_ENABLED = 'true';

    mockSqsWithCounts({ 'queue-a': [10, 0] });
    mockCloudWatchValues({ 'queue-a': [10, 12, 0, 20] });

    const { app, startServer, stopServer, refreshOnce, refreshCloudWatchMetrics } = loadApp();
    await startServer();
    await refreshOnce();
    await refreshCloudWatchMetrics();

    const response = await request(app).get('/metrics');
    expect(response.text).toContain('sqs_messages_deleted_last_5m{queueName="queue-a"} 42');

    await stopServer();
  });

  it('batches all queues into one GetMetricData request with 1-minute Sum queries', async () => {
    process.env.SQS_QUEUES = 'queue-a,queue-b';
    process.env.CW_METRICS_ENABLED = 'true';

    mockSqsWithCounts({ 'queue-a': [5, 0], 'queue-b': [0, 3] });
    mockCloudWatchValues({});

    const { startServer, stopServer, refreshOnce, refreshCloudWatchMetrics } = loadApp();
    await startServer();
    await refreshOnce();
    await refreshCloudWatchMetrics();

    expect(mockCloudWatchSend).toHaveBeenCalledTimes(1);
    const queries = mockCloudWatchSend.mock.calls[0][0].input.MetricDataQueries;
    expect(queries.map((q) => q.MetricStat.Metric.Dimensions[0].Value)).toEqual(['queue-a', 'queue-b']);
    expect(queries[0].MetricStat).toEqual({
      Metric: {
        Namespace: 'AWS/SQS',
        MetricName: 'NumberOfMessagesDeleted',
        Dimensions: [{ Name: 'QueueName', Value: 'queue-a' }]
      },
      Period: 60,
      Stat: 'Sum'
    });

    await stopServer();
  });

  it('only queries queues that hold messages', async () => {
    process.env.SQS_QUEUES = 'busy,idle';
    process.env.CW_METRICS_ENABLED = 'true';

    mockSqsWithCounts({ busy: [3, 1], idle: [0, 0] });
    mockCloudWatchValues({ busy: [1] });

    const { app, startServer, stopServer, refreshOnce, refreshCloudWatchMetrics } = loadApp();
    await startServer();
    await refreshOnce();
    await refreshCloudWatchMetrics();

    const queries = mockCloudWatchSend.mock.calls[0][0].input.MetricDataQueries;
    expect(queries.map((q) => q.MetricStat.Metric.Dimensions[0].Value)).toEqual(['busy']);

    const response = await request(app).get('/metrics');
    expect(response.text).toContain('sqs_messages_deleted_last_5m{queueName="busy"} 1');
    expect(response.text).not.toContain('sqs_messages_deleted_last_5m{queueName="idle"}');
    expect(response.text).toContain('sqs_exporter_cloudwatch_metrics_requested_total 1');

    await stopServer();
  });

  it('does not call CloudWatch when every queue is empty', async () => {
    process.env.SQS_QUEUES = 'idle';
    process.env.CW_METRICS_ENABLED = 'true';

    mockSqsWithCounts({ idle: [0, 0] });

    const { startServer, stopServer, refreshOnce, refreshCloudWatchMetrics } = loadApp();
    await startServer();
    await refreshOnce();
    await refreshCloudWatchMetrics();

    expect(mockCloudWatchSend).not.toHaveBeenCalled();

    await stopServer();
  });

  it('skips queues in a different region than AWS_REGION', async () => {
    process.env.SQS_QUEUE_URLS = 'https://sqs.eu-west-1.amazonaws.com/123/remote-queue';
    process.env.CW_METRICS_ENABLED = 'true';
    delete process.env.SQS_QUEUES;

    mockSend.mockImplementation(async () => ({
      Attributes: {
        ApproximateNumberOfMessages: '5',
        ApproximateNumberOfMessagesNotVisible: '0',
        ApproximateNumberOfMessagesDelayed: '0'
      }
    }));

    const { startServer, stopServer, refreshOnce, refreshCloudWatchMetrics } = loadApp();
    await startServer();
    await refreshOnce();
    await refreshCloudWatchMetrics();

    expect(mockCloudWatchSend).not.toHaveBeenCalled();

    await stopServer();
  });

  it('splits more than 500 queues across GetMetricData requests', async () => {
    const names = Array.from({ length: 501 }, (_, i) => `q-${i}`);
    process.env.SQS_QUEUES = names.join(',');
    process.env.SQS_MAX_CONCURRENCY = '50';
    process.env.CW_METRICS_ENABLED = 'true';

    mockSqsWithCounts(Object.fromEntries(names.map((n) => [n, [1, 0]])));
    mockCloudWatchValues({});

    const { startServer, stopServer, refreshOnce, refreshCloudWatchMetrics } = loadApp();
    await startServer();
    await refreshOnce();
    await refreshCloudWatchMetrics();

    expect(mockCloudWatchSend).toHaveBeenCalledTimes(2);
    expect(mockCloudWatchSend.mock.calls[0][0].input.MetricDataQueries).toHaveLength(500);
    expect(mockCloudWatchSend.mock.calls[1][0].input.MetricDataQueries).toHaveLength(1);

    await stopServer();
  });

  it('follows NextToken and merges values across pages', async () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.CW_METRICS_ENABLED = 'true';

    mockSqsWithCounts({ 'queue-a': [1, 0] });
    mockCloudWatchSend
      .mockResolvedValueOnce({ MetricDataResults: [{ Id: 'q0', Values: [1, 2] }], NextToken: 'next' })
      .mockResolvedValueOnce({ MetricDataResults: [{ Id: 'q0', Values: [3] }] });

    const { app, startServer, stopServer, refreshOnce, refreshCloudWatchMetrics } = loadApp();
    await startServer();
    await refreshOnce();
    await refreshCloudWatchMetrics();

    expect(mockCloudWatchSend.mock.calls[1][0].input.NextToken).toBe('next');
    const response = await request(app).get('/metrics');
    expect(response.text).toContain('sqs_messages_deleted_last_5m{queueName="queue-a"} 6');

    await stopServer();
  });

  it('reports no series (not zero) when CloudWatch returns no datapoints', async () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.CW_METRICS_ENABLED = 'true';

    mockSqsWithCounts({ 'queue-a': [1, 0] });
    mockCloudWatchValues({});

    const { app, startServer, stopServer, refreshOnce, refreshCloudWatchMetrics } = loadApp();
    await startServer();
    await refreshOnce();
    await refreshCloudWatchMetrics();

    const response = await request(app).get('/metrics');
    expect(response.text).not.toContain('sqs_messages_deleted_last_5m{queueName="queue-a"}');

    await stopServer();
  });

  it('drops the previous value when CloudWatch fails, and logs once per batch', async () => {
    process.env.SQS_QUEUES = 'queue-a,queue-b';
    process.env.CW_METRICS_ENABLED = 'true';

    mockSqsWithCounts({ 'queue-a': [1, 0], 'queue-b': [1, 0] });
    mockCloudWatchValues({ 'queue-a': [7], 'queue-b': [8] });

    const { app, startServer, stopServer, refreshOnce, refreshCloudWatchMetrics } = loadApp();
    await startServer();
    await refreshOnce();
    await refreshCloudWatchMetrics();

    let response = await request(app).get('/metrics');
    expect(response.text).toContain('sqs_messages_deleted_last_5m{queueName="queue-a"} 7');

    const consoleSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    mockCloudWatchSend.mockRejectedValue(new Error('AccessDenied: not authorized'));
    await refreshCloudWatchMetrics();

    response = await request(app).get('/metrics');
    expect(response.statusCode).toBe(200);
    expect(response.text).not.toContain('sqs_messages_deleted_last_5m{queueName="queue-a"}');
    expect(response.text).toContain('sqs_exporter_aws_requests_total{api="GetMetricData",result="error"} 1');
    expect(consoleSpy).toHaveBeenCalledTimes(1);

    consoleSpy.mockRestore();
    await stopServer();
  });

  it('drops the value once a queue drains', async () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.CW_METRICS_ENABLED = 'true';

    mockSqsWithCounts({ 'queue-a': [1, 0] });
    mockCloudWatchValues({ 'queue-a': [4] });

    const { app, startServer, stopServer, refreshOnce, refreshCloudWatchMetrics } = loadApp();
    await startServer();
    await refreshOnce();
    await refreshCloudWatchMetrics();

    mockSqsWithCounts({ 'queue-a': [0, 0] });
    await refreshOnce();
    await refreshCloudWatchMetrics();

    const response = await request(app).get('/metrics');
    expect(response.text).not.toContain('sqs_messages_deleted_last_5m{queueName="queue-a"}');

    await stopServer();
  });

  it('does not call CloudWatch when CW_METRICS_ENABLED is false (default)', async () => {
    process.env.SQS_QUEUES = 'queue-a';

    mockSuccessfulSqs();

    const { app, startServer, stopServer, refreshOnce, refreshCloudWatchMetrics } = loadApp();
    await startServer();
    await refreshOnce();
    await refreshCloudWatchMetrics();

    const response = await request(app).get('/metrics');
    expect(mockCloudWatchSend).not.toHaveBeenCalled();
    expect(response.text).not.toContain('sqs_messages_deleted_last_5m{queueName="queue-a"}');

    await stopServer();
  });
});

describe('getCloudWatchWindow', () => {
  it('ends on a whole minute, delaySeconds behind now, spanning 5 minutes', () => {
    const { getCloudWatchWindow } = loadApp();
    const now = Date.parse('2026-10-25T12:07:42Z');

    const { startTime, endTime } = getCloudWatchWindow(now, 120);

    expect(endTime.toISOString()).toBe('2026-10-25T12:05:00.000Z');
    expect(startTime.toISOString()).toBe('2026-10-25T12:00:00.000Z');
  });

  it('stays aligned to whole minutes when the delay is not a multiple of 60', () => {
    const { getCloudWatchWindow } = loadApp();
    const now = Date.parse('2026-10-25T12:07:42Z');

    const { startTime, endTime } = getCloudWatchWindow(now, 90);

    expect(endTime.toISOString()).toBe('2026-10-25T12:06:00.000Z');
    expect(startTime.toISOString()).toBe('2026-10-25T12:01:00.000Z');
  });
});

describe('startServer', () => {
  it('fails fast when queues are not configured', async () => {
    delete process.env.SQS_QUEUES;
    const { startServer } = loadApp();
    await expect(startServer()).rejects.toThrow('SQS_QUEUES or SQS_QUEUE_URLS must include at least one entry');
  });
});
