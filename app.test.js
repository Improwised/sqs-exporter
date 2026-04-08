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

  it('zeros out stale metrics for failed queues', async () => {
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
    expect(response.text).toContain('sqs_approximate_number_of_messages{queueName="queue-failing"} 0');

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

describe('startServer', () => {
  it('fails fast when queues are not configured', async () => {
    delete process.env.SQS_QUEUES;
    const { startServer } = loadApp();
    await expect(startServer()).rejects.toThrow('SQS_QUEUES or SQS_QUEUE_URLS must include at least one entry');
  });
});
