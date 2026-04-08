const { loadConfig } = require('./config');

describe('loadConfig', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    jest.resetModules();
    process.env = { ...originalEnv };
  });

  afterAll(() => {
    process.env = originalEnv;
  });

  it('loads valid configuration from environment', () => {
    process.env.PORT = '9090';
    process.env.SQS_QUEUES = 'queue-a, queue-b';
    process.env.AWS_REGION = 'us-east-1';
    process.env.METRICS_REFRESH_INTERVAL_MS = '30000';

    const config = loadConfig();

    expect(config.port).toBe(9090);
    expect(config.queueNames).toEqual(['queue-a', 'queue-b']);
    expect(config.awsRegion).toBe('us-east-1');
    expect(config.refreshIntervalMs).toBe(30000);
  });

  it('applies defaults when env vars are not set', () => {
    process.env.SQS_QUEUES = 'my-queue';
    process.env.AWS_REGION = 'eu-west-1';
    delete process.env.PORT;
    delete process.env.METRICS_REFRESH_INTERVAL_MS;

    const config = loadConfig();

    expect(config.port).toBe(3001);
    expect(config.refreshIntervalMs).toBe(15000);
  });

  it('throws on empty SQS_QUEUES', () => {
    delete process.env.SQS_QUEUES;
    process.env.AWS_REGION = 'us-west-2';

    expect(() => loadConfig()).toThrow('SQS_QUEUES or SQS_QUEUE_URLS must include at least one entry');
  });

  it('throws on whitespace-only SQS_QUEUES', () => {
    process.env.SQS_QUEUES = '  ,  ,  ';
    process.env.AWS_REGION = 'us-west-2';

    expect(() => loadConfig()).toThrow('SQS_QUEUES or SQS_QUEUE_URLS must include at least one entry');
  });

  it('throws on invalid AWS_REGION', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'invalid-region';

    expect(() => loadConfig()).toThrow('AWS_REGION is invalid');
  });

  it('throws on negative PORT', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.PORT = '-1';

    expect(() => loadConfig()).toThrow('PORT must be a positive integer');
  });

  it('throws on zero PORT', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.PORT = '0';

    expect(() => loadConfig()).toThrow('PORT must be a positive integer');
  });

  it('throws on non-numeric PORT', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.PORT = 'abc';

    expect(() => loadConfig()).toThrow('PORT must be a positive integer');
  });

  it('throws on zero METRICS_REFRESH_INTERVAL_MS', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.METRICS_REFRESH_INTERVAL_MS = '0';

    expect(() => loadConfig()).toThrow('METRICS_REFRESH_INTERVAL_MS must be a positive integer');
  });

  it('throws on negative METRICS_REFRESH_INTERVAL_MS', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.METRICS_REFRESH_INTERVAL_MS = '-100';

    expect(() => loadConfig()).toThrow('METRICS_REFRESH_INTERVAL_MS must be a positive integer');
  });

  it('throws on non-numeric METRICS_REFRESH_INTERVAL_MS', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.METRICS_REFRESH_INTERVAL_MS = 'abc';

    expect(() => loadConfig()).toThrow('METRICS_REFRESH_INTERVAL_MS must be a positive integer');
  });

  it('returns frozen immutable config object', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';

    const config = loadConfig();

    expect(Object.isFrozen(config)).toBe(true);
    config.port = 9999;
    expect(config.port).toBe(3001);
  });

  it('uses default credential provider chain when AWS_PROFILE is not set', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    delete process.env.AWS_PROFILE;

    const config = loadConfig();

    expect(config.sqsConfig.region).toBe('us-west-2');
    expect(config.sqsConfig.credentials).toBeUndefined();
  });

  it('uses fromIni credentials when AWS_PROFILE is set', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.AWS_PROFILE = 'my-profile';

    const config = loadConfig();

    expect(config.sqsConfig.region).toBe('us-west-2');
    expect(config.sqsConfig.credentials).toBeDefined();
  });

  it('defaults SQS_MAX_CONCURRENCY to 5', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    delete process.env.SQS_MAX_CONCURRENCY;

    const config = loadConfig();

    expect(config.maxConcurrency).toBe(5);
  });

  it('throws on zero SQS_MAX_CONCURRENCY', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.SQS_MAX_CONCURRENCY = '0';

    expect(() => loadConfig()).toThrow('SQS_MAX_CONCURRENCY must be a positive integer');
  });

  it('throws on negative SQS_MAX_CONCURRENCY', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.SQS_MAX_CONCURRENCY = '-1';

    expect(() => loadConfig()).toThrow('SQS_MAX_CONCURRENCY must be a positive integer');
  });

  it('throws on non-numeric SQS_MAX_CONCURRENCY', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.SQS_MAX_CONCURRENCY = 'abc';

    expect(() => loadConfig()).toThrow('SQS_MAX_CONCURRENCY must be a positive integer');
  });

  it('accepts SQS_QUEUE_URLS alongside SQS_QUEUES', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.SQS_QUEUE_URLS = 'https://sqs.us-east-1.amazonaws.com/123456789/queue-b';

    const config = loadConfig();

    expect(config.queueNames).toContain('queue-a');
    expect(config.queueUrls).toEqual([{ url: 'https://sqs.us-east-1.amazonaws.com/123456789/queue-b', name: 'queue-b' }]);
  });

  it('runs with only SQS_QUEUE_URLS (no SQS_QUEUES)', () => {
    process.env.AWS_REGION = 'us-west-2';
    process.env.SQS_QUEUE_URLS = 'https://sqs.us-west-2.amazonaws.com/123456789/my-queue';

    const config = loadConfig();

    expect(config.queueNames).toEqual([]);
    expect(config.queueUrls).toEqual([{ url: 'https://sqs.us-west-2.amazonaws.com/123456789/my-queue', name: 'my-queue' }]);
  });

  it('throws when both SQS_QUEUES and SQS_QUEUE_URLS are empty', () => {
    process.env.AWS_REGION = 'us-west-2';
    delete process.env.SQS_QUEUES;
    delete process.env.SQS_QUEUE_URLS;

    expect(() => loadConfig()).toThrow('SQS_QUEUES or SQS_QUEUE_URLS must include at least one entry');
  });

  it('throws on invalid SQS_QUEUE_URLS format', () => {
    process.env.AWS_REGION = 'us-west-2';
    process.env.SQS_QUEUE_URLS = 'not-a-url';

    expect(() => loadConfig()).toThrow('SQS_QUEUE_URLS contains invalid URL');
  });

  it('derives queue name from URL', () => {
    process.env.AWS_REGION = 'us-west-2';
    process.env.SQS_QUEUE_URLS = 'https://sqs.us-east-1.amazonaws.com/123456789012/my-fifo-queue.fifo';

    const config = loadConfig();

    expect(config.queueUrls[0].name).toBe('my-fifo-queue.fifo');
  });

  it('prevents duplicate queue names between SQS_QUEUES and SQS_QUEUE_URLS', () => {
    process.env.SQS_QUEUES = 'my-queue';
    process.env.AWS_REGION = 'us-west-2';
    process.env.SQS_QUEUE_URLS = 'https://sqs.us-west-2.amazonaws.com/123456789/my-queue';

    expect(() => loadConfig()).toThrow('Duplicate queue name');
  });

  it('adds SQS_MAX_ATTEMPTS to sqsConfig', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.SQS_MAX_ATTEMPTS = '5';

    const config = loadConfig();

    expect(config.sqsConfig.maxAttempts).toBe(5);
  });

  it('adds SQS_REQUEST_TIMEOUT_MS to sqsConfig', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.SQS_REQUEST_TIMEOUT_MS = '5000';

    const config = loadConfig();

    expect(config.sqsConfig.requestTimeout).toBe(5000);
  });

  it('throws on invalid SQS_MAX_ATTEMPTS', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.SQS_MAX_ATTEMPTS = '0';

    expect(() => loadConfig()).toThrow('SQS_MAX_ATTEMPTS must be a positive integer');
  });

  it('throws on invalid SQS_REQUEST_TIMEOUT_MS', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.SQS_REQUEST_TIMEOUT_MS = '-1';

    expect(() => loadConfig()).toThrow('SQS_REQUEST_TIMEOUT_MS must be a positive integer');
  });

  it('defaults SQS_MAX_ATTEMPTS and SQS_REQUEST_TIMEOUT_MS when not set', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';

    const config = loadConfig();

    expect(config.sqsConfig.maxAttempts).toBeUndefined();
    expect(config.sqsConfig.requestTimeout).toBeUndefined();
  });
});
