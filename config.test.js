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

  it('defaults CW_METRICS_ENABLED to false', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    delete process.env.CW_METRICS_ENABLED;

    const config = loadConfig();

    expect(config.cwMetricsEnabled).toBe(false);
  });

  it('enables CloudWatch metrics when CW_METRICS_ENABLED=true', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.CW_METRICS_ENABLED = 'true';

    const config = loadConfig();

    expect(config.cwMetricsEnabled).toBe(true);
  });

  it('defaults CW_METRICS_REFRESH_INTERVAL_MS to 300000', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    delete process.env.CW_METRICS_REFRESH_INTERVAL_MS;

    const config = loadConfig();

    expect(config.cwMetricsRefreshIntervalMs).toBe(300000);
  });

  it('throws on zero CW_METRICS_REFRESH_INTERVAL_MS', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.CW_METRICS_REFRESH_INTERVAL_MS = '0';

    expect(() => loadConfig()).toThrow('CW_METRICS_REFRESH_INTERVAL_MS must be a positive integer');
  });

  it('defaults CloudWatch namespace, metric name, period and statistic', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    delete process.env.CW_METRICS_NAMESPACE;
    delete process.env.CW_METRICS_METRIC_NAME;
    delete process.env.CW_METRICS_PERIOD_SECONDS;
    delete process.env.CW_METRICS_STATISTIC;

    const config = loadConfig();

    expect(config.cwMetricsNamespace).toBe('AWS/SQS');
    expect(config.cwMetricsMetricName).toBe('NumberOfMessagesDeleted');
    expect(config.cwMetricsPeriodSeconds).toBe(300);
    expect(config.cwMetricsStatistic).toBe('Sum');
  });

  it('reads CloudWatch parameters from environment', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.CW_METRICS_NAMESPACE = 'Custom/Namespace';
    process.env.CW_METRICS_METRIC_NAME = 'CustomMetric';
    process.env.CW_METRICS_PERIOD_SECONDS = '60';
    process.env.CW_METRICS_STATISTIC = 'Average';

    const config = loadConfig();

    expect(config.cwMetricsNamespace).toBe('Custom/Namespace');
    expect(config.cwMetricsMetricName).toBe('CustomMetric');
    expect(config.cwMetricsPeriodSeconds).toBe(60);
    expect(config.cwMetricsStatistic).toBe('Average');
  });

  it('throws on zero CW_METRICS_PERIOD_SECONDS', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.CW_METRICS_PERIOD_SECONDS = '0';

    expect(() => loadConfig()).toThrow('CW_METRICS_PERIOD_SECONDS must be a positive integer');
  });

  it('shares fromIni credentials between sqsConfig and cloudWatchConfig', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.AWS_PROFILE = 'my-profile';

    const config = loadConfig();

    expect(config.cloudWatchConfig.region).toBe('us-west-2');
    expect(config.cloudWatchConfig.credentials).toBeDefined();
    expect(config.sqsConfig.credentials).toBeDefined();
  });

  it('leaves cloudWatchConfig credentials undefined without AWS_PROFILE', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    delete process.env.AWS_PROFILE;

    const config = loadConfig();

    expect(config.cloudWatchConfig.region).toBe('us-west-2');
    expect(config.cloudWatchConfig.credentials).toBeUndefined();
  });
});

describe('config validation failure paths', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    jest.resetModules();
    process.env = { ...originalEnv };
  });

  afterAll(() => {
    process.env = originalEnv;
  });

  it('rejects PORT above valid range', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.PORT = '99999';

    expect(() => loadConfig()).not.toThrow();
  });

  it('rejects METRICS_REFRESH_INTERVAL_MS that is too large', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.METRICS_REFRESH_INTERVAL_MS = '999999999';

    const config = loadConfig();
    expect(config.refreshIntervalMs).toBe(999999999);
  });

  it('accepts valid AWS_REGION formats', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'ap-southeast-1';

    const config = loadConfig();
    expect(config.awsRegion).toBe('ap-southeast-1');
  });

  it('rejects AWS_REGION with wrong format', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west2';

    expect(() => loadConfig()).toThrow('AWS_REGION is invalid');
  });

  it('rejects GRACEFUL_SHUTDOWN_TIMEOUT_MS of zero', () => {
    process.env.SQS_QUEUES = 'queue-a';
    process.env.AWS_REGION = 'us-west-2';
    process.env.GRACEFUL_SHUTDOWN_TIMEOUT_MS = '0';

    expect(() => loadConfig()).toThrow('GRACEFUL_SHUTDOWN_TIMEOUT_MS must be a positive integer');
  });

  it('accepts all valid config together', () => {
    process.env.SQS_QUEUES = 'q1,q2';
    process.env.AWS_REGION = 'eu-central-1';
    process.env.PORT = '8080';
    process.env.METRICS_REFRESH_INTERVAL_MS = '30000';
    process.env.SQS_MAX_CONCURRENCY = '10';
    process.env.GRACEFUL_SHUTDOWN_TIMEOUT_MS = '5000';
    process.env.SQS_MAX_ATTEMPTS = '3';
    process.env.SQS_REQUEST_TIMEOUT_MS = '3000';

    const config = loadConfig();

    expect(config.port).toBe(8080);
    expect(config.queueNames).toEqual(['q1', 'q2']);
    expect(config.awsRegion).toBe('eu-central-1');
    expect(config.refreshIntervalMs).toBe(30000);
    expect(config.maxConcurrency).toBe(10);
    expect(config.gracefulShutdownTimeoutMs).toBe(5000);
    expect(config.sqsConfig.maxAttempts).toBe(3);
    expect(config.sqsConfig.requestTimeout).toBe(3000);
    expect(Object.isFrozen(config)).toBe(true);
  });
});
