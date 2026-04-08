const { fromIni } = require('@aws-sdk/credential-provider-ini');

function validateRegion(region) {
  return /^[a-z]{2}-[a-z]+-\d+$/.test(region);
}

function validatePositiveInt(value, name) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

function parseQueueNames(raw) {
  if (!raw) {
    return [];
  }

  return raw
    .split(',')
    .map((name) => name.trim())
    .filter(Boolean);
}

function loadConfig() {
  const queueNames = parseQueueNames(process.env.SQS_QUEUES);

  if (queueNames.length === 0) {
    throw new Error('SQS_QUEUES must include at least one queue name.');
  }

  const awsRegion = process.env.AWS_REGION || 'us-west-2';

  if (!validateRegion(awsRegion)) {
    throw new Error(`AWS_REGION is invalid: "${awsRegion}"`);
  }

  const port = validatePositiveInt(process.env.PORT || 3001, 'PORT');
  const refreshIntervalMs = validatePositiveInt(
    process.env.METRICS_REFRESH_INTERVAL_MS || 15000,
    'METRICS_REFRESH_INTERVAL_MS'
  );

  const awsProfile = process.env.AWS_PROFILE;
  const sqsConfig = { region: awsRegion };
  if (awsProfile) {
    sqsConfig.credentials = fromIni({ profile: awsProfile });
  }

  return Object.freeze({
    port,
    queueNames,
    awsRegion,
    awsProfile,
    refreshIntervalMs,
    sqsConfig
  });
}

module.exports = { loadConfig, validateRegion, validatePositiveInt, parseQueueNames };
