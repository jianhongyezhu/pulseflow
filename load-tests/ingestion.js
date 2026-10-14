import { randomBytes } from 'k6/crypto';
import { Counter } from 'k6/metrics';
import { check } from 'k6';
import http from 'k6/http';

function uuidV4() {
  const bytes = new Uint8Array(randomBytes(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function positiveInteger(name, fallback, maximum) {
  const value = Number(__ENV[name] || fallback);
  if (!Number.isInteger(value) || value < 1 || value > maximum) {
    throw new Error(`${name} must be an integer between 1 and ${maximum}`);
  }
  return value;
}

function duration(name, fallback) {
  const value = __ENV[name] || fallback;
  if (!/^[1-9]\d*(s|m)$/.test(value)) {
    throw new Error(`${name} must be a positive duration such as 30s or 2m`);
  }
  return value;
}

const profile = __ENV.K6_PROFILE || 'arrival';
const baseUrl = (__ENV.K6_BASE_URL || 'http://ingestion-api:8080').replace(/\/$/, '');
const runId = __ENV.RUN_ID || `k6-${Date.now()}-${uuidV4()}`;
const acceptedEvents = new Counter('accepted_events');

const rate = positiveInteger(
  'K6_RATE',
  profile === 'stretch' ? 250 : 50,
  1000,
);
const profileDuration = duration(
  'K6_DURATION',
  profile === 'stretch' ? '30s' : '60s',
);
const maxVUs = positiveInteger(
  'K6_MAX_VUS',
  profile === 'stretch' ? 500 : 150,
  500,
);
const preAllocatedVUs = positiveInteger(
  'K6_PRE_ALLOCATED_VUS',
  Math.min(rate, 50),
  maxVUs,
);

const thresholds = {
  http_req_failed: ['rate<0.01'],
  checks: ['rate==1'],
  dropped_iterations: ['count==0'],
};
if (profile !== 'stretch') {
  thresholds.http_req_duration = ['p(95)<150'];
}

let scenarios;
if (profile === 'handbook') {
  scenarios = {
    handbook_ramping_vus: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 20 },
        { duration: '60s', target: 50 },
        { duration: '60s', target: 100 },
        { duration: '30s', target: 0 },
      ],
      gracefulRampDown: '5s',
    },
  };
} else if (profile === 'arrival' || profile === 'stretch') {
  scenarios = {
    fixed_arrival_rate: {
      executor: 'constant-arrival-rate',
      rate,
      timeUnit: '1s',
      duration: profileDuration,
      preAllocatedVUs,
      maxVUs,
    },
  };
} else {
  throw new Error('K6_PROFILE must be handbook, arrival, or stretch');
}

export const options = {
  scenarios,
  thresholds,
  tags: { run_id: runId },
};

export default function () {
  const now = new Date().toISOString();
  const event = {
    event_id: uuidV4(),
    user_id: `k6-user-${__VU}`,
    session_id: `${runId}-vu-${__VU}`,
    event_type: 'PLAY',
    content_id: 'k6-benchmark-content',
    device_type: 'desktop',
    country_code: 'GB',
    event_ts: now,
    attributes: {
      source: 'k6-load-test',
      run_id: runId,
    },
  };

  const response = http.post(`${baseUrl}/v1/events`, JSON.stringify(event), {
    headers: { 'Content-Type': 'application/json' },
    tags: { run_id: runId },
  });
  const accepted = check(response, {
    'event accepted (HTTP 202)': (result) => result.status === 202,
  });
  if (accepted) {
    acceptedEvents.add(1, { run_id: runId });
  }
}

export function handleSummary(data) {
  const metrics = data.metrics;
  const result = {
    profile,
    run_id: runId,
    base_url: baseUrl,
    requested_rate_per_second: profile === 'handbook' ? null : rate,
    duration: profile === 'handbook' ? '180s' : profileDuration,
    requests: metrics.http_reqs?.values?.count ?? 0,
    achieved_requests_per_second: metrics.http_reqs?.values?.rate ?? null,
    accepted_events: metrics.accepted_events?.values?.count ?? 0,
    request_failure_rate: metrics.http_req_failed?.values?.rate ?? null,
    p95_request_duration_ms: metrics.http_req_duration?.values?.['p(95)'] ?? null,
    checks: {
      passes: metrics.checks?.values?.passes ?? 0,
      fails: metrics.checks?.values?.fails ?? 0,
      pass_rate: metrics.checks?.values?.rate ?? null,
    },
    dropped_iterations: metrics.dropped_iterations?.values?.count ?? 0,
  };
  const json = `${JSON.stringify(result, null, 2)}\n`;
  const outputPath = __ENV.K6_SUMMARY_PATH;
  const outputs = { stdout: json };
  if (outputPath) {
    outputs[outputPath] = json;
  }
  return outputs;
}
