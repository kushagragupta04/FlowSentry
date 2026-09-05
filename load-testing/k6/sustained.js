/**
 * k6 sustained load test — 5,000 TPS for 15 minutes
 *
 * Measures: p50, p95, p99 latency under sustained load.
 * Exit criteria: p99 < 100ms throughout the 15-minute window.
 *
 * Run: k6 run --out influxdb=http://localhost:8086/k6 load-testing/k6/sustained.js
 * Or:  k6 run load-testing/k6/sustained.js
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Histogram, Rate } from 'k6/metrics';

// ── Custom metrics ────────────────────────────────────────────
const decisionAllow = new Counter('decisions_allow');
const decisionFlag  = new Counter('decisions_flag');
const decisionBlock = new Counter('decisions_block');
const errorRate     = new Rate('error_rate');

// ── Test configuration ────────────────────────────────────────
export const options = {
  scenarios: {
    sustained_load: {
      executor: 'constant-arrival-rate',
      rate: 5000,              // 5,000 requests per second
      timeUnit: '1s',
      duration: '15m',
      preAllocatedVUs: 200,    // Pre-warm virtual users
      maxVUs: 500,
    },
  },
  thresholds: {
    http_req_duration: [
      'p(50)<30',              // p50 must be < 30ms
      'p(95)<75',              // p95 must be < 75ms
      'p(99)<100',             // p99 SLO: must be < 100ms
    ],
    error_rate: ['rate<0.001'], // < 0.1% error rate
  },
};

// ── Synthetic transaction generator ──────────────────────────
const COUNTRIES = ['US', 'GB', 'DE', 'FR', 'IN', 'CN', 'BR', 'CA', 'AU', 'JP'];
const MERCHANTS = Array.from({length: 100}, (_, i) => `MERCHANT_${String(i).padStart(4, '0')}`);

function randomChoice(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function generateTransaction() {
  const isFraud = Math.random() < 0.02;
  const billingCountry = randomChoice(COUNTRIES);
  const shippingCountry = isFraud
    ? randomChoice(COUNTRIES.filter(c => c !== billingCountry))
    : (Math.random() < 0.9 ? billingCountry : randomChoice(COUNTRIES));

  return {
    transaction_id: `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
    account_id: `ACC_${String(Math.floor(Math.random() * 10000)).padStart(6, '0')}`,
    amount: isFraud
      ? Math.round(Math.random() * 4500 + 500)
      : Math.round(Math.random() * 445 + 5),
    merchant_id: randomChoice(MERCHANTS),
    device_fingerprint: `fp_${Math.random().toString(36).substr(2, 16)}`,
    geo_location: {
      lat: Math.random() * 150 - 75,
      lon: Math.random() * 360 - 180,
      country: randomChoice(COUNTRIES),
      city: 'Unknown',
    },
    timestamp_ms: Date.now(),
    billing_country: billingCountry,
    shipping_country: shippingCountry,
  };
}

// ── Main test function ────────────────────────────────────────
const BASE_URL = __ENV.SCORING_URL || 'http://localhost:8000';

export default function() {
  const tx = generateTransaction();

  const response = http.post(
    `${BASE_URL}/score`,
    JSON.stringify(tx),
    {
      headers: { 'Content-Type': 'application/json' },
      timeout: '5s',
    }
  );

  const ok = check(response, {
    'status is 200': r => r.status === 200,
    'has decision field': r => {
      try {
        const body = JSON.parse(r.body);
        return ['allow', 'flag', 'block'].includes(body.decision);
      } catch { return false; }
    },
    'latency < 100ms': r => r.timings.duration < 100,
  });

  errorRate.add(!ok);

  if (response.status === 200) {
    try {
      const body = JSON.parse(response.body);
      if (body.decision === 'allow') decisionAllow.add(1);
      else if (body.decision === 'flag') decisionFlag.add(1);
      else if (body.decision === 'block') decisionBlock.add(1);
    } catch {}
  }
}

export function handleSummary(data) {
  const p50  = data.metrics.http_req_duration?.values?.['p(50)']?.toFixed(2);
  const p95  = data.metrics.http_req_duration?.values?.['p(95)']?.toFixed(2);
  const p99  = data.metrics.http_req_duration?.values?.['p(99)']?.toFixed(2);
  const rps  = data.metrics.http_reqs?.values?.rate?.toFixed(0);
  const errs = data.metrics.error_rate?.values?.rate;

  console.log(`\n${'='.repeat(50)}`);
  console.log('SUSTAINED LOAD TEST SUMMARY');
  console.log(`${'='.repeat(50)}`);
  console.log(`Throughput:  ${rps} req/s`);
  console.log(`Latency p50: ${p50}ms`);
  console.log(`Latency p95: ${p95}ms`);
  console.log(`Latency p99: ${p99}ms  (SLO: < 100ms)`);
  console.log(`Error rate:  ${(errs * 100).toFixed(3)}%`);
  console.log(`${'='.repeat(50)}\n`);

  return {
    'load-testing/results/sustained-summary.json': JSON.stringify(data, null, 2),
  };
}
