/**
 * k6 spike load test — 5x traffic spike for 10 minutes
 *
 * Simulates a flash-sale or DDoS-style traffic spike.
 * Captures the autoscale reaction time:
 *   T0: lag exceeds HPA threshold (kafka_consumergroup_lag > 1000)
 *   T1: new pods become ready and lag starts recovering
 *   Reaction time = T1 - T0 (measured from Grafana, not k6)
 *
 * Phase structure:
 *   0-2m:    Ramp from 1k to 25k TPS  (spike onset)
 *   2-12m:   Hold at 25k TPS          (spike sustained — 10 minutes)
 *   12-14m:  Ramp down to 1k TPS      (spike recovery)
 *   14-16m:  Cool-down at 1k TPS
 *
 * Run: k6 run load-testing/k6/spike.js
 */

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const errorRate = new Rate('error_rate');

export const options = {
  scenarios: {
    spike_test: {
      executor: 'ramping-arrival-rate',
      startRate: 1000,
      timeUnit: '1s',
      preAllocatedVUs: 500,
      maxVUs: 2000,
      stages: [
        { target: 1000,  duration: '30s' },  // Warm up at 1k TPS
        { target: 25000, duration: '2m' },   // Ramp to 25k TPS (spike onset)
        { target: 25000, duration: '10m' },  // Hold spike — HPA must react here
        { target: 1000,  duration: '2m' },   // Ramp down
        { target: 1000,  duration: '2m' },   // Cool down
      ],
    },
  },
  thresholds: {
    // During spike, we allow degradation — measure what actually happens
    // rather than fail the test. Evidence collection is the goal.
    http_req_duration: ['p(99)<500'],   // Allow up to 500ms p99 during spike
    error_rate: ['rate<0.05'],          // < 5% error rate
  },
};

const COUNTRIES = ['US', 'GB', 'DE', 'FR', 'IN', 'CN', 'BR', 'CA', 'AU', 'JP'];
const MERCHANTS = Array.from({length: 100}, (_, i) => `MERCHANT_${String(i).padStart(4, '0')}`);

function randomChoice(arr) { return arr[Math.floor(Math.random() * arr.length)]; }

function generateTransaction() {
  const billingCountry = randomChoice(COUNTRIES);
  return {
    transaction_id: `spike-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
    account_id: `ACC_${String(Math.floor(Math.random() * 10000)).padStart(6, '0')}`,
    amount: Math.round(Math.random() * 500 + 10),
    merchant_id: randomChoice(MERCHANTS),
    device_fingerprint: `fp_${Math.random().toString(36).substr(2, 16)}`,
    geo_location: { lat: 37.7749, lon: -122.4194, country: billingCountry, city: 'San Francisco' },
    timestamp_ms: Date.now(),
    billing_country: billingCountry,
    shipping_country: billingCountry,
  };
}

const BASE_URL = __ENV.SCORING_URL || 'http://localhost:8000';

export default function() {
  const tx = generateTransaction();
  const response = http.post(
    `${BASE_URL}/score`,
    JSON.stringify(tx),
    { headers: { 'Content-Type': 'application/json' }, timeout: '10s' }
  );
  const ok = check(response, { 'status 200': r => r.status === 200 });
  errorRate.add(!ok);
}

export function handleSummary(data) {
  console.log('\nSPIKE TEST COMPLETE — check Grafana for:');
  console.log('  1. Kafka consumer lag chart (should spike then recover)');
  console.log('  2. Pod count chart (should increase ~2 min after lag spike)');
  console.log('  3. p99 latency chart (should stay <500ms during spike)');
  return {
    'load-testing/results/spike-summary.json': JSON.stringify(data, null, 2),
  };
}
