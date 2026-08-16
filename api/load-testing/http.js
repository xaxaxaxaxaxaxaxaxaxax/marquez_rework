import { SharedArray } from 'k6/data';
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

export const options = {
  vus: Number(__ENV.K6_VUS || 25),
  duration: __ENV.K6_DURATION || '30s',
  thresholds: {
    errors: ['rate<0.01'],
    http_req_failed: ['rate<0.01'],
  },
};

export const errorRate = new Rate('errors');

const marquezUrl = (__ENV.MARQUEZ_URL || 'http://localhost:5000').replace(/\/+$/, '');
const lineageUrl = `${marquezUrl}/api/v1/lineage`;
const sleepSeconds = Number(__ENV.K6_SLEEP_SECONDS || 1);

const metadata = new SharedArray('metadata', function () {
  const events = JSON.parse(open('./metadata.json'));
  if (!Array.isArray(events) || events.length === 0) {
    throw new Error('metadata.json must contain at least one OpenLineage event');
  }
  return events;
});

export default function () {
  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };
  const eventIndex = ((__VU - 1) + (__ITER * options.vus)) % metadata.length;
  const response = http.post(lineageUrl, JSON.stringify(metadata[eventIndex]), params);

  const accepted = check(response, {
    'status is 201': (r) => r.status === 201,
  });
  errorRate.add(!accepted);

  sleep(sleepSeconds);
}
