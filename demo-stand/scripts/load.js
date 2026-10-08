import http from 'k6/http';
import {check} from 'k6';
export const options = {
  scenarios: {orders: {executor: 'constant-arrival-rate', rate: Number(__ENV.RPS), timeUnit: '1s',
    duration: `${__ENV.DURATION}s`, preAllocatedVUs: 200, maxVUs: 1000}},
  thresholds: {checks: ['rate>0.99'], dropped_iterations: ['count==0']},
};
export default function () {
  const response=http.post('http://host.docker.internal:8081/orders',
    JSON.stringify({customerId: 'c-demo',total: 10.00}),
    {headers: {'Content-Type': 'application/json'}, timeout: '5s'});
  check(response, {'committed order': r => r.status===201});
}
