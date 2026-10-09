import http from 'k6/http';
import {check} from 'k6';
import {Trend, Counter} from 'k6/metrics';
const rates=__ENV.RPS ? [Number(__ENV.RPS)] : [100,300,500,1000];
const seconds=Number(__ENV.DURATION || 180);
const latency=new Trend('order_http_latency',true);
const committed=new Counter('orders_committed');
const failed=new Counter('orders_failed');
export const options={
  scenarios:Object.fromEntries(rates.map((rate,i)=>[`rate_${rate}`,{
    executor:'constant-arrival-rate',rate,timeUnit:'1s',duration:`${seconds}s`,
    startTime:`${i*seconds}s`,preAllocatedVUs:200,maxVUs:1000,gracefulStop:'15s',
  }])),
  summaryTrendStats:['min','max','avg','med','p(50)','p(95)','p(99)','count'],
  thresholds:__ENV.STRICT==='true' ? {checks:['rate>0.99'],dropped_iterations:['count==0']} : {},
};
export default function() {
  const r=http.post(__ENV.ORDER_URL || 'http://order-service:8081/orders',
    JSON.stringify({customerId:'perf',total:10}),{headers:{'Content-Type':'application/json'},timeout:'10s'});
  latency.add(r.timings.duration);
  const ok=check(r,{'HTTP 201':x=>x.status===201});
  (ok?committed:failed).add(1);
}
export function handleSummary(data) {
  return {[`/results/${__ENV.RUN_ID || 'k6-summary'}.json`]:JSON.stringify(data,null,2)};
}
