#!/usr/bin/env python3
"""Real measurements only. No synthetic fallback and no overlapping perf suites."""
import argparse, contextlib, csv, fcntl, hashlib, json, math, os, re, shutil, statistics, subprocess, sys, time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen
ROOT=Path(__file__).resolve().parents[2]
RESULTS=ROOT/'perf-harness/results'
COMPOSE=['docker','compose','-p','rmk-perf','-f',str(ROOT/'demo-stand/docker-compose.yml'),'-f',str(ROOT/'perf-harness/compose.perf.yml')]

@contextlib.contextmanager
def awake_host():
    assertion=None
    if sys.platform=='darwin' and shutil.which('caffeinate'):
        assertion=subprocess.Popen(['caffeinate','-i','-s','-w',str(os.getpid())])
    try:yield
    finally:
        if assertion is not None:
            assertion.terminate();assertion.wait(timeout=5)

def valid_clock_window(wall,monotonic,requested,max_gap=0):
    return wall>=requested and wall<=requested+60 and max(abs(wall-monotonic),max_gap)<1

def cleanup_load(prefix,load):
    if load.poll() is None:
        subprocess.run(['docker','rm','-f','rmk-perf-load-'+prefix],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)
        load.wait(timeout=30)

def cleanup_apps():
    subprocess.run(COMPOSE+['stop','order-service','payment-service'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=60)

def command(args,**kw):
    result=subprocess.run(args,cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,**kw)
    if result.returncode: raise RuntimeError(f'Command failed ({result.returncode}): {args}\n{result.stderr}')
    return result.stdout

def compose(*args,env=None): return command(COMPOSE+list(args),env=env)
def sql(service,query):
    user='orders' if service=='order-postgres' else 'payments'
    return compose('exec','-T',service,'psql','-X','-qAt','-v','ON_ERROR_STOP=1','-U',user,'-d',user,'-c',query).strip()

def metrics(port):
    text=urlopen(f'http://localhost:{port}/actuator/prometheus',timeout=10).read().decode()
    values={}
    for line in text.splitlines():
        if line.startswith('#'): continue
        m=re.match(r'([^ {]+)(?:\{(.*)\})?\s+([\d.eE+\-InfNa]+)',line)
        if not m: continue
        name,labels,value=m.groups()
        pairs={k:json.loads('"'+v+'"') for k,v in re.findall(r'(\w+)="((?:\\.|[^"\\])*)"',labels or '')}
        values[(name,tuple(sorted(pairs.items())))]=float(value)
    return values

def total(m,name,**labels):
    return sum(v for (n,ls),v in m.items() if n==name and all(dict(ls).get(k)==v for k,v in labels.items()))

def quantile(before,after,name,q):
    buckets={}
    for (n,labels),v in after.items():
        if n==name:
            bound=float(dict(labels)['le']); delta=v-before.get((n,labels),0)
            if delta<0: raise RuntimeError('Histogram reset during measurement')
            buckets[bound]=buckets.get(bound,0)+delta
    count=buckets.get(math.inf,0)
    if not count: return None
    target=q*count; previous_bound=previous_count=0
    for bound,cumulative in sorted(buckets.items()):
        if cumulative>=target:
            if math.isinf(bound): return None  # Never present an unbounded quantile as a finite value.
            return 1000*(previous_bound+(bound-previous_bound)*(target-previous_count)/(cumulative-previous_count)) if cumulative>previous_count else 1000*bound
        previous_bound,previous_count=bound,cumulative
    return None

def health():
    deadline=time.monotonic()+120
    for port in [18091,18092]:
        while time.monotonic()<deadline:
            try:
                if json.load(urlopen(f'http://localhost:{port}/actuator/health',timeout=2))['status']=='UP': break
            except Exception: pass
            time.sleep(1)
        else: raise RuntimeError(f'health failed on {port}')

def wait_drain(relay=True,timeout=900):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        pending=int(sql('order-postgres',"select count(*) from outbox_message where status in ('NEW','FAILED','IN_FLIGHT')"))
        dead=int(sql('order-postgres',"select count(*) from outbox_message where status='DEAD'"))
        orders=int(sql('order-postgres','select count(*) from orders'))
        paid=int(sql('payment-postgres','select count(*) from payments'))
        if dead: raise RuntimeError(f'{dead} DEAD messages')
        if not relay or pending==0 and paid==orders:
            time.sleep(3) # Allow post-commit metrics and Prometheus to catch up.
            return pending
        time.sleep(2)
    raise RuntimeError(f'drain timed out: pending={pending}, orders={orders}, payments={paid}')

def reconcile(relay):
    order_ids=sql('order-postgres','select id from orders order by id').splitlines()
    payment_ids=sql('payment-postgres','select distinct order_id from payments order by order_id').splitlines()
    effects=int(sql('payment-postgres','select count(*) from payments'))
    duplicate=effects-len(payment_ids)
    missing=len(set(order_ids)-set(payment_ids)); unexpected=len(set(payment_ids)-set(order_ids))
    if relay and (missing or unexpected or duplicate): raise RuntimeError('identity reconciliation failed')
    return dict(orders=len(order_ids),payments=effects,missing_payments=missing,unexpected_payments=unexpected,duplicate_payments=duplicate,
                orders_sha256=hashlib.sha256('\n'.join(order_ids).encode()).hexdigest(),payments_sha256=hashlib.sha256('\n'.join(payment_ids).encode()).hexdigest())

def reset():
    wait_drain()
    sql('order-postgres','truncate orders,outbox_message,inbox_message restart identity')
    sql('payment-postgres','truncate payments,delivery_log,outbox_message,inbox_message restart identity')
    sql('order-postgres','select pg_stat_statements_reset()')

def db_snapshot():
    q="""select json_build_object('calls',(select coalesce(sum(calls),0) from pg_stat_statements where dbid=(select oid from pg_database where datname=current_database())),
      'relay_calls',(select coalesce(sum(calls),0) from pg_stat_statements where query like '%with picked as%' or query like 'update outbox_message%'),
      'n_dead_tup',n_dead_tup,'autovacuum_count',autovacuum_count,'table_bytes',pg_total_relation_size('outbox_message')) from pg_stat_user_tables where relname='outbox_message'"""
    return json.loads(sql('order-postgres',q))

def k6(run_id,rps,duration,strict=False):
    return subprocess.Popen(['docker','run','--rm','--name','rmk-perf-load-'+run_id,'--network','rmk-perf_default','--cpus','2','--memory','1g',
        '--user',f'{os.getuid()}:{os.getgid()}',
        '-e',f'RPS={rps}','-e',f'DURATION={duration}','-e',f'RUN_ID={run_id}','-e',f'STRICT={str(strict).lower()}',
        '-v',f'{ROOT}/perf-harness/k6:/scripts:ro','-v',f'{RESULTS}:/results','grafana/k6:1.3.0','run','/scripts/place-orders.js'],
        stdout=open(RESULTS/f'{run_id}.log','w'),stderr=subprocess.STDOUT,cwd=ROOT)

def configure(config):
    env={k:v for k,v in os.environ.items() if not k.startswith('PERF_')}; env.update({f'PERF_{k}':str(v) for k,v in config.items()})
    compose('stop','order-service','payment-service')
    # Only project-owned transient data is reset; stage 1 volumes are independent.
    sql('order-postgres','truncate orders,outbox_message,inbox_message restart identity')
    sql('payment-postgres','truncate payments,delivery_log,outbox_message,inbox_message restart identity')
    for topic in ['orders.v1','orders.v1.DLT']:
        compose('exec','-T','kafka','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','localhost:29092','--delete','--topic',topic,'--if-exists')
        deadline=time.monotonic()+30
        while topic in compose('exec','-T','kafka','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','localhost:29092','--list').splitlines():
            if time.monotonic()>deadline: raise RuntimeError('topic deletion did not complete')
            time.sleep(1)
        compose('exec','-T','kafka','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','localhost:29092','--create','--topic',topic,'--partitions','6','--replication-factor','1')
    compose('up','-d','--force-recreate','order-service','payment-service',env=env)
    health()
    if config.get('RELAY_ENABLED','true')=='true':
        log=compose('logs','--no-color','--tail','100','order-service')
        expected_virtual=config.get('VIRTUAL_THREADS','false')
        if f'virtualThreads={expected_virtual}' not in log: raise RuntimeError('Image does not expose the requested relay thread mode; rebuild before measuring')
        if 'FAST_EMPTY_HEADERS' in config and f'fastEmptyHeaders={config["FAST_EMPTY_HEADERS"]}' not in log:
            raise RuntimeError('Image did not apply the requested empty-header optimization; rebuild before measuring')
    if total(metrics(18091),'hikaricp_connections_max')!=int(config.get('POOL_SIZE',10)): raise RuntimeError('Requested Hikari pool was not applied')

def run_case(prefix,rps,seconds,relay,soak=False,strict=False):
    before_o=metrics(18091); before_p=metrics(18092); before_db=db_snapshot()
    container_ids=compose('ps','-q','order-service','payment-service','order-postgres','payment-postgres','kafka','toxiproxy').split()
    start=time.time(); monotonic_start=time.monotonic(); max_clock_gap=0;process=k6(prefix,rps,seconds,strict)
    try:
        samples=[]; cpu=[]; previous_p=before_p
        while process.poll() is None:
            m=metrics(18091); p=metrics(18092)
            sample=dict(elapsed=time.time()-start,pending=total(m,'outbox_messages_pending'),in_flight=total(m,'outbox_messages_in_flight'),
                        dead=total(m,'outbox_messages_dead'),processed=total(p,'demo_e2e_latency_seconds_count'),
                        hikari_active=total(m,'hikaricp_connections_active'))
            sample['monotonic_elapsed']=time.monotonic()-monotonic_start
            max_clock_gap=max(max_clock_gap,abs(sample['elapsed']-sample['monotonic_elapsed']))
            sample['e2e_window_p99_ms']=quantile(previous_p,p,'demo_e2e_latency_seconds_bucket',.99);previous_p=p
            sample.update(db_snapshot())
            samples.append(sample)
            stats=command(['docker','stats','--no-stream','--format','{{json .}}']+container_ids)
            cpu.extend([dict(elapsed=sample['elapsed'],**json.loads(line)) for line in stats.splitlines() if line.strip()])
            time.sleep(8 if not soak else 30)
        end=time.time(); monotonic_duration=time.monotonic()-monotonic_start;exit_code=process.wait()
        if not (RESULTS/f'{prefix}.json').exists(): raise RuntimeError(f'k6 failed: {prefix}; exit={exit_code}')
        measured_o=metrics(18091); measured_p=metrics(18092); measured_db=db_snapshot()
        pending=wait_drain(relay=relay)
        after_p=metrics(18092)
        raw=json.loads((RESULTS/f'{prefix}.json').read_text())['metrics']
        latency=raw['order_http_latency']['values']
        counters=lambda n:raw.get(n,{}).get('values',{}).get('count',0)
        committed=counters('orders_committed'); failed=counters('orders_failed')
        stats=dict(http_p50_ms=latency.get('p(50)'),http_p95_ms=latency.get('p(95)'),http_p99_ms=latency.get('p(99)'),
                   e2e_p50_ms=quantile(before_p,after_p,'demo_e2e_latency_seconds_bucket',.50),
                   e2e_p95_ms=quantile(before_p,after_p,'demo_e2e_latency_seconds_bucket',.95),
                   e2e_p99_ms=quantile(before_p,after_p,'demo_e2e_latency_seconds_bucket',.99),
                   requested_rps=rps,duration_seconds=seconds,wall_seconds=end-start,committed=committed,http_failed=failed,
                   monotonic_seconds=monotonic_duration,clock_max_gap_seconds=max(max_clock_gap,abs(end-start-monotonic_duration)),
                   http_error_rate=failed/(failed+committed) if committed+failed else 1,dropped_iterations=counters('dropped_iterations'),
                   committed_rps=committed/seconds,
                   relay_sent_rps=(total(measured_o,'outbox_relay_published_total',result='sent')-total(before_o,'outbox_relay_published_total',result='sent'))/seconds,
                   db_calls_per_second=(measured_db['calls']-before_db['calls'])/seconds,
                   relay_calls_per_second=(measured_db['relay_calls']-before_db['relay_calls'])/seconds,
                   hikari_mean_usage_ms=1000*(total(measured_o,'hikaricp_connections_usage_seconds_sum')-total(before_o,'hikaricp_connections_usage_seconds_sum'))/max(1,total(measured_o,'hikaricp_connections_usage_seconds_count')-total(before_o,'hikaricp_connections_usage_seconds_count')),
                   max_pending=max((s['pending'] for s in samples),default=0),pending_after_drain=pending,k6_exit=exit_code,
                   hikari_leases_per_second=(total(measured_o,'hikaricp_connections_usage_seconds_count')-total(before_o,'hikaricp_connections_usage_seconds_count'))/seconds,
                   hikari_little_law_occupancy=(total(measured_o,'hikaricp_connections_usage_seconds_sum')-total(before_o,'hikaricp_connections_usage_seconds_sum'))/seconds,
                   hikari_active_mean=statistics.mean(s['hikari_active'] for s in samples) if samples else None)
        stats.update(reconcile(relay))
        stats['cpu_percent_mean']={name:statistics.mean(float(s['CPUPerc'].rstrip('%')) for s in cpu if s.get('Name')==name) for name in {s.get('Name') for s in cpu}}
        doc=dict(run_id=prefix,started_at=datetime.fromtimestamp(start,timezone.utc).isoformat(),finished_at=datetime.fromtimestamp(end,timezone.utc).isoformat(),
                 statistics=stats,samples=samples,cpu_samples=cpu)
        if not valid_clock_window(end-start,monotonic_duration,seconds,max_clock_gap):
            doc['excluded_reason']='System sleep, clock discontinuity, or load window overran its fixed duration'
        (RESULTS/f'{prefix}.measurement.json').write_text(json.dumps(doc,indent=2)+'\n')
        print(f'{prefix}: HTTP p99={stats["http_p99_ms"]:.2f}ms e2e p99={stats["e2e_p99_ms"]}ms rps={stats["committed_rps"]:.1f} pending_peak={stats["max_pending"]}',flush=True)
        return doc
    finally:
        if process.poll() is None:
            subprocess.run(['docker','rm','-f','rmk-perf-load-'+prefix],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            process.wait(timeout=30)

def init():
    compose('up','-d','--wait','order-postgres','payment-postgres','kafka','toxiproxy','prometheus')
    compose('up','-d','order-service','payment-service'); health()
    sql('order-postgres','create extension if not exists pg_stat_statements')

def suite_configs(suite):
    if suite=='baseline': return [(f'baseline-{r}',{'RELAY_ENABLED':'true'},r) for r in [100,300,500,1000]]
    if suite=='overhead': return [(f'overhead-{enabled}',{'RELAY_ENABLED':str(enabled).lower()},500) for enabled in [False,True]]
    if suite=='saturation': return [(f'saturation-{r}',{},r) for r in [1500,3000,4500]]
    if suite=='matrix': return [(f'batch-{b}-poll-{p}',{'BATCH_SIZE':b,'POLL_INTERVAL':f'{p}ms'},300) for b in [16,64,128,512] for p in [50,200,1000]]
    if suite=='virtual': return [(f'virtual-{v}-pool-{pool}',{'VIRTUAL_THREADS':str(v).lower(),'POOL_SIZE':pool},500) for v in [False,True] for pool in [10,20]]
    if suite=='optimization': return [(f'fast-headers-{v}',{'FAST_EMPTY_HEADERS':str(v).lower()},500) for v in [False,True]]
    if suite=='soak': return [('soak',{'RETENTION':'30s','CLEANUP_INTERVAL':'5s'},100)]
    if suite=='ci': return [('ci',{},100)]
    if suite=='validation': return [('smoke',{},100)]
    raise ValueError(suite)

def main():
    p=argparse.ArgumentParser(); p.add_argument('--suite',choices=['baseline','overhead','saturation','matrix','virtual','optimization','soak','ci','validation'],required=True)
    p.add_argument('--duration',type=int); p.add_argument('--warmup',type=int,default=60); p.add_argument('--runs',type=int,default=3)
    p.add_argument('--name',default=''); p.add_argument('--chaos-delay',default='0ms'); p.add_argument('--skip-build',action='store_true')
    p.add_argument('--only-rps',help='Resume a baseline configuration, e.g. 300,500,1000; complete repetitions remain mandatory')
    args=p.parse_args(); seconds=args.duration or (7200 if args.suite=='soak' else 60 if args.suite=='ci' else 180)
    selected=suite_configs(args.suite)
    if args.only_rps:
        if args.suite!='baseline':p.error('--only-rps is only for baseline')
        rates={int(rate) for rate in args.only_rps.split(',')}
        if not rates or not rates<={100,300,500,1000}:p.error('unsupported baseline rate')
        selected=[c for c in selected if c[2] in rates]
    if not re.fullmatch(r'[A-Za-z0-9_-]*',args.name): p.error('name must be an ASCII slug')
    if min(seconds,args.warmup,args.runs)<1: p.error('durations and count must be positive')
    if args.suite not in ['ci','validation'] and (args.runs<3 or args.warmup<60 or seconds<180): p.error('real performance suites need 3 runs, 60s warmup and at least 180s measurement')
    RESULTS.mkdir(parents=True,exist_ok=True)
    with awake_host(), open(RESULTS/'.perf.lock','w') as lock, contextlib.ExitStack() as resources:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        resources.callback(cleanup_apps)
        if not args.skip_build:
            command(['./gradlew',':demo-stand:order-service:bootJar',':demo-stand:payment-service:bootJar'])
            compose('build','order-service','payment-service')
        init(); documents=[]
        identity=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        manifest=dict(created_at=datetime.now(timezone.utc).isoformat(),commit=command(['git','rev-parse','HEAD']).strip(),
            args=vars(args),duration=seconds,docker_info=json.loads(command(['docker','info','--format','{{json .}}'])),
            compose_config=json.loads(compose('config','--format','json')),
            image_ids=compose('images','--format','json'))
        # Docker info can contain machine paths but no credentials; keep only needed hardware metadata.
        manifest['docker_info']={k:manifest['docker_info'].get(k) for k in ['ServerVersion','NCPU','MemTotal','Architecture','OperatingSystem','KernelVersion']}
        manifest['status']='running'; manifest['measurements']=[]
        manifest_path=RESULTS/f'{identity}-{args.suite}.manifest.json'
        manifest_path.write_text(json.dumps(manifest,indent=2)+'\n')
        for label,config,rps in selected:
            config['CHAOS_DELAY']=args.chaos_delay; configure(config)
            relay=config.get('RELAY_ENABLED','true')=='true'
            prefix=f'{identity}-{args.name+"-" if args.name else ""}{label}'
            warm_wall=time.time();warm_monotonic=time.monotonic()
            warm=k6(prefix+'-warmup',rps,args.warmup);resources.callback(cleanup_load,prefix+'-warmup',warm)
            if warm.wait()!=0 or not valid_clock_window(time.time()-warm_wall,time.monotonic()-warm_monotonic,args.warmup):raise RuntimeError('Invalid JVM warmup window')
            wait_drain(relay=relay)
            for run in range(1,args.runs+1):
                if not relay:
                    sql('order-postgres','truncate orders,outbox_message,inbox_message restart identity'); sql('payment-postgres','truncate payments,delivery_log,inbox_message')
                    sql('order-postgres','select pg_stat_statements_reset()')
                else: reset()
                doc=run_case(f'{prefix}-r{run}',rps,seconds,relay,args.suite=='soak',args.suite=='ci')
                doc['config']=config.copy(); doc['suite']=args.suite;doc['campaign']=args.name; doc['validation_only']=args.suite=='validation'; documents.append(doc)
                manifest['measurements']=[d['run_id'] for d in documents];manifest_path.write_text(json.dumps(manifest,indent=2)+'\n')
                (RESULTS/f'{doc["run_id"]}.measurement.json').write_text(json.dumps(doc,indent=2)+'\n')
                if doc.get('excluded_reason'):
                    manifest['status']='invalid-clock-window';manifest['invalid_measurement']=doc['run_id']
                    manifest_path.write_text(json.dumps(manifest,indent=2)+'\n')
                    raise RuntimeError('Invalid measurement window: '+doc['run_id'])
        manifest['measurements']=[d['run_id'] for d in documents];manifest['status']='complete'
        (RESULTS/f'{identity}-{args.suite}.manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
        command(['python3',str(ROOT/'perf-harness/scripts/summarize.py')])

if __name__=='__main__': main()
