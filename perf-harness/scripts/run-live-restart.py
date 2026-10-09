#!/usr/bin/env python3
"""Kill the real JVM after Kafka delivery but before the SQL acknowledgement commits."""
import fcntl, json, os, time
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from benchmark import ROOT, RESULTS, command, compose, configure, health, init, reconcile, sql, wait_drain


def until(check, description, timeout=60):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        value=check()
        if value: return value
        time.sleep(.2)
    raise RuntimeError('Timed out: '+description)


def counts():
    return json.loads(sql('payment-postgres', "select json_build_object('deliveries',count(*),'unique_messages',count(distinct message_id)) from delivery_log"))


def run(virtual):
    configure({'VIRTUAL_THREADS':str(virtual).lower()})
    sql('order-postgres', """
        create or replace function perf_pause_ack() returns trigger language plpgsql as $$
        begin
            if new.status='SENT' and old.id=1 then perform pg_sleep(30); end if;
            return new;
        end $$;
        create trigger perf_pause_ack before update on outbox_message
        for each row execute function perf_pause_ack();
        """)
    expected=[]
    try:
        for n in range(200):
            request=Request('http://localhost:18081/orders',data=json.dumps({'customerId':'restart-'+str(n),'total':12.34}).encode(),headers={'Content-Type':'application/json'})
            with urlopen(request,timeout=10) as response:
                expected.append(json.load(response)['orderId'])
        until(lambda:int(sql('order-postgres',"select count(*) from pg_stat_activity where wait_event='PgSleep' and query like 'update outbox_message%'")), 'acknowledgement blocked inside PostgreSQL')
        observed=until(lambda:counts() if counts()['unique_messages'] else None, 'Kafka record reached the consumer')
        in_flight=int(sql('order-postgres',"select count(*) from outbox_message where status='IN_FLIGHT'"))
        if not in_flight: raise RuntimeError('No in-flight lease at restart')
        container=compose('ps','-q','order-service').strip()
        compose('kill','--signal','SIGKILL','order-service')
        state=json.loads(command(['docker','inspect','--format','{{json .State}}',container]))
        if state['Running'] or state['ExitCode']!=137: raise RuntimeError('The JVM was not killed as requested')
    finally:
        # The trigger is a test fixture in this project's database, never a starter migration.
        sql('order-postgres','drop trigger if exists perf_pause_ack on outbox_message; drop function if exists perf_pause_ack()')
    env={k:v for k,v in os.environ.items() if not k.startswith('PERF_')}
    env['PERF_VIRTUAL_THREADS']=str(virtual).lower()
    compose('up','-d','order-service',env=env); health()
    wait_drain(timeout=180)
    result=reconcile(True); delivered=counts()
    duplicates=delivered['deliveries']-delivered['unique_messages']
    actual=sql('order-postgres','select id from orders order by id').splitlines()
    if sorted(expected)!=actual: raise RuntimeError('HTTP successes did not match committed orders')
    if duplicates<1: raise RuntimeError('The sent-before-ack restart did not produce a real duplicate delivery')
    result.update(virtual_threads=virtual,killed_exit_code=state['ExitCode'],in_flight_at_kill=in_flight,
                  delivered_before_kill=observed,delivery_counts=delivered,duplicate_deliveries=duplicates,
                  committed_http_ids=sorted(expected))
    return result


def main():
    RESULTS.mkdir(parents=True,exist_ok=True)
    with open(RESULTS/'.perf.lock','w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        init(); results=[]
        try:
            for virtual in [False,True]:
                result=run(virtual);results.append(result)
                print(f"virtual={virtual}: {result['orders']} orders, {result['payments']} effects, {result['duplicate_deliveries']} harmless duplicate deliveries",flush=True)
        finally: compose('stop','order-service','payment-service')
        identity=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        report={'commit':command(['git','rev-parse','HEAD']).strip(),'completed_at':datetime.now(timezone.utc).isoformat(),'cases':results}
        (RESULTS/f'{identity}-live-restart.json').write_text(json.dumps(report,indent=2)+'\n')


if __name__=='__main__': main()
