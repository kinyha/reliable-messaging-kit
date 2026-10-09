#!/usr/bin/env python3
"""CPU, allocation, wall and JFR captures on the same constrained service."""
import argparse, collections, fcntl, gzip, json, os, shutil, time
from datetime import datetime,timezone
from pathlib import Path
from benchmark import ROOT,RESULTS,command,compose,configure,health,init,k6,reset,wait_drain

def analyze_collapsed(path):
    worker=collections.Counter(); totals=collections.Counter()
    for line in path.read_text().splitlines():
        stack,weight=line.rsplit(' ',1); weight=int(weight)
        is_worker='outbox-relay-' in stack or 'OutboxRelay.work' in stack
        if 'Thread.sleep' in stack or 'Thread/sleep' in stack or 'JVM_Sleep' in stack: category='idle_sleep'
        elif 'com/fasterxml/jackson' in stack or 'com.fasterxml.jackson' in stack: category='jackson'
        elif 'org/postgresql' in stack or 'org.postgresql' in stack or 'org/springframework/jdbc' in stack: category='jdbc'
        elif 'org/apache/kafka' in stack or 'org.apache.kafka' in stack or 'KafkaOutboxDispatcher' in stack: category='kafka'
        else: category='other'
        totals[category]+=weight
        if is_worker: worker[category]+=weight
    active=sum(v for k,v in worker.items() if k!='idle_sleep')
    return dict(all=dict(totals),relay=dict(worker),relay_active=active,
                relay_active_share={k:100*v/active if active else None for k,v in worker.items() if k!='idle_sleep'})

def main():
    p=argparse.ArgumentParser();p.add_argument('--rps',type=int,default=500);p.add_argument('--duration',type=int,default=120)
    p.add_argument('--runs',type=int,default=3);p.add_argument('--virtual',action='store_true');p.add_argument('--name',default='profile'); args=p.parse_args()
    if args.duration<120 or args.runs<3:p.error('profiling requires three captures of at least 120 seconds')
    with open(RESULTS/'.perf.lock','w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        init();configure({'VIRTUAL_THREADS':str(args.virtual).lower()}); identity=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        warm=k6(f'{identity}-{args.name}-warmup',args.rps,60);assert warm.wait()==0;wait_drain()
        reports=[]
        for event in ['cpu','alloc','wall']:
            for run in range(1,args.runs+1):
                reset();prefix=f'{identity}-{args.name}-{event}-r{run}'; load=k6(prefix+'-load',args.rps,args.duration+15)
                time.sleep(5)
                options=['-e',event,'-t','--dot','-d',str(args.duration),'-f',f'/results/{prefix}.html']
                if event=='alloc':options+=['--total']
                if event=='cpu':
                    compose('exec','-T','order-service','jcmd','1','JFR.start',f'name={args.name}{run}','settings=profile',
                        f'duration={args.duration}s',f'filename=/results/{prefix}.jfr','jdk.VirtualThreadPinned#enabled=true',
                        'jdk.VirtualThreadPinned#threshold=1ms','jdk.JavaMonitorEnter#threshold=1ms','jdk.ThreadPark#threshold=1ms')
                profile_log=compose('exec','-T','order-service','/opt/async-profiler/bin/asprof',*options,'1')
                dump_options=['dump','-t','--dot','-o','collapsed']+(['--total'] if event=='alloc' else [])
                compose('exec','-T','order-service','/opt/async-profiler/bin/asprof',*dump_options,
                        '-f',f'/results/{prefix}.collapsed','1')
                if load.wait()!=0: raise RuntimeError('load process failed during profile')
                wait_drain();stats=analyze_collapsed(RESULTS/f'{prefix}.collapsed')
                if not sum(stats['all'].values()):raise RuntimeError('empty profile; no synthetic substitute')
                if event=='cpu':
                    events=compose('exec','-T','order-service','jfr','print','--json','--events',
                        'jdk.ExecutionSample,jdk.JavaMonitorEnter,jdk.ThreadPark,jdk.VirtualThreadPinned,jdk.SocketRead,jdk.SocketWrite',f'/results/{prefix}.jfr')
                    data=json.loads(events);jfr=collections.Counter(); waits=collections.Counter();frames=collections.Counter()
                    for e in data['recording']['events']:
                        jfr[e['type']]+=1;v=e['values'];thread=v.get('eventThread') or v.get('sampledThread') or {}; name=thread.get('javaName','unknown')
                        if e['type'] in ['jdk.JavaMonitorEnter','jdk.ThreadPark','jdk.VirtualThreadPinned']:
                            waits[(e['type'],name)]+=1
                            for frame in (v.get('stackTrace') or {}).get('frames',[])[:5]:
                                method=frame['method'];frames[method['type']['name']+'.'+method['name']]+=1
                    stats['jfr_events']=dict(jfr);stats['jfr_wait_threads']=[{'event':e,'thread':t,'count':c} for (e,t),c in waits.most_common()]
                    stats['jfr_wait_top_frames']=dict(frames.most_common(30))
                    with gzip.open(RESULTS/f'{prefix}.jfr.gz','wb') as out:out.write((RESULTS/f'{prefix}.jfr').read_bytes())
                    (RESULTS/f'{prefix}.jfr').unlink()
                report=dict(profile=prefix,event=event,virtual=args.virtual,commit=command(['git','rev-parse','HEAD']).strip(),statistics=stats,profiler_output=profile_log)
                (RESULTS/f'{prefix}.profile.json').write_text(json.dumps(report,indent=2)+'\n');reports.append(report)
                print(f'{prefix}: {stats["relay_active_share"]}',flush=True)
        (RESULTS/f'{identity}-{args.name}-pinning.txt').write_text(compose('logs','--no-color','order-service'))
        compose('stop','order-service','payment-service')
        (RESULTS/f'{identity}-{args.name}.profiles.json').write_text(json.dumps(reports,indent=2)+'\n')

if __name__=='__main__':main()
