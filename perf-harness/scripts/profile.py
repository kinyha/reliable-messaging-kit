#!/usr/bin/env python3
"""CPU, allocation, wall and JFR captures on the same constrained service."""
import argparse, collections, contextlib, fcntl, gzip, hashlib, json, os, re, shutil, subprocess, time
from datetime import datetime,timezone
from pathlib import Path
from benchmark import ROOT,RESULTS,awake_host,cleanup_apps,cleanup_load,command,compose,configure,health,init,k6,reset,valid_clock_window,wait_drain

def close_manifest(manifest,path):
    if manifest['status']=='running':
        manifest['status']='failed'
        manifest['failure']='Capture campaign exited before all required profiles were completed'
        path.write_text(json.dumps(manifest,indent=2)+'\n')

def analyze_collapsed(path):
    worker=collections.Counter(); totals=collections.Counter(); worker_stacks=collections.Counter()
    for line in path.read_text().splitlines():
        stack,weight=line.rsplit(' ',1); weight=int(weight)
        is_worker='outbox-relay-' in stack or 'OutboxRelay.work' in stack
        if 'Thread.sleep' in stack or 'Thread/sleep' in stack or 'JVM_Sleep' in stack: category='idle_sleep'
        elif 'com/fasterxml/jackson' in stack or 'com.fasterxml.jackson' in stack: category='jackson'
        elif any(frame in stack for frame in ['org/postgresql','org.postgresql','org/springframework/jdbc','org.springframework.jdbc']): category='jdbc'
        elif 'org/apache/kafka' in stack or 'org.apache.kafka' in stack or 'KafkaOutboxDispatcher' in stack: category='kafka'
        else: category='other'
        totals[category]+=weight
        if is_worker:
            worker[category]+=weight
            if category!='idle_sleep': worker_stacks[stack]+=weight
    active=sum(v for k,v in worker.items() if k!='idle_sleep')
    return dict(all=dict(totals),relay=dict(worker),relay_active=active,
                relay_active_share={k:100*v/active if active else None for k,v in worker.items() if k!='idle_sleep'},
                relay_top_active_stacks=[{'stack':s,'weight':w} for s,w in worker_stacks.most_common(15)])

def duration_seconds(value):
    match=re.fullmatch(r'PT(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?',value or '')
    if not match: raise RuntimeError('Unexpected JFR duration: '+str(value))
    hours,minutes,seconds=[float(part or 0) for part in match.groups()]
    return hours*3600+minutes*60+seconds

def main():
    p=argparse.ArgumentParser();p.add_argument('--rps',type=int,default=500);p.add_argument('--duration',type=int,default=120)
    p.add_argument('--runs',type=int,default=3);p.add_argument('--virtual',action='store_true');p.add_argument('--name',default='profile'); args=p.parse_args()
    if args.duration<120 or args.runs<3:p.error('profiling requires three captures of at least 120 seconds')
    RESULTS.mkdir(parents=True,exist_ok=True)
    with awake_host(),open(RESULTS/'.perf.lock','w') as lock, contextlib.ExitStack() as resources:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        resources.callback(cleanup_apps)
        init();configure({'VIRTUAL_THREADS':str(args.virtual).lower()}); identity=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        warm_wall=time.time();warm_monotonic=time.monotonic()
        warm_prefix=f'{identity}-{args.name}-warmup';warm=k6(warm_prefix,args.rps,60)
        resources.callback(cleanup_load,warm_prefix,warm)
        if warm.wait()!=0 or not valid_clock_window(time.time()-warm_wall,time.monotonic()-warm_monotonic,60):raise RuntimeError('Invalid JVM warmup window')
        wait_drain()
        reports=[]
        manifest=dict(created_at=datetime.now(timezone.utc).isoformat(),commit=command(['git','rev-parse','HEAD']).strip(),
                      args=vars(args),warmup_seconds=60,image_ids=compose('images','--format','json'),
                      script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),status='running',profiles=[])
        manifest_path=RESULTS/f'{identity}-{args.name}.profile-manifest.json'
        manifest_path.write_text(json.dumps(manifest,indent=2)+'\n')
        resources.callback(close_manifest,manifest,manifest_path)
        for event in ['cpu','alloc','wall']:
            for run in range(1,args.runs+1):
                reset();prefix=f'{identity}-{args.name}-{event}-r{run}'; load=k6(prefix+'-load',args.rps,args.duration+15)
                resources.callback(cleanup_load,prefix+'-load',load)
                time.sleep(5)
                options=['-e',event,'-t','-d',str(args.duration),'-f',f'/results/{prefix}.html']
                if event=='alloc':options+=['--total']
                if event=='cpu':
                    compose('exec','-T','order-service','jcmd','1','JFR.start',f'name={args.name}{run}','settings=profile',
                        f'duration={args.duration}s',f'filename=/results/{prefix}.jfr','jdk.VirtualThreadPinned#enabled=true',
                        'jdk.VirtualThreadPinned#threshold=1ms','jdk.JavaMonitorEnter#threshold=1ms','jdk.ThreadPark#threshold=1ms')
                wall_start=time.time();monotonic_start=time.monotonic()
                profile_log=compose('exec','-T','order-service','/opt/async-profiler/bin/asprof',*options,'1')
                wall_duration=time.time()-wall_start;monotonic_duration=time.monotonic()-monotonic_start
                dump_options=['dump','-t','-o','collapsed']+(['--total'] if event=='alloc' else [])
                compose('exec','-T','order-service','/opt/async-profiler/bin/asprof',*dump_options,
                        '-f',f'/results/{prefix}.collapsed','1')
                if load.wait()!=0: raise RuntimeError('load process failed during profile')
                wait_drain();stats=analyze_collapsed(RESULTS/f'{prefix}.collapsed')
                if not sum(stats['all'].values()):raise RuntimeError('empty profile; no synthetic substitute')
                if event=='cpu':
                    events=compose('exec','-T','order-service','jfr','print','--json','--events',
                        'jdk.ExecutionSample,jdk.JavaMonitorEnter,jdk.ThreadPark,jdk.VirtualThreadPinned,jdk.SocketRead,jdk.SocketWrite',f'/results/{prefix}.jfr')
                    data=json.loads(events);jfr=collections.Counter(); waits=collections.Counter();wait_seconds=collections.Counter();frames=collections.Counter()
                    for e in data['recording']['events']:
                        jfr[e['type']]+=1;v=e['values'];thread=v.get('eventThread') or v.get('sampledThread') or {}; name=thread.get('javaName','unknown')
                        if e['type'] in ['jdk.JavaMonitorEnter','jdk.ThreadPark','jdk.VirtualThreadPinned']:
                            waits[(e['type'],name)]+=1
                            wait_seconds[(e['type'],name)]+=duration_seconds(v.get('duration'))
                            for frame in (v.get('stackTrace') or {}).get('frames',[])[:5]:
                                method=frame['method'];frames[method['type']['name']+'.'+method['name']]+=1
                    stats['jfr_events']=dict(jfr);stats['jfr_wait_threads']=[{'event':e,'thread':t,'count':c} for (e,t),c in waits.most_common()]
                    stats['jfr_wait_seconds']=[{'event':e,'thread':t,'seconds':s} for (e,t),s in wait_seconds.most_common()]
                    stats['jfr_wait_top_frames']=dict(frames.most_common(30))
                    with gzip.open(RESULTS/f'{prefix}.jfr.gz','wb') as out:out.write((RESULTS/f'{prefix}.jfr').read_bytes())
                    (RESULTS/f'{prefix}.jfr').unlink()
                report=dict(profile=prefix,event=event,virtual=args.virtual,commit=command(['git','rev-parse','HEAD']).strip(),statistics=stats,profiler_output=profile_log)
                if args.virtual:
                    report['attribution_note']='async-profiler CPU/wall stacks may stop at virtual continuation barriers; relay shares only cover attributable stacks. JFR is used for pinning.'
                report['wall_seconds']=wall_duration;report['monotonic_seconds']=monotonic_duration
                if not valid_clock_window(wall_duration,monotonic_duration,args.duration):report['excluded_reason']='Invalid profiler clock window'
                (RESULTS/f'{prefix}.profile.json').write_text(json.dumps(report,indent=2)+'\n');reports.append(report)
                if report.get('excluded_reason'):raise RuntimeError('System sleep or clock discontinuity during profiling')
                manifest['profiles']=[r['profile'] for r in reports];manifest_path.write_text(json.dumps(manifest,indent=2)+'\n')
                print(f'{prefix}: {stats["relay_active_share"]}',flush=True)
        (RESULTS/f'{identity}-{args.name}-pinning.txt').write_text(compose('logs','--no-color','order-service'))
        (RESULTS/f'{identity}-{args.name}.profiles.json').write_text(json.dumps(reports,indent=2)+'\n')
        manifest['status']='complete';manifest_path.write_text(json.dumps(manifest,indent=2)+'\n')

if __name__=='__main__':main()
