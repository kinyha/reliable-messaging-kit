#!/usr/bin/env python3
"""One fixed two-hour H5 replica; a complete result requires replicas 1, 2 and 3."""
import argparse, contextlib, fcntl, hashlib, json, platform
from datetime import datetime,timezone
from pathlib import Path
from benchmark import ROOT,RESULTS,awake_host,cleanup_load,command,compose,configure,init,k6,reset,run_case,valid_clock_window,wait_drain


def main():
    p=argparse.ArgumentParser();p.add_argument('--repeat',type=int,choices=[1,2,3],required=True);args=p.parse_args()
    RESULTS.mkdir(parents=True,exist_ok=True)
    with awake_host(),open(RESULTS/'.perf.lock','w') as lock, contextlib.ExitStack() as resources:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        command(['./gradlew',':demo-stand:order-service:bootJar',':demo-stand:payment-service:bootJar'])
        compose('build','order-service','payment-service');init()
        config={'RETENTION':'30s','CLEANUP_INTERVAL':'5s'};configure(config)
        identity=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        prefix=f'{identity}-gha-h5-replica-{args.repeat}'
        docker=json.loads(command(['docker','info','--format','{{json .}}']))
        manifest=dict(commit=command(['git','rev-parse','HEAD']).strip(),replica=args.repeat,
                      required_replicas=[1,2,3],warmup_seconds=60,duration_seconds=7200,rps=100,
                      architecture=platform.machine(),docker_info={k:docker.get(k) for k in ['ServerVersion','NCPU','MemTotal','Architecture','OperatingSystem','KernelVersion']},
                      image_ids=compose('images','--format','json'),compose_config=json.loads(compose('config','--format','json')),
                      status='running',source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
        path=RESULTS/f'{prefix}.replica-manifest.json';path.write_text(json.dumps(manifest,indent=2)+'\n')
        try:
            import time
            warm_wall=time.time();warm_monotonic=time.monotonic();warm=k6(prefix+'-warmup',100,60)
            resources.callback(cleanup_load,prefix+'-warmup',warm)
            if warm.wait()!=0 or not valid_clock_window(time.time()-warm_wall,time.monotonic()-warm_monotonic,60):raise RuntimeError('Invalid H5 warmup window')
            wait_drain();reset()
            doc=run_case(prefix,100,7200,True,soak=True,strict=True)
            doc.update(config=config,suite='soak',campaign='gha-h5',replica=args.repeat,commit=manifest['commit'],
                       architecture=manifest['architecture'],required_replicas=[1,2,3])
            (RESULTS/f'{prefix}.measurement.json').write_text(json.dumps(doc,indent=2)+'\n')
            if doc.get('excluded_reason'):raise RuntimeError('Invalid H5 clock window')
            manifest['status']='complete';manifest['measurement']=doc['run_id'];path.write_text(json.dumps(manifest,indent=2)+'\n')
        finally:compose('stop','order-service','payment-service')


if __name__=='__main__':main()
