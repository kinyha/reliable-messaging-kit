#!/usr/bin/env python3
"""Accept complete real profiling campaigns; never substitute partial captures."""
import argparse, json, shutil
from pathlib import Path
from benchmark import ROOT, valid_clock_window


def validate(folder, name):
    manifests=[]
    for path in folder.rglob('*.profile-manifest.json'):
        data=json.loads(path.read_text())
        if data.get('args',{}).get('name')==name:manifests.append((path,data))
    if len(manifests)!=1:raise RuntimeError('Need one profiling manifest for the campaign')
    path,manifest=manifests[0];args=manifest['args']
    if manifest.get('status')!='complete' or args['runs']!=3 or args['duration']<120 or manifest['warmup_seconds']<60:
        raise RuntimeError('Incomplete or shortened profiling campaign')
    events=args.get('events',['cpu','alloc','wall'])
    expected={(event,repeat) for event in events for repeat in [1,2,3]}
    seen=set();selected=[path];reports=[]
    for prefix in manifest['profiles']:
        report_path=path.parent/(prefix+'.profile.json');doc=json.loads(report_path.read_text());stats=doc['statistics']
        key=(doc['event'],int(prefix.rsplit('-r',1)[1]))
        if key not in expected or key in seen:raise RuntimeError('Unexpected or repeated profile identity')
        seen.add(key)
        if doc['profile']!=prefix or doc['commit']!=manifest['commit'] or doc['virtual']!=args['virtual']:
            raise RuntimeError('Mixed profile source or mode')
        if doc.get('excluded_reason') or not valid_clock_window(doc['wall_seconds'],doc['monotonic_seconds'],args['duration']):
            raise RuntimeError('Invalid profiling clock window')
        if sum(stats['all'].values())<=0:raise RuntimeError('Empty profile')
        if args.get('fast_empty_headers') is not None:
            if doc.get('config',{}).get('FAST_EMPTY_HEADERS')!=args['fast_empty_headers'] or stats.get('sent_during_capture',0)<=0:
                raise RuntimeError('Missing optimization mode or allocation normalization')
            if any(stats['reconciliation'][k] for k in ['missing_payments','unexpected_payments','duplicate_payments']):
                raise RuntimeError('Profile delivery reconciliation failed')
        extensions=['.profile.json','.html','.collapsed','-load.json']+(['.jfr.gz'] if doc['event']=='cpu' else [])
        for extension in extensions:
            source=path.parent/(prefix+extension)
            if not source.is_file() or not source.stat().st_size:raise RuntimeError('Missing real profile artifact: '+source.name)
            selected.append(source)
        raw=json.loads((path.parent/(prefix+'-load.json')).read_text())['metrics']
        if any(raw.get(k,{}).get('values',{}).get('count',0) for k in ['orders_failed','dropped_iterations']):
            raise RuntimeError('Profile load did not sustain the requested arrival rate')
        reports.append(doc)
    if seen!=expected:raise RuntimeError('Need exactly three captures of every requested mode')
    for suffix in ['.profiles.json','-pinning.txt']:
        source=path.parent/(path.name.removesuffix('.profile-manifest.json')+suffix)
        if not source.is_file():raise RuntimeError('Missing campaign summary or service log')
        selected.append(source)
    return selected,manifest,reports


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('artifacts',type=Path);parser.add_argument('--name',required=True);args=parser.parse_args()
    selected,manifest,reports=validate(args.artifacts,args.name)
    for source in selected:
        target=ROOT/'perf-harness/results'/source.name
        if target.exists() and target.read_bytes()!=source.read_bytes():raise RuntimeError('Refusing to overwrite '+target.name)
        if target.resolve()!=source.resolve():shutil.copyfile(source,target)
    print(f'Accepted {len(reports)} real full profiles from {manifest["commit"]}')
