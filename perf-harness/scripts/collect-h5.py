#!/usr/bin/env python3
"""Accept all three complete independent H5 artifacts, never a shortened or partial set."""
import argparse,json,re,shutil
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]

def validate(folder):
    files=list(folder.rglob('*gha-h5-replica-*.measurement.json'))
    docs=[json.loads(p.read_text()) for p in files]
    if len(docs)!=3 or {d.get('replica') for d in docs}!={1,2,3}:raise RuntimeError('H5 requires exactly replicas 1, 2 and 3')
    for field in ['commit','architecture','config']:
        if len({json.dumps(d.get(field),sort_keys=True) for d in docs})!=1:raise RuntimeError('Replicas differ in '+field)
    selected=[]
    for p,d in zip(files,docs):
        s=d['statistics'];prefix=d['run_id']
        if not re.fullmatch(r'\d{8}T\d{6}Z-gha-h5-replica-[123]',prefix):raise RuntimeError('Unexpected artifact identity')
        if d.get('excluded_reason') or d.get('suite')!='soak' or d.get('campaign')!='gha-h5' or s['duration_seconds']!=7200 or not 7200<=s['wall_seconds']<=7260 or s.get('clock_max_gap_seconds',float('inf'))>=1:raise RuntimeError('Incomplete two-hour observation')
        if any(s[key]!=0 for key in ['http_failed','dropped_iterations','pending_after_drain','missing_payments','unexpected_payments','duplicate_payments']):raise RuntimeError('H5 did not sustain correct delivery')
        if s['committed_rps']<99:raise RuntimeError('H5 requested rate was not sustained')
        manifest=p.with_name(prefix+'.replica-manifest.json')
        m=json.loads(manifest.read_text())
        if m['status']!='complete' or m['warmup_seconds']<60 or m['commit']!=d['commit'] or m['measurement']!=prefix:raise RuntimeError('Invalid replica manifest')
        for suffix in ['.json','.measurement.json','.replica-manifest.json']:
            artifact=p.with_name(prefix+suffix)
            if not artifact.is_file():raise RuntimeError('Missing raw artifact '+artifact.name)
            selected.append(artifact)
    return selected

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('artifacts',type=Path);args=p.parse_args()
    artifacts=validate(args.artifacts)
    for source in artifacts:
        target=ROOT/'perf-harness/results'/source.name
        if target.exists() and target.read_bytes()!=source.read_bytes():raise RuntimeError('Refusing to overwrite a different measurement: '+target.name)
        shutil.copyfile(source,target)
    print('Accepted three full, correct two-hour H5 replicas with matching commit/config/architecture')
