#!/usr/bin/env python3
"""Validate a whole H2/H3 campaign before accepting its GitHub artifact."""
import argparse, json, shutil
from pathlib import Path
from benchmark import ROOT, suite_configs, valid_clock_window


def validate(folder, suite):
    campaign='gha-'+suite
    documents=[]; selected=[]
    for path in folder.rglob('*.measurement.json'):
        doc=json.loads(path.read_text())
        if doc.get('suite')==suite and doc.get('campaign')==campaign:
            documents.append((path,doc))
    expected={(json.dumps(config,sort_keys=True),rps) for _,config,rps in suite_configs(suite)}
    groups={key:[] for key in expected}
    identities=set()
    for path,doc in documents:
        if doc['run_id'] in identities:raise RuntimeError('Repeated measurement identity')
        identities.add(doc['run_id'])
        if doc['config'].get('CHAOS_DELAY')!='0ms':raise RuntimeError('Unexpected chaos delay')
        config={k:v for k,v in doc['config'].items() if k!='CHAOS_DELAY'}
        stats=doc['statistics']; key=(json.dumps(config,sort_keys=True),stats['requested_rps'])
        if key not in groups:raise RuntimeError('Unexpected experiment configuration')
        if doc.get('validation_only') or doc.get('excluded_reason') or stats['duration_seconds']<180:
            raise RuntimeError('Excluded or shortened experiment')
        if not valid_clock_window(stats['wall_seconds'],stats['monotonic_seconds'],stats['duration_seconds'],stats['clock_max_gap_seconds']):
            raise RuntimeError('Invalid experiment clock window')
        if any(stats[k] for k in ['http_failed','dropped_iterations','pending_after_drain','missing_payments','unexpected_payments','duplicate_payments']):
            raise RuntimeError('Experiment did not sustain complete correct delivery')
        groups[key].append(doc)
        raw=path.with_name(doc['run_id']+'.json')
        if not raw.is_file():raise RuntimeError('Missing raw k6 summary')
        selected.extend([path,raw])
    if any(len(group)!=3 for group in groups.values()):raise RuntimeError('Every configuration needs exactly three complete measurements')
    manifests=[]
    wanted={doc['run_id'] for _,doc in documents}
    for path in folder.rglob('*-'+suite+'.manifest.json'):
        manifest=json.loads(path.read_text())
        if manifest.get('args',{}).get('name')!=campaign:continue
        if manifest.get('status')!='complete' or manifest['args']['warmup']<60 or set(manifest['measurements'])!=wanted:
            raise RuntimeError('Incomplete experiment manifest')
        manifests.append(path)
    if len(manifests)!=1:raise RuntimeError('Need one complete campaign on one VM')
    return selected+manifests


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('artifacts',type=Path)
    parser.add_argument('--suite',choices=['virtual','matrix'],required=True);args=parser.parse_args()
    for source in validate(args.artifacts,args.suite):
        target=ROOT/'perf-harness/results'/source.name
        if target.exists() and target.read_bytes()!=source.read_bytes():raise RuntimeError('Refusing to overwrite '+target.name)
        shutil.copyfile(source,target)
    print('Accepted every configuration and all three repetitions from one isolated '+args.suite+' VM')
