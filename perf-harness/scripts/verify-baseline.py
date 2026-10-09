#!/usr/bin/env python3
"""Check complete baseline pairs and report the original strict min/max criterion."""
import argparse, json, statistics
from pathlib import Path
from benchmark import valid_clock_window


def campaign(folder, name):
    groups={r:[] for r in [100,300,500,1000]};identities=set()
    for path in folder.rglob('*.measurement.json'):
        doc=json.loads(path.read_text())
        if doc.get('suite')!='baseline' or doc.get('campaign')!=name:continue
        stats=doc['statistics'];rps=stats['requested_rps']
        if rps not in groups or doc['run_id'] in identities:raise RuntimeError('Unexpected or duplicate baseline identity')
        identities.add(doc['run_id'])
        if doc.get('validation_only') or doc.get('excluded_reason') or stats['duration_seconds']<180:
            raise RuntimeError('Excluded or shortened baseline')
        if not valid_clock_window(stats['wall_seconds'],stats['monotonic_seconds'],stats['duration_seconds'],stats['clock_max_gap_seconds']):
            raise RuntimeError('Invalid baseline clock window')
        if any(stats[k] for k in ['http_failed','dropped_iterations','pending_after_drain','missing_payments','unexpected_payments','duplicate_payments']):
            raise RuntimeError('Baseline did not sustain complete correct delivery')
        if any(stats.get(k) is None for k in ['http_p99_ms','e2e_p99_ms']):raise RuntimeError('Missing finite baseline latency')
        if not path.with_name(doc['run_id']+'.json').is_file():raise RuntimeError('Missing raw k6 summary')
        groups[rps].append(doc)
    if any(len(docs)!=3 for docs in groups.values()):raise RuntimeError('Baseline requires all four rates and exactly three repetitions')
    manifests=[]
    for path in folder.rglob('*-baseline.manifest.json'):
        data=json.loads(path.read_text())
        covered=set(data.get('measurements',[]))&identities
        if not covered:continue
        if data['args']['name']!=name or data['args']['warmup']<60:raise RuntimeError('Unexpected baseline manifest')
        # A resumed campaign may retain complete earlier rates from an interrupted manifest.
        # Only the accepted 12 measurement identities are considered, never partial excluded rates.
        manifests.append((data,covered))
    if not manifests or set().union(*(ids for _,ids in manifests))!=identities:
        raise RuntimeError('Missing manifests for accepted baseline windows')
    signatures=[]
    for manifest,_ in manifests:
        images=json.loads(manifest['image_ids'])
        signatures.append(tuple(sorted((x['ContainerName'],x['ID'],x['Platform']) for x in images)))
    if len(set(signatures))!=1:raise RuntimeError('Mixed baseline images or architectures')
    return groups,signatures[0]


def compare(folder, first, repeat):
    before,first_images=campaign(folder,first);after,repeat_images=campaign(folder,repeat)
    if first_images!=repeat_images:raise RuntimeError('First and repeat used different images or architectures')
    rows=[]
    for rps in before:
        for key in ['http_p99_ms','e2e_p99_ms']:
            a=[d['statistics'][key] for d in before[rps]];b=[d['statistics'][key] for d in after[rps]]
            middle=statistics.median(b)
            rows.append(dict(rps=rps,metric=key,first_median=statistics.median(a),first_min=min(a),first_max=max(a),
                             repeat_median=middle,repeat_min=min(b),repeat_max=max(b),inside_first_range=min(a)<=middle<=max(a)))
    return dict(first=first,repeat=repeat,images=first_images,complete_correct_windows=24,
                criterion='Every repeat median must lie inside the original three-run min/max range',
                passed=all(row['inside_first_range'] for row in rows),comparisons=rows)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('results',type=Path);parser.add_argument('--first',required=True)
    parser.add_argument('--repeat',required=True);parser.add_argument('--output',type=Path);args=parser.parse_args()
    report=compare(args.results,args.first,args.repeat);text=json.dumps(report,indent=2)+'\n'
    if args.output:args.output.write_text(text)
    print(text,end='')
    raise SystemExit(0 if report['passed'] else 1)
