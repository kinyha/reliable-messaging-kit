#!/usr/bin/env python3
"""Make a readable derivative from actual weighted collapsed stacks, preserving raw data."""
import argparse,hashlib,json,subprocess,tempfile
from pathlib import Path

parser=argparse.ArgumentParser();parser.add_argument('profile',type=Path);parser.add_argument('--converter',type=Path,required=True)
args=parser.parse_args();doc=json.loads(args.profile.read_text());prefix=args.profile.name.removesuffix('.profile.json')
source=args.profile.with_name(prefix+'.collapsed');output=args.profile.with_name(prefix+'-relay.html')
selected=[]
with source.open() as file:
    for line in file:
        stack,weight=line.rsplit(' ',1)
        if not ('outbox-relay-' in stack or 'OutboxRelay.work' in stack):continue
        if any(frame in stack for frame in ['Thread.sleep','Thread/sleep','JVM_Sleep']):continue
        selected.append((line,int(weight)))
weight=sum(w for _,w in selected)
if weight!=doc['statistics']['relay_active'] or weight<=0:raise RuntimeError('Filtered weight differs from the captured active relay statistic')
unit='estimated bytes' if doc['event']=='alloc' else 'samples'
title=f'Active relay {doc["event"]} ({unit}): {prefix}'
with tempfile.NamedTemporaryFile(mode='w',suffix='.collapsed') as temporary:
    temporary.writelines(line for line,_ in selected);temporary.flush()
    subprocess.run(['java','-Xmx128m','-jar',str(args.converter),'--title',title,temporary.name,str(output)],check=True)
metadata=dict(source=source.name,source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),output=output.name,
              output_sha256=hashlib.sha256(output.read_bytes()).hexdigest(),captured_weight=weight,unit=unit,
              filter='Only attributable relay stacks; exclude Thread.sleep/JVM_Sleep idle poll, as in profile.py',
              converter='async-profiler 4.5 jfrconv (copied unchanged from the measured image)',
              converter_sha256=hashlib.sha256(args.converter.read_bytes()).hexdigest())
output.with_suffix('.render.json').write_text(json.dumps(metadata,indent=2)+'\n')
print('Preserved original weights:',weight,unit)
