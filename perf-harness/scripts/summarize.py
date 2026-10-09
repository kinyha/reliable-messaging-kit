#!/usr/bin/env python3
import collections, json, statistics
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
groups=collections.defaultdict(list)
for file in sorted((ROOT/'perf-harness/results').glob('*.measurement.json')):
    doc=json.loads(file.read_text())
    if doc.get('validation_only') or doc.get('excluded_reason') or 'suite' not in doc: continue
    key=(doc['suite']+' '+doc.get('campaign',''),json.dumps(doc['config'],sort_keys=True),doc['statistics']['requested_rps'])
    groups[key].append(doc)
lines=['# Измерения этапа 2','','Медиана и min/max по настоящим прогонам; миллисекунды. Пустой e2e — нет измеренного эффекта либо квантиль попал в +Inf bucket.','',
'| Suite / config | rps | N | HTTP p99 median [min; max] | e2e p99 median [min; max] | committed rps | DB calls/s |','|---|---:|---:|---:|---:|---:|---:|']
for (suite,config,rps),docs in groups.items():
    def stats(key):
        v=[d['statistics'][key] for d in docs if d['statistics'].get(key) is not None]
        return f'{statistics.median(v):.2f} [{min(v):.2f}; {max(v):.2f}]' if v else '—'
    lines.append(f'| {suite} {config} | {rps} | {len(docs)} | {stats("http_p99_ms")} | {stats("e2e_p99_ms")} | {stats("committed_rps")} | {stats("db_calls_per_second")} |')
(ROOT/'docs/perf/measurements.md').write_text('\n'.join(lines)+'\n')
