#!/usr/bin/env python3
import argparse,json,sys
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('measurement',type=Path);p.add_argument('--thresholds',type=Path,default=Path(__file__).resolve().parents[1]/'k6/thresholds.json');args=p.parse_args()
document=json.loads(args.measurement.read_text())
if document.get('excluded_reason'):
 print('FAIL: invalid measurement window: '+document['excluded_reason']);sys.exit(1)
stats=document['statistics'];limits=json.loads(args.thresholds.read_text())
checks={
 'HTTP p99':(stats.get('http_p99_ms'),limits['http_p99_ms']),
 'e2e p99':(stats.get('e2e_p99_ms'),limits['e2e_p99_ms']),
 'HTTP error rate':(stats.get('http_error_rate'),limits['max_http_error_rate']),
 'dropped iterations':(stats.get('dropped_iterations'),limits['max_dropped_iterations']),
 'pending after drain':(stats.get('pending_after_drain'),limits['max_pending_after_drain']),
 'missing payments':(stats.get('missing_payments'),limits['max_missing_payments']),
 'duplicate payments':(stats.get('duplicate_payments'),limits['max_duplicate_payments']),
}
failed=[]
for name,(actual,maximum) in checks.items():
 good=actual is not None and actual<=maximum
 print(f'{"PASS" if good else "FAIL"}: {name}: {actual} <= {maximum}')
 if not good:failed.append(name)
if failed:sys.exit(1)
