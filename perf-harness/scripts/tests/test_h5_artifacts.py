"""Structural fixtures only; these are not performance measurements."""
import importlib.util,json,tempfile,unittest
from pathlib import Path
spec=importlib.util.spec_from_file_location('collect_h5',Path(__file__).resolve().parents[1]/'collect-h5.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class H5AcceptanceTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.folder=Path(self.temp.name);self.files=[]
        for replica in [1,2,3]:
            name=f'20260101T000000Z-gha-h5-replica-{replica}'
            stats=dict(duration_seconds=7200,wall_seconds=7201,clock_max_gap_seconds=0,committed_rps=100,
                       **{k:0 for k in ['http_failed','dropped_iterations','pending_after_drain','missing_payments','unexpected_payments','duplicate_payments']})
            d=dict(run_id=name,replica=replica,commit='0'*40,architecture='x86_64',config={'RETENTION':'30s','CLEANUP_INTERVAL':'5s'},suite='soak',campaign='gha-h5',statistics=stats)
            p=self.folder/(name+'.measurement.json');p.write_text(json.dumps(d));self.files.append(p)
            (self.folder/(name+'.json')).write_text('{}')
            (self.folder/(name+'.replica-manifest.json')).write_text(json.dumps(dict(status='complete',warmup_seconds=60,commit=d['commit'],measurement=name)))
    def tearDown(self):self.temp.cleanup()
    def change(self,edit):
        p=self.files[0];d=json.loads(p.read_text());edit(d);p.write_text(json.dumps(d))
    def test_accepts_all_three_complete_replicas(self):self.assertEqual(len(module.validate(self.folder)),9)
    def test_rejects_missing_replica(self):
        self.files[1].unlink()
        with self.assertRaisesRegex(RuntimeError,'exactly'):module.validate(self.folder)
    def test_rejects_short_wall_time_even_when_duration_claims_two_hours(self):
        self.change(lambda d:d['statistics'].update(wall_seconds=7199))
        with self.assertRaisesRegex(RuntimeError,'Incomplete'):module.validate(self.folder)
    def test_rejects_mixed_source_commits(self):
        self.change(lambda d:d.update(commit='1'*40))
        with self.assertRaisesRegex(RuntimeError,'commit'):module.validate(self.folder)
    def test_rejects_lost_business_effect(self):
        self.change(lambda d:d['statistics'].update(missing_payments=1))
        with self.assertRaisesRegex(RuntimeError,'correct delivery'):module.validate(self.folder)

if __name__=='__main__':unittest.main()
