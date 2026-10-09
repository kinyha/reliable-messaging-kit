"""Acceptance fixtures only, never exported as measured performance results."""
import importlib.util,json,sys,tempfile,unittest
from pathlib import Path
SCRIPTS=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(SCRIPTS))
spec=importlib.util.spec_from_file_location('collect_experiment',SCRIPTS/'collect-experiment.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


class ExperimentAcceptanceTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.folder=Path(self.temp.name);self.files=[]
        for label,config,rps in module.suite_configs('virtual'):
            for repeat in [1,2,3]:
                name=f'20260101T000000Z-gha-virtual-{label}-r{repeat}'
                stats=dict(requested_rps=rps,duration_seconds=180,wall_seconds=181,monotonic_seconds=181,clock_max_gap_seconds=0,
                    **{k:0 for k in ['http_failed','dropped_iterations','pending_after_drain','missing_payments','unexpected_payments','duplicate_payments']})
                doc=dict(run_id=name,config=dict(config,CHAOS_DELAY='0ms'),suite='virtual',campaign='gha-virtual',statistics=stats)
                path=self.folder/(name+'.measurement.json');path.write_text(json.dumps(doc));self.files.append(path)
                path.with_name(name+'.json').write_text('{}')
        self.manifest=self.folder/'20260101T000000Z-virtual.manifest.json'
        self.manifest.write_text(json.dumps(dict(status='complete',args=dict(name='gha-virtual',warmup=60),
            measurements=[json.loads(p.read_text())['run_id'] for p in self.files])))
    def tearDown(self):self.temp.cleanup()
    def change(self,edit):
        path=self.files[0];doc=json.loads(path.read_text());edit(doc);path.write_text(json.dumps(doc))
    def test_accepts_the_complete_same_vm_campaign(self):self.assertEqual(len(module.validate(self.folder,'virtual')),25)
    def test_rejects_missing_repetition(self):
        self.files[0].unlink()
        with self.assertRaisesRegex(RuntimeError,'exactly three'):module.validate(self.folder,'virtual')
    def test_rejects_load_generator_drops(self):
        self.change(lambda d:d['statistics'].update(dropped_iterations=1))
        with self.assertRaisesRegex(RuntimeError,'correct delivery'):module.validate(self.folder,'virtual')
    def test_rejects_suspended_clock_window(self):
        self.change(lambda d:d['statistics'].update(wall_seconds=1810))
        with self.assertRaisesRegex(RuntimeError,'clock window'):module.validate(self.folder,'virtual')
    def test_rejects_unfinished_manifest_despite_complete_jsons(self):
        manifest=json.loads(self.manifest.read_text());manifest['status']='running';self.manifest.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(RuntimeError,'Incomplete'):module.validate(self.folder,'virtual')


if __name__=='__main__':unittest.main()
