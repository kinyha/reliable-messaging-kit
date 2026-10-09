"""Acceptance fixtures only, never measurements or inputs to performance plots."""
import importlib.util,json,sys,tempfile,unittest
from pathlib import Path
SCRIPTS=Path(__file__).resolve().parents[1];sys.path.insert(0,str(SCRIPTS))
spec=importlib.util.spec_from_file_location('verify_baseline',SCRIPTS/'verify-baseline.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


class BaselineAcceptanceTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.folder=Path(self.temp.name);self.repeat=[]
        for name in ['first','repeat']:
            ids=[]
            for rate in [100,300,500,1000]:
                for repeat in [1,2,3]:
                    run=f'{name}-{rate}-r{repeat}';ids.append(run)
                    stats=dict(requested_rps=rate,duration_seconds=180,wall_seconds=181,monotonic_seconds=181,clock_max_gap_seconds=0,
                               http_p99_ms=repeat,e2e_p99_ms=repeat*100,
                               **{k:0 for k in ['http_failed','dropped_iterations','pending_after_drain','missing_payments','unexpected_payments','duplicate_payments']})
                    path=self.folder/(run+'.measurement.json');path.write_text(json.dumps(dict(run_id=run,suite='baseline',campaign=name,statistics=stats)))
                    path.with_name(run+'.json').write_text('{}')
                    if name=='repeat':self.repeat.append(path)
            (self.folder/(name+'-baseline.manifest.json')).write_text(json.dumps(dict(args=dict(name=name,warmup=60),measurements=ids,
                image_ids=json.dumps([dict(ContainerName='fixture',ID='fixture-image',Platform='fixture-arch')]))))
    def tearDown(self):self.temp.cleanup()
    def test_accepts_complete_identical_pairs(self):self.assertTrue(module.compare(self.folder,'first','repeat')['passed'])
    def test_preserves_an_outside_range_result_as_a_failed_criterion(self):
        for path in self.repeat:
            doc=json.loads(path.read_text());doc['statistics']['http_p99_ms']=10;path.write_text(json.dumps(doc))
        report=module.compare(self.folder,'first','repeat');self.assertFalse(report['passed'])
        self.assertEqual(report['complete_correct_windows'],24)
    def test_rejects_missing_window(self):
        self.repeat[0].unlink()
        with self.assertRaisesRegex(RuntimeError,'all four rates'):module.compare(self.folder,'first','repeat')
    def test_rejects_different_images(self):
        path=self.folder/'repeat-baseline.manifest.json';doc=json.loads(path.read_text());images=json.loads(doc['image_ids']);images[0]['ID']='other'
        doc['image_ids']=json.dumps(images);path.write_text(json.dumps(doc))
        with self.assertRaisesRegex(RuntimeError,'different images'):module.compare(self.folder,'first','repeat')


if __name__=='__main__':unittest.main()
