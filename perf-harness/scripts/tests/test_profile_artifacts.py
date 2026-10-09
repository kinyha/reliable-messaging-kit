"""Temporary acceptance fixtures only; these are never measurement evidence."""
import importlib.util,json,sys,tempfile,unittest
from pathlib import Path
SCRIPTS=Path(__file__).resolve().parents[1];sys.path.insert(0,str(SCRIPTS))
spec=importlib.util.spec_from_file_location('collect_profiles',SCRIPTS/'collect-profiles.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


class ProfileAcceptanceTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.folder=Path(self.temp.name);self.files=[]
        prefixes=[]
        for event in ['cpu','alloc','wall']:
            for repeat in [1,2,3]:
                prefix=f'fixture-profile-{event}-r{repeat}';prefixes.append(prefix)
                doc=dict(profile=prefix,event=event,commit='fixture',virtual=True,wall_seconds=121,monotonic_seconds=121,statistics=dict(all=dict(other=1)))
                path=self.folder/(prefix+'.profile.json');path.write_text(json.dumps(doc));self.files.append(path)
                for ext in ['.html','.collapsed']+(['.jfr.gz'] if event=='cpu' else []):(self.folder/(prefix+ext)).write_text('fixture')
                (self.folder/(prefix+'-load.json')).write_text(json.dumps(dict(metrics={})))
        self.manifest=self.folder/'fixture.profile-manifest.json'
        self.manifest.write_text(json.dumps(dict(status='complete',commit='fixture',args=dict(name='fixture',runs=3,duration=120,virtual=True),warmup_seconds=60,profiles=prefixes)))
        (self.folder/'fixture.profiles.json').write_text('[]');(self.folder/'fixture-pinning.txt').write_text('fixture')
    def tearDown(self):self.temp.cleanup()
    def change(self,edit):
        path=self.files[0];doc=json.loads(path.read_text());edit(doc);path.write_text(json.dumps(doc))
    def test_accepts_all_three_modes_and_repetitions(self):self.assertEqual(len(module.validate(self.folder,'fixture')[2]),9)
    def test_rejects_partial_manifest(self):
        data=json.loads(self.manifest.read_text());data['profiles'].pop();self.manifest.write_text(json.dumps(data))
        with self.assertRaisesRegex(RuntimeError,'exactly three'):module.validate(self.folder,'fixture')
    def test_rejects_mixed_commits(self):
        self.change(lambda d:d.update(commit='other'))
        with self.assertRaisesRegex(RuntimeError,'Mixed'):module.validate(self.folder,'fixture')
    def test_rejects_invalid_clock_window(self):
        self.change(lambda d:d.update(wall_seconds=1200))
        with self.assertRaisesRegex(RuntimeError,'clock window'):module.validate(self.folder,'fixture')
    def test_rejects_missing_recording(self):
        (self.folder/'fixture-profile-cpu-r1.jfr.gz').unlink()
        with self.assertRaisesRegex(RuntimeError,'Missing real'):module.validate(self.folder,'fixture')


if __name__=='__main__':unittest.main()
