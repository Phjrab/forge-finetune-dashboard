import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from tools.import_mediflow_handoff import import_handoff


class HandoffCatalogTests(unittest.TestCase):
    def test_status_only_import_preserves_originals_and_verifies_completion_hash(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / 'source'; source.mkdir()
            def write(name, value):
                path = source / name
                path.write_text(json.dumps(value))
                return path
            receipt = write('receipt.json', {'status': 'SUCCEEDED', 'optimizer_steps': 500, 'samples_processed': 7997})
            manifest = write('manifest.json', {'jobs': [{'job_id': 'v6-first-seed-T1', 'model_id': 'model/pinned'},
                                                       {'job_id': 'v6-first-seed-T2', 'model_id': 'model/pinned'}]})
            handoff = {'writer': 'TRAINING_OWNER', 'repository_head': 'a' * 40,
                       'old_campaign': {'campaign_id': 'old-campaign', 'manifest_path': str(manifest),
                                        'unstarted_jobs': {'v6-first-seed-T2': {'transition_status': 'MIGRATION_HOLD'}}},
                       'completed_first_seed': {'v6-first-seed-T1': {
                           'receipt': str(receipt), 'receipt_sha256': hashlib.sha256(receipt.read_bytes()).hexdigest(),
                           'final_checkpoint_verified': True, 'fresh_process_reload_verified': True}},
                       'active_workers': []}
            path = write('handoff.json', handoff)
            before = {p.name: p.read_bytes() for p in source.iterdir()}
            result = import_handoff(path, source, root / 'catalog')
            self.assertEqual(result['status_only_runs'], 2)
            self.assertEqual(before, {p.name: p.read_bytes() for p in source.iterdir()})
            status = json.loads((root / 'catalog/old-campaign/v6-first-seed-T1/coordination/status.json').read_text())
            self.assertEqual(status['optimizer_step'], 500)
            self.assertFalse(status['telemetry_complete'])
            self.assertNotIn('train_loss', status)
            self.assertFalse(list((root / 'catalog').rglob('steps.jsonl')))
            receipt.write_text('{}')
            with self.assertRaisesRegex(ValueError, 'HASH_MISMATCH'):
                import_handoff(path, source, root / 'catalog')

    def test_manifest_symlink_escape_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); source = root / 'source'; source.mkdir()
            outside = root / 'private.json'; outside.write_text('{"jobs":[]}')
            link = source / 'manifest.json'; link.symlink_to(outside)
            path = source / 'handoff.json'
            path.write_text(json.dumps({'writer': 'TRAINING_OWNER', 'old_campaign': {
                'campaign_id': 'campaign', 'manifest_path': str(link)}}))
            with self.assertRaisesRegex(ValueError, 'UNSAFE_SOURCE_PATH'):
                import_handoff(path, source, root / 'catalog')


if __name__ == '__main__':
    unittest.main()
