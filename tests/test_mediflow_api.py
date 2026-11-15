"""Mediflow transport uses existing authentication and cannot dispatch training."""
import http.client
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from auth import AuthStore, COOKIE_NAME
from server import DashboardServer, main
from simulator import Simulator


class MediflowAPITests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.auth = AuthStore(root / 'auth.sqlite3')
        token, session = self.auth.login('admin', 'admin', '127.0.0.1')
        self.token, self.session = self.auth.change_password(
            token, '127.0.0.1', 'admin', 'Private-test-password-123!', session['csrf_token'])
        self.sim = Simulator(root / 'state.sqlite3')
        self.bridge = Mock()
        self.bridge.capabilities.return_value = {'start': False, 'read': True}
        self.bridge.dispatch.return_value = {'items': [], 'next_after_seq': 0}
        self.server = DashboardServer(('127.0.0.1', 0), self.sim, root,
                                      auth=self.auth, mediflow=self.bridge)
        self.port = self.server.server_port
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()
        self.auth.close()
        self.sim.close()
        self.tmp.cleanup()

    def request(self, path, body=None, auth=True, csrf=True):
        conn = http.client.HTTPConnection('127.0.0.1', self.port, timeout=5)
        headers = {'Cookie': COOKIE_NAME + '=' + self.token} if auth else {}
        if body is not None:
            headers.update({'Content-Type': 'application/json', 'Origin': f'http://127.0.0.1:{self.port}'})
            if csrf:
                headers['X-CSRF-Token'] = self.session['csrf_token']
        conn.request('GET' if body is None else 'POST', path,
                     None if body is None else json.dumps(body), headers)
        response = conn.getresponse()
        status, raw = response.status, response.read()
        conn.close()
        return status, raw

    def test_read_requires_session_and_registration_requires_csrf(self):
        self.assertEqual(self.request('/api/mediflow/health', auth=False)[0], 401)
        self.assertEqual(self.request('/api/mediflow/datasets/register', {'manifest_id': 'frozen'}, csrf=False)[0], 403)
        self.bridge.dispatch.assert_not_called()
        self.assertEqual(self.request('/api/mediflow/health')[0], 200)
        self.assertEqual(self.request('/api/mediflow/datasets/register', {'manifest_id': 'frozen'})[0], 201)

    def test_server_rejects_all_execution_paths(self):
        for path in ['/api/training/runs', '/api/training/preflight', '/api/runs',
                     '/api/runs/train-123456789abc/resume', '/api/demo/reset',
                     '/api/huggingface/download', '/api/datasets/frozen/split']:
            self.assertEqual(self.request(path, {})[0], 409, path)
        self.bridge.dispatch.assert_not_called()
        self.assertIsNone(self.server.training)
        self.server.start_worker()
        self.assertIsNone(self.server.worker_thread)

    def test_snapshot_cannot_mix_existing_synthetic_runs_or_gpu(self):
        status, raw = self.request('/api/status')
        self.assertEqual(status, 200)
        value = json.loads(raw)
        self.assertEqual(value['mode'], 'mediflow')
        self.assertEqual(value['runs'], [])
        self.assertIsNone(value['gpu']['utilization'])
        self.assertFalse(value['gpu']['simulated'])
        self.assertFalse(value['capabilities']['real_training'])

    def test_path_and_query_guards_before_dispatch(self):
        for path in ['/api/mediflow/runs/%2e%2e/steps', '/api/mediflow/health?path=/etc/passwd',
                     '/api/mediflow/runs?limit=1&limit=2']:
            self.assertEqual(self.request(path)[0], 400)
        self.bridge.dispatch.assert_not_called()

    def test_csv_export_quotes_formula_strings(self):
        self.bridge.dispatch.return_value = {'metrics': [{'label': '=HYPERLINK("private")', 'loss': 1.2}]}
        status, raw = self.request('/api/mediflow/runs/mf-' + 'a' * 24 + '/export?format=csv')
        self.assertEqual(status, 200)
        self.assertIn(b"'=HYPERLINK", raw)
        self.assertIn(b'1.2', raw)

    def test_close_only_closes_bridge_not_external_processes(self):
        self.server.server_close()
        self.bridge.close.assert_called_once_with()
        self.assertEqual([call[0] for call in self.bridge.mock_calls], ['close'])


class MediflowCLITests(unittest.TestCase):
    def test_incompatible_training_rejected_before_construction(self):
        with patch('server.TrainingManager') as trainer, patch('server.AuthStore') as auth:
            with self.assertRaises(SystemExit):
                main(['--backend', 'mediflow', '--mediflow-root', '/tmp/example', '--enable-training'])
            trainer.assert_not_called()
            auth.assert_not_called()


if __name__ == '__main__':
    unittest.main()
