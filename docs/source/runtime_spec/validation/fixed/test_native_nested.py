"""Keep native wiring evidence complete and separate from runtime voting."""
import copy
import json
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import native_nested
from native_nested_build import headers_digest


class NativeWiringEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.recorded = json.loads((native_nested.ROOT / 'native_nested_observed.json').read_text())
        self.bridge = json.loads((native_nested.ROOT / 'observed.json').read_text())

    def test_recorded_evidence_verifies(self):
        native_nested.verify(self.recorded, self.bridge)

    def test_missing_case_tick_or_changed_state_is_rejected(self):
        name = sorted(native_nested.CASES)[0]
        for mutate in (lambda record: record['cases'].pop(name),
                       lambda record: record['cases'][name]['ticks'].pop(),
                       lambda record: record['cases'][name]['ticks'][7].update(last=99),
                       lambda record: record['provenance'].update(stable=False),
                       lambda record: record['provenance']['replay_digests'].pop()):
            record = copy.deepcopy(self.recorded)
            mutate(record)
            with self.assertRaises(AssertionError):
                native_nested.verify(record, self.bridge)

    def test_source_and_candidate_drift_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'native_nested_probe.cpp').write_text('changed source')
            with patch.object(native_nested, 'ROOT', root), self.assertRaisesRegex(AssertionError, 'source changed'):
                native_nested.verify(self.recorded, self.bridge)
        bridge = copy.deepcopy(self.bridge)
        bridge['provenance']['candidate_native_sha256']['runtime'] = 'changed'
        with self.assertRaisesRegex(AssertionError, 'candidate differs'):
            native_nested.verify(self.recorded, bridge)

    def test_every_descendant_reset_is_assessed_without_changing_runtime_votes(self):
        before = copy.deepcopy(self.bridge)
        report = native_nested.assess(self.recorded, self.bridge)
        self.assertEqual(self.bridge, before)
        self.assertGreater(report['counts']['variation'], 0)
        name = 'tsl_tsl_owned_invalidate'
        for child in ('', '/children/0', '/children/1', '/children/0/children/0',
                      '/children/0/children/1', '/children/1/children/0', '/children/1/children/1'):
            for tick in (7, 8):
                changed = copy.deepcopy(self.recorded)
                path = f'/ticks/{tick}{child}'
                native_nested.at(changed['cases'][name], path)['last'] = 123
                result = native_nested.assess(changed, self.bridge)
                finding = next(item for item in result['differences'] if item['case'] == name and item['path'] == path + '/last')
                self.assertEqual(finding['expected'], 'never')
                self.assertEqual(finding['native_cpp'], 123)
                self.assertEqual(finding['status'], 'variation')

    def test_stale_build_source_or_headers_are_rejected(self):
        build = native_nested.expected_build(self.bridge)
        for field in build:
            with self.subTest(field=field), self.assertRaisesRegex(AssertionError, 'different source or SDK'):
                native_nested.verify_run({'build': {**build, field: 'stale'}, 'libraries': []}, self.bridge)

    def test_actual_loaded_libraries_must_match_and_paths_are_not_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root / ('libhgraph_' + role + '.so') for role in ('runtime', 'wiring', 'stdlib')]
            bridge = copy.deepcopy(self.bridge)
            expected = {}
            for path in paths:
                path.write_bytes(path.name.encode())
                expected['lib/' + path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
            bridge['provenance']['candidate_native_sha256'] = expected
            payload = {'build': native_nested.expected_build(bridge), 'libraries': list(map(str, paths))}
            identity = native_nested.verify_run(payload, bridge)
            self.assertNotIn(str(root), str(identity))
            paths[0].write_bytes(b'another SDK or loader override')
            with self.assertRaisesRegex(AssertionError, 'Loaded native library differs'):
                native_nested.verify_run(payload, bridge)
            payload['libraries'] = list(map(str, paths[1:]))
            with self.assertRaisesRegex(AssertionError, 'Missing loaded hgraph runtime'):
                native_nested.verify_run(payload, bridge)

    def test_build_hashes_actual_core_headers_and_ignores_unrelated_extensions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'core.h').write_bytes(b'core')
            names = {'include/core.h': 'unused recorded hash'}
            before = headers_digest(root, names)
            (root / 'extension.h').write_bytes(b'extension')
            self.assertEqual(before, headers_digest(root, names))
            (root / 'core.h').write_bytes(b'older SDK')
            self.assertNotEqual(before, headers_digest(root, names))

    def test_normal_replay_rejects_another_executable_before_running_it(self):
        with tempfile.TemporaryDirectory() as directory:
            executable = Path(directory) / 'probe'
            executable.write_bytes(b'another build')
            with patch('sys.argv', ['native_nested.py', '--executable', str(executable)]), \
                 patch.object(native_nested.subprocess, 'check_output') as run:
                with self.assertRaisesRegex(AssertionError, 'Native executable differs'):
                    native_nested.main()
                run.assert_not_called()


if __name__ == '__main__':
    unittest.main()
