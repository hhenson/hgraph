"""Keep native wiring evidence complete and separate from runtime voting."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import native_nested


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


if __name__ == '__main__':
    unittest.main()
