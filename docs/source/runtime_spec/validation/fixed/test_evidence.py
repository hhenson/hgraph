"""Supplement integrity and all-or-nothing publication of recorded evidence."""
import copy
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import evidence
from check import verify_native_probe


class NativeProbeEvidenceTests(unittest.TestCase):
    def test_absent_supplement_needs_no_native_files(self):
        with tempfile.TemporaryDirectory() as directory:
            verify_native_probe({}, Path(directory))

    def test_valid_supplement_and_changed_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            contents = {'native_probe.cpp': b'probe source', 'native_observed.txt': b'probe output'}
            for name, content in contents.items():
                (root / name).write_bytes(content)
            provenance = {'native_probe': {'source_sha256': hashlib.sha256(contents['native_probe.cpp']).hexdigest(),
                                          'stable': True, 'replay_digests': [hashlib.sha256(contents['native_observed.txt']).hexdigest()] * 3}}
            verify_native_probe(provenance, root)
            for name, content in contents.items():
                with self.subTest(name=name):
                    (root / name).write_bytes(content + b'corrupt')
                    with self.assertRaises(AssertionError):
                        verify_native_probe(provenance, root)
                    (root / name).write_bytes(content)

    def test_stability_metadata_requires_three_matching_runs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'native_probe.cpp').write_bytes(b'source')
            (root / 'native_observed.txt').write_bytes(b'output')
            digest = hashlib.sha256(b'output').hexdigest()
            original = {'source_sha256': hashlib.sha256(b'source').hexdigest(), 'stable': True,
                        'replay_digests': [digest] * 3}
            for changes in ({'stable': False}, {'stable': 1}, {'replay_digests': [digest] * 2},
                            {'replay_digests': [digest, digest, 'wrong']}):
                with self.subTest(changes=changes), self.assertRaises(AssertionError):
                    verify_native_probe({'native_probe': {**copy.deepcopy(original), **changes}}, root)


class AtomicPublicationTests(unittest.TestCase):
    def test_success_replaces_complete_file_and_removes_temporary(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'observed.json'
            output.write_text('previous')
            evidence.atomic_write(output, 'complete new evidence\n')
            self.assertEqual(output.read_text(), 'complete new evidence\n')
            self.assertEqual(list(output.parent.iterdir()), [output])

    def test_partial_write_failure_preserves_previous_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'observed.json'
            output.write_text('previous')
            real_factory = tempfile.NamedTemporaryFile
            def failing_factory(**kwargs):
                stream = real_factory(**kwargs)
                write = stream.write
                def partial(content):
                    write(content[:3])
                    raise OSError('disk full')
                stream.write = partial
                return stream
            with patch.object(evidence.tempfile, 'NamedTemporaryFile', side_effect=failing_factory):
                with self.assertRaisesRegex(OSError, 'disk full'):
                    evidence.atomic_write(output, 'complete new evidence')
            self.assertEqual(output.read_text(), 'previous')
            self.assertEqual(list(output.parent.iterdir()), [output])

    def test_replace_failure_preserves_previous_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'observed.json'
            output.write_text('previous')
            def failure(source, target):
                self.assertEqual(source.parent, target.parent)
                self.assertEqual(source.read_text(), 'complete new evidence')
                self.assertEqual(target.read_text(), 'previous')
                raise OSError('replace failed')
            with patch.object(evidence.os, 'replace', side_effect=failure):
                with self.assertRaisesRegex(OSError, 'replace failed'):
                    evidence.atomic_write(output, 'complete new evidence')
            self.assertEqual(output.read_text(), 'previous')
            self.assertEqual(list(output.parent.iterdir()), [output])


if __name__ == '__main__':
    unittest.main()
