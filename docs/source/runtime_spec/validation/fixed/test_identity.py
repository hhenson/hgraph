"""Reject mislabeled harness trees and distinguish patched reference installs."""
import hashlib
from pathlib import Path, PurePosixPath
import tempfile
from types import SimpleNamespace
import unittest

from harness_identity import git, verified_harness
from reference_identity import reference_identity


class HarnessIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / 'source'
        self.source.mkdir()
        git(self.source, 'init', '--quiet')
        (self.source / '.gitignore').write_text('ignored.py\n__pycache__/\n')
        (self.source / 'runner.py').write_text('value = 1\n')
        git(self.source, 'add', '.')
        git(self.source, '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid',
            '-c', 'commit.gpgsign=false', 'commit', '--quiet', '-m', 'base')
        self.base = git(self.source, 'rev-parse', 'HEAD').decode().strip()
        (self.source / 'runner.py').write_text('value = 2\n')
        (self.source / 'observer.py').write_text('value = 3\n')
        git(self.source, 'add', '.')
        self.adapter = self.root / 'adapter.patch'
        self.adapter.write_bytes(git(self.source, 'diff', '--cached', '--binary', 'HEAD'))

    def test_verified_copy_has_only_expected_code_and_leaves_source_index_unchanged(self):
        before = (self.source / '.git/index').read_bytes()
        (self.source / 'ignored.py').write_text('unexpected code\n')
        with verified_harness(self.source, self.adapter, self.base) as (snapshot, identity):
            self.assertEqual((snapshot / 'runner.py').read_text(), 'value = 2\n')
            self.assertEqual((snapshot / 'observer.py').read_text(), 'value = 3\n')
            self.assertFalse((snapshot / 'ignored.py').exists())
            (self.source / 'runner.py').write_text('changed during replay\n')
            self.assertEqual((snapshot / 'runner.py').read_text(), 'value = 2\n')
            self.assertEqual(identity['harness_base'], self.base)
            self.assertEqual(identity['adapter_sha256'], hashlib.sha256(self.adapter.read_bytes()).hexdigest())
        self.assertEqual((self.source / '.git/index').read_bytes(), before)
        self.assertFalse(snapshot.exists())

    def test_wrong_base_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'HEAD differs'):
            with verified_harness(self.source, self.adapter, '0' * 40):
                self.fail('Wrong base accepted')

    def test_unapplied_adapter_is_rejected(self):
        git(self.source, 'reset', '--hard', self.base)
        with self.assertRaisesRegex(ValueError, 'files differ'):
            with verified_harness(self.source, self.adapter, self.base):
                self.fail('Unapplied adapter accepted')

    def test_runner_change_is_rejected(self):
        (self.source / 'runner.py').write_text('different runner\n')
        with self.assertRaisesRegex(ValueError, 'content differs'):
            with verified_harness(self.source, self.adapter, self.base):
                self.fail('Modified runner accepted')

    def test_extra_untracked_source_is_rejected(self):
        (self.source / 'extra.py').write_text('extra observer\n')
        with self.assertRaisesRegex(ValueError, 'files differ'):
            with verified_harness(self.source, self.adapter, self.base):
                self.fail('Extra source accepted')


class ReferenceIdentityTests(unittest.TestCase):
    def test_editable_sources_and_repackaged_artifacts_change_identity_without_version_change(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / 'editable-source/hgraph'
            source.mkdir(parents=True)
            (source / '__init__.py').write_text('value = 1\n')
            package = SimpleNamespace(__file__=str(source / '__init__.py'))
            artifact = root / 'hgraph.dist-info/METADATA'
            artifact.parent.mkdir()
            artifact.write_text('Version: 0.5.41\n')
            distribution = SimpleNamespace(version='0.5.41', files=[PurePosixPath('hgraph.dist-info/METADATA')],
                                           locate_file=lambda entry: root / entry)
            original = reference_identity(package, distribution)
            (source / '__init__.py').write_text('value = 2\n')
            patched = reference_identity(package, distribution)
            self.assertEqual(original['version'], patched['version'])
            self.assertNotEqual(original['identity_sha256'], patched['identity_sha256'])
            (source / 'new_module.py').write_text('value = 3\n')
            added = reference_identity(package, distribution)
            self.assertNotEqual(patched['identity_sha256'], added['identity_sha256'])
            artifact.write_text('Version: 0.5.41\nRepackaged: yes\n')
            repackaged = reference_identity(package, distribution)
            self.assertNotEqual(added['identity_sha256'], repackaged['identity_sha256'])
            self.assertNotIn(str(root), str(repackaged))
            artifact.unlink()
            with self.assertRaisesRegex(ValueError, 'Missing reference artifact'):
                reference_identity(package, distribution)


if __name__ == '__main__':
    unittest.main()
