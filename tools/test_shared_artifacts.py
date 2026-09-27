"""Retiring compatibility copies must preserve edited and tracked files."""
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from shared_artifacts import retire_copies


class Retirement(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        subprocess.run(['git', 'init', '-q', str(self.root)], check=True)
        self.copy = self.root / 'language/docs/shared.md'
        self.copy.parent.mkdir(parents=True)
        self.copy.write_bytes(b'original')
        self.state = self.root / '.shared-artifacts-state.json'
        self.state.write_text(json.dumps({self.copy.relative_to(self.root).as_posix():
                                         hashlib.sha256(b'original').hexdigest()}))

    def test_unchanged_copy_is_removed_and_second_setup_creates_nothing(self):
        retire_copies(self.root)
        self.assertFalse(self.copy.exists())
        self.assertFalse(self.state.exists())
        retire_copies(self.root)
        self.assertFalse((self.root / 'language').exists())

    def test_check_is_read_only(self):
        with self.assertRaises(ValueError):
            retire_copies(self.root, check=True)
        self.assertEqual(self.copy.read_bytes(), b'original')

    def test_edited_copy_is_preserved(self):
        self.copy.write_bytes(b'edited')
        with self.assertRaisesRegex(ValueError, 'was edited'):
            retire_copies(self.root)
        self.assertEqual(self.copy.read_bytes(), b'edited')
        self.assertTrue(self.state.exists())

    def test_tracked_file_is_preserved(self):
        subprocess.run(['git', 'add', 'language'], cwd=self.root, check=True)
        with self.assertRaisesRegex(ValueError, 'unsafe legacy'):
            retire_copies(self.root)
        self.assertTrue(self.copy.exists())

    def test_noncanonical_tracked_path_is_rejected(self):
        data = json.loads(self.state.read_text())
        data['language/docs/./shared.md'] = data.pop('language/docs/shared.md')
        self.state.write_text(json.dumps(data))
        subprocess.run(['git', 'add', 'language'], cwd=self.root, check=True)
        with self.assertRaises(ValueError):
            retire_copies(self.root)
        self.assertTrue(self.copy.exists())

    def test_symlink_is_preserved(self):
        self.copy.unlink()
        self.copy.symlink_to(self.state)
        with self.assertRaises(ValueError):
            retire_copies(self.root)
        self.assertTrue(self.copy.is_symlink())

    def test_traversal_is_rejected_before_any_removal(self):
        data = json.loads(self.state.read_text())
        data['language/docs/../../keep'] = 'unused'
        self.state.write_text(json.dumps(data))
        with self.assertRaises(ValueError):
            retire_copies(self.root)
        self.assertTrue(self.copy.exists())


if __name__ == '__main__':
    unittest.main()
