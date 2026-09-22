"""Check native provenance coverage and literal handling of trusted CLI paths."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from native_identity import native_hashes, source_head
from replay import run_json, trusted_directory, trusted_interpreter


class NativeIdentityTests(unittest.TestCase):
    def test_platform_libraries_are_hashed_and_content_changes_are_visible(self):
        layouts = (
            ('_hgraph.abi3.so', ['lib/libhgraph_runtime.dylib', 'lib/libhgraph_wiring.dylib']),
            ('_hgraph.abi3.so', ['lib/libhgraph_runtime.so', 'lib/libhgraph_runtime.so.1.2',
                                 'lib/libhgraph_stdlib.so']),
            ('_hgraph.pyd', ['hgraph_runtime.dll', 'hgraph_wiring.dll', 'bin/hgraph_stdlib.dll']),
        )
        for extension_name, libraries in layouts:
            with self.subTest(extension=extension_name, libraries=libraries), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                for name in [extension_name, *libraries, 'lib/unrelated.so', 'lib/hgraph_runtime.lib']:
                    path = root / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(name.encode())
                before = native_hashes(root / extension_name)
                self.assertEqual(set(before), {extension_name, *libraries})
                for name in libraries:
                    (root / name).write_bytes(b'new native build')
                    after = native_hashes(root / extension_name)
                    self.assertNotEqual(before[name], after[name])
                    self.assertEqual(after[name], hashlib.sha256(b'new native build').hexdigest())


class LauncherTests(unittest.TestCase):
    def test_missing_cli_paths_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            missing = str(Path(temp) / 'absent')
            for validator in (trusted_directory, trusted_interpreter):
                with self.assertRaises(argparse.ArgumentTypeError):
                    validator(missing)
            with self.assertRaises(argparse.ArgumentTypeError):
                trusted_interpreter(temp)

    def test_shell_characters_remain_literal_and_interpreter_symlink_is_preserved(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            directory = root / '--path with spaces; $(echo injected)'
            directory.mkdir()
            interpreter = directory / 'python'
            try:
                interpreter.symlink_to(sys.executable)
            except OSError:
                self.skipTest('Interpreter symlinks unavailable')
            selected = trusted_interpreter(str(interpreter))
            self.assertEqual(selected, interpreter)
            self.assertNotEqual(selected, interpreter.resolve())
            script = directory / '--probe; echo injected.py'
            script.write_text('import json, sys\nprint(json.dumps(sys.argv))\n')
            self.assertEqual(run_json(selected, script), [str(script)])
            subprocess.run(['git', 'init', '--quiet'], cwd=directory, check=True)
            subprocess.run(['git', '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid',
                            '-c', 'commit.gpgsign=false', 'commit', '--quiet', '--allow-empty', '-m', 'test'],
                           cwd=directory, check=True)
            self.assertRegex(source_head(trusted_directory(str(directory))), r'^[0-9a-f]{40}$')
            self.assertEqual(sorted(p.name for p in directory.iterdir()), ['--probe; echo injected.py', '.git', 'python'])


if __name__ == '__main__':
    unittest.main()
