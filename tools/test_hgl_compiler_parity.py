"""The parity gate must reject vacuous, partial, and mutually failing runs."""
import tempfile
from pathlib import Path
import unittest

from hgl_compiler_parity import assess, assertion_counts, discover, snapshot


class CompilerParity(unittest.TestCase):
    def test_complete_cpp_and_rust_results_have_the_same_test_identities(self):
        cpp = assess('first ... ok\nsecond ... ok\n2 tests, 0 failed\n', 0, 'hgraph.std', ['first', 'second'])
        rust = assess('hgraph.std::first ... ok\nhgraph.std::second ... ok\n', 0, 'hgraph.std', ['first', 'second'])
        self.assertTrue(cpp['passed'])
        self.assertEqual(cpp, rust)

    def test_assertion_reporting_retains_exact_multiplicity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'test.hgl'
            path.write_text('module hgraph.std\ntest several {\n'
                            'assert eval(f, x: [1]) == [1]\n'
                            'assert "test fake { assert }" == "test fake { assert }"\n'
                            '# assert fake\neval(sink, x: [1])\n}\n')
            expected = assertion_counts([path])
            self.assertEqual(dict(expected), {'hgraph.std::several': 3})
            output = 'hgraph.std::several ... ok\n'
            self.assertTrue(assess(output * 3, 0, 'shared', expected)['passed'])
            self.assertFalse(assess(output * 2, 0, 'shared', expected)['passed'])
            self.assertFalse(assess(output * 4, 0, 'shared', expected)['passed'])

    def test_success_status_alone_is_not_evidence(self):
        for output in ['', 'first ... ok\n', 'first ... ok\nfirst ... ok\nsecond ... ok\n',
                       'first ... ok\nsecond ... FAILED\n', 'other.module::first ... ok\nsecond ... ok\n']:
            with self.subTest(output=output):
                self.assertFalse(assess(output, 0, 'hgraph.std', ['first', 'second'])['passed'])
        self.assertFalse(assess('first ... ok\n', 1, 'hgraph.std', ['first'])['passed'])

    def test_new_test_parts_are_discovered_without_an_allowlist(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'tests').mkdir()
            (root / 'standard.hgl').write_text('module hgraph.std\n')
            (root / 'tests/first.hgl').write_text('module hgraph.std part first\ntest {\n test first { }\n}\n')
            self.assertEqual(discover(root)[1], {'hgraph.std': ['first']})
            (root / 'tests/new.hgl').write_text('module hgraph.std part new\ntest new_case { }\n')
            self.assertEqual(discover(root)[1], {'hgraph.std': ['first', 'new_case']})
            (root / 'tests/new.hgl').write_text('module hgraph.std part new\ntest first { }\n')
            with self.assertRaisesRegex(ValueError, 'duplicate shared tests'):
                discover(root)

    def test_new_modules_and_empty_corpora_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, 'empty'):
                discover(root)
            (root / 'new.hgl').write_text('module new.module\ntest one { }\n')
            with self.assertRaisesRegex(ValueError, 'new shared modules'):
                discover(root)

    def test_working_contents_are_fingerprinted_even_when_head_is_unchanged(self):
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                subprocess.run(['git', '-C', directory, *args], check=True, capture_output=True)
            git('init')
            (root / 'spec.md').write_text('old rule')
            git('add', 'spec.md')
            git('-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'fixture')
            old = snapshot(root)
            (root / 'spec.md').write_text('new rule')
            git('add', 'spec.md')
            new = snapshot(root)
            self.assertEqual(old['revision'], new['revision'])
            self.assertNotEqual(old['files'], new['files'])
            self.assertTrue(new['dirty'])
            (root / 'new.md').write_text('untracked extension')
            self.assertIn('new.md', snapshot(root)['files'])


if __name__ == '__main__':
    unittest.main()
