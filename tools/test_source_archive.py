"""The source bundle cannot redirect writes or publish escaping archive entries."""
import io
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from source_archive import write_archive


def archive(*members):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode='w') as target:
        for member in members:
            data = b'payload' if member.isfile() else b''
            member.size = len(data)
            target.addfile(member, io.BytesIO(data) if member.isfile() else None)
    return buffer.getvalue()


class SourceArchive(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        (self.root / 'dist').mkdir()
        self.output = self.root / 'dist/hgraph-source.tar.gz'
        self.output.write_bytes(b'previous bundle')

    def reject(self, data, *additional):
        with self.assertRaises(ValueError):
            write_archive(self.root, [data, *additional])
        self.assertEqual(self.output.read_bytes(), b'previous bundle')

    def test_member_paths_cannot_escape_on_posix_or_windows(self):
        for name in ('../escape', '/escape', 'other/file', 'hgraph/../escape',
                     'hgraph/C:/escape', 'hgraph/..\\escape'):
            with self.subTest(name=name):
                self.reject(archive(tarfile.TarInfo(name)))

    def test_link_targets_and_special_members_are_rejected(self):
        for target in ('../../escape', '/escape', 'C:/escape', '..\\escape'):
            with self.subTest(target=target):
                member = tarfile.TarInfo('hgraph/link')
                member.type, member.linkname = tarfile.SYMTYPE, target
                self.reject(archive(member))
        for kind in (tarfile.LNKTYPE, tarfile.CHRTYPE, tarfile.FIFOTYPE):
            with self.subTest(kind=kind):
                member = tarfile.TarInfo('hgraph/special')
                member.type, member.linkname = kind, '../../escape'
                self.reject(archive(member))

    def test_pax_path_override_is_validated(self):
        member = tarfile.TarInfo('hgraph/safe')
        member.pax_headers = {'path': '../escape'}
        self.reject(archive(member))

    def test_entries_cannot_be_nested_under_an_archive_link(self):
        link = tarfile.TarInfo('hgraph/alias')
        link.type, link.linkname = tarfile.SYMTYPE, '.'
        nested = tarfile.TarInfo('hgraph/alias/file')
        for members in ((link, nested), (nested, link)):
            self.reject(archive(*members))

    def test_dependency_archives_receive_the_same_validation(self):
        self.reject(archive(), archive(tarfile.TarInfo('hgraph/external/../../escape')))
        link = tarfile.TarInfo('hgraph/external/link')
        link.type, link.linkname = tarfile.SYMTYPE, '../../../escape'
        self.reject(archive(), archive(link))

    def test_duplicate_package_directories_are_merged(self):
        directory = tarfile.TarInfo('hgraph/external/package')
        directory.type = tarfile.DIRTYPE
        member = tarfile.TarInfo('hgraph/external/package/file.hgl')
        output = write_archive(self.root, [archive(directory), archive(directory, member)])
        with tarfile.open(output) as result:
            self.assertEqual(result.getnames().count(directory.name), 1)
            self.assertEqual(result.extractfile(member.name).read(), b'payload')

    def test_symlinked_output_directory_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'dist').symlink_to(self.output.parent, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, 'dist must be'):
                write_archive(root, [archive()])
        self.assertEqual(self.output.read_bytes(), b'previous bundle')

    def test_output_symlink_is_replaced_without_following_it(self):
        victim = self.root / 'keep'
        victim.write_bytes(b'untouched')
        self.output.unlink()
        self.output.symlink_to(victim)
        write_archive(self.root, [archive()])
        self.assertFalse(self.output.is_symlink())
        self.assertEqual(victim.read_bytes(), b'untouched')

    def test_write_failure_preserves_previous_bundle_and_cleans_temporary_file(self):
        with patch('source_archive.os.replace', side_effect=OSError('publication failed')):
            with self.assertRaises(OSError):
                write_archive(self.root, [archive()])
        self.assertEqual(self.output.read_bytes(), b'previous bundle')
        self.assertEqual(list(self.output.parent.iterdir()), [self.output])

    def test_duplicate_entries_are_rejected(self):
        member = tarfile.TarInfo('hgraph/source')
        self.reject(archive(member, member))
        self.reject(archive(member), archive(member))

    def test_regular_files_internal_links_and_shared_inputs_survive(self):
        regular = tarfile.TarInfo('hgraph/Formula/package.rb')
        regular.mode = 0o755
        link = tarfile.TarInfo('hgraph/Aliases/package')
        link.type, link.linkname = tarfile.SYMTYPE, '../Formula/package.rb'
        shared = tarfile.TarInfo('hgraph/external/hgraph_std/shared.hgl')
        output = write_archive(self.root, [archive(regular, link), archive(shared)])
        self.assertEqual(output, self.output)
        with tarfile.open(output) as result:
            self.assertEqual(result.extractfile('hgraph/Formula/package.rb').read(), b'payload')
            self.assertEqual(result.getmember('hgraph/Formula/package.rb').mode, 0o755)
            self.assertEqual(result.getmember('hgraph/Aliases/package').linkname, '../Formula/package.rb')
            self.assertEqual(result.extractfile(shared.name).read(), b'payload')

    def test_cli_does_not_accept_output_or_prefix_arguments(self):
        script = Path(__file__).with_name('source_archive.py')
        for option in ('--prefix=--exec=anything', '--output=../escape'):
            result = subprocess.run([sys.executable, str(script), option], capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            self.assertIn('unrecognized arguments', result.stderr)


if __name__ == '__main__':
    unittest.main()
