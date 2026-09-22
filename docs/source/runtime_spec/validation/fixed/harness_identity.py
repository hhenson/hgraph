"""Verify base plus adapter, then replay from an isolated copy of that tree."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

BASE = '15e7bf41b2b17145f6e3f2742f08ec2826b30a71'


def git(directory, *arguments):
    return subprocess.check_output(['git', *arguments], cwd=directory, timeout=60)


def file_identity(path):
    if path.is_symlink():
        return ['120000', str(path.readlink())]
    if not path.is_file():
        raise ValueError('Missing harness file: ' + path.name)
    mode = '100755' if path.stat().st_mode & 0o111 else '100644'
    return [mode, hashlib.sha256(path.read_bytes()).hexdigest()]


@contextmanager
def verified_harness(source, adapter, base=BASE):
    source = source.resolve()
    adapter = adapter.resolve()
    if git(source, 'rev-parse', 'HEAD').decode().strip() != base:
        raise ValueError('Harness HEAD differs from the required base')
    with tempfile.TemporaryDirectory(prefix='fixed-harness-') as temporary:
        snapshot = Path(temporary) / 'tree'
        git(Path(temporary), 'clone', '--quiet', '--shared', '--no-checkout', '--', str(source), str(snapshot))
        git(snapshot, 'checkout', '--quiet', '--detach', base)
        git(snapshot, 'apply', '--index', str(adapter))
        names = set(git(snapshot, 'ls-files', '-z').decode().split('\0')) - {''}
        actual = set(git(source, 'ls-files', '--cached', '--others', '--exclude-standard', '-z').decode().split('\0')) - {''}
        if names != actual:
            raise ValueError('Harness files differ from base plus adapter')
        manifest = {}
        for name in sorted(names):
            expected = file_identity(snapshot / name)
            if file_identity(source / name) != expected:
                raise ValueError('Harness content differs from base plus adapter: ' + name)
            manifest[name] = expected
        identity = {
            'harness_base': base,
            'harness_tree': git(snapshot, 'write-tree').decode().strip(),
            'harness_files_sha256': hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest(),
            'adapter_sha256': hashlib.sha256(adapter.read_bytes()).hexdigest(),
        }
        yield snapshot, identity
