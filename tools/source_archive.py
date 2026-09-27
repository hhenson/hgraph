"""Export HEAD and pinned shared inputs to dist/hgraph-source.tar.gz."""
import argparse
from contextlib import ExitStack
import io
import os
from pathlib import Path, PurePosixPath
import posixpath
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def relative_name(name: str) -> PurePosixPath:
    """Accept portable relative member names, without traversal or drive syntax."""
    path = PurePosixPath(name)
    if (not name or path.is_absolute() or not path.parts
            or any(part in ('.', '..') for part in name.rstrip('/').split('/'))
            or '\\' in name or ':' in name or '\x00' in name):
        raise ValueError(f'unsafe archive path: {name!r}')
    return path


def archive_name(name: str) -> PurePosixPath:
    path = relative_name(name)
    if path.parts[0] != 'hgraph':
        raise ValueError(f'archive entry is outside hgraph/: {name!r}')
    return path


def safe_member(member: tarfile.TarInfo) -> tarfile.TarInfo:
    path = archive_name(member.name)
    if member.type not in (tarfile.REGTYPE, tarfile.AREGTYPE, tarfile.DIRTYPE, tarfile.SYMTYPE):
        raise ValueError(f'unsupported archive member: {member.name!r}')
    # Rebuild the header so PAX path/link overrides and ownership do not survive.
    safe = tarfile.TarInfo(path.as_posix())
    safe.type = member.type
    safe.mode = member.mode & 0o777
    safe.mtime = member.mtime
    safe.size = member.size if member.isfile() else 0
    if member.issym():
        link = member.linkname
        if not link or PurePosixPath(link).is_absolute() or any(c in link for c in ('\\', ':', '\x00')):
            raise ValueError(f'unsafe symbolic link: {member.name!r}')
        target = archive_name(posixpath.normpath(str(path.parent / link)))
        safe.linkname = posixpath.relpath(str(target), str(path.parent))
    return safe


def write_archive(root: Path, archives: list[bytes]) -> Path:
    root = root.resolve()
    directory = root / 'dist'
    if directory.is_symlink() or directory.resolve() != directory:
        raise ValueError('dist must be a directory inside the checkout')
    output = directory / 'hgraph-source.tar.gz'
    with ExitStack() as stack:
        entries, names, leaves = [], {}, set()
        for archive in archives:
            source = stack.enter_context(tarfile.open(fileobj=io.BytesIO(archive)))
            for member in source:
                safe = safe_member(member)
                if safe.name in names:
                    if safe.isdir() and names[safe.name]:
                        continue  # Gitlinks and their package archives share directory headers.
                    raise ValueError(f'duplicate archive entry: {safe.name}')
                names[safe.name] = safe.isdir()
                if not safe.isdir():
                    leaves.add(safe.name)
                entries.append((source, member, safe))
        for name in names:
            if any(str(parent) in leaves for parent in PurePosixPath(name).parents):
                raise ValueError(f'archive entry has a file or link parent: {name!r}')
        directory.mkdir(exist_ok=True)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=directory, prefix='.hgraph-source-', delete=False) as stream:
                temporary = Path(stream.name)
                with tarfile.open(fileobj=stream, mode='w:gz') as target:
                    for source, original, safe in entries:
                        target.addfile(safe, source.extractfile(original) if original.isfile() else None)
            os.replace(temporary, output)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    return output


def main() -> None:
    argparse.ArgumentParser(description=__doc__).parse_args()
    subprocess.run([sys.executable, str(ROOT / 'tools/shared_artifacts.py'), '--check'], check=True)
    # Fixed package paths and prefixes are not CLI or manifest inputs.
    packages = ('', 'external/hgraph_spec', 'external/hgraph_std',
                'external/hgraph_spec_audit', 'external/hgraph_spec_audit/spec',
                'external/hgraph_spec_audit/stdlib')
    archives = []
    for package in packages:
        checkout = ROOT / package
        subprocess.run(['git', 'diff', '--quiet', 'HEAD', '--'], cwd=checkout, check=True)
        prefix = 'hgraph/' + (package + '/' if package else '')
        archives.append(subprocess.check_output(
            ['git', 'archive', '--format=tar', '--prefix=' + prefix, 'HEAD'], cwd=checkout))
    print(write_archive(ROOT, archives))


if __name__ == '__main__':
    main()
