"""Identify installed native artifacts in the selected candidate environment."""
import hashlib
import json
from pathlib import Path
import subprocess


def is_native_library(name):
    name = name.lower()
    if not name.startswith(('hgraph', 'libhgraph')):
        return False
    if name.endswith(('.dylib', '.dll', '.so')):
        return True
    _, separator, version = name.partition('.so.')
    return bool(separator) and all(part.isascii() and part.isdecimal()
                                   for part in version.split('.'))


def native_hashes(extension):
    root = extension.parent
    files = {extension}
    for directory in (root, root / 'lib', root / 'bin', root / 'hgraph.libs'):
        if directory.is_dir():
            files.update(p for p in directory.iterdir() if p.is_file() and is_native_library(p.name))
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(files)}


def source_head(directory):
    result = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=directory.absolute(),
                            capture_output=True, text=True, shell=False, timeout=30)
    return result.stdout.strip() if result.returncode == 0 else None


def main():
    import _hgraph
    import hgraph
    print(json.dumps({'candidate_source_head': source_head(Path(hgraph.__file__).parent),
                      'candidate_native_sha256': native_hashes(Path(_hgraph.__file__))}))


if __name__ == '__main__':
    main()
