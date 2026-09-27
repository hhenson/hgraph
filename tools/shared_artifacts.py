"""Initialize pinned shared repositories and retire unchanged compatibility copies."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import subprocess

ROOT = Path(__file__).resolve().parents[1]
PACKAGES = ('hgraph_spec', 'hgraph_std', 'hgraph_spec_audit')
LEGACY_ROOTS = ('docs/source/runtime_spec/', 'docs/source/specification/',
                'language/docs/', 'language/examples/', 'language/stdlib/', 'language/tests/')


def retire_copies(root: Path, check: bool = False) -> None:
    state = root / '.shared-artifacts-state.json'
    if state.is_symlink():
        raise ValueError('legacy state must not be a symbolic link')
    if not state.exists():
        return
    tracked = set(subprocess.check_output(
        ['git', 'ls-files', '-z'], cwd=root).decode().split('\0'))
    pending = []
    for name, digest in json.loads(state.read_text()).items():
        relative = PurePosixPath(name)
        if (relative.is_absolute() or any(part in ('', '.', '..') for part in name.split('/'))
                or '\\' in name or ':' in name
                or not name.startswith(LEGACY_ROOTS) or name in tracked):
            raise ValueError(f'unsafe legacy copy: {name}')
        path = root / name
        if not path.exists() and not path.is_symlink():
            continue
        if (path.is_symlink() or not path.resolve().is_relative_to(root.resolve())
                or not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != digest):
            raise ValueError(f'legacy copy was edited; preserve or migrate it before cleanup: {name}')
        pending.append(path)
    if check:
        raise ValueError('legacy compatibility state remains; run tools/shared_artifacts.py --offline')
    for path in pending:
        path.unlink()
        parent = path.parent
        while parent != root:
            try:
                parent.rmdir()
            except OSError:
                break
            parent = parent.parent
    state.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--offline', action='store_true')
    args = parser.parse_args()
    if not args.check and not args.offline:
        subprocess.run(['git', 'submodule', 'update', '--init', '--recursive'], cwd=ROOT, check=True)
    for package in PACKAGES:
        if not (ROOT / 'external' / package / 'README.md').is_file():
            parser.error('shared repositories are missing; run without --offline to initialize them')
    retire_copies(ROOT, args.check)


if __name__ == '__main__':
    main()
