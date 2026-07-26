"""Content fingerprints shared by benchmarks and parity tooling."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


def hg_cpp_source_fingerprint(repo_root: Path, *, python_version: str | None = None) -> str:
    """Hash inputs that can change the hg_cpp wheel.

    Tooling, tests, documentation, and parity corpus files are deliberately
    excluded. Changing those files must not force a native rebuild.
    """

    repo_root = repo_root.resolve()
    digest = hashlib.sha256()
    roots = (
        repo_root / "CMakeLists.txt",
        repo_root / "pyproject.toml",
        repo_root / "cmake",
        repo_root / "include",
        repo_root / "src",
        repo_root / "python" / "CMakeLists.txt",
        repo_root / "python" / "hgraph",
    )
    files: list[Path] = []
    for root in roots:
        if root.is_file():
            files.append(root)
        elif root.is_dir():
            files.extend(
                path
                for path in root.rglob("*")
                if path.is_file() and "__pycache__" not in path.parts
            )
    files.extend((repo_root / "python").glob("*.cpp"))
    files.extend((repo_root / "python").glob("*.h"))
    for path in sorted(set(files)):
        digest.update(path.relative_to(repo_root).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    if python_version is None:
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    digest.update(f"python-{python_version}".encode())
    return digest.hexdigest()
