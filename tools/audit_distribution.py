#!/usr/bin/env python3
"""Validate the contents of hgraph wheels and source distributions."""

from __future__ import annotations

import argparse
import glob
from pathlib import Path, PurePosixPath
import tarfile
from typing import Iterable
import zipfile


SDIST_REQUIRED = {
    "CMakeLists.txt",
    "LICENSE",
    "pyproject.toml",
    "include/hgraph/hgraph.h",
    "python/CMakeLists.txt",
    "python/hgraph/__init__.py",
    "src/CMakeLists.txt",
    "tests/install_consumer/CMakeLists.txt",
    "tests/install_consumer/check.py",
    "tests/install_consumer/fabric_extension_probe.cpp",
    "tests/install_consumer/fabric_extension_probe.h",
    "tests/python_extension_consumer/CMakeLists.txt",
    "tests/python_extension_consumer/check.py",
    "tests/python_extension_consumer/module.cpp",
    "tools/audit_distribution.py",
}

WHEEL_REQUIRED = {
    "hgraph/__init__.py",
    "include/hgraph/hgraph.h",
    "share/hgraph/debugger/hgraph_debug_common.py",
}

WHEEL_CMAKE_ROOTS = (
    "lib/cmake/hgraph",
    "lib64/cmake/hgraph",
)

WHEEL_CMAKE_FILES = (
    "hgraphConfig.cmake",
    "hgraphConfigVersion.cmake",
)

FORBIDDEN_PARTS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
}

FORBIDDEN_PREFIXES = (
    "benchmarks/results/",
    "ext/",
    "hgraph/numpy_/",
    "python/hgraph/numpy_/",
    "reports/",
)


class AuditError(RuntimeError):
    pass


def _safe_name(name: str) -> PurePosixPath:
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts:
        raise AuditError(f"unsafe archive path: {name}")
    return path


def _validate_common(paths: set[str]) -> None:
    forbidden = []
    for name in sorted(paths):
        path = PurePosixPath(name)
        if any(part in FORBIDDEN_PARTS for part in path.parts):
            forbidden.append(name)
            continue
        if any(part.startswith((".venv", "cmake-build")) for part in path.parts):
            forbidden.append(name)
            continue
        if name.startswith(FORBIDDEN_PREFIXES) or path.suffix in {".pyc", ".pyo"}:
            forbidden.append(name)
    if forbidden:
        raise AuditError("forbidden distribution content: " + ", ".join(forbidden))


def _require(paths: set[str], required: set[str]) -> None:
    missing = sorted(required - paths)
    if missing:
        raise AuditError("missing required distribution content: " + ", ".join(missing))


def _audit_wheel(path: Path) -> int:
    with zipfile.ZipFile(path) as archive:
        paths = {
            str(_safe_name(info.filename))
            for info in archive.infolist()
            if not info.is_dir()
        }

    _validate_common(paths)
    _require(paths, WHEEL_REQUIRED)

    if not any(
        all(f"{root}/{name}" in paths for name in WHEEL_CMAKE_FILES)
        for root in WHEEL_CMAKE_ROOTS
    ):
        raise AuditError(
            "wheel does not contain a complete hgraph CMake package under lib or lib64"
        )

    extension_names = {PurePosixPath(name).name for name in paths}
    if not any(
        name == "_hgraph.pyd" or (name.startswith("_hgraph.") and name.endswith(".so"))
        for name in extension_names
    ):
        raise AuditError("wheel does not contain the _hgraph extension")

    # Core links no Parquet (RFC 0025 checkpoint 5 moved the frame store to
    # hgraph-persistence). Staging it here would ship a durable-store
    # dependency in the core wheel.
    if "parquet.dll" in extension_names or any(
        PurePosixPath(name).name.startswith("libparquet") for name in paths
    ):
        raise AuditError(
            "core wheel stages a Parquet runtime; Parquet is durable-store "
            "policy owned by hgraph-persistence"
        )

    libraries = [
        name
        for name in paths
        if PurePosixPath(name).parts[0] in {"lib", "lib64"}
        and "hgraph" in PurePosixPath(name).name
        and PurePosixPath(name).suffix in {".a", ".dylib", ".lib", ".so"}
    ]
    if not libraries:
        raise AuditError("wheel does not contain the native hgraph libraries")
    if not any("nanobind-abi3" in PurePosixPath(name).name for name in paths):
        raise AuditError("wheel does not contain the shared nanobind runtime")
    return len(paths)


def _audit_sdist(path: Path) -> int:
    with tarfile.open(path, "r:gz") as archive:
        members = archive.getmembers()
        unsafe_links = [member.name for member in members if member.issym() or member.islnk()]
        if unsafe_links:
            raise AuditError("source distribution contains links: " + ", ".join(unsafe_links))
        raw_paths = [_safe_name(member.name) for member in members if member.isfile()]

    roots = {item.parts[0] for item in raw_paths if item.parts}
    if len(roots) != 1:
        raise AuditError(f"source distribution must have one root directory, found {sorted(roots)}")
    root = roots.pop()
    if not root.startswith("hgraph-"):
        raise AuditError(f"unexpected source distribution root: {root}")

    paths = {
        str(PurePosixPath(*item.parts[1:]))
        for item in raw_paths
        if len(item.parts) > 1
    }
    _validate_common(paths)
    _require(paths, SDIST_REQUIRED)
    return len(paths)


def audit(path: Path) -> int:
    if path.name.endswith(".whl"):
        return _audit_wheel(path)
    if path.name.endswith(".tar.gz"):
        return _audit_sdist(path)
    raise AuditError(f"unsupported distribution type: {path}")


def _expand_artifacts(arguments: Iterable[str]) -> list[Path]:
    artifacts = []
    for argument in arguments:
        matches = [Path(match) for match in glob.glob(argument)]
        if not matches:
            raise AuditError(f"distribution artifact not found: {argument}")
        artifacts.extend(matches)
    return artifacts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+", help="wheel/sdist paths or glob patterns")
    args = parser.parse_args()

    try:
        artifacts = _expand_artifacts(args.artifacts)
        for artifact in artifacts:
            count = audit(artifact)
            print(f"PASS {artifact.name}: {count} files")
    except (AuditError, OSError, tarfile.TarError, zipfile.BadZipFile) as error:
        parser.exit(1, f"FAIL {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
