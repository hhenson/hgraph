#!/usr/bin/env python3
"""Audit hgraph-persistence wheels and source distributions."""

from __future__ import annotations

import argparse
import glob
import itertools
import tarfile
import zipfile
from pathlib import Path

WHEEL_REQUIRED = (
    "hgraph_persistence/__init__.py",
    "hgraph_persistence/compat.py",
    "include/hgraph/persistence/frame_store.h",
    "include/hgraph/persistence/recording_store.h",
    # GNUInstallDirs selects lib64 on manylinux, while macOS and Windows wheels
    # use lib. Match the package-relative suffix so both layouts are audited.
    "cmake/hgraph-persistence/hgraph-persistenceConfig.cmake",
    "cmake/hgraph-persistence/hgraphPersistenceTargets.cmake",
    "cmake/hgraph-persistence/hgraph_persistence_arrow.cmake",
)
SDIST_REQUIRED = (
    "CMakeLists.txt",
    "cmake/hgraph-persistenceConfig.cmake.in",
    "cmake/hgraph_persistence_arrow.cmake",
    "include/hgraph/persistence/export.h",
    "include/hgraph/persistence/frame_store.h",
    "include/hgraph/persistence/recording_store.h",
    "python/hgraph_persistence/__init__.py",
    "python/hgraph_persistence/compat.py",
    "src/frame_store.cpp",
    "src/python_module.cpp",
    "src/record_replay_frame_impl.cpp",
    "src/record_replay_frame_impl.h",
    "src/recording_store.cpp",
    "src/registration.cpp",
    "test_package/CMakeLists.txt",
    "tests/test_frame_store.cpp",
    "tests/test_record_replay_frame.cpp",
    "tests/test_record_replay_partitioned.cpp",
    "tools/audit_distribution.py",
)


def _assert_required(names: set[str], required: tuple[str, ...], path: Path) -> None:
    for item in required:
        if not any(name.endswith(f"/{item}") or name == item for name in names):
            raise AssertionError(f"{path}: missing {item}")


def _audit_wheel(path: Path) -> None:
    with zipfile.ZipFile(path) as archive:
        names = set(archive.namelist())
        metadata_names = [name for name in names if name.endswith(".dist-info/METADATA")]
        if len(metadata_names) != 1:
            raise AssertionError(f"{path}: expected one METADATA file, got {metadata_names}")
        metadata = archive.read(metadata_names[0]).decode()

    _assert_required(names, WHEEL_REQUIRED, path)
    native_modules = [
        name
        for name in names
        if name.startswith("hgraph_persistence/_hgraph_persistence.")
        and name.endswith((".so", ".pyd"))
    ]
    if len(native_modules) != 1:
        raise AssertionError(
            f"{path}: expected one native hgraph_persistence module, got {native_modules}"
        )
    persistence_libraries = [
        name
        for name in names
        if Path(name).name.startswith(("libhgraph_persistence", "hgraph_persistence"))
        and Path(name).suffix in {".a", ".lib"}
    ]
    if len(persistence_libraries) != 1:
        raise AssertionError(
            f"{path}: expected one native persistence library, got {persistence_libraries}"
        )
    forbidden = [
        name
        for name in names
        if Path(name).name.startswith(
            (
                "hgraph_runtime",
                "hgraph_stdlib",
                "hgraph_wiring",
                "libhgraph_runtime",
                "libhgraph_stdlib",
                "libhgraph_wiring",
                "libnanobind-abi3",
                "nanobind-abi3",
                "libarrow",
                "libparquet",
            )
        )
    ]
    if forbidden:
        raise AssertionError(f"{path}: embeds core or Arrow runtime libraries: {forbidden}")
    if "Requires-Dist: hgraph" not in metadata:
        raise AssertionError(f"{path}: does not declare the core distribution dependency")


def _audit_sdist(path: Path) -> None:
    with tarfile.open(path, "r:gz") as archive:
        names = set(archive.getnames())
    _assert_required(names, SDIST_REQUIRED, path)


def _audit_distribution(path: Path) -> None:
    if path.suffix == ".whl":
        _audit_wheel(path)
    elif path.name.endswith(".tar.gz"):
        _audit_sdist(path)
    else:
        raise SystemExit(f"unsupported distribution: {path}")
    print(f"audited {path}")


def main() -> None:
    arguments = argparse.ArgumentParser()
    arguments.add_argument("distributions", nargs="+")
    patterns = arguments.parse_args().distributions
    matches = itertools.chain.from_iterable(glob.iglob(pattern) for pattern in patterns)
    paths = sorted(map(Path, set(matches)))
    if not paths:
        raise SystemExit("no hgraph-persistence distributions matched")
    for distribution in paths:
        _audit_distribution(distribution)


if __name__ == "__main__":
    main()
