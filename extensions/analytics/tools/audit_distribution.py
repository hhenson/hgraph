#!/usr/bin/env python3
"""Audit hgraph-analytics wheels and source distributions."""

from __future__ import annotations

import argparse
import glob
import tarfile
import zipfile
from pathlib import Path

WHEEL_REQUIRED = (
    "hgraph_analytics/__init__.py",
    "hgraph_analytics/py.typed",
    "include/hgraph/analytics/operators.h",
    # GNUInstallDirs selects lib64 on manylinux, while macOS and Windows wheels
    # use lib. Match the package-relative suffix so both layouts are audited.
    "cmake/hgraph-analytics/hgraph-analyticsConfig.cmake",
    "cmake/hgraph-analytics/hgraphAnalyticsTargets.cmake",
)
SDIST_REQUIRED = (
    "CMakeLists.txt",
    "cmake/hgraph-analyticsConfig.cmake.in",
    "include/hgraph/analytics/operators.h",
    "python/hgraph_analytics/__init__.py",
    "python/hgraph_analytics/py.typed",
    "src/pct_change.cpp",
    "test_package/CMakeLists.txt",
    "tests/test_pct_change.cpp",
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
        if name.startswith("hgraph_analytics/_hgraph_analytics.")
        and name.endswith((".so", ".pyd"))
    ]
    if len(native_modules) != 1:
        raise AssertionError(
            f"{path}: expected one native hgraph_analytics module, got {native_modules}"
        )
    analytics_libraries = [
        name
        for name in names
        if Path(name).name.startswith(("libhgraph_analytics", "hgraph_analytics"))
        and Path(name).suffix in {".a", ".lib"}
    ]
    if len(analytics_libraries) != 1:
        raise AssertionError(
            f"{path}: expected one native analytics library, got {analytics_libraries}"
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
            )
        )
    ]
    if forbidden:
        raise AssertionError(f"{path}: embeds core runtime libraries: {forbidden}")
    if "Requires-Dist: hgraph" not in metadata:
        raise AssertionError(f"{path}: does not declare the core distribution dependency")


def _audit_sdist(path: Path) -> None:
    with tarfile.open(path, "r:gz") as archive:
        names = set(archive.getnames())
    _assert_required(names, SDIST_REQUIRED, path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("distributions", nargs="+")
    args = parser.parse_args()
    paths = sorted(
        {Path(match) for pattern in args.distributions for match in glob.glob(pattern)}
    )
    if not paths:
        raise SystemExit("no hgraph-analytics distributions matched")
    for path in paths:
        if path.suffix == ".whl":
            _audit_wheel(path)
        elif path.name.endswith(".tar.gz"):
            _audit_sdist(path)
        else:
            raise SystemExit(f"unsupported distribution: {path}")
        print(f"audited {path}")


if __name__ == "__main__":
    main()
