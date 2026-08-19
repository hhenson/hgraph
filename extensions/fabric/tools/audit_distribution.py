#!/usr/bin/env python3
"""Audit hgraph-fabric wheels and source distributions."""

from __future__ import annotations

import argparse
import glob
import re
import tarfile
import zipfile
from pathlib import Path

WHEEL_REQUIRED = (
    "hgraph_fabric/__init__.py",
    "hgraph_fabric/py.typed",
    "include/hgraph/fabric/config.h",
    "include/hgraph/fabric/export.h",
    "include/hgraph/fabric/fabric.h",
    "include/hgraph/fabric/metadata_codec.h",
    "include/hgraph/fabric/notifier.h",
    "include/hgraph/fabric/operators.h",
    "include/hgraph/fabric/types.h",
    "include/hgraph/fabric/value_builders.h",
    # GNUInstallDirs selects lib64 on manylinux, while macOS and Windows wheels
    # use lib. Match the package-relative suffix so both layouts are audited.
    "cmake/hgraph-fabric/hgraph-fabricConfig.cmake",
    "cmake/hgraph-fabric/hgraphFabricTargets.cmake",
)
SDIST_REQUIRED = (
    "CMakeLists.txt",
    "cmake/hgraph-fabricConfig.cmake.in",
    "include/hgraph/fabric/config.h",
    "include/hgraph/fabric/export.h",
    "include/hgraph/fabric/fabric.h",
    "include/hgraph/fabric/metadata_codec.h",
    "include/hgraph/fabric/notifier.h",
    "include/hgraph/fabric/operators.h",
    "include/hgraph/fabric/types.h",
    "include/hgraph/fabric/value_builders.h",
    "python/hgraph_fabric/__init__.py",
    "python/hgraph_fabric/py.typed",
    "src/config.cpp",
    "src/impl/memory_notifier.cpp",
    "src/metadata_codec.cpp",
    "src/notifier.cpp",
    "src/operators.cpp",
    "src/python_module.cpp",
    "src/types.cpp",
    "src/value_builders.cpp",
    "test_package/CMakeLists.txt",
    "test_package/main.cpp",
    "tests/fixtures/as_of_v1.hex",
    "tests/fixtures/latest_v1.hex",
    "tests/fixtures/revision_v1.hex",
    "tests/test_public_contracts.cpp",
    "tools/audit_distribution.py",
)


def _missing_members(names: set[str], required: tuple[str, ...]) -> list[str]:
    """Return required archive members not present below any package prefix."""

    return [
        item
        for item in required
        if all(name != item and not name.endswith(f"/{item}") for name in names)
    ]


def _require_members(
    path: Path, names: set[str], required: tuple[str, ...]
) -> None:
    if missing := _missing_members(names, required):
        raise AssertionError(f"{path}: missing archive members: {', '.join(missing)}")


def _require_one(path: Path, description: str, matches: list[str]) -> str:
    if len(matches) != 1:
        raise AssertionError(f"{path}: expected one {description}, got {matches}")
    return matches[0]


def _audit_wheel(path: Path) -> None:
    with zipfile.ZipFile(path) as archive:
        names = set(archive.namelist())
        metadata_name = _require_one(
            path,
            "METADATA file",
            [name for name in names if name.endswith(".dist-info/METADATA")],
        )
        metadata = archive.read(metadata_name).decode()

    _require_members(path, names, WHEEL_REQUIRED)
    native_modules = [
        name
        for name in names
        if name.startswith("hgraph_fabric/_hgraph_fabric.")
        and name.endswith((".so", ".pyd"))
    ]
    _require_one(path, "native hgraph_fabric module", native_modules)
    fabric_libraries = [
        name
        for name in names
        if Path(name).name.startswith(("libhgraph_fabric", "hgraph_fabric"))
        and Path(name).suffix in {".a", ".lib"}
    ]
    _require_one(path, "native fabric library", fabric_libraries)
    forbidden = [
        name
        for name in names
        if Path(name).name.startswith(
            (
                "hgraph_runtime",
                "hgraph_stdlib",
                "hgraph_wiring",
                "hgraph_persistence",
                "libhgraph_runtime",
                "libhgraph_stdlib",
                "libhgraph_wiring",
                "libhgraph_persistence",
                "libnanobind-abi3",
                "nanobind-abi3",
                "libarrow",
                "libparquet",
            )
        )
    ]
    if forbidden:
        raise AssertionError(
            f"{path}: embeds dependency runtime libraries: {forbidden}"
        )
    dependencies = {
        match.group(1).lower().replace("_", "-")
        for line in metadata.splitlines()
        if (match := re.match(r"^Requires-Dist:\s*([A-Za-z0-9_.-]+)", line))
    }
    for dependency in ("hgraph", "hgraph-persistence"):
        if dependency not in dependencies:
            raise AssertionError(
                f"{path}: does not declare the {dependency} distribution dependency"
            )


def _audit_sdist(path: Path) -> None:
    with tarfile.open(path, "r:gz") as archive:
        names = set(archive.getnames())
    _require_members(path, names, SDIST_REQUIRED)


def _auditor(path: Path):
    if path.suffix == ".whl":
        return _audit_wheel
    if path.name.endswith(".tar.gz"):
        return _audit_sdist
    raise SystemExit(f"unsupported distribution: {path}")


def main() -> None:
    arguments = argparse.ArgumentParser()
    arguments.add_argument("distributions", nargs="+")
    patterns = arguments.parse_args().distributions
    paths = sorted(
        {Path(match) for pattern in patterns for match in glob.iglob(pattern)}
    )
    if not paths:
        raise SystemExit("no hgraph-fabric distributions matched")
    for distribution in paths:
        _auditor(distribution)(distribution)
        print(f"audited {distribution}")


if __name__ == "__main__":
    main()
