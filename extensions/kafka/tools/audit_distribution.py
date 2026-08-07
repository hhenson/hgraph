#!/usr/bin/env python3
"""Audit hgraph-kafka wheels and source distributions."""

from __future__ import annotations

import argparse
import glob
import tarfile
import zipfile
from pathlib import Path


WHEEL_REQUIRED = (
    "hgraph_kafka/__init__.py",
    "hgraph/adaptors/kafka/__init__.py",
    "hgraph/adaptors/kafka/_api.py",
    "hgraph/adaptors/kafka/_impl.py",
)
SDIST_REQUIRED = (
    "CMakeLists.txt",
    "cmake/hgraph-kafkaConfig.cmake.in",
    "include/hgraph/kafka/service.h",
    "python/hgraph_kafka/__init__.py",
    "src/librdkafka_service.cpp",
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
    if not any(
        name.startswith("hgraph_kafka/_hgraph_kafka.")
        and (name.endswith(".so") or name.endswith(".pyd"))
        for name in names
    ):
        raise AssertionError(f"{path}: missing the native hgraph_kafka module")
    forbidden = [
        name
        for name in names
        if Path(name).name.startswith(("libhgraph_", "libnanobind-abi3"))
    ]
    if forbidden:
        raise AssertionError(f"{path}: embeds core runtime libraries: {forbidden}")
    if "Requires-Dist: hg_cpp" not in metadata and "Requires-Dist: hg-cpp" not in metadata:
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
        raise SystemExit("no hgraph-kafka distributions matched")
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
