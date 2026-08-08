#!/usr/bin/env python3
"""Audit hgraph-kafka wheels and source distributions."""

from __future__ import annotations

import argparse
import glob
import re
import shutil
import subprocess
import tarfile
import tempfile
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

WINDOWS_EXTENSION_ALLOWED_DEPENDENCIES = {
    "hgraph_stdlib.dll",
    "nanobind-abi3.dll",
    "python3.dll",
    "advapi32.dll",
    "bcrypt.dll",
    "crypt32.dll",
    "kernel32.dll",
    "ntdll.dll",
    "ole32.dll",
    "secur32.dll",
    "shell32.dll",
    "user32.dll",
    "ws2_32.dll",
}


def _assert_required(names: set[str], required: tuple[str, ...], path: Path) -> None:
    for item in required:
        if not any(name.endswith(f"/{item}") or name == item for name in names):
            raise AssertionError(f"{path}: missing {item}")


def _unexpected_windows_dependencies(dependencies: set[str]) -> list[str]:
    unexpected = []
    for dependency in dependencies:
        normalized = dependency.lower()
        if normalized in WINDOWS_EXTENSION_ALLOWED_DEPENDENCIES:
            continue
        if normalized.startswith(("api-ms-win-", "ext-ms-win-")):
            continue
        if normalized.startswith(("msvcp", "vcruntime")):
            continue
        unexpected.append(dependency)
    return sorted(unexpected, key=str.lower)


def _windows_dependencies_from_output(output: str) -> set[str]:
    return set(
        re.findall(
            r"^\s+(?:DLL Name:\s*)?([^\s]+\.dll)\s*$",
            output,
            flags=re.IGNORECASE | re.MULTILINE,
        )
    )


def _audit_windows_extension_dependencies(
    archive: zipfile.ZipFile, module_name: str, path: Path
) -> None:
    dumpbin = shutil.which("dumpbin")
    objdump = shutil.which("objdump")
    if dumpbin is not None:
        command_prefix = [dumpbin, "/DEPENDENTS"]
    elif objdump is not None:
        command_prefix = [objdump, "-p"]
    else:
        raise AssertionError(
            f"{path}: dumpbin or objdump is required to audit a Windows wheel"
        )

    with tempfile.TemporaryDirectory() as directory:
        module_path = Path(directory) / Path(module_name).name
        module_path.write_bytes(archive.read(module_name))
        result = subprocess.run(
            [*command_prefix, str(module_path)],
            check=True,
            capture_output=True,
            text=True,
        )

    dependencies = _windows_dependencies_from_output(result.stdout)
    if not dependencies:
        raise AssertionError(f"{path}: unable to identify native module dependencies")
    unexpected = _unexpected_windows_dependencies(dependencies)
    if unexpected:
        raise AssertionError(
            f"{path}: native module has unbundled runtime dependencies: {unexpected}"
        )


def _audit_wheel(path: Path) -> None:
    with zipfile.ZipFile(path) as archive:
        names = set(archive.namelist())
        metadata_names = [name for name in names if name.endswith(".dist-info/METADATA")]
        if len(metadata_names) != 1:
            raise AssertionError(f"{path}: expected one METADATA file, got {metadata_names}")
        metadata = archive.read(metadata_names[0]).decode()
        native_modules = [
            name
            for name in names
            if name.startswith("hgraph_kafka/_hgraph_kafka.")
            and name.endswith((".so", ".pyd"))
        ]
        if len(native_modules) != 1:
            raise AssertionError(
                f"{path}: expected one native hgraph_kafka module, got {native_modules}"
            )
        if path.name.endswith("-win_amd64.whl"):
            _audit_windows_extension_dependencies(archive, native_modules[0], path)

    _assert_required(names, WHEEL_REQUIRED, path)
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
