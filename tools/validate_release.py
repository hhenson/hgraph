"""Validate shared release tags against package and native API invariants."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib.error import HTTPError
from urllib.request import urlopen


MINIMUM_CORE_VERSION = (0, 8, 0)
RELEASE_PACKAGES = ("hgraph", "hgraph-kafka", "hgraph-analytics", "hgraph-persistence")
_TAG_PATTERN = re.compile(r"(\d+\.\d+\.\d+(?:(?:a|b|rc)\d+)?)")
_VERSION_CORE = re.compile(r"(\d+)\.(\d+)\.(\d+)")
_CMAKE_PROJECT_VERSION = re.compile(
    r"project\(\s*[A-Za-z_][A-Za-z0-9_]*\s+VERSION\s+"
    r"(\d+)\.(\d+)\.(\d+)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Release:
    packages: tuple[str, ...]
    version: str
    core: tuple[int, int, int]


def parse_release_tag(tag: str) -> Release:
    match = _TAG_PATTERN.fullmatch(tag)
    if match is None:
        raise ValueError(
            "release tag must be a bare version matching x.x.x "
            f"(including PEP 440 prereleases), got {tag!r}"
        )
    version = match.group(1)
    core_match = _VERSION_CORE.match(version)
    if core_match is None:  # guarded by the tag pattern
        raise ValueError(f"release tag has no numeric version core: {tag!r}")
    return Release(
        packages=RELEASE_PACKAGES,
        version=version,
        core=tuple(map(int, core_match.groups())),
    )


def pypi_release_exists(package: str, version: str) -> bool:
    try:
        with urlopen(
            f"https://pypi.org/pypi/{package}/{version}/json", timeout=30
        ):
            return True
    except HTTPError as error:
        if error.code == 404:
            return False
        raise


def native_project_version(cmake_path: Path) -> tuple[int, int, int]:
    match = _CMAKE_PROJECT_VERSION.search(cmake_path.read_text())
    if match is None:
        raise ValueError(f"could not read hgraph project version from {cmake_path}")
    return tuple(map(int, match.groups()))


def validate_release(
    tag: str,
    *,
    cmake_path: Path = Path("CMakeLists.txt"),
    kafka_cmake_path: Path = Path("extensions/kafka/CMakeLists.txt"),
    analytics_cmake_path: Path = Path("extensions/analytics/CMakeLists.txt"),
    persistence_cmake_path: Path = Path("extensions/persistence/CMakeLists.txt"),
    release_exists: Callable[[str, str], bool] = pypi_release_exists,
) -> Release:
    release = parse_release_tag(tag)
    if release.core < MINIMUM_CORE_VERSION:
        raise ValueError(
            "the C++-first hgraph release line starts at 0.8.0, "
            f"got {release.version}"
        )
    for package, path in (
        ("hgraph", cmake_path),
        ("hgraph-kafka", kafka_cmake_path),
        ("hgraph-analytics", analytics_cmake_path),
        ("hgraph-persistence", persistence_cmake_path),
    ):
        native_version = native_project_version(path)
        if release.core < native_version:
            raise ValueError(
                f"{package} tag {release.version} predates native API version "
                f"{native_version} declared by {path}"
            )
    for package in release.packages:
        if release_exists(package, release.version):
            raise ValueError(f"{package} {release.version} already exists on PyPI")
    return release


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("tag")
    args = parser.parse_args()
    try:
        release = validate_release(args.tag)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    packages = ", ".join(release.packages[:-1]) + f", and {release.packages[-1]}"
    print(f"Validated new {packages} release {release.version}")


if __name__ == "__main__":
    main()
