#!/usr/bin/env python3
"""Keep the distributable hgraph plugin skills in sync with their sources."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


SKILL_NAMES = (
    "hgraph-compute-sink-node",
    "hgraph-write-graphs",
    "hgraph-write-operators",
)

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SOURCE_ROOT = REPOSITORY_ROOT / ".agents" / "skills"
BUNDLE_ROOT = PLUGIN_ROOT / "skills"


def contents(root: Path) -> dict[Path, bytes]:
    return {
        path.relative_to(root): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


def differences() -> list[str]:
    problems: list[str] = []
    expected_names = set(SKILL_NAMES)
    bundled_names = {path.name for path in BUNDLE_ROOT.iterdir() if path.is_dir()}

    for extra in sorted(bundled_names - expected_names):
        problems.append(f"unexpected bundled skill: {extra}")

    for name in SKILL_NAMES:
        source = SOURCE_ROOT / name
        bundled = BUNDLE_ROOT / name
        if not source.is_dir():
            problems.append(f"missing canonical skill: {name}")
        elif not bundled.is_dir():
            problems.append(f"missing bundled skill: {name}")
        elif contents(source) != contents(bundled):
            problems.append(f"bundled skill differs from canonical source: {name}")

    return problems


def sync() -> None:
    BUNDLE_ROOT.mkdir(parents=True, exist_ok=True)
    for name in SKILL_NAMES:
        source = SOURCE_ROOT / name
        if not source.is_dir():
            raise SystemExit(f"canonical skill does not exist: {source}")

        bundled = BUNDLE_ROOT / name
        if bundled.exists():
            shutil.rmtree(bundled)
        shutil.copytree(source, bundled)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--check", action="store_true", help="check for drift (default)")
    action.add_argument("--write", action="store_true", help="refresh the bundled copies")
    args = parser.parse_args()

    if args.write:
        sync()

    problems = differences()
    if problems:
        for problem in problems:
            print(problem)
        return 1

    print("hgraph plugin skills are in sync")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
