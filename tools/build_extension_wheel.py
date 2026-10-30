"""Build, audit, and test one extension against the matching core artifact."""

from __future__ import annotations

import argparse
from importlib import metadata
import os
from pathlib import Path
import subprocess
import sys

from packaging.tags import Tag
from packaging.utils import parse_wheel_filename
import pyarrow


BUILD_PLANS = {
    "kafka": ("kafka", "-Cbuild-dir=.ci-build/kafka/{wheel_tag}"),
    "analytics": ("analytics", "-Cbuild-dir=.ci-build/analytics/{wheel_tag}"),
    "web": ("web", "-Cbuild-dir=.ci-build/web/{wheel_tag}"),
    "persistence": ("persistence", "-Cbuild-dir=.ci-build/persistence/{wheel_tag}"),
    "fabric": ("fabric", "-Cbuild-dir=.ci-build/fabric/{wheel_tag}"),
}
PACKAGES = tuple(BUILD_PLANS)
ROOT = Path(__file__).resolve().parents[1]


def manylinux_excludes(package: str, arrow_major: int) -> list[str]:
    suffix = f".so.{arrow_major}00"
    excluded = [name + suffix for name in
                ("libarrow", "libarrow_compute", "libarrow_acero")]
    excluded += ["libhgraph_runtime.so", "libhgraph_wiring.so",
                 "libhgraph_stdlib.so", "libnanobind-abi3.so"]
    if package in ("persistence", "fabric"):
        excluded += ["libarrow_s3" + suffix, "libparquet" + suffix]
    if package == "fabric":
        excluded.append("libhgraph_persistence.so")
    return excluded


def verify_wheel(wheel: Path, package: str, manylinux: bool) -> None:
    name, _, _, tags = parse_wheel_filename(wheel.name)
    assert name == f"hgraph-{package}", wheel.name
    if manylinux:
        platform = "manylinux_2_28_x86_64"
    elif sys.platform == "darwin":
        platform = "macosx_26_0_arm64"
    elif sys.platform == "win32":
        platform = "win_amd64"
    else:
        raise ValueError("Linux release wheels must use the manylinux builder")
    assert Tag("cp312", "abi3", platform) in tags, (wheel.name, tags)


def build_extension(package: str, *, manylinux: bool) -> None:
    # Use only command data from the fixed table, including for direct Python
    # callers that bypass argparse's choices validation.
    try:
        package, build_directory = BUILD_PLANS[package]
    except KeyError:
        raise ValueError("unsupported extension package") from None
    sdk = Path(metadata.distribution("hgraph").locate_file("")).resolve()
    if not (sdk / "lib/cmake/hgraph/hgraphConfig.cmake").is_file():
        raise FileNotFoundError("the matching core wheel must install its SDK")
    env = dict(os.environ)
    prefixes = [str(sdk), env.get("CMAKE_PREFIX_PATH", "")]
    env["CMAKE_PREFIX_PATH"] = os.pathsep.join(filter(None, prefixes))
    dist = ROOT / f"{package}-dist"
    output = ROOT / f"{package}-dist-raw" if manylinux else dist
    subprocess.check_call([
        sys.executable, "-m", "build", "--wheel", "--no-isolation",
        "--skip-dependency-check", build_directory,
        "--outdir", str(output), str(ROOT / "extensions" / package),
    ], cwd=ROOT, env=env)
    if manylinux:
        dist.mkdir(exist_ok=True)
        command = ["auditwheel", "repair", "--plat", "manylinux_2_28_x86_64"]
        major = int(pyarrow.__version__.split(".", 1)[0])
        for library in manylinux_excludes(package, major):
            command += ["--exclude", library]
        subprocess.check_call([
            *command, "--wheel-dir", str(dist), *map(str, output.glob("*.whl")),
        ], cwd=ROOT)
    wheels = list(dist.glob("*.whl"))
    assert len(wheels) == 1, wheels
    wheel = wheels[0]
    verify_wheel(wheel, package, manylinux)
    subprocess.check_call(["abi3audit", "--strict", str(wheel)])
    subprocess.check_call([
        sys.executable, str(ROOT / "extensions" / package / "tools/audit_distribution.py"),
        str(wheel),
    ])
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", "--no-deps", str(wheel),
    ])
    subprocess.check_call([
        sys.executable, "-m", "pytest",
        str(ROOT / "extensions" / package / "python/tests"), "-q", "-m", "not wip",
    ], cwd=ROOT)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", choices=PACKAGES, required=True)
    parser.add_argument("--manylinux", action="store_true")
    args = parser.parse_args()
    build_extension(args.package, manylinux=args.manylinux)


if __name__ == "__main__":
    main()
