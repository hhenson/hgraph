"""Build and run a C++ consumer against the installed hgraph-fabric wheel."""

from __future__ import annotations

import importlib
import os
from pathlib import Path
import subprocess
import sys
import sysconfig
import tempfile


SOURCE_DIR = Path(__file__).resolve().parent


def cmake_tool(name: str) -> str:
    """Locate a native CMake executable without a Python console wrapper."""

    executable = f"{name}.exe" if sys.platform == "win32" else name
    try:
        cmake_package = importlib.import_module("cmake")
    except ImportError:
        return executable
    candidate = Path(cmake_package.CMAKE_BIN_DIR, executable)
    if not candidate.is_file():
        raise RuntimeError(f"CMake package does not contain {candidate}")
    return str(candidate)


def run(command: list[str], *, environment: dict[str, str] | None = None) -> None:
    subprocess.run(command, check=True, env=environment)


def runtime_environment(package_prefix: Path) -> dict[str, str]:
    variable = {
        "win32": "PATH",
        "darwin": "DYLD_LIBRARY_PATH",
    }.get(sys.platform, "LD_LIBRARY_PATH")
    directories = (
        package_prefix,
        package_prefix / "lib",
        package_prefix / "lib64",
        package_prefix / "pyarrow",
    )
    additions = os.pathsep.join(map(str, filter(Path.is_dir, directories)))
    environment = os.environ.copy()
    if current := environment.get(variable):
        additions = os.pathsep.join((additions, current))
    environment[variable] = additions
    return environment


def main() -> int:
    package_prefix = Path(sysconfig.get_path("purelib")).resolve()
    cmake = cmake_tool("cmake")
    ctest = cmake_tool("ctest")
    with tempfile.TemporaryDirectory(
        prefix="hgraph-fabric-consumer-"
    ) as directory:
        build_dir = Path(directory)
        configure = [
            cmake,
            f"-S{SOURCE_DIR}",
            f"-B{build_dir}",
            "-DCMAKE_BUILD_TYPE=Release",
            f"-DCMAKE_PREFIX_PATH={package_prefix}",
            f"-DPython_EXECUTABLE={sys.executable}",
        ]
        if sys.platform == "win32":
            configure.extend(["-G", "Visual Studio 18 2026", "-A", "x64"])
        else:
            configure.extend(["-G", "Ninja"])
        run(configure)

        build = [cmake, "--build", str(build_dir), "-j", "2"]
        if sys.platform == "win32":
            build.extend(["--config", "Release"])
        run(build)

        test = [ctest, "--test-dir", str(build_dir), "--output-on-failure"]
        if sys.platform == "win32":
            test.extend(["--build-config", "Release"])
        run(test, environment=runtime_environment(package_prefix))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
