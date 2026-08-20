"""Build and run a C++ consumer against the installed hgraph-fabric wheel."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import sysconfig
import tempfile


SOURCE_DIR = Path(__file__).resolve().parent


def cmake_tool(name: str) -> str:
    executable = f"{name}.exe" if sys.platform == "win32" else name
    try:
        from cmake import CMAKE_BIN_DIR
    except ImportError:
        return executable
    candidate = Path(CMAKE_BIN_DIR) / executable
    if not candidate.is_file():
        raise RuntimeError(f"CMake package does not contain {candidate}")
    return str(candidate)


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
            "-S",
            str(SOURCE_DIR),
            "-B",
            str(build_dir),
            "-DCMAKE_BUILD_TYPE=Release",
            f"-DCMAKE_PREFIX_PATH={package_prefix}",
            f"-DPython_EXECUTABLE={sys.executable}",
        ]
        if sys.platform == "win32":
            configure.extend(["-G", "Visual Studio 18 2026", "-A", "x64"])
        else:
            configure.extend(["-G", "Ninja"])
        subprocess.run(configure, check=True)

        build = [cmake, "--build", str(build_dir), "--parallel", "2"]
        if sys.platform == "win32":
            build.extend(["--config", "Release"])
        subprocess.run(build, check=True)

        runtime_dirs = [
            package_prefix,
            package_prefix / "lib",
            package_prefix / "lib64",
            package_prefix / "pyarrow",
        ]
        runtime_variable = (
            "PATH"
            if sys.platform == "win32"
            else "DYLD_LIBRARY_PATH"
            if sys.platform == "darwin"
            else "LD_LIBRARY_PATH"
        )
        environment = os.environ.copy()
        existing = environment.get(runtime_variable)
        runtime_path = os.pathsep.join(
            str(path) for path in runtime_dirs if path.is_dir()
        )
        environment[runtime_variable] = (
            runtime_path
            if not existing
            else os.pathsep.join((runtime_path, existing))
        )

        test = [ctest, "--test-dir", str(build_dir), "--output-on-failure"]
        if sys.platform == "win32":
            test.extend(["--build-config", "Release"])
        subprocess.run(test, check=True, env=environment)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
