from __future__ import annotations

import io
from pathlib import Path
import subprocess
import sys
import tarfile
import zipfile


ROOT = Path(__file__).resolve().parents[2]
AUDITOR = ROOT / "tools" / "audit_distribution.py"

WHEEL_FILES = (
    "_hgraph.abi3.so",
    "hgraph/__init__.py",
    "include/hgraph/hgraph.h",
    "lib/cmake/hgraph/hgraphConfig.cmake",
    "lib/cmake/hgraph/hgraphConfigVersion.cmake",
    "lib/libhgraph_core.a",
    "lib/libnanobind-abi3.so",
    "share/hgraph/debugger/hgraph_debug_common.py",
)

SDIST_FILES = (
    "CMakeLists.txt",
    "LICENSE",
    "pyproject.toml",
    "include/hgraph/hgraph.h",
    "python/CMakeLists.txt",
    "python/hgraph/__init__.py",
    "src/CMakeLists.txt",
    "tests/install_consumer/CMakeLists.txt",
    "tests/install_consumer/check.py",
    "tests/python_extension_consumer/CMakeLists.txt",
    "tests/python_extension_consumer/check.py",
    "tests/python_extension_consumer/module.cpp",
    "tools/audit_distribution.py",
)


def _wheel(path: Path, files: tuple[str, ...] = WHEEL_FILES) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        for name in files:
            archive.writestr(name, b"content")


def _sdist(path: Path, files: tuple[str, ...] = SDIST_FILES) -> None:
    with tarfile.open(path, "w:gz") as archive:
        for name in files:
            data = b"content"
            info = tarfile.TarInfo(f"hgraph-0.8.0/{name}")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))


def _audit(path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(AUDITOR), str(path)],
        check=False,
        capture_output=True,
        text=True,
    )


def test_wheel_distribution_audit_accepts_release_payload(tmp_path):
    wheel = tmp_path / "hgraph-0.8.0-cp312-abi3-any.whl"
    _wheel(wheel)

    result = _audit(wheel)

    assert result.returncode == 0, result.stderr
    assert "PASS" in result.stdout


def test_wheel_distribution_audit_accepts_linux_lib64_payload(tmp_path):
    wheel = tmp_path / "hgraph-0.8.0-cp312-abi3-any.whl"
    files = tuple(
        name.replace("lib/", "lib64/", 1) if name.startswith("lib/") else name
        for name in WHEEL_FILES
    )
    _wheel(wheel, files)

    result = _audit(wheel)

    assert result.returncode == 0, result.stderr
    assert "PASS" in result.stdout


def test_source_distribution_audit_accepts_release_payload(tmp_path):
    sdist = tmp_path / "hgraph-0.8.0.tar.gz"
    _sdist(sdist)

    result = _audit(sdist)

    assert result.returncode == 0, result.stderr
    assert "PASS" in result.stdout


def test_distribution_audit_rejects_missing_native_payload(tmp_path):
    wheel = tmp_path / "hgraph-0.8.0-cp312-abi3-any.whl"
    _wheel(wheel, tuple(name for name in WHEEL_FILES if not name.startswith("lib/libhgraph")))

    result = _audit(wheel)

    assert result.returncode == 1
    assert "native hgraph libraries" in result.stderr


def test_distribution_audit_rejects_missing_shared_nanobind_runtime(tmp_path):
    wheel = tmp_path / "hgraph-0.8.0-cp312-abi3-any.whl"
    _wheel(wheel, tuple(name for name in WHEEL_FILES if "nanobind-abi3" not in name))

    result = _audit(wheel)

    assert result.returncode == 1
    assert "shared nanobind runtime" in result.stderr


def test_distribution_audit_rejects_incomplete_cmake_payload(tmp_path):
    wheel = tmp_path / "hgraph-0.8.0-cp312-abi3-any.whl"
    _wheel(
        wheel,
        tuple(name for name in WHEEL_FILES if not name.endswith("hgraphConfigVersion.cmake")),
    )

    result = _audit(wheel)

    assert result.returncode == 1
    assert "complete hgraph CMake package" in result.stderr


def test_distribution_audit_rejects_private_or_generated_content(tmp_path):
    sdist = tmp_path / "hgraph-0.8.0.tar.gz"
    _sdist(sdist, SDIST_FILES + ("reports/release.txt", "python/hgraph/__pycache__/bad.pyc"))

    result = _audit(sdist)

    assert result.returncode == 1
    assert "forbidden distribution content" in result.stderr


def test_distribution_audit_rejects_retired_numpy_module(tmp_path):
    wheel = tmp_path / "hgraph-0.8.0-cp312-abi3-any.whl"
    _wheel(wheel, WHEEL_FILES + ("hgraph/numpy_/__init__.py",))

    result = _audit(wheel)

    assert result.returncode == 1
    assert "hgraph/numpy_/__init__.py" in result.stderr


def test_distribution_audit_rejects_unsafe_archive_paths(tmp_path):
    wheel = tmp_path / "hgraph-0.8.0-cp312-abi3-any.whl"
    _wheel(wheel, WHEEL_FILES + ("../outside",))

    result = _audit(wheel)

    assert result.returncode == 1
    assert "unsafe archive path" in result.stderr
