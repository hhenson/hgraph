from __future__ import annotations

import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

PERSISTENCE_ROOT = Path(__file__).resolve().parents[2]
AUDIT_SCRIPT = PERSISTENCE_ROOT / "tools/audit_distribution.py"


@pytest.mark.skipif(sys.platform != "linux", reason="Linux wheel RUNPATH regression")
def test_installed_bridge_imports_in_a_clean_process():
    environment = os.environ.copy()
    environment.pop("LD_LIBRARY_PATH", None)
    result = subprocess.run(
        [sys.executable, "-I", "-c", "import hgraph_persistence"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("library_directory", ["lib", "lib64"])
def test_wheel_audit_accepts_platform_library_directories(
    tmp_path: Path, library_directory: str
):
    wheel = tmp_path / "hgraph_persistence-0.0.0-cp312-abi3-any.whl"
    files = {
        "hgraph_persistence/__init__.py": "",
        "hgraph_persistence/compat.py": "",
        "hgraph_persistence/_hgraph_persistence.abi3.so": "",
        "include/hgraph/persistence/frame_store.h": "",
        "include/hgraph/persistence/object_store.h": "",
        "include/hgraph/persistence/recording_store.h": "",
        "include/hgraph/persistence/store_location.h": "",
        f"{library_directory}/libhgraph_persistence.so": "",
        f"{library_directory}/cmake/hgraph-persistence/hgraph-persistenceConfig.cmake": "",
        f"{library_directory}/cmake/hgraph-persistence/hgraphPersistenceTargets.cmake": "",
        f"{library_directory}/cmake/hgraph-persistence/hgraph_persistence_arrow.cmake": "",
        "hgraph_persistence-0.0.0.dist-info/METADATA": (
            "Metadata-Version: 2.2\n"
            "Name: hgraph-persistence\n"
            "Version: 0.0.0\n"
            "Requires-Dist: hgraph\n"
        ),
    }
    with zipfile.ZipFile(wheel, "w") as archive:
        for name, contents in files.items():
            archive.writestr(name, contents)

    subprocess.run([sys.executable, AUDIT_SCRIPT, wheel], check=True)
