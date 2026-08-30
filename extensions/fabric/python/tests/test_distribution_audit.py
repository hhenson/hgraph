from __future__ import annotations

import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

import pytest

FABRIC_ROOT = Path(__file__).resolve().parents[2]
AUDIT_SCRIPT = FABRIC_ROOT / "tools/audit_distribution.py"


def test_fabric_wheel_build_is_warning_clean_and_outside_temp():
    project = tomllib.loads((FABRIC_ROOT / "pyproject.toml").read_text())
    scikit_build = project["tool"]["scikit-build"]

    assert scikit_build["build-dir"] == "cmake-build-wheel/{wheel_tag}"
    assert "-DHGRAPH_FABRIC_WARNINGS_AS_ERRORS=ON" in scikit_build["cmake"][
        "args"
    ]


@pytest.mark.parametrize("library_directory", ["lib", "lib64"])
def test_wheel_audit_accepts_platform_library_directories(
    tmp_path: Path, library_directory: str
):
    wheel = tmp_path / "hgraph_fabric-0.0.0-cp312-abi3-any.whl"
    files = {
        "hgraph_fabric/__init__.py": "",
        "hgraph_fabric/py.typed": "",
        "hgraph_fabric/_hgraph_fabric.abi3.so": "",
        "include/hgraph/fabric/config.h": "",
        "include/hgraph/fabric/export.h": "",
        "include/hgraph/fabric/fabric.h": "",
        "include/hgraph/fabric/history.h": "",
        "include/hgraph/fabric/kafka.h": "",
        "include/hgraph/fabric/kafka_export.h": "",
        "include/hgraph/fabric/keys.h": "",
        "include/hgraph/fabric/metadata_codec.h": "",
        "include/hgraph/fabric/notifier.h": "",
        "include/hgraph/fabric/operators.h": "",
        "include/hgraph/fabric/planning.h": "",
        "include/hgraph/fabric/publication.h": "",
        "include/hgraph/fabric/resolution.h": "",
        "include/hgraph/fabric/service.h": "",
        "include/hgraph/fabric/types.h": "",
        "include/hgraph/fabric/value_builders.h": "",
        f"{library_directory}/libhgraph_fabric.a": "",
        f"{library_directory}/cmake/hgraph-fabric/hgraph-fabricConfig.cmake": "",
        f"{library_directory}/cmake/hgraph-fabric/hgraphFabricTargets.cmake": "",
        "hgraph_fabric-0.0.0.dist-info/METADATA": (
            "Metadata-Version: 2.2\n"
            "Name: hgraph-fabric\n"
            "Version: 0.0.0\n"
            "Requires-Dist: hgraph\n"
            "Requires-Dist: hgraph-persistence\n"
        ),
    }
    with zipfile.ZipFile(wheel, "w") as archive:
        for name, contents in files.items():
            archive.writestr(name, contents)

    subprocess.run([sys.executable, AUDIT_SCRIPT, wheel], check=True)
