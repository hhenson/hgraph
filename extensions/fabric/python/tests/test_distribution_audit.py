from __future__ import annotations

import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

FABRIC_ROOT = Path(__file__).resolve().parents[2]
AUDIT_SCRIPT = FABRIC_ROOT / "tools/audit_distribution.py"


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
