from __future__ import annotations

import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

ANALYTICS_ROOT = Path(__file__).resolve().parents[2]
AUDIT_SCRIPT = ANALYTICS_ROOT / "tools/audit_distribution.py"


@pytest.mark.parametrize("library_directory", ["lib", "lib64"])
def test_wheel_audit_accepts_platform_library_directories(
    tmp_path: Path, library_directory: str
):
    wheel = tmp_path / "hgraph_analytics-0.0.0-cp312-abi3-any.whl"
    files = {
        "hgraph_analytics/__init__.py": "",
        "hgraph_analytics/py.typed": "",
        "hgraph_analytics/_hgraph_analytics.abi3.so": "",
        "include/hgraph/analytics/operators.h": "",
        f"{library_directory}/libhgraph_analytics.a": "",
        f"{library_directory}/cmake/hgraph-analytics/hgraph-analyticsConfig.cmake": "",
        f"{library_directory}/cmake/hgraph-analytics/hgraphAnalyticsTargets.cmake": "",
        "hgraph_analytics-0.0.0.dist-info/METADATA": (
            "Metadata-Version: 2.2\n"
            "Name: hgraph-analytics\n"
            "Version: 0.0.0\n"
            "Requires-Dist: hgraph\n"
        ),
    }
    with zipfile.ZipFile(wheel, "w") as archive:
        for name, contents in files.items():
            archive.writestr(name, contents)

    subprocess.run([sys.executable, AUDIT_SCRIPT, wheel], check=True)
