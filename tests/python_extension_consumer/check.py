"""Build and import a nanobind extension against the installed wheel SDK."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import tempfile


SOURCE_DIR = Path(__file__).resolve().parent


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="hgraph-python-extension-consumer-") as directory:
        build_dir = Path(directory)
        configure = [
            "cmake",
            "-S",
            str(SOURCE_DIR),
            "-B",
            str(build_dir),
            "-DCMAKE_BUILD_TYPE=Release",
            f"-DPython_EXECUTABLE={sys.executable}",
        ]
        if sys.platform == "win32":
            # The Windows wheel and its import libraries are built with MSVC;
            # a MinGW consumer would require incompatible .dll.a files.
            configure.extend(["-G", "Visual Studio 18 2026", "-A", "x64"])
        else:
            configure.extend(["-G", "Ninja"])
        if prefix := os.environ.get("HGRAPH_CMAKE_PREFIX"):
            configure.append(f"-DCMAKE_PREFIX_PATH={prefix}")
        subprocess.run(
            configure,
            check=True,
        )
        build = ["cmake", "--build", str(build_dir), "--parallel", "2"]
        if sys.platform == "win32":
            build.extend(["--config", "Release"])
        subprocess.run(
            build,
            check=True,
        )

        module_dir = build_dir / "Release" if sys.platform == "win32" else build_dir
        check = subprocess.run(
            [
                sys.executable,
                "-c",
                """
import importlib
import sys

import _hgraph  # Load the wheel's shared runtime first.
import hgraph
from hgraph import TS, pass_through, register_native_scalar_type
from hgraph.reflection import scalar_type
from hgraph.test import eval_node

sys.path.insert(0, sys.argv[1])
consumer = importlib.import_module("_hgraph_consumer")
address = consumer.registry_address()
if not isinstance(address, int) or address == 0:
    raise RuntimeError("downstream extension returned an invalid registry address")

# The extension registered its Python class and native scalar through public
# installed C++ headers. Python annotations and reverse reflection use that
# same process-wide association.
assert repr(TS[consumer.ConsumerScalar].handle) == (
    "TS[hgraph.test.consumer_scalar]"
)
assert scalar_type(TS[consumer.ConsumerScalar]) is consumer.ConsumerScalar

value = consumer.ConsumerScalar(42)
result = eval_node(
    pass_through,
    [value],
    resolution_dict={"ts": TS[consumer.ConsumerScalar]},
)
assert result == [value]

# Repeating the same pair is harmless. Conflicts on either side fail.
register_native_scalar_type(
    consumer.ConsumerScalar, "hgraph.test.consumer_scalar"
)
class OtherConsumerScalar:
    pass
try:
    register_native_scalar_type(consumer.ConsumerScalar, "int")
except ValueError:
    pass
else:
    raise RuntimeError("conflicting Python-class registration was accepted")
try:
    register_native_scalar_type(
        OtherConsumerScalar, "hgraph.test.consumer_scalar"
    )
except ValueError:
    pass
else:
    raise RuntimeError("conflicting native-schema registration was accepted")
try:
    register_native_scalar_type(OtherConsumerScalar, "int")
except ValueError:
    pass
else:
    raise RuntimeError("built-in native-schema registration was replaced")

print(f"installed Python extension consumer passed: registry={address:#x}")
""",
                str(module_dir),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        print(check.stdout, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
