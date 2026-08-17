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

# The extension also registered an external record/replay backend
# ("probe.mem") for the CORE operator markers through public installed
# headers (RFC 0025 checkpoint 3). Selecting its backend id resolves its
# overloads from unchanged hgraph imports, on the Python surface.
from hgraph import TS as _TS, graph, record, replay, set_record_replay_model

with hgraph.GlobalState() as _probe_state:
    set_record_replay_model("probe.mem")

    @graph
    def _probe_record(ts: _TS[int]):
        record(ts, key="probe_out")

    eval_node(_probe_record, [7, 8])
    assert _probe_state[":probe:probe_out.n"] == 2
    assert _probe_state[":probe:probe_out.0"] == 7
    assert _probe_state[":probe:probe_out.1"] == 8

    @graph
    def _probe_replay() -> _TS[int]:
        return replay("probe_out", _TS[int])

    assert eval_node(_probe_replay) == [7, 8]

# Registry reset invalidates the interned metadata every live object of the
# current generation points at (a documented test-only hazard), so nothing
# from this generation may survive it — not the probe GlobalState, and not
# the @graph objects whose decoration-time type handles would otherwise be
# destroyed against freed metadata at interpreter exit.
del _probe_state, _probe_record, _probe_replay
import gc as _gc
_gc.collect()

# reset_registries replays the extension installer exactly as core's —
# operator overloads AND the python scalar association alike.
_hgraph.reset_registries()
assert scalar_type(TS[consumer.ConsumerScalar]) is consumer.ConsumerScalar
assert repr(TS[consumer.ConsumerScalar].handle) == (
    "TS[hgraph.test.consumer_scalar]"
)
with hgraph.GlobalState() as _probe_state:
    set_record_replay_model("probe.mem")

    # Re-decorated after the reset: a @graph object captures resolved type
    # handles at decoration time, and the reset invalidated that generation.
    @graph
    def _probe_record_again(ts: _TS[int]):
        record(ts, key="probe_out")

    eval_node(_probe_record_again, [9])
    assert _probe_state[":probe:probe_out.0"] == 9

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
