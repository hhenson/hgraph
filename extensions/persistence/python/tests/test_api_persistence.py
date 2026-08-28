"""Durable record/replay API behaviours (moved with hgraph-persistence,
RFC 0025 checkpoint 4)."""

import pyarrow as pa

import hgraph as hg
from hgraph import TS, eval_node, graph


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def test_per_call_model_selection_loads_the_extension():
    # A per-call ``model=`` is an extension load point exactly like the
    # graph-level setter (RFC 0025): in a fresh process that never imports
    # hgraph_persistence, the documented ``hg.record(..., model=hg.DATA_FRAME)``
    # flow must register the durable overloads itself. Run in a clean
    # interpreter so this suite's own imports cannot mask the load.
    import os
    import subprocess
    import sys

    script = (
        "import sys\n"
        # Editable-install finders would shadow PYTHONPATH with a different
        # checkout's package; drop them first (a no-op in CI).
        "sys.meta_path = [finder for finder in sys.meta_path"
        " if 'ScikitBuild' not in type(finder).__name__]\n"
        "import hgraph as hg\n"
        "from hgraph import TS, GlobalState, MIN_ST, MIN_TD, set_as_of\n"
        "from hgraph.test import eval_node\n"
        "assert 'hgraph_persistence' not in sys.modules\n"
        "with GlobalState():\n"
        "    set_as_of(MIN_ST + MIN_TD * 30)\n"
        "    eval_node(hg.record[TS[int]], ts=[1, 2, 3], key='ts',\n"
        "              recordable_id='percall', model=hg.DATA_FRAME)\n"
        "    assert 'hgraph_persistence' in sys.modules, 'selection did not load'\n"
        "    import hgraph_persistence\n"
        "    assert hgraph_persistence.frame_store_contains('percall.ts')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_per_call_model_on_replay_const_loads_the_extension():
    # replay_const's durable overload accepts a per-call ``model=`` (the
    # operator has no in-memory implementation), so that call shape is a load
    # point exactly like record/replay/compare's (PR #507 review finding).
    import os
    import subprocess
    import sys

    script = (
        "import sys\n"
        "sys.meta_path = [finder for finder in sys.meta_path"
        " if 'ScikitBuild' not in type(finder).__name__]\n"
        "import hgraph as hg\n"
        "from hgraph import TS\n"
        "assert 'hgraph_persistence' not in sys.modules\n"
        "try:\n"
        "    hg.replay_const[TS[int]](key='price', model=hg.DATA_FRAME)\n"
        "except RuntimeError:\n"
        "    pass  # no active wiring — the load fires before wiring begins\n"
        "assert 'hgraph_persistence' in sys.modules, 'selection did not load'\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_unloaded_extension_wiring_failure_names_the_activation_paths():
    # With hgraph-persistence INSTALLED but never loaded, a durable operator's
    # wiring failure explains that overloads register on backend selection
    # (RFC 0025's "wiring diagnoses the missing backend") rather than leaving
    # an unexplained resolution failure. Clean interpreter: this suite's own
    # imports must not mask the unloaded state.
    import os
    import subprocess
    import sys

    script = (
        "import sys\n"
        "sys.meta_path = [finder for finder in sys.meta_path"
        " if 'ScikitBuild' not in type(finder).__name__]\n"
        "import hgraph as hg\n"
        "from hgraph import TS, GlobalState\n"
        "from hgraph.test import eval_node\n"
        "assert 'hgraph_persistence' not in sys.modules\n"
        "@hg.graph\n"
        "def g() -> TS[int]:\n"
        "    return hg.replay_const[TS[int]](key='price')\n"
        "try:\n"
        "    with GlobalState():\n"
        "        eval_node(g)\n"
        "except Exception as error:\n"
        "    assert 'installed but not loaded' in str(error), str(error)\n"
        "    assert 'set_record_replay_config' in str(error), str(error)\n"
        "else:\n"
        "    raise AssertionError('replay_const wired without activation')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_durable_replay_overload_registers_partition_signature():
    # Moved from core's test_native_docstrings when the partitioned frame
    # replay overload became extension-registered (RFC 0025 checkpoint 4).
    import _hgraph

    import hgraph_persistence  # noqa: F401  (registers the frame overloads)

    replay_overloads = _hgraph.operator_overload_signatures("replay")
    assert any(
        ("partition_names", False, "tuple[str, ...]", True) in parameters
        and ("removed_names", False, "tuple[str, ...]", True) in parameters
        for parameters, *_ in replay_overloads
    )


def test_component_record_replay_modes():
    # One state spans the whole test: it configures record/replay and then
    # reads recordings back between runs, so both must see the same state.
    with hg.GlobalState():
        hg.set_record_replay_config(hg.DATA_FRAME)
        M = hg.RecordReplayEnum

        @hg.component
        def calc(lhs: TS[int], rhs: TS[int]) -> TS[int]:
            return lhs + rhs

        @graph
        def recording(a: TS[int], b: TS[int]) -> TS[int]:
            with hg.record_replay_scope(M.RECORD):
                return calc(a, b)

        out = eval_node(recording, [1, None, 3], [10, 20, None])
        check(out == [11, 21, 23], f"record: {out}")
        for key in ("calc.lhs", "calc.rhs", "calc.__out__"):
            check(hg.frame_store_contains(key), f"missing frame {key}")

        @graph
        def replaying(a: TS[int], b: TS[int]) -> TS[int]:
            with hg.record_replay_scope(M.REPLAY):
                return calc(a, b)

        # The recordings win over garbage live inputs.
        out = eval_node(replaying, [100, 100, 100], [100, 100, 100])
        check(out == [11, 21, 23], f"replay: {out}")

        @graph
        def comparing(a: TS[int], b: TS[int]) -> TS[int]:
            with hg.record_replay_scope(M.COMPARE):
                return calc(a, b)

        eval_node(comparing, [100, 100, 100], [100, 100, 100])
        check(hg.comparison_summary("calc.__compare__") == (3, 0), "compare clean")

        @graph
        def recovering(a: TS[int], b: TS[int]) -> TS[int]:
            with hg.record_replay_scope(M.RECOVER):
                return calc(a, b)

        # Seeded from the recordings at start (1+10), live overrides (100+10).
        out = eval_node(recovering, [None, 100], [None, None])
        check(out == [11, 110], f"recover: {out}")

        hg.set_record_replay_config(hg.IN_MEMORY)




def test_frame_pyarrow_round_trip():
    # One state spans the whole test: it configures record/replay and then
    # reads recordings back between runs, so both must see the same state.
    with hg.GlobalState():
        # Frames cross the boundary as pyarrow.Tables (the Arrow C stream
        # protocol - zero copy): store reads return Tables, and Tables convert
        # back to Frame values.
        import pyarrow as pa

        hg.set_record_replay_config(hg.DATA_FRAME)

        @hg.component
        def snap(x: TS[int]) -> TS[int]:
            return x + x

        @graph
        def recording(x: TS[int]) -> TS[int]:
            with hg.record_replay_scope(hg.RecordReplayEnum.RECORD):
                return snap(x)

        eval_node(recording, [1, 2, 3])
        table = hg.frame_store_read("snap.__out__")
        check(isinstance(table, pa.Table), f"expected a pyarrow.Table, got {type(table)}")
        check(table.column("value").to_pylist() == [2, 4, 6], f"values: {table.to_pydict()}")
        check(table.num_columns == 3, "bitemporal columns present")
        hg.set_record_replay_config(hg.IN_MEMORY)


