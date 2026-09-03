"""A reset followed by ordinary interpreter exit must not crash (issue #505).

The failure is a dangling call during the final GC: a nanobind-wrapped native
held by a Python-side cache outlives the reset, and its destructor dispatches
through a plan the reset freed. Linux segfaults; macOS's allocator keeps the
pages mapped and the same UB passes silently, so this test is only meaningful
as a subprocess exit-code assertion -- an in-process check cannot observe it.
"""
import os
import subprocess
import sys
import textwrap

RESET_THEN_EXIT = textwrap.dedent(
    """
    import faulthandler; faulthandler.enable()
    import _hgraph
    from hgraph import TS, pass_through
    from hgraph.test import eval_node

    assert eval_node(pass_through, [1], resolution_dict={"ts": TS[int]}) == [1]
    _hgraph.reset_registries()
    print("ok")
    # NB: no os._exit() -- ordinary interpreter teardown is the thing under test.
    """
)


def _run(source):
    # Hand the child this process's sys.path so it exercises the hgraph under
    # test rather than whatever happens to be installed in the interpreter's
    # site-packages -- otherwise a stale installed extension turns a crash
    # assertion into an unrelated ImportError.
    env = dict(os.environ, PYTHONPATH=os.pathsep.join(p for p in sys.path if p))
    return subprocess.run(
        [sys.executable, "-c", source], capture_output=True, text=True, env=env
    )


def test_reset_then_ordinary_exit_does_not_crash():
    result = _run(RESET_THEN_EXIT)
    assert result.returncode == 0, (
        f"reset + exit returned {result.returncode}\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert "ok" in result.stdout


RESET_TWICE_THEN_EXIT = textwrap.dedent(
    """
    import faulthandler; faulthandler.enable()
    import _hgraph
    from hgraph import TS, pass_through
    from hgraph.test import eval_node

    assert eval_node(pass_through, [1], resolution_dict={"ts": TS[int]}) == [1]
    _hgraph.reset_registries()
    # Wire again so the second reset has a freshly cached generation to free.
    assert eval_node(pass_through, [2], resolution_dict={"ts": TS[int]}) == [2]
    _hgraph.reset_registries()
    print("ok")
    """
)


def test_reset_twice_then_exit_does_not_crash():
    """A second reset must not resurrect the hazard through a re-cached state.

    Written as its own script rather than patched into the first: RESET_THEN_EXIT
    is already dedented, so a replacement keyed on indented source silently
    matched nothing and this ran a single reset.
    """
    result = _run(RESET_TWICE_THEN_EXIT)
    assert result.returncode == 0, (
        f"double reset + exit returned {result.returncode}\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )


STALE_HIGHER_ORDER_CACHE = textwrap.dedent(
    """
    import _hgraph
    import hgraph as hg
    from hgraph._wiring._graph import _as_wired

    @hg.graph
    def increment(value: hg.TS[int]) -> hg.TS[int]:
        return value + 1

    @hg.compute_node
    def increment_node(value: hg.TS[int]) -> hg.TS[int]:
        return value.value + 1

    _as_wired(increment)
    _as_wired(increment_node)
    _hgraph.reset_registries()
    for callable_ in (increment, increment_node):
        try:
            _as_wired(callable_)
        except hg.WiringError as error:
            assert "recreate the decorated callable" in str(error)
        else:
            raise AssertionError("stale higher-order callable was reused")
    print("ok")
    """
)


def test_reset_rejects_stale_higher_order_cache_without_crashing():
    result = _run(STALE_HIGHER_ORDER_CACHE)
    assert result.returncode == 0, (
        f"reset + cached callable returned {result.returncode}\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert "ok" in result.stdout


NO_STRANDED_STATE = textwrap.dedent(
    """
    import _hgraph
    from hgraph import TS, pass_through
    from hgraph.test import eval_node
    from hgraph._wiring import _state

    def cached():
        return getattr(_state._global_state_local, "state", None)

    assert eval_node(pass_through, [1], resolution_dict={"ts": TS[int]}) == [1]
    assert cached() is None, "wiring left a state on the thread-local"
    _hgraph.reset_registries()
    assert cached() is None, "the reset left a state on the thread-local"
    print("ok")
    """
)


def test_a_reset_after_wiring_has_no_cached_state_to_strand():
    """The reason the crash cannot happen: there is nothing left to free.

    The wrapper collected against freed metadata at exit was a GlobalState
    cached on the thread-local by a lazy accessor. Wiring no longer leaves one
    behind, so a reset has nothing of the previous generation to strand.

    A subprocess, like its siblings: reset_registries() is a process-wide,
    test-only teardown, and calling it in-process detonates every test that
    runs after it in the same session.
    """
    result = _run(NO_STRANDED_STATE)
    assert result.returncode == 0, (
        f"returned {result.returncode}\nstdout: {result.stdout}\n"
        f"stderr: {result.stderr}"
    )
    assert "ok" in result.stdout
