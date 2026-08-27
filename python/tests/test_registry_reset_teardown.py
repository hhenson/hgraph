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


def test_reset_twice_then_exit_does_not_crash():
    """A second reset must not resurrect the hazard through a re-cached state."""
    source = RESET_THEN_EXIT.replace(
        '_hgraph.reset_registries()\n    print("ok")',
        '_hgraph.reset_registries()\n'
        '    assert eval_node(pass_through, [2], resolution_dict={"ts": TS[int]}) == [2]\n'
        '    _hgraph.reset_registries()\n'
        '    print("ok")',
    )
    result = _run(source)
    assert result.returncode == 0, (
        f"double reset + exit returned {result.returncode}\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )


def test_reset_hook_is_registered_for_the_thread_local_state():
    """The cache is cleared by a registered hook, not by luck of access order."""
    import _hgraph
    from hgraph._wiring import _state

    assert _state._clear_thread_local_state in list(_hgraph._reset_hooks)
