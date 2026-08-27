"""The GlobalState thread-local lives only inside its scope.

It is a compatibility surface. Nothing may install it lazily: an accessor that
did made its own side effect the lifecycle, so the state outlived every wiring
that used it and was still there at interpreter exit, where its native handles
were destroyed against freed metadata (issue #505).
"""
import pytest

import hgraph as hg
from hgraph import TS, pass_through
from hgraph._wiring import _state
from hgraph.test import eval_node


def _thread_local_has_state():
    return getattr(_state._global_state_local, "state", None) is not None


def test_instance_raises_without_a_scope():
    assert not _thread_local_has_state()
    with pytest.raises(RuntimeError, match="no active GlobalState"):
        hg.GlobalState.instance()


def test_the_error_points_at_the_injectable():
    with pytest.raises(RuntimeError) as info:
        hg.GlobalState.instance()
    message = str(info.value)
    assert "injectable" in message
    assert "GlobalState = None" in message


def test_wiring_does_not_leave_the_thread_local_behind():
    assert not _thread_local_has_state()
    assert eval_node(pass_through, [1], resolution_dict={"ts": TS[int]}) == [1]
    assert not _thread_local_has_state(), (
        "the runner's scope guard must remove the state it opened"
    )


def test_a_caller_scope_survives_the_run_and_is_removed_at_exit():
    assert not _thread_local_has_state()
    with hg.GlobalState() as state:
        state["marker"] = 1
        assert eval_node(pass_through, [1], resolution_dict={"ts": TS[int]}) == [1]
        # The runner defers to the caller's scope rather than opening its own,
        # so values written before the run are still readable after it.
        assert hg.GlobalState.instance()["marker"] == 1
    assert not _thread_local_has_state()


def test_a_failed_wiring_still_removes_the_scope():
    assert not _thread_local_has_state()

    @hg.graph
    def broken(ts: TS[int]) -> TS[int]:
        raise ValueError("boom")

    with pytest.raises(Exception):
        eval_node(broken, [1])
    assert not _thread_local_has_state(), (
        "the guard must close on the error path too"
    )
