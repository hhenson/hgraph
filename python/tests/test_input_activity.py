"""Instance-level input activity (theme-A ruling 2026-08-01): make_active /
make_passive / active must exist and work on runtime input views; the
annotation-class ABC surface is deliberately not replicated."""

from hgraph import TS, compute_node, graph
from hgraph.test import eval_node

_states: list[bool] = []


@compute_node
def toggle(trigger: TS[int], data: TS[int]) -> TS[int]:
    _states.append(data.active)
    if trigger.value % 2 == 0:
        data.make_passive()
    else:
        data.make_active()
    return trigger.value


def test_active_state_tracks_make_active_passive():
    _states.clear()

    @graph
    def g(trigger: TS[int], data: TS[int]) -> TS[int]:
        return toggle(trigger, data)

    # trigger drives; data stays quiet so only activity state changes.
    assert eval_node(g, [2, 3, 4], [0, None, None]) == [2, 3, 4]
    # initially active; passivated by the first tick; reactivated by the second.
    assert _states == [True, False, True]
