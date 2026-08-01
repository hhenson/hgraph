"""One-shot evaluation notifications (theme-C ruling 2026-08-01):
add_before/after_evaluation_notification fire exactly once at the next
root-cycle boundary; lifecycle observers themselves stay C++-only."""

from hgraph import TS, EvaluationEngineApi, compute_node, graph
from hgraph.test import eval_node

_events: list[str] = []


@compute_node
def observe(t: TS[int], _api: EvaluationEngineApi = None) -> TS[int]:
    tick = t.value
    _events.append(f"eval-{tick}")
    _api.add_after_evaluation_notification(lambda: _events.append(f"after-{tick}"))
    _api.add_before_evaluation_notification(lambda: _events.append(f"before-next-{tick}"))
    return tick


def test_notifications_fire_once_at_cycle_boundaries():
    _events.clear()

    @graph
    def g(t: TS[int]) -> TS[int]:
        return observe(t)

    assert eval_node(g, [1, 2]) == [1, 2]
    # after-N fires at the end of N's cycle; before-next-N at the start of
    # the following cycle; each exactly once.
    assert _events == [
        "eval-1", "after-1",
        "before-next-1", "eval-2", "after-2",
    ] or _events == [
        "eval-1", "after-1",
        "before-next-1", "eval-2", "after-2", "before-next-2",
    ]
