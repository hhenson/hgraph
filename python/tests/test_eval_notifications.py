"""One-shot evaluation notifications (theme-C ruling 2026-08-01):
add_before/after_evaluation_notification fire exactly once at the next
root-cycle boundary; lifecycle observers themselves stay C++-only."""

import pytest

from hgraph import TS, EvaluationEngineApi, compute_node, graph, sink_node
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
    # Clean shutdown drains the final before queue too: deferred final-tick
    # cleanup must not be lost.
    assert _events == [
        "eval-1", "after-1",
        "before-next-1", "eval-2", "after-2", "before-next-2",
    ]


@compute_node
def register_reentrant_after(t: TS[int], _api: EvaluationEngineApi = None) -> TS[int]:
    _events.append("eval-order")

    def first():
        _events.append("after-first")
        _api.add_after_evaluation_notification(lambda: _events.append("nested-first"))

    def second():
        _events.append("after-second")
        _api.add_after_evaluation_notification(lambda: _events.append("nested-second"))

    _api.add_after_evaluation_notification(first)
    _api.add_after_evaluation_notification(second)
    return t.value


def test_after_notifications_are_lifo_and_drain_reentrant_work():
    _events.clear()

    assert eval_node(register_reentrant_after, [1]) == [1]
    assert _events == [
        "eval-order", "after-second", "after-first", "nested-first", "nested-second",
    ]


@compute_node
def register_reentrant_before(t: TS[int], _api: EvaluationEngineApi = None) -> TS[int]:
    tick = t.value
    _events.append(f"eval-before-{tick}")
    if tick == 1:
        def first():
            _events.append("before-first")
            _api.add_before_evaluation_notification(
                lambda: _events.append("nested-before-first"))

        def second():
            _events.append("before-second")
            _api.add_before_evaluation_notification(
                lambda: _events.append("nested-before-second"))

        _api.add_before_evaluation_notification(first)
        _api.add_before_evaluation_notification(second)
    return tick


def test_before_notifications_are_fifo_and_drain_reentrant_work():
    _events.clear()

    assert eval_node(register_reentrant_before, [1, 2]) == [1, 2]
    assert _events == [
        "eval-before-1", "before-first", "before-second", "nested-before-first",
        "nested-before-second", "eval-before-2",
    ]


@compute_node
def capture_runtime_view(t: TS[int], _api: EvaluationEngineApi = None) -> TS[int]:
    tick = t.value
    _events.append(f"eval-view-{tick}")
    _api.add_after_evaluation_notification(
        lambda: _events.append(f"after-view-{t.value}"))
    if tick == 1:
        _api.add_before_evaluation_notification(
            lambda: _events.append(f"before-view-{t.value}"))
    return tick


def test_notification_can_read_the_captured_runtime_view():
    _events.clear()

    assert eval_node(capture_runtime_view, [1, 2]) == [1, 2]
    assert _events == [
        "eval-view-1", "after-view-1", "before-view-1",
        "eval-view-2", "after-view-2",
    ]


@compute_node
def fail_after_registration(t: TS[int], _api: EvaluationEngineApi = None) -> TS[int]:
    _api.add_after_evaluation_notification(lambda: _events.append("after-error"))
    raise RuntimeError("notification test failure")


def test_failed_evaluation_still_drains_after_notifications():
    _events.clear()

    with pytest.raises(RuntimeError, match="notification test failure"):
        eval_node(fail_after_registration, [1])
    assert _events == ["after-error"]


@sink_node
def register_stop_notifications(t: TS[int]):
    pass


@register_stop_notifications.stop
def register_stop_notifications_on_stop(_api: EvaluationEngineApi = None):
    _api.add_after_evaluation_notification(lambda: _events.append("after-stop"))
    _api.add_before_evaluation_notification(lambda: _events.append("before-stop"))


def test_stop_generated_notifications_drain_before_executor_teardown():
    _events.clear()

    assert eval_node(register_stop_notifications, [1]) is None
    assert _events == ["after-stop", "before-stop"]
