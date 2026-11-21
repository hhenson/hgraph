import pytest

from hgraph import MIN_DT, MIN_ST, MIN_TD, EvaluationClock, SCHEDULER, STATE, TS, compute_node, eval_node


def test_many_pending_alarms_fire_in_order():
    @compute_node
    def alarms(count: TS[int], _scheduler: SCHEDULER = None, _state: STATE = None) -> TS[int]:
        if count.modified:
            _state.fired = 0
            for i in range(1, count.value + 1):
                _scheduler.schedule(MIN_TD * i)
            return 0
        if _scheduler.is_scheduled_now:
            _state.fired += 1
            return _state.fired

    assert eval_node(alarms, [1024]) == list(range(1025))


@pytest.mark.parametrize("cancel", ["none", "next", "tag"])
def test_current_wake_and_future_times_are_observed_independently(cancel):
    @compute_node
    def wakes(pulse: TS[int], cancel: str, _scheduler: SCHEDULER = None,
              _clock: EvaluationClock = None) -> TS[tuple[bool, bool, int, bool]]:
        offset = (_clock.evaluation_time - MIN_ST) // MIN_TD
        if offset == 0:
            _scheduler.schedule(MIN_TD)
            _scheduler.schedule(3 * MIN_TD, "third")
            _scheduler.schedule(5 * MIN_TD)
        if offset == 1 and cancel == "next":
            _scheduler.un_schedule()
        if offset == 1 and cancel == "tag":
            _scheduler.un_schedule("third")
        next_time = _scheduler.next_scheduled_time
        return (_scheduler.is_scheduled_now, _scheduler.is_scheduled,
                -1 if next_time == MIN_DT else (next_time - MIN_ST) // MIN_TD, pulse.modified)

    # A pulse at +3 coincides with the middle wake or arrives on its own after cancellation.
    assert eval_node(wakes, [0, None, None, 3], cancel) == [
        (False, True, 1, True), (True, True, 3 if cancel == "none" else 5, False), None,
        (cancel == "none", True, 5, True), None, (True, False, -1, False),
    ]
