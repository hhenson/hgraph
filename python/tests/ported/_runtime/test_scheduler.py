# Ported from release/0.5:hgraph_unit_tests/_runtime/test_scheduler.py.
# SCHEDULER.schedule(datetime|timedelta, tag) and record(ts) default
# key both landed 2026-07-18. The wall-clock record path now selects the
# SPARSE ``:memory:`` recorder under the default IN_MEMORY model (the
# IN_MEMORY_DENSE split, 2026-07-18), so the large cross-cycle real-time
# gap records correctly.
import time
from datetime import timedelta, datetime

from frozendict import frozendict

from hgraph import (
    EvaluationClock,
    compute_node,
    TS,
    SCHEDULER,
    MIN_TD,
    graph,
    TSD,
    map_,
    EvaluationMode,
    record,
    GlobalState,
    get_recorded_value,
    sink_node,
    SIGNAL,
    schedule,
    evaluate_graph,
    GraphConfiguration, run_graph, sample,
    utc_now,
    stop_engine,
)
from hgraph import const
from hgraph.test import eval_node


import pytest

@compute_node
def my_scheduler(ts: TS[int], tag: str = None, _scheduler: SCHEDULER = None) -> TS[int]:
    if ts.modified:
        _scheduler.schedule(MIN_TD * ts.value, tag)
        return ts.value
    if _scheduler.is_scheduled_now:
        return -1


@pytest.mark.smoke
def test_scheduler():
    assert eval_node(my_scheduler, [2, 3]) == [2, 3, -1, None, -1]

@pytest.mark.smoke
def test_map_scheduler():
    @graph
    def map_scheduler(tsd: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return map_(my_scheduler, tsd, "TEST")

    assert eval_node(map_scheduler, [{"ab1": 9, "ab2": 9}, {"ab1": 9}, {"ab2": 9}] + [None] * 7 + [{"ab1": 2}]) == [
        frozendict({"ab1": 9, "ab2": 9}),
        frozendict({"ab1": 9}),
        frozendict({"ab2": 9}),
    ] + [None] * 7 + [frozendict({"ab1": 2}), frozendict({"ab2": -1}), {"ab1": -1}]


@compute_node(valid=("ts1",))
def schedule_bool(ts: TS[bool], ts1: TS[int], _scheduler: SCHEDULER = None) -> TS[bool]:
    if ts.modified or ts1.modified:
        _scheduler.schedule(timedelta(microseconds=ts1.value), "TAG")
        if ts.modified:
            return True
    elif _scheduler.is_scheduled_now:
        return False


@pytest.mark.smoke
def test_tagged_scheduler():
    @graph
    def _schedule_graph(ts: TSD[str, TS[bool]]) -> TSD[str, TS[bool]]:
        config = const(frozendict({"a": 10, "b": 3}), TSD[str, TS[int]])
        return map_(schedule_bool, ts, config)

    d = frozendict
    assert eval_node(_schedule_graph, [None, None, None, None, None, {"b": True}]) == [
        d(),
        None,
        None,
        d({"b": False}),
        None,
        d({"b": True}),
        None,
        None,
        d({"b": False}),
        None,
        d({"a": False}),
    ]


@compute_node
def my_scheduler_realtime(ts: TS[int], tag: str = None, _scheduler: SCHEDULER = None, _clock: EvaluationClock = None) -> TS[tuple[int, datetime]]:
    if ts.modified:
        _scheduler.schedule(MIN_TD * ts.value, tag, on_wall_clock=True)
        return ts.value, _clock.now
    if _scheduler.is_scheduled_now:
        _scheduler.schedule(MIN_TD * ts.value, tag, on_wall_clock=True)
        return -1, _clock.now


@sink_node
def sleep(s: SIGNAL, seconds: float):
    time.sleep(seconds)


@compute_node
def alarm_fired(ts: TS[tuple[int, datetime]]) -> SIGNAL:
    """Tick once the rescheduled alarm has fired (``my_scheduler_realtime``
    reports the firing as -1).

    The wall-clock tests below END on this rather than on their ``end_time``.
    Racing a fixed deadline is what made them flaky: the alarm lands ~150ms in,
    the run was cut off at 350ms, and a loaded runner spends that margin on
    something else - so the failure reported "the runner was slow", not "the
    scheduler is wrong". Loosening the deadline only moves the load at which
    that recurs, which is why it had already been raised twice. Ending on the
    observation makes the deadline a BACKSTOP: the test now fails only if the
    alarm never fires, which is the bug it exists to catch.
    """
    if ts.value[0] == -1:
        return True
    
def test_wall_clock_scheduler():
    @graph
    def g():
        out = my_scheduler_realtime(100000, "TAG")
        record(out)
        my_scheduler_realtime(10000, "TAG")  # to make sure different alarms do not interfere
        sleep(schedule(timedelta(milliseconds=7), initial_delay=True), 0.01)
        # End on the alarm, not on the clock - see alarm_fired.
        stop_engine(alarm_fired(out))

    now = utc_now()
    with GlobalState():
        # A BACKSTOP, not the measurement - see the reschedule test below.
        config = GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, start_time=now,
                                    end_time=now + timedelta(seconds=5))
        evaluate_graph(g, config)
        values = get_recorded_value()

    assert [v[1][0] for v in values][:2] == [100000, -1]
    assert values[0][0] == now
    # Lower bound only: the deliberate 10ms sleep per 7ms schedule accumulates
    # ~42.8ms of lag, and the alarm must not pre-empt it. How much LATER it
    # lands is runner load, not scheduler behaviour.
    assert values[1][0] >= now + timedelta(milliseconds=42)
    
    
def test_wall_clock_scheduler_reschedule():
    @graph
    def g():
        out = my_scheduler_realtime(
            sample(schedule(timedelta(milliseconds=50), initial_delay=False, max_ticks=2), 100000), "TAG")
        record(out)
        # End on the alarm, not on the clock - see alarm_fired.
        stop_engine(alarm_fired(out))

    now = utc_now()
    with GlobalState():
        # A BACKSTOP, not the measurement: the run normally ends ~150ms in, when
        # the alarm fires. Generous enough that no plausible runner load reaches
        # it, so hitting it means the alarm genuinely never fired.
        run_graph(g, run_mode=EvaluationMode.REAL_TIME, start_time=now, end_time=now + timedelta(seconds=5))
        values = get_recorded_value()

    assert [v[1][0] for v in values][:3] == [100000, 100000, -1]
    assert values[0][0] == now
    # Only LOWER bounds are asserted. An alarm firing early is a scheduler bug;
    # an alarm firing late is a statement about how loaded the runner is, and
    # asserting it tests the CI machine rather than hgraph. That is what these
    # upper bounds were doing - they had already been loosened twice (60ms ->
    # 250ms, +110ms -> +300ms) without ever becoming reliable. "Not absurdly
    # late" is still covered, by the run's backstop end_time.
    assert values[1][0] >= now + timedelta(milliseconds=50)
    assert values[2][1][1] >= values[1][1][1] + timedelta(milliseconds=90)
