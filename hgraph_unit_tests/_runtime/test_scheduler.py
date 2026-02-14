import time
from datetime import timedelta, datetime

from frozendict import frozendict

from ace.adaptors.perspective.user_table import sample
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
    GraphConfiguration,
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
    
def test_wall_clock_scheduler():
    @graph
    def g():
        record(my_scheduler_realtime(100000, "TAG"))
        my_scheduler_realtime(10000, "TAG")  # to make sure different alarms do not interfere
        sleep(schedule(timedelta(milliseconds=7), initial_delay=True), 0.01)

    now = datetime.utcnow()
    with GlobalState():
        config = GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, start_time=now,
                                    end_time=now + timedelta(milliseconds=250))
        evaluate_graph(g, config)
        values = get_recorded_value()

    assert [v[1][0] for v in values][:2] == [100000, -1]
    assert values[0][0] == now
    assert values[1][0] >= now + timedelta(milliseconds=42)  # we will expect to accumulate 100/7*3 = 42.8ms lag
    assert values[1][0] < now + timedelta(milliseconds=107)
    
    
def test_wall_clock_scheduler_reschedule():
    @graph
    def g():
        record(my_scheduler_realtime(sample(schedule(timedelta(milliseconds=50), initial_delay=False, max_ticks=2), 100000), "TAG"))

    now = datetime.utcnow()
    with GlobalState():
        run_graph(g, run_mode=EvaluationMode.REAL_TIME, start_time=now, end_time=now + timedelta(milliseconds=350), __trace__=True)
        values = get_recorded_value()

    assert [v[1][0] for v in values][:3] == [100000, 100000, -1]
    assert values[0][0] == now
    assert values[1][0] >= now + timedelta(milliseconds=50)  # we will expect to accumulate 100/7*3 = 42.8ms lag
    assert values[1][0] < now + timedelta(milliseconds=60)
    assert values[2][1][1] >= values[1][1][1] + timedelta(milliseconds=90)  # we will expect to accumulate 100/7*3 = 42.8ms lag
    assert values[2][1][1] < values[1][1][1] + timedelta(milliseconds=110)
