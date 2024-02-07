from datetime import timedelta

from frozendict import frozendict

from hgraph import compute_node, TS, SCHEDULER, MIN_TD, graph, TSD, map_
from hgraph.nodes import const
from hgraph.test import eval_node


@compute_node
def my_scheduler(ts: TS[int], tag: str = None, _scheduler: SCHEDULER = None) -> TS[int]:
    if ts.modified:
        _scheduler.schedule(MIN_TD * ts.value, tag)
        return ts.value
    if _scheduler.is_scheduled_now:
        return -1


def test_scheduler():
    assert eval_node(my_scheduler, [2, 3]) == [2, 3, -1, None, -1]


def test_map_scheduler():
    @graph
    def map_scheduler(tsd: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return map_(my_scheduler, tsd, "TEST")

    assert (eval_node(map_scheduler, [{"ab1": 9, "ab2": 9}, {"ab1": 9}, {"ab2": 9}]) == [
        frozendict({"ab1": 9, "ab2": 9}), frozendict({"ab1": 9}), frozendict({"ab2": 9}), ] + [None] * 7 +
            [frozendict({"ab1": -1}), frozendict({"ab2": -1})])


@compute_node(valid=("ts1",))
def schedule_bool(ts: TS[bool], ts1: TS[int], _scheduler: SCHEDULER = None) -> TS[bool]:
    if ts.modified or ts1.modified:
        _scheduler.schedule(timedelta(microseconds=ts1.value), "TAG")
        if ts.modified:
            return True
    elif _scheduler.is_scheduled_now:
        return False


def test_tagged_scheduler():
    @graph
    def _schedule_graph(ts: TSD[str, TS[bool]]) -> TSD[str, TS[bool]]:
        config = const(frozendict({"a": 10, "b": 3}), TSD[str, TS[int]])
        return map_(schedule_bool, ts, config)

    d = frozendict
    assert eval_node(_schedule_graph,
                     [None, None, None, None, None, {"b": True}]) == [
               d(), None, None, d({"b": False}), None, d({"b": True}), None, None,
               d({"b": False}), None, d({"a": False})]
