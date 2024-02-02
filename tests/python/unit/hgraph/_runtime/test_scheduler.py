from frozendict import frozendict

from hgraph import compute_node, TS, SCHEDULER, MIN_TD, graph, TSD, map_
from hgraph.test import eval_node


@compute_node
def my_scheduler(ts: TS[int], tag: str = None, _scheduler: SCHEDULER = None) -> TS[int]:
    if ts.modified:
        _scheduler.schedule(MIN_TD*ts.value,tag)
        return ts.value
    if _scheduler.is_scheduled_now:
        return -1


def test_scheduler():
    assert eval_node(my_scheduler, [2, 3]) == [2, 3, -1, None, -1]


def test_map_scheduler():

    @graph
    def map_scheduler(tsd: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return map_(my_scheduler, tsd, "TEST")

    assert (eval_node(map_scheduler, [{"ab1":9, "ab2":9}, {"ab1":9}, {"ab2":9}]) == [
        frozendict({"ab1": 9, "ab2":9}), frozendict({"ab1": 9}), frozendict({"ab2": 9}),] + [None] * 7 +
            [frozendict({"ab1": -1}), frozendict({"ab2": -1})])
