from hgraph import compute_node, TS, SCHEDULER, MIN_TD
from hgraph.test import eval_node


@compute_node
def my_scheduler(ts: TS[int], _scheduler: SCHEDULER = None) -> TS[int]:
    if ts.modified:
        _scheduler.schedule(MIN_TD*ts.value)
        return ts.value
    if _scheduler.is_scheduled_now:
        return -1


def test_scheduler():
    assert eval_node(my_scheduler, [2, 3]) == [2, 3, -1, None, -1]
