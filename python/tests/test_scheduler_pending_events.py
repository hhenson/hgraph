from hgraph import MIN_TD, SCHEDULER, STATE, TS, compute_node, eval_node


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
