import logging

from hgraph import compute_node, TSL, TS, Size
from hgraph._impl._runtime._node import _SenderReceiverState
from hgraph.test import eval_node


def test_all_valid():
    @compute_node(all_valid=("tsl",))
    def a_node(tsl: TSL[TS[int], Size[2]]) -> TS[bool]:
        return True

    assert eval_node(a_node, [{0: 1}, {1: 1}]) == [None, True]


def test_valid():
    @compute_node(valid=("tsl",))
    def a_node(tsl: TSL[TS[int], Size[2]]) -> TS[bool]:
        return True

    assert eval_node(a_node, [{0: 1}, {1: 1}]) == [True, True]


class _StubEvaluationClock:

    def __init__(self):
        self.mark_push_node_requires_scheduling_calls = 0

    def mark_push_node_requires_scheduling(self):
        self.mark_push_node_requires_scheduling_calls += 1


def test_sender_receiver_state_ignores_enqueue_once_stopped(caplog):
    receiver = _SenderReceiverState(evaluation_clock=_StubEvaluationClock(), stopped=True)

    with caplog.at_level(logging.WARNING):
        receiver.enqueue((0, "first"))
        receiver.enqueue((0, "second"))

    assert not receiver.queue
    assert receiver.evaluation_clock.mark_push_node_requires_scheduling_calls == 0
    assert [record.getMessage() for record in caplog.records] == ["Ignoring enqueue into a stopped receiver"]
