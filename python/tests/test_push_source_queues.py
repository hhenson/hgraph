import datetime
import threading

import pytest

import hgraph as hg
from hgraph import TS, graph


def _run(g, seconds=0.5):
    end = datetime.datetime.now(datetime.UTC).replace(tzinfo=None) + datetime.timedelta(
        seconds=seconds)
    hg.run_graph(g, end_time=end, run_mode=hg.EvaluationMode.REAL_TIME)


def test_burst_push_queue_delivers_pending_scalars_as_one_tuple():
    observed = []

    @hg.push_queue(TS[tuple[int, ...]], burst=True, max_pending=3)
    def source(sender):
        sender(1)
        sender(2)
        sender(3)

    @hg.sink_node
    def collect(value: TS[tuple[int, ...]]) -> None:
        observed.append(value.value)

    @graph
    def app() -> None:
        collect(source())

    _run(app)
    assert observed == [(1, 2, 3)]


def test_bounded_burst_python_sender_releases_gil_while_waiting_for_dequeue():
    observed = []
    workers = []
    completed = threading.Event()

    @hg.push_queue(TS[tuple[int, ...]], burst=True, max_pending=1)
    def source(sender):
        sender(1)

        def send_second():
            sender(2)
            completed.set()

        worker = threading.Thread(target=send_second)
        workers.append(worker)
        worker.start()

    @hg.sink_node
    def collect(value: TS[tuple[int, ...]]) -> None:
        observed.append(value.value)

    @graph
    def app() -> None:
        collect(source())

    _run(app)
    for worker in workers:
        worker.join(timeout=1.0)

    assert completed.is_set()
    assert observed == [(1,), (2,)]


def test_python_sender_raises_after_graph_stop():
    retained = []

    @hg.push_queue(TS[int])
    def source(sender):
        retained.append(sender)

    @graph
    def app() -> None:
        source()

    _run(app, seconds=0.1)
    assert len(retained) == 1
    with pytest.raises(RuntimeError, match="not accepting"):
        retained[0](1)


def test_push_queue_rejects_incompatible_policy_options():
    with pytest.raises(TypeError, match="mutually exclusive"):
        @hg.push_queue(TS[int], conflate=True, burst=True)
        def conflated_burst(sender):
            pass

    with pytest.raises(TypeError, match="not supported with conflate"):
        @hg.push_queue(TS[int], conflate=True, max_pending=1)
        def bounded_conflated(sender):
            pass

    with pytest.raises(ValueError, match="greater than zero"):
        @hg.push_queue(TS[int], max_pending=0)
        def zero_capacity(sender):
            pass

    with pytest.raises(TypeError, match="positive integer"):
        @hg.push_queue(TS[int], max_pending=True)
        def boolean_capacity(sender):
            pass

    @hg.push_queue(TS[int], burst=True)
    def invalid_burst(sender):
        pass

    @graph
    def invalid_app() -> None:
        invalid_burst()

    with pytest.raises(ValueError, match=r"TS\[tuple\[SCALAR, \.\.\.\]\]"):
        _run(invalid_app, seconds=0.1)
