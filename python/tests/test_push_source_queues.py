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
        assert sender(1) is True
        assert sender(2) is True
        assert sender(3) is True

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


def test_python_sender_returns_false_after_graph_stop():
    retained = []

    @hg.push_queue(TS[int])
    def source(sender):
        retained.append(sender)

    @graph
    def app() -> None:
        source()

    _run(app, seconds=0.1)
    assert len(retained) == 1
    assert retained[0](1) is False


def test_push_queue_stop_hook_shares_state_and_joins_worker():
    started = threading.Event()
    finished = threading.Event()
    lifecycle = []

    @hg.push_queue(TS[int])
    def source(sender, label: str, state: hg.STATE = None):
        stop_requested = threading.Event()
        state.stop_requested = stop_requested

        def run():
            started.set()
            stop_requested.wait()
            finished.set()

        state.worker = threading.Thread(target=run)
        state.worker.start()
        lifecycle.append(("start", label))

    @source.stop
    def stop_source(label: str, state: hg.STATE = None):
        state.stop_requested.set()
        state.worker.join(timeout=1.0)
        lifecycle.append(("stop", label))

    @graph
    def app() -> None:
        source("worker")

    _run(app, seconds=0.1)

    assert started.is_set()
    assert finished.is_set()
    assert lifecycle == [("start", "worker"), ("stop", "worker")]


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
