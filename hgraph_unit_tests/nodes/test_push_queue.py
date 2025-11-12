import threading
import time
from datetime import datetime, timedelta
from typing import Callable, Tuple

from hgraph import REMOVE, TSD, record, TS, evaluate_graph, GraphConfiguration, GlobalState, push_queue, graph, const, if_true, EvaluationMode, contains_
from hgraph import stop_engine
from hgraph._impl._operators._record_replay_in_memory import get_recorded_value


def test_push_queue():

    def _sender(sender: Callable[[str], None], values: [str]):
        for value in values:
            print("-> Sending", value)
            sender(value)
            time.sleep(0.1)

    @push_queue(TS[str])
    def my_message_sender(sender: Callable[[str], None], values: tuple[str, ...]):
        threading.Thread(target=_sender, args=(sender, values)).start()

    @graph
    def main():
        messages = my_message_sender(("1", "2", "3"))
        record(messages)
        stop_engine(if_true(messages == const("3")), "Completed Processing request")

    now = datetime.utcnow()
    # Note that it is possible that the time-out here may be insufficient to allow the task to complete.
    with GlobalState():
        evaluate_graph(main, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, start_time=now, end_time=timedelta(seconds=1)))
        values = get_recorded_value()
    # The exact timings are not that important.
    assert [v[1] for v in values] == ["1", "2", "3"]


def test_batch_push_queue():
    def _sender(sender: Callable[[str], None], values: [str]):
        for value in values:
            sender(value)

    @push_queue(TS[Tuple[str, ...]])
    def my_message_sender(sender: Callable[[str], None], values: tuple[str, ...], batch: bool = True):
        threading.Thread(target=_sender, args=(sender, values)).start()

    @graph
    def main():
        messages = my_message_sender(("1", "2", "3"))
        record(messages)
        stop_engine(if_true(contains_(messages, const("3"))), "Completed Processing request")

    now = datetime.utcnow()
    # Note that it is possible that the time-out here may be insufficient to allow the task to complete.
    with GlobalState():
        evaluate_graph(main, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, start_time=now, end_time=timedelta(seconds=1)))
        values = get_recorded_value()
    # The exact timings are not that important.
    assert [v[1] for v in values] == [("1", "2", "3")]


def test_elide_push_queue():
    def _sender(sender: Callable[[str], None], values: [str]):
        for value in values:
            sender(value)

    @push_queue(TS[str])
    def my_message_sender(sender: Callable[[str], None], values: tuple[str, ...], elide: bool = True):
        threading.Thread(target=_sender, args=(sender, values)).start()

    @graph
    def main():
        messages = my_message_sender(("1", "2", "3"))
        record(messages)
        stop_engine(if_true(messages == const("3")), "Completed Processing request")

    now = datetime.utcnow()
    # Note that it is possible that the time-out here may be insufficient to allow the task to complete.
    with GlobalState():
        evaluate_graph(main, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, start_time=now, end_time=timedelta(seconds=1)))
        values = get_recorded_value()
    # The exact timings are not that important.
    assert [v[1] for v in values] == ["3"]


def test_tsd_push_queue():
    def _sender(sender: Callable[[str, float], None], values: Tuple[dict[str, float]]):
        for value in values:
            sender(value)
            time.sleep(0.01)

    @push_queue(TSD[str, TS[float]])
    def my_message_sender(sender: Callable[[str, float], None], values: tuple[dict[str, float], ...]):
        threading.Thread(target=_sender, args=(sender, values)).start()

    @graph
    def main():
        messages = my_message_sender(({"a": 1.0}, {"b": 2.0}, {"c": 3.0}, {"a": REMOVE}, {'c': 4.0}))
        record(messages)
        stop_engine(if_true(messages['c'] == 4.0))

    # Note that it is possible that the time-out here may be insufficient to allow the task to complete.
    with GlobalState():
        evaluate_graph(main, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=1000), trace=True))
        values = get_recorded_value()
        
    # The exact timings are not that important.
    assert [v[1] for v in values] == [{"a": 1.0}, {"b": 2.0}, {"c": 3.0}, {"a": REMOVE}, {'c': 4.0}]
    
    
def test_tsd_push_batch_queue():
    def _sender(sender: Callable[[str, float], None], values: Tuple[dict[str, float]]):
        time.sleep(0.01)
        for value in values:
            sender(value)

    @push_queue(TSD[str, TS[Tuple[float]]])
    def my_message_sender(sender: Callable[[str, float], None], values: tuple[dict[str, float], ...], batch: bool = True):
        threading.Thread(target=_sender, args=(sender, values)).start()

    @graph
    def main():
        messages = my_message_sender(({"a": 1.0}, {"b": 2.0}, {"c": 3.0}, {"a": REMOVE}, {'c': 4.0}))
        record(messages)
        stop_engine(if_true(messages['c'][0] == 4.0))

    # Note that it is possible that the time-out here may be insufficient to allow the task to complete.
    with GlobalState():
        evaluate_graph(main, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=1), trace=True))
        values = get_recorded_value()
        
    # The exact timings are not that important.
    assert [v[1] for v in values] == [{"a": (1.0,), "b": (2.0,), "c": (3.0,)}, {"a": REMOVE, 'c': (4.0,)}]