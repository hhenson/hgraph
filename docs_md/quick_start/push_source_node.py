import threading
from datetime import timedelta
from typing import Callable
import sys

from hgraph import push_queue, TS, graph, evaluate_graph, GraphConfiguration, EvaluationMode, debug_print, if_true
from hgraph import stop_engine


def _user_input(sender: Callable[[str], None]):
    while True:
        s = sys.stdin.readline().strip("\n")
        sender(s)
        if s == "exit":
            break


@push_queue(TS[str])
def user_input(sender: Callable[[str], None]):
    threading.Thread(target=_user_input, args=(sender,)).start()


@graph
def main():
    in_ = user_input()
    debug_print(">", in_)
    stop_engine(if_true(in_ == "exit"))


evaluate_graph(main, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=2)))
