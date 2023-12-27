import threading
from datetime import datetime, timedelta
from typing import Callable
import sys

from hgraph import push_queue, TS, graph, run_graph, EvaluationMode
from hgraph.nodes import debug_print, stop_engine, if_true


def _user_input(sender: Callable[[str], None]):
    while(True):
        s = sys.stdin.readline().strip('\n')
        sender(s)
        if s == 'exit':
            break


@push_queue(TS[str])
def user_input(sender: Callable[[str], None]):
    threading.Thread(target=_user_input, args=(sender,)).start()


@graph
def main():
    in_ = user_input()
    debug_print(">", in_)
    stop_engine(if_true(in_ == "exit"))


run_graph(main, run_mode=EvaluationMode.REAL_TIME, end_time=datetime.utcnow() + timedelta(minutes=2))
