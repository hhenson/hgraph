import threading
import time
from datetime import datetime, timedelta
from typing import Callable

from hg import TS, run_graph, RunMode
from hg._wiring._decorators import push_queue, graph
from hg.nodes import write_str


def test_push_queue():

    def _sender(sender: Callable[[str], None], values: [str]):
        for value in values:
            sender(value)
            time.sleep(0.1)

    @push_queue(TS[str])
    def my_message_sender(sender: Callable[[str], None], values: tuple[str]):
        threading.Thread(target=_sender, args=(sender, values)).start()

    @graph
    def main():
        messages = my_message_sender(("1", "2", "3"))
        write_str(messages)

    now = datetime.utcnow()
    run_graph(main, run_mode=RunMode.REAL_TIME, start_time=now, end_time=now + timedelta(seconds=3))