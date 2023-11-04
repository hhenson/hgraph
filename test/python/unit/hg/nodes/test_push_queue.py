import threading
import time
from datetime import datetime, timedelta
from typing import Callable

from hg import TS, run_graph, RunMode, GlobalState, push_queue, graph
from hg.nodes import write_str, record, get_recorded_value


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
        record(messages)

    now = datetime.utcnow()
    # Note that it is possible that the time-out here may be insufficient to allow the task to complete.
    GlobalState.reset()
    run_graph(main, run_mode=RunMode.REAL_TIME, start_time=now, end_time=now + timedelta(seconds=1))
    values = get_recorded_value()
    # The exact timings are not that important.
    assert [v[1] for v in values] == ["1", "2", "3"]