Push Source Node
================

For the Python API we provide a simple API to support introducing data
using a queue to enqueue requests to.

The user wraps a function with ``@push_queue``. The wrapped function
must take as its first argument an argument to receive the ``sender``, which
is a callable that takes the scalar type used as a delta value to the time-series
type declared in the decorator.

It is possible to accept other scalar value arguments, this is useful to configure
an instance of the push queue.

Typically, the wrapped function will create a thread and perform it's work on 
the thread feeding results to the graph using the ``sender`` callable.

It is also possible to use a message bus which may run its own threading.

The example below reads lines from std_in until either it times out or the
user types 'exit'.

```python
from typing import Callable
import sys
from datetime import datetime, timedelta
import threading
from hgraph import push_queue, TS, graph, evaluate_graph, GraphConfiguration, EvaluationMode, debug_print, if_true
from hgraph import stop_engine


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


evaluate_graph(main, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(minutes=2)))
```
