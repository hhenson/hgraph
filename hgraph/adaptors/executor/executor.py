import os
from concurrent.futures import Executor, ThreadPoolExecutor
from datetime import timedelta

from hgraph import generator, STATE, TS


@generator
def adaptor_executor(pool_size: int = 50, _state: STATE = None) -> TS[Executor]:
    _state.executor = ThreadPoolExecutor(max_workers=pool_size, thread_name_prefix='adaptor-executor-')
    yield timedelta(), _state.executor
