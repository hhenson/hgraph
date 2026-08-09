from concurrent.futures import Executor, ThreadPoolExecutor

from hgraph import STATE, TS, compute_node, const, graph

__all__ = ("adaptor_executor",)


@compute_node
def _executor(trigger: TS[bool], pool_size: int, state: STATE = None) -> TS[Executor]:
    return state.executor


@_executor.start
def _start_executor(pool_size: int, state: STATE = None):
    state.executor = ThreadPoolExecutor(
        max_workers=pool_size,
        thread_name_prefix="hgraph-adaptor",
    )


@_executor.stop
def _stop_executor(pool_size: int, state: STATE = None):
    state.executor.shutdown(wait=True, cancel_futures=True)


@graph
def adaptor_executor(pool_size: int = 50) -> TS[Executor]:
    """Own a shared thread pool for the lifetime of the running graph."""
    return _executor(const(True), pool_size=pool_size)
