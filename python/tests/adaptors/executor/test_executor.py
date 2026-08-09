from concurrent.futures import Executor

from hgraph import TS, compute_node, eval_node, graph
from hgraph.adaptors.executor import adaptor_executor


@compute_node
def _submit(executor: TS[Executor], value: TS[int]) -> TS[int]:
    return executor.value.submit(lambda item: item + 1, value.value).result()


def test_adaptor_executor_runs_work_inside_eval_node():
    @graph
    def use_executor(value: TS[int]) -> TS[int]:
        return _submit(adaptor_executor(pool_size=1), value)

    assert eval_node(use_executor, [41]) == [42]
