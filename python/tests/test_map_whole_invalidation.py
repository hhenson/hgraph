import hgraph as hg
from hgraph.test import eval_node


def test_map_withdraws_keys_on_whole_invalidation_before_same_or_later_step_write():
    @hg.compute_node
    def book(step: hg.TS[int], _output: hg.TSD_OUT[str, hg.TS[int]] = None) -> hg.TSD[str, hg.TS[int]]:
        if step.value == 0:
            return {"A": 1, "B": 2}
        if step.value == 1:
            _output.invalidate()
            return {"C": 3}
        if step.value == 2:
            _output.invalidate()
            return None
        return {"A": 4}

    @hg.graph
    def mapped(step: hg.TS[int]) -> hg.TSD[str, hg.TS[int]]:
        return hg.map_(lambda value: value + 10, book(step))

    assert eval_node(mapped, [0, 1, 2, 3]) == [
        {"A": 11, "B": 12},
        {"A": hg.REMOVE, "B": hg.REMOVE, "C": 13},
        {"C": hg.REMOVE},
        {"A": 14},
    ]
