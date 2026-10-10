"""The Python node decorator's per-definition wiring plan.

The plan caches what the signature fixes; the facts it borrows from the
registry (parameter roles) must follow the registry, not the process.
"""

import _hgraph
import hgraph as hg
from hgraph import TS, compute_node, eval_node, operator


def test_registration_generation_moves_when_an_overload_is_registered():
    before = _hgraph._operator_registration_generation()

    @operator
    def _call_plan_probe(x: TS[int]) -> TS[int]: ...

    @compute_node(overloads=_call_plan_probe)
    def _call_plan_probe_int(x: TS[int]) -> TS[int]:
        return x.value + 1

    assert _hgraph._operator_registration_generation() > before
    assert eval_node(_call_plan_probe, [1, 2]) == [2, 3]


def test_optional_time_series_input_with_none_default_is_a_scalar_when_unwired():
    # An unwired optional input lands in the scalar slot of the layout and a
    # wired one in an input slot; both shapes of the same definition must
    # wire correctly in one process (the layout cache is per layout string).
    @compute_node(valid=("x",))
    def _add_or_pass(x: TS[int], y: TS[int] = None) -> TS[int]:
        return x.value + (y.value if y is not None and y.valid else 0)

    assert eval_node(_add_or_pass, [1, 2]) == [1, 2]
    assert eval_node(_add_or_pass, [1, 2], [10, None]) == [11, 12]
