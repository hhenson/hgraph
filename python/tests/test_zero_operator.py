import pytest
from dataclasses import dataclass
from typing import Callable

from hgraph import TS, add_, graph, zero
from hgraph.test import eval_node


def test_zero_takes_the_type_then_the_operator():
    # Released hgraph's signature is zero(tp, op) (parity #818 item 2.1).
    assert eval_node(lambda: zero(TS[int], add_)) == [0]
    assert eval_node(lambda: zero(tp=TS[int], op=add_)) == [0]
    # The type may also come from the requested output, as for nothing.
    assert eval_node(lambda: zero[TS[int]](op=add_)) == [0]
    # A bare operator where the type belongs is rejected, as released.
    with pytest.raises(Exception):
        eval_node(lambda: zero[TS[int]](add_))


@dataclass(frozen=True)
class _CustomValue:
    value: str


@graph(overloads=zero)
def _zero_custom_value(tp: type[TS[_CustomValue]], op: Callable) -> TS[_CustomValue]:
    return _CustomValue(op.__name__)


def test_python_zero_overload_can_identify_the_native_operator():
    assert eval_node(lambda: zero(TS[_CustomValue], add_)) == [
        _CustomValue("add_")
    ]
