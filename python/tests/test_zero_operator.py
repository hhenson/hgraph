from dataclasses import dataclass
from typing import Callable

from hgraph import TS, add_, graph, zero
from hgraph.test import eval_node


def test_zero_accepts_an_operator_as_a_positional_or_named_argument():
    assert eval_node(lambda: zero[TS[int]](add_)) == [0]
    assert eval_node(lambda: zero[TS[int]](op=add_)) == [0]


@dataclass(frozen=True)
class _CustomValue:
    value: str


@graph(overloads=zero)
def _zero_custom_value(op: Callable) -> TS[_CustomValue]:
    return _CustomValue(op.__name__)


def test_python_zero_overload_can_identify_the_native_operator():
    assert eval_node(lambda: zero[TS[_CustomValue]](add_)) == [
        _CustomValue("add_")
    ]
