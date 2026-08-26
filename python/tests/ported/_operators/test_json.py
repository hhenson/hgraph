from dataclasses import dataclass

import pytest
from hgraph import (
    CompoundScalar,
    JSON,
    SCHEMA,
    TS,
    TS_SCHEMA,
    TSB,
    combine,
    compute_node,
    const,
    graph,
    json_decode,
    json_encode,
)
from hgraph.test import eval_node



def test_json_combine_encode():
    @graph
    def g() -> TS[str]:
        j1 = combine[TS[JSON]](a=1, b="test")
        j2 = combine[TS[JSON]](c=3.14, d=[1, 2, 3])
        j3 = combine[TS[JSON]](x=j1, y=j2)
        return json_encode[str](j3)
    
    assert eval_node(g) == [
        '{"x": {"a": 1, "b": "test"}, "y": {"c": 3.14, "d": [1, 2, 3]}}'
    ]


def test_json_positional_combine_builds_array_without_flattening():
    @graph
    def g(value: TS[int]) -> TS[JSON]:
        nested = combine[TS[JSON]](key=value)
        return combine[TS[JSON]](value, "text", [2, 3], nested)

    result = eval_node(g, [1])[0]

    assert result.to_python() == [1, "text", [2, 3], {"key": 1}]


def test_json_add_concatenates_arrays_including_lazy_values():
    @graph
    def g() -> TS[JSON]:
        return json_decode('[1, {"a": 2}]') + combine[TS[JSON]](3, [4])

    result = eval_node(g)[0]

    assert result.to_python() == [1, {"a": 2}, 3, [4]]


@pytest.mark.parametrize(
    ("lhs", "rhs", "side"),
    [
        (JSON({"a": 1}), JSON([]), "left"),
        (JSON([]), JSON({"a": 1}), "right"),
    ],
)
def test_json_add_rejects_non_array_values_at_runtime(lhs, rhs, side):
    @graph
    def g(lhs: TS[JSON], rhs: TS[JSON]) -> TS[JSON]:
        return lhs + rhs

    with pytest.raises(RuntimeError, match=rf"JSON array concatenation requires array operands; {side} operand"):
        eval_node(g, [lhs], [rhs])


def test_json_decode():
    @graph
    def g() -> TSB[TS_SCHEMA]:
        j = '{"a": 1, "b": "test", "c": 3.14, "d": [1, 2, 3]}'
        decoded = json_decode(j)
        return combine(a=decoded["a"].int, b=decoded["b"].str, c=decoded["c"].float, d=decoded["d"][0].int)
    
    assert eval_node(g) == [
        {'a': 1, 'b': 'test', 'c': 3.14, 'd': 1}
    ]


def test_json_is_a_native_value_and_const_preserves_its_type():
    value = JSON({"a": 1, "values": [True, None, "x"]})

    assert not isinstance(value, str)
    assert value.to_python() == {"a": 1, "values": [True, None, "x"]}
    assert repr(value) == "JSON({'a': 1, 'values': [True, None, 'x']})"

    @graph
    def inferred() -> TS[str]:
        return json_encode[str](const(value))

    @graph
    def explicitly_typed() -> TS[str]:
        return json_encode[str](const[TS[JSON]]({"a": 1, "values": [True, None, "x"]}))

    expected = '{"a": 1, "values": [true, null, "x"]}'
    assert eval_node(inferred) == [expected]
    assert eval_node(explicitly_typed) == [expected]


def test_json_native_value_crosses_python_node_and_recording_boundaries():
    observed = []

    @compute_node
    def echo(value: TS[JSON]) -> TS[JSON]:
        observed.append(value.value)
        return value.value

    result = eval_node(echo, [JSON({"nested": [1, 2]})])[0]

    assert isinstance(observed[0], JSON)
    assert observed[0].to_python() == {"nested": [1, 2]}
    assert isinstance(result, JSON)
    assert result.to_python() == {"nested": [1, 2]}


def test_json_decode_returns_native_json_value_at_python_boundary():
    @graph
    def decoded() -> TS[JSON]:
        return json_decode(const('{"nested": [1, null]}'))

    result = eval_node(decoded)[0]

    assert isinstance(result, JSON)
    assert result.to_python() == {"nested": [1, None]}


def test_compound_scalar_json_field_preserves_native_value():
    @dataclass(frozen=True)
    class Envelope(CompoundScalar):
        payload: JSON

    @compute_node
    def make_envelope(value: TS[int]) -> TS[Envelope]:
        return Envelope(payload=JSON({"value": value.value}))

    result = eval_node(make_envelope, [7])[0]

    assert isinstance(result.payload, JSON)
    assert result.payload.to_python() == {"value": 7}


def test_combine_json_dereferences_reference_arguments():
    """A JSON object serialises the VALUES it is given.

    ``if_then_else`` publishes a REFERENCE, and ``combine[TS[JSON]]`` packs its
    keyword arguments into a structural bundle. Before the deref rule was
    applied where variadic and keyword arguments are BOUND, the ref token
    reached the encoder and serialised as ``"<ref>"`` instead of the value it
    names - the same defect as the logging one, in a different consumer.
    """
    from hgraph import TS, JSON, combine, graph, if_then_else
    from hgraph.test import eval_node

    @graph
    def g(choose: TS[bool], lhs: TS[int], rhs: TS[int]) -> TS[JSON]:
        return combine[TS[JSON]](v=if_then_else(choose, lhs, rhs))

    result = eval_node(g, [True], [8], [-6])[0]
    assert isinstance(result, JSON)
    assert result.to_python() == {"v": 8}
