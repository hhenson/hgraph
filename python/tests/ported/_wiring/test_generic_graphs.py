# Ported from hgraph 4760fccadd5368b0482393e5acb0ceaac48518e9
# hgraph_unit_tests/_wiring/test_generic_graphs.py
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import TypeVar, Union

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from hgraph import (
    CompoundScalar,
    Frame,
    IncorrectTypeBinding,
    MIN_ST,
    TS,
    TSB,
    TimeSeriesSchema,
    compute_node,
    const,
    generator,
    graph,
    operator,
)
from hgraph.test import eval_node


def test_constraint_typevar_wiring():
    @dataclass(unsafe_hash=True)
    class ScalarType(CompoundScalar):
        value: float

    @dataclass(unsafe_hash=True)
    class ScalarItemType(CompoundScalar):
        name: str
        value: float

    class OneAndMany(TimeSeriesSchema):
        one: TS[ScalarType]
        many: TS[Frame[ScalarItemType]]

    ST = TypeVar("ST", TS[ScalarType], TS[Frame[ScalarItemType]], TSB[OneAndMany])

    @operator
    def add(x: ST, y: TS[float]) -> ST: ...

    @compute_node(overloads=add)
    def add_default(x: TS[ScalarType], y: TS[float]) -> TS[ScalarType]:
        return ScalarType(x.value.value + y.value)

    @compute_node(overloads=add)
    def add_frame(x: TS[Frame[ScalarItemType]], y: TS[float]) -> TS[Frame[ScalarItemType]]:
        return pa.table({
            "name": x.value.column("name"),
            "value": pc.add(x.value.column("value"), y.value),
        })

    @graph(overloads=add)
    def add_bundle(x: TSB[OneAndMany], y: TS[float]) -> TSB[OneAndMany]:
        return {"one": add(x.one, y), "many": add(x.many, y)}

    @compute_node
    def constrained_identity(x: ST) -> ST:
        return x.value

    @graph
    def constrained_scalar(x: TS[ScalarType]) -> TS[ScalarType]:
        return constrained_identity(x)

    @graph
    def constrained_invalid(x: TS[int]) -> TS[int]:
        return constrained_identity(x)

    assert eval_node(constrained_scalar, [ScalarType(1.0)]) == [ScalarType(1.0)]
    with pytest.raises(IncorrectTypeBinding, match="constrained_identity.*expects"):
        eval_node(constrained_invalid, [1])

    class Source(AbstractContextManager):
        __stack__ = []

        @classmethod
        def current(cls):
            return cls.__stack__[-1]

        def __enter__(self):
            self.__stack__.append(self)
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self.__stack__.pop()

    class ScalarSource(Source):
        pass

    class FrameSource(Source):
        pass

    class BundleSource(Source):
        pass

    @generator
    def const_frame() -> TS[Frame[ScalarItemType]]:
        yield MIN_ST, pa.table({"name": ["a", "b"], "value": [1.0, 2.0]})

    ScalarSource.subscribe = lambda self: const(ScalarType(1.0))
    FrameSource.subscribe = lambda self: const_frame()
    BundleSource.subscribe = lambda self: TSB[OneAndMany].from_ts(
        one=const(ScalarType(1.0)), many=const_frame())

    @graph
    def addition(_: TS[bool]) -> ST:
        return add(Source.current().subscribe(), 1.0)

    expected_frame = pa.table({"name": ["a", "b"], "value": [2.0, 3.0]})
    with ScalarSource():
        assert eval_node(addition, [None]) == [ScalarType(2.0)]

    with FrameSource():
        assert eval_node(addition, [None])[0].equals(expected_frame)

    with BundleSource():
        output = eval_node(addition, [None])[0]
        assert output["one"] == ScalarType(2.0)
        assert output["many"].equals(expected_frame)


def test_constraint_scalar_typevar_wiring():
    ScalarT = TypeVar("ScalarT", int, str)

    @compute_node
    def constrained_identity(x: TS[ScalarT]) -> TS[ScalarT]:
        return x.value

    @graph
    def constrained_int(x: TS[int]) -> TS[int]:
        return constrained_identity(x)

    @graph
    def constrained_str(x: TS[str]) -> TS[str]:
        return constrained_identity(x)

    @graph
    def constrained_float(x: TS[float]) -> TS[float]:
        return constrained_identity(x)

    assert eval_node(constrained_int, [1, 2]) == [1, 2]
    assert eval_node(constrained_str, ["a", "b"]) == ["a", "b"]
    with pytest.raises(IncorrectTypeBinding, match="constrained_identity.*expects"):
        eval_node(constrained_float, [1.0])


def test_union_wiring():
    @compute_node
    def union_fn(x: Union[TS[int], TS[str]]) -> TS[str]:
        return str(x.value)

    @graph
    def g(i: TS[int]) -> TS[str]:
        return union_fn(i)

    @graph
    def h(s: TS[str]) -> TS[str]:
        return union_fn(s)

    assert eval_node(g, [None, 1, 2, 3]) == [None, "1", "2", "3"]
    assert eval_node(h, [None, "a", "b", "c"]) == [None, "a", "b", "c"]
