from dataclasses import dataclass
from typing import TypeVar, ContextManager

from polars.testing import assert_frame_equal

from hgraph import CompoundScalar, Frame, graph, compute_node, TS, generator, MIN_DT, MIN_ST, TimeSeriesSchema, TSB
from hgraph.nodes import const
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

    ST = TypeVar('ST', TS[ScalarType], TS[Frame[ScalarItemType]], TSB[OneAndMany])

    @compute_node
    def add(x: TS[ScalarType], y: TS[float]) -> TS[ScalarType]:
        return ScalarType(x.value.value + y.value)


    @compute_node(overloads=add)
    def add_frame(x: TS[Frame[ScalarItemType]], y: TS[float]) -> TS[Frame[ScalarItemType]]:
        import polars as pl
        return pl.DataFrame({'name': x.value['name'], 'value': x.value['value'] + y.value})


    @graph(overloads=add)
    def add_bundle(x: TSB[OneAndMany], y: TS[float]) -> TSB[OneAndMany]:
        return {'one': add(x.one, y), 'many': add(x.many, y)}


    class Source(ContextManager["MarketDataContext"]):
        __stack__: ['Source'] = []

        @classmethod
        def current(cls) -> 'Source':
            return cls.__stack__[-1]

        def __enter__(self):
            self.__stack__.append(self)

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.__stack__.pop()

    class ScalarSource(Source): ...
    class FrameSource(Source): ...
    class BundleSource(Source): ...

    @generator
    def const_frame() -> TS[Frame[ScalarItemType]]:
        import polars as pl
        yield MIN_ST, pl.DataFrame({'name': ['a', 'b'], 'value': [1., 2.]})

    ScalarSource.subscribe = lambda self: const(ScalarType(1.))
    FrameSource.subscribe = lambda self: const_frame()
    BundleSource.subscribe = lambda self: TSB[OneAndMany].from_ts(one=const(ScalarType(1.)), many=const_frame())

    @graph
    def addition(_: TS[bool]) -> ST:
        return add(Source.current().subscribe(), 1.)

    with ScalarSource():
        assert eval_node(addition, [None]) == [ScalarType(2.)]

    with FrameSource():
        import polars as pl
        assert_frame_equal(eval_node(addition, [None])[0], pl.DataFrame({'name': ['a', 'b'], 'value': [2., 3.]}))

    with BundleSource():
        outputs = eval_node(addition, [None])
        assert outputs[0]['one'] == ScalarType(2.)
        assert_frame_equal(outputs[0]['many'], pl.DataFrame({'name': ['a', 'b'], 'value': [2., 3.]}))
