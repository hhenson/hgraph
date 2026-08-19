"""Tests for the lightweight ``hgraph.reflection`` type-decomposition helpers.

These are the migration alternative to upstream's ``Hg*TypeMetaData`` reflection
family (see ``docs/source/developer_guide/type_reflection.rst``).
"""

from dataclasses import dataclass
from typing import Generic

import pytest

from hgraph import (Frame, K, REF, TIME_SERIES_TYPE, TS, TSB, TSD, TSL, TSS,
                    TS_SCHEMA, CompoundScalar, TimeSeriesSchema, compute_node,
                    graph, operator)
from hgraph.reflection import (
    bundle_schema_type,
    dereference,
    element_type,
    fields,
    frame_schema,
    is_bundle,
    is_compound_scalar,
    is_frame,
    is_reference,
    is_ts,
    is_tsd,
    is_tsl,
    is_tss,
    key_type,
    operator_overloads,
    resolved_type,
    scalar_type,
    size,
    value_type,
)
from hgraph.test import eval_node


def test_scalar_type_ts():
    assert scalar_type(TS[int]) == int
    assert scalar_type(TS[str]) == str
    assert scalar_type(TS[float]) == float
    assert scalar_type(TS[bool]) == bool


def test_resolved_type_accepts_public_and_resolution_types():
    assert resolved_type(TS[int]) == TS[int]
    assert resolved_type(TS[int].handle) == TS[int]


def test_scalar_type_tss():
    assert scalar_type(TSS[str]) == str
    assert scalar_type(TSS[int]) == int


def test_scalar_type_compound_scalar_returns_class():
    @dataclass
    class MyCS(CompoundScalar):
        a: int
        b: str

    assert scalar_type(TS[MyCS]) is MyCS


def test_tsd_key_and_value():
    t = TSD[str, TS[int]]
    assert key_type(t) == str
    assert value_type(t) == TS[int]


def test_tsd_value_is_comparable():
    # the returned value type compares equal to the plain annotation
    assert value_type(TSD[int, TS[float]]) == TS[float]


def test_tsl_element_and_size():
    t = TSL[TS[int], 3]
    assert element_type(t) == TS[int]
    assert size(t) == 3


def test_fields_tsb():
    class MyB(TimeSeriesSchema):
        a: TS[int]
        b: TS[str]

    f = fields(TSB[MyB])
    assert f == {"a": TS[int], "b": TS[str]}
    assert list(f) == ["a", "b"]  # ordered


def test_fields_mixed_scalar_and_time_series_generic_tsb():
    class Edits(TimeSeriesSchema, Generic[K, TIME_SERIES_TYPE]):
        edits: TSD[K, TIME_SERIES_TYPE]
        removes: TSS[K]

    concrete = TSB[Edits[int, TS[str]]]

    assert fields(concrete) == {
        "edits": TSD[int, TS[str]],
        "removes": TSS[int],
    }
    assert repr(TSB[Edits[K, TIME_SERIES_TYPE]]) == "TSB[Edits]"


def test_generic_tsb_specialization_uses_resolved_ts_shape_in_name():
    from hgraph._types import _TsExpr

    class Values(TimeSeriesSchema):
        value: TS[int]

    class Edits(TimeSeriesSchema, Generic[K, TIME_SERIES_TYPE]):
        edits: TSD[K, TIME_SERIES_TYPE]
        removes: TSS[K]

    scalar = _TsExpr(TS[int].handle, "resolved[~TIME_SERIES_TYPE]")
    bundle = _TsExpr(TSB[Values].handle, "resolved[~TIME_SERIES_TYPE]")

    scalar_edits = TSB[Edits[str, scalar]]
    bundle_edits = TSB[Edits[str, bundle]]

    assert scalar_edits.handle != bundle_edits.handle
    assert fields(scalar_edits)["edits"] == TSD[str, TS[int]]
    assert fields(bundle_edits)["edits"] == TSD[str, TSB[Values]]


def test_mixed_generic_tsb_rejects_cross_kind_specialization():
    class Edits(TimeSeriesSchema, Generic[K, TIME_SERIES_TYPE]):
        edits: TSD[K, TIME_SERIES_TYPE]
        removes: TSS[K]

    with pytest.raises(TypeError, match="scalar schema parameter"):
        TSB[Edits[TS[int], TS[str]]]
    with pytest.raises(TypeError, match="time-series schema parameter"):
        TSB[Edits[int, str]]


def test_container_key_reflection_preserves_the_declared_python_type():
    key = tuple[int, str]

    assert key_type(TSD[key, TS[int]]) == key
    assert scalar_type(TSS[key]) == key


def test_frame_reflection_preserves_the_row_schema():
    @dataclass
    class Row(CompoundScalar):
        value: int

    assert is_frame(Frame[Row])
    assert is_frame(TS[Frame[Row]])
    assert frame_schema(Frame[Row]) is Row
    assert frame_schema(TS[Frame[Row]]) is Row
    assert not is_frame(TS[int])
    with pytest.raises(TypeError):
        frame_schema(TS[int])


def test_bundle_schema_type_preserves_nominal_schema():
    class MyB(TimeSeriesSchema):
        a: TS[int]

    assert bundle_schema_type(TSB[MyB]) is MyB
    assert bundle_schema_type(TSB[MyB].handle) is MyB

    with pytest.raises(TypeError):
        bundle_schema_type(TS[int])


def test_fields_accepts_variadic_wiring_values():
    observed = {}

    @graph
    def reflect_kwargs(**kwargs: TSB[TS_SCHEMA]) -> TS[int]:
        observed.update(fields(kwargs))
        return kwargs["value"]

    @graph
    def invoke(value: TS[int]) -> TS[int]:
        return reflect_kwargs(value=value)

    assert eval_node(invoke, 1) == [1]
    assert observed == {"value": TS[int]}


def test_fields_compound_scalar_class():
    @dataclass
    class MyCS(CompoundScalar):
        a: int
        b: str

    assert fields(MyCS) == {"a": int, "b": str}


def test_fields_compound_scalar_ts():
    @dataclass
    class MyCS(CompoundScalar):
        a: int
        b: str

    assert fields(TS[MyCS]) == {"a": int, "b": str}


def test_dereference():
    assert dereference(REF[TS[int]]) == TS[int]
    # non-ref returned unchanged
    assert dereference(TS[int]) == TS[int]


def test_dereference_nested_value_type():
    # a TSD whose value is REF-wrapped: value_type keeps the REF, dereference strips it
    t = TSD[str, REF[TS[int]]]
    v = value_type(t)
    assert is_reference(v)
    assert dereference(v) == TS[int]


def test_predicates():
    assert is_ts(TS[int]) and not is_ts(TSD[str, TS[int]])
    assert is_tsd(TSD[str, TS[int]]) and not is_tsd(TS[int])
    assert is_tsl(TSL[TS[int], 3]) and not is_tsl(TS[int])
    assert is_tss(TSS[str]) and not is_tss(TS[int])
    assert is_reference(REF[TS[int]]) and not is_reference(TS[int])


def test_predicates_bundle_and_compound():
    class MyB(TimeSeriesSchema):
        a: TS[int]

    @dataclass
    class MyCS(CompoundScalar):
        a: int

    assert is_bundle(TSB[MyB])
    assert not is_bundle(TS[int])
    assert is_compound_scalar(TS[MyCS])
    assert is_compound_scalar(MyCS)
    assert not is_compound_scalar(TS[int])
    assert not is_compound_scalar(int)


def test_wrong_kind_raises():
    with pytest.raises(TypeError):
        key_type(TS[int])
    with pytest.raises(TypeError):
        value_type(TS[int])
    with pytest.raises(TypeError):
        element_type(TS[int])
    with pytest.raises(TypeError):
        size(TS[int])
    with pytest.raises(TypeError):
        scalar_type(TSD[str, TS[int]])


def test_not_a_type_expression_raises():
    with pytest.raises(TypeError):
        scalar_type(42)


def test_operator_overloads_return_inspectable_implementations():
    import inspect

    @operator
    def identity(ts: TS[int]) -> TS[int]: ...

    @compute_node(overloads=identity)
    def identity_impl(ts: TS[int]) -> TS[int]:
        return ts.value

    assert operator_overloads(identity) == (identity_impl,)
    assert inspect.signature(operator_overloads(identity)[0]).parameters["ts"].annotation == TS[int]
