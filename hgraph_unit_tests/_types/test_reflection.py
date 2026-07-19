from dataclasses import dataclass

import pytest

from hgraph import (CompoundScalar, REF, Size, TS, TSB, TSD, TSL, TSS,
                    TimeSeriesSchema, compute_node, operator)
from hgraph.reflection import (
    dereference,
    element_type,
    fields,
    is_bundle,
    is_compound_scalar,
    is_reference,
    is_ts,
    is_tsd,
    is_tsl,
    is_tss,
    key_type,
    operator_overloads,
    scalar_type,
    size,
    value_type,
)


def test_reflection_returns_public_types():
    assert scalar_type(TS[int]) is int
    assert scalar_type(TSS[str]) is str
    assert key_type(TSD[str, TS[int]]) is str
    assert value_type(TSD[str, TS[int]]) == TS[int]
    assert element_type(TSL[TS[int], Size[3]]) == TS[int]
    assert size(TSL[TS[int], Size[3]]) == 3
    assert dereference(REF[TS[int]]) == TS[int]


def test_reflection_accepts_resolved_signature_metadata():
    metadata = TSD[str, REF[TS[int]]]
    from hgraph import HgTypeMetaData

    metadata = HgTypeMetaData.parse_type(metadata)
    assert key_type(metadata) is str
    assert is_reference(value_type(metadata))
    assert dereference(value_type(metadata)) == TS[int]


def test_fields_return_public_schema_types():
    class Bundle(TimeSeriesSchema):
        number: TS[int]
        text: TS[str]

    @dataclass
    class Value(CompoundScalar):
        number: int
        text: str

    assert fields(TSB[Bundle]) == {"number": TS[int], "text": TS[str]}
    assert fields(Value) == {"number": int, "text": str}
    assert fields(TS[Value]) == {"number": int, "text": str}


def test_reflection_predicates():
    @dataclass
    class Value(CompoundScalar):
        number: int

    assert is_ts(TS[int]) and not is_ts(TSD[str, TS[int]])
    assert is_tsd(TSD[str, TS[int]])
    assert is_tsl(TSL[TS[int], Size[3]])
    assert is_tss(TSS[str])
    assert is_bundle(TSB[TimeSeriesSchema.from_scalar_schema(Value)])
    assert is_reference(REF[TS[int]])
    assert is_compound_scalar(TS[Value])


@pytest.mark.parametrize(
    "accessor,value",
    (
        (key_type, TS[int]),
        (value_type, TS[int]),
        (element_type, TS[int]),
        (size, TS[int]),
        (scalar_type, TSD[str, TS[int]]),
        (scalar_type, 42),
    ),
)
def test_reflection_rejects_wrong_shapes(accessor, value):
    with pytest.raises(TypeError):
        accessor(value)


def test_operator_overloads_return_inspectable_implementations():
    import inspect

    @operator
    def identity(ts: TS[int]) -> TS[int]: ...

    @compute_node(overloads=identity)
    def identity_impl(ts: TS[int]) -> TS[int]:
        return ts.value

    assert operator_overloads(identity) == (identity_impl,)
    assert inspect.signature(operator_overloads(identity)[0]).parameters["ts"].annotation == TS[int]
