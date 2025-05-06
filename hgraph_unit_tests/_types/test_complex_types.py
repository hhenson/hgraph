from dataclasses import dataclass
from typing import Generic

from hgraph import (
    CompoundScalar,
    SCALAR,
    HgTimeSeriesTypeMetaData,
    WiringGraphContext,
    compound_scalar,
    ts_schema,
    HgScalarTypeMetaData,
    HgCompoundScalarType,
    TimeSeriesSchema,
    TSB,
    is_bundle,
    is_compound_scalar,
    TS,
    WiringNodeInstanceContext,
)


@dataclass
class SimpleSchema(TimeSeriesSchema):
    p1: TS[int]


@dataclass
class LessSimpleBundle(SimpleSchema, scalar_type=True):
    p2: TS[str]
    p3: SimpleSchema


def test_matches_bundle():
    tp = HgTimeSeriesTypeMetaData.parse_type(LessSimpleBundle)
    assert tp.matches(tp)


def test_matches_un_named_type():
    tp_1 = HgTimeSeriesTypeMetaData.parse_type(SimpleSchema)
    tp_2 = HgTimeSeriesTypeMetaData.parse_type(ts_schema(p1=TS[int]))
    assert tp_1.matches(tp_2)


def test_simple_bundle():
    assert is_bundle(TSB[SimpleSchema])
    assert is_bundle(TSB[LessSimpleBundle])
    from hgraph import const

    with WiringNodeInstanceContext(), WiringGraphContext(None):
        p1 = const(1)
        b1 = TSB[SimpleSchema].from_ts(p1=p1)
        assert b1.__schema__ == SimpleSchema
        assert b1.p1.__orig_eq__(p1)
        assert b1.as_schema.p1.__orig_eq__(p1)


def test_simple_scalar_from_bundle():
    scalar = SimpleSchema.to_scalar_schema()
    assert is_compound_scalar(scalar)
    assert scalar.__meta_data_schema__["p1"] == HgScalarTypeMetaData.parse_type(int)
    assert len(scalar.__meta_data_schema__) == 1
    i = scalar(p1=1)
    assert i.p1 == 1


def test_less_simple_scalar_from_bundle():
    scalar_type = LessSimpleBundle.scalar_type()
    assert scalar_type is not None
    assert scalar_type.__name__ == "LessSimpleBundleStruct"
    assert scalar_type.__meta_data_schema__["p1"] == HgScalarTypeMetaData.parse_type(int)
    assert scalar_type.__meta_data_schema__["p2"] == HgScalarTypeMetaData.parse_type(str)
    assert scalar_type.__meta_data_schema__["p3"] == HgScalarTypeMetaData.parse_type(SimpleSchema.to_scalar_schema())


@dataclass(frozen=True)
class SimpleCompoundScalar(CompoundScalar):
    p1: int


@dataclass(frozen=True)
class LessSimpleCompoundScalar(SimpleCompoundScalar):
    p2: str
    p3: SimpleCompoundScalar


def test_matches_compound_scalar():
    tp = HgScalarTypeMetaData.parse_type(LessSimpleCompoundScalar)
    assert tp.matches(tp)


def test_matches_compound_scalar_un_named_type():
    tp_1 = HgScalarTypeMetaData.parse_type(SimpleCompoundScalar)
    tp_2 = HgScalarTypeMetaData.parse_type(compound_scalar(p1=int))
    assert tp_1.matches(tp_2)


def test_simple_compound_scalar():
    assert is_compound_scalar(SimpleCompoundScalar)
    meta_data = SimpleCompoundScalar.__meta_data_schema__
    assert len(meta_data) == 1
    assert meta_data["p1"] == HgScalarTypeMetaData.parse_type(int)

    meta_data = LessSimpleCompoundScalar.__meta_data_schema__
    assert len(meta_data) == 3
    assert meta_data["p3"] == HgCompoundScalarType(SimpleCompoundScalar)

    hg_meta = HgCompoundScalarType.parse_type(LessSimpleCompoundScalar)
    assert hg_meta.is_resolved
    assert not hg_meta.is_atomic


@dataclass(frozen=True)
class UnResolvedCompoundScalar(CompoundScalar, Generic[SCALAR]):
    p1: SCALAR
    p2: str


def test_unresolved_compound_scalar():
    hg_meta = HgCompoundScalarType.parse_type(UnResolvedCompoundScalar)
    assert not hg_meta.is_resolved


def test_multiple_inheritance():
    @dataclass(frozen=True)
    class SimpleCompoundScalar(CompoundScalar):
        p1: int

    @dataclass(frozen=True)
    class AnotherCompoundScalar(CompoundScalar):
        p2: str

    @dataclass(frozen=True)
    class MultipleInheritance(SimpleCompoundScalar, AnotherCompoundScalar):
        p3: int

    meta = MultipleInheritance.__meta_data_schema__
    assert len(meta) == 3
    assert meta["p1"] == HgScalarTypeMetaData.parse_type(int)
    assert meta["p2"] == HgScalarTypeMetaData.parse_type(str)
    assert meta["p3"] == HgScalarTypeMetaData.parse_type(int)


# def test_recursive_scalar():
#     @dataclass(frozen=True)
#     class RecursiveCompoundScalar(CompoundScalar):
#         p1: SCALAR
#         p2: ForwardRef("RecursiveCompoundScalar")
#
#     hg_meta = HgCompoundScalarType.parse_type(RecursiveCompoundScalar)
#     assert hg_meta.is_resolved
#     assert not hg_meta.is_atomic
