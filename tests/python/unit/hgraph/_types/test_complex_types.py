from dataclasses import dataclass

from hgraph import CompoundScalar, SCALAR, WiringPort, WiringNodeInstance, HgTimeSeriesTypeMetaData, WiringGraphContext
from hgraph._types import HgScalarTypeMetaData, HgCompoundScalarType, TimeSeriesSchema, TSB, is_bundle
from hgraph._types._scalar_types import is_compound_scalar
from hgraph._types._ts_type import TS
from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext


@dataclass
class SimpleSchema(TimeSeriesSchema):
    p1: TS[int]


@dataclass
class LessSimpleBundle(SimpleSchema):
    p2: TS[str]
    p3: SimpleSchema


def test_matches_bundle():
    tp = HgTimeSeriesTypeMetaData.parse_type(LessSimpleBundle)
    assert tp.matches(tp)


def test_simple_bundle():
    assert is_bundle(TSB[SimpleSchema])
    assert is_bundle(TSB[LessSimpleBundle])
    from hgraph.nodes import const
    with WiringNodeInstanceContext(), WiringGraphContext(None):
        p1 = const(1)
        b1 = TSB[SimpleSchema].from_ts(p1=p1)
        assert b1.__schema__ == SimpleSchema
        assert b1.p1.__orig_eq__(p1)
        assert b1.as_schema.p1.__orig_eq__(p1)


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
class UnResolvedCompoundScalar(CompoundScalar):
    p1: SCALAR
    p2: str


def test_unresolved_compound_scalar():
    hg_meta = HgCompoundScalarType.parse_type(UnResolvedCompoundScalar)
    assert not hg_meta.is_resolved
