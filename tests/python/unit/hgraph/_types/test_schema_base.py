from dataclasses import dataclass
from typing import Generic

from hgraph import CompoundScalar, COMPOUND_SCALAR, Base, HgTypeMetaData, TimeSeriesSchema
from hgraph._types._scalar_types import COMPOUND_SCALAR_1


def test_schema_base():
    @dataclass
    class SimpleCompoundScalar(CompoundScalar):
        p1: int

    @dataclass
    class GenericallyDerivedCompoundScalar(Base[COMPOUND_SCALAR], Generic[COMPOUND_SCALAR]):
        p2: int


    tp = GenericallyDerivedCompoundScalar[SimpleCompoundScalar]


    assert 'p1' in tp.__meta_data_schema__
    assert 'p2' in tp.__meta_data_schema__
    assert issubclass(tp, SimpleCompoundScalar)
    assert issubclass(tp, GenericallyDerivedCompoundScalar)


def test_schema_base_generic():
    @dataclass
    class SimpleCompoundScalar(CompoundScalar):
        p1: int

    @dataclass
    class GenericallyDerivedCompoundScalar(Base[COMPOUND_SCALAR], Generic[COMPOUND_SCALAR]):
        p2: int

    tp = GenericallyDerivedCompoundScalar[COMPOUND_SCALAR]

    meta = HgTypeMetaData.parse_type(tp)
    assert not meta.is_resolved
    assert meta.typevars == {COMPOUND_SCALAR}

    tp1 = tp[SimpleCompoundScalar]

    assert issubclass(tp1, SimpleCompoundScalar)
    assert issubclass(tp1, GenericallyDerivedCompoundScalar)

    meta1 = HgTypeMetaData.parse_type(tp1)
    assert meta1.is_resolved is True
    assert meta1.typevars == set()

    tp2 = GenericallyDerivedCompoundScalar[COMPOUND_SCALAR_1]
    assert tp2.__parameters__ == (COMPOUND_SCALAR_1,)

    meta2 = HgTypeMetaData.parse_type(tp2)
    assert not meta2.is_resolved
    assert meta2.typevars == {COMPOUND_SCALAR_1}

    tp3 = tp2[SimpleCompoundScalar]

    assert issubclass(tp3, SimpleCompoundScalar)
    assert issubclass(tp3, GenericallyDerivedCompoundScalar)
    assert tp3.__base_resolution_meta__ == HgTypeMetaData.parse_type(SimpleCompoundScalar)

    meta3 = HgTypeMetaData.parse_type(tp3)
    assert meta3.is_resolved is True
    assert meta3.typevars == set()

    tp4 = TimeSeriesSchema.from_scalar_schema(tp2)

    meta4 = HgTypeMetaData.parse_type(tp4)
    assert not meta4.is_resolved
    assert meta4.typevars == {COMPOUND_SCALAR_1}

    tp5 = tp4[SimpleCompoundScalar]
    assert tp5.__base_resolution_meta__ == HgTypeMetaData.parse_type(TimeSeriesSchema.from_scalar_schema(SimpleCompoundScalar))
    assert tp5.scalar_type() == tp2[SimpleCompoundScalar]

    assert 'p1' in tp5.__meta_data_schema__
    assert 'p2' in tp5.__meta_data_schema__

    meta5 = HgTypeMetaData.parse_type(tp5)
    assert meta5.is_resolved is True
    assert meta5.typevars == set()

    resolution_dict = {}
    meta4.build_resolution_dict(resolution_dict, meta5)
    assert resolution_dict == {
        COMPOUND_SCALAR_1: HgTypeMetaData.parse_type(SimpleCompoundScalar)
    }

    meta6 = meta4.resolve(resolution_dict)
    assert meta6 == meta5
    assert meta6.scalar_type() == meta5.scalar_type()

