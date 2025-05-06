import json
from dataclasses import dataclass
from typing import Generic

from hgraph import (
    CompoundScalar,
    COMPOUND_SCALAR,
    Base,
    HgTypeMetaData,
    TimeSeriesSchema,
    TS,
    HgCompoundScalarType,
    from_json_builder,
)
from hgraph._impl._operators._to_json import to_json_converter, from_json_converter
from hgraph._types._scalar_types import COMPOUND_SCALAR_1
from frozendict import frozendict as fd


def test_schema_base():
    @dataclass
    class SimpleCompoundScalar(CompoundScalar):
        p1: int

    @dataclass
    class GenericallyDerivedCompoundScalar(Base[COMPOUND_SCALAR], Generic[COMPOUND_SCALAR]):
        p2: int

    tp = GenericallyDerivedCompoundScalar[SimpleCompoundScalar]

    assert "p1" in tp.__meta_data_schema__
    assert "p2" in tp.__meta_data_schema__
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
    assert meta.type_vars == {COMPOUND_SCALAR}

    tp1 = tp[SimpleCompoundScalar]

    assert issubclass(tp1, SimpleCompoundScalar)
    assert issubclass(tp1, GenericallyDerivedCompoundScalar)

    meta1 = HgTypeMetaData.parse_type(tp1)
    assert meta1.is_resolved is True
    assert meta1.type_vars == set()

    tp2 = GenericallyDerivedCompoundScalar[COMPOUND_SCALAR_1]
    assert tp2.__parameters__ == (COMPOUND_SCALAR_1,)

    meta2 = HgTypeMetaData.parse_type(tp2)
    assert not meta2.is_resolved
    assert meta2.type_vars == {COMPOUND_SCALAR_1}

    tp3 = tp2[SimpleCompoundScalar]

    assert issubclass(tp3, SimpleCompoundScalar)
    assert issubclass(tp3, GenericallyDerivedCompoundScalar)
    assert tp3.__base_resolution_meta__ == HgTypeMetaData.parse_type(SimpleCompoundScalar)

    meta3 = HgTypeMetaData.parse_type(tp3)
    assert meta3.is_resolved is True
    assert meta3.type_vars == set()

    tp4 = TimeSeriesSchema.from_scalar_schema(tp2)

    meta4 = HgTypeMetaData.parse_type(tp4)
    assert not meta4.is_resolved
    assert meta4.type_vars == {COMPOUND_SCALAR_1}

    tp5 = tp4[SimpleCompoundScalar]
    assert tp5.__base_resolution_meta__ == HgTypeMetaData.parse_type(
        TimeSeriesSchema.from_scalar_schema(SimpleCompoundScalar)
    )
    assert tp5.scalar_type() == tp2[SimpleCompoundScalar]

    assert "p1" in tp5.__meta_data_schema__
    assert "p2" in tp5.__meta_data_schema__

    meta5 = HgTypeMetaData.parse_type(tp5)
    assert meta5.is_resolved is True
    assert meta5.type_vars == set()

    resolution_dict = {}
    meta4.build_resolution_dict(resolution_dict, meta5)
    assert resolution_dict == {COMPOUND_SCALAR_1: HgTypeMetaData.parse_type(SimpleCompoundScalar)}

    meta6 = meta4.resolve(resolution_dict)
    assert meta6 == meta5
    assert meta6.scalar_type() == meta5.scalar_type()


def test_deep_schema_base():
    @dataclass
    class SimpleCompoundScalar(CompoundScalar):
        p1: int

    @dataclass
    class GenericallyDerivedCompoundScalar(Base[COMPOUND_SCALAR], Generic[COMPOUND_SCALAR]):
        p2: int

    p1 = GenericallyDerivedCompoundScalar[GenericallyDerivedCompoundScalar[COMPOUND_SCALAR]]
    p2 = p1[SimpleCompoundScalar]

    assert p2


def test_mixed_schema_base():
    @dataclass
    class SimpleCompoundScalar(TimeSeriesSchema):
        m1: TS[int]

    @dataclass
    class GenericallyDerivedCompoundScalar(Base[COMPOUND_SCALAR], Generic[COMPOUND_SCALAR]):
        m2: int

    p1 = GenericallyDerivedCompoundScalar[COMPOUND_SCALAR]
    p2 = p1[SimpleCompoundScalar]

    assert p2.__meta_data_schema__ == {
        "m1": HgTypeMetaData.parse_type(TS[int]),
        "m2": HgTypeMetaData.parse_type(TS[int]),
    }


def test_serialise_parent_child_schema():
    @dataclass
    class SimpleCompoundScalar(CompoundScalar):
        __serialise_base__ = True
        p1: int

    @dataclass
    class LessSimpleCompoundScalar(SimpleCompoundScalar):
        p2: float

    assert SimpleCompoundScalar.__meta_data_schema__ == fd({"p1": HgTypeMetaData.parse_type(int)})
    assert LessSimpleCompoundScalar.__meta_data_schema__ == fd(
        {"p1": HgTypeMetaData.parse_type(int), "p2": HgTypeMetaData.parse_type(float)}
    )
    assert SimpleCompoundScalar.__serialise_discriminator_field__ == "__type__"
    assert SimpleCompoundScalar.__serialise_children__ == {"LessSimpleCompoundScalar": LessSimpleCompoundScalar}
    assert LessSimpleCompoundScalar.__serialise_base__ == False
    assert SimpleCompoundScalar.__serialise_base__ == True
    json_builder = to_json_converter(HgCompoundScalarType(SimpleCompoundScalar))
    s = json_builder(LessSimpleCompoundScalar(p1=1, p2=2.0))
    assert s == '{"__type__": "LessSimpleCompoundScalar", "p1": 1, "p2": 2.0}'
    v = from_json_converter(HgCompoundScalarType(SimpleCompoundScalar))(json.loads(s))
    assert v == LessSimpleCompoundScalar(p1=1, p2=2.0)


def test_serialise_parent_child_schema_with_discriminator():
    @dataclass
    class SimpleCompoundScalar(CompoundScalar):
        __serialise_base__ = True
        __serialise_discriminator_field__ = "name"
        p1: int

    @dataclass
    class LessSimpleCompoundScalar(SimpleCompoundScalar):
        name = "LSCS"
        p2: float = 1.0

    assert SimpleCompoundScalar.__meta_data_schema__ == fd({"p1": HgTypeMetaData.parse_type(int)})
    assert LessSimpleCompoundScalar.__meta_data_schema__ == fd(
        {"p1": HgTypeMetaData.parse_type(int), "p2": HgTypeMetaData.parse_type(float)}
    )
    assert SimpleCompoundScalar.__serialise_discriminator_field__ == "name"
    assert SimpleCompoundScalar.__serialise_children__ == {"LSCS": LessSimpleCompoundScalar}
    assert LessSimpleCompoundScalar.__serialise_base__ == False
    assert SimpleCompoundScalar.__serialise_base__ == True
    json_builder = to_json_converter(HgCompoundScalarType(SimpleCompoundScalar))
    s = json_builder(LessSimpleCompoundScalar(p1=1, p2=2.0))
    assert s == '{"name": "LSCS", "p1": 1, "p2": 2.0}'
    v = from_json_converter(HgCompoundScalarType(SimpleCompoundScalar))(json.loads(s))
    assert v == LessSimpleCompoundScalar(p1=1, p2=2.0)


def test_serialise_parent_child_schema_with_discriminator_in_schema():
    @dataclass
    class SimpleCompoundScalar(CompoundScalar):
        __serialise_base__ = True
        __serialise_discriminator_field__ = "name"
        p1: int
        name: str

    @dataclass
    class LessSimpleCompoundScalar(SimpleCompoundScalar):
        name: str = "LSCS"
        p2: float = 1.0

    assert SimpleCompoundScalar.__meta_data_schema__ == fd(
        {"p1": HgTypeMetaData.parse_type(int), "name": HgTypeMetaData.parse_type(str)}
    )
    assert LessSimpleCompoundScalar.__meta_data_schema__ == fd({
        "p1": HgTypeMetaData.parse_type(int),
        "p2": HgTypeMetaData.parse_type(float),
        "name": HgTypeMetaData.parse_type(str),
    })
    assert SimpleCompoundScalar.__serialise_discriminator_field__ == "name"
    assert SimpleCompoundScalar.__serialise_children__ == {"LSCS": LessSimpleCompoundScalar}
    assert LessSimpleCompoundScalar.__serialise_base__ == False
    assert SimpleCompoundScalar.__serialise_base__ == True
    json_builder = to_json_converter(HgCompoundScalarType(SimpleCompoundScalar))
    s = json_builder(LessSimpleCompoundScalar(p1=1, p2=2.0))
    assert s == '{"p1": 1, "name": "LSCS", "p2": 2.0}'
    v = from_json_converter(HgCompoundScalarType(SimpleCompoundScalar))(json.loads(s))
    assert v == LessSimpleCompoundScalar(p1=1, p2=2.0)


def test_from_dict():
    @dataclass
    class SimpleCompoundScalar(CompoundScalar):
        p1: int

    @dataclass
    class LessSimpleCompoundScalar(CompoundScalar):
        p2: float
        p3: SimpleCompoundScalar

    v = LessSimpleCompoundScalar.from_dict({"p2": 2.0, "p3": {"p1": 1}})
    assert v == LessSimpleCompoundScalar(p2=2.0, p3=SimpleCompoundScalar(p1=1))
