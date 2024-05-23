from dataclasses import dataclass
from typing import Generic

from hgraph import CompoundScalar, COMPOUND_SCALAR, Base


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

