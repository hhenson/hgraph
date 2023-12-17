from collections.abc import Mapping as Mapping_, Set as Set_
from datetime import time, datetime, date, timedelta

from frozendict import frozendict
from typing import Type, Tuple, FrozenSet, Set, Mapping, Dict

import pytest

from hgraph import SIZE, Size
from hgraph._runtime import EvaluationClock
from hgraph._types._ref_meta_data import HgREFTypeMetaData
from hgraph._types._ref_type import REF
from hgraph._types._ts_type import TS, TS_OUT
from hgraph._types import TSL, TSL_OUT, TSD, TSD_OUT, TSS, TSS_OUT
from hgraph._types._scalar_type_meta_data import HgAtomicType, HgScalarTypeMetaData, HgTupleCollectionScalarType, \
    HgTupleFixedScalarType, HgSetScalarType, HgDictScalarType, HgTypeOfTypeMetaData, HgInjectableType
from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._tsd_meta_data import HgTSDTypeMetaData, HgTSDOutTypeMetaData
from hgraph._types._tss_meta_data import HgTSSTypeMetaData, HgTSSOutTypeMetaData
from hgraph._types._tsl_meta_data import HgTSLTypeMetaData, HgTSLOutTypeMetaData
from hgraph._types._ts_meta_data import HgTSTypeMetaData, HgTSOutTypeMetaData
from hgraph._types._type_meta_data import HgTypeMetaData


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        [bool, bool],
        [True, bool],
        [False, bool],
        [int, int],
        [0, int],
        [1, int],
        [float, float],
        [0.0, float],
        [1.0, float],
        [date, date],
        [date(2022, 6, 13), date],
        [datetime, datetime],
        [datetime(2022, 6, 13, 10, 13, 0), datetime],
        [time, time],
        [time(10, 13, 0), time],
        [timedelta, timedelta],
        [timedelta(days=1), timedelta],
        [str, str],
        ["Test", str],
    ]
)
def test_atomic_scalars(value, expected: Type):
    meta_type = HgTypeMetaData.parse(value)
    assert meta_type is not None
    assert isinstance(meta_type, HgAtomicType)
    assert meta_type.py_type == expected


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        [EvaluationClock, EvaluationClock],
    ]
)
def test_special_atomic_scalars(value, expected: Type):
    meta_type = HgTypeMetaData.parse(value)
    assert meta_type is not None
    assert isinstance(meta_type, HgInjectableType)
    assert meta_type.py_type == expected


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        [Tuple[bool, ...], HgTupleCollectionScalarType(HgScalarTypeMetaData.parse(bool))],
        [tuple[bool, ...], HgTupleCollectionScalarType(HgScalarTypeMetaData.parse(bool))],
        [Tuple[bool, int], HgTupleFixedScalarType([HgScalarTypeMetaData.parse(bool), HgScalarTypeMetaData.parse(int)])],
        [tuple[bool, int], HgTupleFixedScalarType([HgScalarTypeMetaData.parse(bool), HgScalarTypeMetaData.parse(int)])],
        [FrozenSet[bool], HgSetScalarType(HgScalarTypeMetaData.parse(bool))],
        [frozenset[bool], HgSetScalarType(HgScalarTypeMetaData.parse(bool))],
        [Set[bool], HgSetScalarType(HgScalarTypeMetaData.parse(bool))],
        [set[bool], HgSetScalarType(HgScalarTypeMetaData.parse(bool))],
        [Mapping[int, str], HgDictScalarType(HgScalarTypeMetaData.parse(int), HgScalarTypeMetaData.parse(str))],
        [Dict[int, str], HgDictScalarType(HgScalarTypeMetaData.parse(int), HgScalarTypeMetaData.parse(str))],
        [dict[int, str], HgDictScalarType(HgScalarTypeMetaData.parse(int), HgScalarTypeMetaData.parse(str))],
        [frozendict[int, str], HgDictScalarType(HgScalarTypeMetaData.parse(int), HgScalarTypeMetaData.parse(str))],
        [TS[bool], HgTSTypeMetaData(HgScalarTypeMetaData.parse(bool))],
        [TS_OUT[bool], HgTSOutTypeMetaData(HgScalarTypeMetaData.parse(bool))],
        [TSL[TS[bool], SIZE], HgTSLTypeMetaData(HgTSTypeMetaData(HgScalarTypeMetaData.parse(bool)), HgScalarTypeMetaData.parse(SIZE))],
        [TSL_OUT[TS[bool], SIZE], HgTSLOutTypeMetaData(HgTSTypeMetaData(HgScalarTypeMetaData.parse(bool)), HgScalarTypeMetaData.parse(SIZE))],
        [TSS[bool], HgTSSTypeMetaData(HgScalarTypeMetaData.parse(bool))],
        [TSS_OUT[bool], HgTSSOutTypeMetaData(HgScalarTypeMetaData.parse(bool))],
        [TSD[int, TS[str]], HgTSDTypeMetaData(HgScalarTypeMetaData.parse(int), HgTimeSeriesTypeMetaData.parse(TS[str]))],
        [TSD[int, TSL[TS[int], Size[2]]], HgTSDTypeMetaData(HgScalarTypeMetaData.parse(int), HgTimeSeriesTypeMetaData.parse(TSL[TS[int], Size[2]]))],
        [TSD_OUT[int, TS[str]], HgTSDOutTypeMetaData(HgScalarTypeMetaData.parse(int), HgTimeSeriesTypeMetaData.parse(TS[str]))],
        [REF[TS[bool]], HgREFTypeMetaData(HgTSTypeMetaData(HgScalarTypeMetaData.parse(bool)))],
        [Type[bool], HgTypeOfTypeMetaData(HgScalarTypeMetaData.parse(bool))],
        [type[bool], HgTypeOfTypeMetaData(HgScalarTypeMetaData.parse(bool))],
    ]
)
def test_collection_scalars(value, expected: HgScalarTypeMetaData):
    meta_type = HgTypeMetaData.parse(value)
    assert meta_type is not None
    assert meta_type == expected
    assert meta_type.matches(expected)


@pytest.mark.parametrize(
    "tp",
    [
        bool,
        int,
        float,
        str,
        date,
        datetime,
        time,
        timedelta,
        tuple[bool, ...],
        tuple[bool, int],
        Size[2],
        type[bool],
        EvaluationClock
    ]
)
def test_py_type(tp):
    assert tp == HgTypeMetaData.parse(tp).py_type


@pytest.mark.parametrize(
    ["tp", "py_tp"],
    [
        [frozendict[int, str], Mapping_[int, str]],
        [dict[int, str], Mapping_[int, str]],
        [Mapping_[int, str], Mapping_[int, str]],
        [Mapping[int, str], Mapping_[int, str]],
        [frozenset[int], Set_[int]],
        [set[int], Set_[int]],
        [Set[int], Set_[int]],
    ]
)
def test_py_type_collections(tp, py_tp):
    assert py_tp == HgTypeMetaData.parse(tp).py_type

def test_size():
    sz = Size[20]
    assert sz.SIZE == 20
    assert sz.FIXED_SIZE

    sz2 = Size[20]
    assert sz is sz2

    assert sz.__name__ == 'Size_20'
