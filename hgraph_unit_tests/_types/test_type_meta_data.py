from collections.abc import Mapping as Mapping_, Set as Set_
from datetime import time, datetime, date, timedelta
from enum import Enum
from typing import Type, Tuple, FrozenSet, Set, Mapping, Dict, TypeVar, Union

import pytest
from frozendict import frozendict

from hgraph import TIME_SERIES_TYPE, HgTsTypeVarTypeMetaData, TimeSeries
from hgraph._runtime import EvaluationClock
from hgraph._types import TSL, TSL_OUT, TSD, TSD_OUT, TSS, TSS_OUT
from hgraph._types._tsw_meta_data import HgTSWTypeMetaData, HgTSWOutTypeMetaData
from hgraph._types._tsw_type import TSW, TSW_OUT
from hgraph._types._ref_meta_data import HgREFTypeMetaData
from hgraph._types._ref_type import REF
from hgraph._types._scalar_type_meta_data import (
    HgAtomicType,
    HgScalarTypeMetaData,
    HgTupleCollectionScalarType,
    HgTupleFixedScalarType,
    HgSetScalarType,
    HgDictScalarType,
    HgTypeOfTypeMetaData,
    HgInjectableType,
    HgArrayScalarTypeMetaData,
)
from hgraph._types._scalar_types import SIZE, Size, WindowSize
from hgraph._types._scalar_value import Array
from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._ts_meta_data import HgTSTypeMetaData, HgTSOutTypeMetaData
from hgraph._types._ts_type import TS, TS_OUT
from hgraph._types._tsd_meta_data import HgTSDTypeMetaData, HgTSDOutTypeMetaData
from hgraph._types._tsl_meta_data import HgTSLTypeMetaData, HgTSLOutTypeMetaData
from hgraph._types._tss_meta_data import HgTSSTypeMetaData, HgTSSOutTypeMetaData
from hgraph._types._type_meta_data import HgTypeMetaData


class MyEnum(Enum):
    A = "A"
    B = "B"


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        [bool, bool],
        [int, int],
        [float, float],
        [date, date],
        [datetime, datetime],
        [time, time],
        [timedelta, timedelta],
        [str, str],
        [MyEnum, MyEnum],
        [Size, Size],
        [WindowSize, WindowSize],
    ],
)
def test_atomic_scalars_type(value, expected: Type):
    meta_type = HgTypeMetaData.parse_type(value)
    assert meta_type is not None
    assert isinstance(meta_type, HgAtomicType)
    assert meta_type.py_type == expected


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        [True, bool],
        [False, bool],
        [0, int],
        [1, int],
        [0.0, float],
        [1.0, float],
        [date(2022, 6, 13), date],
        [datetime(2022, 6, 13, 10, 13, 0), datetime],
        [time(10, 13, 0), time],
        [timedelta(days=1), timedelta],
        [b"bytes", bytes],
        ["Test", str],
        [MyEnum.A, MyEnum],
        [Size[3], Size[3]],
        [WindowSize[10], WindowSize[10]],
    ],
)
def test_atomic_scalars_value(value, expected: Type):
    meta_type = HgTypeMetaData.parse_value(value)
    assert meta_type is not None
    assert isinstance(meta_type, HgAtomicType)
    assert meta_type.py_type == expected


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        [EvaluationClock, EvaluationClock],
    ],
)
def test_special_atomic_scalars(value, expected: Type):
    meta_type = HgTypeMetaData.parse_type(value)
    assert meta_type is not None
    assert isinstance(meta_type, HgInjectableType)
    assert meta_type.py_type == expected


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        [Tuple[bool, ...], HgTupleCollectionScalarType(HgScalarTypeMetaData.parse_type(bool))],
        [tuple[bool, ...], HgTupleCollectionScalarType(HgScalarTypeMetaData.parse_type(bool))],
        [
            Tuple[bool, int],
            HgTupleFixedScalarType([HgScalarTypeMetaData.parse_type(bool), HgScalarTypeMetaData.parse_type(int)]),
        ],
        [
            tuple[bool, int],
            HgTupleFixedScalarType([HgScalarTypeMetaData.parse_type(bool), HgScalarTypeMetaData.parse_type(int)]),
        ],
        [FrozenSet[bool], HgSetScalarType(HgScalarTypeMetaData.parse_type(bool))],
        [frozenset[bool], HgSetScalarType(HgScalarTypeMetaData.parse_type(bool))],
        [Set[bool], HgSetScalarType(HgScalarTypeMetaData.parse_type(bool))],
        [set[bool], HgSetScalarType(HgScalarTypeMetaData.parse_type(bool))],
        [
            Mapping[int, str],
            HgDictScalarType(HgScalarTypeMetaData.parse_type(int), HgScalarTypeMetaData.parse_type(str)),
        ],
        [Dict[int, str], HgDictScalarType(HgScalarTypeMetaData.parse_type(int), HgScalarTypeMetaData.parse_type(str))],
        [dict[int, str], HgDictScalarType(HgScalarTypeMetaData.parse_type(int), HgScalarTypeMetaData.parse_type(str))],
        [
            frozendict[int, str],
            HgDictScalarType(HgScalarTypeMetaData.parse_type(int), HgScalarTypeMetaData.parse_type(str)),
        ],
        [TS[bool], HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(bool))],
        [TS_OUT[bool], HgTSOutTypeMetaData(HgScalarTypeMetaData.parse_type(bool))],
        [
            TSW[bool, WindowSize[10]],
            HgTSWTypeMetaData(
                HgScalarTypeMetaData.parse_type(bool),
                HgScalarTypeMetaData.parse_type(WindowSize[10]),
                HgScalarTypeMetaData.parse_type(WindowSize[10]),
            ),
        ],
        [
            TSW_OUT[bool, WindowSize[10], WindowSize[5]],
            HgTSWOutTypeMetaData(
                HgScalarTypeMetaData.parse_type(bool),
                HgScalarTypeMetaData.parse_type(WindowSize[10]),
                HgScalarTypeMetaData.parse_type(WindowSize[5]),
            ),
        ],
        [
            TSL[TS[bool], SIZE],
            HgTSLTypeMetaData(
                HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(bool)), HgScalarTypeMetaData.parse_type(SIZE)
            ),
        ],
        [
            TSL_OUT[TS[bool], SIZE],
            HgTSLOutTypeMetaData(
                HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(bool)), HgScalarTypeMetaData.parse_type(SIZE)
            ),
        ],
        [TSS[bool], HgTSSTypeMetaData(HgScalarTypeMetaData.parse_type(bool))],
        [TSS_OUT[bool], HgTSSOutTypeMetaData(HgScalarTypeMetaData.parse_type(bool))],
        [
            TSD[int, TS[str]],
            HgTSDTypeMetaData(HgScalarTypeMetaData.parse_type(int), HgTimeSeriesTypeMetaData.parse_type(TS[str])),
        ],
        [
            TSD[int, TSL[TS[int], Size[2]]],
            HgTSDTypeMetaData(
                HgScalarTypeMetaData.parse_type(int), HgTimeSeriesTypeMetaData.parse_type(TSL[TS[int], Size[2]])
            ),
        ],
        [
            TSD_OUT[int, TS[str]],
            HgTSDOutTypeMetaData(HgScalarTypeMetaData.parse_type(int), HgTimeSeriesTypeMetaData.parse_type(TS[str])),
        ],
        [REF[TS[bool]], HgREFTypeMetaData(HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(bool)))],
        [Type[bool], HgTypeOfTypeMetaData(HgScalarTypeMetaData.parse_type(bool))],
        [type[bool], HgTypeOfTypeMetaData(HgScalarTypeMetaData.parse_type(bool))],
        [Array[int], HgArrayScalarTypeMetaData(HgScalarTypeMetaData.parse_type(int), tuple())],
        [
            Array[int, Size[1]],
            HgArrayScalarTypeMetaData(
                HgScalarTypeMetaData.parse_type(int), (HgScalarTypeMetaData.parse_type(Size[1]),)
            ),
        ],
        [
            Array[int, Size[1], SIZE],
            HgArrayScalarTypeMetaData(
                HgScalarTypeMetaData.parse_type(int),
                (HgScalarTypeMetaData.parse_type(Size[1]), HgScalarTypeMetaData.parse_type(SIZE)),
            ),
        ],
    ],
)
def test_collection_scalars(value, expected: HgScalarTypeMetaData):
    meta_type = HgTypeMetaData.parse_type(value)
    assert meta_type is not None
    assert meta_type == expected
    assert meta_type.matches(expected)


TEST_TS_TYPE = TypeVar("TEST_TS_TYPE", TS[int], TS[str])

@pytest.mark.parametrize(
    ["value", "expected"],
    [
        (TIME_SERIES_TYPE, HgTsTypeVarTypeMetaData(TIME_SERIES_TYPE, TimeSeries)),
        (TEST_TS_TYPE, HgTsTypeVarTypeMetaData(TEST_TS_TYPE, (HgTimeSeriesTypeMetaData.parse_type(TS[int]), HgTimeSeriesTypeMetaData.parse_type(TS[str])))),
        (Union[TS[int], TS[str]], HgTsTypeVarTypeMetaData(TEST_TS_TYPE, (HgTimeSeriesTypeMetaData.parse_type(TS[int]), HgTimeSeriesTypeMetaData.parse_type(TS[str])))),
        # (TS[int], HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))),
        # (TS[str], HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(str))),
    ],
)
def test_type_var_parse(value, expected):
    tp_meta = HgTypeMetaData.parse_type(value)
    assert tp_meta is not None
    assert isinstance(tp_meta, HgTsTypeVarTypeMetaData)
    assert tp_meta.matches(expected)

@pytest.mark.parametrize(
    "tp",
    [
        bool,
        int,
        float,
        bytes,
        str,
        date,
        datetime,
        time,
        timedelta,
        tuple[bool, ...],
        tuple[bool, int],
        Size[2],
        type[bool],
        EvaluationClock,
        Array[int],
        Array[int, Size[1]],
        Array[int, Size[1], SIZE],
    ],
)
def test_py_type(tp):
    tp_meta = HgTypeMetaData.parse_type(tp)
    parsed = tp_meta.py_type
    assert parsed == tp


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
    ],
)
def test_py_type_collections(tp, py_tp):
    assert py_tp == HgTypeMetaData.parse_type(tp).py_type


def test_size():
    sz = Size[20]
    assert sz.SIZE == 20
    assert sz.FIXED_SIZE

    sz2 = Size[20]
    assert sz is sz2

    assert sz.__name__ == "Size_20"


def test_array_type():
    with pytest.raises(TypeError):
        Array[int, 1]

    Array[int, Size[1], SIZE]
