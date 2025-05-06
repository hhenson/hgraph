from dataclasses import dataclass
from typing import Generic

import pytest

from hgraph import (
    SCALAR,
    Size,
    SIZE,
    TIME_SERIES_TYPE,
    CompoundScalar,
    K,
    KEYABLE_SCALAR,
    TSW,
    TSW_OUT,
    WINDOW_SIZE,
    WINDOW_SIZE_MIN,
    WindowSize,
)
from hgraph._types._ts_type import TS, TS_OUT
from hgraph._types import HgTypeMetaData, TSL, TSL_OUT, TSD, TSD_OUT, TSS, TSS_OUT, TimeSeriesSchema, TSB, REF
from hgraph._types._typing_utils import clone_type_var


@dataclass
class SimpleSchema(TimeSeriesSchema):
    p1: TS[int]


@dataclass
class UnResolvedSchema(TimeSeriesSchema, Generic[TIME_SERIES_TYPE]):
    p2: TIME_SERIES_TYPE


TIME_SERIES_TYPE_2 = clone_type_var(TIME_SERIES_TYPE, "TIME_SERIES_TYPE_2")


@dataclass
class UnResolvedSchema2(TimeSeriesSchema, Generic[TIME_SERIES_TYPE, TIME_SERIES_TYPE_2]):
    p1: TIME_SERIES_TYPE
    p2: TIME_SERIES_TYPE_2


@dataclass
class UnResolvedCompoundScalar(CompoundScalar, Generic[SCALAR]):
    s1: SCALAR


SCALAR_2 = clone_type_var(SCALAR, "SCALAR_2")


@dataclass
class UnResolvedCompoundScalar2(CompoundScalar, Generic[SCALAR, SCALAR_2]):
    s1: SCALAR
    s2: SCALAR_2


@pytest.mark.parametrize(
    ("ts", "wiring_ts", "expected_dict"),
    [
        [TS[int], TS[int], {}],
        [TS[SCALAR], TS[int], {SCALAR: int}],
        [TS_OUT[SCALAR], TS_OUT[int], {SCALAR: int}],
        [
            TSW[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN],
            TSW[int, WindowSize[10]],
            {SCALAR: int, WINDOW_SIZE: WindowSize[10], WINDOW_SIZE_MIN: WindowSize[10]},
        ],
        [
            TSW_OUT[SCALAR],
            TSW_OUT[int, WindowSize[10], WindowSize[5]],
            {SCALAR: int, WINDOW_SIZE: WindowSize[10], WINDOW_SIZE_MIN: WindowSize[5]},
        ],
        [TSL[TS[int], Size[2]], TSL[TS[int], Size[2]], {}],
        [TSL[TS[SCALAR], Size[2]], TSL[TS[int], Size[2]], {SCALAR: int}],
        [TSL[TIME_SERIES_TYPE, Size[2]], TSL[TS[int], Size[2]], {TIME_SERIES_TYPE: TS[int]}],
        [TSL[TS[int], SIZE], TSL[TS[int], Size[2]], {SIZE: Size[2]}],
        [TSL_OUT[TS[int], SIZE], TSL_OUT[TS[int], Size[2]], {SIZE: Size[2]}],
        [TSS[int], TSS[int], {}],
        [TSS[KEYABLE_SCALAR], TSS[int], {KEYABLE_SCALAR: int}],
        [TSS_OUT[SCALAR], TSS_OUT[int], {SCALAR: int}],
        [TSD[str, TS[int]], TSD[str, TS[int]], {}],
        [TSD[str, TIME_SERIES_TYPE], TSD[str, TS[int]], {TIME_SERIES_TYPE: TS[int]}],
        [TSD[K, TS[int]], TSD[str, TS[int]], {K: str}],
        [TSD_OUT[SCALAR, TS[int]], TSD_OUT[str, TS[int]], {SCALAR: str}],
        [REF[TS[SCALAR]], REF[TS[int]], {SCALAR: int}],
        [REF[TIME_SERIES_TYPE], REF[TS[int]], {TIME_SERIES_TYPE: TS[int]}],
        [TSB[SimpleSchema], TSB[SimpleSchema], {}],
        [TSB[UnResolvedSchema], TSB[UnResolvedSchema[TS[int]]], {TIME_SERIES_TYPE: TS[int]}],
        [
            TSB[UnResolvedSchema2],
            TSB[UnResolvedSchema2[TS[int], TS[str]]],
            {TIME_SERIES_TYPE: TS[int], TIME_SERIES_TYPE_2: TS[str]},
        ],
        [
            TSB[UnResolvedSchema2[TS[int]]],
            TSB[UnResolvedSchema2[TS[int], TS[str]]],
            {TIME_SERIES_TYPE: TS[int], TIME_SERIES_TYPE_2: TS[str]},
        ],
        [TS[UnResolvedCompoundScalar], TS[UnResolvedCompoundScalar[int]], {SCALAR: int}],
        [TS[UnResolvedCompoundScalar2], TS[UnResolvedCompoundScalar2[int, str]], {SCALAR: int, SCALAR_2: str}],
        [TS[UnResolvedCompoundScalar2[int]], TS[UnResolvedCompoundScalar2[int, str]], {SCALAR: int, SCALAR_2: str}],
        [
            TS[UnResolvedCompoundScalar2[SCALAR_2:str]],
            TS[UnResolvedCompoundScalar2[int, str]],
            {SCALAR: int, SCALAR_2: str},
        ],
        [
            TS[UnResolvedCompoundScalar2[SCALAR_2:str, SCALAR:int]],
            TS[UnResolvedCompoundScalar2[int, str]],
            {},
        ],  # This is fully resolved
        [type[SCALAR], type[int], {SCALAR: int}],
        [type[int], type[int], {}],  # Already fully resolved
        [type[TS[SCALAR]], type[TS[int]], {SCALAR: int}],
    ],
)
def test_build_resolve_dict(ts, wiring_ts, expected_dict):
    # Convert to HgTypeMetaData values
    expected_dict = {k: HgTypeMetaData.parse_type(v) for k, v in expected_dict.items()}
    actual_dict = {}
    ts_meta_data = HgTypeMetaData.parse_type(ts)
    wiring_ts_meta_data = HgTypeMetaData.parse_type(wiring_ts)
    ts_meta_data.build_resolution_dict(actual_dict, wiring_ts_meta_data)

    assert actual_dict == expected_dict

    resolved_meta_data = ts_meta_data.resolve(actual_dict)
    assert resolved_meta_data == wiring_ts_meta_data


@pytest.mark.parametrize(
    ("ts", "wiring_ts", "expected_dict", "resolved_ts"),
    [
        [REF[TS[SCALAR]], TS[int], {SCALAR: int}, REF[TS[int]]],
        [TS[SCALAR], REF[TS[int]], {SCALAR: int}, TS[int]],
        [REF[TIME_SERIES_TYPE], TS[int], {TIME_SERIES_TYPE: TS[int]}, REF[TS[int]]],
        [TIME_SERIES_TYPE, REF[TS[int]], {TIME_SERIES_TYPE: TS[int]}, TS[int]],
    ],
)
def test_build_resolve_dict_ref(ts, wiring_ts, expected_dict, resolved_ts):
    # Convert to HgTypeMetaData values
    expected_dict = {k: HgTypeMetaData.parse_type(v) for k, v in expected_dict.items()}
    actual_dict = {}
    ts_meta_data = HgTypeMetaData.parse_type(ts)
    wiring_ts_meta_data = HgTypeMetaData.parse_type(wiring_ts)
    ts_meta_data.build_resolution_dict(actual_dict, wiring_ts_meta_data)

    assert actual_dict == expected_dict

    resolved_meta_data = ts_meta_data.resolve(actual_dict)
    assert resolved_meta_data == HgTypeMetaData.parse_type(resolved_ts)
