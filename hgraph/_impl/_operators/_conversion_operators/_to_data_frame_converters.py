"""
Tooling to convert to a dataframe from variable time-series types.
"""

from dataclasses import asdict
from datetime import date, datetime
from typing import Dict, Type, Callable

import polars as pl
from frozendict import frozendict
from polars import DataFrame
from polars.datatypes import dtype_to_py_type

from hgraph import (
    K,
    TS,
    Frame,
    COMPOUND_SCALAR,
    compute_node,
    SCALAR,
    AUTO_RESOLVE,
    TSB,
    TS_SCHEMA,
    TSD,
    compound_scalar,
    convert,
    OUT,
    COMPOUND_SCALAR_1,
    DEFAULT,
    combine,
    HgTypeMetaData,
    SCHEMA,
)

__all__ = (
    "convert_ts_to_frame",
    "convert_tsd_to_frame",
    "convert_df_to_frame",
    "convert_tsb_to_frame",
    "combine_frame",
)


def _ts_frame_cs_resolver(mapping, scalars):
    if HgTypeMetaData.parse_type(TS[Frame[COMPOUND_SCALAR]]).matches(mapping[OUT]):
        return mapping[OUT].value_scalar_tp.schema

    dt_col = scalars["dt_col"]
    value_col = scalars["value_col"]

    if value_col is None:
        raise ValueError("value_col cannot be None")
    schema = {value_col: mapping[SCALAR].py_type}
    dt_col = dt_col
    if dt_col is not None:
        schema = {dt_col: date if scalars["dt_is_date"] else datetime} | schema
    return compound_scalar(**schema)


def _ts_frame_cs_checker(mapping, scalars):
    dt_col = scalars["dt_col"]
    value_col = scalars["value_col"]
    schema = mapping[COMPOUND_SCALAR].meta_data_schema

    if len(schema) > 2:
        return f"to_frame_ts cannot have more than 2 columns in the schema definition, got: {schema}"
    if len(schema) < 1:
        return f"to_frame_ts cannot have less than 1 column in the schema definition, got: {schema}"
    if len(schema) == 1 and dt_col is not None:
        return f"to_frame_ts cannot have only one column with dt_col('{dt_col}') being defined"

    if dt_col is None and len(schema) == 2:
        scalars["dt_col"], dt_type = next(iter(schema.items()))

        if dt_type.py_type not in (date, datetime):
            raise RuntimeError(
                f"to_frame_ts type of dt_col('{dt_col}') is {dt_type}, which is not date or datetime as required"
            )

        scalars["dt_is_date"] = dt_type.py_type is date

    if value_col is None:
        v, t = next(i := iter(schema.items()))
        if len(schema) == 2:
            v, t = next(i)
        scalars["value_col"] = v

        if not t.matches(mapping[SCALAR]):
            return f"to_frame_ts(ts: TS[{mapping[SCALAR]}]) value_col('{v}') is not a compatible with {t}"

    return True


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type == TS[Frame] or _ts_frame_cs_checker(m, s),
    resolvers={COMPOUND_SCALAR: _ts_frame_cs_resolver},
)
def convert_ts_to_frame(
    ts: TS[SCALAR],
    value_col: str = None,
    dt_col: str = None,
    dt_is_date: bool = False,
    to: type[OUT] = DEFAULT[OUT],
    _tp_cs: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
) -> TS[Frame[COMPOUND_SCALAR]]:
    if dt_col:
        if dt_is_date:
            return pl.DataFrame({dt_col: [ts.last_modified_time.date()], value_col: [ts.value]})
        else:
            return pl.DataFrame({dt_col: [ts.last_modified_time], value_col: [ts.value]})
    else:
        return pl.DataFrame({value_col: [ts.value]})


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type == TS[Frame] or m[OUT].matches_type(TS[Frame[m[COMPOUND_SCALAR_1].py_type]]),
    resolvers={
        COMPOUND_SCALAR_1: lambda m, s: (
            m[COMPOUND_SCALAR].py_type if m[OUT].py_type == TS[Frame] else m[OUT].scalar_type().schema
        )
    },
)
def convert_tsd_to_frame(
    ts: TSD[K, TS[COMPOUND_SCALAR]],
    key_col: str = None,
    dt_col: str = None,
    mapping: Dict[str, str] = frozendict(),
    dt_is_date: bool = False,
    to: Type[OUT] = DEFAULT[OUT],
) -> TS[Frame[COMPOUND_SCALAR_1]]:
    data = []
    for k, v in ts.valid_items():
        data.append(
            ({mapping.get("key_col", key_col): k} if "key_col" in mapping or key_col else {})
            | (
                {mapping.get("dt_col", dt_col): ts.last_modified_time if dt_is_date else ts.last_modified_time.date()}
                if "dt_col" in mapping or dt_col
                else {}
            )
            | {
                mapping.get(k, k): v if isinstance(v, (bool, int, str, float, date, datetime)) else str(v)
                for k, v in v.value.to_dict().items()
            }
        )

    return pl.DataFrame(data)


def _tsb_frame_cs_resolver(mapping, scalars):
    if HgTypeMetaData.parse_type(TS[Frame[COMPOUND_SCALAR]]).matches(mapping[OUT]):
        return mapping[OUT].value_scalar_tp.schema

    _tsb_tp = mapping[TS_SCHEMA]
    tsb_schema = {k: v.scalar_type().py_type for k, v in _tsb_tp.py_type.__meta_data_schema__.items()}
    map_ = scalars["map_"]
    if map_:
        tsb_schema = {map_.get(k, k): v for k, v in tsb_schema.items()}
    dt_col = scalars["dt_col"]
    if dt_col:
        tsb_schema = {dt_col: date if scalars["dt_is_date"] else datetime} | tsb_schema
    return compound_scalar(**tsb_schema)


def _tsb_frame_cs_checker(mapping, scalars):
    tsb_schema = {k: v.scalar_type().py_type for k, v in mapping[TS_SCHEMA].meta_data_schema.items()}
    df_schema = {k: v.py_type for k, v in mapping[COMPOUND_SCALAR].meta_data_schema.items()}
    frame_schema = dict(df_schema)

    dt_col = scalars.get("dt_col")
    map_ = scalars.get("map_")

    if len(tsb_schema) + 1 == len(frame_schema):
        # Implies we are expecting date / dt col.
        if dt_col is None:
            dt_col = next(iter(frame_schema.keys()))
            scalars["dt_col"] = dt_col
        if frame_schema[dt_col] not in (date, datetime):
            raise RuntimeError(f"the dt_col('{dt_col}') is not a date or datetime as required")
        scalars["dt_is_date"] = frame_schema.pop(dt_col) is date

    if len(tsb_schema) == len(frame_schema):
        if tsb_schema == frame_schema:
            if dt_col is None:
                scalars["_to_frame"] = lambda ts: pl.DataFrame(
                    {k: [t.value if (t := ts[k]).valid else None] for k in tsb_schema.keys()}, schema=df_schema
                )
            else:
                scalars["_to_frame"] = lambda ts: pl.DataFrame(
                    {k: [t.value if (t := ts[k]).valid else None] for k in tsb_schema.keys()}
                    | {dt_col: [ts.last_modified_time if not scalars["dt_is_date"] else ts.last_modified_time.date()]},
                    schema=df_schema,
                )
        elif map_:
            if {map_.get(k, k): v for k, v in tsb_schema.items()} == frame_schema:
                if dt_col is None:
                    scalars["_to_frame"] = lambda ts: pl.DataFrame(
                        {map_.get(k, k): t.value if (t := ts[k]).valid else None for k in tsb_schema.keys()},
                        schema=df_schema,
                    )
                else:
                    scalars["_to_frame"] = lambda ts: pl.DataFrame(
                        {map_.get(k, k): t.value if (t := ts[k]).valid else None for k in tsb_schema.keys()}
                        | {
                            dt_col: [
                                ts.last_modified_time if not scalars["dt_is_date"] else ts.last_modified_time.date()
                            ]
                        },
                        schema=df_schema,
                    )
            else:
                return f"Unable to map from {tsb_schema} to {frame_schema} using the mapping {map_}"
        else:
            return f"No mapping provided to convert from {tsb_schema} to {frame_schema}"
    else:
        return (
            f"to_frame unable to map from {tsb_schema} to"
            f" {frame_schema} {'using the mapping ' if map_ else ''}{map_ if map_ else ''}"
        )

    return True


@compute_node(
    overloads=convert,
    requires=lambda m, s: (m[OUT].py_type == TS[Frame] or m[OUT].matches_type(TS[Frame[m[COMPOUND_SCALAR].py_type]]))
    and _tsb_frame_cs_checker(m, s),
    resolvers={COMPOUND_SCALAR: _tsb_frame_cs_resolver},
)
def convert_tsb_to_frame(
    ts: TSB[TS_SCHEMA],
    dt_col: str = None,
    map_: frozendict[str, str] = None,
    dt_is_date: bool = False,
    _tsb_tp: type[TS_SCHEMA] = AUTO_RESOLVE,
    _frame_tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
    _to_frame: Callable = None,
) -> TS[Frame[COMPOUND_SCALAR]]:
    return _to_frame(ts)


@compute_node(overloads=convert)
def convert_df_to_frame(
    ts: TS[DataFrame], _tp: Type[TS[Frame[SCHEMA]]] = DEFAULT[OUT], _schema: Type[SCHEMA] = AUTO_RESOLVE
) -> TS[Frame[SCHEMA]]:
    df: DataFrame = ts.value

    if df.schema.keys() != _schema.__meta_data_schema__.keys():
        raise ValueError(
            f"expected schema keys {_schema.__meta_data_schema__.keys()} does not match received frame with"
            f" {df.schema.keys()}"
        )

    wrong_types = []
    for k, v in _schema.__meta_data_schema__.items():
        if dtype_to_py_type(df.schema[k]) != v.py_type:
            wrong_types.append(
                f"{k}: schema type {v.py_type} does not match frame type {dtype_to_py_type(df.schema[k])}"
            )

    if wrong_types:
        raise ValueError(f"schemas do not match: {', '.join(wrong_types)}")

    return df


@compute_node(overloads=convert)
def convert_cs_to_frame(
    ts: TS[COMPOUND_SCALAR],
    _tp: Type[TS[Frame[COMPOUND_SCALAR]]] = DEFAULT[OUT],
    _cs: Type[COMPOUND_SCALAR] = AUTO_RESOLVE,
) -> TS[Frame[COMPOUND_SCALAR]]:
    return pl.DataFrame(asdict(ts.value))


def _check_schema(scalar, bundle):
    from hgraph import HgSeriesScalarTypeMetaData
    from hgraph import HgTupleCollectionScalarType

    if bundle.meta_data_schema.keys() - scalar.meta_data_schema.keys():
        return f"Extra fields: {bundle.meta_data_schema.keys() - scalar.meta_data_schema.keys()}"
    for k, t in scalar.meta_data_schema.items():
        if (kt := bundle.meta_data_schema.get(k)) is None:
            if getattr(scalar.py_type, k, None) is None:
                return f"Missing input: {k}"

        skt = kt if kt.is_scalar else kt.scalar_type()
        if isinstance(skt, HgSeriesScalarTypeMetaData):
            if not t.matches(skt.value_tp):
                return f"column {k} of type {t} does not accept {skt}"
        elif isinstance(skt, HgTupleCollectionScalarType):
            if not t.matches(skt.element_type):
                return f"column {k} of type {t} does not accept {skt}"
        elif not t.matches(skt):
            return f"column {k} of type {t} does not accept {skt}"

    return True


@compute_node(
    overloads=combine,
    requires=lambda m, s: _check_schema(m[COMPOUND_SCALAR], m[TS_SCHEMA]),
    all_valid=lambda m, s: ("bundle",) if s["__strict__"] else None,
)
def combine_frame(
    tp_out_: Type[TS[Frame[COMPOUND_SCALAR]]] = DEFAULT[OUT],
    tp_: Type[COMPOUND_SCALAR] = COMPOUND_SCALAR,
    __strict__: bool = True,
    **bundle: TSB[TS_SCHEMA],
) -> TS[Frame[COMPOUND_SCALAR]]:
    return pl.DataFrame(data={k: v.value for k, v in bundle.items()})
