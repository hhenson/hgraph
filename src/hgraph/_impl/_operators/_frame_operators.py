from typing import Type

from hgraph import (
    compute_node,
    Frame,
    TS,
    SCHEMA,
    SCALAR,
    AUTO_RESOLVE,
    Series,
    getitem_,
    getattr_,
    max_,
    min_,
)

__all__ = tuple()


@compute_node(
    overloads=getitem_,
    resolvers={SCALAR: lambda mapping, scalars: Series[mapping[SCHEMA].meta_data_schema[scalars["key"]].py_type]},
)
def get_frame_col(ts: TS[Frame[SCHEMA]], key: str) -> TS[SCALAR]:
    return ts.value[key]


@compute_node(
    overloads=getattr_,
    resolvers={SCALAR: lambda mapping, scalars: Series[mapping[SCHEMA].meta_data_schema[scalars["key"]].py_type]},
)
def get_frame_col(ts: TS[Frame[SCHEMA]], key: str) -> TS[SCALAR]:
    if not ts.value.is_empty():
        return ts.value[key]


@compute_node(overloads=getitem_)
def get_frame_item_(ts: TS[Frame[SCHEMA]], key: int, _tp: Type[SCHEMA] = AUTO_RESOLVE) -> TS[SCHEMA]:
    return _tp(**ts.value[key].to_dicts()[0])


@compute_node(overloads=getitem_)
def get_frame_item_ts_(ts: TS[Frame[SCHEMA]], key: TS[int], _tp: Type[SCHEMA] = AUTO_RESOLVE) -> TS[SCHEMA]:
    return _tp(**ts.value[key].to_dicts()[0])


@compute_node(overloads=min_)
def min_of_series(series: TS[Series[SCALAR]]) -> TS[SCALAR]:
    return series.value.min()


@compute_node(overloads=max_)
def max_of_series(series: TS[Series[SCALAR]]) -> TS[SCALAR]:
    return series.value.max()
