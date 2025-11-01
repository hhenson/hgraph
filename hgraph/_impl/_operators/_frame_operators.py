from typing import Type

from hgraph._wiring._decorators import compute_node
from hgraph._types._frame_scalar_type_meta_data import Frame
from hgraph._types._ts_type import TS
from hgraph._types._frame_scalar_type_meta_data import SCHEMA
from hgraph._types._scalar_types import SCALAR
from hgraph._types._type_meta_data import AUTO_RESOLVE
from hgraph._types._frame_scalar_type_meta_data import Series
from hgraph._operators._operators import getitem_, getattr_

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
    return ts.value[key]


@compute_node(overloads=getitem_)
def get_frame_item_(ts: TS[Frame[SCHEMA]], key: int, _tp: Type[SCHEMA] = AUTO_RESOLVE) -> TS[SCHEMA]:
    return _tp(**ts.value[key].to_dicts()[0])


@compute_node(overloads=getitem_)
def get_frame_item_ts_(ts: TS[Frame[SCHEMA]], key: TS[int], _tp: Type[SCHEMA] = AUTO_RESOLVE) -> TS[SCHEMA]:
    return _tp(**ts.value[key.value].to_dicts()[0])
