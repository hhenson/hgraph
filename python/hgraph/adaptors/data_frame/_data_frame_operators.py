"""Python nodes for manipulating Arrow-backed :class:`~hgraph.Frame` values."""

import operator as operators
from typing import TypeVar

import _hgraph
import pyarrow.compute as pc

from hgraph._frame import as_arrow_table

from hgraph import (
    AUTO_RESOLVE,
    SCALAR,
    TS_SCHEMA,
    TSB,
    TSD,
    TS,
    Frame,
    KEYABLE_SCALAR,
    add_,
    and_,
    compute_node,
    div_,
    eq_,
    filter_,
    floordiv_,
    ge_,
    gt_,
    graph,
    le_,
    lt_,
    mul_,
    operator_function,
    or_,
    sub_,
)


__all__ = (
    "join",
    "filter_frame",
    "filter_cs",
    "filter_exp",
    "filter_exp_seq",
    "group_by",
    "ungroup",
    "sorted_",
    "concat",
    "with_columns",
)


ROW = TypeVar("ROW")
ROW_1 = TypeVar("ROW_1")
ROW_2 = TypeVar("ROW_2")

# hgraph parity: the join-key type variable. Upstream constrains the
# expression form to a polars expression; this port's expression dialect is
# ``pyarrow.compute.Expression`` throughout.
ON_TYPE = TypeVar("ON_TYPE", str, tuple, pc.Expression)


def _join_signature(lhs, rhs, on, how="inner", suffix="_right"):
    pass


join = operator_function("join", signature=_join_signature)


_filter_frame_native = operator_function("filter_frame")


def _pack_tsb(values):
    from hgraph._wiring._core import WiringPort, _unwrap

    ports = {name: _unwrap(value) for name, value in values.items()}
    schema = _hgraph.un_named_tsb_type(
        [(name, port.ts_type) for name, port in ports.items()]
    )
    return WiringPort(_hgraph.tsb_port(schema, ports))


@graph
def filter_frame(ts: TS[Frame[ROW]], **predicate: TSB[TS_SCHEMA]) -> TS[Frame[ROW]]:
    return _filter_frame_native(ts, _pack_tsb(predicate))


_filter_cs_native = operator_function("filter_cs")


@graph
def filter_cs(ts: TS[Frame[ROW]], predicate: TS[ROW]) -> TS[Frame[ROW]]:
    return _filter_cs_native(ts, predicate)


@compute_node
def filter_exp(ts: TS[Frame[ROW]], predicate: pc.Expression) -> TS[Frame[ROW]]:
    return as_arrow_table(ts.value).filter(predicate)


@compute_node
def filter_exp_ts(ts: TS[Frame[ROW]], predicate: TS[pc.Expression]) -> TS[Frame[ROW]]:
    return as_arrow_table(ts.value).filter(predicate.value)


@compute_node(overloads=filter_)
def filter_exp_ts_(condition: TS[pc.Expression], ts: TS[Frame[ROW]]) -> TS[Frame[ROW]]:
    return as_arrow_table(ts.value).filter(condition.value)


for _op, _impl in (
    (lt_, operators.lt),
    (gt_, operators.gt),
    (le_, operators.le),
    (ge_, operators.ge),
    (eq_, operators.eq),
    (add_, operators.add),
    (sub_, operators.sub),
    (mul_, operators.mul),
    (div_, operators.truediv),
    (floordiv_, operators.floordiv),
    (and_, operators.and_),
    (or_, operators.or_),
):

    @compute_node(overloads=_op)
    def _arrow_expression_rhs(lhs: TS[pc.Expression], rhs: TS[SCALAR], op: object = _impl) -> TS[pc.Expression]:
        return op(lhs.value, pc.scalar(rhs.value))

    @compute_node(overloads=_op)
    def _arrow_expression_lhs(lhs: TS[SCALAR], rhs: TS[pc.Expression], op: object = _impl) -> TS[pc.Expression]:
        return op(pc.scalar(lhs.value), rhs.value)


@compute_node
def filter_exp_seq(ts: TS[Frame[ROW]], predicate: tuple[pc.Expression, ...]) -> TS[Frame[ROW]]:
    expression = None
    for term in predicate:
        expression = term if expression is None else expression & term
    frame = as_arrow_table(ts.value)
    return frame if expression is None else frame.filter(expression)


def _group_by_signature(ts, by):
    pass


group_by = operator_function("group_by", signature=_group_by_signature)


def tuple_resolver(m, by):
    """hgraph parity: resolve the grouped-key tuple type for a tuple ``by``.

    The native ``group_by`` performs this resolution itself; the callable is
    retained so upstream-compatible code (custom overloads copying the
    upstream pattern) keeps importing and working.
    """
    if by.__class__ is tuple:
        from hgraph.reflection import fields

        row = m.get("ROW", m.get("COMPOUND_SCALAR"))
        schema = fields(row)
        return tuple[tuple(schema[b] for b in by)]


def group_by_single(ts, by):
    """hgraph parity: ``group_by`` keyed by one column (native dispatch)."""
    return group_by(ts, by)


def group_by_tuple(ts, by):
    """hgraph parity: ``group_by`` keyed by a column tuple (native dispatch)."""
    return group_by(ts, by)


def _ungroup_signature(ts):
    pass


ungroup = operator_function("ungroup", signature=_ungroup_signature)


def ungroup_default(ts):
    """hgraph parity: ``ungroup`` discarding the keys (native dispatch)."""
    return ungroup(ts)


def ungroup_with_key(ts, key_col, _tp_out=None):
    """hgraph parity: ``ungroup`` materializing the key into ``key_col``."""
    if _tp_out is not None:
        return ungroup[TS[Frame[_tp_out]]](ts, key_col)
    return ungroup(ts, key_col)


def ungroup_with_keys(ts, key_col, _tp_out=None):
    """hgraph parity: ``ungroup`` materializing a tuple key into ``key_col``."""
    if _tp_out is not None:
        return ungroup[TS[Frame[_tp_out]]](ts, key_col)
    return ungroup(ts, key_col)


def ungroup_from_items(ts):
    """hgraph parity: build a frame from a TSD of compound-scalar items."""
    return ungroup(ts)


def _explicit_output_type(mapping, _tp_out):
    return _tp_out


@graph(overloads=ungroup, resolvers={ROW_1: _explicit_output_type})
def _ungroup_typed(
    ts: TSD[KEYABLE_SCALAR, TS[Frame[ROW]]], key_col: str, _tp_out: type[ROW_1] = AUTO_RESOLVE
) -> TS[Frame[ROW_1]]:
    return ungroup[TS[Frame[_tp_out]]](ts, key_col)


@graph(overloads=ungroup, resolvers={ROW_1: _explicit_output_type})
def _ungroup_typed_tuple(
    ts: TSD[KEYABLE_SCALAR, TS[Frame[ROW]]], key_col: tuple[str, ...], _tp_out: type[ROW_1] = AUTO_RESOLVE
) -> TS[Frame[ROW_1]]:
    return ungroup[TS[Frame[_tp_out]]](ts, key_col)


def _sorted_signature(ts, by, descending=False):
    pass


sorted_ = operator_function("sorted_", signature=_sorted_signature)


def _concat_signature(ts1, ts2):
    pass


concat = operator_function("concat", signature=_concat_signature)


def concat_frames(ts1, ts2):
    """hgraph parity: append the rows of two same-schema frames."""
    return concat(ts1, ts2)


def _with_columns_signature(ts, **columns):
    pass


with_columns = operator_function("with_columns", signature=_with_columns_signature)


def with_columns_default(ts, **columns):
    """hgraph parity: replace/add columns keeping the input row schema."""
    return with_columns(ts, **columns)


def with_columns_typed(ts, _tp_out=None, **columns):
    """hgraph parity: replace/add columns projecting to ``_tp_out``."""
    if _tp_out is not None:
        return with_columns(ts, _tp_out=_tp_out, **columns)
    return with_columns(ts, **columns)


def _columns_output_type(mapping, _tp_out):
    return mapping["ROW"] if _tp_out is AUTO_RESOLVE else _tp_out


@graph(overloads=with_columns, resolvers={ROW_1: _columns_output_type})
def _with_columns_adapter(
    ts: TS[Frame[ROW]], _tp_out: type[ROW_1] = AUTO_RESOLVE, **columns: TSB[TS_SCHEMA]
) -> TS[Frame[ROW_1]]:
    return with_columns[TS[Frame[_tp_out]]](ts, _pack_tsb(columns))
