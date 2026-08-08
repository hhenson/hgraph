"""Typed Perspective row publication and editable-table feedback."""

from collections import defaultdict
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
import threading
import typing

import pyarrow as pa

from hgraph import (
    EvaluationClock,
    GlobalState,
    K,
    Removed,
    STATE,
    TIME_SERIES_TYPE,
    TS,
    TSB,
    TSD,
    TSS,
    TimeSeriesSchema,
    push_queue,
    sink_node,
)
from hgraph.reflection import (
    dereference,
    fields,
    frame_schema,
    is_bundle,
    is_compound_scalar,
    is_frame,
    is_ts,
    key_type,
    scalar_type,
    value_type,
)

from ._perspective import PerspectiveTablesManager

__all__ = ("TableEdits", "defaultdbldict")


class TableEdits(TimeSeriesSchema, typing.Generic[K, TIME_SERIES_TYPE]):
    """Typed edits and removals emitted by an editable Perspective table."""

    edits: TSD[K, TIME_SERIES_TYPE]
    removes: TSS[K]


class defaultdbldict(defaultdict):
    """A one-to-one defaultdict with reverse lookup, used for empty rows."""

    def __init__(self, default_factory):
        super().__init__(default_factory)
        self.key_tracker = {}

    def __missing__(self, key):
        value = super().__missing__(key)
        self.key_tracker[value] = key
        return value

    def __setitem__(self, key, value):
        old_key = self.key_tracker.get(value)
        if old_key is not None and old_key != key:
            super().__delitem__(old_key)
        result = super().__setitem__(key, value)
        self.key_tracker[value] = key
        return result

    def __delitem__(self, key):
        value = self.get(key)
        result = super().__delitem__(key)
        self.key_tracker.pop(value, None)
        return result

    def reverse_get(self, value):
        return self.key_tracker.get(value)


def _cast(value, target):
    if value is None:
        return None
    try:
        return value if isinstance(value, target) else target(value)
    except TypeError:
        return value


def _object_dict(value):
    if is_dataclass(value):
        return asdict(value)
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return dict(to_dict())
    if isinstance(value, dict):
        return dict(value)
    raise TypeError(f"Perspective row value {type(value)!r} is not structured")


def _arrow_rows(value):
    if isinstance(value, (pa.Table, pa.RecordBatch)):
        return value.to_pylist()
    to_arrow = getattr(value, "to_arrow", None)
    if callable(to_arrow):
        return to_arrow().to_pylist()
    raise TypeError(f"Perspective Frame value {type(value)!r} is not Arrow-compatible")


@dataclass(frozen=True)
class _PerspectiveRowPlan:
    key_tp: object
    value_tp: object
    key_kind: str
    key_columns: tuple
    key_column_types: tuple
    residual_index_columns: tuple
    index_columns: tuple
    index: str
    synthetic_index: bool
    key_schema: dict
    value_schema: dict
    value_kind: str
    value_fields: tuple
    value_scalar: object = None

    @property
    def multi_row(self):
        return self.value_kind == "frame"

    def process_key(self, key):
        if self.key_kind == "tuple":
            values = tuple(key)
        elif self.key_kind == "compound":
            source = _object_dict(key)
            values = tuple(source[name] for name in self.key_columns)
        else:
            values = (key,)
        return dict(zip(self.key_columns, values))

    def parse_key(self, row):
        values = tuple(
            _cast(row[name], tp)
            for name, tp in zip(self.key_columns, self.key_column_types)
        )
        if self.key_kind == "tuple":
            return values
        if self.key_kind == "compound":
            return self.key_tp(**dict(zip(self.key_columns, values)))
        return values[0]

    def add_index(self, row):
        if self.synthetic_index:
            row["index"] = ",".join(str(row[name]) for name in self.index_columns)
        return row

    def row_index(self, row):
        return row[self.index]

    def rows(self, child):
        if self.value_kind == "bundle":
            delta = dict(child.delta_value)
            for name in self.residual_index_columns:
                item = child[name]
                if item.valid:
                    delta[name] = item.value
            return [delta]
        value = child.value
        if self.value_kind == "compound":
            return [_object_dict(value)]
        if self.value_kind == "frame":
            return _arrow_rows(value)
        return [{"value": value}]

    def sample_row(self, child, row):
        if self.value_kind != "bundle":
            return dict(row)
        sample = {}
        for name in self.value_fields:
            item = child[name]
            sample[name] = item.value if item.valid else None
        return sample

    def parse_value(self, row):
        values = {
            name: row[name]
            for name in self.value_fields
            if name in row and name not in self.key_columns
        }
        if self.value_kind == "bundle":
            return values
        if self.value_kind == "compound":
            return self.value_scalar(**{
                name: _cast(value, self.value_schema[name])
                for name, value in values.items()
            })
        if self.value_kind == "scalar":
            return _cast(row["value"], self.value_scalar)
        raise TypeError("editable Perspective tables do not support Frame values")

    def empty_key(self):
        values = tuple(tp() for tp in self.key_column_types)
        if self.key_kind == "tuple":
            return values
        if self.key_kind == "compound":
            return self.key_tp(**dict(zip(self.key_columns, values)))
        return values[0]


def _row_plan(ts_type, index_col_name=None):
    key_tp = key_type(ts_type)
    value_tp = dereference(value_type(ts_type))
    requested = tuple(
        value.strip() for value in (index_col_name or "").split(",")
        if value.strip()
    )

    origin = typing.get_origin(key_tp)
    if origin is tuple:
        key_kind = "tuple"
        key_types = tuple(typing.get_args(key_tp))
        key_columns = requested[:len(key_types)] or tuple(
            f"index_{index}" for index in range(len(key_types)))
        if len(key_columns) != len(key_types):
            raise ValueError("tuple Perspective keys require one index column per element")
    elif is_compound_scalar(key_tp):
        key_kind = "compound"
        key_fields = fields(key_tp)
        key_columns = tuple(
            name for name in requested if name in key_fields
        ) or tuple(key_fields)
        key_types = tuple(key_fields[name] for name in key_columns)
    else:
        key_kind = "scalar"
        key_columns = (requested[0] if requested else "index",)
        key_types = (key_tp,)

    residual = requested[len(key_columns):] if requested else ()
    synthetic = key_kind != "scalar" or bool(residual)
    index_columns = tuple(key_columns) + tuple(residual)
    index = "index" if synthetic else key_columns[0]
    key_schema = dict(zip(key_columns, key_types))
    if synthetic:
        key_schema = {"index": str, **key_schema}

    if is_bundle(value_tp):
        value_kind = "bundle"
        value_fields = fields(value_tp)
        value_schema = {
            name: scalar_type(dereference(field_tp))
            for name, field_tp in value_fields.items()
        }
        value_scalar = None
    elif is_frame(value_tp):
        value_kind = "frame"
        row_schema = frame_schema(value_tp)
        value_schema = fields(row_schema)
        value_fields = value_schema
        value_scalar = scalar_type(value_tp)
    elif is_ts(value_tp) and is_compound_scalar(value_tp):
        value_kind = "compound"
        value_schema = fields(value_tp)
        value_fields = value_schema
        value_scalar = scalar_type(value_tp)
    elif is_ts(value_tp):
        value_kind = "scalar"
        value_scalar = scalar_type(value_tp)
        value_schema = {"value": value_scalar}
        value_fields = value_schema
    else:
        raise TypeError(
            "Perspective TSD values must be a TSB, scalar TS, compound-scalar TS, or Frame TS")

    missing = tuple(name for name in residual if name not in value_schema)
    if missing:
        raise ValueError(
            f"Perspective index column(s) {missing!r} are not present in the value schema")

    return _PerspectiveRowPlan(
        key_tp=key_tp,
        value_tp=value_tp,
        key_kind=key_kind,
        key_columns=tuple(key_columns),
        key_column_types=tuple(key_types),
        residual_index_columns=tuple(residual),
        index_columns=index_columns,
        index=index,
        synthetic_index=synthetic,
        key_schema=key_schema,
        value_schema=dict(value_schema),
        value_kind=value_kind,
        value_fields=tuple(value_fields),
        value_scalar=value_scalar,
    )


@sink_node
def _publish_table_node(
    ts: TSD[K, TIME_SERIES_TYPE],
    name: str,
    plan: object,
    editable: bool = False,
    edit_role: str = None,
    empty_row: bool = False,
    history: int = None,
    ec: EvaluationClock = None,
    state: STATE = None,
):
    data = []
    history_data = []

    for key in ts.removed_keys():
        previous = state.key_tracker.pop(key, set() if plan.multi_row else None)
        if plan.multi_row:
            state.removed.update(previous)
        elif previous is not None:
            state.removed.add(previous)

    for key, child in ts.modified_items():
        rows = []
        for value_row in plan.rows(child):
            row = plan.add_index({**plan.process_key(key), **value_row})
            rows.append(row)
            if history is not None:
                sample = plan.add_index({
                    **plan.process_key(key),
                    **plan.sample_row(child, value_row),
                    "time": ec.now,
                })
                history_data.append(sample)

        indices = {plan.row_index(row) for row in rows}
        previous = state.key_tracker.get(key, set() if plan.multi_row else None)
        if plan.multi_row:
            state.removed.update(previous - indices)
            state.key_tracker[key] = indices
        elif indices:
            new_index = next(iter(indices))
            if previous is not None and previous != new_index:
                state.removed.add(previous)
            state.key_tracker[key] = new_index
        state.removed.difference_update(indices)

        if empty_row:
            for row in rows:
                logical = tuple(row[column] for column in plan.index_columns)
                row["_id"] = state.index_to_id[logical]
        data.extend(rows)

    if data or state.removed:
        state.manager.update_table(name, data, state.removed)
    if history is not None and history_data:
        if history == 0:
            state.manager.replace_table(f"{name}_history", history_data)
        else:
            state.manager.update_table(f"{name}_history", history_data)
    state.removed.clear()


@_publish_table_node.start
def _start_publish_table_node(
    name: str,
    plan: object,
    editable: bool,
    edit_role: str,
    empty_row: bool,
    history: int,
    state: STATE = None,
    _global_state: GlobalState = None,
):
    if history is not None and history < 0:
        raise ValueError("Perspective history must be None or a non-negative integer")
    if empty_row and plan.multi_row:
        raise ValueError("Perspective empty-row editing is not supported for Frame values")

    manager = PerspectiveTablesManager.current(_global_state)
    if name in manager.get_table_names():
        raise ValueError(f"Perspective table {name!r} already exists")
    state.manager = manager
    state.key_tracker = defaultdict(set) if plan.multi_row else {}
    state.removed = set()

    schema = {**plan.key_schema, **plan.value_schema}
    if empty_row:
        running_id = iter(range(1, 2**63))
        state.index_to_id = defaultdbldict(lambda: next(running_id))
        state.index_to_id_lock = threading.Lock()
        _global_state[f"perspective_table_index_to_id_{name}"] = {
            "mapping": state.index_to_id,
            "lock": state.index_to_id_lock,
        }
        table = manager.create_table(
            {"_id": int, **schema}, index="_id", name=name,
            editable=editable, edit_role=edit_role,
        )
        key = plan.empty_key()
        table.update([{"_id": 0, **plan.process_key(key)}])
    else:
        manager.create_table(
            schema, index=plan.index, name=name,
            editable=editable, edit_role=edit_role,
        )

    if history is not None:
        manager.create_table(
            {"time": datetime, **schema},
            limit=history if history > 0 else None,
            name=f"{name}_history", user=False,
        )


def _publish_table(
    name,
    ts,
    editable=False,
    edit_role=None,
    empty_row=False,
    index_col_name=None,
    history=None,
):
    """Wire the native sink after deriving one immutable public-reflection plan."""
    plan = _row_plan(ts.output_type, index_col_name)
    _publish_table_node(
        ts,
        name=name,
        plan=plan,
        editable=editable,
        edit_role=edit_role,
        empty_row=empty_row,
        history=history,
    )
    return plan


class _SenderHolder:
    sender = None


@sink_node
def _edit_subscription_lifetime(
    value: TIME_SERIES_TYPE,
    name: str,
    plan: object,
    empty_row: bool,
    holder: object,
):
    pass


@_edit_subscription_lifetime.start
def _start_edit_subscription_lifetime(
    name: str,
    plan: object,
    empty_row: bool,
    holder: object,
    state: STATE = None,
    _global_state: GlobalState = None,
):
    manager = PerspectiveTablesManager.current(_global_state)

    def receive(rows, removals=()):
        edits = {}
        removed = set(removals)
        mapping_state = _global_state.get(
            f"perspective_table_index_to_id_{name}") if empty_row else None

        for original in rows:
            row = dict(original)
            key = plan.parse_key(row)
            if empty_row:
                row_id = row.get("_id")
                if mapping_state:
                    with mapping_state["lock"]:
                        previous = mapping_state["mapping"].reverse_get(row_id)
                    if previous is not None:
                        previous_key = (
                            previous[0] if plan.key_kind == "scalar"
                            else tuple(previous)
                        )
                        if previous_key != key:
                            removed.add(previous_key)
                            removed.add(Removed(key))
                if row_id != 0:
                    edits[key] = plan.parse_value(row)
                if row_id is not None and row_id < 0:
                    manager.update_table(name, None, {row_id})
                    if row_id % 2:
                        removed.add(Removed(key))
                    else:
                        removed.add(key)
                        edits.pop(key, None)
            else:
                edits[key] = plan.parse_value(row)

        update = {}
        if edits:
            update["edits"] = edits
        if removed:
            update["removes"] = removed
        if update:
            holder.sender(update)

    state.manager = manager
    state.token = manager.subscribe_table_updates(name, receive)


@_edit_subscription_lifetime.stop
def _stop_edit_subscription_lifetime(
    name: str,
    state: STATE = None,
):
    token = getattr(state, "token", None)
    if token is not None:
        state.manager.unsubscribe_table_updates(name, token)
        state.token = None


def _receive_table_edits(name, ts, index_col_name=None, empty_row=False, plan=None):
    """Wire a typed push source and lifecycle-owned Perspective subscription."""
    plan = plan or _row_plan(ts.output_type, index_col_name)
    if plan.multi_row:
        raise TypeError("editable Perspective tables do not support Frame values")
    output_type = TSB[TableEdits[plan.key_tp, plan.value_tp]]
    holder = _SenderHolder()

    @push_queue(output_type)
    def edits(sender, sender_holder: object):
        sender_holder.sender = sender

    output = edits(holder)
    _edit_subscription_lifetime(
        output, name=name, plan=plan,
        empty_row=empty_row, holder=holder,
    )
    return output
