import logging
import threading
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any, Callable, Generic, Type

from perspective import View

from hgraph import (
    AUTO_RESOLVE,
    SCHEDULER,
    STATE,
    TIME_SERIES_TYPE,
    TSB,
    TSD,
    TSS,
    EvaluationClock,
    GlobalState,
    HgCompoundScalarType,
    HgDataFrameScalarTypeMetaData,
    HgScalarTypeMetaData,
    HgTimeSeriesTypeMetaData,
    HgTSBTypeMetaData,
    HgTSTypeMetaData,
    HgTupleFixedScalarType,
    K,
    Removed,
    TimeSeriesSchema,
    operator,
    push_queue,
    sink_node,
)

from ._perspective import PerspectiveTablesManager

logger = logging.getLogger(__name__)


@operator
def _publish_table(
    name: str,
    ts: TIME_SERIES_TYPE,
    editable: bool = False,
    edit_role: str = None,
    empty_row: bool = False,
    history: int = None,
): ...


@operator
def _receive_table_edits(name: str, type: Type[TIME_SERIES_TYPE]) -> TIME_SERIES_TYPE:
    """
    Receive the edits to a perspective table
    """
    ...


@sink_node(overloads=_publish_table, label="{name}")
def _publish_table_from_tsd(
    name: str,
    ts: TSD[K, TIME_SERIES_TYPE],
    editable: bool = False,
    edit_role: str = None,
    empty_row: bool = False,
    index_col_name: str = None,
    history: int = None,
    _key: Type[K] = AUTO_RESOLVE,
    _schema: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
    ec: EvaluationClock = None,
    state: STATE = None,
    sched: SCHEDULER = None,
):
    """
    Publish a TSD to a perspective table as a collection of rows. The value types supported are a bundle,
    a compound scalar, Frame. Schema defines the columns along with the keys of the TSD which are allowed
    to be tuples in which case the index_col_name is expected to be a comma separated list (no spaces) of the key names
    in order. index_col_name can also include names of columns from the value in case of Frame value type.
    """
    data = None
    removes = 0
    if state.multi_row:
        for k in ts.removed_keys():
            state.removed.update(state.key_tracker.pop(k, set()))

        for k, v in ts.modified_items():
            data = [{**state.process_key(k), **row} for row in state.process_row(v)]
            if state.create_index:
                data = [{**i, **state.create_index(i)} for i in data]

            if history is not None:
                sample = [{**data, "time": ec.evaluation_time} for data in data]
                state.history += (
                    sample  # multirow types do not do partial updates do data and a sample are always the same
                )

            index = state.index
            updated_indices = set(i[index] for i in data)
            state.removed.update(state.key_tracker[k] - updated_indices)
            state.removed -= updated_indices
            state.key_tracker[k] = updated_indices
            state.data += data

        removes = len(state.removed)
    else:
        for k in ts.removed_keys():
            if v := state.key_tracker.pop(k, None):
                state.removed.add(v)

        for k, v in ts.modified_items():
            # psp3 treats nulls differently so sampling always reduces load (EXPERIMENTAL CODE)
            if False and state.sample_row:
                data = {**state.process_key(k), **state.sample_row(v)}
            else:
                data = {**state.process_key(k), **state.process_row(v)}
                
            if state.create_index:
                data = {**data, **state.create_index(data)}
            if history is not None:
                if state.sample_row:
                    sample = {**state.sample_row(v), **data, "time": ec.evaluation_time}
                    state.history.append(sample)
                else:
                    state.history.append({**data, "time": ec.evaluation_time})

            if state.map_index:
                with state.index_to_id_lock:
                    row_key = state.create_row_key(data)
                    new_index = data.setdefault("_id", state.index_to_id[row_key])
            else:
                new_index = data[state.index]

            if (old_i := state.key_tracker.get(k)) is not None and old_i != new_index:
                state.removed.add(old_i)
            state.removed.discard(new_index)
            state.key_tracker[k] = new_index
            state.data.append(data)

        removes = len(state.removed)

    if not sched.is_scheduled and (data or removes):
        sched.schedule(timedelta(milliseconds=250), on_wall_clock=True, tag='')

    if sched.is_scheduled_now:
        state.manager.update_table(name, state.data, state.removed)
        if history:
            state.manager.update_table(name + "_history", state.history)
        elif history is 0:
            state.manager.replace_table(name + "_history", state.history)

        state.data = []
        state.history = []
        state.removed = set()


@_publish_table_from_tsd.start
def _publish_table_from_tsd_start(
    name: str,
    index_col_name: str,
    history: int,
    editable: bool,
    edit_role: str,
    empty_row: bool,
    _key: Type[K],
    _schema: Type[TIME_SERIES_TYPE],
    state: STATE,
):
    manager = PerspectiveTablesManager.current()
    state.manager = manager

    if name in manager.get_table_names():
        raise ValueError(f"Table '{name}' already exists")

    index_col_names = [s.strip() for s in index_col_name.split(",")]

    # process the key types
    _key = HgScalarTypeMetaData.parse_type(_key)
    if isinstance(_key, HgTupleFixedScalarType):
        ks_tps = _key.py_type.__args__
        ks = len(ks_tps)
        if index_col_name:
            index = index_col_names[:ks]
        else:
            index = [f"index_{i}" for i in range(ks)]

        index_col_name = "index"

        state.process_key = lambda k: {index[i]: ki for i, ki in enumerate(k)}
        state.create_index = lambda v: {"index": ",".join(str(v[i]) for i in index_col_names)}
        state.create_row_key = lambda v: tuple(v[i] for i in index_col_names)
        state.key_schema = {"index": str, **{index[i]: ks_tps[i] for i in range(ks)}}

        residual_index_col_names = index_col_names[ks:]
    elif isinstance(_key, HgCompoundScalarType):
        if index_col_name:
            index = [i for i in index_col_names if i in _key.meta_data_schema]
        else:
            index = list(_key.meta_data_schema.keys())

        index_col_name = "index"

        state.process_key = lambda k: {index[i]: getattr(k, index[i]) for i in range(len(index))}
        state.create_index = lambda v: {"index": ",".join(str(v[i]) for i in index_col_names)}
        state.create_row_key = lambda v: _key.py_type(**{i: v[i] for i in index_col_names})
        state.key_schema = {"index": str, **{k: v.py_type for k, v in _key.meta_data_schema.items() if k in index}}

        residual_index_col_names = [i for i in index_col_names if i not in index]
    else:
        if index_col_name is None:
            index = "index"
        else:
            index = index_col_names[0]

        residual_index_col_names = index_col_names[1:]

        if residual_index_col_names:
            state.process_key = lambda k: {"index": str, index: k}
            state.create_index = lambda v: {"index": ",".join(str(v[i]) for i in index_col_names)}
            state.create_row_key = lambda v: tuple(v[i] for i in index_col_names)
            state.key_schema = state.process_key(_key.py_type)
            index_col_name = "index"
        else:
            state.process_key = lambda k: {index: k}
            state.create_index = None
            state.create_row_key = lambda v: v[index]
            state.key_schema = state.process_key(_key.py_type)

    # process the value type
    _schema = HgTimeSeriesTypeMetaData.parse_type(_schema)
    if isinstance(_schema, HgTSBTypeMetaData):
        state.schema = {k: t.scalar_type().py_type for k, t in _schema.bundle_schema_tp.meta_data_schema.items()}
        if residual_index_col_names:
            state.process_row = lambda v: v.delta_value | {k: v[k].value for k in residual_index_col_names}
        else:
            state.process_row = lambda v: v.delta_value

        state.sample_row = lambda v: {k: ts.value if ts.valid else None for k, ts in v.items()}

        state.multi_row = False
    elif isinstance(_schema, HgTSTypeMetaData):
        if isinstance(_schema.value_scalar_tp, HgCompoundScalarType):
            state.schema = {k: v.py_type for k, v in _schema.value_scalar_tp.meta_data_schema.items()}
            state.process_row = lambda v: asdict(v.value)
            state.multi_row = False
        elif isinstance(_schema.value_scalar_tp, HgDataFrameScalarTypeMetaData):
            state.schema = {k: v.py_type for k, v in _schema.value_scalar_tp.schema.meta_data_schema.items()}
            state.process_row = lambda v: v.value.to_dicts()
            state.multi_row = True
        else:
            state.schema = {"value": _schema.value_scalar_tp.py_type}
            state.process_row = lambda v: {"value": v.value}
            state.multi_row = False

        state.sample_row = False
    else:
        raise ValueError(f"Unsupported schema type '{_schema}'")

    state.index = index_col_name or "index"

    if empty_row:
        state.map_index = True
        state.running_id = 0

        def _new_id():
            state.running_id += 1
            return state.running_id

        state.index_to_id = defaultdbldict(_new_id)
        state.index_to_id_lock = threading.Lock()
        index_to_id_and_lock = GlobalState.instance().setdefault(f"perspective_table_index_to_id_{name}", dict())
        index_to_id_and_lock["mapping"] = state.index_to_id
        index_to_id_and_lock["lock"] = state.index_to_id_lock

        if state.multi_row:
            raise ValueError("Empty row is not supported for multi-row tables")

        table = manager.create_table(
            {"_id": int, **state.key_schema, **{k: v for k, v in state.schema.items()}},
            index="_id",
            name=name,
            editable=editable,
            edit_role=edit_role,
        )
        if isinstance(_key, HgTupleFixedScalarType):
            empty_values = tuple(i.py_type() for i in _key.element_types)
        elif isinstance(_key, HgCompoundScalarType):
            empty_values = _key.py_type(**{k: v.py_type() for k, v in _key.meta_data_schema.items()})
        else:
            empty_values = _key.py_type()
        table.update([{"_id": 0, **state.process_key(empty_values)}])
    else:
        state.map_index = False
        table = manager.create_table(
            {**state.key_schema, **{k: v for k, v in state.schema.items()}},
            index=state.index,
            name=name,
            editable=editable,
            edit_role=edit_role,
        )

    state.data = []
    state.history = []
    state.removed = set()
    state.key_tracker = defaultdict(set) if state.multi_row else {}

    if history is not None:
        history_table = manager.create_table(
            {"time": datetime, **state.key_schema, **{k: v for k, v in state.schema.items()}},
            limit=min(history, 4294967295) if history > 0 else None,
            name=name + "_history",
        )


class TableEdits(TimeSeriesSchema, Generic[K, TIME_SERIES_TYPE]):
    edits: TSD[K, TIME_SERIES_TYPE]
    removes: TSS[K]


@push_queue(TSB[TableEdits[K, TIME_SERIES_TYPE]], overloads=_receive_table_edits, label="{name}")
def _receive_table_edits_tsd(
    sender: Callable,
    name: str,
    tp: Type[TSD[K, TIME_SERIES_TYPE]],
    index_col_name: str = None,
    empty_row: bool = False,
    _key: Type[K] = AUTO_RESOLVE,
    _schema: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
) -> TSB[TableEdits[K, TIME_SERIES_TYPE]]:
    _schema = HgTimeSeriesTypeMetaData.parse_type(_schema)
    if isinstance(_schema, HgTSBTypeMetaData):
        tp_schema = {k: v.scalar_type().py_type for k, v in _schema.bundle_schema_tp.meta_data_schema.items()}
        process_row = lambda row, i: {k: row[k] for k in row if k not in i and k not in ("index", "_id")}
    elif isinstance(_schema, HgTSTypeMetaData):
        if isinstance(_schema.value_scalar_tp, HgCompoundScalarType):
            tp = _schema.value_scalar_tp.py_type
            tp_schema = {k: v.py_type for k, v in _schema.value_scalar_tp.meta_data_schema.items()}
            process_row = lambda row, i: tp(**{k: tp_schema[k](row[k]) for k in row if k in tp_schema})
        else:
            process_row = lambda row, i: row["value"]
    else:
        raise ValueError(f"Unsupported schema type '{_schema}'")

    _key = HgScalarTypeMetaData.parse_type(_key)
    if isinstance(_key, HgTupleFixedScalarType):
        if index_col_name:
            index = [s.strip() for s in index_col_name.split(",")]
        else:
            index = [f"index_{i}" for i in range(len(_key.py_type.__args__))]
        process_key = lambda row, _index: tuple(row[i] for i in _index)
    elif isinstance(_key, HgCompoundScalarType):
        if index_col_name:
            index = [s.strip() for s in index_col_name.split(",")]
        else:
            index = list(_key.meta_data_schema.keys())
        process_key = lambda row, _index: _key.py_type(**{i: row[i] for i in _index})
    else:
        index = index_col_name or "index"
        process_key = lambda row, _index: row[_index]

    # the publish node will populate the global state with the index to id mapping object and its lock to be used for lookups
    if empty_row:
        index_mapping = GlobalState.instance().setdefault(f"perspective_table_index_to_id_{name}", dict())

    def on_update(data: View):
        logger.info(f"Update from perspective: {data.to_records()}")

        edits = {}
        removes = set()
        for row in data.to_records():
            if empty_row:
                _id = row.get("_id")
                key = process_key(row, index)

                if index_mapping:
                    with index_mapping["lock"]:
                        mapping = index_mapping["mapping"]
                        prev_mapped_key = mapping.reverse_get(_id)

                    if prev_mapped_key is not None:
                        if prev_mapped_key != key:
                            # the row's key was updated, i.e one of more coluimns in the index
                            # therefore we need to remove the old row
                            # manager.update_table(name, data=None, removals=set((_id,)))
                            removes.add(prev_mapped_key)  # remove the old key
                            removes.add(Removed(key))  # undo any previous removals of the new key

                if _id != 0:  # _id == 0 is for the `new row` in the editable table
                    edits[key] = process_row(row, index)
                if (
                    _id < 0 and _id % 2 == 1
                ):  # this is a row being inserted, the graph is supposed to re-add it with proper _id handling
                    manager.update_table(name, data=None, removals=set((_id,)))
                    removes.add(Removed(key))  # undo any previous removals of this key
                if _id < 0 and _id % 2 == 0:  # this is a row being deleted
                    manager.update_table(name, data=None, removals=set((_id,)))
                    removes.add(key)
                    edits.pop(key)
            else:
                key = row[index] if type(index) is str else tuple(row[i] for i in index)
                edits[key] = process_row(row, index)

        sender({"edits": edits, "removes": removes})
        data.delete()

    manager = PerspectiveTablesManager.current()
    manager.subscribe_table_updates(name, on_update)


class defaultdbldict(defaultdict):
    """
    A defaultdict that keeps a reverse lookup if values back to keys and assumes that mapping is 1-to-1
    """

    def __init__(self, default_factory: Callable):
        super().__init__(default_factory)
        self.key_tracker = {}

    def __missing__(self, key):
        value = super().__missing__(key)
        self.key_tracker[value] = key
        return value

    def __setitem__(self, key: Any, value: Any) -> None:
        r = super().__setitem__(key, value)
        if value in self.key_tracker:
            # If the value already exists, remove the old key
            old_key = self.key_tracker[value]
            if old_key != key:
                del self.key_tracker[value]

        self.key_tracker[value] = key
        return r

    def __delitem__(self, key: Any) -> None:
        value = self.get(key, None)
        super().__delitem__(key)
        if value in self.key_tracker:
            del self.key_tracker[value]
        return value

    def reverse_get(self, value):
        """
        Get the key for a given value
        """
        return self.key_tracker.get(value, None)
