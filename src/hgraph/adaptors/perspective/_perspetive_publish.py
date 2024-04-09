from dataclasses import asdict
from datetime import datetime
from typing import Type, Callable

from perspective import Table, View

from hgraph import TIME_SERIES_TYPE, sink_node, graph, TSD, K, AUTO_RESOLVE, EvaluationClock, STATE, \
    HgScalarTypeMetaData, HgTupleFixedScalarType, HgTimeSeriesTypeMetaData, HgTSBTypeMetaData, HgTSTypeMetaData, \
    HgCompoundScalarType, HgDataFrameScalarTypeMetaData, push_queue
from hgraph.nodes import nothing

from ._perspective import PerspectiveTablesManager

__all__ = ('publish_table',)


def publish_table(name: str, ts: TIME_SERIES_TYPE, editable: bool = False, empty_row: bool = False, history: int = None,
                  **kwargs) -> TIME_SERIES_TYPE:
    """
    Publish a time series to a perspective table. The overrides define the logic specific to the input time series type.
    Passing the history scalar as a non-zero value will create a history table with the same name as the main table plus
    '_history' suffix. The history table will have a 'time' column with the evaluation time of the data arriving.

    If editable is set to True, the table will be editable in the perspective web interface and the edited rows will be
    returned as a time series of the same shape as input.
    """
    _publish_table(name, ts, editable=editable, empty_row=empty_row, history=history, **kwargs)
    if editable:
        return _receive_table_edits(name=name, type=ts.output_type.dereference().py_type, **kwargs)
    else:
        return nothing[TIME_SERIES_TYPE: ts.output_type]()


@sink_node
def _publish_table(name: str, ts: TIME_SERIES_TYPE, editable: bool = False, empty_row: bool = False, history: int = None):
    ...


@graph
def _receive_table_edits(name: str, type: Type[TIME_SERIES_TYPE]) -> TIME_SERIES_TYPE:
    """
    Receive the edits to a perspective table
    """
    ...


@sink_node(overloads=_publish_table)
def _publish_table_from_tsd(name: str, ts: TSD[K, TIME_SERIES_TYPE],
                            editable: bool = False,
                            empty_row: bool = False,
                            index_col_name: str = None,
                            history: int = None,
                            _key: Type[K] = AUTO_RESOLVE,
                            _schema: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
                            ec: EvaluationClock = None,
                            state: STATE = None):
    """
    Publish a TSD to a perspective table as a collection of rows. The value types supported are a bundle,
    a compound scalar, Frame. Schema defines the columns along with the keys of the TSD which are allowed
    to be tuples in which case the index_col_name is expected to be a comma separated list (no spaces) of the key names
    in order. index_col_name can also include names of columns from the value in case of Frame value type.
    """
    if state.multi_row:
        data = [{**state.process_key(k), **row} for k, v in ts.modified_items() for row in state.process_row(v)]
    else:
        data = [{**state.process_key(k), **state.process_row(v)} for k, v in ts.modified_items()]

    if state.create_index:
        data = [{**i, **state.create_index(i)} for i in data]

    state.manager.update_table(name, data, ts.removed_keys())
    if history:
        state.manager.update_table(name + '_history', [{'time': ec.evaluation_time, **d} for d in data])


@_publish_table_from_tsd.start
def _publish_table_from_tsd_start(name: str, index_col_name: str, history: int, editable: bool,
                                  empty_row: bool, _key: Type[K], _schema: Type[TIME_SERIES_TYPE], state: STATE):
    manager = PerspectiveTablesManager.current()
    state.manager = manager

    if name in manager.get_table_names():
        raise ValueError(f"Table '{name}' already exists")

    index_col_names = [s.strip() for s in index_col_name.split(',')]

    # process the key types
    _key = HgScalarTypeMetaData.parse_type(_key)
    if isinstance(_key, HgTupleFixedScalarType):
        ks_tps = _key.py_type.__args__
        ks = len(ks_tps)
        if index_col_name:
            index = index_col_names[:ks]
        else:
            index = [f'index_{i}' for i in range(ks)]

        index_col_name = 'index'

        state.process_key = lambda k: {index[i]: ki for i, ki in enumerate(k)}
        state.create_index = lambda v: {'index': ','.join(str(v[i]) for i in index_col_names)}
        state.key_schema = {'index': str, **{index[i]: ks_tps[i] for i in range(ks)}}

        residual_index_col_names = index_col_names[ks:]
    else:
        if index_col_name is None:
            index = 'index'
        else:
            index = index_col_names[0]

        residual_index_col_names = index_col_names[1:]

        if residual_index_col_names:
            state.process_key = lambda k: {'index': str, index: k}
            state.create_index = lambda v: {'index': ','.join(str(v[i]) for i in index_col_names)}
            state.key_schema = state.process_key(_key.py_type)
            index_col_name = 'index'
        else:
            state.process_key = lambda k: {index: k}
            state.create_index = None
            state.key_schema = state.process_key(_key.py_type)

    #process the value type
    _schema = HgTimeSeriesTypeMetaData.parse_type(_schema)
    if isinstance(_schema, HgTSBTypeMetaData):
        state.schema = {k: t.scalar_type().py_type for k, t in _schema.bundle_schema_tp.meta_data_schema.items()}
        state.process_row = lambda v: v.delta_value
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
            state.schema = {'value': _schema.value_scalar_tp.py_type}
            state.process_row = lambda v: {'value': v.value}
            state.multi_row = False
    else:
        raise ValueError(f"Unsupported schema type '{_schema}'")

    table = Table({
        **state.key_schema,
        **{k: v for k, v in state.schema.items()}
    }, index=(index_col_name or 'index'))

    if empty_row:
        empty_values = ['-' if i is str else i.py_type() for i in
                        (_key.py_type.__args__ if isinstance(_key, HgTupleFixedScalarType) else [_key])]
        table.update([state.process_key(empty_values)])

    manager.add_table(name, table, editable)

    if history:
        history_table = Table({
            'time': datetime,
            **state.key_schema,
            **{k: v for k, v in state.schema.items()}
        }, limit=min(history, 4294967295))

        manager.add_table(name + '_history', history_table)


@push_queue(TSD[K, TIME_SERIES_TYPE], overloads=_receive_table_edits)
def _receive_table_edits_tsd(sender: Callable,
                             name: str, type: Type[TSD[K, TIME_SERIES_TYPE]],
                             index_col_name: str = None,
                             _key: Type[K] = AUTO_RESOLVE,
                             _schema: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
                             ) -> TSD[K, TIME_SERIES_TYPE]:
    _schema = HgTimeSeriesTypeMetaData.parse_type(_schema)
    if isinstance(_schema, HgTSBTypeMetaData):
        tp_schema = {k: v.scalar_type().py_type for k, v in _schema.bundle_schema_tp.meta_data_schema.items()}
        process_row = lambda row, i: {k: row[k] for k in row if k not in i and k != 'index'}
    elif isinstance(_schema, HgTSTypeMetaData):
        if isinstance(_schema.value_scalar_tp, HgCompoundScalarType):
            tp = _schema.value_scalar_tp.py_type
            tp_schema = {k: v.py_type for k, v in _schema.value_scalar_tp.meta_data_schema.items()}
            process_row = lambda row, i: tp(**{k: tp_schema[k](row[k]) for k in row if k not in i and k != 'index'})
        else:
            process_row = lambda row, i: row['value']
    else:
        raise ValueError(f"Unsupported schema type '{_schema}'")

    _key = HgScalarTypeMetaData.parse_type(_key)
    if isinstance(_key, HgTupleFixedScalarType):
        if index_col_name:
            index = [s.strip() for s in index_col_name.split(',')]
        else:
            index = [f'index_{i}' for i in range(len(_key.py_type.__args__))]

        def on_update(data: View):
            # print(data.to_dict())
            data = {tuple(row[i] for i in index): process_row(row, index) for row in data.to_records()}
            sender(data)
    else:
        index = [index_col_name or 'index']

        def on_update(data: View):
            # print(data.to_records())
            data = {row[index[0]]: process_row(row, index) for row in data.to_records()}
            sender(data)

    manager = PerspectiveTablesManager.current()
    manager.subscribe_table_updates(name, on_update)
