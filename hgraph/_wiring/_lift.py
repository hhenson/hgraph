from datetime import datetime
from typing import Callable, Sequence, Mapping, Any

import polars as pl

from hgraph._types import TimeSeries, with_signature, TS

__all__ = ("lift", "lower")


def lift(
    fn: Callable,
    inputs: dict[str, type[TimeSeries]] = None,
    output: type[TimeSeries] = None,
    active: Sequence[str] | Callable = None,
    valid: Sequence[str] | Callable = None,
    all_valid: Sequence[str] | Callable = None,
    dedup_output: bool = False,
    defaults: dict[str, Any] = None,
):
    """
    Wraps a scalar function producing a time-series version of the function.

    By default, and assuming the function is appropriately annotated, the function will be wrapped into a
    ``compute_node``, with the args each wrapped with TS[<type>] and the result wrapped with TS[<type>].

    If different time-series types are required, then supply the overrides as appropriate.

    """
    from inspect import signature, Parameter
    from hgraph._wiring._decorators import compute_node, graph

    sig = signature(fn)

    def _wrapped(*args, **kwargs):
        return fn(
            *(a.value if a.valid else None for a in args),
            **{k: v.value if v.valid else None for k, v in kwargs.items()},
        )

    args = {
        k: TS[v.annotation] if inputs is None or k not in inputs else inputs[k]
        for k, v in sig.parameters.items()
        if v.kind in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.POSITIONAL_ONLY) and v.default is Parameter.empty
    }

    kwargs = {
        k: TS[v.annotation] if inputs is None or k not in inputs else inputs[k]
        for k, v in sig.parameters.items()
        if v.kind == Parameter.KEYWORD_ONLY
        or (v.kind == Parameter.POSITIONAL_OR_KEYWORD and v.default is not Parameter.empty)
    }

    defaults = {k: v.default for k, v in sig.parameters.items() if v.default is not Parameter.empty} | (
        defaults if defaults is not None else {}
    )

    out = sig.return_annotation
    return_annotation = TS[out] if output is None else output
    name = fn.__name__
    _wrapped = with_signature(
        _wrapped, args=args, kwargs=kwargs, defaults=defaults, return_annotation=return_annotation
    )
    _wrapped.__name__ = name
    cn_fn = compute_node(_wrapped, active=active, valid=valid, all_valid=all_valid)
    if dedup_output:
        from hgraph._operators._stream import dedup

        g_fn = graph(
            with_signature(
                lambda *args, **kwargs: dedup(cn_fn(*args, **kwargs)),
                args=args,
                kwargs=kwargs,
                return_annotation=return_annotation,
            )
        )
        return g_fn
    else:
        return cn_fn


def lower(fn: Callable, /, date_col: str = "date", as_of_col: str = "as_of", no_as_of_support: bool = True) -> Callable:
    """
    This is the opposite of ``lift``. It takes a reactive function (``graph`` or ``node``) and returns a
    normal scalar function that can be called in standard Python code.

    The returned function will expect a ``DataFrame`` for each time-series input and will return one
    or more ``DataFrames`` to represent the time-series output.

    Usage:

    ::

        @graph
        def my_reactive_fn(x: TS[int], y: TS[int]) -> TS[int]:
            return x + y

        my_normal_fn: Callable[[pl.DataFrame, pl.DataFrame], pl.DataFrame] = lower(my_reactive_fn)

        df1 = pl.DataFrame({"__date_time__": [MIN_ST, MIN_ST + MIN_TD], "value": [1, 2]})
        df2 = pl.DataFrame({"__date_time__": [MIN_ST, MIN_ST + MIN_TD], "value": [2, 3]})

        out = my_normal_fn(df1, df2)

        assert out == pl.DataFrame({"__date_time__": [MIN_ST, MIN_ST + MIN_TD], "value": [3, 5]})

    By default, the ``no_as_of_support`` argument is set to True, this means the input frame only requires
    a date column. If you want to use an as-of column, then set ``no_as_of_support`` to False.

    The date column can be configured to be any name, but the default is ``date``.
    All inputs and the resultant output will need to have a column with this name. It is not possible to
    use different names for different inputs or for the output.

    The as-of column can be configured to be any name, but the default is ``as_of``.
    """
    from hgraph._operators._record_replay import replay, record, set_record_replay_model
    from hgraph._operators._to_table import set_as_of, set_table_schema_date_key, set_table_schema_as_of_key
    from hgraph._runtime._global_state import GlobalState
    from hgraph._runtime._graph_runner import evaluate_graph, GraphConfiguration
    from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeClass
    from hgraph._wiring._wiring_node_signature import WiringNodeSignature
    from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
    from hgraph._wiring._wiring_node_class._wiring_node_class import extract_kwargs
    from hgraph._wiring._decorators import graph
    from hgraph.adaptors.data_frame._data_frame_record_replay import MemoryDataFrameStorage, DATA_FRAME_RECORD_REPLAY

    fn: WiringNodeClass
    signature: WiringNodeSignature = fn.signature

    ts_inputs: Mapping[str, HgTimeSeriesTypeMetaData] = signature.time_series_inputs
    output: HgTimeSeriesTypeMetaData = signature.output_type

    def lower_wrapper(
        *args, __start_time__: datetime = None, __end_time__: datetime = None, __trace__: bool = False, **kwargs
    ):

        kwargs_ = extract_kwargs(signature, *args, **kwargs)
        recordable_id = f"lower.{signature.name}"

        @graph
        def g():
            inputs_ = {k: replay(k, tp=ts_inputs[k].py_type, recordable_id=recordable_id) for k in ts_inputs}
            out = fn(**(kwargs_ | inputs_))
            if output:
                # Need to deal with complex schema results in a better way
                record(out, "__out__", recordable_id=recordable_id)

        with GlobalState(), MemoryDataFrameStorage() as storage:
            set_table_schema_date_key(date_col)
            set_table_schema_as_of_key(as_of_col)
            set_as_of(datetime.utcnow())
            set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
            _prepare_inputs(storage, ts_inputs, recordable_id, no_as_of_support, as_of_col, **kwargs_)
            config_kwargs = {}
            if __start_time__ is not None:
                config_kwargs["start_time"] = __start_time__
            if __end_time__ is not None:
                config_kwargs["end_time"] = __end_time__
            if __trace__:
                config_kwargs["trace"] = __trace__
            evaluate_graph(g, GraphConfiguration(**config_kwargs))
            if output:
                result = storage.read_frame(f"{recordable_id}.__out__")
                if no_as_of_support:
                    result = result.drop(as_of_col)
                return result

    return lower_wrapper


def _prepare_inputs(storage, ts_inputs, recordable_id, no_as_of_support, as_of_col, **kwargs):
    from hgraph import table_schema
    from hgraph import MIN_DT

    for k, v in ts_inputs.items():
        schema = table_schema(v.py_type).value
        # match value schema with supplied data-frame
        df = kwargs[k]
        assert isinstance(df, pl.DataFrame)
        df_schema = df.schema
        assert all(k in df_schema for k in schema.keys if not no_as_of_support and k != as_of_col)
        if no_as_of_support:
            df = df.with_columns(pl.lit(MIN_DT).alias(as_of_col))
        storage.write_frame(f"{recordable_id}.{k}", df)
