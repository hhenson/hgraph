from pathlib import Path
from typing import Union, Callable, Generic

from frozendict import frozendict

from hgraph._types._error_type import NodeError
from hgraph._types._scalar_types import SIZE, STATE
from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._time_series_types import TIME_SERIES_TYPE, K
from hgraph._types._ts_type import TS
from hgraph._types._tsb_type import TimeSeriesSchema, TSB
from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
from hgraph._types._tsd_type import TSD
from hgraph._types._tsl_type import TSL
from hgraph._wiring._map import map_
from hgraph._wiring._reduce import reduce
from hgraph._wiring._source_code_details import SourceCodeDetails
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_node_class._map_wiring_node import TsdMapWiringSignature
from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeClass, extract_kwargs
from hgraph._wiring._wiring_node_signature import WiringNodeSignature, WiringNodeType
from hgraph._wiring._wiring_utils import as_reference

__all__ = ("exception_time_series", "try_except", "TryExceptResult", "TryExceptTsdMapResult")


class TryExceptResult(TimeSeriesSchema, Generic[TIME_SERIES_TYPE]):
    exception: TS[NodeError]
    out: TIME_SERIES_TYPE


class TryExceptTsdMapResult(TimeSeriesSchema, Generic[K, TIME_SERIES_TYPE]):
    exception: TSD[K, TS[NodeError]]
    out: TIME_SERIES_TYPE


def try_except(func: Callable[..., TIME_SERIES_TYPE],
               *args,
               __trace_back_depth__: int = 1,
               __capture_values__: bool = False,
               **kwargs) -> Union[TSB[TryExceptResult], TS[NodeError], TSB[TryExceptTsdMapResult]]:
    """
    Wrap a graph with a try/except wrapper. This will catch an exception in the graph and report it as
    a time-series result (on the "exception" ts of the TryExceptResult). Do not wrap single nodes, this
    is technically possible, but is not very efficient, prefer ``exception_time_series`` in this case.

    :param func: The graph to wrap
    :param args: The arguments to pass to the wrapped graph
    :param __trace_back_depth__: The desired trace-back-depth to record when an exception is thrown (default is 1)
    :param __capture_values__: True to capture the values of the inputs to the trace-back (default is False)
    :param kwargs: The kwargs to pass to the wrapped graph.
    :return: TSB[TryExceptResult] with "error" being the error time-series and "out" being the result of the wrapped
            graph. For sink nodes/graphs the return is just the TS[NodeError]
    """
    is_special_node: bool = False
    if not isinstance(func, WiringNodeClass):
        if not func in (map_, reduce):
            raise RuntimeError(f"The supplied function is not a graph or node function: '{func.__name__}'")
        else:
            is_special_node = True

    if is_special_node or func.signature.node_type in (
            WiringNodeType.COMPUTE_NODE, WiringNodeType.PULL_SOURCE_NODE, WiringNodeType.PUSH_SOURCE_NODE):
        return _try_except_node(func, *args, __trace_back_depth__=__trace_back_depth__,
                                __capture_values__=__capture_values__, **kwargs)

    with WiringContext(current_signature=STATE(signature=f"try_except('{func.signature.signature}', ...)")):
        func: WiringNodeClass
        signature: WiringNodeSignature = func.signature
        kwargs_ = extract_kwargs(signature, *args, **kwargs)
        resolved_signature = func.resolve_signature(**kwargs_)
        inner_output = as_reference(resolved_signature.output_type) if resolved_signature.output_type else None
        output_type = HgTimeSeriesTypeMetaData.parse_type(
            TSB[TryExceptResult[inner_output.py_type]]) if inner_output else \
            HgTimeSeriesTypeMetaData.parse_type(TS[NodeError])
        input_types = {k: as_reference(v) if isinstance(v, HgTimeSeriesTypeMetaData) else v for k, v in
                       resolved_signature.input_types.items()}
        time_series_args = resolved_signature.time_series_args

        has_ts_inputs = bool(time_series_args)

        node_type = WiringNodeType.COMPUTE_NODE if has_ts_inputs else \
            WiringNodeType.PULL_SOURCE_NODE  # Since we do not support PUSH nodes in nested graphs

        resolved_signature_outer = WiringNodeSignature(
            node_type=node_type,
            name="try_except",
            # All actual inputs are encoded in the input_types, so we just need to add the keys if present.
            args=resolved_signature.args,
            defaults=frozendict(),  # Defaults would have already been applied.
            input_types=frozendict(input_types),
            output_type=output_type,
            src_location=SourceCodeDetails(Path(__file__), 25),
            active_inputs=frozenset(),
            valid_inputs=frozenset(),
            all_valid_inputs=frozenset(),
            context_inputs=None,
            # We have constructed the map so that the key are is always present.
            unresolved_args=frozenset(),
            time_series_args=time_series_args,
            label=f"try_except({resolved_signature.signature}, {', '.join(resolved_signature.args)})",
        )
        from hgraph import TryExceptWiringNodeClass
        # noinspection PyTypeChecker
        return TryExceptWiringNodeClass(resolved_signature_outer, func, resolved_signature)(**kwargs_)


def _try_except_node(func, *args, __trace_back_depth__: int = 1, __capture_values__: bool = False, **kwargs):
    # We can short circuit the result using the error output on a node
    out = func(*args, **kwargs)
    err = out.__error__(trace_back_depth=__trace_back_depth__, capture_values=__capture_values__)
    if func is map_:
        if type(out.output_type) is HgTSDTypeMetaData:
            signature: TsdMapWiringSignature = out.node_instance.resolved_signature
            return TSB[TryExceptTsdMapResult[signature.key_tp.py_type, signature.output_type.py_type]].from_ts(out=out, exception=err)
        else:
            # Currently don't have a good way to process TSL exceptions as these are actually hard-wired.
            raise NotImplementedError("A TSL based map is not currently supported for exceptions")
    else:
        return TSB[TryExceptResult[out.output_type.py_type]].from_ts(out=out, exception=err)


def exception_time_series(ts: TIME_SERIES_TYPE, trace_back_depth: int = 1, capture_values: bool = False) \
        -> Union[TSL[TS[NodeError], SIZE], TSD[K, TS[NodeError]], TS[NodeError]]:
    """
    A light-weight wrapper to extract the error time-series from a single node.
    Depending on the nature of the node this will potentially return different results, for a normal node,
    this will return a ``TS[NodeError]``, but for a ``map_`` this will return a collection of ``TS[NodeError]``,
    for example a TSD based ``map_`` (one with ``TSD`` inputs) will return a ``TSD[K, TS[NodeError]]`` where
    ``SCALAR`` is the key type of the ``map_``.

    This is called on the result of calling the node, for example:
    ```python
    c = const('1')
    error = exception_time_series(c)
    ```

    This can't be used to wrap a sink node, for those, use the ``try_except`` function instead.
    """
    return ts.__error__(trace_back_depth=trace_back_depth, capture_values=capture_values)
