"""Arrow-native lowering of a reactive graph to a scalar callable."""

import functools
import inspect

import _hgraph

from .._types import _ContextExpr, _GenericTsExpr, _TsExpr
from ._graph import _GraphFn, _wrap_graph_fn
from ._markers import _INJECTABLE_MARKERS
from ._node import _PyNode, _is_time_series_annotation
from ._runner import _make_evaluation_trace
from ._state import GlobalState


def lower(fn, /, date_col="date", as_of_col="as_of", no_as_of_support=True):
    """Turn a reactive graph or node into an Arrow-frame callable.

    Each time-series parameter becomes one Arrow-compatible frame input. Plain
    scalar parameters remain ordinary call arguments and are captured while
    the native graph is wired. The returned value is a PyArrow table, except
    that a Polars input selects a Polars result for compatibility.

    Input and output frames use ``date_col`` for evaluation time. With
    ``no_as_of_support=False``, every input also requires ``as_of_col`` and the
    latest visible input row per ``(date, key)`` is replayed. The output
    contains one fixed as-of value for the invocation.
    """
    if not isinstance(fn, (_GraphFn, _PyNode)):
        raise TypeError("lower expects a function decorated with @graph, @compute_node, or @sink_node")

    user_fn = fn.fn
    signature = inspect.signature(getattr(user_fn, "fn", user_fn), eval_str=True)
    parameters = tuple(signature.parameters.values())
    if any(parameter.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD) for parameter in parameters):
        raise TypeError("lower does not support variadic reactive functions")
    call_parameters = tuple(
        parameter for parameter in parameters
        if (parameter.annotation not in _INJECTABLE_MARKERS and
            not isinstance(parameter.annotation, _ContextExpr))
    )
    call_signature = signature.replace(parameters=call_parameters)

    @functools.wraps(user_fn)
    def lower_wrapper(
            *args, __start_time__=None, __end_time__=None,
            __trace__=False, **kwargs):
        bound = call_signature.bind(*args, **kwargs)
        bound.apply_defaults()

        input_names = []
        input_frames = []
        scalar_bindings = {}
        return_polars = False
        for parameter in call_parameters:
            annotation = parameter.annotation
            value = bound.arguments[parameter.name]
            if _is_time_series_annotation(annotation):
                if isinstance(annotation, _GenericTsExpr):
                    raise TypeError(
                        f"lower requires a concrete time-series annotation for "
                        f"'{parameter.name}'")
                if not isinstance(annotation, _TsExpr):
                    raise TypeError(
                        f"lower cannot resolve the time-series annotation for "
                        f"'{parameter.name}'")
                input_names.append(parameter.name)
                input_frames.append(value)
                return_polars = return_polars or type(value).__module__.startswith("polars.")
            else:
                scalar_bindings[parameter.name] = value

        wired = _wrap_graph_fn(
            fn, input_names=input_names, scalar_bindings=scalar_bindings)
        result = _hgraph._lower(
            GlobalState.instance()._impl,
            wired,
            input_frames,
            date_column=date_col,
            as_of_column=as_of_col,
            no_as_of_support=no_as_of_support,
            start_time=__start_time__,
            end_time=__end_time__,
            trace=_make_evaluation_trace(__trace__),
        )
        if result is None or not return_polars:
            return result
        import polars as pl
        return pl.from_arrow(result)

    return lower_wrapper


__all__ = ["lower"]
