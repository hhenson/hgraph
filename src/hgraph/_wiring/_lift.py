from typing import Callable, Sequence

from hgraph._types import TimeSeries
from hgraph._wiring import compute_node, graph
from hgraph._types import with_signature, TS

__all__ = ("lift",)


def lift(
    fn: Callable,
    inputs: dict[str, type[TimeSeries]] = None,
    output: type[TimeSeries] = None,
    active: Sequence[str] | Callable = None,
    valid: Sequence[str] | Callable = None,
    all_valid: Sequence[str] | Callable = None,
    dedup_output: bool = False,
):
    """
    Wraps a scalar function producing a time-series version of the function.

    By default, and assuming the function is appropriately annotated, the function will be wrapped into a
    ``compute_node``, with the args each wrapped with TS[<type>] and the result wrapped with TS[<type>].

    If different time-series types are required, then supply the overrides as appropriate.

    """
    from inspect import signature, Parameter, Signature

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

    defaults = {k: v.default for k, v in sig.parameters.items() if v.default is not Parameter.empty}

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
