"""Python wiring for the native process-backed map operator."""
import importlib
import json
import math
import sys
from typing import get_args, get_origin

import _hgraph

from ._wiring._core import WiringPort, _current_wiring, _unwrap


def _describe_type(annotation):
    """A bootstrap-only import recipe; executable code is never serialized."""
    from ._types import _FrameType, _SeriesType, _SharedType, _ArrayType, _TsExpr
    if isinstance(annotation, _FrameType):
        return ["frame", _describe_type(annotation.schema),
                None if annotation.metadata is None else _describe_type(annotation.metadata)]
    if isinstance(annotation, _SeriesType):
        return ["series", _describe_type(annotation.element)]
    if isinstance(annotation, _SharedType):
        return ["shared", _describe_type(annotation.element)]
    if isinstance(annotation, _ArrayType):
        return ["array", _describe_type(annotation.element),
                [getattr(size, "SIZE", size) for size in annotation.dimensions]]
    if isinstance(annotation, _TsExpr):
        return ["time_series", _hgraph._distributed_describe_ts(annotation.handle)]
    if annotation is Ellipsis:
        return ["ellipsis"]
    origin = get_origin(annotation)
    if origin is not None:
        return ["generic", _describe_type(origin), [_describe_type(arg) for arg in get_args(annotation)]]
    module = getattr(annotation, "__module__", None)
    name = getattr(annotation, "__qualname__", None)
    if not module or module == "__main__" or not name or "<" in name:
        raise TypeError("dmap_ boundary types must be importable Python annotations")
    return ["named", module, name]


def _load_type(recipe):
    from ._types import Frame, Series, Shared, Array, _TsExpr
    if recipe[0] == "frame":
        row = _load_type(recipe[1])
        return Frame[row] if recipe[2] is None else Frame[row, _load_type(recipe[2])]
    if recipe[0] == "series":
        return Series[_load_type(recipe[1])]
    if recipe[0] == "shared":
        return Shared[_load_type(recipe[1])]
    if recipe[0] == "array":
        return Array[_load_type(recipe[1]), *recipe[2]]
    if recipe[0] == "time_series":
        return _TsExpr(_hgraph._distributed_load_ts(recipe[1]), "distributed boundary")
    if recipe[0] == "ellipsis":
        return Ellipsis
    if recipe[0] == "generic":
        origin = _load_type(recipe[1])
        args = tuple(_load_type(arg) for arg in recipe[2])
        return origin[args[0] if len(args) == 1 else args]
    if recipe[0] != "named":
        raise ValueError("invalid dmap_ type recipe")
    result = importlib.import_module(recipe[1])
    for part in recipe[2].split("."):
        result = getattr(result, part)
    return result


def _pack_config(value):
    if value is None:
        return ["none"]
    from ._types import _TsExpr, _FrameType, _SeriesType, _SharedType, _ArrayType
    if isinstance(value, (type, _TsExpr, _FrameType, _SeriesType, _SharedType, _ArrayType)) or get_origin(value) is not None:
        return ["type", _describe_type(value)]
    import base64
    annotation, payload = _hgraph._distributed_pack_scalar(value)
    return ["value", annotation, base64.b64encode(payload).decode("ascii")]


def _unpack_config(recipe):
    if recipe[0] == "none":
        return None
    if recipe[0] == "type":
        return _load_type(recipe[1])
    import base64
    return _hgraph._distributed_unpack_scalar(recipe[1], base64.b64decode(recipe[2]))


def _callable_recipe(func):
    from ._wiring._core import _OperatorFunction
    if isinstance(func, str):
        return {"operator": func}
    if isinstance(func, _OperatorFunction):
        return {"operator": func.__name__, "output_type":
                None if func._output_type is None else _describe_type(func._output_type)}
    origin = getattr(func, "fn", func)
    module = getattr(origin, "__module__", "")
    qualname = getattr(origin, "__qualname__", "")
    if not module or module == "__main__" or not qualname or "<" in qualname:
        raise ValueError("dmap_ child must be defined in an importable module, not a lambda, closure or __main__")
    imported = importlib.import_module(module)
    for part in qualname.split("."):
        imported = getattr(imported, part)
    if imported is not func:
        raise ValueError("dmap_ child must be reachable by its module-qualified name")
    return {"module": module, "qualname": qualname}


def _load_callable(recipe):
    if "operator" in recipe:
        from ._wiring._core import _OperatorFunction
        return _OperatorFunction(recipe["operator"], output_type=
            None if recipe.get("output_type") is None else _load_type(recipe["output_type"]))
    result = importlib.import_module(recipe["module"])
    for part in recipe["qualname"].split("."):
        result = getattr(result, part)
    return result


def dmap_(func, *args, __workers__=2, __worker_timeout__=60.0, in_process=False, __label__=None,
          __keys__=None, __key_arg__=None, **kwargs):
    """Run map_ children in worker processes using the native map wiring rules.

    Positional/named inputs, scalar configuration, pass_through/no_key,
    explicit keys, key/index injection and sink children follow map_. Boundary
    references are materialized values. Children must be importable and cannot
    capture live resources or access parent services/contexts. in_process is
    an explicit diagnostic mode; it uses the same plans and transfer protocol.
    """
    from ._wiring._graph import _prepare_higher_order_call
    if isinstance(__workers__, bool) or not isinstance(__workers__, int) or __workers__ <= 0:
        raise ValueError("dmap_ needs a positive integer worker count")
    if (isinstance(__worker_timeout__, bool) or not isinstance(__worker_timeout__, (int, float))
            or __worker_timeout__ <= 0 or __worker_timeout__ > 86_400
            or not math.isfinite(__worker_timeout__)):
        raise ValueError("dmap_ __worker_timeout__ must be a finite positive number of seconds, at most 24 hours")
    recipe = {} if in_process else _callable_recipe(func)
    if __keys__ is not None:
        kwargs["__keys__"] = __keys__
    if __key_arg__ is not None:
        kwargs["__key_arg__"] = __key_arg__

    def record_bindings(names, bindings):
        if not in_process:
            recipe["input_names"] = names
            recipe["scalars"] = {name: _pack_config(value) for name, value in bindings.items()}

    wired, args, kwargs = _prepare_higher_order_call(
        func, args, kwargs, default_key_arg="key", binding_observer=record_bindings)
    if not in_process:
        recipe["paths"] = list(sys.path)
    arguments = ["-m", "hgraph._distributed_worker"]

    def wire_call():
        result = _hgraph.distributed_map(
            _current_wiring(), wired, tuple(_unwrap(arg) for arg in args),
            {name: _unwrap(value) for name, value in kwargs.items()}, __workers__,
            in_process, json.dumps(recipe), sys.executable, arguments, __worker_timeout__)
        return None if result is None else WiringPort(result)

    if __label__:
        from ._wiring._core import _graph_scope

        with _graph_scope(_current_wiring(), str(__label__)):
            return wire_call()
    return wire_call()


__all__ = ["dmap_"]
