"""Python wiring for the native process-backed map operator."""
import importlib
import json
import sys
from typing import get_args, get_origin

import _hgraph

from ._wiring._core import WiringPort, _current_wiring, _unwrap
from ._wiring._graph import _as_wired


def _describe_type(annotation):
    """A bootstrap-only import recipe; executable code is never serialized."""
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


def dmap_(func, ts, *, __workers__=2, in_process=False):
    """Map an importable child over one TSD of scalar time series.

    Each worker hosts an ordinary native ``map_``. Python child nodes execute
    in separate interpreters; values cross the native binary protocol. The
    child must accept one value input and return a scalar TS. An optional
    conventional ``key`` argument follows ``map_`` semantics.

    For process execution, define the graph/node at module scope in an
    importable module. Lambdas, closures and ``__main__`` functions cannot be
    reconstructed in a fresh worker. ``in_process=True`` is a diagnostic mode.
    Boundary scalar types must be registered by the imported module and have
    a native binary codec; opaque Python objects are unsupported.

    This first release does not support multiple inputs, explicit key sets,
    custom partition functions, worker recovery, or rebalance. It inherits
    the documented native limitation for keys added without valid values.
    """
    if isinstance(__workers__, bool) or not isinstance(__workers__, int) or __workers__ <= 0:
        raise ValueError("dmap_ needs a positive integer worker count")
    origin = getattr(func, "fn", func)
    module = getattr(origin, "__module__", "")
    qualname = getattr(origin, "__qualname__", "")
    if not in_process:
        if not module or module == "__main__" or not qualname or "<" in qualname:
            raise ValueError("dmap_ child must be defined in an importable module, not a lambda, closure or __main__")
        imported = importlib.import_module(module)
        for part in qualname.split("."):
            imported = getattr(imported, part)
        if imported is not func:
            raise ValueError("dmap_ child must be reachable by its module-qualified name")
    arguments = ["-m", "hgraph._distributed_worker", "--paths", json.dumps(sys.path)]
    return WiringPort(_hgraph.distributed_map(
        _current_wiring(), _as_wired(func), _unwrap(ts), __workers__, in_process,
        module, qualname, sys.executable, arguments))


__all__ = ["dmap_"]
