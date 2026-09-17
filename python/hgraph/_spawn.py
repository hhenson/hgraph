"""Wiring-only composition for native, parent-clocked asynchronous children."""

from dataclasses import dataclass
import inspect
import json
import math
import sys

from ._distributed import _callable_recipe, _pack_config

import _hgraph

from ._wiring._core import WiringPort, _current_wiring, _unwrap
from ._wiring._graph import (
    _GraphFn, _as_wired, _is_injectable_annotation, _wrap_graph_fn,
)
from ._wiring._node import (
    _PyNode, _ensure_current_signature, _is_time_series_annotation,
    _lift_time_series_argument,
)


@dataclass(frozen=True)
class _BoundStage:
    function: object
    bindings: tuple = ()


@dataclass(frozen=True)
class _Pipeline:
    stages: tuple


def bind_(function, **bindings):
    """Bind named external inputs or scalar configuration to a pipeline stage.

    Unbound inputs remain available for the pipeline flow. Binding a parameter
    twice is an error; bindings do not change its normal activation semantics.
    """
    if isinstance(function, _Pipeline):
        raise TypeError("bind_ applies to an individual stage, not a pipeline")
    if isinstance(function, _BoundStage):
        previous = dict(function.bindings)
        overlap = previous.keys() & bindings.keys()
        if overlap:
            raise TypeError(f"bind_: parameters already bound: {sorted(overlap)}")
        previous.update(bindings)
        return _BoundStage(function.function, tuple(previous.items()))
    return _BoundStage(function, tuple(bindings.items()))


def pipeline_(entries):
    """Compose ordered asynchronous stages; nested pipeline fragments flatten.

    Each stage after the first must have one unbound time-series input, supplied
    by its predecessor. A spawned pipeline must end in a sink. Output-producing
    fragments may be composed before the final sink is attached.
    """
    stages = []
    for entry in entries:
        if isinstance(entry, _Pipeline):
            stages.extend(entry.stages)
        else:
            stages.append(entry if isinstance(entry, _BoundStage) else _BoundStage(entry))
    if not stages:
        raise ValueError("pipeline_ requires at least one stage")
    return _Pipeline(tuple(stages))


def _prepare_stage(stage, args=(), kwargs=None, *, first=False):
    """Capture Python scalar configuration while preserving named TS slots."""
    function = stage.function
    try:
        recipe = _callable_recipe(function)
    except (TypeError, ValueError) as error:
        raise ValueError(f"spawn_ process stage: {error}") from error
    recipe["paths"] = list(sys.path)
    explicit = dict(stage.bindings)
    kwargs = dict(kwargs or {})
    if not isinstance(function, (_GraphFn, _PyNode)):
        # Native erased functions already contain their scalar configuration.
        return (_as_wired(function), {name: _unwrap(value) for name, value in explicit.items()}, json.dumps(recipe)), args, kwargs

    _ensure_current_signature(function)
    signature = function._wiring_signature
    parameters = [p for p in signature.parameters.values()
                  if not _is_injectable_annotation(p.annotation)]
    if any(p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD) for p in parameters):
        raise TypeError("spawn_ stages require fixed, explicitly typed signatures")
    ordinary = signature.replace(parameters=parameters)
    ordinary.bind_partial(**explicit)  # Reject unknown names and positional-only binding.
    remaining = ordinary.replace(parameters=[p for p in parameters if p.name not in explicit])
    supplied = remaining.bind_partial(*args, **kwargs).arguments
    all_values = explicit | supplied
    input_names, scalar_bindings, external, inputs = [], {}, {}, {}
    for parameter in parameters:
        name = parameter.name
        value = all_values.get(name, inspect.Parameter.empty)
        if _is_time_series_annotation(parameter.annotation):
            input_names.append(name)
            if value is not inspect.Parameter.empty:
                if not isinstance(value, WiringPort):
                    value = _lift_time_series_argument(value, parameter.annotation)
                (external if name in explicit else inputs)[name] = _unwrap(value)
            elif first and parameter.default is not inspect.Parameter.empty:
                inputs[name] = _unwrap(_lift_time_series_argument(parameter.default, parameter.annotation))
        elif value is not inspect.Parameter.empty:
            scalar_bindings[name] = value
        elif parameter.default is inspect.Parameter.empty:
            raise TypeError(f"spawn_: missing scalar configuration '{name}'")
    wired = _wrap_graph_fn(function, input_names=input_names,
                           scalar_bindings=scalar_bindings, signature=signature)
    recipe["input_names"] = input_names
    recipe["scalars"] = {name: _pack_config(value) for name, value in scalar_bindings.items()}
    return (wired, external, json.dumps(recipe)), (), inputs


def spawn_(function, *args, __capacity_frames__=256,
           __capacity_bytes__=64 * 1024 * 1024, __worker_timeout__=60.0, **kwargs):
    """Run a sink graph or sink-terminated pipeline behind its owning graph.

    Each stage runs in a separate worker process and may lag, but never advances
    beyond time authorized by its owner. Ordered input frames are bounded by
    both capacity limits; backpressure pauses the owner without dropping ticks.
    The call produces no output. Normal shutdown drains admitted work.
    Stages must be importable module-level functions; configuration crosses a
    value codec, and live resources or closure captures cannot cross processes.
    The timeout bounds worker startup, each evaluation and shutdown.
    """
    for name, value in (("__capacity_frames__", __capacity_frames__),
                        ("__capacity_bytes__", __capacity_bytes__)):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"spawn_: {name} must be a positive integer")
    if (isinstance(__worker_timeout__, bool) or not isinstance(__worker_timeout__, (int, float))
            or not math.isfinite(__worker_timeout__) or not 0 < __worker_timeout__ <= 86_400):
        raise ValueError("spawn_: __worker_timeout__ must be finite, positive and at most 24 hours")
    stages = function.stages if isinstance(function, _Pipeline) else (
        function if isinstance(function, _BoundStage) else _BoundStage(function),)
    prepared, remaining_args, remaining_kwargs = _prepare_stage(stages[0], args, kwargs, first=True)
    native_stages = [prepared]
    for stage in stages[1:]:
        prepared, _, _ = _prepare_stage(stage)
        native_stages.append(prepared)
    _hgraph.spawn(_current_wiring(), native_stages,
                  tuple(_unwrap(value) for value in remaining_args),
                  {name: _unwrap(value) for name, value in remaining_kwargs.items()},
                  __capacity_frames__, __capacity_bytes__, __worker_timeout__, sys.executable,
                  ["-m", "hgraph._spawn_worker"])


__all__ = ["spawn_", "pipeline_", "bind_"]
