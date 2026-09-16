"""The child graphs a dmap_ may run, addressed by name.

A worker cannot be sent a graph. RFC 0022 is explicit that a manifest "does
**not** serialize executable code, function pointers, credentials, or live
resources" -- an application "reconstructs a graph through its ordinary C++ or
Python wiring path". So both sides import this module and agree on a *name*.

This registry is the prototype's stand-in for that identity check. It is not a
proposal: a real implementation would compare manifests and fail on a diff,
where this merely trusts that both sides imported the same file.

Every kernel here is deliberately within the v0 restrictions in README.md:
input-driven, no internal scheduling, no clock reads, no services, no
references.
"""

from __future__ import annotations

from hgraph import STATE, TS, compute_node, graph

__all__ = ["KERNELS", "kernel"]

KERNELS: dict[str, object] = {}


def kernel(name: str):
    def register(fn):
        KERNELS[name] = fn
        return fn

    return register


# --- stateless -------------------------------------------------------------


@kernel("increment")
@graph
def increment(value: TS[float]) -> TS[float]:
    """The simplest possible child: one arithmetic node."""
    return value + 1.0


@kernel("scale")
@graph
def scale(value: TS[float], factor: TS[float]) -> TS[float]:
    """Takes the broadcast input as well as its own key's element."""
    return value * factor


# --- stateful --------------------------------------------------------------
# These are the kernels that make the experiment worth running: per-key state
# must live in the worker and survive between cycles, which is what separates
# "distribute a map_" from "call a function remotely".


@compute_node
def _running_sum(value: TS[float], _state: STATE = None) -> TS[float]:
    total = getattr(_state, "total", 0.0) + value.value
    _state.total = total
    return total


@kernel("running_sum")
@graph
def running_sum(value: TS[float]) -> TS[float]:
    return _running_sum(value)


@compute_node
def _tick_count(value: TS[float], _state: STATE = None) -> TS[float]:
    n = getattr(_state, "n", 0.0) + 1.0
    _state.n = n
    return n


@kernel("tick_count")
@graph
def tick_count(value: TS[float]) -> TS[float]:
    """Counts this key's ticks: output depends on history, not on the value."""
    return _tick_count(value)


#: Kernels taking the broadcast input as a second argument.
SHARED_ARG_KERNELS = frozenset({"scale"})


# --- tunable cost ----------------------------------------------------------


@compute_node
def _busy(value: TS[float]) -> TS[float]:
    """Burns a configurable amount of CPU so the crossover can be measured.

    The spin count is read from the environment on every tick rather than at
    import: a spawned worker inherits the environment, so parent and worker
    agree without the count having to travel in the protocol.
    """
    import os

    total = value.value
    for _ in range(int(os.environ.get("DMAP_SPIN", "0"))):
        total = (total * 1.0000001) % 1e9
    return total


@kernel("busy")
@graph
def busy(value: TS[float]) -> TS[float]:
    return _busy(value)
