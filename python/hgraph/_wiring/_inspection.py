"""Read-only queries over the current, live wiring graph."""
from typing import NamedTuple
import _hgraph
from ._core import _current_wiring, _unwrap


class WiringDependency(NamedTuple):
    source_index: int
    passive: bool


def inspect_wiring_dependencies(output, sources):
    """Find upstream source boundaries; any fully active path wins.

    Stops at matching source ports and includes passive paths. Inspection is
    wiring-time only: runtime changes to input activity are not inferred.
    Inline graphs, structural inputs and bound delayed ports are supported.
    Compiled child graphs and unresolved inputs raise ValueError rather than
    silently omit dependencies. Supply a boundary at such a graph's output.
    """
    _current_wiring()
    return tuple(WiringDependency(*item) for item in _hgraph.inspect_wiring_dependencies(
        _unwrap(output), tuple(_unwrap(source) for source in sources)))


def wiring_extension_state(key, factory):
    """Acquire state owned by the current wiring, shared by inline graphs.

    Use a package-qualified key. The factory runs once per key and wiring;
    state is retired when that wiring is destroyed, including on failure.
    """
    return _current_wiring()._acquire_extension_state(key, factory)
