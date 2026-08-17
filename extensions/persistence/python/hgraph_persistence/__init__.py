"""hgraph durable persistence (RFC 0025).

Importing this package loads the native bridge, which registers the
``"hgraph.persistence.frame"`` record/replay backend with the shared hgraph
runtime through the keyed-installer mechanism — durable overloads then
resolve from unchanged ``hgraph`` imports once the backend is selected.
"""

import hgraph as _hgraph_pkg  # Load the shared runtime before the bridge.

from . import _hgraph_persistence

__all__ = (
    "FRAME_BACKEND",
    "frame_store_contains",
    "frame_store_read",
    "python_frame_store_active",
    "start_recording_session",
)

FRAME_BACKEND = "hgraph.persistence.frame"


def _state(global_state=None):
    from hgraph import GlobalState

    state = global_state if global_state is not None else GlobalState.instance()
    return state._impl


def frame_store_contains(key, global_state=None):
    """True when the state-selected frame store holds ``key``."""
    return _hgraph_persistence._frame_store_contains(_state(global_state), key)


def frame_store_read(key, global_state=None):
    """The stored frame under ``key`` (Arrow table in the store
    presentation), or ``None`` when absent."""
    return _hgraph_persistence._frame_store_read(_state(global_state), key)


def python_frame_store_active(global_state=None):
    """True when a Python compatibility store is installed in the state."""
    return _hgraph_persistence._python_frame_store_active(_state(global_state))


def start_recording_session(global_state=None):
    """Start a NEW recording session for the frame backend: install a fresh
    native store unless a user-owned Python compatibility store is active.
    Called by ``hgraph``'s backend-selection choke point when this backend
    is selected."""
    _hgraph_persistence._start_recording_session(_state(global_state))
