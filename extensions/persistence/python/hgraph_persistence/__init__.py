"""hgraph durable persistence (RFC 0025).

Importing this package loads the native bridge, which registers the
``"hgraph.persistence.frame"`` record/replay backend with the shared hgraph
runtime through the keyed-installer mechanism — durable overloads then
resolve from unchanged ``hgraph`` imports once the backend is selected.
"""

import os as _os
import sys as _sys

import hgraph as _hgraph_pkg  # Load the shared runtime before the bridge.

if _sys.platform == "win32":
    # Windows resolves a module's dependent DLLs only from the module's own
    # directory, the loaded-module list, and added DLL directories — there is
    # no rpath. The shared hgraph runtime is already loaded (import above),
    # but the bridge links pyarrow's Parquet, which nothing loads for it:
    # core's Arrow use no longer references Parquet symbols (RFC 0025 moved
    # the frame store here), so those DLLs must resolve from the pyarrow
    # package (macOS/Linux reach them via the ../pyarrow rpath).
    import pyarrow as _pyarrow

    _pyarrow_dir = _os.path.dirname(_pyarrow.__file__)
    _package_prefix = _os.path.dirname(_pyarrow_dir)
    _dll_directory_handles = []
    for _dll_dir in (
        _os.path.join(_package_prefix, "bin"),
        _os.path.join(_package_prefix, "lib"),
        _pyarrow_dir,
        _os.path.join(_package_prefix, "pyarrow.libs"),
    ):
        if _os.path.isdir(_dll_dir):
            # Keep the handle alive for as long as the extension is loaded;
            # discarding it immediately removes the directory again.
            _dll_directory_handles.append(_os.add_dll_directory(_dll_dir))

from enum import Enum

from . import _hgraph_persistence

__all__ = (
    "FRAME_BACKEND",
    "RecordAsOf",
    "RecordRemoves",
    "frame_store_contains",
    "frame_store_read",
    "python_frame_store_active",
    "start_recording_session",
)

FRAME_BACKEND = "hgraph.persistence.frame"


class RecordAsOf(Enum):
    """Whether a durable recording carries an as-of column (RFC 0019).

    ``INHERIT`` defers to the wiring-time table configuration, which is what
    makes the configuration local WITH a global default rather than a second
    override registry keyed on name.

    Moved here from ``hgraph`` at RFC 0025 checkpoint 5: core's memory and
    testing backends have no as-of column, so the vocabulary belongs to the
    extension that implements it. ``hgraph.RecordAsOf`` remains a deprecated
    alias for the deprecation window.
    """
    # Values match the native persistence::RecordAsOf scale.
    INHERIT = 0
    TRACK = 1
    OMIT = 2


class RecordRemoves(Enum):
    """Whether a durable recording carries a removed flag per TSD level
    (RFC 0019).

    Omitting them means a removal records NOTHING - the stream simply stops
    carrying that key, which is how most data streams are consumed.

    Moved here from ``hgraph`` at RFC 0025 checkpoint 5; ``hgraph.RecordRemoves``
    remains a deprecated alias for the deprecation window.
    """
    # Values match the native persistence::RecordRemoves scale.
    INHERIT = 0
    OMIT = 1
    TRACK = 2


# Associate the Python spellings with the native scalars through the keyed
# installer, so a registry reset-and-rebuild replays the association.
_hgraph_persistence._register_recording_option_enums(RecordAsOf, RecordRemoves)


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
