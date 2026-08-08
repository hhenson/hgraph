"""GlobalState keys retained for source compatibility with Python hgraph."""

from hgraph._wiring._state import (
    get_recorder_api,
    get_recording_label,
    set_recorder_api,
    set_recording_label,
)

__all__ = (
    "get_recorder_api",
    "get_recording_label",
    "set_recorder_api",
    "set_recording_label",
)
