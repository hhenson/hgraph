"""Compatibility access to state-backed runtime configuration helpers."""

from ._global_keys import (
    get_recorder_api,
    get_recording_label,
    set_recorder_api,
    set_recording_label,
)
from ._global_state import GlobalState

__all__ = (
    "GlobalState",
    "get_recorder_api",
    "get_recording_label",
    "set_recorder_api",
    "set_recording_label",
)
