from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Optional

from ._global_state import GlobalState

__all__ = (
    "OutputKeyBuilder",
    "DefaultOutputKeyBuilder",
    "get_output_key_builder",
    "set_output_key_builder",
    "output_key",
    "output_subscriber_key",
    "context_output_key",
    "component_key",
    # Recorder API registry helpers
    "set_recorder_api",
    "get_recorder_api",
    "set_recording_label",
    "get_recording_label",
    "reset_output_key_builder"
)


class OutputKeyBuilder(Protocol):
    """Protocol for building keys used to store/retrieve outputs in GlobalState.

    This isolates string construction so other runtimes (e.g. C++) can share the scheme.
    """

    def output_key(self, path: str) -> str:
        """Key for storing primary output reference of a service/graph at path."""
        ...

    def output_subscriber_key(self, path: str) -> str:
        """Key for storing subscriber associated with the output at path."""
        ...

    def context_output_key(self, owning_graph_id: tuple[int, ...], path: str) -> str:
        """Key for storing a context output reference captured for cross-graph usage."""
        ...

    def component_key(self, id_or_label: str | int) -> str:
        """Key for storing component-level references by id or label."""
        ...


@dataclass(frozen=True)
class DefaultOutputKeyBuilder:
    """Default implementation that preserves existing key formats used across the codebase."""

    def output_key(self, path: str) -> str:
        # Historically, the path itself is the key for output reference
        return path

    def output_subscriber_key(self, path: str) -> str:
        # Historically uses "{path}_subscriber"
        return f"{path}_subscriber"

    def context_output_key(self, owning_graph_id: tuple[int, ...], path: str) -> str:
        # Matches context_wiring.capture_context_start format: f"context-{graph_id}-{path}"
        return f"context-{owning_graph_id}-{path}"

    def component_key(self, id_or_label: str | int) -> str:
        # Matches component storage format: f"component::{id_or_label}"
        return f"component::{id_or_label}"


# Recorder API keys
_RECORDER_API_KEY = "__recorder_api__"
_RECORDER_LABEL_KEY = "__recorder_api__label__"


_OUTPUT_KEY_BUILDER: OutputKeyBuilder = None

def get_output_key_builder() -> OutputKeyBuilder:
    global _OUTPUT_KEY_BUILDER
    builder: Optional[OutputKeyBuilder] = _OUTPUT_KEY_BUILDER  # type: ignore
    if builder is None:
        builder = DefaultOutputKeyBuilder()
        _OUTPUT_KEY_BUILDER = builder
    return builder


def set_output_key_builder(builder: OutputKeyBuilder) -> None:
    global _OUTPUT_KEY_BUILDER
    _OUTPUT_KEY_BUILDER = builder


def reset_output_key_builder() -> None:
    global _OUTPUT_KEY_BUILDER
    _OUTPUT_KEY_BUILDER = None


# Convenience wrappers

def output_key(path: str) -> str:
    return get_output_key_builder().output_key(path)


def output_subscriber_key(path: str) -> str:
    return get_output_key_builder().output_subscriber_key(path)


def context_output_key(owning_graph_id: tuple[int, ...], path: str) -> str:
    return get_output_key_builder().context_output_key(owning_graph_id, path)


def component_key(id_or_label: str | int) -> str:
    return get_output_key_builder().component_key(id_or_label)


# Recorder API registry helpers

def set_recorder_api(recorder: object) -> None:
    GlobalState.instance()[_RECORDER_API_KEY] = recorder


def get_recorder_api() -> object:
    return GlobalState.instance()[_RECORDER_API_KEY]


def set_recording_label(label: str) -> None:
    GlobalState.instance()[_RECORDER_LABEL_KEY] = label


def get_recording_label() -> str:
    return GlobalState.instance()[_RECORDER_LABEL_KEY]
