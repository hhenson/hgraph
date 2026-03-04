from __future__ import annotations

from collections.abc import Callable, Mapping
from threading import RLock
from typing import Any

__all__ = (
    "clear_cpp_node_mappings",
    "derive_cpp_node_id",
    "list_cpp_node_mappings",
    "lookup_cpp_node_builder",
    "lookup_cpp_node_builder_for_callable",
    "merge_cpp_node_mappings",
    "register_cpp_node_builder",
    "register_cpp_node_builder_for_callable",
    "set_cpp_node_mappings",
    "try_derive_cpp_node_id",
)


_CPP_NODE_MAPPINGS: dict[str, Any] = {}
_CPP_NODE_MAPPING_LOCK = RLock()


def _split_cpp_node_id(cpp_node_id: str) -> tuple[str, ...]:
    if not isinstance(cpp_node_id, str) or not cpp_node_id:
        raise ValueError("cpp_node_id must be a non-empty string")

    segments = tuple(segment for segment in cpp_node_id.split("::") if segment)
    if not segments:
        raise ValueError(f"Invalid cpp_node_id '{cpp_node_id}'")
    return segments


def _filtered_module_segments(module_path: str) -> tuple[str, ...]:
    return tuple(segment for segment in module_path.split(".") if segment and not segment.startswith("_"))


def try_derive_cpp_node_id(fn: Callable) -> str | None:
    module_name = getattr(fn, "__module__", None)
    fn_name = getattr(fn, "__name__", None)
    if not isinstance(module_name, str) or not module_name:
        return None
    if not isinstance(fn_name, str) or not fn_name:
        return None

    module_segments = _filtered_module_segments(module_name)
    if not module_segments:
        return None

    return "::".join((*module_segments, fn_name))


def derive_cpp_node_id(fn: Callable) -> str:
    cpp_node_id = try_derive_cpp_node_id(fn)
    if cpp_node_id is None:
        raise ValueError(f"Could not derive a C++ node id from callable '{fn!r}'")
    return cpp_node_id


def _validated_mapping_tree(mapping: Mapping[str, Any], path: tuple[str, ...] = ()) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in mapping.items():
        if not isinstance(key, str) or not key:
            joined = "::".join(path) if path else "<root>"
            raise TypeError(f"Invalid mapping key '{key!r}' under '{joined}'")

        if isinstance(value, Mapping):
            out[key] = _validated_mapping_tree(value, (*path, key))
        elif callable(value):
            out[key] = value
        else:
            joined = "::".join((*path, key))
            raise TypeError(
                f"Invalid mapping value for '{joined}'. Expected nested mapping or callable, got {type(value).__name__}"
            )
    return out


def _merge_mapping_tree(target: dict[str, Any], incoming: dict[str, Any], path: tuple[str, ...] = ()) -> None:
    for key, value in incoming.items():
        if isinstance(value, dict):
            existing = target.get(key)
            if existing is None:
                target[key] = value
                continue
            if callable(existing):
                joined = "::".join((*path, key))
                raise ValueError(f"Cannot merge namespace '{joined}' into an existing callable mapping")
            _merge_mapping_tree(existing, value, (*path, key))
        else:
            existing = target.get(key)
            if isinstance(existing, dict):
                joined = "::".join((*path, key))
                raise ValueError(f"Cannot overwrite namespace '{joined}' with callable mapping")
            target[key] = value


def clear_cpp_node_mappings() -> None:
    with _CPP_NODE_MAPPING_LOCK:
        _CPP_NODE_MAPPINGS.clear()


def set_cpp_node_mappings(mapping: Mapping[str, Any]) -> None:
    validated = _validated_mapping_tree(mapping)
    with _CPP_NODE_MAPPING_LOCK:
        _CPP_NODE_MAPPINGS.clear()
        _CPP_NODE_MAPPINGS.update(validated)


def merge_cpp_node_mappings(mapping: Mapping[str, Any]) -> None:
    validated = _validated_mapping_tree(mapping)
    with _CPP_NODE_MAPPING_LOCK:
        _merge_mapping_tree(_CPP_NODE_MAPPINGS, validated)


def register_cpp_node_builder(cpp_node_id: str, builder_factory: Callable) -> None:
    if not callable(builder_factory):
        raise TypeError("builder_factory must be callable")

    segments = _split_cpp_node_id(cpp_node_id)
    with _CPP_NODE_MAPPING_LOCK:
        target = _CPP_NODE_MAPPINGS
        for segment in segments[:-1]:
            existing = target.get(segment)
            if existing is None:
                target[segment] = {}
                existing = target[segment]
            elif callable(existing):
                joined = "::".join(segments[:-1])
                raise ValueError(f"Cannot create namespace '{joined}' under callable mapping")
            target = existing

        leaf_key = segments[-1]
        existing_leaf = target.get(leaf_key)
        if isinstance(existing_leaf, dict):
            raise ValueError(f"Cannot overwrite namespace '{cpp_node_id}' with callable mapping")
        target[leaf_key] = builder_factory


def register_cpp_node_builder_for_callable(fn: Callable, builder_factory: Callable) -> str:
    cpp_node_id = derive_cpp_node_id(fn)
    register_cpp_node_builder(cpp_node_id, builder_factory)
    return cpp_node_id


def lookup_cpp_node_builder(cpp_node_id: str) -> Callable | None:
    segments = _split_cpp_node_id(cpp_node_id)
    with _CPP_NODE_MAPPING_LOCK:
        current: Any = _CPP_NODE_MAPPINGS
        for segment in segments:
            if not isinstance(current, Mapping):
                return None
            current = current.get(segment)
            if current is None:
                return None
        return current if callable(current) else None


def lookup_cpp_node_builder_for_callable(fn: Callable) -> tuple[str | None, Callable | None]:
    cpp_node_id = try_derive_cpp_node_id(fn)
    if cpp_node_id is None:
        return None, None
    return cpp_node_id, lookup_cpp_node_builder(cpp_node_id)


def list_cpp_node_mappings() -> Mapping[str, str]:
    out: dict[str, str] = {}

    def _walk(prefix: tuple[str, ...], value: Any) -> None:
        if isinstance(value, Mapping):
            for key, child in value.items():
                _walk((*prefix, key), child)
        elif callable(value):
            cpp_node_id = "::".join(prefix)
            out[cpp_node_id] = f"{value.__module__}.{value.__name__}"

    with _CPP_NODE_MAPPING_LOCK:
        _walk(tuple(), _CPP_NODE_MAPPINGS)

    return out
