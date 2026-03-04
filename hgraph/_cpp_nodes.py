"""
C++ node mapping registration.

This module owns Python-side mapping from derived node ids to C++ builder
factories exposed from ``hgraph._hgraph``.
"""

from __future__ import annotations

from hgraph._wiring import merge_cpp_node_mappings

__all__ = ("register_cpp_node_mappings",)


def register_cpp_node_mappings(_hgraph_module) -> None:
    merge_cpp_node_mappings({
        "hgraph": {
            "const_default": _hgraph_module.op_const_default,
            "nothing_impl": _hgraph_module.op_nothing_impl,
            "null_sink_impl": _hgraph_module.op_null_sink_impl,
        }
    })
