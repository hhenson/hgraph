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
            "add_float_to_int": _hgraph_module.op_add_float_to_int,
            "add_int_to_float": _hgraph_module.op_add_int_to_float,
            "sub_int_from_float": _hgraph_module.op_sub_int_from_float,
            "sub_float_from_int": _hgraph_module.op_sub_float_from_int,
            "mul_float_and_int": _hgraph_module.op_mul_float_and_int,
            "mul_int_and_float": _hgraph_module.op_mul_int_and_float,
            "eq_float_int": _hgraph_module.op_eq_float_int,
            "eq_int_float": _hgraph_module.op_eq_int_float,
            "eq_float_float": _hgraph_module.op_eq_float_float,
            "ln_impl": _hgraph_module.op_ln_impl,
            "div_numbers": _hgraph_module.op_div_numbers,
            "floordiv_numbers": _hgraph_module.op_floordiv_numbers,
            "floordiv_ints": _hgraph_module.op_floordiv_ints,
            "mod_numbers": _hgraph_module.op_mod_numbers,
            "mod_ints": _hgraph_module.op_mod_ints,
            "pow_int_float": _hgraph_module.op_pow_int_float,
            "pow_float_int": _hgraph_module.op_pow_float_int,
        }
    })
