import pytest
from hgraph._feature_switch import is_feature_enabled


def test_cpp_node_builder_factories_are_exposed():
    import hgraph._hgraph as _hgraph

    assert hasattr(_hgraph, "op_nothing_impl")
    assert hasattr(_hgraph, "op_null_sink_impl")
    assert hasattr(_hgraph, "op_const_default")
    for factory_name in (
        "op_add_float_to_int",
        "op_add_int_to_int",
        "op_add_float_to_float",
        "op_add_int_to_float",
        "op_sub_int_from_int",
        "op_sub_float_from_float",
        "op_sub_int_from_float",
        "op_sub_float_from_int",
        "op_mul_float_and_int",
        "op_mul_int_and_int",
        "op_mul_float_and_float",
        "op_mul_int_and_float",
        "op_div_numbers",
        "op_floordiv_numbers",
        "op_floordiv_ints",
        "op_mod_numbers",
        "op_mod_ints",
        "op_divmod_numbers",
        "op_divmod_ints",
        "op_pow_int_float",
        "op_pow_float_int",
        "op_eq_float_int",
        "op_eq_int_float",
        "op_eq_float_float",
        "op_eq_enum",
        "op_lt_enum",
        "op_le_enum",
        "op_gt_enum",
        "op_ge_enum",
        "op_getattr_enum_name",
        "op_ln_impl",
    ):
        assert hasattr(_hgraph, factory_name)


def test_cpp_node_builder_factory_requires_signature_and_scalars():
    import hgraph._hgraph as _hgraph

    with pytest.raises(TypeError, match="signature"):
        _hgraph.op_nothing_impl()


@pytest.mark.skipif(not is_feature_enabled("use_cpp"), reason="C++ runtime only")
def test_cpp_operator_mappings_are_registered():
    import hgraph._hgraph as _hgraph
    from hgraph._wiring._cpp_node_registry import lookup_cpp_node_builder

    assert lookup_cpp_node_builder("hgraph::const_default") is _hgraph.op_const_default
    assert lookup_cpp_node_builder("hgraph::nothing_impl") is _hgraph.op_nothing_impl
    assert lookup_cpp_node_builder("hgraph::null_sink_impl") is _hgraph.op_null_sink_impl
    expected_mappings = {
        "hgraph::add_float_to_int": _hgraph.op_add_float_to_int,
        "hgraph::add_int_to_int": _hgraph.op_add_int_to_int,
        "hgraph::add_float_to_float": _hgraph.op_add_float_to_float,
        "hgraph::add_int_to_float": _hgraph.op_add_int_to_float,
        "hgraph::sub_int_from_int": _hgraph.op_sub_int_from_int,
        "hgraph::sub_float_from_float": _hgraph.op_sub_float_from_float,
        "hgraph::sub_int_from_float": _hgraph.op_sub_int_from_float,
        "hgraph::sub_float_from_int": _hgraph.op_sub_float_from_int,
        "hgraph::mul_float_and_int": _hgraph.op_mul_float_and_int,
        "hgraph::mul_int_and_int": _hgraph.op_mul_int_and_int,
        "hgraph::mul_float_and_float": _hgraph.op_mul_float_and_float,
        "hgraph::mul_int_and_float": _hgraph.op_mul_int_and_float,
        "hgraph::div_numbers": _hgraph.op_div_numbers,
        "hgraph::floordiv_numbers": _hgraph.op_floordiv_numbers,
        "hgraph::floordiv_ints": _hgraph.op_floordiv_ints,
        "hgraph::mod_numbers": _hgraph.op_mod_numbers,
        "hgraph::mod_ints": _hgraph.op_mod_ints,
        "hgraph::divmod_numbers": _hgraph.op_divmod_numbers,
        "hgraph::divmod_ints": _hgraph.op_divmod_ints,
        "hgraph::pow_int_float": _hgraph.op_pow_int_float,
        "hgraph::pow_float_int": _hgraph.op_pow_float_int,
        "hgraph::eq_float_int": _hgraph.op_eq_float_int,
        "hgraph::eq_int_float": _hgraph.op_eq_int_float,
        "hgraph::eq_float_float": _hgraph.op_eq_float_float,
        "hgraph::eq_enum": _hgraph.op_eq_enum,
        "hgraph::lt_enum": _hgraph.op_lt_enum,
        "hgraph::le_enum": _hgraph.op_le_enum,
        "hgraph::gt_enum": _hgraph.op_gt_enum,
        "hgraph::ge_enum": _hgraph.op_ge_enum,
        "hgraph::getattr_enum_name": _hgraph.op_getattr_enum_name,
        "hgraph::ln_impl": _hgraph.op_ln_impl,
    }
    for cpp_node_id, expected_factory in expected_mappings.items():
        assert lookup_cpp_node_builder(cpp_node_id) is expected_factory
