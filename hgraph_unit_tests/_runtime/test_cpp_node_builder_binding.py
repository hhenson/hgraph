import pytest
from hgraph._feature_switch import is_feature_enabled


def test_cpp_node_builder_factories_are_exposed():
    import hgraph._hgraph as _hgraph

    assert hasattr(_hgraph, "op_nothing_impl")
    assert hasattr(_hgraph, "op_null_sink_impl")
    assert hasattr(_hgraph, "op_const_default")
    assert hasattr(_hgraph, "op_add_float_to_int")
    assert hasattr(_hgraph, "op_eq_float_float")
    assert hasattr(_hgraph, "op_ln_impl")


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
    assert lookup_cpp_node_builder("hgraph::add_float_to_int") is _hgraph.op_add_float_to_int
    assert lookup_cpp_node_builder("hgraph::eq_float_float") is _hgraph.op_eq_float_float
    assert lookup_cpp_node_builder("hgraph::ln_impl") is _hgraph.op_ln_impl
