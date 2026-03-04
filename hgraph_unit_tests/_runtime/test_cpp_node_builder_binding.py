import pytest
from hgraph._feature_switch import is_feature_enabled


def test_cpp_node_builder_factory_is_exposed():
    import hgraph._hgraph as _hgraph

    assert hasattr(_hgraph, "_cpp_noop_builder")


def test_cpp_node_builder_factory_requires_signature_and_scalars():
    import hgraph._hgraph as _hgraph

    with pytest.raises(TypeError, match="signature"):
        _hgraph._cpp_noop_builder()


def test_cpp_const_default_builder_factory_is_exposed():
    import hgraph._hgraph as _hgraph

    assert hasattr(_hgraph, "op_const_default")


@pytest.mark.skipif(not is_feature_enabled("use_cpp"), reason="C++ runtime only")
def test_cpp_const_default_mapping_is_registered():
    import hgraph._hgraph as _hgraph
    from hgraph._wiring._cpp_node_registry import lookup_cpp_node_builder

    assert lookup_cpp_node_builder("hgraph::const_default") is _hgraph.op_const_default
