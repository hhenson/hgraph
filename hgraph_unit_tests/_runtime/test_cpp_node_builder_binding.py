import pytest


def test_cpp_node_builder_factory_is_exposed():
    import hgraph._hgraph as _hgraph

    assert hasattr(_hgraph, "_cpp_noop_builder")


def test_cpp_node_builder_factory_requires_signature_and_scalars():
    import hgraph._hgraph as _hgraph

    with pytest.raises(TypeError, match="signature"):
        _hgraph._cpp_noop_builder()

