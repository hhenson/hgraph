import pytest
from datetime import timedelta

from hgraph import TS, compute_node, const, generator, graph, null_sink, wire_graph
from hgraph._wiring._cpp_node_registry import (
    clear_cpp_node_mappings,
    derive_cpp_node_id,
    list_cpp_node_mappings,
    lookup_cpp_node_builder,
    merge_cpp_node_mappings,
    register_cpp_node_builder_for_callable,
    set_cpp_node_mappings,
)
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext


@pytest.fixture(autouse=True)
def _clear_cpp_mapping_registry():
    clear_cpp_node_mappings()
    yield
    clear_cpp_node_mappings()


def test_derive_cpp_node_id_strips_private_segments():
    def _fn():
        pass

    _fn.__module__ = "hgraph._impl._operators._getattr"
    _fn.__name__ = "getattr_cs"
    assert derive_cpp_node_id(_fn) == "hgraph::getattr_cs"


def test_nested_mapping_registration_and_lookup():
    def _builder_a(**kwargs):
        return kwargs

    def _builder_b(**kwargs):
        return kwargs

    set_cpp_node_mappings({"hgraph": {"getattr_cs": _builder_a}})
    merge_cpp_node_mappings({"hgraph": {"getitem_cs": _builder_b}})

    assert lookup_cpp_node_builder("hgraph::getattr_cs") is _builder_a
    assert lookup_cpp_node_builder("hgraph::getitem_cs") is _builder_b
    assert lookup_cpp_node_builder("hgraph::missing") is None

    assert list_cpp_node_mappings() == {
        "hgraph::getattr_cs": f"{_builder_a.__module__}.{_builder_a.__name__}",
        "hgraph::getitem_cs": f"{_builder_b.__module__}.{_builder_b.__name__}",
    }


def test_cpp_mapping_dispatch_uses_registered_builder(monkeypatch):
    from hgraph._wiring._wiring_node_class import _python_wiring_node_classes as _pwc

    monkeypatch.setattr(_pwc, "_is_cpp_runtime_enabled_for_dispatch", lambda: True)

    @compute_node
    def mapped_node(ts: TS[int]) -> TS[int]:
        return ts.value + 1

    base_builder = _pwc.PythonWiringNodeClass.BUILDER_CLASS
    if base_builder is None:
        from hgraph._impl._builder import PythonNodeBuilder

        base_builder = PythonNodeBuilder

    calls = []

    def _mapped_builder(**kwargs):
        calls.append(kwargs["signature"].name)
        return base_builder(**kwargs)

    register_cpp_node_builder_for_callable(mapped_node.fn, _mapped_builder)

    @graph
    def _g():
        null_sink(mapped_node(const(1)))

    with WiringNodeInstanceContext():
        wire_graph(_g)

    assert calls == ["mapped_node"]


def test_cpp_mapping_dispatch_raises_for_failed_builder(monkeypatch):
    from hgraph._wiring._wiring_node_class import _python_wiring_node_classes as _pwc

    monkeypatch.setattr(_pwc, "_is_cpp_runtime_enabled_for_dispatch", lambda: True)

    @compute_node
    def mapped_fail(ts: TS[int]) -> TS[int]:
        return ts.value + 1

    def _raise_builder(**kwargs):
        raise RuntimeError("boom")

    cpp_node_id = register_cpp_node_builder_for_callable(mapped_fail.fn, _raise_builder)

    @graph
    def _g():
        null_sink(mapped_fail(const(1)))

    with WiringNodeInstanceContext(), pytest.raises(CustomMessageWiringError) as e:
        wire_graph(_g)

    message = str(e.value)
    assert cpp_node_id in message
    assert "mapped_fail" in message
    assert "boom" in message


def test_cpp_mapping_dispatch_uses_registered_builder_for_generator(monkeypatch):
    from hgraph._wiring._wiring_node_class import _python_wiring_node_classes as _pwc

    monkeypatch.setattr(_pwc, "_is_cpp_runtime_enabled_for_dispatch", lambda: True)

    @generator
    def mapped_gen() -> TS[int]:
        yield timedelta(), 1

    base_builder = _pwc.PythonGeneratorWiringNodeClass.BUILDER_CLASS
    if base_builder is None:
        from hgraph._impl._builder import PythonGeneratorNodeBuilder

        base_builder = PythonGeneratorNodeBuilder

    calls = []

    def _mapped_builder(**kwargs):
        calls.append(kwargs["signature"].name)
        return base_builder(**kwargs)

    register_cpp_node_builder_for_callable(mapped_gen.fn, _mapped_builder)

    @graph
    def _g():
        null_sink(mapped_gen())

    with WiringNodeInstanceContext():
        wire_graph(_g)

    assert calls == ["mapped_gen"]
