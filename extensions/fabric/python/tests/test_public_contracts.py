from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import _hgraph
import pyarrow as pa
import pytest

import hgraph as hg
import hgraph_fabric as hgf
from hgraph import TS, Frame, graph, if_then_else
from hgraph.test import use_wiring


FIXTURES = Path(__file__).resolve().parents[2] / "tests" / "fixtures"


def _fixture(name: str) -> bytes:
    return bytes.fromhex((FIXTURES / name).read_text())


def _revision() -> hgf.DataRevision:
    return hgf.DataRevision(
        format_version=1,
        data_id="derived/α",
        revision=3,
        output_version=42,
        dependencies=(
            hgf.DataDependency("input-b", 11),
            hgf.DataDependency("input-a", 7),
        ),
        self_predecessor=41,
        as_of=datetime(2026, 1, 2, 3, 4, 5, 6007),
    )


def test_python_package_loads_its_native_persistence_dependency_first():
    assert "hgraph_persistence" in sys.modules


def test_python_memory_config_validates_notification_backpressure_limit():
    with pytest.raises(ValueError, match="request limit must be positive"):
        hgf.make_memory_fabric_config(notification_request_limit=0)


def test_python_codec_uses_shared_native_golden_fixtures():
    encoded = hgf.encode_revision(_revision())
    assert encoded == _fixture("revision_v1.hex")

    decoded = hgf.decode_revision(encoded)
    assert decoded == hgf.DataRevision(
        format_version=1,
        data_id="derived/α",
        revision=3,
        output_version=42,
        dependencies=(
            hgf.DataDependency("input-a", 7),
            hgf.DataDependency("input-b", 11),
        ),
        self_predecessor=41,
        as_of=datetime(2026, 1, 2, 3, 4, 5, 6007),
    )
    assert hgf.encode_as_of_reference(3) == _fixture("as_of_v1.hex")
    assert hgf.encode_latest_reference(3) == _fixture("latest_v1.hex")
    assert hgf.decode_as_of_reference(_fixture("as_of_v1.hex")) == 3
    assert hgf.decode_latest_reference(_fixture("latest_v1.hex")) == 3


def test_python_codec_does_not_replace_an_unsupported_format_version():
    revision = _revision()
    invalid = hgf.DataRevision(
        format_version=2,
        data_id=revision.data_id,
        revision=revision.revision,
        output_version=revision.output_version,
        dependencies=revision.dependencies,
        self_predecessor=revision.self_predecessor,
        as_of=revision.as_of,
    )
    with pytest.raises(ValueError, match="unsupported fabric revision format"):
        hgf.encode_revision(invalid)


def test_python_codec_uses_hgraph_datetime_timezone_semantics():
    revision = _revision()
    aware = hgf.DataRevision(
        format_version=revision.format_version,
        data_id=revision.data_id,
        revision=revision.revision,
        output_version=revision.output_version,
        dependencies=revision.dependencies,
        self_predecessor=revision.self_predecessor,
        as_of=datetime(
            2026,
            1,
            2,
            7,
            4,
            5,
            6007,
            tzinfo=timezone(timedelta(hours=4)),
        ),
    )

    decoded = hgf.decode_revision(hgf.encode_revision(aware))

    assert decoded.as_of == datetime(2026, 1, 2, 3, 4, 5, 6007)
    assert decoded.as_of.tzinfo is None


@pytest.mark.parametrize(
    "as_of",
    (
        datetime.max.replace(
            tzinfo=timezone(-timedelta(hours=23, minutes=59))
        ),
        datetime.min.replace(
            tzinfo=timezone(timedelta(hours=23, minutes=59))
        ),
    ),
)
def test_python_codec_rejects_datetime_normalization_outside_python_range(as_of):
    revision = _revision()
    invalid = hgf.DataRevision(
        format_version=revision.format_version,
        data_id=revision.data_id,
        revision=revision.revision,
        output_version=revision.output_version,
        dependencies=revision.dependencies,
        self_predecessor=revision.self_predecessor,
        as_of=as_of,
    )

    with pytest.raises(TypeError):
        hgf.encode_revision(invalid)


def test_python_public_operators_wire_through_native_registry():
    wiring = _hgraph.Wiring()
    with use_wiring(wiring):
        hgf.register_memory_fabric_service()
        value = hgf.subscribe_data("python/input")
        hgf.publish_data("python/output", value)
    wiring.run()


def test_python_service_paths_select_independent_configurations():
    left = hgf.make_memory_fabric_config(prefix="python/path/left")
    right = hgf.make_memory_fabric_config(prefix="python/path/right")

    @graph
    def publish_to_both_paths() -> None:
        hgf.register_fabric_service(left, path="left-fabric")
        hgf.register_fabric_service(right, path="right-fabric")
        left_value = hg.const(pa.table({"value": [1]}), tp=TS[Frame])
        right_value = hg.const(pa.table({"value": [2]}), tp=TS[Frame])
        hgf.publish_data("prices", left_value, path="left-fabric")
        hgf.publish_data("prices", right_value, path="right-fabric")

    with hg.GlobalState():
        hg.run_graph(
            publish_to_both_paths,
            start_time=hg.MIN_ST,
            end_time=hg.MIN_ST + timedelta(microseconds=5),
        )

    previous = _hgraph.polars_frames()
    _hgraph.set_polars_frames(False)
    try:
        left_frame = hgf.load_data(left, "prices")
        right_frame = hgf.load_data(right, "prices")
    finally:
        _hgraph.set_polars_frames(previous)

    assert left_frame.to_pydict() == {"value": [1]}
    assert right_frame.to_pydict() == {"value": [2]}


def test_python_subscription_execution_policy_is_service_scoped():
    with pytest.raises(TypeError, match="unexpected keyword argument 'mode'"):
        hgf.subscribe_data("python/input", mode="live")
    with pytest.raises(TypeError, match="unexpected keyword argument 'as_of'"):
        hgf.subscribe_data("python/input", as_of=datetime(2026, 1, 1))
    with pytest.raises(TypeError, match="unexpected keyword argument 'as_of'"):
        hgf.register_memory_fabric_service(as_of=datetime(2026, 1, 1))
    wiring = _hgraph.Wiring()
    with use_wiring(wiring):
        hgf.register_memory_fabric_service()
        hgf.subscribe_data("python/input")
    wiring.run()


def _config_with_load_fixture() -> hgf.FabricConfig:
    config = hgf.make_memory_fabric_config(prefix="python/load-as-of")

    @graph
    def publish_load_fixture() -> None:
        hgf.register_fabric_service(config)
        value = hg.const(pa.table({"value": [42]}), tp=TS[Frame])
        hgf.publish_data("prices", value)

    with hg.GlobalState():
        hg.run_graph(
            publish_load_fixture,
            start_time=hg.MIN_ST,
            end_time=hg.MIN_ST + timedelta(microseconds=5),
        )
    return config


def test_python_load_data_is_a_non_graph_arrow_point_lookup():
    config = _config_with_load_fixture()

    assert hgf.load_data(config, "prices", datetime(1971, 1, 1)) is None
    previous = _hgraph.polars_frames()
    _hgraph.set_polars_frames(False)
    try:
        loaded = hgf.load_data(config, "prices")
    finally:
        _hgraph.set_polars_frames(previous)

    assert isinstance(loaded, pa.Table)
    assert loaded.to_pydict() == {"value": [42]}


def test_python_load_data_honours_polars_frame_presentation():
    polars = pytest.importorskip("polars")
    config = _config_with_load_fixture()
    previous = _hgraph.polars_frames()
    _hgraph.set_polars_frames(True)
    try:
        loaded = hgf.load_data(config, "prices")
    finally:
        _hgraph.set_polars_frames(previous)

    assert isinstance(loaded, polars.DataFrame)
    assert loaded.to_dict(as_series=False) == {"value": [42]}


def test_python_wiring_rejects_duplicate_publishers():
    wiring = _hgraph.Wiring()
    with use_wiring(wiring):
        first = hgf.subscribe_data("python/input-a")
        second = hgf.subscribe_data("python/input-b")
        hgf.publish_data("python/output", first)
        with pytest.raises(RuntimeError, match="data id already has a publisher"):
            hgf.publish_data("python/output", second)


def test_explicit_dependency_selection_rejects_empty():
    with pytest.raises(ValueError, match="must not be empty"):
        hgf.DependencySelection.explicit()


def _finish_contract_wiring(wiring: _hgraph.Wiring) -> None:
    wiring.run()


def test_python_planner_wires_direct_shared_conditional_and_nested_graphs():
    @graph
    def nested_subscription() -> TS[Frame]:
        return hgf.subscribe_data("python/nested")

    wiring = _hgraph.Wiring()
    with use_wiring(wiring):
        hgf.register_memory_fabric_service()
        direct = hgf.subscribe_data("python/direct")
        hgf.publish_data("python/direct-output", direct)

        shared = hgf.subscribe_data("python/shared")
        hgf.publish_data("python/shared-left", shared)
        hgf.publish_data("python/shared-right", shared)

        left = hgf.subscribe_data("python/conditional-a")
        right = hgf.subscribe_data("python/conditional-b")
        hgf.publish_data(
            "python/conditional-output", if_then_else(True, left, right)
        )

        hgf.publish_data("python/nested-output", nested_subscription())
    _finish_contract_wiring(wiring)


def test_python_explicit_dependencies_use_the_native_planner():
    wiring = _hgraph.Wiring()
    with use_wiring(wiring):
        hgf.register_memory_fabric_service()
        source = hgf.subscribe_data("python/explicit-input")
        handle = hgf.dependency_handle(source)
        hgf.publish_data(
            "python/explicit-output",
            source,
            dependencies=hgf.DependencySelection.explicit(handle),
        )
    _finish_contract_wiring(wiring)


def test_python_explicit_dependencies_reject_arbitrary_and_duplicate_sources():
    with pytest.raises(TypeError, match="requires a WiringPort"):
        hgf.dependency_handle("arbitrary")

    wiring = _hgraph.Wiring()
    with use_wiring(wiring):
        value = hgf.subscribe_data("python/value")
        arbitrary = hgf.DependencySelection.explicit(
            hgf.DependencyHandle("arbitrary")
        )
        with pytest.raises(Exception, match="no matching overload"):
            hgf.publish_data(
                "python/arbitrary-output",
                value,
                dependencies=arbitrary,
            )

    wiring = _hgraph.Wiring()
    with use_wiring(wiring):
        first = hgf.subscribe_data("python/duplicate")
        second = hgf.subscribe_data("python/duplicate")
        duplicate = hgf.DependencySelection.explicit(
            hgf.dependency_handle(first),
            hgf.dependency_handle(second),
        )
        with pytest.raises(Exception, match="dependency data ids must be unique"):
            hgf.publish_data(
                "python/duplicate-output",
                first,
                dependencies=duplicate,
            )


def test_python_operator_registration_survives_registry_reset():
    _hgraph.reset_registries()
    wiring = _hgraph.Wiring()
    with use_wiring(wiring):
        hgf.register_memory_fabric_service()
        value = hgf.subscribe_data("python/reset-input")
        hgf.publish_data("python/reset-output", value)
    wiring.run()
