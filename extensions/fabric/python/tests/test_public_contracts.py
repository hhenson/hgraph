from __future__ import annotations

from datetime import datetime
from pathlib import Path

import _hgraph
import pytest

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


def test_python_public_operators_wire_through_native_registry():
    wiring = _hgraph.Wiring()
    with use_wiring(wiring):
        hgf.register_memory_fabric_service()
        value = hgf.subscribe_data(
            "python/input", mode=hgf.SubscriptionMode.LIVE
        )
        hgf.publish_data("python/output", value)
    wiring.run()


def test_python_operator_validation_is_wiring_time():
    with pytest.raises(TypeError, match="mode"):
        hgf.subscribe_data("python/input")

    wiring = _hgraph.Wiring()
    with use_wiring(wiring):
        with pytest.raises(RuntimeError, match="Snapshot requires as_of"):
            hgf.subscribe_data(
                "python/input", mode=hgf.SubscriptionMode.SNAPSHOT
            )
        with pytest.raises(RuntimeError, match="as_of is valid only for Snapshot"):
            hgf.subscribe_data(
                "python/input",
                mode=hgf.SubscriptionMode.LIVE,
                as_of=datetime(2026, 1, 1),
            )


def test_python_wiring_rejects_duplicate_publishers():
    wiring = _hgraph.Wiring()
    with use_wiring(wiring):
        first = hgf.subscribe_data(
            "python/input-a", mode=hgf.SubscriptionMode.LIVE
        )
        second = hgf.subscribe_data(
            "python/input-b", mode=hgf.SubscriptionMode.LIVE
        )
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
        return hgf.subscribe_data(
            "python/nested", mode=hgf.SubscriptionMode.LIVE
        )

    wiring = _hgraph.Wiring()
    with use_wiring(wiring):
        hgf.register_memory_fabric_service()
        direct = hgf.subscribe_data(
            "python/direct", mode=hgf.SubscriptionMode.LIVE
        )
        hgf.publish_data("python/direct-output", direct)

        shared = hgf.subscribe_data(
            "python/shared", mode=hgf.SubscriptionMode.LIVE
        )
        hgf.publish_data("python/shared-left", shared)
        hgf.publish_data("python/shared-right", shared)

        left = hgf.subscribe_data(
            "python/conditional-a", mode=hgf.SubscriptionMode.LIVE
        )
        right = hgf.subscribe_data(
            "python/conditional-b", mode=hgf.SubscriptionMode.LIVE
        )
        hgf.publish_data(
            "python/conditional-output", if_then_else(True, left, right)
        )

        hgf.publish_data("python/nested-output", nested_subscription())
    _finish_contract_wiring(wiring)


def test_python_explicit_dependencies_use_the_native_planner():
    wiring = _hgraph.Wiring()
    with use_wiring(wiring):
        hgf.register_memory_fabric_service()
        source = hgf.subscribe_data(
            "python/explicit-input", mode=hgf.SubscriptionMode.LIVE
        )
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
        value = hgf.subscribe_data(
            "python/value", mode=hgf.SubscriptionMode.LIVE
        )
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
        first = hgf.subscribe_data(
            "python/duplicate", mode=hgf.SubscriptionMode.LIVE
        )
        second = hgf.subscribe_data(
            "python/duplicate", mode=hgf.SubscriptionMode.LIVE
        )
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
        value = hgf.subscribe_data(
            "python/reset-input", mode=hgf.SubscriptionMode.LIVE
        )
        hgf.publish_data("python/reset-output", value)
    wiring.run()
