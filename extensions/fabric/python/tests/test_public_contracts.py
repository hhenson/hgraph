from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import _hgraph
import hgraph as hg
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
        value = hgf.subscribe_data("python/input")
        hgf.publish_data("python/output", value)
    wiring.build_services()


def test_python_operator_validation_is_wiring_time():
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
        first = hgf.subscribe_data("python/input-a")
        second = hgf.subscribe_data("python/input-b")
        hgf.publish_data("python/output", first)
        with pytest.raises(RuntimeError, match="data id already has a publisher"):
            hgf.publish_data("python/output", second)


def test_explicit_dependency_selection_rejects_empty():
    with pytest.raises(ValueError, match="must not be empty"):
        hgf.DependencySelection.explicit()


def _finish_contract_wiring(wiring: _hgraph.Wiring) -> None:
    with pytest.raises(RuntimeError, match="requires FabricConfig"):
        wiring.run()


def _runtime_revision(
    revision: int,
    output_version: int,
    as_of: datetime,
) -> hgf.DataRevision:
    return hgf.DataRevision(
        format_version=1,
        data_id="python/runtime",
        revision=revision,
        output_version=output_version,
        dependencies=(),
        self_predecessor=None,
        as_of=as_of,
    )


def test_python_subscription_modes_execute_the_native_runtime_strategies():
    base = datetime(2026, 1, 1)
    fixture = hgf._MemoryFabricFixture("python/subscription-runtime")
    for revision in (
        _runtime_revision(1, 1, base + timedelta(microseconds=1)),
        _runtime_revision(2, 2, base + timedelta(microseconds=2)),
        _runtime_revision(3, 3, base + timedelta(microseconds=3)),
    ):
        fixture.seed(hgf.encode_revision(revision))

    @graph
    def replay_subscription() -> TS[Frame]:
        return hgf.subscribe_data(
            "python/runtime", mode=hgf.SubscriptionMode.REPLAY
        )

    @graph
    def snapshot_subscription() -> TS[Frame]:
        return hgf.subscribe_data(
            "python/runtime",
            mode=hgf.SubscriptionMode.SNAPSHOT,
            as_of=base + timedelta(microseconds=2),
        )

    with hg.GlobalState() as state:
        fixture.install(state._impl)
        replay = hg.run_graph(
            replay_subscription,
            start_time=base + timedelta(microseconds=1),
            end_time=base + timedelta(microseconds=3),
        )
        snapshot = hg.run_graph(snapshot_subscription)

    assert [value.get_column("value")[0] for _, value in replay] == [1, 2]
    assert [when for when, _ in replay] == [
        base + timedelta(microseconds=1),
        base + timedelta(microseconds=2),
    ]
    assert [value.get_column("value")[0] for _, value in snapshot] == [2]
    assert snapshot[0][0] == hg.MIN_ST


def test_python_planner_wires_direct_shared_conditional_and_nested_graphs():
    @graph
    def nested_subscription() -> TS[Frame]:
        return hgf.subscribe_data("python/nested")

    wiring = _hgraph.Wiring()
    with use_wiring(wiring):
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
        value = hgf.subscribe_data("python/reset-input")
        hgf.publish_data("python/reset-output", value)
    wiring.build_services()
