from __future__ import annotations

from datetime import datetime
from pathlib import Path

import _hgraph
import pytest

import hgraph_fabric as hgf
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


def test_python_operator_registration_survives_registry_reset():
    _hgraph.reset_registries()
    wiring = _hgraph.Wiring()
    with use_wiring(wiring):
        value = hgf.subscribe_data("python/reset-input")
        hgf.publish_data("python/reset-output", value)
    wiring.build_services()
